package backup

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/Yoone/blobber/internal/config"
	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

// ConnectTimeoutSeconds is the timeout for database connections (in seconds)
// Used for backup, restore, and connection testing
const ConnectTimeoutSeconds = 5

// Result contains the outcome of a backup operation
type Result struct {
	Name     string
	Filename string
	Path     string
	Size     int64
	Duration time.Duration
	Error    error
}

// Compression extensions
var compressionExt = map[string]string{
	"none": "",
	"gz":   ".gz",
	"zstd": ".zst",
	"xz":   ".xz",
	"zip":  ".zip",
}

// Run performs a backup for the given database and returns the local file path
func Run(name string, db config.Database) (*Result, error) {
	start := time.Now()

	// Create temp directory for backup
	tmpDir, err := os.MkdirTemp("", "blobber-")
	if err != nil {
		return nil, fmt.Errorf("creating temp dir: %w", err)
	}

	// Generate filename
	timestamp := time.Now().Format("20060102_150405")
	ext := ".sql"
	if db.Type == "file" {
		ext = filepath.Ext(db.Path)
		if ext == "" {
			ext = ".bak"
		}
	}
	if compExt, ok := compressionExt[db.Compression]; ok {
		ext += compExt
	}
	filename := fmt.Sprintf("%s_%s%s", name, timestamp, ext)
	outPath := filepath.Join(tmpDir, filename)

	// Perform the dump
	var dumpErr error
	switch db.Type {
	case "file":
		dumpErr = dumpFile(db, outPath)
	case "mysql":
		dumpErr = dumpMySQL(db, outPath)
	case "postgres":
		dumpErr = dumpPostgres(db, outPath)
	default:
		return nil, fmt.Errorf("unknown database type: %s", db.Type)
	}

	if dumpErr != nil {
		os.RemoveAll(tmpDir)
		return nil, dumpErr
	}

	// Get file size
	stat, err := os.Stat(outPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("stat backup file: %w", err)
	}

	return &Result{
		Name:     name,
		Filename: filename,
		Path:     outPath,
		Size:     stat.Size(),
		Duration: time.Since(start),
	}, nil
}

// Cleanup removes the temporary backup file
func Cleanup(result *Result) {
	if result != nil && result.Path != "" {
		os.RemoveAll(filepath.Dir(result.Path))
	}
}

func dumpFile(db config.Database, outPath string) error {
	src, err := os.Open(db.Path)
	if err != nil {
		return fmt.Errorf("opening source file: %w", err)
	}
	defer src.Close()

	dst, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("creating backup file: %w", err)
	}
	defer dst.Close()

	writer, cleanup, err := newCompressWriter(dst, db.Compression, filepath.Base(db.Path))
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}

	if _, err := io.Copy(writer, src); err != nil {
		return fmt.Errorf("copying file: %w", err)
	}

	return nil
}

// newCompressWriter returns a writer that compresses data according to the compression type.
// Returns the writer, a cleanup function to call when done, and any error.
func newCompressWriter(dst io.Writer, compression, filename string) (io.Writer, func(), error) {
	switch compression {
	case "none", "":
		return dst, nil, nil
	case "gz":
		w := gzip.NewWriter(dst)
		return w, func() { w.Close() }, nil
	case "zstd":
		w, err := zstd.NewWriter(dst)
		if err != nil {
			return nil, nil, fmt.Errorf("creating zstd writer: %w", err)
		}
		return w, func() { w.Close() }, nil
	case "xz":
		w, err := xz.NewWriter(dst)
		if err != nil {
			return nil, nil, fmt.Errorf("creating xz writer: %w", err)
		}
		return w, func() { w.Close() }, nil
	case "zip":
		// zip is handled specially since it's an archive format
		zw := zip.NewWriter(dst)
		fw, err := zw.Create(filename)
		if err != nil {
			return nil, nil, fmt.Errorf("creating zip entry: %w", err)
		}
		return fw, func() { zw.Close() }, nil
	default:
		return nil, nil, fmt.Errorf("unknown compression type: %s", compression)
	}
}

func dumpMySQL(db config.Database, outPath string) error {
	// Test connection first with timeout (mysqldump doesn't support --connect-timeout)
	if err := TestConnection(db); err != nil {
		return err
	}

	args := []string{
		"-h", db.Host,
		"-P", fmt.Sprintf("%d", db.Port),
		"-u", db.User,
		"--column-statistics=0", // Compatibility with MariaDB
		"--add-drop-table",      // Include DROP TABLE for clean restore
		db.Database,
	}

	cmd := exec.Command("mysqldump", args...)
	if db.Password != "" {
		cmd.Env = append(os.Environ(), "MYSQL_PWD="+db.Password)
	}

	return runDumpCommand(cmd, outPath, db.Compression, db.Database+".sql")
}

// TestConnection tests database connectivity with a timeout.
// Supports mysql and postgres database types.
func TestConnection(db config.Database) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ConnectTimeoutSeconds)*time.Second)
	defer cancel()

	var cmd *exec.Cmd
	switch db.Type {
	case "mysql":
		args := []string{
			"-h", db.Host,
			"-P", fmt.Sprintf("%d", db.Port),
			"-u", db.User,
			"-e", "SELECT 1",
			db.Database,
		}
		cmd = exec.CommandContext(ctx, "mysql", args...)
		if db.Password != "" {
			cmd.Env = append(os.Environ(), "MYSQL_PWD="+db.Password)
		}
	case "postgres":
		args := []string{
			"-h", db.Host,
			"-p", fmt.Sprintf("%d", db.Port),
			"-U", db.User,
			"-d", db.Database,
			"-c", "SELECT 1",
		}
		cmd = exec.CommandContext(ctx, "psql", args...)
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGCONNECT_TIMEOUT=%d", ConnectTimeoutSeconds))
		if db.Password != "" {
			cmd.Env = append(cmd.Env, "PGPASSWORD="+db.Password)
		}
	default:
		return nil // No connection test for file type
	}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("connection timed out after %ds", ConnectTimeoutSeconds)
		}
		if stderr.Len() > 0 {
			return fmt.Errorf("connection failed: %s", strings.TrimSpace(stderr.String()))
		}
		return fmt.Errorf("connection failed: %w", err)
	}
	return nil
}

func dumpPostgres(db config.Database, outPath string) error {
	args := []string{
		"-h", db.Host,
		"-p", fmt.Sprintf("%d", db.Port),
		"-U", db.User,
		"--clean",      // Include DROP statements for clean restore
		"--if-exists",  // Don't error if objects don't exist
		db.Database,
	}

	cmd := exec.Command("pg_dump", args...)
	// Set connection timeout and password
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGCONNECT_TIMEOUT=%d", ConnectTimeoutSeconds))
	if db.Password != "" {
		cmd.Env = append(cmd.Env, "PGPASSWORD="+db.Password)
	}

	return runDumpCommand(cmd, outPath, db.Compression, db.Database+".sql")
}

func runDumpCommand(cmd *exec.Cmd, outPath, compression, innerFilename string) error {
	outFile, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("creating output file: %w", err)
	}
	defer outFile.Close()

	writer, cleanup, err := newCompressWriter(outFile, compression, innerFilename)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("creating stdout pipe: %w", err)
	}

	// Capture stderr instead of sending to terminal (interferes with TUI)
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting command: %w", err)
	}

	if _, err := io.Copy(writer, stdout); err != nil {
		return fmt.Errorf("writing output: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		// Include stderr in error message if available
		if stderrBuf.Len() > 0 {
			return fmt.Errorf("command failed: %s", strings.TrimSpace(stderrBuf.String()))
		}
		return fmt.Errorf("command failed: %w", err)
	}

	return nil
}
