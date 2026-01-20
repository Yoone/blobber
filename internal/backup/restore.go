package backup

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/Yoone/blobber/internal/config"
	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

// Restore restores a backup file to the given database
func Restore(db config.Database, backupPath string) error {
	switch db.Type {
	case "file":
		return restoreFile(db, backupPath)
	case "mysql":
		return restoreMySQL(db, backupPath)
	case "postgres":
		return restorePostgres(db, backupPath)
	default:
		return fmt.Errorf("unknown database type: %s", db.Type)
	}
}

func restoreFile(db config.Database, backupPath string) error {
	reader, cleanup, err := newDecompressReader(backupPath)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}

	dst, err := os.Create(db.Path)
	if err != nil {
		return fmt.Errorf("creating destination file: %w", err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, reader); err != nil {
		return fmt.Errorf("copying file: %w", err)
	}

	return nil
}

func restoreMySQL(db config.Database, backupPath string) error {
	args := []string{
		"-h", db.Host,
		"-P", fmt.Sprintf("%d", db.Port),
		"-u", db.User,
		fmt.Sprintf("--connect-timeout=%d", ConnectTimeoutSeconds),
		db.Database,
	}

	cmd := exec.Command("mysql", args...)
	if db.Password != "" {
		cmd.Env = append(os.Environ(), "MYSQL_PWD="+db.Password)
	}

	return runRestoreCommand(cmd, backupPath)
}

func restorePostgres(db config.Database, backupPath string) error {
	args := []string{
		"-h", db.Host,
		"-p", fmt.Sprintf("%d", db.Port),
		"-U", db.User,
		"-d", db.Database,
	}

	cmd := exec.Command("psql", args...)
	// Set connection timeout and password
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGCONNECT_TIMEOUT=%d", ConnectTimeoutSeconds))
	if db.Password != "" {
		cmd.Env = append(cmd.Env, "PGPASSWORD="+db.Password)
	}

	return runRestoreCommand(cmd, backupPath)
}

func runRestoreCommand(cmd *exec.Cmd, backupPath string) error {
	reader, cleanup, err := newDecompressReader(backupPath)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}

	cmd.Stdin = reader
	// Capture stdout/stderr instead of sending to terminal (interferes with TUI)
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	if err := cmd.Run(); err != nil {
		// Include stderr in error message if available
		if stderrBuf.Len() > 0 {
			return fmt.Errorf("restore command failed: %s", strings.TrimSpace(stderrBuf.String()))
		}
		return fmt.Errorf("restore command failed: %w", err)
	}

	return nil
}

// newDecompressReader returns a reader that decompresses data based on file extension.
// Returns the reader, a cleanup function to call when done, and any error.
func newDecompressReader(path string) (io.Reader, func(), error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("opening backup file: %w", err)
	}

	switch {
	case strings.HasSuffix(path, ".gz"):
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("creating gzip reader: %w", err)
		}
		return gzReader, func() { gzReader.Close(); file.Close() }, nil

	case strings.HasSuffix(path, ".zst"):
		zstReader, err := zstd.NewReader(file)
		if err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("creating zstd reader: %w", err)
		}
		return zstReader, func() { zstReader.Close(); file.Close() }, nil

	case strings.HasSuffix(path, ".xz"):
		xzReader, err := xz.NewReader(file)
		if err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("creating xz reader: %w", err)
		}
		return xzReader, func() { file.Close() }, nil

	case strings.HasSuffix(path, ".zip"):
		// For zip, we need to use zip.OpenReader since zip requires seeking
		file.Close() // Close the regular file, zip.OpenReader opens its own handle
		zipReader, err := zip.OpenReader(path)
		if err != nil {
			return nil, nil, fmt.Errorf("opening zip file: %w", err)
		}
		if len(zipReader.File) == 0 {
			zipReader.Close()
			return nil, nil, fmt.Errorf("zip file is empty")
		}
		// Read the first file in the archive
		rc, err := zipReader.File[0].Open()
		if err != nil {
			zipReader.Close()
			return nil, nil, fmt.Errorf("opening zip entry: %w", err)
		}
		return rc, func() { rc.Close(); zipReader.Close() }, nil

	default:
		// No compression
		return file, func() { file.Close() }, nil
	}
}
