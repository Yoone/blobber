package backup

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yoone/blobber/internal/config"
	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

func TestCompressionExt(t *testing.T) {
	tests := []struct {
		compression string
		expected    string
	}{
		{"none", ""},
		{"gz", ".gz"},
		{"zstd", ".zst"},
		{"xz", ".xz"},
		{"zip", ".zip"},
	}

	for _, tt := range tests {
		t.Run(tt.compression, func(t *testing.T) {
			ext, ok := compressionExt[tt.compression]
			if !ok {
				t.Fatalf("compressionExt[%q] not found", tt.compression)
			}
			if ext != tt.expected {
				t.Errorf("compressionExt[%q] = %q, want %q", tt.compression, ext, tt.expected)
			}
		})
	}
}

func TestNewCompressWriter(t *testing.T) {
	testData := []byte("hello world test data for compression")

	t.Run("none compression", func(t *testing.T) {
		var buf bytes.Buffer
		w, cleanup, err := newCompressWriter(&buf, "none", "test.txt")
		if err != nil {
			t.Fatalf("newCompressWriter() error = %v", err)
		}
		if cleanup != nil {
			t.Error("cleanup should be nil for 'none' compression")
		}

		w.Write(testData)

		if !bytes.Equal(buf.Bytes(), testData) {
			t.Errorf("output differs from input for 'none' compression")
		}
	})

	t.Run("empty compression", func(t *testing.T) {
		var buf bytes.Buffer
		w, cleanup, err := newCompressWriter(&buf, "", "test.txt")
		if err != nil {
			t.Fatalf("newCompressWriter() error = %v", err)
		}
		if cleanup != nil {
			t.Error("cleanup should be nil for empty compression")
		}

		w.Write(testData)

		if !bytes.Equal(buf.Bytes(), testData) {
			t.Errorf("output differs from input for empty compression")
		}
	})

	t.Run("gz compression", func(t *testing.T) {
		var buf bytes.Buffer
		w, cleanup, err := newCompressWriter(&buf, "gz", "test.txt")
		if err != nil {
			t.Fatalf("newCompressWriter() error = %v", err)
		}
		if cleanup == nil {
			t.Fatal("cleanup should not be nil for 'gz' compression")
		}

		w.Write(testData)
		cleanup()

		// Verify it's valid gzip by decompressing
		reader, err := gzip.NewReader(&buf)
		if err != nil {
			t.Fatalf("gzip.NewReader() error = %v", err)
		}
		decompressed, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("reading gzip data: %v", err)
		}
		if !bytes.Equal(decompressed, testData) {
			t.Errorf("decompressed data differs from original")
		}
	})

	t.Run("zstd compression", func(t *testing.T) {
		var buf bytes.Buffer
		w, cleanup, err := newCompressWriter(&buf, "zstd", "test.txt")
		if err != nil {
			t.Fatalf("newCompressWriter() error = %v", err)
		}
		if cleanup == nil {
			t.Fatal("cleanup should not be nil for 'zstd' compression")
		}

		w.Write(testData)
		cleanup()

		// Verify it's valid zstd by decompressing
		reader, err := zstd.NewReader(&buf)
		if err != nil {
			t.Fatalf("zstd.NewReader() error = %v", err)
		}
		decompressed, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("reading zstd data: %v", err)
		}
		if !bytes.Equal(decompressed, testData) {
			t.Errorf("decompressed data differs from original")
		}
	})

	t.Run("unknown compression", func(t *testing.T) {
		var buf bytes.Buffer
		_, _, err := newCompressWriter(&buf, "lz4", "test.txt")
		if err == nil {
			t.Error("expected error for unknown compression, got nil")
		}
		if !strings.Contains(err.Error(), "unknown compression") {
			t.Errorf("error = %q, want error containing 'unknown compression'", err.Error())
		}
	})
}

func TestDumpFile(t *testing.T) {
	// Create a temp source file
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "source.db")
	srcContent := []byte("test database content for backup")
	if err := os.WriteFile(srcPath, srcContent, 0644); err != nil {
		t.Fatalf("writing source file: %v", err)
	}

	t.Run("no compression", func(t *testing.T) {
		outPath := filepath.Join(tmpDir, "backup_none.db")
		db := config.Database{
			Type:        "file",
			Path:        srcPath,
			Compression: "none",
		}

		err := dumpFile(db, outPath)
		if err != nil {
			t.Fatalf("dumpFile() error = %v", err)
		}

		// Verify content matches
		outContent, err := os.ReadFile(outPath)
		if err != nil {
			t.Fatalf("reading output: %v", err)
		}
		if !bytes.Equal(outContent, srcContent) {
			t.Errorf("output content differs from source")
		}
	})

	t.Run("gz compression", func(t *testing.T) {
		outPath := filepath.Join(tmpDir, "backup.db.gz")
		db := config.Database{
			Type:        "file",
			Path:        srcPath,
			Compression: "gz",
		}

		err := dumpFile(db, outPath)
		if err != nil {
			t.Fatalf("dumpFile() error = %v", err)
		}

		// Verify by decompressing
		outFile, err := os.Open(outPath)
		if err != nil {
			t.Fatalf("opening output: %v", err)
		}
		defer outFile.Close()

		reader, err := gzip.NewReader(outFile)
		if err != nil {
			t.Fatalf("gzip.NewReader() error = %v", err)
		}
		decompressed, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("reading gzip: %v", err)
		}
		if !bytes.Equal(decompressed, srcContent) {
			t.Errorf("decompressed content differs from source")
		}
	})

	t.Run("missing source file", func(t *testing.T) {
		outPath := filepath.Join(tmpDir, "backup_fail.db")
		db := config.Database{
			Type:        "file",
			Path:        "/nonexistent/file.db",
			Compression: "none",
		}

		err := dumpFile(db, outPath)
		if err == nil {
			t.Error("expected error for missing source file, got nil")
		}
	})
}

func TestRunAndCleanup(t *testing.T) {
	// Create a temp source file
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "source.db")
	srcContent := []byte("test database content")
	if err := os.WriteFile(srcPath, srcContent, 0644); err != nil {
		t.Fatalf("writing source file: %v", err)
	}

	db := config.Database{
		Type:        "file",
		Path:        srcPath,
		Dest:        "/backups", // not used in this test
		Compression: "gz",
	}

	result, err := Run("testdb", db)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	// Verify result fields
	if result.Name != "testdb" {
		t.Errorf("Name = %q, want %q", result.Name, "testdb")
	}
	if !strings.HasPrefix(result.Filename, "testdb_") {
		t.Errorf("Filename = %q, want prefix 'testdb_'", result.Filename)
	}
	if !strings.HasSuffix(result.Filename, ".db.gz") {
		t.Errorf("Filename = %q, want suffix '.db.gz'", result.Filename)
	}
	if result.Size <= 0 {
		t.Errorf("Size = %d, want > 0", result.Size)
	}
	if result.Duration <= 0 {
		t.Errorf("Duration = %v, want > 0", result.Duration)
	}

	// Verify file exists
	if _, err := os.Stat(result.Path); err != nil {
		t.Errorf("backup file not found: %v", err)
	}

	// Test cleanup
	backupDir := filepath.Dir(result.Path)
	Cleanup(result)

	// Verify temp dir was removed
	if _, err := os.Stat(backupDir); !os.IsNotExist(err) {
		t.Errorf("temp dir still exists after Cleanup")
	}
}

func TestCleanupNil(t *testing.T) {
	// Should not panic
	Cleanup(nil)

	// Should not panic with empty path
	Cleanup(&Result{Path: ""})
}

// Helper to create a gzip compressed file
func createGzipFile(t *testing.T, path string, content []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("creating file: %v", err)
	}
	defer f.Close()

	w := gzip.NewWriter(f)
	if _, err := w.Write(content); err != nil {
		t.Fatalf("writing gzip: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing gzip writer: %v", err)
	}
}

// Helper to create a zstd compressed file
func createZstdFile(t *testing.T, path string, content []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("creating file: %v", err)
	}
	defer f.Close()

	w, err := zstd.NewWriter(f)
	if err != nil {
		t.Fatalf("creating zstd writer: %v", err)
	}
	if _, err := w.Write(content); err != nil {
		t.Fatalf("writing zstd: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing zstd writer: %v", err)
	}
}

// Helper to create an xz compressed file
func createXzFile(t *testing.T, path string, content []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("creating file: %v", err)
	}
	defer f.Close()

	w, err := xz.NewWriter(f)
	if err != nil {
		t.Fatalf("creating xz writer: %v", err)
	}
	if _, err := w.Write(content); err != nil {
		t.Fatalf("writing xz: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing xz writer: %v", err)
	}
}

// Helper to create a zip file
func createZipFile(t *testing.T, path string, content []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("creating file: %v", err)
	}
	defer f.Close()

	w := zip.NewWriter(f)
	entry, err := w.Create("data.sql")
	if err != nil {
		t.Fatalf("creating zip entry: %v", err)
	}
	if _, err := entry.Write(content); err != nil {
		t.Fatalf("writing zip entry: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing zip writer: %v", err)
	}
}

func TestNewDecompressReader(t *testing.T) {
	testData := []byte("test data for decompression testing")
	tmpDir := t.TempDir()

	t.Run("no compression", func(t *testing.T) {
		path := filepath.Join(tmpDir, "plain.sql")
		if err := os.WriteFile(path, testData, 0644); err != nil {
			t.Fatalf("writing file: %v", err)
		}

		reader, cleanup, err := newDecompressReader(path)
		if err != nil {
			t.Fatalf("newDecompressReader() error = %v", err)
		}
		if cleanup == nil {
			t.Error("cleanup should not be nil")
		}
		defer cleanup()

		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		if !bytes.Equal(data, testData) {
			t.Errorf("data mismatch: got %q, want %q", data, testData)
		}
	})

	t.Run("gz compression", func(t *testing.T) {
		path := filepath.Join(tmpDir, "data.sql.gz")
		createGzipFile(t, path, testData)

		reader, cleanup, err := newDecompressReader(path)
		if err != nil {
			t.Fatalf("newDecompressReader() error = %v", err)
		}
		if cleanup == nil {
			t.Error("cleanup should not be nil")
		}
		defer cleanup()

		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		if !bytes.Equal(data, testData) {
			t.Errorf("data mismatch: got %q, want %q", data, testData)
		}
	})

	t.Run("zstd compression", func(t *testing.T) {
		path := filepath.Join(tmpDir, "data.sql.zst")
		createZstdFile(t, path, testData)

		reader, cleanup, err := newDecompressReader(path)
		if err != nil {
			t.Fatalf("newDecompressReader() error = %v", err)
		}
		if cleanup == nil {
			t.Error("cleanup should not be nil")
		}
		defer cleanup()

		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		if !bytes.Equal(data, testData) {
			t.Errorf("data mismatch: got %q, want %q", data, testData)
		}
	})

	t.Run("xz compression", func(t *testing.T) {
		path := filepath.Join(tmpDir, "data.sql.xz")
		createXzFile(t, path, testData)

		reader, cleanup, err := newDecompressReader(path)
		if err != nil {
			t.Fatalf("newDecompressReader() error = %v", err)
		}
		if cleanup == nil {
			t.Error("cleanup should not be nil")
		}
		defer cleanup()

		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		if !bytes.Equal(data, testData) {
			t.Errorf("data mismatch: got %q, want %q", data, testData)
		}
	})

	t.Run("zip compression", func(t *testing.T) {
		path := filepath.Join(tmpDir, "data.sql.zip")
		createZipFile(t, path, testData)

		reader, cleanup, err := newDecompressReader(path)
		if err != nil {
			t.Fatalf("newDecompressReader() error = %v", err)
		}
		if cleanup == nil {
			t.Error("cleanup should not be nil")
		}
		defer cleanup()

		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		if !bytes.Equal(data, testData) {
			t.Errorf("data mismatch: got %q, want %q", data, testData)
		}
	})

	t.Run("empty zip file", func(t *testing.T) {
		path := filepath.Join(tmpDir, "empty.zip")
		f, err := os.Create(path)
		if err != nil {
			t.Fatalf("creating file: %v", err)
		}
		w := zip.NewWriter(f)
		w.Close()
		f.Close()

		_, _, err = newDecompressReader(path)
		if err == nil {
			t.Error("expected error for empty zip, got nil")
		}
		if !strings.Contains(err.Error(), "empty") {
			t.Errorf("error = %q, want error containing 'empty'", err.Error())
		}
	})

	t.Run("missing file", func(t *testing.T) {
		_, _, err := newDecompressReader("/nonexistent/file.sql")
		if err == nil {
			t.Error("expected error for missing file, got nil")
		}
	})

	t.Run("invalid gzip", func(t *testing.T) {
		path := filepath.Join(tmpDir, "invalid.gz")
		if err := os.WriteFile(path, []byte("not gzip data"), 0644); err != nil {
			t.Fatalf("writing file: %v", err)
		}

		_, _, err := newDecompressReader(path)
		if err == nil {
			t.Error("expected error for invalid gzip, got nil")
		}
	})
}

func TestRestoreFile(t *testing.T) {
	testData := []byte("restored database content")
	tmpDir := t.TempDir()

	t.Run("no compression", func(t *testing.T) {
		backupPath := filepath.Join(tmpDir, "backup_plain.db")
		if err := os.WriteFile(backupPath, testData, 0644); err != nil {
			t.Fatalf("writing backup: %v", err)
		}

		destPath := filepath.Join(tmpDir, "restored_plain.db")
		db := config.Database{
			Type: "file",
			Path: destPath,
		}

		err := restoreFile(db, backupPath)
		if err != nil {
			t.Fatalf("restoreFile() error = %v", err)
		}

		restored, err := os.ReadFile(destPath)
		if err != nil {
			t.Fatalf("reading restored file: %v", err)
		}
		if !bytes.Equal(restored, testData) {
			t.Errorf("restored data mismatch")
		}
	})

	t.Run("gz compression", func(t *testing.T) {
		backupPath := filepath.Join(tmpDir, "backup.db.gz")
		createGzipFile(t, backupPath, testData)

		destPath := filepath.Join(tmpDir, "restored_gz.db")
		db := config.Database{
			Type: "file",
			Path: destPath,
		}

		err := restoreFile(db, backupPath)
		if err != nil {
			t.Fatalf("restoreFile() error = %v", err)
		}

		restored, err := os.ReadFile(destPath)
		if err != nil {
			t.Fatalf("reading restored file: %v", err)
		}
		if !bytes.Equal(restored, testData) {
			t.Errorf("restored data mismatch")
		}
	})

	t.Run("zstd compression", func(t *testing.T) {
		backupPath := filepath.Join(tmpDir, "backup.db.zst")
		createZstdFile(t, backupPath, testData)

		destPath := filepath.Join(tmpDir, "restored_zst.db")
		db := config.Database{
			Type: "file",
			Path: destPath,
		}

		err := restoreFile(db, backupPath)
		if err != nil {
			t.Fatalf("restoreFile() error = %v", err)
		}

		restored, err := os.ReadFile(destPath)
		if err != nil {
			t.Fatalf("reading restored file: %v", err)
		}
		if !bytes.Equal(restored, testData) {
			t.Errorf("restored data mismatch")
		}
	})

	t.Run("missing backup file", func(t *testing.T) {
		db := config.Database{
			Type: "file",
			Path: filepath.Join(tmpDir, "wont_be_created.db"),
		}

		err := restoreFile(db, "/nonexistent/backup.db")
		if err == nil {
			t.Error("expected error for missing backup, got nil")
		}
	})

	t.Run("invalid destination path", func(t *testing.T) {
		backupPath := filepath.Join(tmpDir, "backup_for_invalid_dest.db")
		if err := os.WriteFile(backupPath, testData, 0644); err != nil {
			t.Fatalf("writing backup: %v", err)
		}

		db := config.Database{
			Type: "file",
			Path: "/nonexistent/dir/restored.db",
		}

		err := restoreFile(db, backupPath)
		if err == nil {
			t.Error("expected error for invalid destination, got nil")
		}
	})
}

func TestRestore(t *testing.T) {
	testData := []byte("test restore data")
	tmpDir := t.TempDir()

	t.Run("file type", func(t *testing.T) {
		backupPath := filepath.Join(tmpDir, "backup_restore.db")
		if err := os.WriteFile(backupPath, testData, 0644); err != nil {
			t.Fatalf("writing backup: %v", err)
		}

		destPath := filepath.Join(tmpDir, "restored_file.db")
		db := config.Database{
			Type: "file",
			Path: destPath,
		}

		err := Restore(db, backupPath)
		if err != nil {
			t.Fatalf("Restore() error = %v", err)
		}

		restored, err := os.ReadFile(destPath)
		if err != nil {
			t.Fatalf("reading restored file: %v", err)
		}
		if !bytes.Equal(restored, testData) {
			t.Errorf("restored data mismatch")
		}
	})

	t.Run("unknown type", func(t *testing.T) {
		db := config.Database{
			Type: "mongodb",
		}

		err := Restore(db, "/some/backup.db")
		if err == nil {
			t.Error("expected error for unknown type, got nil")
		}
		if !strings.Contains(err.Error(), "unknown database type") {
			t.Errorf("error = %q, want error containing 'unknown database type'", err.Error())
		}
	})
}
