package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var (
	testDir    string
	backupDir  string
	blobberBin string
	configFile string
)

func TestMain(m *testing.M) {
	// Setup
	var err error
	testDir, err = os.MkdirTemp("", "blobber-integration-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create temp dir: %v\n", err)
		os.Exit(1)
	}

	backupDir = filepath.Join(testDir, "backups")
	os.MkdirAll(backupDir, 0755)
	os.MkdirAll(filepath.Join(backupDir, "mysql"), 0755)
	os.MkdirAll(filepath.Join(backupDir, "mariadb"), 0755)
	os.MkdirAll(filepath.Join(backupDir, "postgres"), 0755)

	// Set env var for config
	os.Setenv("TEST_BACKUP_DIR", backupDir)

	// Find blobber binary (one level up from integration/)
	blobberBin = filepath.Join("..", "blobber")
	if _, err := os.Stat(blobberBin); err != nil {
		fmt.Fprintf(os.Stderr, "Blobber binary not found at %s. Run 'go build' first.\n", blobberBin)
		os.Exit(1)
	}

	configFile = "blobber-test.yaml"

	// Run tests
	code := m.Run()

	// Cleanup
	os.RemoveAll(testDir)

	os.Exit(code)
}

func runBlobber(args ...string) (string, error) {
	allArgs := append([]string{"-c", configFile}, args...)
	cmd := exec.Command(blobberBin, allArgs...)
	cmd.Env = append(os.Environ(), "TEST_BACKUP_DIR="+backupDir)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func TestMySQLBackupRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Connect to MySQL
	db, err := sql.Open("mysql", "testuser:testpass@tcp(localhost:3306)/testdb")
	if err != nil {
		t.Fatalf("Failed to connect to MySQL: %v", err)
	}
	defer db.Close()

	// Wait for connection
	for i := 0; i < 30; i++ {
		if err := db.PingContext(ctx); err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	// Verify initial data
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM customers").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query customers: %v", err)
	}
	if count != 5 {
		t.Fatalf("Expected 5 customers, got %d", count)
	}

	// Run backup
	output, err := runBlobber("backup")
	if err != nil {
		t.Fatalf("Backup failed: %v\nOutput: %s", err, output)
	}
	t.Logf("Backup output:\n%s", output)

	// List backups
	output, err = runBlobber("list", "mysql-test")
	if err != nil {
		t.Fatalf("List failed: %v\nOutput: %s", err, output)
	}
	t.Logf("List output:\n%s", output)

	// Find the backup file
	files, err := os.ReadDir(filepath.Join(backupDir, "mysql"))
	if err != nil {
		t.Fatalf("Failed to read backup dir: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("No backup files found")
	}
	backupFile := files[0].Name()

	// Modify the database
	_, err = db.ExecContext(ctx, "DELETE FROM orders")
	if err != nil {
		t.Fatalf("Failed to delete orders: %v", err)
	}
	_, err = db.ExecContext(ctx, "DELETE FROM customers WHERE id > 2")
	if err != nil {
		t.Fatalf("Failed to delete customers: %v", err)
	}

	// Verify modification
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM customers").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query customers after delete: %v", err)
	}
	if count != 2 {
		t.Fatalf("Expected 2 customers after delete, got %d", count)
	}

	// Restore from backup
	backupPath := filepath.Join(backupDir, "mysql", backupFile)
	output, err = runBlobber("restore", "--local", "mysql-test", backupPath)
	if err != nil {
		t.Fatalf("Restore failed: %v\nOutput: %s", err, output)
	}
	t.Logf("Restore output:\n%s", output)

	// Verify restore
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM customers").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query customers after restore: %v", err)
	}
	if count != 5 {
		t.Fatalf("Expected 5 customers after restore, got %d", count)
	}
}

func TestMariaDBBackupRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Connect to MariaDB
	db, err := sql.Open("mysql", "testuser:testpass@tcp(localhost:3307)/testdb")
	if err != nil {
		t.Fatalf("Failed to connect to MariaDB: %v", err)
	}
	defer db.Close()

	// Wait for connection
	for i := 0; i < 30; i++ {
		if err := db.PingContext(ctx); err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	// Verify initial data
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM customers").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query customers: %v", err)
	}
	if count != 5 {
		t.Fatalf("Expected 5 customers, got %d", count)
	}

	// Run backup (already ran in MySQL test, but let's be explicit)
	output, err := runBlobber("backup")
	if err != nil {
		t.Fatalf("Backup failed: %v\nOutput: %s", err, output)
	}

	// Find the backup file
	files, err := os.ReadDir(filepath.Join(backupDir, "mariadb"))
	if err != nil {
		t.Fatalf("Failed to read backup dir: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("No backup files found")
	}
	backupFile := files[0].Name()

	// Modify the database
	_, err = db.ExecContext(ctx, "DELETE FROM orders")
	if err != nil {
		t.Fatalf("Failed to delete orders: %v", err)
	}
	_, err = db.ExecContext(ctx, "DELETE FROM customers WHERE id > 3")
	if err != nil {
		t.Fatalf("Failed to delete customers: %v", err)
	}

	// Verify modification
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM customers").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query customers after delete: %v", err)
	}
	if count != 3 {
		t.Fatalf("Expected 3 customers after delete, got %d", count)
	}

	// Restore from backup
	backupPath := filepath.Join(backupDir, "mariadb", backupFile)
	output, err = runBlobber("restore", "--local", "mariadb-test", backupPath)
	if err != nil {
		t.Fatalf("Restore failed: %v\nOutput: %s", err, output)
	}

	// Verify restore
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM customers").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query customers after restore: %v", err)
	}
	if count != 5 {
		t.Fatalf("Expected 5 customers after restore, got %d", count)
	}
}

func TestPostgresBackupRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Connect to PostgreSQL
	db, err := sql.Open("postgres", "host=localhost port=5432 user=testuser password=testpass dbname=testdb sslmode=disable")
	if err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Wait for connection
	for i := 0; i < 30; i++ {
		if err := db.PingContext(ctx); err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	// Verify initial data
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM customers").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query customers: %v", err)
	}
	if count != 5 {
		t.Fatalf("Expected 5 customers, got %d", count)
	}

	// Run backup
	output, err := runBlobber("backup")
	if err != nil {
		t.Fatalf("Backup failed: %v\nOutput: %s", err, output)
	}

	// Find the backup file
	files, err := os.ReadDir(filepath.Join(backupDir, "postgres"))
	if err != nil {
		t.Fatalf("Failed to read backup dir: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("No backup files found")
	}
	backupFile := files[0].Name()

	// Modify the database
	_, err = db.ExecContext(ctx, "DELETE FROM orders")
	if err != nil {
		t.Fatalf("Failed to delete orders: %v", err)
	}
	_, err = db.ExecContext(ctx, "DELETE FROM customers WHERE id > 1")
	if err != nil {
		t.Fatalf("Failed to delete customers: %v", err)
	}

	// Verify modification
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM customers").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query customers after delete: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected 1 customer after delete, got %d", count)
	}

	// Restore from backup
	backupPath := filepath.Join(backupDir, "postgres", backupFile)
	output, err = runBlobber("restore", "--local", "postgres-test", backupPath)
	if err != nil {
		t.Fatalf("Restore failed: %v\nOutput: %s", err, output)
	}

	// Verify restore
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM customers").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query customers after restore: %v", err)
	}
	if count != 5 {
		t.Fatalf("Expected 5 customers after restore, got %d", count)
	}
}

func TestDryRun(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Clear backup directory
	for _, subdir := range []string{"mysql", "mariadb", "postgres"} {
		dir := filepath.Join(backupDir, subdir)
		files, _ := os.ReadDir(dir)
		for _, f := range files {
			os.Remove(filepath.Join(dir, f.Name()))
		}
	}

	// Run dry-run backup
	output, err := runBlobber("backup", "--dry-run")
	if err != nil {
		t.Fatalf("Dry-run backup failed: %v\nOutput: %s", err, output)
	}

	if !strings.Contains(output, "dry-run") {
		t.Error("Expected 'dry-run' in output")
	}

	// Verify no files were uploaded
	for _, subdir := range []string{"mysql", "mariadb", "postgres"} {
		files, err := os.ReadDir(filepath.Join(backupDir, subdir))
		if err != nil {
			t.Fatalf("Failed to read backup dir: %v", err)
		}
		if len(files) > 0 {
			t.Errorf("Expected no files in %s after dry-run, found %d", subdir, len(files))
		}
	}
}

func TestRetention(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create multiple backups
	for i := 0; i < 5; i++ {
		output, err := runBlobber("backup")
		if err != nil {
			t.Fatalf("Backup %d failed: %v\nOutput: %s", i+1, err, output)
		}
		time.Sleep(time.Second) // Ensure different timestamps
	}

	// Check that retention policy was applied (keep_last: 3)
	for _, subdir := range []string{"mysql", "mariadb", "postgres"} {
		files, err := os.ReadDir(filepath.Join(backupDir, subdir))
		if err != nil {
			t.Fatalf("Failed to read backup dir: %v", err)
		}
		if len(files) > 3 {
			t.Errorf("Expected at most 3 files in %s (retention policy), found %d", subdir, len(files))
		}
	}
}
