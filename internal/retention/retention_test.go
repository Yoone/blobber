package retention

import (
	"context"
	"testing"
	"time"

	"github.com/Yoone/blobber/internal/config"
	"github.com/Yoone/blobber/internal/storage"
)

func TestParseFilename(t *testing.T) {
	tests := []struct {
		name          string
		filename      string
		wantName      string
		wantTimestamp string
		wantOk        bool
	}{
		{
			name:          "valid sql.gz",
			filename:      "mydb_20240115_143022.sql.gz",
			wantName:      "mydb",
			wantTimestamp: "20240115_143022",
			wantOk:        true,
		},
		{
			name:          "valid with path",
			filename:      "backups/mydb_20240115_143022.sql.gz",
			wantName:      "mydb",
			wantTimestamp: "20240115_143022",
			wantOk:        true,
		},
		{
			name:          "valid db extension",
			filename:      "sqlite_db_20240115_143022.db.zst",
			wantName:      "sqlite_db",
			wantTimestamp: "20240115_143022",
			wantOk:        true,
		},
		{
			name:          "underscores in name",
			filename:      "my_database_name_20240115_143022.sql",
			wantName:      "my_database_name",
			wantTimestamp: "20240115_143022",
			wantOk:        true,
		},
		{
			name:     "no extension",
			filename: "mydb_20240115_143022",
			wantOk:   false,
		},
		{
			name:     "invalid timestamp format",
			filename: "mydb_2024-01-15_14:30:22.sql.gz",
			wantOk:   false,
		},
		{
			name:     "no timestamp",
			filename: "mydb.sql.gz",
			wantOk:   false,
		},
		{
			name:     "random file",
			filename: "readme.txt",
			wantOk:   false,
		},
		{
			name:     "empty string",
			filename: "",
			wantOk:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, ts, ok := parseFilename(tt.filename)
			if ok != tt.wantOk {
				t.Errorf("parseFilename(%q) ok = %v, want %v", tt.filename, ok, tt.wantOk)
				return
			}
			if !tt.wantOk {
				return
			}
			if name != tt.wantName {
				t.Errorf("parseFilename(%q) name = %q, want %q", tt.filename, name, tt.wantName)
			}
			wantTs, _ := time.Parse("20060102_150405", tt.wantTimestamp)
			if !ts.Equal(wantTs) {
				t.Errorf("parseFilename(%q) timestamp = %v, want %v", tt.filename, ts, wantTs)
			}
		})
	}
}

func TestFilterByName(t *testing.T) {
	files := []storage.RemoteFile{
		{Name: "db1_20240115_100000.sql.gz", Size: 100},
		{Name: "db1_20240115_110000.sql.gz", Size: 100},
		{Name: "db2_20240115_120000.sql.gz", Size: 100},
		{Name: "db1_20240115_090000.sql.gz", Size: 100},
		{Name: "random_file.txt", Size: 50},
		{Name: "db1_invalid.sql.gz", Size: 100},
	}

	t.Run("filters by name", func(t *testing.T) {
		result := filterByName(files, "db1")
		if len(result) != 3 {
			t.Fatalf("expected 3 files for db1, got %d", len(result))
		}
	})

	t.Run("case insensitive", func(t *testing.T) {
		result := filterByName(files, "DB1")
		if len(result) != 3 {
			t.Fatalf("expected 3 files for DB1 (case insensitive), got %d", len(result))
		}
	})

	t.Run("sorted newest first", func(t *testing.T) {
		result := filterByName(files, "db1")
		if len(result) != 3 {
			t.Fatalf("expected 3 files, got %d", len(result))
		}
		// Should be sorted: 110000, 100000, 090000
		expected := []string{
			"db1_20240115_110000.sql.gz",
			"db1_20240115_100000.sql.gz",
			"db1_20240115_090000.sql.gz",
		}
		for i, f := range result {
			if f.Name != expected[i] {
				t.Errorf("index %d: got %q, want %q", i, f.Name, expected[i])
			}
		}
	})

	t.Run("no matches", func(t *testing.T) {
		result := filterByName(files, "nonexistent")
		if len(result) != 0 {
			t.Errorf("expected 0 files for nonexistent, got %d", len(result))
		}
	})
}

func TestApplyKeepLast(t *testing.T) {
	ctx := context.Background()

	files := []storage.RemoteFile{
		{Name: "mydb_20240115_150000.sql.gz", Size: 100},
		{Name: "mydb_20240115_140000.sql.gz", Size: 100},
		{Name: "mydb_20240115_130000.sql.gz", Size: 100},
		{Name: "mydb_20240115_120000.sql.gz", Size: 100},
		{Name: "mydb_20240115_110000.sql.gz", Size: 100},
	}

	t.Run("keep 3 deletes 2", func(t *testing.T) {
		ret := config.Retention{KeepLast: 3}
		toDelete := Apply(ctx, files, "mydb", ret)
		if len(toDelete) != 2 {
			t.Fatalf("expected 2 to delete, got %d", len(toDelete))
		}
		// Should delete the oldest two
		if toDelete[0].Name != "mydb_20240115_120000.sql.gz" {
			t.Errorf("expected oldest to be deleted first, got %s", toDelete[0].Name)
		}
	})

	t.Run("keep more than exists", func(t *testing.T) {
		ret := config.Retention{KeepLast: 10}
		toDelete := Apply(ctx, files, "mydb", ret)
		if len(toDelete) != 0 {
			t.Errorf("expected 0 to delete, got %d", len(toDelete))
		}
	})

	t.Run("ignores other databases", func(t *testing.T) {
		mixedFiles := []storage.RemoteFile{
			{Name: "mydb_20240115_150000.sql.gz", Size: 100},
			{Name: "other_20240115_140000.sql.gz", Size: 100},
			{Name: "mydb_20240115_130000.sql.gz", Size: 100},
		}
		ret := config.Retention{KeepLast: 1}
		toDelete := Apply(ctx, mixedFiles, "mydb", ret)
		if len(toDelete) != 1 {
			t.Fatalf("expected 1 to delete, got %d", len(toDelete))
		}
		if toDelete[0].Name != "mydb_20240115_130000.sql.gz" {
			t.Errorf("expected mydb_20240115_130000.sql.gz to be deleted, got %s", toDelete[0].Name)
		}
	})
}

func TestApplyKeepDays(t *testing.T) {
	ctx := context.Background()

	// Create files with timestamps relative to now
	now := time.Now()
	files := []storage.RemoteFile{
		{Name: "mydb_" + now.Format("20060102_150405") + ".sql.gz", Size: 100},
		{Name: "mydb_" + now.AddDate(0, 0, -3).Format("20060102_150405") + ".sql.gz", Size: 100},
		{Name: "mydb_" + now.AddDate(0, 0, -7).Format("20060102_150405") + ".sql.gz", Size: 100},
		{Name: "mydb_" + now.AddDate(0, 0, -10).Format("20060102_150405") + ".sql.gz", Size: 100},
	}

	t.Run("keep 5 days deletes old", func(t *testing.T) {
		ret := config.Retention{KeepDays: 5}
		toDelete := Apply(ctx, files, "mydb", ret)
		if len(toDelete) != 2 {
			t.Fatalf("expected 2 to delete (7 and 10 days old), got %d", len(toDelete))
		}
	})
}

func TestApplyMaxSize(t *testing.T) {
	ctx := context.Background()

	files := []storage.RemoteFile{
		{Name: "mydb_20240115_150000.sql.gz", Size: 5 * 1024 * 1024}, // 5MB
		{Name: "mydb_20240115_140000.sql.gz", Size: 5 * 1024 * 1024}, // 5MB
		{Name: "mydb_20240115_130000.sql.gz", Size: 5 * 1024 * 1024}, // 5MB
		{Name: "mydb_20240115_120000.sql.gz", Size: 5 * 1024 * 1024}, // 5MB
	}

	t.Run("max 12MB keeps 2", func(t *testing.T) {
		ret := config.Retention{MaxSizeMB: 12}
		toDelete := Apply(ctx, files, "mydb", ret)
		// Total: 20MB, max: 12MB
		// Keep first 2 (10MB), delete 2 (10MB)
		if len(toDelete) != 2 {
			t.Fatalf("expected 2 to delete, got %d", len(toDelete))
		}
	})
}

func TestApplyCombinedRules(t *testing.T) {
	ctx := context.Background()

	// Create files with timestamps relative to now
	now := time.Now()
	files := []storage.RemoteFile{
		{Name: "mydb_" + now.Format("20060102_150405") + ".sql.gz", Size: 1024 * 1024},                         // today, 1MB
		{Name: "mydb_" + now.AddDate(0, 0, -1).Format("20060102_150405") + ".sql.gz", Size: 1024 * 1024},       // 1 day ago, 1MB
		{Name: "mydb_" + now.AddDate(0, 0, -2).Format("20060102_150405") + ".sql.gz", Size: 1024 * 1024},       // 2 days ago, 1MB
		{Name: "mydb_" + now.AddDate(0, 0, -5).Format("20060102_150405") + ".sql.gz", Size: 1024 * 1024},       // 5 days ago, 1MB
		{Name: "mydb_" + now.AddDate(0, 0, -10).Format("20060102_150405") + ".sql.gz", Size: 10 * 1024 * 1024}, // 10 days ago, 10MB
	}

	t.Run("combined keep_last and keep_days", func(t *testing.T) {
		// keep_last: 3 would delete files 4 and 5 (5 days and 10 days old)
		// keep_days: 7 would delete file 5 (10 days old)
		// Combined: should delete files 4 and 5
		ret := config.Retention{KeepLast: 3, KeepDays: 7}
		toDelete := Apply(ctx, files, "mydb", ret)
		if len(toDelete) != 2 {
			t.Fatalf("expected 2 to delete, got %d", len(toDelete))
		}
	})

	t.Run("combined all three rules", func(t *testing.T) {
		// keep_last: 4 would delete file 5 (10 days old)
		// keep_days: 3 would delete files 4 and 5 (5 and 10 days old)
		// max_size_mb: 5 would delete file 5 (cumulative 14MB > 5MB)
		// Combined: should delete files 4 and 5 (union of all rules)
		ret := config.Retention{KeepLast: 4, KeepDays: 3, MaxSizeMB: 5}
		toDelete := Apply(ctx, files, "mydb", ret)
		if len(toDelete) != 2 {
			t.Fatalf("expected 2 to delete, got %d", len(toDelete))
		}
	})

	t.Run("no rules configured", func(t *testing.T) {
		ret := config.Retention{}
		toDelete := Apply(ctx, files, "mydb", ret)
		if len(toDelete) != 0 {
			t.Fatalf("expected 0 to delete when no rules, got %d", len(toDelete))
		}
	})
}
