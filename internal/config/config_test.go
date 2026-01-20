package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExpandEnvVars(t *testing.T) {
	// Set test environment variables
	os.Setenv("TEST_VAR", "test_value")
	os.Setenv("TEST_HOST", "localhost")
	os.Setenv("TEST_PORT", "3306")
	defer func() {
		os.Unsetenv("TEST_VAR")
		os.Unsetenv("TEST_HOST")
		os.Unsetenv("TEST_PORT")
	}()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "single variable",
			input:    "host: ${TEST_HOST}",
			expected: "host: localhost",
		},
		{
			name:     "multiple variables",
			input:    "host: ${TEST_HOST}\nport: ${TEST_PORT}",
			expected: "host: localhost\nport: 3306",
		},
		{
			name:     "no variables",
			input:    "host: localhost",
			expected: "host: localhost",
		},
		{
			name:     "undefined variable stays unchanged",
			input:    "host: ${UNDEFINED_VAR}",
			expected: "host: ${UNDEFINED_VAR}",
		},
		{
			name:     "mixed defined and undefined",
			input:    "${TEST_VAR} and ${UNDEFINED}",
			expected: "test_value and ${UNDEFINED}",
		},
		{
			name:     "variable in middle of string",
			input:    "prefix_${TEST_VAR}_suffix",
			expected: "prefix_test_value_suffix",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := expandEnvVars(tt.input)
			if result != tt.expected {
				t.Errorf("expandEnvVars(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name:    "empty databases",
			cfg:     Config{Databases: map[string]Database{}},
			wantErr: "no databases configured",
		},
		{
			name: "invalid name with spaces",
			cfg: Config{Databases: map[string]Database{
				"my db": {Type: "file", Path: "/test", Dest: "/backup"},
			}},
			wantErr: "name must contain only letters",
		},
		{
			name: "invalid name with special chars",
			cfg: Config{Databases: map[string]Database{
				"my.db": {Type: "file", Path: "/test", Dest: "/backup"},
			}},
			wantErr: "name must contain only letters",
		},
		{
			name: "valid name with underscore",
			cfg: Config{Databases: map[string]Database{
				"my_db": {Type: "file", Path: "/test", Dest: "/backup", Compression: "none"},
			}},
			wantErr: "",
		},
		{
			name: "valid name with dash",
			cfg: Config{Databases: map[string]Database{
				"my-db": {Type: "file", Path: "/test", Dest: "/backup", Compression: "none"},
			}},
			wantErr: "",
		},
		{
			name: "file type missing path",
			cfg: Config{Databases: map[string]Database{
				"mydb": {Type: "file", Dest: "/backup", Compression: "none"},
			}},
			wantErr: "path is required for file type",
		},
		{
			name: "mysql missing host",
			cfg: Config{Databases: map[string]Database{
				"mydb": {Type: "mysql", User: "root", Database: "test", Dest: "/backup", Compression: "none"},
			}},
			wantErr: "host is required",
		},
		{
			name: "mysql missing user",
			cfg: Config{Databases: map[string]Database{
				"mydb": {Type: "mysql", Host: "localhost", Database: "test", Dest: "/backup", Compression: "none"},
			}},
			wantErr: "user is required",
		},
		{
			name: "mysql missing database",
			cfg: Config{Databases: map[string]Database{
				"mydb": {Type: "mysql", Host: "localhost", User: "root", Dest: "/backup", Compression: "none"},
			}},
			wantErr: "database name is required",
		},
		{
			name: "postgres valid",
			cfg: Config{Databases: map[string]Database{
				"mydb": {Type: "postgres", Host: "localhost", User: "postgres", Database: "test", Dest: "/backup", Compression: "none"},
			}},
			wantErr: "",
		},
		{
			name: "unknown type",
			cfg: Config{Databases: map[string]Database{
				"mydb": {Type: "oracle", Dest: "/backup", Compression: "none"},
			}},
			wantErr: "unknown type",
		},
		{
			name: "missing dest",
			cfg: Config{Databases: map[string]Database{
				"mydb": {Type: "file", Path: "/test", Compression: "none"},
			}},
			wantErr: "dest is required",
		},
		{
			name: "invalid compression",
			cfg: Config{Databases: map[string]Database{
				"mydb": {Type: "file", Path: "/test", Dest: "/backup", Compression: "lz4"},
			}},
			wantErr: "compression must be one of",
		},
		{
			name: "valid compression gz",
			cfg: Config{Databases: map[string]Database{
				"mydb": {Type: "file", Path: "/test", Dest: "/backup", Compression: "gz"},
			}},
			wantErr: "",
		},
		{
			name: "valid compression zstd",
			cfg: Config{Databases: map[string]Database{
				"mydb": {Type: "file", Path: "/test", Dest: "/backup", Compression: "zstd"},
			}},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("Validate() error = %v, want nil", err)
				}
			} else {
				if err == nil {
					t.Errorf("Validate() error = nil, want error containing %q", tt.wantErr)
				} else if !contains(err.Error(), tt.wantErr) {
					t.Errorf("Validate() error = %q, want error containing %q", err.Error(), tt.wantErr)
				}
			}
		})
	}
}

func TestApplyDefaults(t *testing.T) {
	tests := []struct {
		name     string
		input    Database
		wantComp string
		wantPort int
	}{
		{
			name:     "empty compression defaults to none",
			input:    Database{Type: "file"},
			wantComp: "none",
			wantPort: 0,
		},
		{
			name:     "mysql default port",
			input:    Database{Type: "mysql"},
			wantComp: "none",
			wantPort: 3306,
		},
		{
			name:     "postgres default port",
			input:    Database{Type: "postgres"},
			wantComp: "none",
			wantPort: 5432,
		},
		{
			name:     "custom port not overwritten",
			input:    Database{Type: "mysql", Port: 3307},
			wantComp: "none",
			wantPort: 3307,
		},
		{
			name:     "custom compression not overwritten",
			input:    Database{Type: "file", Compression: "gz"},
			wantComp: "gz",
			wantPort: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{Databases: map[string]Database{"test": tt.input}}
			cfg.applyDefaults()
			db := cfg.Databases["test"]
			if db.Compression != tt.wantComp {
				t.Errorf("Compression = %q, want %q", db.Compression, tt.wantComp)
			}
			if db.Port != tt.wantPort {
				t.Errorf("Port = %d, want %d", db.Port, tt.wantPort)
			}
		})
	}
}

func TestLoadOrEmpty(t *testing.T) {
	t.Run("nonexistent file returns empty config", func(t *testing.T) {
		cfg, err := LoadOrEmpty("/nonexistent/path/blobber.yaml")
		if err != nil {
			t.Fatalf("LoadOrEmpty() error = %v, want nil", err)
		}
		if cfg == nil {
			t.Fatal("LoadOrEmpty() returned nil config")
		}
		if len(cfg.Databases) != 0 {
			t.Errorf("Databases = %v, want empty map", cfg.Databases)
		}
	})

	t.Run("valid file loads correctly", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfgPath := filepath.Join(tmpDir, "blobber.yaml")
		content := `databases:
  mydb:
    type: file
    path: /data/test.db
    dest: /backups
    compression: gz
`
		if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
			t.Fatalf("writing test config: %v", err)
		}

		cfg, err := LoadOrEmpty(cfgPath)
		if err != nil {
			t.Fatalf("LoadOrEmpty() error = %v", err)
		}
		if len(cfg.Databases) != 1 {
			t.Errorf("len(Databases) = %d, want 1", len(cfg.Databases))
		}
		db, ok := cfg.Databases["mydb"]
		if !ok {
			t.Fatal("mydb not found in Databases")
		}
		if db.Type != "file" {
			t.Errorf("Type = %q, want %q", db.Type, "file")
		}
		if db.Path != "/data/test.db" {
			t.Errorf("Path = %q, want %q", db.Path, "/data/test.db")
		}
	})

	t.Run("env vars expanded", func(t *testing.T) {
		os.Setenv("TEST_DB_PATH", "/custom/path.db")
		defer os.Unsetenv("TEST_DB_PATH")

		tmpDir := t.TempDir()
		cfgPath := filepath.Join(tmpDir, "blobber.yaml")
		content := `databases:
  mydb:
    type: file
    path: ${TEST_DB_PATH}
    dest: /backups
    compression: none
`
		if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
			t.Fatalf("writing test config: %v", err)
		}

		cfg, err := LoadOrEmpty(cfgPath)
		if err != nil {
			t.Fatalf("LoadOrEmpty() error = %v", err)
		}
		db := cfg.Databases["mydb"]
		if db.Path != "/custom/path.db" {
			t.Errorf("Path = %q, want %q", db.Path, "/custom/path.db")
		}
	})
}

func TestSave(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "blobber.yaml")

	cfg := &Config{
		path: cfgPath,
		Databases: map[string]Database{
			"testdb": {
				Type:        "file",
				Path:        "/data/test.db",
				Dest:        "/backups",
				Compression: "gz",
			},
		},
	}

	if err := cfg.Save(); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// Verify file was created
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("reading saved file: %v", err)
	}

	content := string(data)
	if !contains(content, "testdb") {
		t.Errorf("saved content missing 'testdb': %s", content)
	}
	if !contains(content, "file") {
		t.Errorf("saved content missing 'file': %s", content)
	}

	// Verify it can be loaded back
	loaded, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.Databases["testdb"].Path != "/data/test.db" {
		t.Errorf("loaded Path = %q, want %q", loaded.Databases["testdb"].Path, "/data/test.db")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
