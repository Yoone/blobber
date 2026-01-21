package config

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	path      string              `yaml:"-"` // not serialized
	Databases map[string]Database `yaml:"databases"`
}

type Database struct {
	Type        string    `yaml:"type"`                  // file, mysql, postgres
	Path        string    `yaml:"path,omitempty"`        // for file type
	Host        string    `yaml:"host,omitempty"`        // for mysql/postgres
	Port        int       `yaml:"port,omitempty"`        // for mysql/postgres
	User        string    `yaml:"user,omitempty"`        // for mysql/postgres
	Password    string    `yaml:"password,omitempty"`    // for mysql/postgres
	Database    string    `yaml:"database,omitempty"`    // database name for mysql/postgres
	Dest        string    `yaml:"dest"`                  // rclone destination
	Compression string    `yaml:"compression,omitempty"` // none, gz, zstd, xz, zip
	Retention   Retention `yaml:"retention,omitempty"`
}

type Retention struct {
	KeepLast  int `yaml:"keep_last,omitempty"`
	KeepDays  int `yaml:"keep_days,omitempty"`
	MaxSizeMB int `yaml:"max_size_mb,omitempty"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	// Expand environment variables
	expanded := expandEnvVars(string(data))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	cfg.path = path
	cfg.applyDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// LoadOrEmpty loads config from path, or returns empty config if file doesn't exist
func LoadOrEmpty(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &Config{
			path:      path,
			Databases: make(map[string]Database),
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	// Expand environment variables
	expanded := expandEnvVars(string(data))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	cfg.path = path
	if cfg.Databases == nil {
		cfg.Databases = make(map[string]Database)
	}
	cfg.applyDefaults()

	// Validate if there are databases configured
	if len(cfg.Databases) > 0 {
		if err := cfg.Validate(); err != nil {
			return nil, err
		}
	}

	return &cfg, nil
}

func (c *Config) applyDefaults() {
	for name, db := range c.Databases {
		if db.Compression == "" {
			db.Compression = "none"
		}
		if db.Port == 0 {
			switch db.Type {
			case "mysql":
				db.Port = 3306
			case "postgres":
				db.Port = 5432
			}
		}
		c.Databases[name] = db
	}
}

// Save writes the config to its file path
func (c *Config) Save() error {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(c); err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	if err := os.WriteFile(c.path, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("writing config file: %w", err)
	}

	return nil
}

// Path returns the config file path
func (c *Config) Path() string {
	return c.path
}

// validNamePattern matches only letters, digits, dashes, and underscores
var validNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

func (c *Config) Validate() error {
	if len(c.Databases) == 0 {
		return fmt.Errorf("no databases configured")
	}

	for name, db := range c.Databases {
		// Validate database name (must be filename-safe)
		if !validNamePattern.MatchString(name) {
			return fmt.Errorf("database %q: name must contain only letters, digits, dashes, and underscores", name)
		}

		switch db.Type {
		case "file":
			if db.Path == "" {
				return fmt.Errorf("database %q: path is required for file type", name)
			}
		case "mysql", "postgres":
			if db.Host == "" {
				return fmt.Errorf("database %q: host is required", name)
			}
			if db.User == "" {
				return fmt.Errorf("database %q: user is required", name)
			}
			if db.Database == "" {
				return fmt.Errorf("database %q: database name is required", name)
			}
		default:
			return fmt.Errorf("database %q: unknown type %q", name, db.Type)
		}

		if db.Dest == "" {
			return fmt.Errorf("database %q: dest is required", name)
		}

		validCompressions := map[string]bool{
			"none": true, "gz": true, "zstd": true, "xz": true, "zip": true,
		}
		if !validCompressions[db.Compression] {
			return fmt.Errorf("database %q: compression must be one of: none, gz, zstd, xz, zip", name)
		}
	}

	return nil
}

// expandEnvVars replaces ${VAR} patterns with environment variable values
func expandEnvVars(s string) string {
	re := regexp.MustCompile(`\$\{([^}]+)\}`)
	return re.ReplaceAllStringFunc(s, func(match string) string {
		varName := strings.TrimSuffix(strings.TrimPrefix(match, "${"), "}")
		if val := os.Getenv(varName); val != "" {
			return val
		}
		return match
	})
}
