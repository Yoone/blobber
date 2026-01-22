# Blobber

![Blobber Demo](.github/assets/blobber-demo.gif)

CLI for automated backups. TUI for guided restores. Restores matter as much as backups, and during an incident you don't want to be digging through docs for CLI flags. Supports MySQL, MariaDB, PostgreSQL, and SQLite to local or cloud storage (via rclone).

## Features

- **Interactive TUI** - Manage databases, run backups, and restore with a terminal interface
- **CLI mode** - Script backups in cron jobs or CI/CD pipelines
- **Multiple storage backends** - Local paths or 70+ cloud providers via rclone (S3, GCS, Azure, B2, etc.)
- **Compression** - gzip, zstd, xz, zip, or none
- **Retention policies** - Automatically clean up old backups by count, age, or size
- **Parallel execution** - Back up multiple databases concurrently

## Database Support

| Database   | Backup Tool   | Restore Tool | Notes |
|------------|---------------|--------------|-------|
| MySQL      | `mysqldump`   | `mysql`      | Also works with MariaDB |
| MariaDB    | `mysqldump`   | `mysql`      | Uses MySQL tools |
| PostgreSQL | `pg_dump`     | `psql`       | |
| SQLite     | file copy     | file copy    | Any file-based database |

Ensure the required tools are installed and available in your `PATH`.

## Installation

### Homebrew (macOS/Linux)

```bash
brew install Yoone/tap/blobber
```

### Install Script (Linux/macOS)

```bash
curl -sSL https://raw.githubusercontent.com/Yoone/blobber/main/install.sh | sh
```

Or specify a version and install directory:

```bash
VERSION=v0.1.0 INSTALL_DIR=/opt/bin curl -sSL https://raw.githubusercontent.com/Yoone/blobber/main/install.sh | sh
```

### Go Install

```bash
go install github.com/Yoone/blobber@latest
```

### From Source

```bash
git clone git@github.com:Yoone/blobber.git
cd blobber
make
```

### Prerequisites

- [Go 1.24+](https://go.dev/dl/) to build from source
- Database tools for your DBMS (see table above)
- rclone CLI (optional - only needed if configuring remotes outside the TUI)

### Verify Installation

```bash
blobber version
```

## Quick Start

1. Create a config file `~/.config/blobber/config.yaml`:

```yaml
databases:
  my-postgres:
    type: postgres
    host: localhost
    port: 5432
    user: myuser
    password: mypassword
    database: mydb
    dest: /backups/postgres
    compression: gz
    retention:
      keep_last: 7
```

2. Run blobber:

```bash
blobber
```

## Configuration

Blobber uses a YAML configuration file. By default, it looks for `~/.config/blobber/config.yaml`. If `./blobber.yaml` exists in the current directory, it will be used instead (useful for project-specific configs).

### Database Configuration

```yaml
databases:
  # SQLite / file backup
  myapp:
    type: file
    path: /var/lib/myapp/data.db
    dest: s3:mybucket/myapp
    compression: gz
    retention:
      keep_last: 7

  # MySQL / MariaDB
  wordpress:
    type: mysql
    host: localhost
    port: 3306
    user: backup_user
    password: ${MYSQL_BACKUP_PASS}    # Environment variable
    database: wordpress
    dest: b2:backups/wordpress
    compression: zstd
    retention:
      keep_days: 30

  # PostgreSQL
  analytics:
    type: postgres
    host: localhost
    port: 5432
    user: backup_user
    password: ${PG_BACKUP_PASS}
    database: analytics
    dest: /backups/analytics          # Local path
    compression: none
    retention:
      max_size_mb: 500
```

### Compression Options

| Option | Description |
|--------|-------------|
| `none` | No compression |
| `gz`   | gzip (recommended for most cases) |
| `zstd` | Zstandard (faster, better compression) |
| `xz`   | XZ (best compression, slower) |
| `zip`  | ZIP archive |

### Retention Policies

| Option | Description |
|--------|-------------|
| `keep_last: N` | Keep the N most recent backups |
| `keep_days: N` | Keep backups from the last N days |
| `max_size_mb: N` | Keep backups until total size exceeds N MB |

Rules can be combined. A backup is deleted if **any** rule marks it for deletion.

### Destinations

Destinations can be:

- **Local paths**: `/backups/mydb` or `./backups/mydb`
- **Rclone remotes**: `s3:bucket/path`, `gcs:bucket/path`, `b2:bucket/path`, etc.

## Storage Backends (rclone)

Blobber uses [rclone](https://rclone.org/) internally for cloud storage. You can configure storage destinations in two ways:

1. **Through the TUI** - Navigate to "Manage rclone destinations" to add, edit, or test remotes interactively. No rclone CLI needed.

2. **Using existing rclone config** - If you have rclone installed and configured, blobber will use your existing remotes from `~/.config/rclone/rclone.conf`.

To use a custom rclone config file:

```bash
blobber --rclone-config /path/to/rclone.conf
```

## Usage

### TUI Mode

Launch the interactive terminal interface:

```bash
blobber                                  # Uses ~/.config/blobber/config.yaml
blobber -c /path/to/config.yaml          # Custom config path
blobber --rclone-config ~/rclone.conf    # Custom rclone config
```

### CLI Mode

#### Global Flags

| Flag | Short | Description |
|------|-------|-------------|
| `--config` | `-c` | Path to config file (default: `~/.config/blobber/config.yaml`) |
| `--rclone-config` | | Path to rclone config file (default: `~/.config/rclone/rclone.conf`) |

#### `blobber backup`

Run database backups.

```bash
blobber backup                   # Backup all databases
blobber backup mydb              # Backup specific database
blobber backup db1 db2           # Backup multiple databases
blobber backup --dry-run         # Dump only, skip upload
blobber backup --skip-retention  # Skip retention policy cleanup
```

| Flag | Description |
|------|-------------|
| `--dry-run` | Perform dump but skip upload and retention cleanup |
| `--skip-retention` | Skip retention policy for this run |

#### `blobber list`

List available backups for a database.

```bash
blobber list mydb
```

Output shows backup filename, size, and timestamp.

#### `blobber restore`

Restore a database from backup.

```bash
blobber restore mydb backup_2024-01-15_120000.sql.gz       # From remote
blobber restore --local mydb /path/to/local/backup.sql.gz  # From local file
```

| Flag | Description |
|------|-------------|
| `--local` | Restore from a local file instead of downloading from remote |

## Development

### Additional Prerequisites

- Docker and Docker Compose
- sqlite3 CLI

### Dev Environment

Start a local environment with test databases:

```bash
make dev-up     # Start MySQL, MariaDB, PostgreSQL, MinIO, Azurite
make dev        # Run TUI with dev config
make dev-down   # Stop containers
make dev-clean  # Clean up backup files
```

| Service    | Host      | Port  | Credentials |
|------------|-----------|-------|-------------|
| MySQL      | localhost | 3306  | testuser / testpass / testdb |
| MariaDB    | localhost | 3307  | testuser / testpass / testdb |
| PostgreSQL | localhost | 5432  | testuser / testpass / testdb |
| SQLite     | dev/test.db | -   | - |
| MinIO      | localhost | 9001  | minioadmin / minioadmin |
| Azurite    | localhost | 10000 | devstoreaccount1 |

### Running Tests

```bash
make test              # Unit tests
make test-integration  # Integration tests (starts Docker)
make test-all          # All tests
```
