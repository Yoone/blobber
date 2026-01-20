package retention

import (
	"context"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Yoone/blobber/internal/config"
	"github.com/Yoone/blobber/internal/storage"
)

// backupFile represents a backup file with parsed timestamp
type backupFile struct {
	storage.RemoteFile
	Timestamp time.Time
}

// filenamePattern matches: {name}_{YYYYMMDD_HHMMSS}.{ext}
// Example: mydb_20240115_143022.sql.gz
var filenamePattern = regexp.MustCompile(`^(.+)_(\d{8}_\d{6})\.(.+)$`)

// parseFilename extracts the database name and timestamp from a backup filename.
// Returns the name, timestamp, and whether the parse was successful.
func parseFilename(filename string) (name string, timestamp time.Time, ok bool) {
	// Remove any directory prefix
	base := filepath.Base(filename)

	matches := filenamePattern.FindStringSubmatch(base)
	if matches == nil {
		return "", time.Time{}, false
	}

	name = matches[1]
	ts, err := time.Parse("20060102_150405", matches[2])
	if err != nil {
		return "", time.Time{}, false
	}

	return name, ts, true
}

// filterByName filters files to only include those matching the given database name
// and that follow the expected naming convention. Returns files sorted newest first.
func filterByName(files []storage.RemoteFile, dbName string) []backupFile {
	var filtered []backupFile

	for _, f := range files {
		name, ts, ok := parseFilename(f.Name)
		if !ok {
			// Skip files not matching our naming convention
			continue
		}
		if !strings.EqualFold(name, dbName) {
			// Skip files for other databases
			continue
		}
		filtered = append(filtered, backupFile{
			RemoteFile: f,
			Timestamp:  ts,
		})
	}

	// Sort by timestamp, newest first
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Timestamp.After(filtered[j].Timestamp)
	})

	return filtered
}

// Apply applies the retention policy and returns files to delete.
// Only considers files matching the database name and naming convention.
func Apply(ctx context.Context, files []storage.RemoteFile, dbName string, retention config.Retention) []storage.RemoteFile {
	if len(files) == 0 {
		return nil
	}

	// Filter to only files for this database with valid naming
	filtered := filterByName(files, dbName)
	if len(filtered) == 0 {
		return nil
	}

	var toDelete []backupFile

	switch {
	case retention.KeepLast > 0:
		toDelete = applyKeepLast(filtered, retention.KeepLast)
	case retention.KeepDays > 0:
		toDelete = applyKeepDays(filtered, retention.KeepDays)
	case retention.MaxSizeMB > 0:
		toDelete = applyMaxSize(filtered, retention.MaxSizeMB)
	}

	// Convert back to RemoteFile slice
	result := make([]storage.RemoteFile, len(toDelete))
	for i, f := range toDelete {
		result[i] = f.RemoteFile
	}
	return result
}

func applyKeepLast(files []backupFile, keepLast int) []backupFile {
	if len(files) <= keepLast {
		return nil
	}
	return files[keepLast:]
}

func applyKeepDays(files []backupFile, keepDays int) []backupFile {
	cutoff := time.Now().AddDate(0, 0, -keepDays)

	var toDelete []backupFile
	for _, f := range files {
		if f.Timestamp.Before(cutoff) {
			toDelete = append(toDelete, f)
		}
	}
	return toDelete
}

func applyMaxSize(files []backupFile, maxSizeMB int) []backupFile {
	maxBytes := int64(maxSizeMB) * 1024 * 1024

	var totalSize int64
	var toDelete []backupFile

	for _, f := range files {
		totalSize += f.Size
		if totalSize > maxBytes {
			toDelete = append(toDelete, f)
		}
	}

	return toDelete
}
