package storage

import (
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"sort"
	"sync"
	"time"

	_ "github.com/rclone/rclone/backend/all"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/walk"
)

// RemoteFile represents a file on the remote storage
type RemoteFile struct {
	Name    string
	Size    int64
	ModTime time.Time
}

var initOnce sync.Once

// Init initializes the rclone storage backend.
// If configPath is empty, the default rclone config location is used (~/.config/rclone/rclone.conf).
// This function is safe to call multiple times; only the first call has effect.
func Init(configPath string) {
	initOnce.Do(func() {
		// Set custom config path if provided
		if configPath != "" {
			config.SetConfigPath(configPath)
		}

		// Initialize rclone config
		configfile.Install()

		// Suppress standard log output from rclone internals
		log.SetOutput(io.Discard)

		// Configure rclone to be completely quiet
		ci := fs.GetConfig(context.Background())
		ci.LogLevel = fs.LogLevelEmergency // Only log emergencies (effectively off)
		ci.StatsLogLevel = fs.LogLevelEmergency
		ci.UseJSONLog = false
		ci.Progress = false
	})
}

// Upload uploads a local file to the remote destination
func Upload(ctx context.Context, localPath, remoteDest string) error {
	// Create fs for local directory containing the file
	localDir := filepath.Dir(localPath)
	fileName := filepath.Base(localPath)

	fsrc, err := fs.NewFs(ctx, localDir)
	if err != nil {
		return fmt.Errorf("parsing local path: %w", err)
	}

	fdst, err := fs.NewFs(ctx, remoteDest)
	if err != nil {
		return fmt.Errorf("parsing remote destination: %w", err)
	}

	// Get the source file object
	srcObj, err := fsrc.NewObject(ctx, fileName)
	if err != nil {
		return fmt.Errorf("getting source object: %w", err)
	}

	// Copy the file
	_, err = operations.Copy(ctx, fdst, nil, srcObj.Remote(), srcObj)
	if err != nil {
		return fmt.Errorf("uploading file: %w", err)
	}

	return nil
}

// List lists files at the remote destination
func List(ctx context.Context, remoteDest string) ([]RemoteFile, error) {
	fdst, err := fs.NewFs(ctx, remoteDest)
	if err != nil {
		return nil, fmt.Errorf("parsing remote destination: %w", err)
	}

	var files []RemoteFile
	err = walk.ListR(ctx, fdst, "", false, -1, walk.ListObjects, func(entries fs.DirEntries) error {
		for _, entry := range entries {
			if obj, ok := entry.(fs.Object); ok {
				files = append(files, RemoteFile{
					Name:    obj.Remote(),
					Size:    obj.Size(),
					ModTime: obj.ModTime(ctx),
				})
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("listing files: %w", err)
	}

	// Sort by modification time, newest first
	sort.Slice(files, func(i, j int) bool {
		return files[i].ModTime.After(files[j].ModTime)
	})

	return files, nil
}

// Download downloads a file from remote storage to local path
func Download(ctx context.Context, remoteDest, fileName, localPath string) error {
	fsrc, err := fs.NewFs(ctx, remoteDest)
	if err != nil {
		return fmt.Errorf("parsing remote destination: %w", err)
	}

	fdst, err := fs.NewFs(ctx, localPath)
	if err != nil {
		return fmt.Errorf("parsing local path: %w", err)
	}

	// Get the source file object
	srcObj, err := fsrc.NewObject(ctx, fileName)
	if err != nil {
		return fmt.Errorf("getting remote object: %w", err)
	}

	// Copy the file
	_, err = operations.Copy(ctx, fdst, nil, srcObj.Remote(), srcObj)
	if err != nil {
		return fmt.Errorf("downloading file: %w", err)
	}

	return nil
}

// Delete deletes a file from remote storage
func Delete(ctx context.Context, remoteDest, fileName string) error {
	fdst, err := fs.NewFs(ctx, remoteDest)
	if err != nil {
		return fmt.Errorf("parsing remote destination: %w", err)
	}

	obj, err := fdst.NewObject(ctx, fileName)
	if err != nil {
		return fmt.Errorf("getting object: %w", err)
	}

	if err := obj.Remove(ctx); err != nil {
		return fmt.Errorf("deleting file: %w", err)
	}

	return nil
}

// TestAccess tests if the destination is accessible (can list files)
func TestAccess(ctx context.Context, remoteDest string) error {
	fdst, err := fs.NewFs(ctx, remoteDest)
	if err != nil {
		return fmt.Errorf("invalid destination: %w", err)
	}

	// Try to list the root to verify access
	_, err = fdst.List(ctx, "")
	if err != nil {
		return fmt.Errorf("cannot access destination: %w", err)
	}

	return nil
}
