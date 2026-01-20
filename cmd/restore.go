package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Yoone/blobber/internal/backup"
	"github.com/Yoone/blobber/internal/storage"
	"github.com/spf13/cobra"
)

var localRestore bool

var restoreCmd = &cobra.Command{
	Use:   "restore <db_name> <backup_file>",
	Short: "Restore a database from backup",
	Long:  `Downloads the specified backup file and restores it to the database. Use --local to restore from a local file instead.`,
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runRestore(context.Background(), args[0], args[1], localRestore)
	},
}

func init() {
	rootCmd.AddCommand(restoreCmd)
	restoreCmd.Flags().BoolVar(&localRestore, "local", false, "Restore from a local file instead of downloading from remote")
}

func runRestore(ctx context.Context, dbName, backupFile string, local bool) error {
	db, ok := cfg.Databases[dbName]
	if !ok {
		return fmt.Errorf("database %q not found in config", dbName)
	}

	var localPath string

	if local {
		// Use local file directly
		localPath = backupFile
		if _, err := os.Stat(localPath); err != nil {
			return fmt.Errorf("local file not found: %w", err)
		}
		fmt.Printf("Restoring from local file %s...\n", localPath)
	} else {
		// Download from remote
		tmpDir, err := os.MkdirTemp("", "blobber-restore-")
		if err != nil {
			return fmt.Errorf("creating temp dir: %w", err)
		}
		defer os.RemoveAll(tmpDir)

		localPath = filepath.Join(tmpDir, backupFile)

		fmt.Printf("Downloading %s from %s...\n", backupFile, db.Dest)
		if err := storage.Download(ctx, db.Dest, backupFile, tmpDir); err != nil {
			return fmt.Errorf("downloading backup: %w", err)
		}
	}

	fmt.Printf("Restoring to %s...\n", dbName)
	if err := backup.Restore(db, localPath); err != nil {
		return fmt.Errorf("restoring backup: %w", err)
	}

	fmt.Println("Restore completed successfully")
	return nil
}
