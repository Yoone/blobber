package cmd

import (
	"context"
	"fmt"

	"github.com/Yoone/blobber/internal/storage"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

var listCmd = &cobra.Command{
	Use:   "list <db_name>",
	Short: "List backups for a database",
	Long:  `Lists all backup files stored in the cloud for the specified database.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runList(context.Background(), args[0])
	},
}

func init() {
	rootCmd.AddCommand(listCmd)
}

func runList(ctx context.Context, dbName string) error {
	db, ok := cfg.Databases[dbName]
	if !ok {
		return fmt.Errorf("database %q not found in config", dbName)
	}

	files, err := storage.ListForDatabase(ctx, db.Dest, dbName)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		fmt.Printf("[%s] No backups found in %s\n", dbName, db.Dest)
		return nil
	}

	fmt.Printf("[%s] %d backup(s) in %s\n", dbName, len(files), db.Dest)
	for _, f := range files {
		fmt.Printf("%s  %s  %s\n", f.Name, f.ModTime.Format("2006-01-02 15:04:05"), humanize.IBytes(uint64(f.Size)))
	}

	return nil
}
