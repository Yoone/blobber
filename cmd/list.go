package cmd

import (
	"context"
	"fmt"

	"github.com/Yoone/blobber/internal/storage"
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

	files, err := storage.List(ctx, db.Dest)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		fmt.Println("No backups found")
		return nil
	}

	fmt.Printf("Backups for %s (%s):\n\n", dbName, db.Dest)
	for _, f := range files {
		fmt.Printf("  %s  %10.2f MB  %s\n", f.ModTime.Format("2006-01-02 15:04:05"), float64(f.Size)/(1024*1024), f.Name)
	}

	return nil
}
