package cmd

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/Yoone/blobber/internal/backup"
	"github.com/Yoone/blobber/internal/orchestrator"
	"github.com/spf13/cobra"
)

var (
	dryRun        bool
	skipRetention bool
)

var backupCmd = &cobra.Command{
	Use:   "backup [database...]",
	Short: "Backup databases",
	Long: `Dumps configured databases and uploads them to their respective cloud destinations.

If no databases are specified, all configured databases are backed up.
Databases are backed up in parallel for faster execution.

Examples:
  blobber backup              # backup all databases
  blobber backup mydb         # backup only 'mydb'
  blobber backup db1 db2      # backup 'db1' and 'db2'
  blobber backup --dry-run    # dump only, skip upload`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runBackup(context.Background(), args, dryRun, skipRetention)
	},
}

func init() {
	rootCmd.AddCommand(backupCmd)
	backupCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Perform dump but skip upload and retention")
	backupCmd.Flags().BoolVar(&skipRetention, "skip-retention", false, "Skip retention policy for this run")
}

func runBackup(ctx context.Context, databases []string, dryRun, skipRetention bool) error {
	// Validate specified databases exist
	if len(databases) > 0 {
		for _, name := range databases {
			if _, exists := cfg.Databases[name]; !exists {
				return fmt.Errorf("database %q not found in config", name)
			}
		}
	} else {
		// Use all databases
		for name := range cfg.Databases {
			databases = append(databases, name)
		}
		sort.Strings(databases)
	}

	if len(databases) == 0 {
		fmt.Println("No databases configured")
		return nil
	}

	fmt.Printf("Starting backup of %d database(s): %s\n", len(databases), strings.Join(databases, ", "))

	// Pre-check retention policies
	var retentionPlan orchestrator.RetentionPlan
	if !dryRun && !skipRetention {
		var err error
		retentionPlan, err = orchestrator.PreCheckRetention(ctx, cfg, databases)
		if err != nil {
			return fmt.Errorf("checking retention policies: %w", err)
		}
	}

	// Track errors for summary
	errors := make(map[string]bool)
	errorsMu := sync.Mutex{}

	// Progress channel
	progress := make(chan orchestrator.BackupProgress, 100)

	// Start backup in background
	done := make(chan struct{})
	go func() {
		orchestrator.RunBackups(ctx, cfg, databases, orchestrator.BackupOptions{
			DryRun:        dryRun,
			SkipRetention: skipRetention,
		}, retentionPlan, progress)
		close(progress)
		close(done)
	}()

	// Print progress updates as they come in
	for p := range progress {
		// Get step name, with compression info for dump step
		stepName := p.Step.String()
		if p.Step == orchestrator.StepDumping {
			if db, ok := cfg.Databases[p.DBName]; ok {
				if label := backup.CompressionLabel(db.Compression); label != "" {
					stepName = fmt.Sprintf("Dumping & compressing database (%s)", label)
				}
			}
		}

		if p.Error != nil {
			// Error occurred
			if p.Message != "" {
				fmt.Printf("[%s] %s failed: %s\n", p.DBName, stepName, p.Message)
			} else {
				fmt.Printf("[%s] %s failed: %v\n", p.DBName, stepName, p.Error)
			}
			errorsMu.Lock()
			errors[p.DBName] = true
			errorsMu.Unlock()
		} else if p.Message != "" {
			// Step completed with message
			if p.Skipped {
				fmt.Printf("[%s] %s skipped: %s\n", p.DBName, stepName, p.Message)
			} else {
				fmt.Printf("[%s] %s completed: %s\n", p.DBName, stepName, p.Message)
			}
		} else {
			// Step starting
			fmt.Printf("[%s] %s...\n", p.DBName, stepName)
		}
	}

	<-done

	// Summary
	failed := len(errors)
	succeeded := len(databases) - failed
	if failed > 0 {
		fmt.Printf("Backup finished: %d succeeded, %d failed\n", succeeded, failed)
	} else {
		fmt.Printf("Backup finished: %d succeeded\n", succeeded)
	}

	return nil
}
