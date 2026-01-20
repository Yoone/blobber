package cmd

import (
	"context"
	"fmt"
	"sort"
	"sync"

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

	fmt.Printf("Backing up %d database(s) in parallel...\n\n", len(databases))

	// Pre-check retention policies
	var retentionPlan orchestrator.RetentionPlan
	if !dryRun && !skipRetention {
		var err error
		retentionPlan, err = orchestrator.PreCheckRetention(ctx, cfg, databases)
		if err != nil {
			return fmt.Errorf("checking retention policies: %w", err)
		}
	}

	// Track per-database state for display
	type dbState struct {
		currentStep orchestrator.BackupStep
		messages    []string
		done        bool
		hasError    bool
	}
	states := make(map[string]*dbState)
	statesMu := sync.Mutex{}
	for _, name := range databases {
		states[name] = &dbState{}
	}

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

	// Process progress updates
	for p := range progress {
		statesMu.Lock()
		state := states[p.DBName]
		if state == nil {
			statesMu.Unlock()
			continue
		}

		if p.Message != "" {
			// Step completed
			prefix := "  ✓"
			if p.Skipped {
				prefix = "  ○"
			}
			if p.Error != nil {
				prefix = "  ✗"
				state.hasError = true
			}
			state.messages = append(state.messages, fmt.Sprintf("%s %s", prefix, p.Message))
		} else if p.Error != nil {
			// Step failed without message
			state.messages = append(state.messages, fmt.Sprintf("  ✗ %s: %v", p.Step.String(), p.Error))
			state.hasError = true
		}

		state.currentStep = p.Step
		if p.Done {
			state.done = true
		}
		statesMu.Unlock()
	}

	<-done

	// Print final results
	var failed int
	for _, name := range databases {
		state := states[name]
		fmt.Println(name)
		for _, msg := range state.messages {
			fmt.Println(msg)
		}
		if state.hasError {
			failed++
		}
		fmt.Println()
	}

	// Summary
	succeeded := len(databases) - failed
	if failed > 0 {
		fmt.Printf("Completed: %d succeeded, %d failed\n", succeeded, failed)
	} else {
		fmt.Printf("Completed: %d succeeded\n", succeeded)
	}

	return nil
}
