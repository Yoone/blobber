package orchestrator

import (
	"context"
	"fmt"
	"sync"

	"github.com/Yoone/blobber/internal/backup"
	"github.com/Yoone/blobber/internal/config"
	"github.com/Yoone/blobber/internal/retention"
	"github.com/Yoone/blobber/internal/storage"
)

// BackupStep represents a step in the backup process
type BackupStep string

const (
	StepDumping   BackupStep = "dumping"
	StepUploading BackupStep = "uploading"
	StepRetention BackupStep = "retention"
)

func (s BackupStep) String() string {
	switch s {
	case StepDumping:
		return "Dumping database"
	case StepUploading:
		return "Saving backup"
	case StepRetention:
		return "Applying retention policy"
	default:
		return string(s)
	}
}

// BackupOptions configures the backup run
type BackupOptions struct {
	DryRun        bool // perform dump but skip upload and retention
	SkipRetention bool // skip retention policy
}

// BackupProgress reports progress for a single database backup
type BackupProgress struct {
	DBName  string
	Step    BackupStep
	Message string
	Done    bool
	Error   error
	Skipped bool // true if step was skipped (e.g., no retention policy)
}

// BackupResult contains the final result for a database backup
type BackupResult struct {
	DBName  string
	Success bool
	Error   error
	Steps   []BackupProgress // completed steps
}

// RetentionPlan maps database names to files that would be deleted
type RetentionPlan map[string][]storage.RemoteFile

// PreCheckRetention calculates which files would be deleted by retention policies
// without actually deleting them. Returns a plan that can be reviewed before execution.
func PreCheckRetention(ctx context.Context, cfg *config.Config, databases []string) (RetentionPlan, error) {
	plan := make(RetentionPlan)

	for _, name := range databases {
		db := cfg.Databases[name]
		if db.Retention.KeepLast == 0 && db.Retention.KeepDays == 0 && db.Retention.MaxSizeMB == 0 {
			continue
		}

		files, err := storage.List(ctx, db.Dest)
		if err != nil {
			continue // skip on error, don't fail the whole check
		}

		toDelete := retention.Apply(ctx, files, name, db.Retention)
		if len(toDelete) > 0 {
			plan[name] = toDelete
		}
	}

	return plan, nil
}

// RunBackups executes backups for the specified databases in parallel.
// Progress updates are sent to the progress channel.
// The function blocks until all backups complete.
// If databases is empty, all configured databases are backed up.
func RunBackups(ctx context.Context, cfg *config.Config, databases []string, opts BackupOptions, retentionPlan RetentionPlan, progress chan<- BackupProgress) []BackupResult {
	// If no databases specified, use all
	if len(databases) == 0 {
		for name := range cfg.Databases {
			databases = append(databases, name)
		}
	}

	var wg sync.WaitGroup
	results := make([]BackupResult, len(databases))
	resultsMu := sync.Mutex{}

	for i, name := range databases {
		wg.Add(1)
		go func(idx int, dbName string) {
			defer wg.Done()
			result := runSingleBackup(ctx, cfg, dbName, opts, progress)
			resultsMu.Lock()
			results[idx] = result
			resultsMu.Unlock()
		}(i, name)
	}

	wg.Wait()
	return results
}

// runSingleBackup executes all backup steps for a single database
func runSingleBackup(ctx context.Context, cfg *config.Config, name string, opts BackupOptions, progress chan<- BackupProgress) BackupResult {
	db := cfg.Databases[name]
	result := BackupResult{DBName: name, Success: true}

	// Step 1: Dump
	progress <- BackupProgress{DBName: name, Step: StepDumping}

	backupResult, err := backup.Run(name, db)
	if err != nil {
		progress <- BackupProgress{DBName: name, Step: StepDumping, Error: err, Done: true}
		result.Success = false
		result.Error = err
		return result
	}
	// Skip cleanup in dry-run mode so user can access the file
	if !opts.DryRun {
		defer backup.Cleanup(backupResult)
	}

	msg := fmt.Sprintf("Dumped %s (%.2f MB)", backupResult.Filename, float64(backupResult.Size)/(1024*1024))
	progress <- BackupProgress{DBName: name, Step: StepDumping, Message: msg}
	result.Steps = append(result.Steps, BackupProgress{DBName: name, Step: StepDumping, Message: msg})

	// Step 2: Upload
	if opts.DryRun {
		msg := fmt.Sprintf("Upload skipped (dry-run), file at %s", backupResult.Path)
		progress <- BackupProgress{DBName: name, Step: StepUploading, Message: msg, Skipped: true}
		result.Steps = append(result.Steps, BackupProgress{DBName: name, Step: StepUploading, Message: msg, Skipped: true})
	} else {
		progress <- BackupProgress{DBName: name, Step: StepUploading}

		if err := storage.Upload(ctx, backupResult.Path, db.Dest); err != nil {
			progress <- BackupProgress{DBName: name, Step: StepUploading, Error: err, Done: true}
			result.Success = false
			result.Error = err
			return result
		}

		msg := fmt.Sprintf("Saved to %s", db.Dest)
		progress <- BackupProgress{DBName: name, Step: StepUploading, Message: msg}
		result.Steps = append(result.Steps, BackupProgress{DBName: name, Step: StepUploading, Message: msg})
	}

	// Step 3: Retention
	// Re-calculate retention after upload to include the new file
	if opts.DryRun {
		progress <- BackupProgress{DBName: name, Step: StepRetention, Message: "Retention skipped (dry-run)", Skipped: true, Done: true}
		result.Steps = append(result.Steps, BackupProgress{DBName: name, Step: StepRetention, Message: "Retention skipped (dry-run)", Skipped: true})
	} else if opts.SkipRetention {
		progress <- BackupProgress{DBName: name, Step: StepRetention, Message: "Skipped (--skip-retention)", Skipped: true, Done: true}
		result.Steps = append(result.Steps, BackupProgress{DBName: name, Step: StepRetention, Message: "Skipped (--skip-retention)", Skipped: true})
	} else if db.Retention.KeepLast > 0 || db.Retention.KeepDays > 0 || db.Retention.MaxSizeMB > 0 {
		progress <- BackupProgress{DBName: name, Step: StepRetention}

		// Re-fetch files after upload to get accurate count including new backup
		files, err := storage.List(ctx, db.Dest)
		if err != nil {
			progress <- BackupProgress{DBName: name, Step: StepRetention, Error: err, Done: true}
			result.Steps = append(result.Steps, BackupProgress{DBName: name, Step: StepRetention, Error: err})
			result.Error = err
			return result
		}

		toDelete := retention.Apply(ctx, files, name, db.Retention)
		if len(toDelete) > 0 {
			var deleted int
			for _, f := range toDelete {
				if err := storage.Delete(ctx, db.Dest, f.Name); err == nil {
					deleted++
				}
			}
			msg := fmt.Sprintf("Deleted %d old backup(s)", deleted)
			progress <- BackupProgress{DBName: name, Step: StepRetention, Message: msg, Done: true}
			result.Steps = append(result.Steps, BackupProgress{DBName: name, Step: StepRetention, Message: msg})
		} else {
			progress <- BackupProgress{DBName: name, Step: StepRetention, Message: "No old backups to delete", Skipped: true, Done: true}
			result.Steps = append(result.Steps, BackupProgress{DBName: name, Step: StepRetention, Message: "No old backups to delete", Skipped: true})
		}
	} else {
		progress <- BackupProgress{DBName: name, Step: StepRetention, Message: "No retention policy", Skipped: true, Done: true}
		result.Steps = append(result.Steps, BackupProgress{DBName: name, Step: StepRetention, Message: "No retention policy", Skipped: true})
	}

	return result
}
