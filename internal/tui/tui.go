package tui

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Yoone/blobber/internal/backup"
	"github.com/Yoone/blobber/internal/config"
	"github.com/Yoone/blobber/internal/retention"
	"github.com/Yoone/blobber/internal/storage"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/huh"
	"github.com/rclone/rclone/fs"
	rcloneconfig "github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
)

type view int

const (
	viewMainMenu view = iota
	viewBackupSelect
	viewRetentionPreCheck   // checking retention policies before backup
	viewRetentionPreConfirm // confirmation before starting backups
	viewBackupRunning
	viewRestoreDBSelect
	viewRestoreSourceSelect
	viewRestoreFileSelect
	viewRestoreLocalInput
	viewRestoreConfirm
	viewRestoreRunning
	viewAddDBType
	viewAddDBForm
	viewAddDBFormConfirmExit
	viewDBList
	viewDBActions
	viewEditDBForm
	viewEditDBFormConfirmExit
	viewDeleteConfirm
	viewDBTest // Testing database connection
	viewDone

	// Rclone management views
	viewRcloneList               // List configured remotes
	viewRcloneActions            // Edit/Delete/Test actions for a remote
	viewRcloneAddType            // Select backend type (s3, azureblob, etc.)
	viewRcloneAddForm            // Form for configuring the remote
	viewRcloneAddFormConfirmExit // Confirm exit with unsaved changes
	viewRcloneDeleteConfirm      // Confirm deletion
	viewRcloneTestBucket         // Input bucket/path for testing
	viewRcloneTest               // Testing remote connection
	viewRcloneOAuth              // OAuth authentication in progress
)

// Menu option constants
const (
	// Main menu options
	menuBackup = iota
	menuRestore
	menuManage
	menuManageRclone
	menuExit
)

const (
	// DB type options
	dbTypeFile = iota
	dbTypeMySQL
	dbTypePostgres
)

const (
	// Restore source options
	restoreSourceRemote = iota
	restoreSourceLocal
)

const (
	// DB actions options
	dbActionEdit = iota
	dbActionTest
	dbActionDelete
	dbActionBack
)

const (
	// Rclone actions options
	rcloneActionEdit = iota
	rcloneActionTest
	rcloneActionDelete
	rcloneActionBack
)

const (
	// Yes/No confirmation options
	confirmYes = iota
	confirmNo
)

// backupStep represents the current step in the backup process
type backupStep int

const (
	stepIdle backupStep = iota
	stepDumping
	stepUploading
	stepRetention
)

func (s backupStep) String() string {
	switch s {
	case stepDumping:
		return "Dumping database"
	case stepUploading:
		return "Saving backup"
	case stepRetention:
		return "Applying retention policy"
	default:
		return ""
	}
}

// backupLogEntry represents a completed backup step
type backupLogEntry struct {
	DBName    string
	Step      backupStep
	Message   string
	IsError   bool
	IsSkipped bool
}

// dbBackupState tracks the backup state for a single database
type dbBackupState struct {
	currentStep     backupStep       // current step (stepIdle when done)
	logs            []backupLogEntry // completed steps
	result          *backup.Result   // result from dump step (for upload)
	done            bool             // true when all steps complete
	uploadBytesDone int64            // bytes uploaded so far
	uploadBytesTotal int64           // total bytes to upload
	uploadSpeed     float64          // upload speed in bytes/second
}

// restoreStep represents the current step in the restore process
type restoreStep int

const (
	restoreStepIdle restoreStep = iota
	restoreStepDownloading
	restoreStepRestoring
)

func (s restoreStep) String() string {
	switch s {
	case restoreStepDownloading:
		return "Downloading backup"
	case restoreStepRestoring:
		return "Restoring database"
	default:
		return ""
	}
}

// restoreLogEntry represents a completed restore step
type restoreLogEntry struct {
	Message string
	IsError bool
}

// formFields holds form field values in a heap-allocated struct
// so huh's pointer bindings survive bubbletea's model copying
type formFields struct {
	name        string
	path        string
	host        string
	port        string
	user        string
	password    string
	database    string
	dest        string
	compression string
	keepLast    string
	keepDays    string
	maxSizeMB   string
}

// restoreFormFields holds restore form field values in a heap-allocated struct
type restoreFormFields struct {
	path string
}

// rcloneTestFormFields holds rclone test form field values in a heap-allocated struct
type rcloneTestFormFields struct {
	bucket string
}

// downloadState holds download state in a heap-allocated struct to survive model copies
type downloadState struct {
	progressCh <-chan storage.TransferProgress
	tmpDir     string
	fileName   string
	fileSize   int64
}

// uploadState holds upload state in a heap-allocated struct to survive model copies
type uploadState struct {
	progressCh <-chan storage.TransferProgress
	dbName     string
	fileSize   int64
}

type model struct {
	cfg            *config.Config
	view           view
	cursor         int
	width          int // terminal width for dynamic sizing
	dbNames        []string
	selected       map[string]bool // for backup multi-select
	skipRetention  bool            // skip retention policy for this backup run
	dryRun         bool            // perform dump but skip upload and retention
	selectedDB          string // for restore
	backupFiles         []storage.RemoteFile
	backupFilesLoading  bool   // true while fetching backup files
	selectedFile        string
	selectedFileSize    int64  // size of selected file for restore
	isLocalRestore bool // true if restoring from local file
	logs           []string
	err            error
	quitting       bool

	// Spinner for progress indication
	spinner spinner.Model

	// Progress bar for downloads
	progressBar progress.Model

	// Backup progress tracking (parallel execution)
	backupQueue  []string                  // databases to backup (in order for display)
	backupStates map[string]*dbBackupState // per-database state
	uploadStates map[string]*uploadState   // per-database upload state (heap-allocated for channel)

	// Restore progress tracking
	restoreStep      restoreStep       // current restore step
	restoreLogs      []restoreLogEntry // completed restore steps
	restoreLocalPath string            // path to local file being restored

	// Download progress tracking
	downloadBytesDone int64          // bytes downloaded so far
	downloadSpeed     float64        // download speed in bytes/second
	downloadState     *downloadState // heap-allocated download state (survives model copies)

	// Retention plan (pre-calculated before backup starts)
	retentionPlan map[string][]storage.RemoteFile // dbName -> files to delete

	// Add database form (huh)
	addDBType string      // file, mysql, postgres
	addDBForm *huh.Form   // huh form for adding database
	formData  *formFields // heap-allocated form values (survives bubbletea copies)

	// Test state
	testRunning     bool   // true while test is running
	testConnResult  string // result of connection test (MySQL/Postgres page 1)
	testDestResult  string // result of destination test (page 2)
	formError       string // validation error to display in form
	pendingSave     bool // true when form completed and running pre-save tests
	pendingDestTest bool // true when destination test should run after connection test

	// Database list/edit/delete (viewDBList)
	editingDB      string   // name of database being edited (empty for add)
	dbFilter       string   // search filter text for database list
	dbFilteredList []string // databases filtered by search

	// Backup select (viewBackupSelect)
	backupFilter       string   // search filter for backup database selection
	backupFilteredList []string // databases filtered by search

	// Restore database select (viewRestoreDBSelect)
	restoreDBFilter       string   // search filter for restore database selection
	restoreDBFilteredList []string // databases filtered by search

	// Restore file select (viewRestoreFileSelect)
	restoreFileFilter       string               // search filter for backup files
	restoreFileFilteredList []storage.RemoteFile // backup files filtered by search

	// Backup running scroll (viewBackupRunning)
	backupScrollOffset int // index of first visible DB in backup progress

	// Retention pre-confirm pagination (viewRetentionPreConfirm)
	retentionDBPage int // current page (0-indexed) in retention preview

	// Restore local path form
	restorePathForm *huh.Form
	restoreFormData *restoreFormFields // heap-allocated form values

	// Rclone management
	rcloneRemotes            []string           // list of configured remote names
	rcloneRemoteFilter       string             // search filter for remote list
	rcloneRemoteFilteredList []string           // remotes filtered by search
	rcloneBackends           []*fs.RegInfo      // available backends (filtered, non-hidden)
	rcloneFilteredList       []*fs.RegInfo      // backends filtered by search
	rcloneFilter             string             // search filter text
	selectedRemote           string             // currently selected remote for actions
	selectedBackend          *fs.RegInfo        // backend type for new remote
	rcloneForm               *huh.Form          // dynamic form for remote config
	rcloneFormValues         map[string]*string // pointers to form values
	showAdvanced             bool                  // toggle for advanced options
	advancedLoaded           bool                  // true after form rebuilt with advanced options
	advancedStartPage        int                   // page index where advanced options start
	rcloneTestForm           *huh.Form             // form for entering bucket to test
	rcloneTestFormData       *rcloneTestFormFields // heap-allocated form values
	rcloneTestResult         string                // result of rclone connection test

	// OAuth state
	oauthStatus string // status message during OAuth
	oauthErr    error  // error from OAuth, if any
}

// expandPath expands ~ to home directory (for local file paths like SQLite)
func expandPath(path string) string {
	if strings.HasPrefix(path, "~/") {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, path[2:])
	}
	if path == "~" {
		home, _ := os.UserHomeDir()
		return home
	}
	return path
}

// expandDest expands ~ and converts relative paths to absolute for local destinations
// Remote destinations (containing :) are returned as-is
func expandDest(dest string) string {
	// If it contains ":", it's an rclone remote - don't modify
	if strings.Contains(dest, ":") {
		return dest
	}

	// Expand ~ to home directory
	if strings.HasPrefix(dest, "~/") {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, dest[2:])
	}
	if dest == "~" {
		home, _ := os.UserHomeDir()
		return home
	}

	// Convert relative paths to absolute
	if !filepath.IsAbs(dest) {
		abs, err := filepath.Abs(dest)
		if err == nil {
			return abs
		}
	}

	return dest
}

// formatDestForDisplay formats a destination path for user-friendly display
// It tries to show relative paths when possible, uses ~ for home directory,
// and truncates only if still too long
func formatDestForDisplay(dest string, maxLen int) string {
	// If it contains ":", it's an rclone remote - show as-is (maybe truncated)
	if strings.Contains(dest, ":") {
		if len(dest) > maxLen {
			return "..." + dest[len(dest)-(maxLen-3):]
		}
		return dest
	}

	// Try to make path relative to current working directory
	if cwd, err := os.Getwd(); err == nil {
		if rel, err := filepath.Rel(cwd, dest); err == nil && !strings.HasPrefix(rel, "..") {
			dest = rel
		}
	}

	// If still absolute and in home directory, use ~ notation
	if filepath.IsAbs(dest) {
		if home, err := os.UserHomeDir(); err == nil && strings.HasPrefix(dest, home) {
			dest = "~" + dest[len(home):]
		}
	}

	// Truncate if still too long
	if len(dest) > maxLen {
		return "..." + dest[len(dest)-(maxLen-3):]
	}
	return dest
}

// getPathSuggestions returns suggestions for a partial local path (used for SQLite file paths)
func getPathSuggestions(partial string) []string {
	// Skip suggestions for rclone remotes (contain :)
	if strings.Contains(partial, ":") {
		return nil
	}

	if partial == "" {
		return nil
	}

	expanded := expandPath(partial)
	dir := filepath.Dir(expanded)
	base := filepath.Base(expanded)

	// If path ends with /, list contents of that directory
	if strings.HasSuffix(partial, "/") {
		dir = expanded
		base = ""
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	var suggestions []string
	home, _ := os.UserHomeDir()

	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(strings.ToLower(name), strings.ToLower(base)) || base == "" {
			// Construct full path, preserving the original prefix style
			var fullPath string
			switch {
			case strings.HasPrefix(partial, "./"):
				// Preserve ./ prefix (filepath.Join removes it)
				fullPath = "./" + name
			case partial == "." || dir == ".":
				// Current directory without explicit ./
				fullPath = name
			case dir == "/":
				// Root directory
				fullPath = "/" + name
			default:
				fullPath = filepath.Join(dir, name)
			}

			// Convert back to ~ format if applicable
			if home != "" && strings.HasPrefix(fullPath, home) {
				fullPath = "~" + fullPath[len(home):]
			}

			// Check if it's a directory (follow symlinks)
			isDir := entry.IsDir()
			if entry.Type()&os.ModeSymlink != 0 {
				// For symlinks, stat the target to check if it's a directory
				targetPath := filepath.Join(dir, name)
				if info, err := os.Stat(targetPath); err == nil {
					isDir = info.IsDir()
				}
			}
			if isDir {
				fullPath += "/"
			}
			suggestions = append(suggestions, fullPath)
		}
	}
	return suggestions
}

// isFormDirty returns true if the user has entered any data in the form
func (m *model) isFormDirty() bool {
	if m.formData == nil {
		return false
	}
	if m.formData.name != "" || m.formData.dest != "" {
		return true
	}

	switch m.addDBType {
	case "file":
		if m.formData.path != "" {
			return true
		}
	case "mysql", "postgres":
		// Check if values differ from defaults
		defaultPort := "3306"
		if m.addDBType == "postgres" {
			defaultPort = "5432"
		}
		if m.formData.host != "127.0.0.1" || m.formData.port != defaultPort ||
			m.formData.user != "" || m.formData.password != "" || m.formData.database != "" {
			return true
		}
	}

	return false
}

// customKeyMap returns a KeyMap with customized bindings:
// - Arrow keys + Enter to navigate between fields
// - Tab to accept suggestions in Input
// - Tab/Shift+Tab to cycle options in Select
func customKeyMap() *huh.KeyMap {
	km := huh.NewDefaultKeyMap()

	// Input: arrows + enter for field navigation, tab for suggestions
	km.Input.AcceptSuggestion = key.NewBinding(
		key.WithKeys("tab"),
		key.WithHelp("tab", "complete"),
	)
	km.Input.Next = key.NewBinding(
		key.WithKeys("down", "enter"),
		key.WithHelp("↓/enter", "next"),
	)
	km.Input.Prev = key.NewBinding(
		key.WithKeys("up"),
		key.WithHelp("↑", "prev"),
	)

	// Select: Tab to cycle options, arrows + enter for field navigation
	km.Select.Up = key.NewBinding(
		key.WithKeys("shift+tab"),
		key.WithHelp("shift+tab", "prev option"),
	)
	km.Select.Down = key.NewBinding(
		key.WithKeys("tab"),
		key.WithHelp("tab", "next option"),
	)
	km.Select.Next = key.NewBinding(
		key.WithKeys("down", "enter"),
		key.WithHelp("↓/enter", "next field"),
	)
	km.Select.Prev = key.NewBinding(
		key.WithKeys("up"),
		key.WithHelp("↑", "prev field"),
	)

	// Confirm: left/right/tab to toggle, arrows + enter for navigation
	km.Confirm.Toggle = key.NewBinding(
		key.WithKeys("left", "right", "tab"),
		key.WithHelp("←/→/tab", "toggle"),
	)
	km.Confirm.Next = key.NewBinding(
		key.WithKeys("down", "enter"),
		key.WithHelp("↓/enter", "next"),
	)
	km.Confirm.Prev = key.NewBinding(
		key.WithKeys("up"),
		key.WithHelp("↑", "prev"),
	)

	return km
}

// validNamePattern matches only letters, digits, dashes, and underscores
var validNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// pageNumberPattern matches page indicators like "(2/3)" in form titles
var pageNumberPattern = regexp.MustCompile(`\((\d+)/\d+\)`)

// getFormPage returns the current form page (1-indexed) by parsing the form view
func (m *model) getFormPage() int {
	if m.addDBForm == nil {
		return 1
	}
	view := m.addDBForm.View()
	match := pageNumberPattern.FindStringSubmatch(view)
	if len(match) >= 2 {
		var p int
		fmt.Sscanf(match[1], "%d", &p)
		if p > 0 {
			return p
		}
	}
	return 1
}

// validateName checks if a name contains only filename-safe characters
func validateName(name string) error {
	if name == "" {
		return nil // Empty is handled by required field check
	}
	if !validNamePattern.MatchString(name) {
		return fmt.Errorf("only letters, digits, dashes, and underscores allowed")
	}
	return nil
}

// validateForm checks required fields and returns an error message if any are missing
func (m *model) validateForm() string {
	if m.formData == nil {
		return "Form data not initialized"
	}

	var errors []string

	if m.formData.name == "" {
		errors = append(errors, "Name is required")
	} else if err := validateName(m.formData.name); err != nil {
		errors = append(errors, "Name: "+err.Error())
	}
	if m.formData.dest == "" {
		errors = append(errors, "Backup destination is required")
	}

	switch m.addDBType {
	case "file":
		if m.formData.path == "" {
			errors = append(errors, "File path is required")
		}
	case "mysql", "postgres":
		if m.formData.user == "" {
			errors = append(errors, "Username is required")
		}
		if m.formData.database == "" {
			errors = append(errors, "Database name is required")
		}
	}

	if len(errors) > 0 {
		return strings.Join(errors, "; ")
	}
	return ""
}

// compressionOptions returns the available compression options for the form
func compressionOptions() []huh.Option[string] {
	return []huh.Option[string]{
		huh.NewOption("gz (recommended)", "gz"),
		huh.NewOption("none", "none"),
		huh.NewOption("zstd", "zstd"),
		huh.NewOption("xz", "xz"),
		huh.NewOption("zip", "zip"),
	}
}

// buildAddDBForm creates a huh form for the current database type.
// If resetValues is true, form field values are reset to defaults.
func (m *model) buildAddDBForm(resetValues bool) *huh.Form {
	if resetValues {
		m.testConnResult = ""
		m.testDestResult = ""
		// Allocate new formFields struct on heap
		m.formData = &formFields{
			host:        "127.0.0.1",
			port:        "3306",
			compression: "gz",
		}
		if m.addDBType == "postgres" {
			m.formData.port = "5432"
		}
	}

	// Build groups with names (titles added at end with page numbers)
	type namedGroup struct {
		name  string
		group *huh.Group
	}
	var namedGroups []namedGroup

	nameInput := huh.NewInput().
		Key("name").
		Title("Name (identifier)").
		Placeholder("mydb").
		Value(&m.formData.name).
		Validate(validateName)

	switch m.addDBType {
	case "file":
		pathInput := huh.NewInput().
			Key("path").
			Title("File path").
			Placeholder("~/data/mydb.sqlite").
			Value(&m.formData.path).
			SuggestionsFunc(func() []string {
				return getPathSuggestions(m.formData.path)
			}, &m.formData.path)

		namedGroups = append(namedGroups, namedGroup{
			name:  "File Configuration",
			group: huh.NewGroup(nameInput, pathInput),
		})

		destInput := huh.NewInput().
			Key("dest").
			Title("Backup destination (Ctrl+T to test)").
			Placeholder("~/backups or s3:bucket/path").
			Description("Local path or rclone remote").
			Value(&m.formData.dest).
			SuggestionsFunc(func() []string {
				return getPathSuggestions(m.formData.dest)
			}, &m.formData.dest)

		compressionSelect := huh.NewSelect[string]().
			Key("compression").
			Title("Compression").
			Options(compressionOptions()...).
			Value(&m.formData.compression)

		namedGroups = append(namedGroups, namedGroup{
			name:  "Backup Configuration",
			group: huh.NewGroup(destInput, compressionSelect),
		})

	case "mysql", "postgres":
		hostInput := huh.NewInput().
			Key("host").
			Title("Host").
			Placeholder("127.0.0.1").
			Value(&m.formData.host)

		portInput := huh.NewInput().
			Key("port").
			Title("Port").
			Placeholder(m.formData.port).
			Value(&m.formData.port)

		userInput := huh.NewInput().
			Key("user").
			Title("Username").
			Value(&m.formData.user)

		passwordInput := huh.NewInput().
			Key("password").
			Title("Password").
			EchoMode(huh.EchoModePassword).
			Value(&m.formData.password)

		databaseInput := huh.NewInput().
			Key("database").
			Title("Database name (Ctrl+T to test connection)").
			Value(&m.formData.database)

		namedGroups = append(namedGroups, namedGroup{
			name: "Database Configuration",
			group: huh.NewGroup(
				nameInput,
				hostInput,
				portInput,
				userInput,
				passwordInput,
				databaseInput,
			),
		})

		destInput := huh.NewInput().
			Key("dest").
			Title("Backup destination (Ctrl+T to test)").
			Placeholder("~/backups or s3:bucket/path").
			Description("Local path or rclone remote").
			Value(&m.formData.dest).
			SuggestionsFunc(func() []string {
				return getPathSuggestions(m.formData.dest)
			}, &m.formData.dest)

		compressionSelect := huh.NewSelect[string]().
			Key("compression").
			Title("Compression").
			Options(compressionOptions()...).
			Value(&m.formData.compression)

		namedGroups = append(namedGroups, namedGroup{
			name:  "Backup Configuration",
			group: huh.NewGroup(destInput, compressionSelect),
		})
	}

	// Retention policy fields (common to all database types)
	keepLastInput := huh.NewInput().
		Key("keep_last").
		Title("Keep last N backups").
		Description("Applied on backup. Leave empty for unlimited.").
		Placeholder("e.g. 10").
		Value(&m.formData.keepLast)

	keepDaysInput := huh.NewInput().
		Key("keep_days").
		Title("Keep backups for N days").
		Description("Delete older backups. Leave empty for unlimited.").
		Placeholder("e.g. 30").
		Value(&m.formData.keepDays)

	maxSizeInput := huh.NewInput().
		Key("max_size_mb").
		Title("Max total size (MB)").
		Description("Delete oldest when exceeded. Leave empty for unlimited.").
		Placeholder("e.g. 1000").
		Value(&m.formData.maxSizeMB)

	namedGroups = append(namedGroups, namedGroup{
		name:  "Retention Policy (applied on backup)",
		group: huh.NewGroup(keepLastInput, keepDaysInput, maxSizeInput),
	})

	// Add page numbers to group titles
	var groups []*huh.Group
	total := len(namedGroups)
	for i, ng := range namedGroups {
		groups = append(groups, ng.group.Title(fmt.Sprintf("%s (%d/%d)", ng.name, i+1, total)))
	}

	return huh.NewForm(groups...).
		WithShowHelp(true).
		WithShowErrors(true).
		WithKeyMap(customKeyMap()).
		WithTheme(themeAmber()).
		WithWidth(m.formWidth())
}

// formWidth returns the width for forms (terminal width - 8 for border margin + padding, min 60)
func (m *model) formWidth() int {
	// Border margin is 4 chars, padding is 4 chars (2 each side)
	if m.width > 8 {
		return m.width - 8
	}
	return 60 // fallback
}

// buildRestorePathForm creates a huh form for entering a local backup file path
func (m *model) buildRestorePathForm() *huh.Form {
	// Allocate on heap so pointer survives bubbletea model copies
	if m.restoreFormData == nil {
		m.restoreFormData = &restoreFormFields{}
	}

	pathInput := huh.NewInput().
		Key("path").
		Title("Path to backup file").
		Placeholder("~/backups/mydb_backup.sql.gz").
		Value(&m.restoreFormData.path).
		Validate(func(s string) error {
			if s == "" {
				return fmt.Errorf("path is required")
			}
			expanded := expandPath(s)
			if _, err := os.Stat(expanded); os.IsNotExist(err) {
				return fmt.Errorf("file not found: %s", s)
			}
			return nil
		}).
		SuggestionsFunc(func() []string {
			return getPathSuggestions(m.restoreFormData.path)
		}, &m.restoreFormData.path)

	// Use a simpler key map for single-field form
	km := huh.NewDefaultKeyMap()
	km.Input.AcceptSuggestion = key.NewBinding(
		key.WithKeys("tab"),
		key.WithHelp("tab", "complete"),
	)

	return huh.NewForm(huh.NewGroup(pathInput)).
		WithShowHelp(true).
		WithShowErrors(true).
		WithKeyMap(km).
		WithTheme(themeAmber()).
		WithWidth(m.formWidth())
}

// buildRcloneTestForm creates a huh form for entering a bucket/path to test
func (m *model) buildRcloneTestForm() *huh.Form {
	// Allocate on heap so pointer survives bubbletea model copies
	if m.rcloneTestFormData == nil {
		m.rcloneTestFormData = &rcloneTestFormFields{}
	}

	bucketInput := huh.NewInput().
		Key("bucket").
		Title("Bucket or container name").
		Description("Leave empty to list all buckets (requires ListBuckets permission)").
		Placeholder("my-bucket").
		Value(&m.rcloneTestFormData.bucket)

	return huh.NewForm(huh.NewGroup(bucketInput)).
		WithShowHelp(true).
		WithShowErrors(true).
		WithTheme(themeAmber()).
		WithWidth(m.formWidth())
}

// isS3LikeBackend checks if a backend type requires a bucket/container.
// These backends need a bucket specified when testing, as root-level access
// often requires ListBuckets permission which users may not have.
func isS3LikeBackend(backendType string) bool {
	s3LikeBackends := []string{"s3", "gcs", "azureblob", "b2", "swift", "wasabi"}
	for _, backend := range s3LikeBackends {
		if strings.EqualFold(backendType, backend) {
			return true
		}
	}
	return false
}

// populateFormFromDB loads values from an existing database config into form fields
func (m *model) populateFormFromDB(name string) {
	db := m.cfg.Databases[name]
	m.addDBType = db.Type

	// Allocate formData on heap
	m.formData = &formFields{
		name:        name,
		dest:        db.Dest,
		compression: db.Compression,
	}
	if m.formData.compression == "" {
		m.formData.compression = "gz"
	}

	// Retention fields
	if db.Retention.KeepLast > 0 {
		m.formData.keepLast = fmt.Sprintf("%d", db.Retention.KeepLast)
	}
	if db.Retention.KeepDays > 0 {
		m.formData.keepDays = fmt.Sprintf("%d", db.Retention.KeepDays)
	}
	if db.Retention.MaxSizeMB > 0 {
		m.formData.maxSizeMB = fmt.Sprintf("%d", db.Retention.MaxSizeMB)
	}

	switch db.Type {
	case "file":
		m.formData.path = db.Path
	case "mysql", "postgres":
		m.formData.host = db.Host
		if m.formData.host == "" {
			m.formData.host = "127.0.0.1"
		}
		if db.Port > 0 {
			m.formData.port = fmt.Sprintf("%d", db.Port)
		} else if db.Type == "mysql" {
			m.formData.port = "3306"
		} else {
			m.formData.port = "5432"
		}
		m.formData.user = db.User
		m.formData.password = db.Password
		m.formData.database = db.Database
	}

	m.testConnResult = ""
	m.testDestResult = ""
}

// testDestinationAccess tests if the backup destination is accessible
func (m *model) testDestinationAccess() (bool, string) {
	if m.formData == nil {
		return false, "Form data not initialized"
	}

	dest := expandDest(m.formData.dest)

	ctx := context.Background()
	err := storage.TestAccess(ctx, dest)
	if err != nil {
		return false, fmt.Sprintf("Destination not accessible: %v", err)
	}

	return true, "Destination accessible"
}

// runConnectionTestCmd returns a tea.Cmd that tests MySQL/Postgres database connection
// runDBTestCmd runs connection and destination tests for the selected database
func (m *model) runDBTestCmd() tea.Cmd {
	dbName := m.editingDB
	db := m.cfg.Databases[dbName]
	m.testRunning = true

	return func() tea.Msg {
		// First test connection for MySQL/Postgres
		if db.Type == "mysql" || db.Type == "postgres" {
			if err := backup.TestConnection(db); err != nil {
				// Send connection failure, then test destination
				return dbTestResultMsg{testType: "connection", success: false, message: err.Error()}
			}
			// Connection succeeded, send result and continue to destination test
			return dbTestResultMsg{testType: "connection", success: true, message: "Database connection successful"}
		}
		// For file type, skip to destination test
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		dest := expandDest(db.Dest)
		if err := storage.TestAccess(ctx, dest); err != nil {
			return dbTestResultMsg{testType: "destination", success: false, message: err.Error(), done: true}
		}
		return dbTestResultMsg{testType: "destination", success: true, message: "Destination accessible", done: true}
	}
}

// runDBDestTestCmd runs the destination test after connection test
func (m *model) runDBDestTestCmd() tea.Cmd {
	dbName := m.editingDB
	db := m.cfg.Databases[dbName]

	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		dest := expandDest(db.Dest)
		if err := storage.TestAccess(ctx, dest); err != nil {
			return dbTestResultMsg{testType: "destination", success: false, message: err.Error(), done: true}
		}
		return dbTestResultMsg{testType: "destination", success: true, message: "Destination accessible", done: true}
	}
}

func (m *model) runConnectionTestCmd() tea.Cmd {
	if m.formData == nil {
		return func() tea.Msg {
			return testResultMsg{testType: "connection", success: false, message: "Form data not initialized"}
		}
	}

	// Build a temporary config.Database from form data
	dbType := m.addDBType
	host := m.formData.host
	port := m.formData.port
	user := m.formData.user
	password := m.formData.password
	database := m.formData.database

	return func() tea.Msg {
		if host == "" || user == "" || database == "" {
			return testResultMsg{testType: "connection", success: false, message: "Fill in connection details first"}
		}

		portNum, err := strconv.Atoi(port)
		if err != nil {
			return testResultMsg{testType: "connection", success: false, message: "Invalid port number"}
		}

		db := config.Database{
			Type:     dbType,
			Host:     host,
			Port:     portNum,
			User:     user,
			Password: password,
			Database: database,
		}

		if err := backup.TestConnection(db); err != nil {
			return testResultMsg{testType: "connection", success: false, message: err.Error()}
		}
		return testResultMsg{testType: "connection", success: true, message: "Database connection successful"}
	}
}

// runDestinationTestCmd returns a tea.Cmd that tests backup destination access
func (m *model) runDestinationTestCmd() tea.Cmd {
	if m.formData == nil {
		return func() tea.Msg {
			return testResultMsg{testType: "destination", success: false, message: "Form data not initialized"}
		}
	}

	dest := m.formData.dest

	return func() tea.Msg {
		if dest == "" {
			return testResultMsg{testType: "destination", success: false, message: "Enter a destination first"}
		}
		expandedDest := expandDest(dest)

		// Create context with connection timeout
		timeout := time.Duration(backup.ConnectTimeoutSeconds) * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		err := storage.TestAccess(ctx, expandedDest)
		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				return testResultMsg{testType: "destination", success: false, message: fmt.Sprintf("Destination access timed out (%ds)", backup.ConnectTimeoutSeconds)}
			}
			return testResultMsg{testType: "destination", success: false, message: fmt.Sprintf("Destination not accessible: %v", err)}
		}
		return testResultMsg{testType: "destination", success: true, message: "Destination accessible"}
	}
}

// checkRequiredUtilities checks if required dump/restore utilities are in PATH
// Returns a list of warning messages for missing utilities
func checkRequiredUtilities(dbType string) []string {
	var warnings []string

	switch dbType {
	case "mysql":
		if _, err := exec.LookPath("mysqldump"); err != nil {
			warnings = append(warnings, "mysqldump not found in PATH (required for backup)")
		}
		if _, err := exec.LookPath("mysql"); err != nil {
			warnings = append(warnings, "mysql client not found in PATH (required for restore)")
		}
	case "postgres":
		if _, err := exec.LookPath("pg_dump"); err != nil {
			warnings = append(warnings, "pg_dump not found in PATH (required for backup)")
		}
		if _, err := exec.LookPath("psql"); err != nil {
			warnings = append(warnings, "psql not found in PATH (required for restore)")
		}
	}

	return warnings
}

func Run(cfg *config.Config) error {
	// Get sorted database names
	var dbNames []string
	for name := range cfg.Databases {
		dbNames = append(dbNames, name)
	}
	sort.Strings(dbNames)

	// Initialize with all DBs selected for backup
	selected := make(map[string]bool)
	for _, name := range dbNames {
		selected[name] = true
	}

	// Initialize spinner
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = selectedStyle

	// Initialize progress bar
	prog := progress.New(progress.WithDefaultGradient())

	m := model{
		cfg:            cfg,
		view:           viewMainMenu,
		dbNames:        dbNames,
		dbFilteredList: dbNames, // Initialize filtered list with all databases
		selected:       selected,
		spinner:        s,
		progressBar:    prog,
	}

	p := tea.NewProgram(m)
	_, err := p.Run()
	return err
}

func (m model) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		return m, nil

	case tea.KeyMsg:
		// Handle Ctrl+C globally
		if msg.Type == tea.KeyCtrlC {
			m.quitting = true
			return m, tea.Quit
		}

		// Handle confirm exit view for add form
		if m.view == viewAddDBFormConfirmExit {
			switch msg.Type {
			case tea.KeyCtrlC:
				m.quitting = true
				return m, tea.Quit
			case tea.KeyUp, tea.KeyDown:
				// Toggle between yes/no
				m.cursor = 1 - m.cursor
				return m, nil
			case tea.KeyEnter:
				if m.cursor == confirmYes { // Yes, discard changes
					m.view = viewAddDBType
					m.cursor = 0
					m.addDBForm = nil
					return m, nil
				} else { // No, go back to form
					m.view = viewAddDBForm
					m.cursor = 0
					return m, nil
				}
			case tea.KeyEsc:
				// ESC on confirm goes back to form
				m.view = viewAddDBForm
				m.cursor = 0
				return m, nil
			}
			return m, nil
		}

		// Handle confirm exit view for edit form
		if m.view == viewEditDBFormConfirmExit {
			switch msg.Type {
			case tea.KeyCtrlC:
				m.quitting = true
				return m, tea.Quit
			case tea.KeyUp, tea.KeyDown:
				m.cursor = 1 - m.cursor
				return m, nil
			case tea.KeyEnter:
				if m.cursor == confirmYes { // Yes, discard changes
					m.view = viewDBActions
					m.cursor = 0
					m.addDBForm = nil
					return m, nil
				} else { // No, go back to form
					m.view = viewEditDBForm
					m.cursor = 0
					return m, nil
				}
			case tea.KeyEsc:
				m.view = viewEditDBForm
				m.cursor = 0
				return m, nil
			}
			return m, nil
		}

		// Handle confirm exit view for rclone add form
		if m.view == viewRcloneAddFormConfirmExit {
			switch msg.Type {
			case tea.KeyCtrlC:
				m.quitting = true
				return m, tea.Quit
			case tea.KeyUp, tea.KeyDown:
				m.cursor = 1 - m.cursor
				return m, nil
			case tea.KeyEnter:
				if m.cursor == confirmYes { // Yes, discard changes
					// Check if we were editing or adding
					_, wasAdding := m.rcloneFormValues["_name"]
					if wasAdding {
						m.view = viewRcloneAddType
						m.cursor = 0
					} else {
						m.view = viewRcloneActions
						m.cursor = 0
					}
					m.rcloneForm = nil
					m.rcloneFormValues = nil
					m.selectedBackend = nil
					return m, nil
				} else { // No, go back to form
					m.view = viewRcloneAddForm
					m.cursor = 0
					return m, nil
				}
			case tea.KeyEsc:
				m.view = viewRcloneAddForm
				m.cursor = 0
				return m, nil
			}
			return m, nil
		}

		// Handle huh form for add database
		if m.view == viewAddDBForm && m.addDBForm != nil {
			if msg.Type == tea.KeyCtrlC {
				m.quitting = true
				return m, tea.Quit
			}
			if msg.Type == tea.KeyEsc {
				m.view = viewAddDBFormConfirmExit
				m.cursor = confirmNo // Default to "No, continue editing"
				return m, nil
			}
			// Ctrl+S: Save from anywhere in the form
			if msg.String() == "ctrl+s" {
				return m.saveNewDatabase()
			}
		}

		// Handle huh form for edit database
		if m.view == viewEditDBForm && m.addDBForm != nil {
			if msg.Type == tea.KeyCtrlC {
				m.quitting = true
				return m, tea.Quit
			}
			if msg.Type == tea.KeyEsc {
				m.view = viewEditDBFormConfirmExit
				m.cursor = confirmNo // Default to "No, continue editing"
				return m, nil
			}
			// Ctrl+S: Save from anywhere in the form
			if msg.String() == "ctrl+s" {
				return m.saveEditedDatabase()
			}
		}

		// Handle huh form for restore local path
		if m.view == viewRestoreLocalInput && m.restorePathForm != nil {
			if msg.Type == tea.KeyCtrlC {
				m.quitting = true
				return m, tea.Quit
			}
		}

		// Handle DB test view - any key returns to actions when done
		if m.view == viewDBTest && !m.testRunning {
			m.view = viewDBActions
			m.cursor = dbActionTest
			m.testConnResult = ""
			m.testDestResult = ""
			return m, nil
		}

		// Handle rclone test view - any key returns to previous view
		if m.view == viewRcloneTest && m.rcloneTestResult != "" {
			// Return to form if we came from there, otherwise to actions menu
			if m.rcloneForm != nil {
				m.view = viewRcloneAddForm
			} else {
				m.view = viewRcloneActions
				m.cursor = rcloneActionTest
			}
			m.rcloneTestResult = ""
			return m, nil
		}

		// Handle rclone OAuth view - allow escape to cancel on error
		if m.view == viewRcloneOAuth && m.oauthErr != nil {
			if msg.Type == tea.KeyEsc || msg.Type == tea.KeyEnter {
				// Delete the incomplete remote and go back
				rcloneconfig.DeleteRemote(m.selectedRemote)
				rcloneconfig.SaveConfig()
				m.refreshRcloneRemotes()
				m.view = viewRcloneList
				m.cursor = 0
				m.selectedRemote = ""
				m.oauthStatus = ""
				m.oauthErr = nil
				return m, nil
			}
		}

		// Handle filter input for filterable list views (backspace and typing)
		if m.isFilterableView() {
			switch msg.Type {
			case tea.KeyBackspace:
				if handled, newModel := m.handleFilterBackspace(); handled {
					return newModel, nil
				}
			case tea.KeyRunes:
				return m.handleFilterInput(string(msg.Runes)), nil
			}
			// Fall through to generic key handling for esc/up/down/enter
		}

		// Skip generic key handling for form views - let the form handle its own keys
		if m.view != viewAddDBForm && m.view != viewEditDBForm && m.view != viewRestoreLocalInput && m.view != viewRcloneAddForm && m.view != viewRcloneTestBucket {
			switch msg.String() {
			case "ctrl+c":
				m.quitting = true
				return m, tea.Quit

			case "esc":
				if m.view == viewMainMenu {
					m.quitting = true
					return m, tea.Quit
				}
				return m.goBack(), nil

			case "up", "k":
				if m.cursor > 0 {
					m.cursor--
				} else {
					m.cursor = m.maxCursor() // cycle to bottom
				}

			case "down", "j":
				if m.cursor < m.maxCursor() {
					m.cursor++
				} else {
					m.cursor = 0 // cycle to top
				}

			case "left", "h":
				// Previous page in retention preview
				if m.view == viewRetentionPreConfirm && m.retentionDBPage > 0 {
					m.retentionDBPage--
				}

			case "right", "l":
				// Next page in retention preview
				if m.view == viewRetentionPreConfirm {
					// Count DBs with files to delete
					dbCount := 0
					for _, name := range m.backupQueue {
						if len(m.retentionPlan[name]) > 0 {
							dbCount++
						}
					}
					maxPage := (dbCount - 1) / 5 // 5 DBs per page
					if m.retentionDBPage < maxPage {
						m.retentionDBPage++
					}
				}

			case "a":
				// Shortcut to add new rclone remote
				if m.view == viewRcloneList {
					m.loadRcloneBackends()
					m.view = viewRcloneAddType
					m.cursor = 0
					return m, nil
				}

			case " ":
				// Toggle selection in backup view
				if m.view == viewBackupSelect {
					if m.cursor < len(m.backupFilteredList) {
						// Toggle database selection
						name := m.backupFilteredList[m.cursor]
						m.selected[name] = !m.selected[name]
					} else if m.cursor == len(m.backupFilteredList) {
						// Toggle retention policy
						m.skipRetention = !m.skipRetention
					} else if m.cursor == len(m.backupFilteredList)+1 {
						// Toggle dry-run mode
						m.dryRun = !m.dryRun
					}
				}

			case "enter":
				return m.handleEnter()
			}
		}

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd

	case retentionPreCheckMsg:
		m.retentionPlan = msg.plan
		if len(m.retentionPlan) > 0 {
			// Show confirmation screen
			m.view = viewRetentionPreConfirm
			m.cursor = 0
			m.retentionDBPage = 0
			return m, nil
		}
		// No files to delete, start backups directly
		return m.startBackups()

	case backupStepDoneMsg:
		return m.handleBackupStepDone(msg)

	case allBackupsDoneMsg:
		// Stay on viewBackupRunning to show results with scrolling
		// User can press enter or esc to go back
		return m, nil

	case fileListMsg:
		m.backupFilesLoading = false
		m.backupFiles = msg.files
		m.err = msg.err
		if m.err == nil {
			m.view = viewRestoreFileSelect
			m.cursor = 0
			m.restoreFileFilter = ""
			m.restoreFileFilteredList = m.backupFiles
		}

	case downloadProgressMsg:
		return m.handleDownloadProgress(msg)

	case uploadProgressMsg:
		return m.handleUploadProgress(msg)

	case startUploadMsg:
		return m.startUploadWithProgress(msg.dbName, msg.backupPath, msg.dest)

	case restoreStepDoneMsg:
		return m.handleRestoreStepDone(msg)

	case testResultMsg:
		m.testRunning = false
		var result string
		if msg.success {
			result = successStyle.Render("✓ " + msg.message)
		} else {
			result = errorStyle.Render("✗ " + msg.message)
		}
		if msg.testType == "connection" {
			m.testConnResult = result
		} else {
			m.testDestResult = result
		}

		// Handle pre-save test flow
		if m.pendingSave {
			if m.pendingDestTest {
				// Connection test done, now run destination test
				m.pendingDestTest = false
				m.testRunning = true
				return m, tea.Batch(m.spinner.Tick, m.runDestinationTestCmd())
			}
			// All tests done, proceed to save
			m.pendingSave = false
			if m.view == viewAddDBForm {
				return m.saveNewDatabase()
			} else if m.view == viewEditDBForm {
				return m.saveEditedDatabase()
			}
		}
		return m, nil

	case rcloneTestResultMsg:
		m.testRunning = false
		if msg.success {
			m.rcloneTestResult = successStyle.Render("✓ " + msg.message)
		} else {
			m.rcloneTestResult = errorStyle.Render("✗ " + msg.message)
		}
		// Stay in current view showing the result
		return m, nil

	case dbTestResultMsg:
		if msg.success {
			if msg.testType == "connection" {
				m.testConnResult = successStyle.Render("✓ " + msg.message)
			} else {
				m.testDestResult = successStyle.Render("✓ " + msg.message)
			}
		} else {
			if msg.testType == "connection" {
				m.testConnResult = errorStyle.Render("✗ " + msg.message)
			} else {
				m.testDestResult = errorStyle.Render("✗ " + msg.message)
			}
		}
		// If connection test done, continue with destination test
		if msg.testType == "connection" && !msg.done {
			return m, m.runDBDestTestCmd()
		}
		// All tests done, stay in test view to show results
		m.testRunning = false
		return m, nil

	case oauthCompleteMsg:
		if msg.err != nil {
			m.oauthErr = msg.err
			m.oauthStatus = "Authentication failed"
			// Stay in OAuth view to show error
			return m, nil
		}

		// OAuth succeeded
		rcloneconfig.SaveConfig()
		m.refreshRcloneRemotes()

		if msg.isEdit {
			m.view = viewRcloneActions
			m.cursor = 0
		} else {
			m.view = viewRcloneList
			m.cursor = 0
			m.selectedRemote = ""
		}
		m.rcloneForm = nil
		m.rcloneFormValues = nil
		m.selectedBackend = nil
		m.oauthStatus = ""
		m.oauthErr = nil
		return m, nil
	}

	// Update huh form if active (add mode)
	if m.view == viewAddDBForm && m.addDBForm != nil {
		// Save old values to detect changes (formData is heap-allocated so pointers survive)
		var oldHost, oldPort, oldUser, oldPassword, oldDatabase, oldDest string
		if m.formData != nil {
			oldHost = m.formData.host
			oldPort = m.formData.port
			oldUser = m.formData.user
			oldPassword = m.formData.password
			oldDatabase = m.formData.database
			oldDest = m.formData.dest
		}

		form, cmd := m.addDBForm.Update(msg)
		if f, ok := form.(*huh.Form); ok {
			m.addDBForm = f
		}

		// Reset test results if relevant fields changed (values are directly bound to formData)
		if m.formData != nil {
			if m.formData.host != oldHost || m.formData.port != oldPort || m.formData.user != oldUser ||
				m.formData.password != oldPassword || m.formData.database != oldDatabase {
				m.testConnResult = ""
			}
			if m.formData.dest != oldDest {
				m.testDestResult = ""
			}
		}

		// Handle Ctrl+T to trigger test based on current page
		if keyMsg, ok := msg.(tea.KeyMsg); ok && keyMsg.String() == "ctrl+t" && !m.testRunning {
			page := m.getFormPage()

			// Page 1: Test connection (MySQL/Postgres only)
			// Page 2: Test destination (all types)
			// Page 3: No test
			if page == 1 && (m.addDBType == "mysql" || m.addDBType == "postgres") {
				m.testRunning = true
				m.testConnResult = ""
				return m, tea.Batch(m.spinner.Tick, m.runConnectionTestCmd())
			} else if page == 2 {
				m.testRunning = true
				m.testDestResult = ""
				return m, tea.Batch(m.spinner.Tick, m.runDestinationTestCmd())
			}
		}

		// Check if form completed
		if m.addDBForm.State == huh.StateCompleted {
			if err := m.validateForm(); err != "" {
				m.formError = err
				m.addDBForm = m.buildAddDBForm(false)
				return m, m.addDBForm.Init()
			}

			m.formError = ""
			// Run tests before saving
			m.pendingSave = true
			m.testConnResult = ""
			m.testDestResult = ""
			m.testRunning = true
			if m.addDBType == "mysql" || m.addDBType == "postgres" {
				// Run connection test first, then destination test
				m.pendingDestTest = true
				return m, tea.Batch(m.spinner.Tick, m.runConnectionTestCmd())
			}
			// File type: only run destination test
			return m, tea.Batch(m.spinner.Tick, m.runDestinationTestCmd())
		}

		// Check if form aborted
		if m.addDBForm.State == huh.StateAborted {
			return m.goBack(), nil
		}

		return m, cmd
	}

	// Update huh form if active (edit mode)
	if m.view == viewEditDBForm && m.addDBForm != nil {
		// Save old values to detect changes (formData is heap-allocated so pointers survive)
		var oldHost, oldPort, oldUser, oldPassword, oldDatabase, oldDest string
		if m.formData != nil {
			oldHost = m.formData.host
			oldPort = m.formData.port
			oldUser = m.formData.user
			oldPassword = m.formData.password
			oldDatabase = m.formData.database
			oldDest = m.formData.dest
		}

		form, cmd := m.addDBForm.Update(msg)
		if f, ok := form.(*huh.Form); ok {
			m.addDBForm = f
		}

		// Reset test results if relevant fields changed (values are directly bound to formData)
		if m.formData != nil {
			if m.formData.host != oldHost || m.formData.port != oldPort || m.formData.user != oldUser ||
				m.formData.password != oldPassword || m.formData.database != oldDatabase {
				m.testConnResult = ""
			}
			if m.formData.dest != oldDest {
				m.testDestResult = ""
			}
		}

		// Handle Ctrl+T to trigger test based on current page
		if keyMsg, ok := msg.(tea.KeyMsg); ok && keyMsg.String() == "ctrl+t" && !m.testRunning {
			page := m.getFormPage()

			// Page 1: Test connection (MySQL/Postgres only)
			// Page 2: Test destination (all types)
			// Page 3: No test
			if page == 1 && (m.addDBType == "mysql" || m.addDBType == "postgres") {
				m.testRunning = true
				m.testConnResult = ""
				return m, tea.Batch(m.spinner.Tick, m.runConnectionTestCmd())
			} else if page == 2 {
				m.testRunning = true
				m.testDestResult = ""
				return m, tea.Batch(m.spinner.Tick, m.runDestinationTestCmd())
			}
		}

		// Check if form completed
		if m.addDBForm.State == huh.StateCompleted {
			if err := m.validateForm(); err != "" {
				m.formError = err
				m.addDBForm = m.buildAddDBForm(false)
				return m, m.addDBForm.Init()
			}

			m.formError = ""
			// Run tests before saving
			m.pendingSave = true
			m.testConnResult = ""
			m.testDestResult = ""
			m.testRunning = true
			if m.addDBType == "mysql" || m.addDBType == "postgres" {
				// Run connection test first, then destination test
				m.pendingDestTest = true
				return m, tea.Batch(m.spinner.Tick, m.runConnectionTestCmd())
			}
			// File type: only run destination test
			return m, tea.Batch(m.spinner.Tick, m.runDestinationTestCmd())
		}

		// Check if form aborted
		if m.addDBForm.State == huh.StateAborted {
			return m.goBack(), nil
		}

		return m, cmd
	}

	// Update rclone form if active
	if m.view == viewRcloneAddForm && m.rcloneForm != nil {
		// Handle special keys before form consumes them
		if keyMsg, ok := msg.(tea.KeyMsg); ok {
			if keyMsg.Type == tea.KeyCtrlC {
				m.quitting = true
				return m, tea.Quit
			}
			if keyMsg.Type == tea.KeyEsc {
				m.view = viewRcloneAddFormConfirmExit
				m.cursor = confirmNo // Default to "No, continue editing"
				return m, nil
			}
			// Ctrl+S: Save from anywhere in the form
			if keyMsg.String() == "ctrl+s" {
				return m.saveRcloneRemote()
			}
			// Ctrl+T: Test the remote configuration
			if keyMsg.String() == "ctrl+t" && !m.testRunning {
				// For S3-like backends, prompt for bucket first since root-level
				// access requires ListBuckets permission which users may not have
				if m.selectedBackend != nil && isS3LikeBackend(m.selectedBackend.Name) {
					m.view = viewRcloneTestBucket
					m.rcloneTestFormData = nil
					m.rcloneTestResult = ""
					m.rcloneTestForm = m.buildRcloneTestForm()
					return m, m.rcloneTestForm.Init()
				}
				m.testRunning = true
				m.rcloneTestResult = ""
				return m, tea.Batch(m.spinner.Tick, m.runRcloneFormTestCmd(""))
			}
		}

		form, cmd := m.rcloneForm.Update(msg)
		if f, ok := form.(*huh.Form); ok {
			m.rcloneForm = f
		}

		// Check if form completed
		if m.rcloneForm.State == huh.StateCompleted {
			// Check if user selected "Show advanced options"
			showAdvPtr := m.rcloneFormValues["_show_advanced"]
			wantsAdvanced := showAdvPtr != nil && *showAdvPtr == "yes"

			// If user selected "Show advanced options" and we haven't loaded them yet, rebuild form
			if wantsAdvanced && !m.advancedLoaded {
				// Collect existing values to preserve them when rebuilding
				existingValues := make(map[string]string)
				for k, v := range m.rcloneFormValues {
					if v != nil && !strings.HasPrefix(k, "_") {
						existingValues[k] = *v
					}
				}
				// Rebuild form with advanced options
				m.showAdvanced = true
				m.advancedLoaded = true
				m.rcloneForm = m.buildRcloneForm(existingValues)
				// Jump to the first advanced options page
				cmds := []tea.Cmd{m.rcloneForm.Init()}
				for i := 0; i < m.advancedStartPage; i++ {
					cmds = append(cmds, m.rcloneForm.NextGroup())
				}
				return m, tea.Batch(cmds...)
			}
			return m.saveRcloneRemote()
		}

		// Check if form aborted
		if m.rcloneForm.State == huh.StateAborted {
			m.view = viewRcloneAddFormConfirmExit
			m.cursor = confirmNo
			return m, nil
		}

		return m, cmd
	}

	// Update restore path form if active
	if m.view == viewRestoreLocalInput && m.restorePathForm != nil {
		// Handle Esc before form consumes it
		if keyMsg, ok := msg.(tea.KeyMsg); ok {
			if keyMsg.Type == tea.KeyEsc {
				return m.goBack(), nil
			}
		}

		form, cmd := m.restorePathForm.Update(msg)
		if f, ok := form.(*huh.Form); ok {
			m.restorePathForm = f
		}

		// Check if form completed (validation passed)
		if m.restorePathForm.State == huh.StateCompleted {
			m.selectedFile = expandPath(m.restoreFormData.path)
			if stat, err := os.Stat(m.selectedFile); err == nil {
				m.selectedFileSize = stat.Size()
			}
			m.view = viewRestoreConfirm
			m.cursor = 0
			return m, nil
		}

		// Check if form aborted
		if m.restorePathForm.State == huh.StateAborted {
			return m.goBack(), nil
		}

		return m, cmd
	}

	// Update rclone test bucket form if active
	if m.view == viewRcloneTestBucket && m.rcloneTestForm != nil {
		// Handle Esc before form consumes it
		if keyMsg, ok := msg.(tea.KeyMsg); ok {
			if keyMsg.Type == tea.KeyEsc {
				return m.goBack(), nil
			}
		}

		form, cmd := m.rcloneTestForm.Update(msg)
		if f, ok := form.(*huh.Form); ok {
			m.rcloneTestForm = f
		}

		// Check if form completed - start the test
		if m.rcloneTestForm.State == huh.StateCompleted {
			m.view = viewRcloneTest
			bucket := ""
			if m.rcloneTestFormData != nil {
				bucket = m.rcloneTestFormData.bucket
			}
			m.rcloneTestForm = nil
			// Use form test (with current form values) if coming from the form,
			// otherwise use saved remote test
			if m.rcloneForm != nil {
				return m, m.runRcloneFormTestCmd(bucket)
			}
			return m, m.runRcloneTestCmd()
		}

		// Check if form aborted
		if m.rcloneTestForm.State == huh.StateAborted {
			return m.goBack(), nil
		}

		return m, cmd
	}

	return m, nil
}

func (m model) goBack() model {
	switch m.view {
	case viewBackupSelect, viewRestoreDBSelect, viewDBList, viewDone:
		m.view = viewMainMenu
		m.cursor = 0
		m.err = nil
		m.logs = nil
	case viewRetentionPreConfirm:
		m.view = viewBackupSelect
		m.cursor = 0
		m.retentionPlan = nil
	case viewRestoreSourceSelect:
		m.view = viewRestoreDBSelect
		m.cursor = 0
	case viewRestoreFileSelect, viewRestoreLocalInput:
		m.view = viewRestoreSourceSelect
		m.cursor = 0
		m.restoreFormData = nil
	case viewRestoreConfirm:
		if m.isLocalRestore {
			m.view = viewRestoreLocalInput
			// Rebuild form to keep the path value
			m.restorePathForm = m.buildRestorePathForm()
		} else {
			m.view = viewRestoreFileSelect
		}
	case viewAddDBType:
		m.view = viewDBList
		m.cursor = 0
	case viewAddDBForm:
		m.view = viewAddDBType
		m.cursor = 0
		m.addDBForm = nil
		m.pendingSave = false
		m.pendingDestTest = false
		m.testRunning = false
	case viewDBActions:
		m.view = viewDBList
		m.cursor = 0
		m.editingDB = ""
	case viewEditDBForm:
		m.view = viewDBActions
		m.cursor = 0
		m.addDBForm = nil
		m.pendingSave = false
		m.pendingDestTest = false
		m.testRunning = false
	case viewDeleteConfirm:
		m.view = viewDBActions
		m.cursor = 0
	case viewDBTest:
		m.view = viewDBActions
		m.cursor = dbActionTest
		m.testConnResult = ""
		m.testDestResult = ""
		m.testRunning = false
	case viewRcloneList:
		m.view = viewMainMenu
		m.cursor = menuManageRclone
		m.rcloneRemoteFilter = ""
		m.rcloneRemoteFilteredList = m.rcloneRemotes
	case viewRcloneActions:
		m.view = viewRcloneList
		m.cursor = 0
		m.selectedRemote = ""
		m.rcloneRemoteFilter = ""
		m.rcloneRemoteFilteredList = m.rcloneRemotes
	case viewRcloneAddType:
		m.view = viewRcloneList
		m.cursor = len(m.rcloneRemoteFilteredList) // back to Add button
		m.rcloneFilter = ""
		m.rcloneFilteredList = m.rcloneBackends
		m.rcloneRemoteFilter = ""
		m.rcloneRemoteFilteredList = m.rcloneRemotes
	case viewRcloneAddForm:
		// Check if we were editing or adding
		_, wasAdding := m.rcloneFormValues["_name"]
		if wasAdding {
			m.view = viewRcloneAddType
			m.cursor = 0
		} else {
			m.view = viewRcloneActions
			m.cursor = rcloneActionEdit
		}
		m.rcloneForm = nil
		m.rcloneFormValues = nil
		m.selectedBackend = nil
		m.showAdvanced = false
		m.advancedLoaded = false
	case viewRcloneDeleteConfirm:
		m.view = viewRcloneActions
		m.cursor = rcloneActionDelete
	case viewRcloneTestBucket:
		// Return to form if we came from there, otherwise to actions menu
		if m.rcloneForm != nil {
			m.view = viewRcloneAddForm
		} else {
			m.view = viewRcloneActions
			m.cursor = rcloneActionTest
		}
		m.rcloneTestFormData = nil
		m.rcloneTestForm = nil
	case viewRcloneTest:
		// Return to form if we came from there, otherwise to actions menu
		if m.rcloneForm != nil {
			m.view = viewRcloneAddForm
		} else {
			m.view = viewRcloneActions
			m.cursor = rcloneActionTest
		}
		m.rcloneTestResult = ""
	case viewRcloneOAuth:
		m.view = viewRcloneAddForm
		m.oauthStatus = ""
		m.oauthErr = nil
	}
	return m
}

func (m model) handleEnter() (tea.Model, tea.Cmd) {
	switch m.view {
	case viewMainMenu:
		switch m.cursor {
		case menuBackup:
			if len(m.dbNames) == 0 {
				m.err = fmt.Errorf("no databases configured")
				m.view = viewDone
				return m, nil
			}
			m.view = viewBackupSelect
			m.cursor = 0
			m.backupFilter = ""
			m.backupFilteredList = m.dbNames
		case menuRestore:
			if len(m.dbNames) == 0 {
				m.err = fmt.Errorf("no databases configured")
				m.view = viewDone
				return m, nil
			}
			m.view = viewRestoreDBSelect
			m.cursor = 0
			m.restoreDBFilter = ""
			m.restoreDBFilteredList = m.dbNames
		case menuManage:
			m.view = viewDBList
			m.cursor = 0
			m.dbFilter = ""
			m.dbFilteredList = m.dbNames
		case menuManageRclone:
			m.view = viewRcloneList
			m.cursor = 0
			m.refreshRcloneRemotes()
		case menuExit:
			m.quitting = true
			return m, tea.Quit
		}

	case viewAddDBType:
		types := []string{"file", "mysql", "postgres"}
		m.addDBType = types[m.cursor]
		m.addDBForm = m.buildAddDBForm(true)
		m.view = viewAddDBForm
		return m, m.addDBForm.Init()

	case viewBackupSelect:
		// Run Backup is after filtered databases, retention toggle, and dry-run toggle
		if m.cursor == len(m.backupFilteredList)+2 {
			// Build ordered queue of selected databases (from ALL databases, not just filtered)
			m.backupQueue = nil
			for _, name := range m.dbNames {
				if m.selected[name] {
					m.backupQueue = append(m.backupQueue, name)
				}
			}
			if len(m.backupQueue) == 0 {
				m.err = fmt.Errorf("no databases selected")
				m.view = viewDone
				return m, nil
			}

			// Reset cursor for backup running view
			m.cursor = 0

			// Skip retention pre-check if dry-run or skip-retention is enabled
			if m.dryRun || m.skipRetention {
				return m.startBackups()
			}

			// Check if any selected database has retention policy
			hasRetention := false
			for _, name := range m.backupQueue {
				db := m.cfg.Databases[name]
				if db.Retention.KeepLast > 0 || db.Retention.KeepDays > 0 || db.Retention.MaxSizeMB > 0 {
					hasRetention = true
					break
				}
			}

			if hasRetention {
				// Pre-check retention policies before starting backups
				m.view = viewRetentionPreCheck
				m.retentionPlan = nil
				return m, tea.Batch(m.spinner.Tick, m.runRetentionPreCheck())
			}

			// No retention to check, start backups directly
			return m.startBackups()
		}

	case viewRestoreDBSelect:
		if m.cursor < len(m.restoreDBFilteredList) {
			m.selectedDB = m.restoreDBFilteredList[m.cursor]
			m.view = viewRestoreSourceSelect
			m.cursor = 0
		}

	case viewRestoreSourceSelect:
		if m.cursor == restoreSourceRemote {
			// From remote
			m.isLocalRestore = false
			m.view = viewRestoreFileSelect
			m.backupFilesLoading = true
			m.backupFiles = nil
			return m, tea.Batch(m.spinner.Tick, m.fetchBackupFiles())
		} else {
			// From local file
			m.isLocalRestore = true
			m.view = viewRestoreLocalInput
			m.restoreFormData = nil // Reset so buildRestorePathForm allocates fresh
			m.restorePathForm = m.buildRestorePathForm()
			return m, m.restorePathForm.Init()
		}

	case viewRestoreFileSelect:
		if m.cursor < len(m.restoreFileFilteredList) {
			m.selectedFile = m.restoreFileFilteredList[m.cursor].Name
			m.selectedFileSize = m.restoreFileFilteredList[m.cursor].Size
			m.view = viewRestoreConfirm
			m.cursor = 0
		}

	case viewRestoreConfirm:
		if m.cursor == confirmYes { // Yes
			return m.startRestore()
		} else {
			if m.isLocalRestore {
				m.view = viewRestoreLocalInput
				m.restorePathForm = m.buildRestorePathForm()
				return m, m.restorePathForm.Init()
			} else {
				m.view = viewRestoreFileSelect
			}
			m.cursor = 0
		}

	case viewDBList:
		if m.cursor < len(m.dbFilteredList) {
			// Selected a database from filtered list
			m.editingDB = m.dbFilteredList[m.cursor]
			m.view = viewDBActions
			m.cursor = 0
		} else {
			// Add new database (cursor == len(dbFilteredList))
			m.view = viewAddDBType
			m.cursor = 0
		}

	case viewDBActions:
		switch m.cursor {
		case dbActionEdit:
			m.populateFormFromDB(m.editingDB)
			m.addDBForm = m.buildAddDBForm(false)
			m.view = viewEditDBForm
			return m, m.addDBForm.Init()
		case dbActionTest:
			m.view = viewDBTest
			m.testConnResult = ""
			m.testDestResult = ""
			return m, m.runDBTestCmd()
		case dbActionDelete:
			m.view = viewDeleteConfirm
			m.cursor = confirmNo // Default to "No, go back"
		case dbActionBack:
			m.view = viewDBList
			m.cursor = 0
			m.editingDB = ""
		}

	case viewDeleteConfirm:
		if m.cursor == confirmYes { // Yes, delete
			return m.deleteDatabase()
		} else {
			m.view = viewDBActions
			m.cursor = 0
		}

	case viewRetentionPreConfirm:
		if m.cursor == confirmYes { // Yes, proceed with retention
			return m.startBackups()
		} else { // No, skip retention
			m.skipRetention = true
			m.retentionPlan = nil
			return m.startBackups()
		}

	case viewRcloneList:
		if m.cursor < len(m.rcloneRemoteFilteredList) {
			// Selected a remote from filtered list
			m.selectedRemote = m.rcloneRemoteFilteredList[m.cursor]
			m.view = viewRcloneActions
			m.cursor = 0
		} else {
			// Add new remote
			m.loadRcloneBackends()
			m.view = viewRcloneAddType
			m.cursor = 0
		}

	case viewRcloneActions:
		switch m.cursor {
		case rcloneActionEdit:
			// Load existing values and build form
			existingValues := m.loadRcloneRemoteValues(m.selectedRemote)
			backendType := getRcloneRemoteType(m.selectedRemote)
			backend, _ := fs.Find(backendType)
			if backend != nil {
				m.selectedBackend = backend
				m.rcloneTestResult = ""
				m.showAdvanced = false
				m.advancedLoaded = false
				m.rcloneForm = m.buildRcloneForm(existingValues)
				m.view = viewRcloneAddForm
				return m, m.rcloneForm.Init()
			}
		case rcloneActionTest:
			m.view = viewRcloneTestBucket
			m.rcloneTestFormData = nil // Reset so buildRcloneTestForm allocates fresh
			m.rcloneTestResult = ""
			m.rcloneTestForm = m.buildRcloneTestForm()
			return m, m.rcloneTestForm.Init()
		case rcloneActionDelete:
			m.view = viewRcloneDeleteConfirm
			m.cursor = confirmNo // Default to "No, go back"
		case rcloneActionBack:
			m.view = viewRcloneList
			m.cursor = 0
			m.selectedRemote = ""
			m.rcloneRemoteFilter = ""
			m.rcloneRemoteFilteredList = m.rcloneRemotes
		}

	case viewRcloneAddType:
		if len(m.rcloneFilteredList) > 0 && m.cursor < len(m.rcloneFilteredList) {
			m.selectedBackend = m.rcloneFilteredList[m.cursor]
			m.rcloneTestResult = ""
			m.showAdvanced = false
			m.advancedLoaded = false
			m.rcloneForm = m.buildRcloneForm(nil)
			m.view = viewRcloneAddForm
			return m, m.rcloneForm.Init()
		}

	case viewRcloneDeleteConfirm:
		if m.cursor == confirmYes {
			rcloneconfig.DeleteRemote(m.selectedRemote)
			rcloneconfig.SaveConfig()
			m.refreshRcloneRemotes()
			m.view = viewRcloneList
			m.cursor = 0
			m.selectedRemote = ""
		} else {
			m.view = viewRcloneActions
			m.cursor = 0
		}

	case viewBackupRunning:
		// If all backups done, allow enter to go back to menu
		if m.allBackupsDone() {
			m.view = viewMainMenu
			m.cursor = 0
			m.backupQueue = nil
			m.backupStates = nil
		}

	case viewDone:
		m.view = viewMainMenu
		m.cursor = 0
		m.logs = nil
		m.err = nil
	}

	return m, nil
}

func (m model) maxCursor() int {
	switch m.view {
	case viewMainMenu:
		return menuExit // Backup, Restore, Manage DBs, Manage rclone, Exit
	case viewBackupSelect:
		// Filtered DBs + retention toggle + dry-run toggle + Run button
		return len(m.backupFilteredList) + 2
	case viewRestoreDBSelect:
		// Filtered DBs
		if len(m.restoreDBFilteredList) == 0 {
			return 0
		}
		return len(m.restoreDBFilteredList) - 1
	case viewRestoreSourceSelect:
		return restoreSourceLocal // Remote or Local
	case viewRestoreFileSelect:
		// Filtered backup files
		if len(m.restoreFileFilteredList) == 0 {
			return 0
		}
		return len(m.restoreFileFilteredList) - 1
	case viewRestoreConfirm, viewDeleteConfirm, viewRetentionPreConfirm, viewRcloneDeleteConfirm:
		return confirmNo // Yes or No
	case viewAddDBType:
		return dbTypePostgres // file, mysql, postgres
	case viewDBList:
		// Filtered DBs + Add button
		return len(m.dbFilteredList) // Add button at position len(dbFilteredList)
	case viewDBActions:
		return dbActionBack // Edit, Delete, Back
	case viewRcloneList:
		// Filtered remotes + Add button
		return len(m.rcloneRemoteFilteredList) // Add button at position len(filtered list)
	case viewRcloneActions:
		return rcloneActionBack // Edit, Test, Delete, Back
	case viewRcloneAddType:
		// Filtered backends list
		if len(m.rcloneFilteredList) == 0 {
			return 0
		}
		return len(m.rcloneFilteredList) - 1
	case viewBackupRunning:
		// Number of databases being backed up (for scroll navigation)
		if len(m.backupQueue) == 0 {
			return 0
		}
		return len(m.backupQueue) - 1
	}
	return 0
}

func (m model) View() string {
	if m.quitting {
		return ""
	}

	var s strings.Builder

	s.WriteString(titleStyle.Render("░█▀▄░█░░░█▀█░█▀▄░█▀▄░█▀▀░█▀▄"))
	s.WriteString("\n")
	s.WriteString(titleStyle.Render("░█▀▄░█░░░█░█░█▀▄░█▀▄░█▀▀░█▀▄   Database Backup & Restore Tool"))
	s.WriteString("\n")
	s.WriteString(titleStyle.Render("░▀▀░░▀▀▀░▀▀▀░▀▀░░▀▀░░▀▀▀░▀░▀"))
	s.WriteString("\n\n")

	switch m.view {
	case viewMainMenu:
		s.WriteString(m.renderMainMenu())
	case viewBackupSelect:
		s.WriteString(m.renderBackupSelect())
	case viewRetentionPreCheck:
		s.WriteString(m.renderRetentionPreCheck())
	case viewRetentionPreConfirm:
		s.WriteString(m.renderRetentionPreConfirm())
	case viewBackupRunning:
		s.WriteString(m.renderBackupRunning())
	case viewRestoreDBSelect:
		s.WriteString(m.renderRestoreDBSelect())
	case viewRestoreSourceSelect:
		s.WriteString(m.renderRestoreSourceSelect())
	case viewRestoreFileSelect:
		s.WriteString(m.renderRestoreFileSelect())
	case viewRestoreLocalInput:
		s.WriteString(m.renderRestoreLocalInput())
	case viewRestoreConfirm:
		s.WriteString(m.renderRestoreConfirm())
	case viewRestoreRunning:
		s.WriteString(m.renderRestoreRunning())
	case viewAddDBType:
		s.WriteString(m.renderAddDBType())
	case viewAddDBForm:
		s.WriteString(fmt.Sprintf("Configure %s database:\n\n", selectedStyle.Render(m.addDBType)))
		for _, warning := range checkRequiredUtilities(m.addDBType) {
			s.WriteString(errorStyle.Render("⚠ " + warning))
			s.WriteString("\n")
		}
		if len(checkRequiredUtilities(m.addDBType)) > 0 {
			s.WriteString("\n")
		}
		if m.formError != "" {
			s.WriteString(errorStyle.Render("⚠ " + m.formError))
			s.WriteString("\n\n")
		}
		// Show test/save status
		if m.pendingSave {
			// Show pre-save test progress
			if m.testConnResult != "" {
				s.WriteString(m.testConnResult)
				s.WriteString("\n")
			}
			if m.testDestResult != "" {
				s.WriteString(m.testDestResult)
				s.WriteString("\n")
			}
			if m.testRunning {
				s.WriteString(fmt.Sprintf("%s Saving...\n", m.spinner.View()))
			}
			s.WriteString("\n")
		} else if m.testRunning {
			// Manual test in progress
			s.WriteString(fmt.Sprintf("%s Testing...\n\n", m.spinner.View()))
		} else {
			// Show test results based on current page
			page := m.getFormPage()
			if page == 1 && m.testConnResult != "" && (m.addDBType == "mysql" || m.addDBType == "postgres") {
				s.WriteString(m.testConnResult)
				s.WriteString("\n\n")
			} else if page == 2 && m.testDestResult != "" {
				s.WriteString(m.testDestResult)
				s.WriteString("\n\n")
			}
		}
		if m.addDBForm != nil {
			s.WriteString(m.addDBForm.View())
		}
	case viewAddDBFormConfirmExit, viewEditDBFormConfirmExit:
		s.WriteString(m.renderConfirmExit())
	case viewDBList:
		s.WriteString(m.renderDBList())
	case viewDBActions:
		s.WriteString(m.renderDBActions())
	case viewEditDBForm:
		s.WriteString(fmt.Sprintf("Edit %s database:\n\n", selectedStyle.Render(m.editingDB)))
		for _, warning := range checkRequiredUtilities(m.addDBType) {
			s.WriteString(errorStyle.Render("⚠ " + warning))
			s.WriteString("\n")
		}
		if len(checkRequiredUtilities(m.addDBType)) > 0 {
			s.WriteString("\n")
		}
		if m.formError != "" {
			s.WriteString(errorStyle.Render("⚠ " + m.formError))
			s.WriteString("\n\n")
		}
		// Show test/save status
		if m.pendingSave {
			// Show pre-save test progress
			if m.testConnResult != "" {
				s.WriteString(m.testConnResult)
				s.WriteString("\n")
			}
			if m.testDestResult != "" {
				s.WriteString(m.testDestResult)
				s.WriteString("\n")
			}
			if m.testRunning {
				s.WriteString(fmt.Sprintf("%s Saving...\n", m.spinner.View()))
			}
			s.WriteString("\n")
		} else if m.testRunning {
			// Manual test in progress
			s.WriteString(fmt.Sprintf("%s Testing...\n\n", m.spinner.View()))
		} else {
			// Show test results based on current page
			page := m.getFormPage()
			if page == 1 && m.testConnResult != "" && (m.addDBType == "mysql" || m.addDBType == "postgres") {
				s.WriteString(m.testConnResult)
				s.WriteString("\n\n")
			} else if page == 2 && m.testDestResult != "" {
				s.WriteString(m.testDestResult)
				s.WriteString("\n\n")
			}
		}
		if m.addDBForm != nil {
			s.WriteString(m.addDBForm.View())
		}
	case viewDeleteConfirm:
		s.WriteString(m.renderDeleteConfirm())
	case viewDBTest:
		s.WriteString(m.renderDBTest())
	case viewRcloneList:
		s.WriteString(m.renderRcloneList())
	case viewRcloneActions:
		s.WriteString(m.renderRcloneActions())
	case viewRcloneAddType:
		s.WriteString(m.renderRcloneAddType())
	case viewRcloneAddForm:
		s.WriteString(m.renderRcloneAddForm())
	case viewRcloneAddFormConfirmExit:
		s.WriteString(m.renderConfirmExit())
	case viewRcloneDeleteConfirm:
		s.WriteString(m.renderRcloneDeleteConfirm())
	case viewRcloneTestBucket:
		s.WriteString(m.renderRcloneTestBucket())
	case viewRcloneTest:
		s.WriteString(m.renderRcloneTest())
	case viewRcloneOAuth:
		s.WriteString(m.renderRcloneOAuth())
	case viewDone:
		s.WriteString(m.renderDone())
	}

	s.WriteString("\n")
	switch m.view {
	case viewMainMenu:
		s.WriteString(dimStyle.Render("↑/↓: navigate • enter: select • esc: quit"))
	case viewBackupSelect:
		s.WriteString(dimStyle.Render("type to filter • ↑/↓: navigate • space: toggle • enter: run • esc: back"))
	case viewRestoreDBSelect, viewRestoreFileSelect:
		s.WriteString(dimStyle.Render("type to filter • ↑/↓: navigate • enter: select • esc: back"))
	case viewRestoreLocalInput:
		s.WriteString(dimStyle.Render("type path • enter: confirm • esc: back"))
	case viewAddDBForm, viewEditDBForm:
		s.WriteString(dimStyle.Render("↑/↓/enter: navigate • tab: cycle • ctrl+s: save • ctrl+t: test • esc: back"))
	case viewAddDBFormConfirmExit, viewEditDBFormConfirmExit, viewRcloneAddFormConfirmExit:
		s.WriteString(dimStyle.Render("↑/↓: select • enter: confirm • esc: cancel"))
	case viewDBList:
		s.WriteString(dimStyle.Render("type to filter • ↑/↓: navigate • enter: select • esc: back"))
	case viewRetentionPreCheck:
		s.WriteString(dimStyle.Render("Checking retention policies..."))
	case viewRetentionPreConfirm:
		s.WriteString(dimStyle.Render("←/→: page • ↑/↓: select • enter: confirm • esc: back"))
	case viewBackupRunning:
		if m.allBackupsDone() {
			s.WriteString(dimStyle.Render("↑/↓: scroll • enter: back to menu"))
		} else {
			s.WriteString(dimStyle.Render("↑/↓: scroll • waiting for backups to complete..."))
		}
	case viewRestoreRunning:
		// No help text needed - progress is shown in main view
	case viewDone:
		s.WriteString(dimStyle.Render("enter: continue"))
	case viewRcloneList:
		s.WriteString(dimStyle.Render("type to filter • ↑/↓: navigate • enter: select • a: add • esc: back"))
	case viewRcloneAddType:
		s.WriteString(dimStyle.Render("type to filter • ↑/↓: navigate • enter: select • esc: back"))
	case viewRcloneAddForm:
		s.WriteString(dimStyle.Render("↑/↓/enter: navigate • tab: cycle • ctrl+s: save • ctrl+t: test • esc: back"))
	case viewRcloneTestBucket:
		s.WriteString(dimStyle.Render("enter: test • esc: back"))
	case viewDBTest:
		if !m.testRunning {
			s.WriteString(dimStyle.Render("enter: continue"))
		} else {
			s.WriteString(dimStyle.Render("Testing..."))
		}
	case viewRcloneTest:
		if m.rcloneTestResult != "" {
			s.WriteString(dimStyle.Render("enter: continue"))
		} else {
			s.WriteString(dimStyle.Render("esc: cancel"))
		}
	case viewRcloneOAuth:
		if m.oauthErr != nil {
			s.WriteString(dimStyle.Render("enter: dismiss • esc: cancel"))
		} else {
			s.WriteString(dimStyle.Render("Waiting for authentication..."))
		}
	default:
		s.WriteString(dimStyle.Render("↑/↓: navigate • enter: select • esc: back"))
	}

	// Apply dynamic width to border (terminal width - 4 for safety margin, fallback to 80)
	width := m.width - 4
	if width < 40 {
		width = 80
	}
	return borderStyle.Width(width).Render(s.String())
}

func (m model) renderMainMenu() string {
	var s strings.Builder

	cfgPath, _ := filepath.Abs(m.cfg.Path())
	dbCount := len(m.dbNames)
	if dbCount == 0 {
		s.WriteString(dimStyle.Render(fmt.Sprintf("No databases configured (config: %s)", cfgPath)))
	} else if dbCount == 1 {
		s.WriteString(dimStyle.Render(fmt.Sprintf("1 database configured (config: %s)", cfgPath)))
	} else {
		s.WriteString(dimStyle.Render(fmt.Sprintf("%d databases configured (config: %s)", dbCount, cfgPath)))
	}
	s.WriteString("\n\n")

	s.WriteString("What would you like to do?\n\n")

	items := []string{"Backup databases", "Restore a database", "Manage databases", "Manage rclone destinations", "Exit"}
	for i, item := range items {
		cursor := "  "
		if m.cursor == i {
			cursor = cursorStyle.Render("▸ ")
			item = selectedStyle.Render(item)
		}
		s.WriteString(fmt.Sprintf("%s%s\n", cursor, item))
	}

	return s.String()
}

func (m model) renderBackupSelect() string {
	var s strings.Builder
	s.WriteString("Select databases to backup:\n\n")

	// Filter input
	if m.backupFilter != "" {
		s.WriteString(fmt.Sprintf("Filter: %s\n\n", selectedStyle.Render(m.backupFilter)))
	} else {
		s.WriteString(dimStyle.Render("Type to filter..."))
		s.WriteString("\n\n")
	}

	if len(m.backupFilteredList) == 0 {
		s.WriteString(dimStyle.Render("  No matching databases found."))
		s.WriteString("\n")
	} else {
		// Show databases with scrolling
		maxVisible := 10
		start, end := calcScrollWindow(m.cursor, len(m.backupFilteredList), maxVisible)

		// Scroll indicator if there are items above
		if start > 0 {
			s.WriteString(dimStyle.Render(fmt.Sprintf("↑ %d more above", start)))
			s.WriteString("\n\n")
		}

		for i := start; i < end; i++ {
			name := m.backupFilteredList[i]
			cursor := "  "
			if m.cursor == i {
				cursor = cursorStyle.Render("▸ ")
			}

			check := "[ ]"
			if m.selected[name] {
				check = checkStyle.Render("[✓]")
			}

			db := m.cfg.Databases[name]
			line := fmt.Sprintf("%s %s %s", check, name, dimStyle.Render(fmt.Sprintf("(%s)", db.Type)))
			if m.cursor == i {
				line = selectedStyle.Render(fmt.Sprintf("%s %s", check, name)) + " " + dimStyle.Render(fmt.Sprintf("(%s)", db.Type))
			}
			s.WriteString(fmt.Sprintf("%s%s\n", cursor, line))
		}

		// Scroll indicator if there are items below
		if end < len(m.backupFilteredList) {
			s.WriteString("\n")
			s.WriteString(dimStyle.Render(fmt.Sprintf("↓ %d more below", len(m.backupFilteredList)-end)))
		}

		// Show count
		s.WriteString("\n")
		if m.backupFilter != "" {
			s.WriteString(dimStyle.Render(fmt.Sprintf("Showing %d of %d databases", len(m.backupFilteredList), len(m.dbNames))))
		} else {
			s.WriteString(dimStyle.Render(fmt.Sprintf("%d databases", len(m.dbNames))))
		}
		s.WriteString("\n")
	}

	// Retention toggle (index = len(backupFilteredList))
	s.WriteString("\n")
	retentionIdx := len(m.backupFilteredList)
	cursor := "  "
	if m.cursor == retentionIdx {
		cursor = cursorStyle.Render("▸ ")
	}
	check := "[ ]"
	if !m.skipRetention {
		check = checkStyle.Render("[✓]")
	}
	retentionLabel := fmt.Sprintf("%s Apply retention policy", check)
	if m.cursor == retentionIdx {
		retentionLabel = selectedStyle.Render(fmt.Sprintf("%s Apply retention policy", check))
	}
	s.WriteString(fmt.Sprintf("%s%s\n", cursor, retentionLabel))

	// Dry-run toggle (index = len(backupFilteredList) + 1)
	dryRunIdx := retentionIdx + 1
	cursor = "  "
	if m.cursor == dryRunIdx {
		cursor = cursorStyle.Render("▸ ")
	}
	check = "[ ]"
	if m.dryRun {
		check = checkStyle.Render("[✓]")
	}
	dryRunLabel := fmt.Sprintf("%s Dry run (dump only, skip upload)", check)
	if m.cursor == dryRunIdx {
		dryRunLabel = selectedStyle.Render(fmt.Sprintf("%s Dry run (dump only, skip upload)", check))
	}
	s.WriteString(fmt.Sprintf("%s%s\n", cursor, dryRunLabel))

	// Run Backup button (index = len(backupFilteredList) + 2)
	s.WriteString("\n")
	runLabel := "▶ Run Backup"
	cursor = "  "
	if m.cursor == dryRunIdx+1 {
		cursor = cursorStyle.Render("▸ ")
		runLabel = selectedStyle.Render(runLabel)
	}
	s.WriteString(fmt.Sprintf("%s%s\n", cursor, runLabel))

	return s.String()
}

func (m model) renderRetentionPreCheck() string {
	var s strings.Builder
	s.WriteString("Checking retention policies...\n\n")
	s.WriteString(fmt.Sprintf("  %s Scanning backup destinations\n", m.spinner.View()))
	return s.String()
}

func (m model) renderRetentionPreConfirm() string {
	var s strings.Builder

	// Count total files and build list of DBs with files to delete
	totalFiles := 0
	var dbsWithFiles []string
	for _, name := range m.backupQueue {
		files := m.retentionPlan[name]
		if len(files) > 0 {
			totalFiles += len(files)
			dbsWithFiles = append(dbsWithFiles, name)
		}
	}

	s.WriteString(fmt.Sprintf("Retention policy will delete %d backup(s):\n\n", totalFiles))

	// Calculate page bounds (5 DBs per page)
	perPage := 5
	totalPages := (len(dbsWithFiles) + perPage - 1) / perPage
	start := m.retentionDBPage * perPage
	end := start + perPage
	if end > len(dbsWithFiles) {
		end = len(dbsWithFiles)
	}

	// Show page indicator if there are multiple pages
	if totalPages > 1 {
		s.WriteString(dimStyle.Render(fmt.Sprintf("Page %d/%d", m.retentionDBPage+1, totalPages)))
		s.WriteString("\n\n")
	}

	// Show files grouped by database (only current page)
	maxFilesPerDB := 4
	for _, name := range dbsWithFiles[start:end] {
		files := m.retentionPlan[name]

		s.WriteString(selectedStyle.Render(name))
		s.WriteString("\n")

		for i, f := range files {
			if i >= maxFilesPerDB {
				s.WriteString(dimStyle.Render(fmt.Sprintf("  ... and %d more", len(files)-maxFilesPerDB)))
				s.WriteString("\n")
				break
			}
			sizeStr := fmt.Sprintf("%.2f MB", float64(f.Size)/(1024*1024))
			s.WriteString(fmt.Sprintf("  • %s %s\n", f.Name, dimStyle.Render("("+sizeStr+")")))
		}
	}

	s.WriteString("\n")

	items := []string{"Yes, delete old backups", "No, keep all backups"}
	for i, item := range items {
		cursor := "  "
		if m.cursor == i {
			cursor = cursorStyle.Render("▸ ")
			item = selectedStyle.Render(item)
		}
		s.WriteString(fmt.Sprintf("%s%s\n", cursor, item))
	}

	return s.String()
}

func (m model) renderBackupRunning() string {
	var s strings.Builder

	// Count progress
	var done, total int
	for _, state := range m.backupStates {
		total++
		if state.done {
			done++
		}
	}

	// Header with progress
	if done == total {
		s.WriteString(fmt.Sprintf("Backup complete: %d / %d databases backed up\n\n", done, total))
	} else {
		s.WriteString(fmt.Sprintf("Running backups: %d / %d databases backed up\n\n", done, total))
	}

	// Calculate visible window (show 5 databases at a time)
	maxVisible := 5
	start := 0
	if m.cursor >= maxVisible {
		start = m.cursor - maxVisible + 1
	}
	end := start + maxVisible
	if end > len(m.backupQueue) {
		end = len(m.backupQueue)
	}

	// Scroll indicator if there are items above
	if start > 0 {
		s.WriteString(dimStyle.Render(fmt.Sprintf("↑ %d more above", start)))
		s.WriteString("\n\n")
	}

	// Render visible databases
	for i := start; i < end; i++ {
		dbName := m.backupQueue[i]
		state := m.backupStates[dbName]
		if state == nil {
			continue
		}

		// Add blank line between DBs (except first visible)
		if i > start {
			s.WriteString("\n")
		}

		// DB name with cursor indicator
		cursor := "  "
		if m.cursor == i {
			cursor = cursorStyle.Render("▸ ")
		}
		s.WriteString(fmt.Sprintf("%s%s\n", cursor, selectedStyle.Render(truncateString(dbName, 60))))

		// Show completed steps
		for _, entry := range state.logs {
			if entry.IsError {
				s.WriteString(fmt.Sprintf("    %s %s\n", errorStyle.Render("✗"), errorStyle.Render(entry.Message)))
			} else if entry.IsSkipped {
				s.WriteString(fmt.Sprintf("    %s %s\n", dimStyle.Render("○"), dimStyle.Render(entry.Message)))
			} else {
				s.WriteString(fmt.Sprintf("    %s %s\n", successStyle.Render("✓"), entry.Message))
			}
		}

		// Show current step with spinner (if not done)
		if !state.done && state.currentStep != stepIdle {
			s.WriteString(fmt.Sprintf("    %s %s...\n", m.spinner.View(), state.currentStep.String()))

			// Show progress bar for upload step
			if state.currentStep == stepUploading && state.uploadBytesTotal > 0 {
				var pct float64
				if state.uploadBytesTotal > 0 {
					pct = float64(state.uploadBytesDone) / float64(state.uploadBytesTotal)
				}

				// Progress bar
				s.WriteString("       ")
				s.WriteString(m.progressBar.ViewAs(pct))
				s.WriteString("\n")

				// Size and speed info
				s.WriteString(fmt.Sprintf("       %s / %s",
					formatFileSize(state.uploadBytesDone),
					formatFileSize(state.uploadBytesTotal)))
				if state.uploadSpeed > 0 {
					s.WriteString(fmt.Sprintf(" • %s/s", formatFileSize(int64(state.uploadSpeed))))
				}
				s.WriteString("\n")
			}
		}
	}

	// Scroll indicator if there are items below
	if end < len(m.backupQueue) {
		s.WriteString(dimStyle.Render(fmt.Sprintf("\n  ↓ %d more below", len(m.backupQueue)-end)))
		s.WriteString("\n")
	}

	return s.String()
}

func (m model) renderRestoreRunning() string {
	var s strings.Builder

	// Header
	s.WriteString(fmt.Sprintf("Restoring to %s\n", selectedStyle.Render(m.selectedDB)))

	// Show completed steps
	for _, entry := range m.restoreLogs {
		if entry.IsError {
			s.WriteString(fmt.Sprintf("  %s %s\n", errorStyle.Render("✗"), errorStyle.Render(entry.Message)))
		} else {
			s.WriteString(fmt.Sprintf("  %s %s\n", successStyle.Render("✓"), entry.Message))
		}
	}

	// Show current step with spinner
	if m.restoreStep != restoreStepIdle {
		stepStr := m.restoreStep.String()
		s.WriteString(fmt.Sprintf("  %s %s...\n", m.spinner.View(), stepStr))

		// Show progress bar for download step
		if m.restoreStep == restoreStepDownloading && m.selectedFileSize > 0 {
			// Calculate progress percentage
			var pct float64
			if m.selectedFileSize > 0 {
				pct = float64(m.downloadBytesDone) / float64(m.selectedFileSize)
			}

			// Progress bar
			s.WriteString("     ")
			s.WriteString(m.progressBar.ViewAs(pct))
			s.WriteString("\n")

			// Size and speed info
			s.WriteString(fmt.Sprintf("     %s / %s",
				formatFileSize(m.downloadBytesDone),
				formatFileSize(m.selectedFileSize)))
			if m.downloadSpeed > 0 {
				s.WriteString(fmt.Sprintf(" • %s/s", formatFileSize(int64(m.downloadSpeed))))
			}
			s.WriteString("\n")
		}
	}

	return s.String()
}

// truncateString truncates a string to maxLen, adding "..." if truncated
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

// formatFileSize formats a file size in bytes to a human-readable string
func formatFileSize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}

func (m model) renderRestoreDBSelect() string {
	var s strings.Builder
	s.WriteString("Select database to restore:\n\n")

	// Filter input
	if m.restoreDBFilter != "" {
		s.WriteString(fmt.Sprintf("Filter: %s\n\n", selectedStyle.Render(m.restoreDBFilter)))
	} else {
		s.WriteString(dimStyle.Render("Type to filter..."))
		s.WriteString("\n\n")
	}

	if len(m.restoreDBFilteredList) == 0 {
		s.WriteString(dimStyle.Render("  No matching databases found."))
		s.WriteString("\n")
	} else {
		// Show databases with scrolling
		maxVisible := 10
		start := 0
		if m.cursor >= maxVisible {
			start = m.cursor - maxVisible + 1
		}
		end := start + maxVisible
		if end > len(m.restoreDBFilteredList) {
			end = len(m.restoreDBFilteredList)
		}

		for i := start; i < end; i++ {
			name := m.restoreDBFilteredList[i]
			cursor := "  "
			db := m.cfg.Databases[name]
			line := fmt.Sprintf("%s %s", name, dimStyle.Render(fmt.Sprintf("(%s)", db.Type)))
			if m.cursor == i {
				cursor = cursorStyle.Render("▸ ")
				line = selectedStyle.Render(name) + " " + dimStyle.Render(fmt.Sprintf("(%s)", db.Type))
			}
			s.WriteString(fmt.Sprintf("%s%s\n", cursor, line))
		}

		// Show count
		s.WriteString("\n")
		if m.restoreDBFilter != "" {
			s.WriteString(dimStyle.Render(fmt.Sprintf("Showing %d of %d databases", len(m.restoreDBFilteredList), len(m.dbNames))))
		} else {
			s.WriteString(dimStyle.Render(fmt.Sprintf("%d databases", len(m.dbNames))))
		}
		s.WriteString("\n")
	}

	return s.String()
}

func (m model) renderRestoreSourceSelect() string {
	var s strings.Builder
	s.WriteString(fmt.Sprintf("Restore %s from:\n\n", selectedStyle.Render(m.selectedDB)))

	items := []string{"Remote backup", "Local file"}
	for i, item := range items {
		cursor := "  "
		if m.cursor == i {
			cursor = cursorStyle.Render("▸ ")
			item = selectedStyle.Render(item)
		}
		s.WriteString(fmt.Sprintf("%s%s\n", cursor, item))
	}

	return s.String()
}

func (m model) renderRestoreLocalInput() string {
	var s strings.Builder
	s.WriteString(fmt.Sprintf("Restore %s from local file:\n\n", selectedStyle.Render(m.selectedDB)))
	if m.restorePathForm != nil {
		s.WriteString(m.restorePathForm.View())
	}
	return s.String()
}

func (m model) renderRestoreFileSelect() string {
	var s strings.Builder
	s.WriteString(fmt.Sprintf("Select backup to restore for %s:\n\n", selectedStyle.Render(m.selectedDB)))

	// Show spinner while loading
	if m.backupFilesLoading {
		s.WriteString(m.spinner.View())
		s.WriteString(" Loading backups...\n")
		return s.String()
	}

	// Filter input
	if m.restoreFileFilter != "" {
		s.WriteString(fmt.Sprintf("Filter: %s\n\n", selectedStyle.Render(m.restoreFileFilter)))
	} else {
		s.WriteString(dimStyle.Render("Type to filter..."))
		s.WriteString("\n\n")
	}

	if len(m.backupFiles) == 0 {
		s.WriteString(dimStyle.Render("  No backups found\n"))
		return s.String()
	}

	if len(m.restoreFileFilteredList) == 0 {
		s.WriteString(dimStyle.Render("  No matching backups found."))
		s.WriteString("\n")
	} else {
		// Show files with scrolling
		maxVisible := 10
		start := 0
		if m.cursor >= maxVisible {
			start = m.cursor - maxVisible + 1
		}
		end := start + maxVisible
		if end > len(m.restoreFileFilteredList) {
			end = len(m.restoreFileFilteredList)
		}

		for i := start; i < end; i++ {
			f := m.restoreFileFilteredList[i]
			cursor := "  "
			line := fmt.Sprintf("%s  %8.2f MB  %s", f.ModTime.Format("2006-01-02 15:04"), float64(f.Size)/(1024*1024), f.Name)
			if m.cursor == i {
				cursor = cursorStyle.Render("▸ ")
				line = selectedStyle.Render(line)
			}
			s.WriteString(fmt.Sprintf("%s%s\n", cursor, line))
		}

		// Show count
		s.WriteString("\n")
		if m.restoreFileFilter != "" {
			s.WriteString(dimStyle.Render(fmt.Sprintf("Showing %d of %d backups", len(m.restoreFileFilteredList), len(m.backupFiles))))
		} else {
			s.WriteString(dimStyle.Render(fmt.Sprintf("%d backups", len(m.backupFiles))))
		}
		s.WriteString("\n")
	}

	return s.String()
}

func (m model) renderRestoreConfirm() string {
	var s strings.Builder

	// Get file size
	var fileSize int64
	if m.isLocalRestore {
		if stat, err := os.Stat(m.selectedFile); err == nil {
			fileSize = stat.Size()
		}
	} else {
		for _, f := range m.backupFiles {
			if f.Name == m.selectedFile {
				fileSize = f.Size
				break
			}
		}
	}

	s.WriteString(fmt.Sprintf("Restore to %s?\n\n", selectedStyle.Render(m.selectedDB)))
	s.WriteString(fmt.Sprintf("  File: %s\n", m.selectedFile))
	if fileSize > 0 {
		s.WriteString(fmt.Sprintf("  Size: %s\n", formatFileSize(fileSize)))
	}
	s.WriteString("\n")
	s.WriteString(errorStyle.Render("⚠ This will overwrite the current database!"))
	s.WriteString("\n\n")

	items := []string{"Yes, restore", "No, go back"}
	for i, item := range items {
		cursor := "  "
		if m.cursor == i {
			cursor = cursorStyle.Render("▸ ")
			item = selectedStyle.Render(item)
		}
		s.WriteString(fmt.Sprintf("%s%s\n", cursor, item))
	}

	return s.String()
}

func (m model) renderDone() string {
	var s strings.Builder

	if m.err != nil {
		s.WriteString(errorStyle.Render("Error: " + m.err.Error()))
		s.WriteString("\n\n")
	}

	for _, log := range m.logs {
		s.WriteString(log + "\n")
	}

	return s.String()
}

func (m model) renderAddDBType() string {
	var s strings.Builder
	s.WriteString("Select database type:\n\n")

	types := []struct{ name, desc string }{
		{"File", "SQLite or any file backup"},
		{"MySQL", "MySQL or MariaDB database"},
		{"PostgreSQL", "PostgreSQL database"},
	}

	for i, t := range types {
		cursor := "  "
		line := fmt.Sprintf("%s %s", t.name, dimStyle.Render(t.desc))
		if m.cursor == i {
			cursor = cursorStyle.Render("▸ ")
			line = selectedStyle.Render(t.name) + " " + dimStyle.Render(t.desc)
		}
		s.WriteString(fmt.Sprintf("%s%s\n", cursor, line))
	}

	return s.String()
}

func (m model) renderConfirmExit() string {
	var s strings.Builder
	s.WriteString("Exit form?\n\n")

	items := []string{"Yes, go back", "No, continue editing"}
	for i, item := range items {
		cursor := "  "
		if m.cursor == i {
			cursor = cursorStyle.Render("▸ ")
			item = selectedStyle.Render(item)
		}
		s.WriteString(fmt.Sprintf("%s%s\n", cursor, item))
	}

	return s.String()
}

func (m model) renderDBList() string {
	var s strings.Builder
	s.WriteString("Manage databases:\n\n")

	// Filter input
	if m.dbFilter != "" {
		s.WriteString(fmt.Sprintf("Filter: %s\n\n", selectedStyle.Render(m.dbFilter)))
	} else {
		s.WriteString(dimStyle.Render("Type to filter..."))
		s.WriteString("\n\n")
	}

	if len(m.dbNames) == 0 {
		s.WriteString(dimStyle.Render("  No databases configured"))
		s.WriteString("\n\n")
	} else if len(m.dbFilteredList) == 0 {
		s.WriteString(dimStyle.Render("  No matching databases found."))
		s.WriteString("\n\n")
	} else {
		// Show databases with scrolling
		maxVisible := 10
		start, end := calcScrollWindow(m.cursor, len(m.dbFilteredList), maxVisible)

		// Scroll indicator if there are items above
		if start > 0 {
			s.WriteString(dimStyle.Render(fmt.Sprintf("↑ %d more above", start)))
			s.WriteString("\n\n")
		}

		for i := start; i < end; i++ {
			name := m.dbFilteredList[i]
			db := m.cfg.Databases[name]
			cursor := "  "
			line := fmt.Sprintf("%s %s", name, dimStyle.Render(fmt.Sprintf("(%s)", db.Type)))
			if m.cursor == i {
				cursor = cursorStyle.Render("▸ ")
				line = selectedStyle.Render(name) + " " + dimStyle.Render(fmt.Sprintf("(%s)", db.Type))
			}
			s.WriteString(fmt.Sprintf("%s%s\n", cursor, line))
		}

		// Scroll indicator if there are items below
		if end < len(m.dbFilteredList) {
			s.WriteString("\n")
			s.WriteString(dimStyle.Render(fmt.Sprintf("↓ %d more below", len(m.dbFilteredList)-end)))
		}

		// Show count
		s.WriteString("\n")
		if m.dbFilter != "" {
			s.WriteString(dimStyle.Render(fmt.Sprintf("Showing %d of %d databases", len(m.dbFilteredList), len(m.dbNames))))
		} else {
			s.WriteString(dimStyle.Render(fmt.Sprintf("%d databases", len(m.dbNames))))
		}
		s.WriteString("\n")
	}

	// Add new database option
	s.WriteString("\n")
	addIdx := len(m.dbFilteredList)
	cursor := "  "
	addLabel := "+ Add new database"
	if m.cursor == addIdx {
		cursor = cursorStyle.Render("▸ ")
		addLabel = selectedStyle.Render(addLabel)
	}
	s.WriteString(fmt.Sprintf("%s%s\n", cursor, addLabel))

	return s.String()
}

func (m model) renderDBActions() string {
	var s strings.Builder
	db := m.cfg.Databases[m.editingDB]
	s.WriteString(fmt.Sprintf("Database: %s %s\n\n", selectedStyle.Render(m.editingDB), dimStyle.Render(fmt.Sprintf("(%s)", db.Type))))

	items := []string{"Edit", "Test connection", "Delete", "Back"}
	for i, item := range items {
		cursor := "  "
		if m.cursor == i {
			cursor = cursorStyle.Render("▸ ")
			item = selectedStyle.Render(item)
		}
		s.WriteString(fmt.Sprintf("%s%s\n", cursor, item))
	}

	return s.String()
}

func (m model) renderDBTest() string {
	var s strings.Builder

	db := m.cfg.Databases[m.editingDB]
	s.WriteString(fmt.Sprintf("Testing %s\n\n", selectedStyle.Render(m.editingDB)))

	if m.testRunning {
		s.WriteString(m.spinner.View())
		s.WriteString(" Testing...\n")
	}

	// Show connection test result (for mysql/postgres)
	if db.Type == "mysql" || db.Type == "postgres" {
		if m.testConnResult != "" {
			s.WriteString("Connection: ")
			s.WriteString(m.testConnResult)
			s.WriteString("\n")
		} else if m.testRunning {
			s.WriteString("Connection: testing...\n")
		}
	}

	// Show destination test result
	if m.testDestResult != "" {
		s.WriteString("Destination: ")
		s.WriteString(m.testDestResult)
		s.WriteString("\n")
	} else if m.testRunning && m.testConnResult != "" {
		s.WriteString("Destination: testing...\n")
	}

	if !m.testRunning {
		s.WriteString("\n")
		s.WriteString(dimStyle.Render("Press any key to continue"))
	}

	return s.String()
}

func (m model) renderDeleteConfirm() string {
	var s strings.Builder
	s.WriteString(fmt.Sprintf("Delete database %s?\n\n", errorStyle.Render(m.editingDB)))
	s.WriteString(errorStyle.Render("⚠ This will remove the database configuration."))
	s.WriteString("\n")
	s.WriteString(dimStyle.Render("  (Existing backups will not be deleted)"))
	s.WriteString("\n\n")

	items := []string{"Yes, delete", "No, go back"}
	for i, item := range items {
		cursor := "  "
		if m.cursor == i {
			cursor = cursorStyle.Render("▸ ")
			item = selectedStyle.Render(item)
		}
		s.WriteString(fmt.Sprintf("%s%s\n", cursor, item))
	}

	return s.String()
}

func (m model) saveNewDatabase() (tea.Model, tea.Cmd) {
	// Build the database config using form field values
	// (validation is done before calling this function via validateForm())
	db := config.Database{
		Type:        m.addDBType,
		Dest:        expandDest(m.formData.dest),
		Compression: m.formData.compression,
	}

	if db.Compression == "" {
		db.Compression = "none"
	}

	switch m.addDBType {
	case "file":
		db.Path = expandPath(m.formData.path)
	case "mysql", "postgres":
		db.Host = m.formData.host
		db.User = m.formData.user
		db.Password = m.formData.password
		db.Database = m.formData.database
		if m.formData.port != "" {
			fmt.Sscanf(m.formData.port, "%d", &db.Port)
		}
	}

	// Parse retention settings
	if m.formData.keepLast != "" {
		fmt.Sscanf(m.formData.keepLast, "%d", &db.Retention.KeepLast)
	}
	if m.formData.keepDays != "" {
		fmt.Sscanf(m.formData.keepDays, "%d", &db.Retention.KeepDays)
	}
	if m.formData.maxSizeMB != "" {
		fmt.Sscanf(m.formData.maxSizeMB, "%d", &db.Retention.MaxSizeMB)
	}

	// Add to config
	m.cfg.Databases[m.formData.name] = db

	// Save config file
	if err := m.cfg.Save(); err != nil {
		m.err = fmt.Errorf("saving config: %w", err)
		m.view = viewDone
		return m, nil
	}

	// Update dbNames list
	m.dbNames = append(m.dbNames, m.formData.name)
	sort.Strings(m.dbNames)
	m.selected[m.formData.name] = true
	m.filterDatabases(m.dbFilter) // Refresh filtered list

	// Show success (include test results if available - already styled)
	m.logs = []string{successStyle.Render(fmt.Sprintf("Database '%s' added successfully!", m.formData.name))}
	if m.testConnResult != "" {
		m.logs = append(m.logs, m.testConnResult)
	}
	if m.testDestResult != "" {
		m.logs = append(m.logs, m.testDestResult)
	}
	m.logs = append(m.logs, dimStyle.Render(fmt.Sprintf("Config saved to %s", m.cfg.Path())))
	m.view = viewDone

	return m, nil
}

func (m model) saveEditedDatabase() (tea.Model, tea.Cmd) {
	// Build the database config using form field values
	db := config.Database{
		Type:        m.addDBType,
		Dest:        expandDest(m.formData.dest),
		Compression: m.formData.compression,
	}

	if db.Compression == "" {
		db.Compression = "none"
	}

	switch m.addDBType {
	case "file":
		db.Path = expandPath(m.formData.path)
	case "mysql", "postgres":
		db.Host = m.formData.host
		db.User = m.formData.user
		db.Password = m.formData.password
		db.Database = m.formData.database
		if m.formData.port != "" {
			fmt.Sscanf(m.formData.port, "%d", &db.Port)
		}
	}

	// Parse retention settings
	if m.formData.keepLast != "" {
		fmt.Sscanf(m.formData.keepLast, "%d", &db.Retention.KeepLast)
	}
	if m.formData.keepDays != "" {
		fmt.Sscanf(m.formData.keepDays, "%d", &db.Retention.KeepDays)
	}
	if m.formData.maxSizeMB != "" {
		fmt.Sscanf(m.formData.maxSizeMB, "%d", &db.Retention.MaxSizeMB)
	}

	// Check if name changed
	oldName := m.editingDB
	newName := m.formData.name

	if oldName != newName {
		// Delete old entry, add new
		delete(m.cfg.Databases, oldName)
		m.cfg.Databases[newName] = db

		// Update dbNames list
		for i, name := range m.dbNames {
			if name == oldName {
				m.dbNames[i] = newName
				break
			}
		}
		sort.Strings(m.dbNames)

		// Update selected map
		if m.selected[oldName] {
			delete(m.selected, oldName)
			m.selected[newName] = true
		}
	} else {
		// Just update the entry
		m.cfg.Databases[newName] = db
	}

	// Save config file
	if err := m.cfg.Save(); err != nil {
		m.err = fmt.Errorf("saving config: %w", err)
		m.view = viewDone
		return m, nil
	}

	// Show success (include test results if available - already styled)
	m.logs = []string{successStyle.Render(fmt.Sprintf("Database '%s' updated successfully!", newName))}
	if m.testConnResult != "" {
		m.logs = append(m.logs, m.testConnResult)
	}
	if m.testDestResult != "" {
		m.logs = append(m.logs, m.testDestResult)
	}
	m.logs = append(m.logs, dimStyle.Render(fmt.Sprintf("Config saved to %s", m.cfg.Path())))
	m.view = viewDone
	m.editingDB = ""

	return m, nil
}

func (m model) deleteDatabase() (tea.Model, tea.Cmd) {
	name := m.editingDB

	// Delete from config
	delete(m.cfg.Databases, name)

	// Save config file
	if err := m.cfg.Save(); err != nil {
		m.err = fmt.Errorf("saving config: %w", err)
		m.view = viewDone
		return m, nil
	}

	// Update dbNames list
	for i, n := range m.dbNames {
		if n == name {
			m.dbNames = append(m.dbNames[:i], m.dbNames[i+1:]...)
			break
		}
	}

	// Remove from selected map
	delete(m.selected, name)

	// Refresh filtered list
	m.filterDatabases(m.dbFilter)

	// Show success
	m.logs = []string{successStyle.Render(fmt.Sprintf("Database '%s' deleted.", name))}
	m.logs = append(m.logs, dimStyle.Render(fmt.Sprintf("Config saved to %s", m.cfg.Path())))
	m.view = viewDone
	m.editingDB = ""

	return m, nil
}

// Messages

// retentionPreCheckMsg is sent when retention pre-check completes
type retentionPreCheckMsg struct {
	plan map[string][]storage.RemoteFile // dbName -> files to delete
	err  error
}

// backupStepDoneMsg is sent when a backup step completes
type backupStepDoneMsg struct {
	dbName  string
	step    backupStep
	result  *backup.Result // set after dump step
	message string         // status message
	err     error
	skipped bool // true if step was skipped (e.g., retention skipped)
}

type fileListMsg struct {
	files []storage.RemoteFile
	err   error
}

// restoreStepDoneMsg is sent when a restore step completes
type restoreStepDoneMsg struct {
	step      restoreStep
	message   string
	localPath string // set after download step (path to downloaded file)
	err       error
	done      bool // true if restore is complete
}

// downloadProgressMsg is sent periodically during file download with progress info
type downloadProgressMsg struct {
	bytesDone  int64
	bytesTotal int64
	speed      float64
	done       bool
	err        error
}

// uploadProgressMsg is sent periodically during file upload with progress info
type uploadProgressMsg struct {
	dbName     string
	bytesDone  int64
	bytesTotal int64
	speed      float64
	done       bool
	err        error
}

// startUploadMsg triggers upload with progress tracking
type startUploadMsg struct {
	dbName     string
	backupPath string
	dest       string
}

// testResultMsg is sent when a connection/destination test completes
type testResultMsg struct {
	testType string // "connection" or "destination"
	success  bool
	message  string
}

// rcloneTestResultMsg is sent when an rclone remote connection test completes
type rcloneTestResultMsg struct {
	success bool
	message string
}

// dbTestResultMsg is sent when a database test completes
type dbTestResultMsg struct {
	testType string // "connection" or "destination"
	success  bool
	message  string
	done     bool // true when all tests are complete
}

// oauthCompleteMsg is sent when OAuth authentication completes
type oauthCompleteMsg struct {
	err    error
	isEdit bool
}

// Commands

// allBackupsDone returns true if all backups have completed
func (m model) allBackupsDone() bool {
	if len(m.backupStates) == 0 {
		return false
	}
	for _, state := range m.backupStates {
		if !state.done {
			return false
		}
	}
	return true
}

// startBackups initializes and starts the backup process for all DBs in parallel
func (m model) startBackups() (tea.Model, tea.Cmd) {
	m.backupStates = make(map[string]*dbBackupState)
	m.view = viewBackupRunning

	// Initialize state for each DB and start all dumps in parallel
	var cmds []tea.Cmd
	cmds = append(cmds, m.spinner.Tick)

	for _, name := range m.backupQueue {
		m.backupStates[name] = &dbBackupState{
			currentStep: stepDumping,
		}
		cmds = append(cmds, m.runBackupStepFor(name))
	}

	return m, tea.Batch(cmds...)
}

// runRetentionPreCheck checks retention policies for all selected databases
func (m model) runRetentionPreCheck() tea.Cmd {
	// Capture values needed inside the closure
	queue := m.backupQueue
	databases := make(map[string]config.Database)
	for _, name := range queue {
		databases[name] = m.cfg.Databases[name]
	}

	return func() tea.Msg {
		ctx := context.Background()
		plan := make(map[string][]storage.RemoteFile)

		for _, name := range queue {
			db := databases[name]
			if db.Retention.KeepLast == 0 && db.Retention.KeepDays == 0 && db.Retention.MaxSizeMB == 0 {
				continue
			}

			files, err := storage.List(ctx, db.Dest)
			if err != nil {
				// Skip this database on error, don't fail the whole check
				continue
			}

			// pendingBackups=1 because we're about to create a new backup
			toDelete := retention.Apply(ctx, files, name, db.Retention, 1)
			if len(toDelete) > 0 {
				plan[name] = toDelete
			}
		}

		return retentionPreCheckMsg{plan: plan}
	}
}

// runBackupStepFor runs the current step for a specific database
func (m model) runBackupStepFor(name string) tea.Cmd {
	state := m.backupStates[name]
	if state == nil || state.done {
		return nil
	}

	db := m.cfg.Databases[name]
	step := state.currentStep

	// Capture values needed inside the closure to avoid race conditions
	skipRetention := m.skipRetention
	dryRun := m.dryRun
	var backupPath string
	if state.result != nil {
		backupPath = state.result.Path
	}
	// Get pre-calculated retention files for this database
	retentionFiles := m.retentionPlan[name]

	return func() tea.Msg {
		ctx := context.Background()

		switch step {
		case stepDumping:
			result, err := backup.Run(name, db)
			if err != nil {
				return backupStepDoneMsg{
					dbName: name,
					step:   stepDumping,
					err:    err,
				}
			}
			return backupStepDoneMsg{
				dbName:  name,
				step:    stepDumping,
				result:  result,
				message: fmt.Sprintf("Dumped %s (%.2f MB)", result.Filename, float64(result.Size)/(1024*1024)),
			}

		case stepUploading:
			if dryRun {
				return backupStepDoneMsg{
					dbName:  name,
					step:    stepUploading,
					message: fmt.Sprintf("Upload skipped (dry-run), file at %s", backupPath),
					skipped: true,
				}
			}

			if backupPath == "" {
				return backupStepDoneMsg{
					dbName: name,
					step:   stepUploading,
					err:    fmt.Errorf("no backup result to upload"),
				}
			}

			// Return a message to trigger upload with progress tracking
			return startUploadMsg{
				dbName:     name,
				backupPath: backupPath,
				dest:       db.Dest,
			}

		case stepRetention:
			var message string
			var skipped bool

			if dryRun {
				message = "Retention skipped (dry-run)"
				skipped = true
			} else if skipRetention {
				message = "Retention skipped"
				skipped = true
			} else if len(retentionFiles) > 0 {
				// Delete pre-calculated files (user already confirmed)
				var deleted int
				for _, f := range retentionFiles {
					if err := storage.Delete(ctx, db.Dest, f.Name); err == nil {
						deleted++
					}
				}
				message = fmt.Sprintf("Deleted %d old backup(s)", deleted)
			} else if db.Retention.KeepLast > 0 || db.Retention.KeepDays > 0 || db.Retention.MaxSizeMB > 0 {
				message = "No old backups to delete"
				skipped = true
			} else {
				message = "No retention policy"
				skipped = true
			}

			return backupStepDoneMsg{
				dbName:  name,
				step:    stepRetention,
				message: message,
				skipped: skipped,
			}
		}

		return nil
	}
}

// handleBackupStepDone processes a completed backup step and schedules the next one
func (m model) handleBackupStepDone(msg backupStepDoneMsg) (tea.Model, tea.Cmd) {
	state := m.backupStates[msg.dbName]
	if state == nil {
		return m, nil
	}

	// Log the completed step
	entry := backupLogEntry{
		DBName:    msg.dbName,
		Step:      msg.step,
		Message:   msg.message,
		IsError:   msg.err != nil,
		IsSkipped: msg.skipped,
	}
	if msg.err != nil {
		entry.Message = msg.err.Error()
	}
	state.logs = append(state.logs, entry)

	// Handle errors - mark this DB as done
	if msg.err != nil {
		if state.result != nil {
			backup.Cleanup(state.result)
			state.result = nil
		}
		state.done = true
		state.currentStep = stepIdle
		return m, m.checkAllBackupsDone()
	}

	// Save result from dump step for upload
	if msg.step == stepDumping && msg.result != nil {
		state.result = msg.result
	}

	// Advance to next step
	switch msg.step {
	case stepDumping:
		state.currentStep = stepUploading
	case stepUploading:
		// Clean up upload state
		delete(m.uploadStates, msg.dbName)
		state.currentStep = stepRetention
	case stepRetention:
		// Clean up the backup result (skip in dry-run so user can access the file)
		if state.result != nil && !m.dryRun {
			backup.Cleanup(state.result)
			state.result = nil
		}
		state.done = true
		state.currentStep = stepIdle
		return m, m.checkAllBackupsDone()
	}

	// Continue with next step for this DB
	return m, tea.Batch(m.spinner.Tick, m.runBackupStepFor(msg.dbName))
}

// checkAllBackupsDone checks if all backups are complete and transitions to done view
func (m model) checkAllBackupsDone() tea.Cmd {
	allDone := true
	for _, state := range m.backupStates {
		if !state.done {
			allDone = false
			break
		}
	}

	if allDone {
		return func() tea.Msg {
			return allBackupsDoneMsg{}
		}
	}
	return m.spinner.Tick
}

// allBackupsDoneMsg signals all backups are complete
type allBackupsDoneMsg struct{}

func (m model) handleDownloadProgress(msg downloadProgressMsg) (tea.Model, tea.Cmd) {
	// Handle download error
	if msg.err != nil {
		m.err = msg.err
		m.view = viewDone
		m.restoreStep = restoreStepIdle
		m.restoreLogs = append(m.restoreLogs, restoreLogEntry{
			Message: "Downloading backup failed",
			IsError: true,
		})
		m.logs = m.buildRestoreSummaryLogs()
		m.downloadState = nil
		return m, nil
	}

	// Update progress
	m.downloadBytesDone = msg.bytesDone
	m.downloadSpeed = msg.speed

	// If done, the next message will be restoreStepDoneMsg
	// Continue waiting for progress updates
	return m, tea.Batch(m.spinner.Tick, m.waitForDownloadProgress())
}

func (m model) handleUploadProgress(msg uploadProgressMsg) (tea.Model, tea.Cmd) {
	state := m.backupStates[msg.dbName]
	if state == nil {
		return m, nil
	}

	// Handle upload error
	if msg.err != nil {
		// Clean up upload state
		delete(m.uploadStates, msg.dbName)

		// Record error and move to next step
		state.logs = append(state.logs, backupLogEntry{
			DBName:  msg.dbName,
			Step:    stepUploading,
			Message: "Upload failed",
			IsError: true,
		})
		state.done = true
		state.currentStep = stepIdle

		return m, m.checkAllBackupsDone()
	}

	// Update progress
	state.uploadBytesDone = msg.bytesDone
	state.uploadBytesTotal = msg.bytesTotal
	state.uploadSpeed = msg.speed

	// If done, the next message will be backupStepDoneMsg
	// Continue waiting for progress updates
	return m, tea.Batch(m.spinner.Tick, m.waitForUploadProgress(msg.dbName))
}

func (m model) handleRestoreStepDone(msg restoreStepDoneMsg) (tea.Model, tea.Cmd) {
	// Log the completed step
	entry := restoreLogEntry{
		Message: msg.message,
		IsError: msg.err != nil,
	}
	if msg.err != nil {
		// Use generic message for log entry (full error shown separately at top)
		entry.Message = msg.step.String() + " failed"
	}
	m.restoreLogs = append(m.restoreLogs, entry)

	// Handle errors
	if msg.err != nil {
		m.err = msg.err
		m.view = viewDone
		m.restoreStep = restoreStepIdle
		m.logs = m.buildRestoreSummaryLogs()
		return m, nil
	}

	// Save the local path from download step and clean up download state
	if msg.step == restoreStepDownloading && msg.localPath != "" {
		m.restoreLocalPath = msg.localPath
		m.downloadState = nil
	}

	// Check if done
	if msg.done {
		m.view = viewDone
		m.restoreStep = restoreStepIdle
		m.logs = m.buildRestoreSummaryLogs()
		return m, nil
	}

	// Advance to next step
	switch msg.step {
	case restoreStepDownloading:
		m.restoreStep = restoreStepRestoring
	}

	return m, tea.Batch(m.spinner.Tick, m.runRestoreStep())
}

// buildRestoreSummaryLogs converts restore log entries to display strings
func (m model) buildRestoreSummaryLogs() []string {
	var logs []string

	logs = append(logs, selectedStyle.Render(fmt.Sprintf("Restore to %s", m.selectedDB)))

	for _, entry := range m.restoreLogs {
		if entry.IsError {
			logs = append(logs, fmt.Sprintf("  %s %s", errorStyle.Render("✗"), errorStyle.Render(entry.Message)))
		} else {
			logs = append(logs, fmt.Sprintf("  %s %s", successStyle.Render("✓"), entry.Message))
		}
	}

	return logs
}

// buildBackupSummaryLogs converts backup log entries to display strings
func (m model) buildBackupSummaryLogs() []string {
	var logs []string

	for i, dbName := range m.backupQueue {
		state := m.backupStates[dbName]
		if state == nil {
			continue
		}

		// Add blank line between DBs (except first)
		if i > 0 {
			logs = append(logs, "")
		}

		// DB name
		logs = append(logs, selectedStyle.Render(truncateString(dbName, 60)))

		// Steps
		for _, entry := range state.logs {
			if entry.IsError {
				logs = append(logs, fmt.Sprintf("  %s %s", errorStyle.Render("✗"), errorStyle.Render(entry.Message)))
			} else if entry.IsSkipped {
				logs = append(logs, fmt.Sprintf("  %s %s", dimStyle.Render("○"), dimStyle.Render(entry.Message)))
			} else {
				logs = append(logs, fmt.Sprintf("  %s %s", successStyle.Render("✓"), entry.Message))
			}
		}
	}

	return logs
}

func (m model) fetchBackupFiles() tea.Cmd {
	return func() tea.Msg {
		ctx := context.Background()
		db := m.cfg.Databases[m.selectedDB]

		files, err := storage.List(ctx, db.Dest)
		return fileListMsg{files: files, err: err}
	}
}

// startRestore initializes and starts the restore process
func (m model) startRestore() (tea.Model, tea.Cmd) {
	// Guard against double submission
	if m.restoreStep != restoreStepIdle {
		return m, nil
	}

	m.restoreLogs = nil
	m.view = viewRestoreRunning
	m.downloadBytesDone = 0
	m.downloadSpeed = 0
	m.downloadState = nil

	if m.isLocalRestore {
		// Local restore: skip download, go straight to restoring
		m.restoreStep = restoreStepRestoring
		m.restoreLocalPath = m.selectedFile
		return m, tea.Batch(m.spinner.Tick, m.runRestoreStep())
	}

	// Remote restore: start with download
	m.restoreStep = restoreStepDownloading
	m.restoreLocalPath = ""
	m, cmd := m.startDownload()
	return m, tea.Batch(m.spinner.Tick, cmd)
}

// runRestoreStep runs the current step in the restore process
func (m model) runRestoreStep() tea.Cmd {
	db := m.cfg.Databases[m.selectedDB]
	step := m.restoreStep
	localPath := m.restoreLocalPath

	switch step {
	case restoreStepDownloading:
		// Download progress is handled via downloadState which is set up before this is called
		ds := m.downloadState
		if ds == nil {
			return nil
		}
		return m.waitForDownloadProgress()

	case restoreStepRestoring:
		return func() tea.Msg {
			if err := backup.Restore(db, localPath); err != nil {
				return restoreStepDoneMsg{
					step: restoreStepRestoring,
					err:  err,
				}
			}

			return restoreStepDoneMsg{
				step:    restoreStepRestoring,
				message: fmt.Sprintf("Restored to %s", db.Database),
				done:    true,
			}
		}
	}

	return nil
}

// startDownload initializes download state and starts the download goroutine
// Returns the model with downloadState set and a command to wait for progress
func (m model) startDownload() (model, tea.Cmd) {
	db := m.cfg.Databases[m.selectedDB]
	fileName := m.selectedFile
	fileSize := m.selectedFileSize
	remoteDest := db.Dest

	tmpDir, err := createTempDir()
	if err != nil {
		// Return an immediate error
		return m, func() tea.Msg {
			return downloadProgressMsg{err: err, done: true}
		}
	}

	progressCh := make(chan storage.TransferProgress, 10)

	// Store state in heap-allocated struct
	m.downloadState = &downloadState{
		progressCh: progressCh,
		tmpDir:     tmpDir,
		fileName:   fileName,
		fileSize:   fileSize,
	}

	// Start download in a goroutine
	go storage.DownloadWithProgress(context.Background(), remoteDest, fileName, tmpDir, fileSize, progressCh)

	// Return command to wait for first progress update
	return m, m.waitForDownloadProgress()
}

// waitForDownloadProgress waits for the next progress update from the channel
func (m model) waitForDownloadProgress() tea.Cmd {
	ds := m.downloadState
	if ds == nil {
		return nil
	}

	return func() tea.Msg {
		progress, ok := <-ds.progressCh
		if !ok {
			// Channel closed, download complete
			downloadedPath := ds.tmpDir + "/" + ds.fileName
			return restoreStepDoneMsg{
				step:      restoreStepDownloading,
				message:   fmt.Sprintf("Downloaded %s (%s)", ds.fileName, formatFileSize(ds.fileSize)),
				localPath: downloadedPath,
			}
		}

		if progress.Done {
			if progress.Error != nil {
				return downloadProgressMsg{err: progress.Error, done: true}
			}
			downloadedPath := ds.tmpDir + "/" + ds.fileName
			return restoreStepDoneMsg{
				step:      restoreStepDownloading,
				message:   fmt.Sprintf("Downloaded %s (%s)", ds.fileName, formatFileSize(ds.fileSize)),
				localPath: downloadedPath,
			}
		}

		return downloadProgressMsg{
			bytesDone:  progress.BytesDone,
			bytesTotal: progress.BytesTotal,
			speed:      progress.Speed,
			done:       false,
		}
	}
}

// startUploadWithProgress initializes upload state and starts the upload goroutine
func (m model) startUploadWithProgress(dbName, backupPath, dest string) (tea.Model, tea.Cmd) {
	// Get file size for progress tracking
	fileInfo, err := os.Stat(backupPath)
	if err != nil {
		// Return error as backup step done
		return m, func() tea.Msg {
			return backupStepDoneMsg{
				dbName: dbName,
				step:   stepUploading,
				err:    fmt.Errorf("getting file info: %w", err),
			}
		}
	}
	fileSize := fileInfo.Size()

	// Initialize upload states map if needed
	if m.uploadStates == nil {
		m.uploadStates = make(map[string]*uploadState)
	}

	progressCh := make(chan storage.TransferProgress, 10)

	// Store state in heap-allocated struct
	m.uploadStates[dbName] = &uploadState{
		progressCh: progressCh,
		dbName:     dbName,
		fileSize:   fileSize,
	}

	// Initialize progress in backup state
	if state := m.backupStates[dbName]; state != nil {
		state.uploadBytesTotal = fileSize
		state.uploadBytesDone = 0
		state.uploadSpeed = 0
	}

	// Start upload in a goroutine
	go storage.UploadWithProgress(context.Background(), backupPath, dest, fileSize, progressCh)

	// Return command to wait for first progress update
	return m, m.waitForUploadProgress(dbName)
}

// waitForUploadProgress waits for the next progress update from the channel
func (m model) waitForUploadProgress(dbName string) tea.Cmd {
	us := m.uploadStates[dbName]
	if us == nil {
		return nil
	}

	// Capture dest for the completion message
	db := m.cfg.Databases[dbName]
	dest := db.Dest

	return func() tea.Msg {
		progress, ok := <-us.progressCh
		if !ok {
			// Channel closed, upload complete
			return backupStepDoneMsg{
				dbName:  dbName,
				step:    stepUploading,
				message: fmt.Sprintf("Saved to %s", formatDestForDisplay(dest, 50)),
			}
		}

		if progress.Done {
			if progress.Error != nil {
				return uploadProgressMsg{
					dbName: dbName,
					err:    progress.Error,
					done:   true,
				}
			}
			return backupStepDoneMsg{
				dbName:  dbName,
				step:    stepUploading,
				message: fmt.Sprintf("Saved to %s", formatDestForDisplay(dest, 50)),
			}
		}

		return uploadProgressMsg{
			dbName:     dbName,
			bytesDone:  progress.BytesDone,
			bytesTotal: progress.BytesTotal,
			speed:      progress.Speed,
			done:       false,
		}
	}
}

func createTempDir() (string, error) {
	return os.MkdirTemp("", "blobber-restore-")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// === Rclone management views ===

func (m model) renderRcloneList() string {
	var s strings.Builder

	s.WriteString("Rclone Destinations\n\n")

	// Filter input
	if m.rcloneRemoteFilter != "" {
		s.WriteString(fmt.Sprintf("Filter: %s\n\n", selectedStyle.Render(m.rcloneRemoteFilter)))
	} else {
		s.WriteString(dimStyle.Render("Type to filter..."))
		s.WriteString("\n\n")
	}

	if len(m.rcloneRemotes) == 0 {
		s.WriteString(dimStyle.Render("  No rclone remotes configured."))
		s.WriteString("\n\n")
	} else if len(m.rcloneRemoteFilteredList) == 0 {
		s.WriteString(dimStyle.Render("  No matching remotes found."))
		s.WriteString("\n\n")
	} else {
		// Show remotes with scrolling
		maxVisible := 10
		start, end := calcScrollWindow(m.cursor, len(m.rcloneRemoteFilteredList), maxVisible)

		// Scroll indicator if there are items above
		if start > 0 {
			s.WriteString(dimStyle.Render(fmt.Sprintf("↑ %d more above", start)))
			s.WriteString("\n\n")
		}

		for i := start; i < end; i++ {
			name := m.rcloneRemoteFilteredList[i]
			cursor := "  "
			remoteType := getRcloneRemoteType(name)
			line := fmt.Sprintf("%s %s", name, dimStyle.Render(fmt.Sprintf("(%s)", remoteType)))
			if m.cursor == i {
				cursor = cursorStyle.Render("▸ ")
				line = selectedStyle.Render(name) + " " + dimStyle.Render(fmt.Sprintf("(%s)", remoteType))
			}
			s.WriteString(fmt.Sprintf("%s%s\n", cursor, line))
		}

		// Scroll indicator if there are items below
		if end < len(m.rcloneRemoteFilteredList) {
			s.WriteString("\n")
			s.WriteString(dimStyle.Render(fmt.Sprintf("↓ %d more below", len(m.rcloneRemoteFilteredList)-end)))
		}

		// Show count
		s.WriteString("\n")
		if m.rcloneRemoteFilter != "" {
			s.WriteString(dimStyle.Render(fmt.Sprintf("Showing %d of %d remotes", len(m.rcloneRemoteFilteredList), len(m.rcloneRemotes))))
		} else {
			s.WriteString(dimStyle.Render(fmt.Sprintf("%d remotes", len(m.rcloneRemotes))))
		}
		s.WriteString("\n")
	}

	// Add button
	addIdx := len(m.rcloneRemoteFilteredList)
	addCursor := "  "
	addItem := "+ Add new destination"
	if m.cursor == addIdx {
		addCursor = cursorStyle.Render("▸ ")
		addItem = selectedStyle.Render(addItem)
	}
	s.WriteString("\n")
	s.WriteString(fmt.Sprintf("%s%s\n", addCursor, addItem))

	return s.String()
}

func (m model) renderRcloneActions() string {
	var s strings.Builder

	remoteType := getRcloneRemoteType(m.selectedRemote)
	s.WriteString(fmt.Sprintf("%s %s\n\n", selectedStyle.Render(m.selectedRemote), dimStyle.Render(fmt.Sprintf("(%s)", remoteType))))

	items := []string{"Edit", "Test connection", "Delete", "Back"}
	for i, item := range items {
		cursor := "  "
		if m.cursor == i {
			cursor = cursorStyle.Render("▸ ")
			item = selectedStyle.Render(item)
		}
		s.WriteString(fmt.Sprintf("%s%s\n", cursor, item))
	}

	return s.String()
}

func (m model) renderRcloneAddType() string {
	var s strings.Builder

	s.WriteString("Select Storage Type\n\n")

	// Filter input
	if m.rcloneFilter != "" {
		s.WriteString(fmt.Sprintf("Filter: %s\n\n", selectedStyle.Render(m.rcloneFilter)))
	} else {
		s.WriteString(dimStyle.Render("Type to filter..."))
		s.WriteString("\n\n")
	}

	// Show backends
	maxVisible := 10
	if len(m.rcloneFilteredList) == 0 {
		s.WriteString(dimStyle.Render("No matching backends found."))
		s.WriteString("\n")
	} else {
		// Calculate visible window
		start := 0
		if m.cursor >= maxVisible {
			start = m.cursor - maxVisible + 1
		}
		end := start + maxVisible
		if end > len(m.rcloneFilteredList) {
			end = len(m.rcloneFilteredList)
		}

		for i := start; i < end; i++ {
			ri := m.rcloneFilteredList[i]
			cursor := "  "
			line := fmt.Sprintf("%s %s", ri.Name, dimStyle.Render(truncateText(ri.Description, 50)))
			if m.cursor == i {
				cursor = cursorStyle.Render("▸ ")
				line = selectedStyle.Render(ri.Name) + " " + dimStyle.Render(truncateText(ri.Description, 50))
			}
			s.WriteString(fmt.Sprintf("%s%s\n", cursor, line))
		}

		// Show count
		s.WriteString("\n")
		if m.rcloneFilter != "" {
			s.WriteString(dimStyle.Render(fmt.Sprintf("Showing %d of %d backends", len(m.rcloneFilteredList), len(m.rcloneBackends))))
		} else {
			s.WriteString(dimStyle.Render(fmt.Sprintf("%d backends available", len(m.rcloneBackends))))
		}
		s.WriteString("\n")
	}

	return s.String()
}

func (m model) renderRcloneAddForm() string {
	var s strings.Builder

	if m.selectedBackend != nil {
		// Check if editing (no _name field in form)
		_, isNew := m.rcloneFormValues["_name"]
		if isNew {
			s.WriteString(fmt.Sprintf("Configure %s remote:\n\n", selectedStyle.Render(m.selectedBackend.Name)))
		} else {
			s.WriteString(fmt.Sprintf("Editing %s (%s):\n\n", selectedStyle.Render(m.selectedRemote), m.selectedBackend.Name))
		}

		// Show description
		if m.selectedBackend.Description != "" {
			s.WriteString(dimStyle.Render(truncateText(m.selectedBackend.Description, 70)))
			s.WriteString("\n\n")
		}
	}

	// Show test result if available
	if m.testRunning {
		s.WriteString(fmt.Sprintf("%s Testing connection...\n\n", m.spinner.View()))
	} else if m.rcloneTestResult != "" {
		s.WriteString(m.rcloneTestResult)
		s.WriteString("\n\n")
	}

	if m.rcloneForm != nil {
		s.WriteString(m.rcloneForm.View())
	}

	return s.String()
}

func (m model) renderRcloneDeleteConfirm() string {
	var s strings.Builder

	s.WriteString(fmt.Sprintf("Delete rclone remote %s?\n\n", selectedStyle.Render(m.selectedRemote)))
	s.WriteString("This cannot be undone.\n\n")

	items := []string{"Yes, delete", "No, go back"}
	for i, item := range items {
		cursor := "  "
		if m.cursor == i {
			cursor = cursorStyle.Render("▸ ")
			item = selectedStyle.Render(item)
		}
		s.WriteString(fmt.Sprintf("%s%s\n", cursor, item))
	}

	return s.String()
}

func (m model) renderRcloneTestBucket() string {
	var s strings.Builder

	s.WriteString(fmt.Sprintf("Test %s\n\n", selectedStyle.Render(m.selectedRemote)))
	if m.rcloneTestForm != nil {
		s.WriteString(m.rcloneTestForm.View())
	}

	return s.String()
}

func (m model) renderRcloneTest() string {
	var s strings.Builder

	testPath := m.selectedRemote + ":"
	if m.rcloneTestFormData != nil && m.rcloneTestFormData.bucket != "" {
		testPath = m.selectedRemote + ":" + m.rcloneTestFormData.bucket
	}
	s.WriteString(fmt.Sprintf("Testing %s\n\n", selectedStyle.Render(testPath)))

	if m.rcloneTestResult != "" {
		s.WriteString(m.rcloneTestResult)
		s.WriteString("\n\n")
		s.WriteString(dimStyle.Render("Press any key to continue"))
	} else {
		s.WriteString(m.spinner.View())
		s.WriteString(" Checking connection...\n")
	}

	return s.String()
}

func (m model) renderRcloneOAuth() string {
	var s strings.Builder

	backendName := ""
	if m.selectedBackend != nil {
		backendName = m.selectedBackend.Name
	}
	s.WriteString(fmt.Sprintf("Authenticating with %s\n\n", selectedStyle.Render(backendName)))

	if m.oauthErr != nil {
		s.WriteString(errorStyle.Render("Authentication failed"))
		s.WriteString("\n\n")
		s.WriteString(fmt.Sprintf("Error: %v", m.oauthErr))
		s.WriteString("\n\n")
		s.WriteString(dimStyle.Render("Press Enter or Esc to go back"))
	} else {
		s.WriteString(m.spinner.View())
		s.WriteString(" Waiting for authentication...\n\n")
		s.WriteString(dimStyle.Render("A browser window should have opened.\n"))
		s.WriteString(dimStyle.Render("Complete the authentication there."))
	}

	return s.String()
}

// buildRcloneForm creates a dynamic huh form based on rclone backend options
func (m *model) buildRcloneForm(existingValues map[string]string) *huh.Form {
	m.rcloneFormValues = make(map[string]*string)

	// Build groups with names (titles added at end with page numbers)
	type namedGroup struct {
		name  string
		group *huh.Group
	}
	var namedGroups []namedGroup

	// Collect standard (non-advanced) options
	var standardOpts []fs.Option
	var advancedOpts []fs.Option
	for _, opt := range m.selectedBackend.Options {
		if opt.Hide != 0 {
			continue
		}
		if opt.Advanced {
			advancedOpts = append(advancedOpts, opt)
		} else {
			standardOpts = append(standardOpts, opt)
		}
	}

	// First page: Remote name (if new) + first batch of standard options
	var firstPageFields []huh.Field
	isNewRemote := m.selectedRemote == "" // Adding new vs editing existing
	if isNewRemote {
		// Preserve existing name if we're rebuilding (e.g., for advanced options)
		name := ""
		if existingValues != nil {
			if namePtr, ok := m.rcloneFormValues["_name"]; ok && namePtr != nil {
				name = *namePtr
			}
		}
		namePtr := &name
		m.rcloneFormValues["_name"] = namePtr
		firstPageFields = append(firstPageFields,
			huh.NewInput().
				Key("_name").
				Title("Remote name").
				Description("Identifier for this destination (no spaces)").
				Value(namePtr).
				Validate(func(s string) error {
					if s == "" {
						return fmt.Errorf("name is required")
					}
					if strings.Contains(s, " ") {
						return fmt.Errorf("name cannot contain spaces")
					}
					for _, existing := range m.rcloneRemotes {
						if existing == s {
							return fmt.Errorf("remote '%s' already exists", s)
						}
					}
					return nil
				}),
		)
	}

	// Helper to split options into pages of maxFieldsPerPage
	const maxFieldsPerPage = 5
	buildPages := func(opts []fs.Option, pageName string) {
		var currentFields []huh.Field
		pageNum := 1

		for i, opt := range opts {
			field := m.buildOptionField(opt, existingValues)
			if field != nil {
				currentFields = append(currentFields, field)
			}

			// Create new page when we reach limit or end of options
			if len(currentFields) >= maxFieldsPerPage || i == len(opts)-1 {
				if len(currentFields) > 0 {
					name := pageName
					if pageNum > 1 || len(opts) > maxFieldsPerPage {
						name = fmt.Sprintf("%s (cont.)", pageName)
					}
					namedGroups = append(namedGroups, namedGroup{
						name:  name,
						group: huh.NewGroup(currentFields...),
					})
					currentFields = nil
					pageNum++
				}
			}
		}
	}

	// Add first batch of standard options to first page
	firstBatchEnd := maxFieldsPerPage - len(firstPageFields)
	if firstBatchEnd > len(standardOpts) {
		firstBatchEnd = len(standardOpts)
	}

	for i := 0; i < firstBatchEnd; i++ {
		field := m.buildOptionField(standardOpts[i], existingValues)
		if field != nil {
			firstPageFields = append(firstPageFields, field)
		}
	}

	if len(firstPageFields) > 0 {
		namedGroups = append(namedGroups, namedGroup{
			name:  "Configuration",
			group: huh.NewGroup(firstPageFields...),
		})
	}

	// Add remaining standard options as additional pages
	if len(standardOpts) > firstBatchEnd {
		buildPages(standardOpts[firstBatchEnd:], "Configuration")
	}

	// Add advanced options toggle page if there are advanced options and not showing them
	if len(advancedOpts) > 0 && !m.showAdvanced {
		showAdvancedChoice := "no"
		m.rcloneFormValues["_show_advanced"] = &showAdvancedChoice
		namedGroups = append(namedGroups, namedGroup{
			name: "Advanced Options",
			group: huh.NewGroup(
				huh.NewSelect[string]().
					Key("_show_advanced").
					Title(fmt.Sprintf("Show %d advanced options?", len(advancedOpts))).
					Description("Advanced options are usually not needed. Use tab to change selection.").
					Options(
						huh.NewOption("No, save now", "no"),
						huh.NewOption("Yes, show advanced options", "yes"),
					).
					Value(&showAdvancedChoice),
			),
		})
	}

	// Add advanced options pages if showing advanced
	if m.showAdvanced && len(advancedOpts) > 0 {
		// Track where advanced options start (for navigation after rebuild)
		m.advancedStartPage = len(namedGroups)
		buildPages(advancedOpts, "Advanced")
	}

	// Add page numbers to group titles
	var groups []*huh.Group
	total := len(namedGroups)
	for i, ng := range namedGroups {
		groups = append(groups, ng.group.Title(fmt.Sprintf("%s (%d/%d)", ng.name, i+1, total)))
	}

	return huh.NewForm(groups...).
		WithShowHelp(true).
		WithShowErrors(true).
		WithKeyMap(customKeyMap()).
		WithTheme(themeAmber()).
		WithWidth(m.formWidth())
}

// buildOptionField creates a huh field from an fs.Option
func (m *model) buildOptionField(opt fs.Option, existing map[string]string) huh.Field {
	// Get existing or default value
	val := ""
	if existing != nil {
		val = existing[opt.Name]
	} else if opt.Default != nil {
		val = fmt.Sprintf("%v", opt.Default)
	}

	// Allocate value on heap and store pointer
	valPtr := new(string)
	*valPtr = val
	m.rcloneFormValues[opt.Name] = valPtr

	title := opt.Name
	if opt.Required {
		title += " *"
	}

	// Only show first line of description to keep form compact
	description := opt.Help
	if idx := strings.Index(description, "\n"); idx != -1 {
		description = description[:idx]
	}

	// Select field (has exclusive examples)
	if len(opt.Examples) > 0 && opt.Exclusive {
		options := make([]huh.Option[string], 0, len(opt.Examples))
		for _, ex := range opt.Examples {
			label := ex.Value
			if ex.Help != "" {
				label = fmt.Sprintf("%s - %s", ex.Value, truncateText(ex.Help, 40))
			}
			options = append(options, huh.NewOption(label, ex.Value))
		}
		return huh.NewSelect[string]().
			Title(title).
			Description(description).
			Options(options...).
			Value(valPtr)
	}

	// Boolean field (default is bool type)
	if opt.Default != nil {
		if _, ok := opt.Default.(bool); ok {
			boolVal := *valPtr == "true"
			boolPtr := &boolVal
			// Store the conversion - we'll handle this in save
			m.rcloneFormValues[opt.Name+"_bool"] = valPtr
			return huh.NewConfirm().
				Title(title).
				Description(description).
				Value(boolPtr)
		}
	}

	// Password field
	if opt.IsPassword {
		return huh.NewInput().
			Title(title).
			Description(description).
			EchoMode(huh.EchoModePassword).
			Value(valPtr)
	}

	// Text input (with suggestions if has non-exclusive examples)
	input := huh.NewInput().
		Title(title).
		Description(description).
		Value(valPtr)

	if opt.Required {
		input = input.Validate(huh.ValidateNotEmpty())
	}

	if len(opt.Examples) > 0 && !opt.Exclusive {
		suggestions := make([]string, 0, len(opt.Examples))
		for _, ex := range opt.Examples {
			suggestions = append(suggestions, ex.Value)
		}
		input = input.Suggestions(suggestions)
	}

	return input
}

// saveRcloneRemote saves the form values to rclone config
func (m model) saveRcloneRemote() (tea.Model, tea.Cmd) {
	// Determine if this is a new remote or editing existing
	var name string
	isEdit := false
	if namePtr, ok := m.rcloneFormValues["_name"]; ok && namePtr != nil {
		name = *namePtr
	} else {
		// Editing existing remote
		name = m.selectedRemote
		isEdit = true
	}

	backendType := m.selectedBackend.Name

	// Set the type first
	rcloneconfig.FileSetValue(name, "type", backendType)

	// Set all other values
	for key, valPtr := range m.rcloneFormValues {
		if key == "_name" || strings.HasSuffix(key, "_bool") || valPtr == nil || *valPtr == "" {
			continue
		}
		rcloneconfig.FileSetValue(name, key, *valPtr)
	}

	// Check if this backend needs OAuth (has Config function)
	if m.selectedBackend.Config != nil {
		// Save config so PostConfig can read it
		rcloneconfig.SaveConfig()

		// Start OAuth flow
		m.selectedRemote = name // Store name for OAuth view
		m.view = viewRcloneOAuth
		m.oauthStatus = "Opening browser for authentication..."
		m.oauthErr = nil

		return m, tea.Batch(m.spinner.Tick, m.runOAuthConfig(name, isEdit))
	}

	// No OAuth needed, just save
	rcloneconfig.SaveConfig()

	// Refresh remotes and return to appropriate view
	m.refreshRcloneRemotes()
	if isEdit {
		m.view = viewRcloneActions
		m.cursor = 0
	} else {
		m.view = viewRcloneList
		m.cursor = 0
		m.selectedRemote = ""
	}
	m.rcloneForm = nil
	m.rcloneFormValues = nil
	m.selectedBackend = nil

	return m, nil
}

// runRcloneTestCmd runs a connection test for the selected rclone remote
func (m *model) runRcloneTestCmd() tea.Cmd {
	remoteName := m.selectedRemote
	bucket := ""
	if m.rcloneTestFormData != nil {
		bucket = m.rcloneTestFormData.bucket
	}

	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Build test path - include bucket if provided by user
		testPath := remoteName + ":"
		if bucket != "" {
			testPath = remoteName + ":" + bucket
		}

		err := storage.TestAccess(ctx, testPath)
		if err != nil {
			return rcloneTestResultMsg{
				success: false,
				message: err.Error(),
			}
		}
		return rcloneTestResultMsg{
			success: true,
			message: "Connection successful",
		}
	}
}

// runRcloneFormTestCmd tests the rclone remote using current form values
// It temporarily saves the config, tests, and reports results.
// If bucket is non-empty, it tests at that bucket path instead of root level.
func (m *model) runRcloneFormTestCmd(bucket string) tea.Cmd {
	backend := m.selectedBackend
	formValues := m.rcloneFormValues
	isEdit := m.selectedRemote != "" // Check if editing existing remote

	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Determine remote name - use temp name to avoid modifying existing config
		var remoteName string
		if isEdit {
			// For edits, use a temp name for testing
			remoteName = "__test_edit_remote__"
		} else {
			if namePtr, ok := formValues["_name"]; ok && namePtr != nil && *namePtr != "" {
				// Use the user's chosen name
				remoteName = *namePtr
			} else {
				remoteName = "__test_temp_remote__"
			}
		}

		// Set backend type
		rcloneconfig.FileSetValue(remoteName, "type", backend.Name)

		// Set all form values
		for key, valPtr := range formValues {
			if valPtr == nil || strings.HasPrefix(key, "_") {
				continue
			}
			if *valPtr != "" {
				rcloneconfig.FileSetValue(remoteName, key, *valPtr)
			}
		}

		// Save config
		rcloneconfig.SaveConfig()

		// Build test path - include bucket if provided
		testPath := remoteName + ":"
		if bucket != "" {
			testPath = remoteName + ":" + bucket
		}

		err := storage.TestAccess(ctx, testPath)

		// Clean up temp remote if we created one
		if remoteName == "__test_temp_remote__" || remoteName == "__test_edit_remote__" {
			rcloneconfig.DeleteRemote(remoteName)
			rcloneconfig.SaveConfig()
		}

		if err != nil {
			return rcloneTestResultMsg{
				success: false,
				message: err.Error(),
			}
		}
		return rcloneTestResultMsg{
			success: true,
			message: "Connection successful",
		}
	}
}

// runOAuthConfig runs the OAuth configuration for backends that require it
func (m *model) runOAuthConfig(remoteName string, isEdit bool) tea.Cmd {
	backend := m.selectedBackend
	return func() tea.Msg {
		ctx := context.Background()

		// Build a config map from the saved values
		cm := configmap.Simple{}
		for _, opt := range backend.Options {
			val, ok := rcloneconfig.FileGetValue(remoteName, opt.Name)
			if ok && val != "" {
				cm[opt.Name] = val
			}
		}

		// Run the backend's Config function (handles OAuth)
		// This will open a browser for OAuth backends
		err := rcloneconfig.PostConfig(ctx, remoteName, cm, backend)
		if err != nil {
			return oauthCompleteMsg{err: err, isEdit: isEdit}
		}

		return oauthCompleteMsg{err: nil, isEdit: isEdit}
	}
}

// refreshRcloneRemotes loads the list of configured rclone remotes
func (m *model) refreshRcloneRemotes() {
	m.rcloneRemotes = rcloneconfig.GetRemoteNames()
	sort.Strings(m.rcloneRemotes)
	// Reset filter when remotes change
	m.rcloneRemoteFilter = ""
	m.rcloneRemoteFilteredList = m.rcloneRemotes
}

// loadRcloneRemoteValues loads all values for an existing remote
func (m *model) loadRcloneRemoteValues(remoteName string) map[string]string {
	values := make(map[string]string)

	// Get all options from the backend
	backendType := getRcloneRemoteType(remoteName)
	backend, err := fs.Find(backendType)
	if err != nil {
		return values
	}

	// Load each option value
	for _, opt := range backend.Options {
		val, _ := rcloneconfig.FileGetValue(remoteName, opt.Name)
		if val != "" {
			values[opt.Name] = val
		}
	}

	return values
}

// loadRcloneBackends loads the list of available rclone backends (filtered, non-hidden)
func (m *model) loadRcloneBackends() {
	m.rcloneBackends = nil
	for _, ri := range fs.Registry {
		if ri.Hide {
			continue
		}
		m.rcloneBackends = append(m.rcloneBackends, ri)
	}
	// Sort alphabetically by name
	sort.Slice(m.rcloneBackends, func(i, j int) bool {
		return m.rcloneBackends[i].Name < m.rcloneBackends[j].Name
	})
	// Initialize filtered list to all backends
	m.rcloneFilteredList = m.rcloneBackends
	m.rcloneFilter = ""
}

// filterRcloneBackends filters the backend list by search term
func (m *model) filterRcloneBackends(filter string) {
	m.rcloneFilter = filter
	if filter == "" {
		m.rcloneFilteredList = m.rcloneBackends
		return
	}

	filter = strings.ToLower(filter)
	m.rcloneFilteredList = nil
	for _, ri := range m.rcloneBackends {
		if strings.Contains(strings.ToLower(ri.Name), filter) ||
			strings.Contains(strings.ToLower(ri.Description), filter) {
			m.rcloneFilteredList = append(m.rcloneFilteredList, ri)
		}
	}
}

// filterDatabases filters the database list by search term (viewDBList)
func (m *model) filterDatabases(filter string) {
	m.dbFilter = filter
	if filter == "" {
		m.dbFilteredList = m.dbNames
		return
	}

	filter = strings.ToLower(filter)
	m.dbFilteredList = nil
	for _, name := range m.dbNames {
		db := m.cfg.Databases[name]
		if strings.Contains(strings.ToLower(name), filter) ||
			strings.Contains(strings.ToLower(db.Type), filter) {
			m.dbFilteredList = append(m.dbFilteredList, name)
		}
	}
}

// filterRcloneRemotes filters the rclone remote list by search term (viewRcloneList)
func (m *model) filterRcloneRemotes(filter string) {
	m.rcloneRemoteFilter = filter
	if filter == "" {
		m.rcloneRemoteFilteredList = m.rcloneRemotes
		return
	}

	filter = strings.ToLower(filter)
	m.rcloneRemoteFilteredList = nil
	for _, name := range m.rcloneRemotes {
		remoteType := getRcloneRemoteType(name)
		if strings.Contains(strings.ToLower(name), filter) ||
			strings.Contains(strings.ToLower(remoteType), filter) {
			m.rcloneRemoteFilteredList = append(m.rcloneRemoteFilteredList, name)
		}
	}
}

// filterBackupDatabases filters the backup database list by search term (viewBackupSelect)
func (m *model) filterBackupDatabases(filter string) {
	m.backupFilter = filter
	if filter == "" {
		m.backupFilteredList = m.dbNames
		return
	}

	filter = strings.ToLower(filter)
	m.backupFilteredList = nil
	for _, name := range m.dbNames {
		db := m.cfg.Databases[name]
		if strings.Contains(strings.ToLower(name), filter) ||
			strings.Contains(strings.ToLower(db.Type), filter) {
			m.backupFilteredList = append(m.backupFilteredList, name)
		}
	}
}

// filterRestoreDatabases filters the restore database list by search term (viewRestoreDBSelect)
func (m *model) filterRestoreDatabases(filter string) {
	m.restoreDBFilter = filter
	if filter == "" {
		m.restoreDBFilteredList = m.dbNames
		return
	}

	filter = strings.ToLower(filter)
	m.restoreDBFilteredList = nil
	for _, name := range m.dbNames {
		db := m.cfg.Databases[name]
		if strings.Contains(strings.ToLower(name), filter) ||
			strings.Contains(strings.ToLower(db.Type), filter) {
			m.restoreDBFilteredList = append(m.restoreDBFilteredList, name)
		}
	}
}

// filterRestoreFiles filters the backup files list by search term (viewRestoreFileSelect)
func (m *model) filterRestoreFiles(filter string) {
	m.restoreFileFilter = filter
	if filter == "" {
		m.restoreFileFilteredList = m.backupFiles
		return
	}

	filter = strings.ToLower(filter)
	m.restoreFileFilteredList = nil
	for _, f := range m.backupFiles {
		if strings.Contains(strings.ToLower(f.Name), filter) {
			m.restoreFileFilteredList = append(m.restoreFileFilteredList, f)
		}
	}
}

// isFilterableView returns true if the view supports filter input
func (m model) isFilterableView() bool {
	switch m.view {
	case viewRcloneAddType, viewRcloneList, viewDBList, viewBackupSelect, viewRestoreDBSelect, viewRestoreFileSelect:
		return true
	}
	return false
}

// calcScrollWindow calculates the visible window for a scrollable list.
// cursor is the current cursor position, listLen is the number of items in the list
// (excluding any extra items like "Add" buttons), maxVisible is the max items to show.
// Returns (start, end) indices for slicing the list.
func calcScrollWindow(cursor, listLen, maxVisible int) (start, end int) {
	// Cap cursor at listLen-1 for scroll calculation (handles "Add" button case)
	scrollCursor := cursor
	if scrollCursor >= listLen {
		scrollCursor = listLen - 1
	}
	if scrollCursor < 0 {
		scrollCursor = 0
	}

	start = 0
	if scrollCursor >= maxVisible {
		start = scrollCursor - maxVisible + 1
	}
	end = start + maxVisible
	if end > listLen {
		end = listLen
	}
	return start, end
}

// handleFilterBackspace handles backspace in filterable list views
// Returns (handled, newModel) - handled is true if there was text to delete
func (m model) handleFilterBackspace() (bool, model) {
	switch m.view {
	case viewRcloneAddType:
		if len(m.rcloneFilter) > 0 {
			m.rcloneFilter = m.rcloneFilter[:len(m.rcloneFilter)-1]
			m.filterRcloneBackends(m.rcloneFilter)
			m.cursor = 0
			return true, m
		}
	case viewRcloneList:
		if len(m.rcloneRemoteFilter) > 0 {
			m.rcloneRemoteFilter = m.rcloneRemoteFilter[:len(m.rcloneRemoteFilter)-1]
			m.filterRcloneRemotes(m.rcloneRemoteFilter)
			m.cursor = 0
			return true, m
		}
	case viewDBList:
		if len(m.dbFilter) > 0 {
			m.dbFilter = m.dbFilter[:len(m.dbFilter)-1]
			m.filterDatabases(m.dbFilter)
			m.cursor = 0
			return true, m
		}
	case viewBackupSelect:
		if len(m.backupFilter) > 0 {
			m.backupFilter = m.backupFilter[:len(m.backupFilter)-1]
			m.filterBackupDatabases(m.backupFilter)
			m.cursor = 0
			return true, m
		}
	case viewRestoreDBSelect:
		if len(m.restoreDBFilter) > 0 {
			m.restoreDBFilter = m.restoreDBFilter[:len(m.restoreDBFilter)-1]
			m.filterRestoreDatabases(m.restoreDBFilter)
			m.cursor = 0
			return true, m
		}
	case viewRestoreFileSelect:
		if len(m.restoreFileFilter) > 0 {
			m.restoreFileFilter = m.restoreFileFilter[:len(m.restoreFileFilter)-1]
			m.filterRestoreFiles(m.restoreFileFilter)
			m.cursor = 0
			return true, m
		}
	}
	return false, m
}

// handleFilterInput handles text input in filterable list views
func (m model) handleFilterInput(input string) model {
	switch m.view {
	case viewRcloneAddType:
		m.rcloneFilter += input
		m.filterRcloneBackends(m.rcloneFilter)
		m.cursor = 0
	case viewRcloneList:
		m.rcloneRemoteFilter += input
		m.filterRcloneRemotes(m.rcloneRemoteFilter)
		m.cursor = 0
	case viewDBList:
		m.dbFilter += input
		m.filterDatabases(m.dbFilter)
		m.cursor = 0
	case viewBackupSelect:
		m.backupFilter += input
		m.filterBackupDatabases(m.backupFilter)
		m.cursor = 0
	case viewRestoreDBSelect:
		m.restoreDBFilter += input
		m.filterRestoreDatabases(m.restoreDBFilter)
		m.cursor = 0
	case viewRestoreFileSelect:
		m.restoreFileFilter += input
		m.filterRestoreFiles(m.restoreFileFilter)
		m.cursor = 0
	}
	return m
}

// getRcloneRemoteType returns the type of a configured remote
func getRcloneRemoteType(name string) string {
	t, _ := rcloneconfig.FileGetValue(name, "type")
	return t
}

// truncateText truncates text to maxLen, adding "..." if truncated
func truncateText(text string, maxLen int) string {
	if len(text) <= maxLen {
		return text
	}
	if maxLen <= 3 {
		return text[:maxLen]
	}
	return text[:maxLen-3] + "..."
}
