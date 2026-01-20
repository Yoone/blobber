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
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/huh"
)

type view int

const (
	viewMainMenu view = iota
	viewBackupSelect
	viewRetentionPreCheck  // checking retention policies before backup
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
	viewDone
)

// Menu option constants
const (
	// Main menu options
	menuBackup = iota
	menuRestore
	menuManage
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
	dbActionDelete
	dbActionBack
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
	currentStep backupStep       // current step (stepIdle when done)
	logs        []backupLogEntry // completed steps
	result      *backup.Result   // result from dump step (for upload)
	done        bool             // true when all steps complete
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
	Message   string
	IsError   bool
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

type model struct {
	cfg          *config.Config
	view         view
	cursor       int
	dbNames      []string
	selected      map[string]bool // for backup multi-select
	skipRetention bool            // skip retention policy for this backup run
	dryRun        bool            // perform dump but skip upload and retention
	selectedDB    string          // for restore
	backupFiles  []storage.RemoteFile
	selectedFile string
	isLocalRestore bool // true if restoring from local file
	logs         []string
	err          error
	quitting     bool

	// Spinner for progress indication
	spinner spinner.Model

	// Backup progress tracking (parallel execution)
	backupQueue  []string                  // databases to backup (in order for display)
	backupStates map[string]*dbBackupState // per-database state

	// Restore progress tracking
	restoreStep     restoreStep       // current restore step
	restoreLogs     []restoreLogEntry // completed restore steps
	restoreLocalPath string           // path to local file being restored

	// Retention plan (pre-calculated before backup starts)
	retentionPlan map[string][]storage.RemoteFile // dbName -> files to delete

	// Add database form (huh)
	addDBType string       // file, mysql, postgres
	addDBForm *huh.Form    // huh form for adding database
	formData  *formFields  // heap-allocated form values (survives bubbletea copies)

	// Test state
	testRunning     bool   // true while test is running
	testConnResult  string // result of connection test (MySQL/Postgres page 1)
	testDestResult  string // result of destination test (page 2)
	formError       string // validation error to display in form
	pendingSave     bool   // true when form completed and running pre-save tests
	pendingDestTest bool   // true when destination test should run after connection test

	// Database list/edit/delete
	editingDB  string // name of database being edited (empty for add)
	listPage   int    // current page in DB list
	listPageSize int  // items per page

	// Restore local path form
	restorePathForm *huh.Form
	restoreFormData *restoreFormFields // heap-allocated form values
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
		WithWidth(60)
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
		WithWidth(60)
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

	m := model{
		cfg:          cfg,
		view:         viewMainMenu,
		dbNames:      dbNames,
		selected:     selected,
		listPageSize: 8, // items per page in DB list
		spinner:      s,
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
		}

		// Handle huh form for restore local path
		if m.view == viewRestoreLocalInput && m.restorePathForm != nil {
			if msg.Type == tea.KeyCtrlC {
				m.quitting = true
				return m, tea.Quit
			}
		}

		// Skip generic key handling for form views - let the form handle its own keys
		if m.view != viewAddDBForm && m.view != viewEditDBForm && m.view != viewRestoreLocalInput {
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
				}

			case "down", "j":
				m.cursor = min(m.cursor+1, m.maxCursor())

			case "left", "h":
				// Page left in DB list
				if m.view == viewDBList && m.listPage > 0 {
					m.listPage--
					m.cursor = 0
				}

			case "right", "l":
				// Page right in DB list
				if m.view == viewDBList {
					totalPages := (len(m.dbNames) + m.listPageSize - 1) / m.listPageSize
					if m.listPage < totalPages-1 {
						m.listPage++
						m.cursor = 0
					}
				}

			case " ":
				// Toggle selection in backup view
				if m.view == viewBackupSelect {
					if m.cursor < len(m.dbNames) {
						// Toggle database selection
						name := m.dbNames[m.cursor]
						m.selected[name] = !m.selected[name]
					} else if m.cursor == len(m.dbNames) {
						// Toggle retention policy
						m.skipRetention = !m.skipRetention
					} else if m.cursor == len(m.dbNames)+1 {
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
			return m, nil
		}
		// No files to delete, start backups directly
		return m.startBackups()

	case backupStepDoneMsg:
		return m.handleBackupStepDone(msg)

	case allBackupsDoneMsg:
		m.view = viewDone
		m.logs = m.buildBackupSummaryLogs()
		return m, nil

	case fileListMsg:
		m.backupFiles = msg.files
		m.err = msg.err
		if m.err == nil {
			m.view = viewRestoreFileSelect
			m.cursor = 0
		}

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
		case menuRestore:
			if len(m.dbNames) == 0 {
				m.err = fmt.Errorf("no databases configured")
				m.view = viewDone
				return m, nil
			}
			m.view = viewRestoreDBSelect
			m.cursor = 0
		case menuManage:
			m.view = viewDBList
			m.cursor = 0
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
		// Run Backup is after databases, retention toggle, and dry-run toggle
		if m.cursor == len(m.dbNames)+2 {
			// Build ordered queue of selected databases
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
		if m.cursor < len(m.dbNames) {
			m.selectedDB = m.dbNames[m.cursor]
			m.view = viewRestoreSourceSelect
			m.cursor = 0
		}

	case viewRestoreSourceSelect:
		if m.cursor == restoreSourceRemote {
			// From remote
			m.isLocalRestore = false
			m.view = viewRestoreFileSelect
			return m, m.fetchBackupFiles()
		} else {
			// From local file
			m.isLocalRestore = true
			m.view = viewRestoreLocalInput
			m.restoreFormData = nil // Reset so buildRestorePathForm allocates fresh
			m.restorePathForm = m.buildRestorePathForm()
			return m, m.restorePathForm.Init()
		}

	case viewRestoreFileSelect:
		if m.cursor < len(m.backupFiles) {
			m.selectedFile = m.backupFiles[m.cursor].Name
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
		// Calculate which item is selected
		startIdx := m.listPage * m.listPageSize
		itemsOnPage := min(len(m.dbNames)-startIdx, m.listPageSize)

		if m.cursor < itemsOnPage {
			// Selected a database
			m.editingDB = m.dbNames[startIdx+m.cursor]
			m.view = viewDBActions
			m.cursor = 0
		} else {
			// Add new database
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
		return menuExit // Backup, Restore, Manage databases, Exit
	case viewBackupSelect:
		return len(m.dbNames) + 2 // DBs + retention toggle + dry-run toggle + Run button
	case viewRestoreDBSelect:
		return len(m.dbNames) - 1
	case viewRestoreSourceSelect:
		return restoreSourceLocal // Remote or Local
	case viewRestoreFileSelect:
		return len(m.backupFiles) - 1
	case viewRestoreConfirm, viewDeleteConfirm, viewRetentionPreConfirm:
		return confirmNo // Yes or No
	case viewAddDBType:
		return dbTypePostgres // file, mysql, postgres
	case viewDBList:
		// DBs on current page + Add button
		startIdx := m.listPage * m.listPageSize
		itemsOnPage := min(len(m.dbNames)-startIdx, m.listPageSize)
		return itemsOnPage // includes Add button at position itemsOnPage
	case viewDBActions:
		return dbActionBack // Edit, Delete, Back
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
	s.WriteString(titleStyle.Render("░█▀▄░█░░░█░█░█▀▄░█▀▄░█▀▀░█▀▄  Database Backup & Restore Tool"))
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
	case viewDone:
		s.WriteString(m.renderDone())
	}

	s.WriteString("\n")
	switch m.view {
	case viewMainMenu:
		s.WriteString(dimStyle.Render("↑/↓: navigate • enter: select • esc: quit"))
	case viewBackupSelect:
		s.WriteString(dimStyle.Render("↑/↓: navigate • space: toggle • enter: select • esc: back"))
	case viewRestoreLocalInput:
		s.WriteString(dimStyle.Render("type path • enter: confirm • esc: back"))
	case viewAddDBForm, viewEditDBForm:
		s.WriteString(dimStyle.Render("↑/↓/enter: navigate • tab: complete/cycle • ctrl+t: test • esc: back"))
	case viewAddDBFormConfirmExit, viewEditDBFormConfirmExit:
		s.WriteString(dimStyle.Render("↑/↓: select • enter: confirm • esc: cancel"))
	case viewDBList:
		if len(m.dbNames) > m.listPageSize {
			s.WriteString(dimStyle.Render("↑/↓: navigate • ←/→: page • enter: select • esc: back"))
		} else {
			s.WriteString(dimStyle.Render("↑/↓: navigate • enter: select • esc: back"))
		}
	case viewRetentionPreCheck:
		s.WriteString(dimStyle.Render("Checking retention policies..."))
	case viewRetentionPreConfirm:
		s.WriteString(dimStyle.Render("↑/↓: select • enter: confirm • esc: back"))
	case viewBackupRunning, viewRestoreRunning:
		// No help text needed - progress is shown in main view
	case viewDone:
		s.WriteString(dimStyle.Render("enter: continue"))
	default:
		s.WriteString(dimStyle.Render("↑/↓: navigate • enter: select • esc: back"))
	}

	return borderStyle.Render(s.String())
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

	items := []string{"Backup databases", "Restore a database", "Manage databases", "Exit"}
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

	for i, name := range m.dbNames {
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

	// Retention toggle (index = len(dbNames))
	s.WriteString("\n")
	retentionIdx := len(m.dbNames)
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

	// Dry-run toggle (index = len(dbNames) + 1)
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

	// Run Backup button (index = len(dbNames) + 2)
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

	// Count total files
	totalFiles := 0
	for _, files := range m.retentionPlan {
		totalFiles += len(files)
	}

	s.WriteString(fmt.Sprintf("Retention policy will delete %d backup(s):\n\n", totalFiles))

	// Show files grouped by database
	maxFilesPerDB := 4
	for _, name := range m.backupQueue {
		files := m.retentionPlan[name]
		if len(files) == 0 {
			continue
		}

		s.WriteString(selectedStyle.Render(name))
		s.WriteString("\n")

		for i, f := range files {
			if i >= maxFilesPerDB {
				s.WriteString(dimStyle.Render(fmt.Sprintf("  ... and %d more\n", len(files)-maxFilesPerDB)))
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
	s.WriteString(fmt.Sprintf("Running backups (%d/%d)\n\n", done, total))

	// Render each database in order with its steps
	for i, dbName := range m.backupQueue {
		state := m.backupStates[dbName]
		if state == nil {
			continue
		}

		// Add blank line between DBs (except first)
		if i > 0 {
			s.WriteString("\n")
		}

		// DB name
		s.WriteString(selectedStyle.Render(truncateString(dbName, 60)))
		s.WriteString("\n")

		// Show completed steps
		for _, entry := range state.logs {
			if entry.IsError {
				s.WriteString(fmt.Sprintf("  %s %s\n", errorStyle.Render("✗"), errorStyle.Render(entry.Message)))
			} else if entry.IsSkipped {
				s.WriteString(fmt.Sprintf("  %s %s\n", dimStyle.Render("○"), dimStyle.Render(entry.Message)))
			} else {
				s.WriteString(fmt.Sprintf("  %s %s\n", successStyle.Render("✓"), entry.Message))
			}
		}

		// Show current step with spinner (if not done)
		if !state.done && state.currentStep != stepIdle {
			s.WriteString(fmt.Sprintf("  %s %s...\n", m.spinner.View(), state.currentStep.String()))
		}
	}

	return s.String()
}

func (m model) renderRestoreRunning() string {
	var s strings.Builder

	// Header
	s.WriteString(fmt.Sprintf("Restoring to %s\n\n", selectedStyle.Render(m.selectedDB)))

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
		s.WriteString(fmt.Sprintf("  %s %s...\n", m.spinner.View(), m.restoreStep.String()))
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

	for i, name := range m.dbNames {
		cursor := "  "
		db := m.cfg.Databases[name]
		line := fmt.Sprintf("%s %s", name, dimStyle.Render(fmt.Sprintf("(%s)", db.Type)))
		if m.cursor == i {
			cursor = cursorStyle.Render("▸ ")
			line = selectedStyle.Render(name) + " " + dimStyle.Render(fmt.Sprintf("(%s)", db.Type))
		}
		s.WriteString(fmt.Sprintf("%s%s\n", cursor, line))
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

	if len(m.backupFiles) == 0 {
		s.WriteString(dimStyle.Render("  No backups found\n"))
		return s.String()
	}

	for i, f := range m.backupFiles {
		cursor := "  "
		line := fmt.Sprintf("%s  %8.2f MB  %s", f.ModTime.Format("2006-01-02 15:04"), float64(f.Size)/(1024*1024), f.Name)
		if m.cursor == i {
			cursor = cursorStyle.Render("▸ ")
			line = selectedStyle.Render(line)
		}
		s.WriteString(fmt.Sprintf("%s%s\n", cursor, line))
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

	if len(m.dbNames) == 0 {
		s.WriteString(dimStyle.Render("  No databases configured"))
		s.WriteString("\n\n")
	} else {
		// Calculate pagination
		totalPages := (len(m.dbNames) + m.listPageSize - 1) / m.listPageSize
		startIdx := m.listPage * m.listPageSize
		endIdx := min(startIdx+m.listPageSize, len(m.dbNames))

		for i := startIdx; i < endIdx; i++ {
			name := m.dbNames[i]
			db := m.cfg.Databases[name]
			cursor := "  "
			line := fmt.Sprintf("%s %s", name, dimStyle.Render(fmt.Sprintf("(%s)", db.Type)))
			if m.cursor == i-startIdx {
				cursor = cursorStyle.Render("▸ ")
				line = selectedStyle.Render(name) + " " + dimStyle.Render(fmt.Sprintf("(%s)", db.Type))
			}
			s.WriteString(fmt.Sprintf("%s%s\n", cursor, line))
		}

		// Page indicator
		if totalPages > 1 {
			s.WriteString(fmt.Sprintf("\n%s\n", dimStyle.Render(fmt.Sprintf("Page %d/%d (←/→ to navigate)", m.listPage+1, totalPages))))
		}
		s.WriteString("\n")
	}

	// Add new database option
	addIdx := min(len(m.dbNames), m.listPageSize) // position after DBs on current page
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

	items := []string{"Edit", "Delete", "Back"}
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

	// Adjust list page if needed
	if m.listPage > 0 && len(m.dbNames) <= m.listPage*m.listPageSize {
		m.listPage--
	}

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
	done      bool   // true if restore is complete
}

// testResultMsg is sent when a connection/destination test completes
type testResultMsg struct {
	testType string // "connection" or "destination"
	success  bool
	message  string
}

// Commands

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

			toDelete := retention.Apply(ctx, files, name, db.Retention)
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

			err := storage.Upload(ctx, backupPath, db.Dest)
			if err != nil {
				return backupStepDoneMsg{
					dbName: name,
					step:   stepUploading,
					err:    err,
				}
			}

			return backupStepDoneMsg{
				dbName:  name,
				step:    stepUploading,
				message: fmt.Sprintf("Saved to %s", formatDestForDisplay(db.Dest, 50)),
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
		m.logs = m.buildRestoreSummaryLogs()
		return m, nil
	}

	// Save the local path from download step
	if msg.step == restoreStepDownloading && msg.localPath != "" {
		m.restoreLocalPath = msg.localPath
	}

	// Check if done
	if msg.done {
		m.view = viewDone
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
	m.restoreLogs = nil
	m.view = viewRestoreRunning

	if m.isLocalRestore {
		// Local restore: skip download, go straight to restoring
		m.restoreStep = restoreStepRestoring
		m.restoreLocalPath = m.selectedFile
	} else {
		// Remote restore: start with download
		m.restoreStep = restoreStepDownloading
		m.restoreLocalPath = ""
	}

	return m, tea.Batch(m.spinner.Tick, m.runRestoreStep())
}

// runRestoreStep runs the current step in the restore process
func (m model) runRestoreStep() tea.Cmd {
	db := m.cfg.Databases[m.selectedDB]
	step := m.restoreStep
	selectedFile := m.selectedFile
	localPath := m.restoreLocalPath

	return func() tea.Msg {
		ctx := context.Background()

		switch step {
		case restoreStepDownloading:
			tmpDir, err := createTempDir()
			if err != nil {
				return restoreStepDoneMsg{
					step: restoreStepDownloading,
					err:  err,
				}
			}

			if err := storage.Download(ctx, db.Dest, selectedFile, tmpDir); err != nil {
				return restoreStepDoneMsg{
					step: restoreStepDownloading,
					err:  err,
				}
			}

			downloadedPath := tmpDir + "/" + selectedFile
			return restoreStepDoneMsg{
				step:      restoreStepDownloading,
				message:   fmt.Sprintf("Downloaded %s", selectedFile),
				localPath: downloadedPath,
			}

		case restoreStepRestoring:
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

		return nil
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
