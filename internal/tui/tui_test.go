package tui

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestCollapsePath(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("failed to get home dir: %v", err)
	}

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get cwd: %v", err)
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "absolute path in home directory",
			input:    filepath.Join(home, ".config", "blobber", "config.yaml"),
			expected: "~/.config/blobber/config.yaml",
		},
		{
			name:     "absolute path outside home directory",
			input:    "/etc/blobber/config.yaml",
			expected: "/etc/blobber/config.yaml",
		},
		{
			name:     "home directory itself",
			input:    home,
			expected: "~",
		},
	}

	// Add test for relative path - should be made absolute first
	// If cwd is under home, result should use ~; otherwise absolute path
	if strings.HasPrefix(cwd, home) {
		relInHome := "blobber.yaml"
		expectedRel := "~" + cwd[len(home):] + "/blobber.yaml"
		tests = append(tests, struct {
			name     string
			input    string
			expected string
		}{
			name:     "relative path in home directory becomes absolute with tilde",
			input:    relInHome,
			expected: expectedRel,
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := collapsePath(tt.input)
			if result != tt.expected {
				t.Errorf("collapsePath(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestExpandPath(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("failed to get home dir: %v", err)
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "tilde with path",
			input:    "~/Documents",
			expected: filepath.Join(home, "Documents"),
		},
		{
			name:     "tilde only",
			input:    "~",
			expected: home,
		},
		{
			name:     "tilde with nested path",
			input:    "~/foo/bar/baz",
			expected: filepath.Join(home, "foo/bar/baz"),
		},
		{
			name:     "absolute path unchanged",
			input:    "/tmp/foo",
			expected: "/tmp/foo",
		},
		{
			name:     "relative path unchanged",
			input:    "foo/bar",
			expected: "foo/bar",
		},
		{
			name:     "dot relative unchanged",
			input:    "./foo",
			expected: "./foo",
		},
		{
			name:     "parent relative unchanged",
			input:    "../foo",
			expected: "../foo",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "tilde in middle unchanged",
			input:    "/foo/~/bar",
			expected: "/foo/~/bar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := expandPath(tt.input)
			if result != tt.expected {
				t.Errorf("expandPath(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestExpandDest(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("failed to get home dir: %v", err)
	}

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get cwd: %v", err)
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "tilde with path",
			input:    "~/backups",
			expected: filepath.Join(home, "backups"),
		},
		{
			name:     "tilde only",
			input:    "~",
			expected: home,
		},
		{
			name:     "rclone s3 remote unchanged",
			input:    "s3:mybucket/path",
			expected: "s3:mybucket/path",
		},
		{
			name:     "rclone b2 remote unchanged",
			input:    "b2:mybucket",
			expected: "b2:mybucket",
		},
		{
			name:     "rclone gdrive remote unchanged",
			input:    "gdrive:folder/subfolder",
			expected: "gdrive:folder/subfolder",
		},
		{
			name:     "absolute path unchanged",
			input:    "/tmp/backups",
			expected: "/tmp/backups",
		},
		{
			name:     "relative path becomes absolute",
			input:    "backups",
			expected: filepath.Join(cwd, "backups"),
		},
		{
			name:     "dot relative becomes absolute",
			input:    "./backups",
			expected: filepath.Join(cwd, "backups"),
		},
		{
			name:     "parent relative becomes absolute",
			input:    "../backups",
			expected: filepath.Join(filepath.Dir(cwd), "backups"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := expandDest(tt.input)
			if result != tt.expected {
				t.Errorf("expandDest(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestGetPathSuggestions(t *testing.T) {
	// Create a temporary directory structure for testing
	tmpDir, err := os.MkdirTemp("", "pathtest")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test structure:
	// tmpDir/
	//   file1.txt
	//   file2.txt
	//   dir1/
	//   dir2/
	//   .hidden
	dirs := []string{"dir1", "dir2"}
	files := []string{"file1.txt", "file2.txt", ".hidden"}

	for _, d := range dirs {
		if err := os.Mkdir(filepath.Join(tmpDir, d), 0755); err != nil {
			t.Fatalf("failed to create dir %s: %v", d, err)
		}
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(tmpDir, f), []byte("test"), 0644); err != nil {
			t.Fatalf("failed to create file %s: %v", f, err)
		}
	}

	// Save and change to temp directory for relative path tests
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get cwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir: %v", err)
	}
	defer os.Chdir(origDir)

	t.Run("empty string returns nil", func(t *testing.T) {
		result := getPathSuggestions("")
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})

	t.Run("rclone remote returns nil", func(t *testing.T) {
		remotes := []string{"s3:bucket", "b2:bucket/path", "gdrive:"}
		for _, remote := range remotes {
			result := getPathSuggestions(remote)
			if result != nil {
				t.Errorf("getPathSuggestions(%q) = %v, want nil", remote, result)
			}
		}
	})

	t.Run("dot slash prefix preserved", func(t *testing.T) {
		result := getPathSuggestions("./")
		if len(result) == 0 {
			t.Fatal("expected suggestions, got none")
		}
		for _, s := range result {
			if !strings.HasPrefix(s, "./") {
				t.Errorf("suggestion %q should start with ./", s)
			}
		}
	})

	t.Run("dot slash with partial name", func(t *testing.T) {
		result := getPathSuggestions("./fi")
		if len(result) != 2 {
			t.Fatalf("expected 2 suggestions (file1.txt, file2.txt), got %d: %v", len(result), result)
		}
		for _, s := range result {
			if !strings.HasPrefix(s, "./file") {
				t.Errorf("suggestion %q should start with ./file", s)
			}
		}
	})

	t.Run("directories have trailing slash", func(t *testing.T) {
		result := getPathSuggestions("./d")
		if len(result) != 2 {
			t.Fatalf("expected 2 suggestions (dir1/, dir2/), got %d: %v", len(result), result)
		}
		for _, s := range result {
			if !strings.HasSuffix(s, "/") {
				t.Errorf("directory suggestion %q should end with /", s)
			}
		}
	})

	t.Run("files do not have trailing slash", func(t *testing.T) {
		result := getPathSuggestions("./file1")
		if len(result) != 1 {
			t.Fatalf("expected 1 suggestion, got %d: %v", len(result), result)
		}
		if strings.HasSuffix(result[0], "/") {
			t.Errorf("file suggestion %q should not end with /", result[0])
		}
	})

	t.Run("case insensitive matching", func(t *testing.T) {
		result := getPathSuggestions("./FI")
		if len(result) != 2 {
			t.Errorf("expected 2 suggestions for case-insensitive match, got %d: %v", len(result), result)
		}
	})

	t.Run("parent directory prefix preserved", func(t *testing.T) {
		// Create a subdirectory and test from there
		subDir := filepath.Join(tmpDir, "subdir")
		if err := os.Mkdir(subDir, 0755); err != nil {
			t.Fatalf("failed to create subdir: %v", err)
		}
		if err := os.Chdir(subDir); err != nil {
			t.Fatalf("failed to chdir to subdir: %v", err)
		}
		defer os.Chdir(tmpDir)

		result := getPathSuggestions("../")
		if len(result) == 0 {
			t.Fatal("expected suggestions, got none")
		}
		for _, s := range result {
			if !strings.HasPrefix(s, "../") {
				t.Errorf("suggestion %q should start with ../", s)
			}
		}
	})

	t.Run("nonexistent directory returns nil", func(t *testing.T) {
		result := getPathSuggestions("/nonexistent/path/")
		if result != nil {
			t.Errorf("expected nil for nonexistent path, got %v", result)
		}
	})

	t.Run("hidden files included", func(t *testing.T) {
		result := getPathSuggestions("./.")
		found := false
		for _, s := range result {
			if strings.Contains(s, ".hidden") {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("hidden file .hidden not found in suggestions: %v", result)
		}
	})
}

func TestGetPathSuggestionsRoot(t *testing.T) {
	// Test root directory (/) suggestions
	result := getPathSuggestions("/")
	if len(result) == 0 {
		t.Fatal("expected suggestions for /, got none")
	}

	for _, s := range result {
		if !strings.HasPrefix(s, "/") {
			t.Errorf("root suggestion %q should start with /", s)
		}
	}

	// Check that common root directories are present (at least one should exist)
	commonDirs := []string{"/tmp/", "/usr/", "/var/", "/etc/", "/Users/", "/home/"}
	found := false
	for _, s := range result {
		for _, d := range commonDirs {
			if s == d {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		t.Logf("suggestions: %v", result)
		// Don't fail, just log - different systems have different root contents
	}
}

func TestGetPathSuggestionsAbsolutePartial(t *testing.T) {
	// Test partial absolute path like /tm
	result := getPathSuggestions("/tm")

	// /tmp should be suggested on most systems (it's a symlink on macOS)
	found := false
	for _, s := range result {
		if s == "/tmp/" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("/tmp/ not found in suggestions for /tm: %v", result)
	}

	// All suggestions should start with /tm
	for _, s := range result {
		if !strings.HasPrefix(s, "/tm") {
			t.Errorf("suggestion %q should start with /tm", s)
		}
	}
}

func TestGetPathSuggestionsSymlinks(t *testing.T) {
	// Create a temp directory with a symlink to a directory
	tmpDir, err := os.MkdirTemp("", "symlinktest")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a target directory and a symlink to it
	targetDir := filepath.Join(tmpDir, "target_dir")
	if err := os.Mkdir(targetDir, 0755); err != nil {
		t.Fatalf("failed to create target dir: %v", err)
	}

	symlinkPath := filepath.Join(tmpDir, "link_to_dir")
	if err := os.Symlink(targetDir, symlinkPath); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	// Also create a symlink to a file
	targetFile := filepath.Join(tmpDir, "target_file.txt")
	if err := os.WriteFile(targetFile, []byte("test"), 0644); err != nil {
		t.Fatalf("failed to create target file: %v", err)
	}

	fileLinkPath := filepath.Join(tmpDir, "link_to_file")
	if err := os.Symlink(targetFile, fileLinkPath); err != nil {
		t.Fatalf("failed to create file symlink: %v", err)
	}

	result := getPathSuggestions(tmpDir + "/")

	// Find the symlink to directory - should have trailing slash
	dirLinkFound := false
	fileLinkFound := false
	for _, s := range result {
		if strings.Contains(s, "link_to_dir") {
			dirLinkFound = true
			if !strings.HasSuffix(s, "/") {
				t.Errorf("symlink to directory %q should end with /", s)
			}
		}
		if strings.Contains(s, "link_to_file") {
			fileLinkFound = true
			if strings.HasSuffix(s, "/") {
				t.Errorf("symlink to file %q should not end with /", s)
			}
		}
	}

	if !dirLinkFound {
		t.Error("symlink to directory not found in suggestions")
	}
	if !fileLinkFound {
		t.Error("symlink to file not found in suggestions")
	}
}

func TestGetPathSuggestionsTilde(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("failed to get home dir: %v", err)
	}

	// Create a test file in home directory
	testFile := filepath.Join(home, ".blobber_test_file")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer os.Remove(testFile)

	t.Run("tilde slash lists home contents", func(t *testing.T) {
		result := getPathSuggestions("~/")
		if len(result) == 0 {
			t.Fatal("expected suggestions for ~/, got none")
		}
		for _, s := range result {
			if !strings.HasPrefix(s, "~/") {
				t.Errorf("suggestion %q should start with ~/", s)
			}
		}
	})

	t.Run("tilde with partial matches", func(t *testing.T) {
		result := getPathSuggestions("~/.blobber_test")
		found := false
		for _, s := range result {
			if s == "~/.blobber_test_file" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected ~/.blobber_test_file in suggestions, got %v", result)
		}
	})

	t.Run("paths under home converted to tilde format", func(t *testing.T) {
		// When browsing an absolute path under home, results should use ~ format
		result := getPathSuggestions(home + "/")
		if len(result) == 0 {
			t.Fatal("expected suggestions, got none")
		}
		for _, s := range result {
			if !strings.HasPrefix(s, "~/") {
				t.Errorf("suggestion %q should be converted to ~/ format", s)
			}
		}
	})
}

func TestGetPathSuggestionsOrdering(t *testing.T) {
	// Create temp directory with specific files
	tmpDir, err := os.MkdirTemp("", "ordertest")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	files := []string{"alpha.txt", "beta.txt", "gamma.txt"}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(tmpDir, f), []byte("test"), 0644); err != nil {
			t.Fatalf("failed to create file: %v", err)
		}
	}

	result := getPathSuggestions(tmpDir + "/")

	// Verify all files are present
	if len(result) != 3 {
		t.Fatalf("expected 3 suggestions, got %d: %v", len(result), result)
	}

	// Extract just the filenames for comparison
	var names []string
	for _, s := range result {
		names = append(names, filepath.Base(s))
	}

	// The suggestions should contain all files (order depends on os.ReadDir)
	sort.Strings(names)
	expected := []string{"alpha.txt", "beta.txt", "gamma.txt"}
	for i, name := range names {
		if name != expected[i] {
			t.Errorf("expected %v, got %v", expected, names)
			break
		}
	}
}

func TestTruncateString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		maxLen   int
		expected string
	}{
		{
			name:     "short string unchanged",
			input:    "hello",
			maxLen:   10,
			expected: "hello",
		},
		{
			name:     "exact length unchanged",
			input:    "hello",
			maxLen:   5,
			expected: "hello",
		},
		{
			name:     "long string truncated",
			input:    "hello world",
			maxLen:   8,
			expected: "hello...",
		},
		{
			name:     "very short maxLen",
			input:    "hello",
			maxLen:   3,
			expected: "hel",
		},
		{
			name:     "maxLen of 4",
			input:    "hello world",
			maxLen:   4,
			expected: "h...",
		},
		{
			name:     "empty string",
			input:    "",
			maxLen:   10,
			expected: "",
		},
		{
			name:     "unicode string (byte-based)",
			input:    "héllo wörld",
			maxLen:   8,
			expected: "héll...", // 'é' is 2 bytes, so only 4 visible chars + "..."
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncateString(tt.input, tt.maxLen)
			if result != tt.expected {
				t.Errorf("truncateString(%q, %d) = %q, want %q", tt.input, tt.maxLen, result, tt.expected)
			}
		})
	}
}

func TestBackupStepString(t *testing.T) {
	tests := []struct {
		step     backupStep
		expected string
	}{
		{stepIdle, ""},
		{stepDumping, "Dumping database"},
		{stepUploading, "Saving backup"},
		{stepRetention, "Applying retention policy"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.step.String()
			if result != tt.expected {
				t.Errorf("backupStep(%d).String() = %q, want %q", tt.step, result, tt.expected)
			}
		})
	}
}

func TestTestDestinationAccess(t *testing.T) {
	// Create temp directory for testing local destination access
	tmpDir, err := os.MkdirTemp("", "destaccesstest")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	t.Run("local directory succeeds", func(t *testing.T) {
		m := &model{formData: &formFields{dest: tmpDir}}
		success, result := m.testDestinationAccess()
		if !success {
			t.Errorf("expected success for local directory, got: %s", result)
		}
		if !strings.Contains(result, "accessible") {
			t.Errorf("expected 'accessible' in result, got: %s", result)
		}
	})

	t.Run("nonexistent directory fails", func(t *testing.T) {
		m := &model{formData: &formFields{dest: "/nonexistent/path/that/does/not/exist"}}
		success, result := m.testDestinationAccess()
		if success {
			t.Error("expected failure for nonexistent directory")
		}
		if !strings.Contains(result, "not accessible") {
			t.Errorf("expected 'not accessible' in result, got: %s", result)
		}
	})

	t.Run("tilde expansion works", func(t *testing.T) {
		home, _ := os.UserHomeDir()
		// Create a test directory in home
		testDir := filepath.Join(home, ".blobber_dest_test")
		if err := os.MkdirAll(testDir, 0755); err != nil {
			t.Skip("cannot create directory in home")
		}
		defer os.RemoveAll(testDir)

		m := &model{formData: &formFields{dest: "~/.blobber_dest_test"}}
		success, result := m.testDestinationAccess()
		if !success {
			t.Errorf("expected success for tilde path, got: %s", result)
		}
	})
}
