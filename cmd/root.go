package cmd

import (
	"fmt"
	"os"

	"github.com/Yoone/blobber/internal/config"
	"github.com/Yoone/blobber/internal/storage"
	"github.com/Yoone/blobber/internal/tui"
	"github.com/Yoone/blobber/internal/version"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var cfgFile string
var rcloneCfgFile string
var cfg *config.Config
var cfgPath string

var rootCmd = &cobra.Command{
	Use:   "blobber",
	Short: "Database backup and restore tool with cloud storage",
	Long: `Blobber backs up and restores databases (SQLite, MySQL, PostgreSQL) to cloud storage using rclone.

Run without arguments to launch the interactive TUI.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Initialize rclone storage with optional custom config
		storage.Init(rcloneCfgFile)

		// For TUI mode (root command), allow empty config
		if cmd.Name() == "blobber" {
			return loadConfigAllowEmpty()
		}
		// For subcommands, require valid config with databases
		return loadConfigStrict()
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		// Check if we have a TTY on both stdin and stdout - if not, show help instead of TUI
		if !term.IsTerminal(int(os.Stdin.Fd())) || !term.IsTerminal(int(os.Stdout.Fd())) {
			return cmd.Help()
		}
		// Launch TUI
		return tui.Run(cfg, version.String())
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default: ./blobber.yaml)")
	rootCmd.PersistentFlags().StringVar(&rcloneCfgFile, "rclone-config", "", "rclone config file (default: ~/.config/rclone/rclone.conf)")
}

func getConfigPath() string {
	if cfgFile != "" {
		return cfgFile
	}
	return "blobber.yaml"
}

func loadConfigAllowEmpty() error {
	var err error
	cfg, err = config.LoadOrEmpty(getConfigPath())
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}
	return nil
}

func loadConfigStrict() error {
	var err error
	cfg, err = config.Load(getConfigPath())
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}
	return nil
}
