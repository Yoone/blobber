package cmd

import (
	"fmt"

	"github.com/Yoone/blobber/internal/version"
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("blobber " + version.Full())
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
