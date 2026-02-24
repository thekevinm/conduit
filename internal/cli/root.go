package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// NewRootCmd creates the root Cobra command.
func NewRootCmd(version, commit, date string) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "conduit",
		Short: "One binary. Every database. Your AI's data layer.",
		Long: `Conduit auto-generates MCP (Model Context Protocol) servers from databases.
Point it at any SQL database and get typed, per-table tools for AI agents in seconds.

Usage:
  conduit <DSN>                    Start MCP server (stdio) for a database
  conduit serve <DSN>              Start HTTP MCP server
  conduit demo                     Demo with sample data (no database needed)
  conduit init                     Interactive setup wizard`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			// If a DSN is provided as positional arg, start stdio mode
			return runServe(args[0], serveFlags{stdio: true})
		},
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	rootCmd.AddCommand(
		newServeCmd(),
		newDemoCmd(),
		newVersionCmd(version, commit, date),
	)

	return rootCmd
}

func newVersionCmd(version, commit, date string) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("conduit %s\n", version)
			if commit != "none" {
				fmt.Printf("  commit: %s\n", commit)
			}
			if date != "unknown" {
				fmt.Printf("  built:  %s\n", date)
			}
		},
	}
}
