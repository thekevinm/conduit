package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// version is set by the serve command from main.go's ldflags.
var version = "dev"

// NewRootCmd creates the root Cobra command.
func NewRootCmd(ver, commit, date string) *cobra.Command {
	version = ver

	rootCmd := &cobra.Command{
		Use:   "conduit [DSN]",
		Short: "One binary. Every database. Your AI's data layer.",
		Long: `Conduit auto-generates MCP (Model Context Protocol) servers from databases.
Point it at any SQL database and get typed, per-table tools for AI agents in seconds.

Usage:
  conduit <DSN>                    Start MCP server (stdio) for a database
  conduit serve <DSN> --http       Start HTTP MCP server with dashboard
  conduit demo                     Demo with sample data (no database needed)
  conduit config --client cursor   Generate MCP client config`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			return runServe(args[0], serveFlags{stdio: true})
		},
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	rootCmd.AddCommand(
		newServeCmd(),
		newDemoCmd(),
		newConfigCmd(),
		newVersionCmd(ver, commit, date),
	)

	return rootCmd
}

func newVersionCmd(ver, commit, date string) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("conduit %s\n", ver)
			if commit != "none" {
				fmt.Printf("  commit: %s\n", commit)
			}
			if date != "unknown" {
				fmt.Printf("  built:  %s\n", date)
			}
		},
	}
}
