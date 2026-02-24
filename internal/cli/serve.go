package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

type serveFlags struct {
	stdio       bool
	port        int
	host        string
	allowWrites bool
	allowRawSQL bool
	maskPII     bool
	maxRows     int
	authToken   string
	configFile  string
}

func newServeCmd() *cobra.Command {
	var flags serveFlags

	cmd := &cobra.Command{
		Use:   "serve [DSN...]",
		Short: "Start the MCP server",
		Long:  `Start the Conduit MCP server. By default uses stdio transport. Use --http for Streamable HTTP.`,
		Args:  cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 && flags.configFile == "" {
				return fmt.Errorf("provide a DSN or use --config")
			}
			dsn := ""
			if len(args) > 0 {
				dsn = args[0]
			}
			return runServe(dsn, flags)
		},
	}

	cmd.Flags().BoolVar(&flags.stdio, "stdio", false, "Use stdio transport (default when no --http)")
	cmd.Flags().IntVarP(&flags.port, "port", "p", 8090, "HTTP server port")
	cmd.Flags().StringVar(&flags.host, "host", "localhost", "HTTP server host")
	cmd.Flags().BoolVar(&flags.allowWrites, "allow-writes", false, "Enable write operations (insert/update/delete)")
	cmd.Flags().BoolVar(&flags.allowRawSQL, "allow-raw-sql", false, "Enable raw SQL tool")
	cmd.Flags().BoolVar(&flags.maskPII, "mask-pii", false, "Mask PII columns in output")
	cmd.Flags().IntVar(&flags.maxRows, "max-rows", 1000, "Maximum rows per query")
	cmd.Flags().StringVar(&flags.authToken, "auth-token", "", "Bearer token for HTTP auth")
	cmd.Flags().StringVarP(&flags.configFile, "config", "c", "", "Path to config file")

	return cmd
}

func runServe(dsn string, flags serveFlags) error {
	// TODO: Wire up the full server pipeline
	fmt.Println("conduit: starting server...")
	fmt.Printf("  DSN: %s\n", dsn)
	return nil
}
