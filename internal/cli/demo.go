package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newDemoCmd() *cobra.Command {
	var httpMode bool
	var port int

	cmd := &cobra.Command{
		Use:   "demo",
		Short: "Start with embedded sample data (no database needed)",
		Long: `Start Conduit in demo mode with an embedded SQLite database
containing sample e-commerce data (customers, products, orders, reviews).
Perfect for trying out Conduit without setting up a database.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDemo(httpMode, port)
		},
	}

	cmd.Flags().BoolVar(&httpMode, "http", false, "Use HTTP transport instead of stdio")
	cmd.Flags().IntVarP(&port, "port", "p", 8090, "HTTP server port")

	return cmd
}

func runDemo(httpMode bool, port int) error {
	// TODO: Wire up demo mode with embedded SQLite
	fmt.Println("conduit demo: starting with sample data...")
	return nil
}
