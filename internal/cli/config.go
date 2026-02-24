package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func newConfigCmd() *cobra.Command {
	var client string

	cmd := &cobra.Command{
		Use:   "config",
		Short: "Generate MCP client configuration snippets",
		Long:  `Generate configuration snippets for popular MCP clients (Claude Code, Cursor, VS Code, Claude Desktop).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runConfig(client, args)
		},
	}

	cmd.Flags().StringVar(&client, "client", "", "MCP client name (claude-code, cursor, vscode, claude-desktop)")

	return cmd
}

func runConfig(client string, args []string) error {
	if client == "" {
		fmt.Fprintln(os.Stderr, "Available MCP clients:")
		fmt.Fprintln(os.Stderr, "  --client claude-code     Claude Code")
		fmt.Fprintln(os.Stderr, "  --client cursor          Cursor IDE")
		fmt.Fprintln(os.Stderr, "  --client vscode          VS Code")
		fmt.Fprintln(os.Stderr, "  --client claude-desktop  Claude Desktop")
		return nil
	}

	dsn := "<your-database-dsn>"
	if len(args) > 0 {
		dsn = args[0]
	}

	snippet := generateSnippet(client, dsn)
	fmt.Println(snippet)
	return nil
}

func generateSnippet(client, dsn string) string {
	switch client {
	case "claude-code":
		return fmt.Sprintf("# Add to Claude Code:\nclaude mcp add conduit -- conduit %s", dsn)

	case "cursor":
		data, _ := json.MarshalIndent(map[string]any{
			"mcpServers": map[string]any{
				"conduit": map[string]any{
					"command": "conduit",
					"args":    []string{dsn},
				},
			},
		}, "", "  ")
		return fmt.Sprintf("# Add to .cursor/mcp.json:\n%s", string(data))

	case "vscode":
		data, _ := json.MarshalIndent(map[string]any{
			"servers": map[string]any{
				"conduit": map[string]any{
					"type":    "stdio",
					"command": "conduit",
					"args":    []string{dsn},
				},
			},
		}, "", "  ")
		return fmt.Sprintf("# Add to .vscode/mcp.json:\n%s", string(data))

	case "claude-desktop":
		data, _ := json.MarshalIndent(map[string]any{
			"mcpServers": map[string]any{
				"conduit": map[string]any{
					"command": "conduit",
					"args":    []string{dsn},
				},
			},
		}, "", "  ")
		return fmt.Sprintf("# Add to Claude Desktop config:\n%s", string(data))

	default:
		return fmt.Sprintf("Unknown client %q. Use: claude-code, cursor, vscode, claude-desktop", client)
	}
}
