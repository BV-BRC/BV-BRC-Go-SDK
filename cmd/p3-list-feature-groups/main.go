// Command p3-list-feature-groups lists feature groups in your workspace.
//
// Usage:
//
//	p3-list-feature-groups [options]
//
// List feature groups in the user's default workspace (/<username>/home/Feature Groups).
//
// Examples:
//
//	# List all feature groups
//	p3-list-feature-groups
package main

import (
	"fmt"
	"os"
	"sort"

	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/workspace"
	"github.com/spf13/cobra"
)

var workspaceURL string

var rootCmd = &cobra.Command{
	Use:   "p3-list-feature-groups",
	Short: "List feature groups in your workspace",
	Long: `List feature groups in the user's default BV-BRC workspace.

Reads from /<username>/home/Feature Groups and prints the name of each
feature_group object, sorted alphabetically.

Examples:

  # List all feature groups
  p3-list-feature-groups`,
	Args:         cobra.ExactArgs(0),
	RunE:         run,
	SilenceUsage: true,
}

func init() {
	rootCmd.Flags().StringVar(&workspaceURL, "url", "", "workspace URL")
}

func run(cmd *cobra.Command, args []string) error {
	// Require authentication
	token, err := auth.GetToken()
	if err != nil || token == nil {
		return fmt.Errorf("you must login with p3-login")
	}

	// Build the Feature Groups path
	home := fmt.Sprintf("/%s/home", token.UserID)
	groupPath := home + "/Feature Groups"

	// Create workspace client
	wsOpts := []workspace.Option{workspace.WithToken(token)}
	if workspaceURL != "" {
		wsOpts = append(wsOpts, workspace.WithURL(workspaceURL))
	}
	ws := workspace.New(wsOpts...)

	// List contents of the Feature Groups folder
	result, err := ws.Ls(workspace.LsParams{
		Paths: []string{groupPath},
	})
	if err != nil {
		return fmt.Errorf("listing feature groups at %s: %w", groupPath, err)
	}

	entries := result[groupPath]

	// Collect names of feature_group entries
	var names []string
	for _, meta := range entries {
		if meta != nil && meta.Type == "feature_group" {
			names = append(names, meta.Name)
		}
	}

	// Sort alphabetically (matching the Perl: sort { $a cmp $b })
	sort.Strings(names)

	// Print each name
	for _, name := range names {
		fmt.Println(name)
	}

	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
