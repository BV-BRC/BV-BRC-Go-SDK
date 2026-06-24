// Command p3-list-genome-groups lists genome groups in your workspace.
//
// Usage:
//
//	p3-list-genome-groups [options]
//
// List genome groups in the user's workspace. The groups are listed from the
// "Genome Groups" folder in the user's home workspace, sorted alphabetically.
//
// Examples:
//
//	# List all genome groups
//	p3-list-genome-groups
package main

import (
	"fmt"
	"os"
	"sort"

	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/workspace"
	"github.com/spf13/cobra"
)

var (
	workspaceURL string
)

var rootCmd = &cobra.Command{
	Use:   "p3-list-genome-groups",
	Short: "List genome groups in your workspace",
	Long: `List genome groups in the BV-BRC workspace.

Reads the "Genome Groups" folder in the user's home workspace and prints the
names of all genome group objects, sorted alphabetically.

Examples:

  # List all genome groups
  p3-list-genome-groups`,
	Args:         cobra.NoArgs,
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

	// Build the Genome Groups path from the user's home workspace
	home := "/" + token.UserID + "/home"
	groupPath := home + "/Genome Groups"

	// Create workspace client
	wsOpts := []workspace.Option{workspace.WithToken(token)}
	if workspaceURL != "" {
		wsOpts = append(wsOpts, workspace.WithURL(workspaceURL))
	}
	ws := workspace.New(wsOpts...)

	// List the Genome Groups folder
	result, err := ws.Ls(workspace.LsParams{
		Paths: []string{groupPath},
	})
	if err != nil {
		return fmt.Errorf("listing genome groups: %w", err)
	}

	entries := result[groupPath]

	// Collect names of genome_group type objects
	var groups []string
	for _, meta := range entries {
		if meta.Type == "genome_group" {
			groups = append(groups, meta.Name)
		}
	}

	// Sort alphabetically, matching the Perl sort { $a cmp $b }
	sort.Strings(groups)

	for _, name := range groups {
		fmt.Println(name)
	}

	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
