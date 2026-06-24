// Command p3-get-genome-group retrieves genome IDs from a workspace genome group.
//
// Usage:
//
//	p3-get-genome-group [options] group-name
//
// Retrieve a genome group from a BV-BRC workspace. Use the --title option to
// specify the output column header. A value of "none" will omit the header;
// the default is the group name followed by ".genome_id".
//
// If group-name starts with a /, the genome group will be located using that
// path. Otherwise it will be read from the folder "Genome Groups" in the
// user's default workspace (/<username>/home/Genome Groups/<group-name>).
//
// Examples:
//
//	# List genome IDs in a group
//	p3-get-genome-group "My Pathogens"
//
//	# List by full workspace path
//	p3-get-genome-group /username@patricbrc.org/home/Genome Groups/My Pathogens
//
//	# Suppress the header
//	p3-get-genome-group --title none "My Pathogens"
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/workspace"
	"github.com/spf13/cobra"
)

var (
	titleFlag    string
	workspaceURL string
)

var rootCmd = &cobra.Command{
	Use:   "p3-get-genome-group group-name",
	Short: "Retrieve genome IDs from a workspace genome group",
	Long: `Retrieve a genome group from a BV-BRC workspace and output its genome IDs.

Use --title to specify the output column header. A value of "none" omits the
header entirely. The default header is <group-name>.genome_id.

If group-name starts with /, it is used as an absolute workspace path.
Otherwise the group is looked up in the Genome Groups folder of the user's
default workspace.

Examples:

  # List genome IDs in a group
  p3-get-genome-group "My Pathogens"

  # List by full workspace path
  p3-get-genome-group /username@patricbrc.org/home/Genome Groups/My Pathogens

  # Suppress the header
  p3-get-genome-group --title none "My Pathogens"`,
	Args:         cobra.ExactArgs(1),
	RunE:         run,
	SilenceUsage: true,
}

func init() {
	rootCmd.Flags().StringVarP(&titleFlag, "title", "t", "", "output column title (default: <group-name>.genome_id; use 'none' to omit)")
	rootCmd.Flags().StringVar(&workspaceURL, "url", "", "workspace URL")
}

// genomeGroupData represents the JSON structure stored in a genome group object.
type genomeGroupData struct {
	IDList struct {
		GenomeID []string `json:"genome_id"`
	} `json:"id_list"`
}

func run(cmd *cobra.Command, args []string) error {
	groupName := args[0]

	// Require authentication
	token, err := auth.GetToken()
	if err != nil || token == nil {
		return fmt.Errorf("you must login with p3-login")
	}

	// Build the title
	title := titleFlag
	if title == "" {
		title = groupName + ".genome_id"
	} else if strings.ToLower(title) == "none" {
		title = ""
	}

	// Determine workspace path
	var groupPath string
	if strings.HasPrefix(groupName, "/") {
		groupPath = groupName
	} else {
		// Home workspace is /<userID>/home
		home := "/" + token.UserID + "/home"
		groupPath = home + "/Genome Groups/" + groupName
	}

	// Create workspace client
	wsOpts := []workspace.Option{workspace.WithToken(token)}
	if workspaceURL != "" {
		wsOpts = append(wsOpts, workspace.WithURL(workspaceURL))
	}
	ws := workspace.New(wsOpts...)

	// Fetch the genome group object
	results, err := ws.Get(workspace.GetParams{
		Objects: []string{groupPath},
	})
	if err != nil {
		return fmt.Errorf("retrieving genome group %q: %w", groupPath, err)
	}
	if len(results) == 0 || results[0] == nil {
		return fmt.Errorf("genome group not found: %s", groupPath)
	}

	dataTxt := results[0].Data
	if dataTxt == "" {
		return fmt.Errorf("genome group %s has no data", groupPath)
	}

	// Parse the JSON group data
	var groupData genomeGroupData
	if err := json.Unmarshal([]byte(dataTxt), &groupData); err != nil {
		return fmt.Errorf("parsing genome group data: %w", err)
	}

	// Print header if requested
	if title != "" {
		fmt.Println(title)
	}

	// Print genome IDs
	for _, id := range groupData.IDList.GenomeID {
		fmt.Println(id)
	}

	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
