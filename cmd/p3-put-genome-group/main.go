// Command p3-put-genome-group pushes genome IDs from stdin to a workspace genome group.
//
// Usage:
//
//	p3-put-genome-group [options] group-name < genome-ids
//
// Push genome IDs to a BV-BRC workspace genome group. The standard input should
// be a tab-delimited file containing genome IDs. The specified genome IDs will
// replace whatever is already in the named group. If the group does not exist,
// it will be created.
//
// If group-name starts with a /, the genome group will be created using that
// path. Otherwise it will be created in the folder "Genome Groups" in the
// user's default workspace (/<username>/home/Genome Groups/<group-name>).
//
// Examples:
//
//	# Push genome IDs to a named group
//	p3-put-genome-group "My Pathogens" < genome-ids.txt
//
//	# Push genome IDs to a group at an explicit workspace path
//	p3-put-genome-group /username@patricbrc.org/home/Genome Groups/My Pathogens < genome-ids.txt
//
//	# Read from a specific column
//	p3-put-genome-group --col genome_id "My Pathogens" < data.txt
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/BV-BRC/BV-BRC-Go-SDK/workspace"
	"github.com/spf13/cobra"
)

var (
	colOpts      cli.ColOptions
	ioOpts       cli.IOOptions
	workspaceURL string
)

var rootCmd = &cobra.Command{
	Use:   "p3-put-genome-group [options] group-name",
	Short: "Push genome IDs from stdin to a workspace genome group",
	Long: `Push genome IDs to a BV-BRC workspace genome group. The standard input should
be a tab-delimited file containing genome IDs. The specified genome IDs will
replace whatever is already in the named group. If the group does not exist,
it will be created.

If group-name starts with a /, the genome group will be created using that
path. Otherwise it will be created in the folder "Genome Groups" in the
user's default workspace.

Examples:

  # Push genome IDs to a named group
  p3-put-genome-group "My Pathogens" < genome-ids.txt

  # Push genome IDs to a group at an explicit workspace path
  p3-put-genome-group /username@patricbrc.org/home/Genome Groups/My Pathogens < genome-ids.txt

  # Read from a specific column
  p3-put-genome-group --col genome_id "My Pathogens" < data.txt`,
	Args:         cobra.ExactArgs(1),
	RunE:         run,
	SilenceUsage: true,
}

func init() {
	cli.AddColFlags(rootCmd, &colOpts, 0)
	cli.AddIOFlags(rootCmd, &ioOpts)
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

	// Determine workspace path
	var groupPath string
	if strings.HasPrefix(groupName, "/") {
		groupPath = groupName
	} else {
		home := "/" + token.UserID + "/home"
		groupPath = home + "/Genome Groups/" + groupName
	}

	// Open input
	inFile, err := cli.OpenInput(ioOpts.Input)
	if err != nil {
		return fmt.Errorf("opening input: %w", err)
	}
	defer inFile.Close()

	reader := cli.NewTabReader(inFile, !colOpts.NoHead)

	// Read (and discard) headers, then find key column
	_, err = reader.Headers()
	if err != nil && err != io.EOF {
		return fmt.Errorf("reading headers: %w", err)
	}

	keyCol, err := reader.FindColumn(colOpts.Col)
	if err != nil {
		return fmt.Errorf("finding column: %w", err)
	}

	// Collect genome IDs from the input
	var genomeIDs []string
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading row: %w", err)
		}

		var id string
		if keyCol < 0 {
			// Last column
			if len(row) > 0 {
				id = row[len(row)-1]
			}
		} else if keyCol < len(row) {
			id = row[keyCol]
		}

		if id != "" {
			genomeIDs = append(genomeIDs, id)
		}
	}

	// Build the genome group JSON
	var groupData genomeGroupData
	groupData.IDList.GenomeID = genomeIDs
	if groupData.IDList.GenomeID == nil {
		groupData.IDList.GenomeID = []string{}
	}

	groupJSON, err := json.Marshal(groupData)
	if err != nil {
		return fmt.Errorf("encoding genome group data: %w", err)
	}

	// Create workspace client
	wsOpts := []workspace.Option{workspace.WithToken(token)}
	if workspaceURL != "" {
		wsOpts = append(wsOpts, workspace.WithURL(workspaceURL))
	}
	ws := workspace.New(wsOpts...)

	// Create (or overwrite) the genome group in the workspace
	_, err = ws.Create(workspace.CreateParams{
		Objects: []workspace.CreateObject{
			{
				Path: groupPath,
				Type: "genome_group",
				Data: string(groupJSON),
			},
		},
		Permission: "w",
		Overwrite:  true,
	})
	if err != nil {
		return fmt.Errorf("creating genome group: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Genome group %q saved with %d genome IDs.\n", groupPath, len(genomeIDs))
	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
