// Command p3-put-feature-group creates or replaces a feature group in the BV-BRC workspace.
//
// Usage:
//
//	p3-put-feature-group [options] group-name < feature-ids
//
// Push feature IDs to a BV-BRC feature group. The standard input should be a
// tab-delimited file containing patric_id values. The IDs in the named group
// will be replaced entirely. If the group does not exist, it will be created.
//
// If group-name starts with a /, it is used as an absolute workspace path.
// Otherwise the group is created in the folder "Feature Groups" in the user's
// default workspace (/<username>/home/Feature Groups/<group-name>).
//
// Examples:
//
//	# Push feature IDs from a file
//	p3-get-feature-data --attr patric_id < genomes.txt | p3-put-feature-group MyFeatureGroup
//
//	# Use a full path
//	p3-put-feature-group /username@patricbrc.org/home/Feature Groups/MyGroup < ids.txt
//
//	# Specify which column contains the IDs
//	p3-put-feature-group --col patric_id MyGroup < data.txt
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/BV-BRC/BV-BRC-Go-SDK/api"
	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/BV-BRC/BV-BRC-Go-SDK/workspace"
	"github.com/spf13/cobra"
)

var (
	colOpts cli.ColOptions
	ioOpts  cli.IOOptions
)

var rootCmd = &cobra.Command{
	Use:   "p3-put-feature-group [options] group-name",
	Short: "Create or replace a feature group in the BV-BRC workspace",
	Long: `Push feature IDs to a BV-BRC feature group. The standard input should be a
tab-delimited file containing patric_id values. The IDs in the named group
will be replaced entirely. If the group does not exist, it will be created.

If group-name starts with a /, it is used as an absolute workspace path.
Otherwise the group is created in the folder "Feature Groups" in the user's
default workspace.

Examples:

  # Push feature IDs from a file
  p3-get-feature-data --attr patric_id < genomes.txt | p3-put-feature-group MyFeatureGroup

  # Use a full path
  p3-put-feature-group /username@patricbrc.org/home/Feature Groups/MyGroup < ids.txt

  # Specify which column contains the IDs
  p3-put-feature-group --col patric_id MyGroup < data.txt`,
	Args:         cobra.ExactArgs(1),
	RunE:         run,
	SilenceUsage: true,
}

func init() {
	cli.AddColFlags(rootCmd, &colOpts, 0)
	cli.AddIOFlags(rootCmd, &ioOpts)
}

// featureGroupData represents the JSON structure of a feature group.
type featureGroupData struct {
	IDList struct {
		FeatureID []string `json:"feature_id"`
	} `json:"id_list"`
}

func run(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	group := args[0]

	// Require authentication
	token, err := auth.GetToken()
	if err != nil {
		return fmt.Errorf("getting token: %w", err)
	}
	if token == nil {
		return fmt.Errorf("you must login with p3-login")
	}

	// Build the workspace path
	var groupPath string
	if strings.HasPrefix(group, "/") {
		groupPath = group
	} else {
		home := fmt.Sprintf("/%s/home", token.UserID)
		groupPath = home + "/Feature Groups/" + group
	}

	// Open input
	var input io.Reader
	if ioOpts.Input != "" && ioOpts.Input != "-" {
		f, err := os.Open(ioOpts.Input)
		if err != nil {
			return fmt.Errorf("opening input file: %w", err)
		}
		defer f.Close()
		input = f
	} else {
		input = os.Stdin
	}

	// Read patric_ids from stdin
	reader := cli.NewTabReader(input, !colOpts.NoHead)

	_, err = reader.Headers()
	if err != nil {
		return fmt.Errorf("reading headers: %w", err)
	}

	keyCol, err := reader.FindColumn(colOpts.Col)
	if err != nil {
		return fmt.Errorf("finding column: %w", err)
	}

	var patricIDs []string
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading input: %w", err)
		}

		var val string
		if keyCol < 0 {
			// Last column
			if len(row) > 0 {
				val = row[len(row)-1]
			}
		} else if keyCol < len(row) {
			val = row[keyCol]
		}

		if val != "" {
			patricIDs = append(patricIDs, val)
		}
	}

	// Look up feature_id for each patric_id via the API in batches of 500
	clientOpts := []api.ClientOption{api.WithToken(token)}
	apiClient := api.NewClient(clientOpts...)

	const batchSize = 500
	featureIDs := make([]string, 0, len(patricIDs))

	for i := 0; i < len(patricIDs); i += batchSize {
		end := i + batchSize
		if end > len(patricIDs) {
			end = len(patricIDs)
		}
		chunk := patricIDs[i:end]

		query := api.NewQuery().Select("feature_id", "patric_id").In("patric_id", chunk...)

		results, err := apiClient.Query(ctx, "genome_feature", query)
		if err != nil {
			return fmt.Errorf("querying features: %w", err)
		}

		// Build lookup: patric_id -> feature_id
		lookup := make(map[string]string, len(results))
		for _, r := range results {
			pid, _ := r["patric_id"].(string)
			fid, _ := r["feature_id"].(string)
			if pid != "" {
				lookup[pid] = fid
			}
		}

		// Preserve input order
		for _, pid := range chunk {
			fid := lookup[pid]
			featureIDs = append(featureIDs, fid)
		}
	}

	// Build the feature group JSON
	var groupData featureGroupData
	groupData.IDList.FeatureID = featureIDs

	groupJSON, err := json.Marshal(groupData)
	if err != nil {
		return fmt.Errorf("encoding feature group data: %w", err)
	}

	// Create/overwrite the workspace feature group object
	ws := workspace.New(workspace.WithToken(token))
	_, err = ws.Create(workspace.CreateParams{
		Objects: []workspace.CreateObject{
			{
				Path: groupPath,
				Type: "feature_group",
				Data: string(groupJSON),
			},
		},
		Permission: "w",
		Overwrite:  true,
	})
	if err != nil {
		return fmt.Errorf("creating feature group: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Feature group saved to %s (%d features)\n", groupPath, len(featureIDs))
	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
