// Command p3-get-feature-group retrieves a feature group from the BV-BRC workspace.
//
// Usage:
//
//	p3-get-feature-group [options] group-name
//
// Retrieve a feature group from the BV-BRC workspace and output the patric_id
// for each feature in the group. If group-name starts with a /, the feature
// group will be located using that path. Otherwise it will be read from the
// Feature Groups folder in the user's default workspace.
//
// Examples:
//
//	# List features in a group
//	p3-get-feature-group MyFeatureGroup
//
//	# Use a full path
//	p3-get-feature-group /username@patricbrc.org/home/Feature Groups/MyGroup
//
//	# Suppress the header line
//	p3-get-feature-group --title none MyFeatureGroup
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/BV-BRC/BV-BRC-Go-SDK/api"
	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/workspace"
	"github.com/spf13/cobra"
)

var (
	title string
)

var rootCmd = &cobra.Command{
	Use:   "p3-get-feature-group group-name",
	Short: "Retrieve a feature group from the BV-BRC workspace",
	Long: `Retrieve a feature group from the BV-BRC workspace and output the
patric_id for each feature in the group.

If group-name starts with a /, the feature group will be located using that
path. Otherwise it will be read from the Feature Groups folder in the user's
default workspace.

Use --title to specify the output column header. A value of 'none' will omit
the header; the default is the group name followed by .patric_id.

Examples:

  # List features in a group
  p3-get-feature-group MyFeatureGroup

  # Use a full path
  p3-get-feature-group /username@patricbrc.org/home/Feature Groups/MyGroup

  # Suppress the header line
  p3-get-feature-group --title none MyFeatureGroup`,
	Args:         cobra.ExactArgs(1),
	RunE:         run,
	SilenceUsage: true,
}

func init() {
	rootCmd.Flags().StringVarP(&title, "title", "t", "", "output column title (use 'none' to omit header)")
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

	// Determine the output column title
	hdr := title
	if hdr == "" {
		hdr = group + ".patric_id"
	} else if hdr == "none" {
		hdr = ""
	}

	// Require authentication
	token, err := auth.GetToken()
	if err != nil {
		return fmt.Errorf("getting token: %w", err)
	}
	if token == nil {
		return fmt.Errorf("you must login with p3-login")
	}

	// Print header if requested
	if hdr != "" {
		fmt.Println(hdr)
	}

	// Build the workspace path
	var groupPath string
	if len(group) > 0 && group[0] == '/' {
		groupPath = group
	} else {
		// Use the user's default workspace
		home := fmt.Sprintf("/%s/home", token.UserID)
		groupPath = home + "/Feature Groups/" + group
	}

	// Fetch the feature group from the workspace
	ws := workspace.New(workspace.WithToken(token))
	results, err := ws.Get(workspace.GetParams{
		Objects: []string{groupPath},
	})
	if err != nil {
		return fmt.Errorf("reading feature group %s: %w", groupPath, err)
	}
	if len(results) == 0 {
		return fmt.Errorf("feature group not found: %s", groupPath)
	}

	// Parse the feature group JSON
	var groupData featureGroupData
	if err := json.Unmarshal([]byte(results[0].Data), &groupData); err != nil {
		return fmt.Errorf("parsing feature group data: %w", err)
	}

	members := groupData.IDList.FeatureID
	if len(members) == 0 {
		return nil
	}

	// Create API client (token optional for public data but required here since
	// we already validated above)
	clientOpts := []api.ClientOption{api.WithToken(token)}
	client := api.NewClient(clientOpts...)

	// Process in batches of 500 (matching the Perl implementation)
	const batchSize = 500
	for i := 0; i < len(members); i += batchSize {
		end := i + batchSize
		if end > len(members) {
			end = len(members)
		}
		chunk := members[i:end]

		// Query feature_id -> patric_id mapping
		query := api.NewQuery().Select("feature_id", "patric_id").In("feature_id", chunk...)

		apiResults, err := client.Query(ctx, "feature", query)
		if err != nil {
			return fmt.Errorf("querying features: %w", err)
		}

		// Build lookup map: feature_id -> patric_id
		lookup := make(map[string]string, len(apiResults))
		for _, r := range apiResults {
			fid, _ := r["feature_id"].(string)
			pid, _ := r["patric_id"].(string)
			if fid != "" {
				lookup[fid] = pid
			}
		}

		// Output patric_id values in original order
		for _, fid := range chunk {
			fmt.Println(lookup[fid])
		}
	}

	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
