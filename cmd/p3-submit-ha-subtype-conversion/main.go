// Command p3-submit-ha-subtype-conversion submits an Influenza HA Subtype
// Numbering Conversion job.
//
// Usage:
//
//	p3-submit-ha-subtype-conversion [options] output-path output-name
//
// This script submits a request to renumber influenza HA protein sequences
// according to function rather than position. The proteins are compared to the
// selected reference proteins and the numbering is adjusted accordingly.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/BV-BRC/BV-BRC-Go-SDK/appservice"
	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/workspace"
	"github.com/spf13/cobra"
)

var (
	workspacePrefix    string
	workspaceUploadDir string
	overwrite          bool
	dryRun             bool

	// HA subtype conversion options
	fasta string
	group string
	types string
)

// proteinNames is the set of permissible HA protein types.
var proteinNames = map[string]bool{
	"H1PR34": true, "H11933": true, "H1post1995": true, "H1N1pdm": true,
	"H2": true, "H3": true, "H4": true, "H5mEAnonGsGD": true, "H5": true,
	"H5c221": true, "H6": true, "H7N3": true, "H7N7": true, "H8": true,
	"H9": true, "H10": true, "H11": true, "H12": true, "H13": true,
	"H14": true, "H15": true, "H16": true, "H17": true, "H18": true,
	"BHongKong": true, "BFlorida": true, "BBrisbane": true,
}

var rootCmd = &cobra.Command{
	Use:   "p3-submit-ha-subtype-conversion [options] output-path output-name",
	Short: "Submit an Influenza HA subtype numbering conversion job to BV-BRC",
	Long: `Submit a request to renumber influenza HA protein sequences according to
function rather than position. The proteins are compared to the selected
reference proteins and the numbering is adjusted accordingly.

Examples:

  # Submit using a local FASTA file
  p3-submit-ha-subtype-conversion --fasta proteins.fasta \
    /username@patricbrc.org/home/ha MyConversion

  # Submit using a workspace feature group
  p3-submit-ha-subtype-conversion --group /username@patricbrc.org/home/groups/MyGroup \
    /username@patricbrc.org/home/ha MyConversion`,
	Args: cobra.ExactArgs(2),
	RunE: run,
}

func init() {
	rootCmd.Flags().StringVarP(&workspacePrefix, "workspace-path-prefix", "p", "", "prefix for workspace pathnames")
	rootCmd.Flags().StringVarP(&workspaceUploadDir, "workspace-upload-path", "P", "", "upload directory for local files")
	rootCmd.Flags().BoolVarP(&overwrite, "overwrite", "f", false, "overwrite existing files")
	rootCmd.Flags().BoolVar(&dryRun, "dry-run", false, "validate but don't submit")

	// HA subtype conversion options
	rootCmd.Flags().StringVar(&fasta, "fasta", "", "name of a FASTA input file of HA influenza protein sequences (prefix with ws: for a workspace file)")
	rootCmd.Flags().StringVar(&group, "group", "", "path of a workspace feature group of influenza HA features")
	rootCmd.Flags().StringVar(&types, "types", "H3,H4", "comma-delimited list of HA protein types")
}

func run(cmd *cobra.Command, args []string) error {
	outputPath := args[0]
	outputName := args[1]

	// Exactly one of --fasta or --group is required.
	if fasta != "" && group != "" {
		return fmt.Errorf("cannot specify both a FASTA file and a feature group")
	}
	if fasta == "" && group == "" {
		return fmt.Errorf("must specify either a FASTA file or a feature group")
	}

	// Parse and validate the types list.
	typeList := strings.Split(types, ",")
	for _, t := range typeList {
		if !proteinNames[t] {
			valid := make([]string, 0, len(proteinNames))
			for k := range proteinNames {
				valid = append(valid, k)
			}
			sort.Strings(valid)
			return fmt.Errorf("invalid HA protein type %q. Valid types are: %s.", t, strings.Join(valid, ", "))
		}
	}

	// Get auth token
	token, err := auth.GetToken()
	if err != nil {
		return fmt.Errorf("getting token: %w", err)
	}
	if token == nil {
		return fmt.Errorf("you must be logged in to BV-BRC via the p3-login command to submit jobs")
	}

	// Create clients
	ws := workspace.New(workspace.WithToken(token))
	app := appservice.New(appservice.WithToken(token))

	// Clean output path
	outputPath = strings.TrimPrefix(outputPath, "ws:")
	outputPath = expandWorkspacePath(outputPath)
	outputPath = strings.TrimSuffix(outputPath, "/")

	if !dryRun {
		if err := ws.RequireFolder(outputPath); err != nil {
			return err
		}
	}

	// Set upload path default
	if workspaceUploadDir == "" {
		workspaceUploadDir = outputPath
	}

	// Build parameters
	params := map[string]interface{}{
		"output_path": outputPath,
		"output_file": outputName,
		"types":       typeList,
	}

	if fasta != "" {
		// Validate and upload (if necessary) the FASTA input file.
		fastaPath, err := processFilename(ws, fasta, "feature_protein_fasta", token)
		if err != nil {
			return err
		}
		params["input_fasta_file"] = fastaPath
		params["input_source"] = "fasta_file"
	} else {
		// Validate the feature group and set the parameter.
		groupPath := expandWorkspacePath(strings.TrimPrefix(group, "ws:"))
		params["input_feature_group"] = groupPath
		params["input_source"] = "feature_group"
	}

	startParams := appservice.StartParams{}

	if dryRun {
		fmt.Println("Would submit with data:")
		paramsJSON, _ := json.MarshalIndent(params, "", "  ")
		fmt.Println(string(paramsJSON))
		return nil
	}

	// Submit the job
	task, err := app.StartApp2("HASubtypeNumberingConversion", params, startParams)
	if err != nil {
		return fmt.Errorf("submitting HA subtype numbering conversion: %w", err)
	}

	fmt.Printf("Submitted HA subtype numbering conversion with id %s\n", task.GetID())
	return nil
}

func expandWorkspacePath(path string) string {
	if strings.HasPrefix(path, "/") {
		return path
	}
	if workspacePrefix == "" {
		return path
	}
	return strings.TrimSuffix(workspacePrefix, "/") + "/" + path
}

func processFilename(ws *workspace.Client, path, fileType string, token *auth.Token) (string, error) {
	// Check if it's a workspace path
	if strings.HasPrefix(path, "ws:") {
		wsPath := expandWorkspacePath(strings.TrimPrefix(path, "ws:"))
		meta, err := ws.Stat(wsPath, false)
		if err != nil || meta.IsFolder() {
			return "", fmt.Errorf("workspace path %s not found", wsPath)
		}
		return wsPath, nil
	}

	// Local file - needs upload
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("local file %s does not exist", path)
	}
	if info.IsDir() {
		return "", fmt.Errorf("%s is a directory", path)
	}

	if workspaceUploadDir == "" {
		return "", fmt.Errorf("upload requested for %s but no upload path specified", path)
	}

	fileName := filepath.Base(path)
	wsPath := workspaceUploadDir + "/" + fileName

	// Check if file exists
	existing, _ := ws.Stat(wsPath, false)
	if existing != nil && !overwrite {
		return "", fmt.Errorf("target path %s already exists and --overwrite not specified", wsPath)
	}

	// Upload the file
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("reading file: %w", err)
	}

	fmt.Printf("Uploading %s to %s...\n", path, wsPath)
	_, err = ws.Create(workspace.CreateParams{
		Objects: []workspace.CreateObject{{
			Path: wsPath,
			Type: fileType,
			Data: string(data),
		}},
		Overwrite: overwrite,
	})
	if err != nil {
		return "", fmt.Errorf("uploading file: %w", err)
	}
	fmt.Println("done")

	return wsPath, nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
