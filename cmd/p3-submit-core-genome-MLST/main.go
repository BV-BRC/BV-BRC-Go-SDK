// Command p3-submit-core-genome-MLST submits a core genome MLST analysis job.
//
// Usage:
//
//	p3-submit-core-genome-MLST [options] output-path output-name
//
// This command submits a request for a MultiLocus Sequence Typing (MLST) job
// to BV-BRC. It accepts as input a genome group, and selects core and accessory
// functions based on the selected species. It then analyzes the genomes for how
// they differ from each other regarding those functions.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

	// Analysis options
	species     string
	genomeGroup string
)

var rootCmd = &cobra.Command{
	Use:   "p3-submit-core-genome-MLST [options] output-path output-name",
	Short: "Submit a core genome MLST analysis job to BV-BRC",
	Long: `Submit a request for a MultiLocus Sequence Typing (MLST) job to BV-BRC.

It accepts as input a genome group, and selects core and accessory functions
based on the selected species. It then analyzes the genomes for how they differ
from each other regarding those functions.

Examples:

  # Run a core genome MLST analysis on a workspace genome group
  p3-submit-core-genome-MLST --species "Escherichia coli" \
    --group /username@patricbrc.org/home/Genome Groups/MyGroup \
    /username@patricbrc.org/home/mlst MyResults`,
	Args: cobra.ExactArgs(2),
	RunE: run,
}

func init() {
	rootCmd.Flags().StringVarP(&workspacePrefix, "workspace-path-prefix", "p", "", "prefix for workspace pathnames")
	rootCmd.Flags().StringVarP(&workspaceUploadDir, "workspace-upload-path", "P", "", "upload directory for local files")
	rootCmd.Flags().BoolVarP(&overwrite, "overwrite", "f", false, "overwrite existing files")
	rootCmd.Flags().BoolVar(&dryRun, "dry-run", false, "validate but don't submit")

	// Analysis options
	rootCmd.Flags().StringVar(&species, "species", "", "species for the MLST schema selection (required)")
	rootCmd.Flags().StringVar(&genomeGroup, "group", "", "workspace genome group")
}

func run(cmd *cobra.Command, args []string) error {
	outputPath := args[0]
	outputName := args[1]

	// Species is required.
	if species == "" {
		return fmt.Errorf("a species must be specified with --species")
	}

	// Fix the species name. The user can use spaces, but we want underscores
	// in the service call.
	realSpecies := strings.Join(strings.Fields(species), "_")

	// Get auth token
	token, err := auth.GetToken()
	if err != nil {
		return fmt.Errorf("getting token: %w", err)
	}
	if token == nil {
		return fmt.Errorf("you must be logged in to BV-BRC via the p3-login command to submit jobs")
	}

	// Create clients
	app := appservice.New(appservice.WithToken(token))

	// Clean output path
	outputPath = strings.TrimPrefix(outputPath, "ws:")
	outputPath = expandWorkspacePath(outputPath)
	outputPath = strings.TrimSuffix(outputPath, "/")

	if !dryRun {
		ws := workspace.New(workspace.WithToken(token))
		if err := ws.RequireFolder(outputPath); err != nil {
			return err
		}
	}

	// Set upload path default
	if workspaceUploadDir == "" {
		workspaceUploadDir = outputPath
	}

	// Fix up the genome group name. This can never be a local file, so a
	// leading "ws:" prefix is simply stripped off before path expansion.
	var realGenomeGroup string
	if genomeGroup != "" {
		realGenomeGroup = expandWorkspacePath(strings.TrimPrefix(genomeGroup, "ws:"))
	}

	// Build parameters
	params := map[string]interface{}{
		"input_genome_type":      "genome_group",
		"input_genome_group":     realGenomeGroup,
		"input_schema_selection": realSpecies,
		"analysis_type":          "chewbbaca",
		"output_path":            outputPath,
		"output_file":            outputName,
	}

	startParams := appservice.StartParams{}

	if dryRun {
		fmt.Println("Would submit with data:")
		paramsJSON, _ := json.MarshalIndent(params, "", "  ")
		fmt.Println(string(paramsJSON))
		return nil
	}

	// Submit the job
	task, err := app.StartApp2("CoreGenomeMLST", params, startParams)
	if err != nil {
		return fmt.Errorf("submitting core genome MLST: %w", err)
	}

	fmt.Printf("Submitted core genome MLST with id %s\n", task.GetID())
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
