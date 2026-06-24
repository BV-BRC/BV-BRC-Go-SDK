// Command p3-submit-whole-genome-SNP-analysis submits a whole-genome SNP analysis job.
//
// Usage:
//
//	p3-submit-whole-genome-SNP-analysis [options] output-path output-name
//
// This command submits a request for SNP analysis between the genomes in a
// genome group to BV-BRC, identifying single-nucleotide polymorphisms (SNPs).
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

// validThresholds mirrors the THRESHOLDS constant in the Perl script.
var validThresholds = []float64{0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}

var (
	workspacePrefix    string
	workspaceUploadDir string
	overwrite          bool
	dryRun             bool

	// Analysis options
	threshold   float64
	genomeGroup string
)

var rootCmd = &cobra.Command{
	Use:   "p3-submit-whole-genome-SNP-analysis [options] output-path output-name",
	Short: "Submit a whole-genome SNP analysis job to BV-BRC",
	Long: `Submit a request for SNP analysis between the genomes in a genome group.

This finds single-nucleotide polymorphisms (SNPs) between the genomes in the
group, identifying Majority SNPs (see --threshold), Core SNPs (present in all
genomes), and other SNPs.

Examples:

  # Analyze a workspace genome group
  p3-submit-whole-genome-SNP-analysis \
    --group /username@patricbrc.org/home/Genome Groups/MyGroup \
    /username@patricbrc.org/home/snp MyAnalysis`,
	Args: cobra.ExactArgs(2),
	RunE: run,
}

func init() {
	rootCmd.Flags().StringVarP(&workspacePrefix, "workspace-path-prefix", "p", "", "prefix for workspace pathnames")
	rootCmd.Flags().StringVarP(&workspaceUploadDir, "workspace-upload-path", "P", "", "upload directory for local files")
	rootCmd.Flags().BoolVarP(&overwrite, "overwrite", "f", false, "overwrite existing files")
	rootCmd.Flags().BoolVar(&dryRun, "dry-run", false, "validate but don't submit")

	// Analysis options
	rootCmd.Flags().Float64Var(&threshold, "threshold", 0.5, "fraction of genomes that must contain a SNP for it to be a Majority SNP")
	rootCmd.Flags().StringVar(&genomeGroup, "group", "", "workspace genome group file")
}

func run(cmd *cobra.Command, args []string) error {
	outputPath := args[0]
	outputName := args[1]

	// Validate the threshold against the allowed set.
	valid := false
	for _, t := range validThresholds {
		if t == threshold {
			valid = true
			break
		}
	}
	if !valid {
		parts := make([]string, len(validThresholds))
		for i, t := range validThresholds {
			parts[i] = fmt.Sprintf("%g", t)
		}
		return fmt.Errorf("invalid threshold value: %g.  Must be one of: %s", threshold, strings.Join(parts, ", "))
	}

	// Get auth token
	token, err := auth.GetToken()
	if err != nil {
		return fmt.Errorf("getting token: %w", err)
	}
	if token == nil {
		return fmt.Errorf("you must be logged in to BV-BRC via the p3-login command to submit jobs")
	}

	// Create client
	app := appservice.New(appservice.WithToken(token))

	// Fix up the genome group name. This can never be a local file, so a
	// "ws:" prefix is simply stripped off and the workspace prefix applied.
	realGenomeGroup := expandWorkspacePath(strings.TrimPrefix(genomeGroup, "ws:"))

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

	// Build parameters
	params := map[string]interface{}{
		"input_genome_type":  "genome_group",
		"input_genome_group": realGenomeGroup,
		"majority-threshold": threshold,
		"analysis_type":      "Whole Genome SNP Analysis",
		"output_path":        outputPath,
		"output_file":        outputName,
	}

	startParams := appservice.StartParams{}

	if dryRun {
		fmt.Println("Would submit with data:")
		paramsJSON, _ := json.MarshalIndent(params, "", "  ")
		fmt.Println(string(paramsJSON))
		return nil
	}

	// Submit the job
	task, err := app.StartApp2("WholeGenomeSNPAnalysis", params, startParams)
	if err != nil {
		return fmt.Errorf("submitting whole-genome SNP analysis: %w", err)
	}

	fmt.Printf("Submitted whole-genome SNP analysis with id %s\n", task.GetID())
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
