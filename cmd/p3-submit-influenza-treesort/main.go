// Command p3-submit-influenza-treesort submits an influenza reassortment
// analysis job.
//
// Usage:
//
//	p3-submit-influenza-treesort [options] output-path output-name
//
// This script submits a request to build a reassortment of influenza virus
// segments. The reassortment uses the phylogenetic tree of a reference segment
// to infer which other segments belong with it.
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

// segmentNames is the set of permissible influenza segment names.
var segmentNames = map[string]bool{
	"PB2": true, "PB1": true, "PA": true, "HA": true,
	"NP": true, "NA": true, "MP": true, "NS": true,
}

// methodNames is the set of valid reassortment inference methods.
var methodNames = map[string]bool{
	"local": true, "mincut": true,
}

// infNames is the set of valid reference tree inference methods.
var infNames = map[string]bool{
	"FastTree": true, "IQTree": true,
}

var (
	workspacePrefix    string
	workspaceUploadDir string
	overwrite          bool
	dryRun             bool

	// TreeSort options
	fasta      string
	ref        string
	names      string
	method     string
	inf        string
	maxDev     float64
	cutoff     float64
	cladesFile string
	clock      bool
	noCollapse bool
)

var rootCmd = &cobra.Command{
	Use:   "p3-submit-influenza-treesort [options] output-path output-name",
	Short: "Submit an influenza reassortment analysis job to BV-BRC",
	Long: `Submit a request to build a reassortment of influenza virus segments.

The reassortment uses the phylogenetic tree of a reference segment to infer
which other segments belong with it.

Example:

  p3-submit-influenza-treesort --fasta segments.fasta --ref HA --names HA,NA \
    /username@patricbrc.org/home/treesort MyReassortment`,
	Args: cobra.ExactArgs(2),
	RunE: run,
}

func init() {
	rootCmd.Flags().StringVarP(&workspacePrefix, "workspace-path-prefix", "p", "", "prefix for workspace pathnames")
	rootCmd.Flags().StringVarP(&workspaceUploadDir, "workspace-upload-path", "P", "", "upload directory for local files")
	rootCmd.Flags().BoolVarP(&overwrite, "overwrite", "f", false, "overwrite existing files")
	rootCmd.Flags().BoolVar(&dryRun, "dry-run", false, "validate but don't submit")

	// TreeSort options
	rootCmd.Flags().StringVar(&fasta, "fasta", "", "FASTA input file (local or ws:)")
	rootCmd.Flags().StringVar(&ref, "ref", "HA", "reference segment name")
	rootCmd.Flags().StringVar(&names, "names", "HA,NA", "comma-delimited list of segment names")
	rootCmd.Flags().StringVar(&method, "method", "local", "tree building method (local or mincut)")
	rootCmd.Flags().StringVar(&inf, "inf", "FastTree", "inference method (FastTree or IQTree)")
	rootCmd.Flags().Float64Var(&maxDev, "max-dev", 2.0, "maximum deviation from the standard substitution rate")
	rootCmd.Flags().Float64Var(&cutoff, "cutoff", 0.001, "cutoff p-value for the reassortment tests")
	rootCmd.Flags().StringVar(&cladesFile, "clades-file", "", "optional output file for clades with evidence of reassortment")
	rootCmd.Flags().BoolVar(&clock, "clock", false, "estimate molecular clock rates assuming equal rates")
	rootCmd.Flags().BoolVar(&noCollapse, "no-collapse", false, "disable collapsing of near-zero-length branches")
}

func run(cmd *cobra.Command, args []string) error {
	outputPath := args[0]
	outputName := args[1]

	// Fix the clock parameter.
	equalRates := 0
	if clock {
		equalRates = 1
	}
	// Fix the no-collapse parameter.
	noCollapseVal := 0
	if noCollapse {
		noCollapseVal = 1
	}

	// Validate the segment names, adding the reference segment.
	segments := map[string]bool{}
	for _, seg := range strings.Split(names, ",") {
		segments[seg] = true
	}
	segments[ref] = true
	for seg := range segments {
		if !segmentNames[seg] {
			return fmt.Errorf("invalid segment name specified: %s", seg)
		}
	}
	// Format the segment names for the service (sorted, comma-joined).
	segList := make([]string, 0, len(segments))
	for seg := range segments {
		segList = append(segList, seg)
	}
	sort.Strings(segList)
	formattedNames := strings.Join(segList, ",")

	// Validate the tuning parameters.
	if !methodNames[method] {
		return fmt.Errorf("invalid method specified")
	}
	if !infNames[inf] {
		return fmt.Errorf("invalid inference method specified")
	}
	if maxDev <= 1.0 {
		return fmt.Errorf("invalid max deviation specified-- must be greater than 1.0")
	}
	if cutoff <= 0.0 || cutoff > 1.0 {
		return fmt.Errorf("invalid cutoff specified-- must be between 0 and 1")
	}

	// A FASTA file must be specified.
	if fasta == "" {
		return fmt.Errorf("a FASTA file must be specified")
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

	// Validate and upload (if necessary) the FASTA input file.
	realFastaFileName, err := processFilename(ws, fasta, "contigs", token)
	if err != nil {
		return err
	}

	// Build the parameter structure.
	params := map[string]interface{}{
		"segments":                     formattedNames,
		"ref_segment":                  ref,
		"inference_method":             method,
		"ref_tree_inference":           inf,
		"deviation":                    maxDev,
		"p_value":                      cutoff,
		"clades_path":                  cladesFile,
		"output_path":                  outputPath,
		"output_file":                  outputName,
		"input_fasta_file_id":          realFastaFileName,
		"equal_rates":                  equalRates,
		"no_collapse":                  noCollapseVal,
		"match_regex":                  nil,
		"input_fasta_data":             nil,
		"input_fasta_existing_dataset": nil,
		"input_fasta_group_id":         nil,
		"input_source":                 "fasta_file_id",
	}

	startParams := appservice.StartParams{}

	if dryRun {
		fmt.Println("Would submit with data:")
		paramsJSON, _ := json.MarshalIndent(params, "", "  ")
		fmt.Println(string(paramsJSON))
		return nil
	}

	// Submit the job.
	task, err := app.StartApp2("TreeSort", params, startParams)
	if err != nil {
		return fmt.Errorf("submitting influenza tree sort: %w", err)
	}

	fmt.Printf("Submitted influenza tree sort with id %s\n", task.GetID())
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
