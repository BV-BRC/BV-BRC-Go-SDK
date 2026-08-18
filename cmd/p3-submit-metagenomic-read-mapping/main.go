// Command p3-submit-metagenomic-read-mapping submits a metagenomic read mapping job.
//
// Usage:
//
//	p3-submit-metagenomic-read-mapping [options] output-path output-name
//
// This command submits reads to be mapped against CARD or VFDB databases.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BV-BRC/BV-BRC-Go-SDK/appservice"
	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	_ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliversion"
	"github.com/BV-BRC/BV-BRC-Go-SDK/workspace"
	"github.com/spf13/cobra"
)

var (
	workspacePrefix    string
	workspaceUploadDir string
	overwrite          bool
	dryRun             bool

	pairedEndLibs []string
	singleEndLibs []string
	srrIDs        []string
	validateSRR   bool
	geneSetName   string
)

var validGeneSets = map[string]bool{
	"CARD": true, "VFDB": true,
}

var rootCmd = &cobra.Command{
	Use:   "p3-submit-metagenomic-read-mapping [options] output-path output-name",
	Short: "Submit a metagenomic read mapping job to BV-BRC",
	Long: `Submit reads to be mapped against CARD or VFDB databases.

Examples:

  # Map reads against CARD database (antimicrobial resistance)
  p3-submit-metagenomic-read-mapping --paired-end-lib reads_1.fq,reads_2.fq \
    /username@patricbrc.org/home/mapping MyMapping

  # Map reads against VFDB database (virulence factors)
  p3-submit-metagenomic-read-mapping --gene-set-name VFDB \
    --paired-end-lib reads_1.fq,reads_2.fq \
    /username@patricbrc.org/home/mapping MyMapping`,
	Args: cobra.ExactArgs(2),
	RunE: run,
}

func init() {
	rootCmd.Flags().StringVarP(&workspacePrefix, "workspace-path-prefix", "p", "", "prefix for workspace pathnames")
	rootCmd.Flags().StringVarP(&workspaceUploadDir, "workspace-upload-path", "P", "", "upload directory for local files")
	rootCmd.Flags().BoolVarP(&overwrite, "overwrite", "f", false, "overwrite existing files")
	rootCmd.Flags().BoolVar(&dryRun, "dry-run", false, "validate but don't submit")

	rootCmd.Flags().StringArrayVar(&pairedEndLibs, "paired-end-lib", nil, cli.PairedEndLibUsage)
	rootCmd.Flags().StringArrayVar(&singleEndLibs, "single-end-lib", nil, "single-end read library")
	rootCmd.Flags().StringArrayVar(&srrIDs, "srr-id", nil, "SRA run ID")
	rootCmd.Flags().BoolVar(&validateSRR, "validate-srr", false, cli.ValidateSRRUsage)
	rootCmd.Flags().StringVar(&geneSetName, "gene-set-name", "CARD", "gene set to use (CARD or VFDB)")
}

func run(cmd *cobra.Command, args []string) error {
	outputPath := args[0]
	outputName := args[1]

	// Validate gene set
	if !validGeneSets[geneSetName] {
		return fmt.Errorf("invalid gene set name: %s (must be CARD or VFDB)", geneSetName)
	}

	// Validate input
	if len(pairedEndLibs) == 0 && len(singleEndLibs) == 0 && len(srrIDs) == 0 {
		return fmt.Errorf("at least one read library or SRR ID must be specified")
	}

	// Get auth token
	token, err := auth.GetToken()
	if err != nil {
		return fmt.Errorf("getting token: %w", err)
	}
	if token == nil {
		return fmt.Errorf("you must be logged in to BV-BRC via the p3-login command to submit jobs")
	}

	ws := workspace.New(workspace.WithToken(token))
	app := appservice.New(appservice.WithToken(token))

	outputPath = strings.TrimPrefix(outputPath, "ws:")
	outputPath = expandWorkspacePath(outputPath)
	outputPath = strings.TrimSuffix(outputPath, "/")

	if !dryRun {
		if err := ws.RequireFolder(outputPath); err != nil {
			return err
		}
	}

	if workspaceUploadDir == "" {
		workspaceUploadDir = outputPath
	}

	// Look the SRA accessions up before touching any read files, so a bad
	// accession fails the run before anything is uploaded.
	if _, err := cli.LookupSRRTitles(validateSRR, srrIDs); err != nil {
		cmd.SilenceUsage = true
		return err
	}

	params := map[string]interface{}{
		"gene_set_type":   "predefined_list",
		"gene_set_fasta":  "",
		"gene_set_name":   geneSetName,
		"output_path":     outputPath,
		"output_file":     outputName,
		"paired_end_libs": []map[string]interface{}{},
		"single_end_libs": []map[string]interface{}{},
	}

	pairedLibs := params["paired_end_libs"].([]map[string]interface{})
	for _, lib := range pairedEndLibs {
		f1, f2, err := cli.SplitPairedEndLib(lib)
		if err != nil {
			return err
		}
		read1, err := processFilename(ws, f1, "reads", token)
		if err != nil {
			return err
		}
		read2, err := processFilename(ws, f2, "reads", token)
		if err != nil {
			return err
		}
		pairedLibs = append(pairedLibs, map[string]interface{}{
			"read1": read1,
			"read2": read2,
		})
	}
	params["paired_end_libs"] = pairedLibs

	singleLibs := params["single_end_libs"].([]map[string]interface{})
	for _, lib := range singleEndLibs {
		read, err := processFilename(ws, lib, "reads", token)
		if err != nil {
			return err
		}
		singleLibs = append(singleLibs, map[string]interface{}{
			"read": read,
		})
	}
	params["single_end_libs"] = singleLibs

	if len(srrIDs) > 0 {
		params["srr_ids"] = srrIDs
	}

	startParams := appservice.StartParams{}

	if dryRun {
		fmt.Println("Would submit with data:")
		paramsJSON, _ := json.MarshalIndent(params, "", "  ")
		fmt.Println(string(paramsJSON))
		return nil
	}

	task, err := app.StartApp2("MetagenomicReadMapping", params, startParams)
	if err != nil {
		return fmt.Errorf("submitting metagenomic read mapping: %w", err)
	}

	fmt.Printf("Submitted metagenomic read mapping with id %s\n", task.GetID())
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
	if strings.HasPrefix(path, "ws:") {
		wsPath := expandWorkspacePath(strings.TrimPrefix(path, "ws:"))
		meta, err := ws.Stat(wsPath, false)
		if err != nil || meta.IsFolder() {
			return "", fmt.Errorf("workspace path %s not found", wsPath)
		}
		return wsPath, nil
	}

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

	existing, _ := ws.Stat(wsPath, false)
	if existing != nil && !overwrite {
		return "", fmt.Errorf("target path %s already exists and --overwrite not specified", wsPath)
	}

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
	os.Args = cli.NormalizePairedEndLibArgs(os.Args)
	if err := cliversion.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}
