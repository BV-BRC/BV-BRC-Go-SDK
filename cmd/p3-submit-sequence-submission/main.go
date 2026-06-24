// Command p3-submit-sequence-submission submits a viral sequence submission job.
//
// Usage:
//
//	p3-submit-sequence-submission [options] output-path output-name
//
// This script submits a request to validate a viral GENBANK sequence
// submission. The sequences are validated and annotated along with the
// associated metadata and submission information. The output is a folder in
// the specified workspace directory that will contain an error report or, if
// the submission is valid, a validation report.
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

	// Input options
	fasta    string
	metadata string

	// Submitter information
	affiliation string
	firstName   string
	lastName    string
	email       string
	consortium  string
	country     string
	phone       string
	street      string
	postalCode  string
	city        string
	state       string
)

var rootCmd = &cobra.Command{
	Use:   "p3-submit-sequence-submission [options] output-path output-name",
	Short: "Submit a viral sequence submission job to BV-BRC",
	Long: `Submit a request to validate a viral GENBANK sequence submission.

The sequences are validated and annotated along with the associated metadata
and submission information. The output is a folder in the specified workspace
directory that will contain an error report or, if the submission is valid, a
validation report.

Examples:

  p3-submit-sequence-submission \
    --fasta sequences.fasta --metadata metadata.csv \
    --first-name Jane --last-name Doe --email jane@example.org \
    /username@patricbrc.org/home/submissions MySubmission`,
	Args: cobra.ExactArgs(2),
	RunE: run,
}

func init() {
	rootCmd.Flags().StringVarP(&workspacePrefix, "workspace-path-prefix", "p", "", "prefix for workspace pathnames")
	rootCmd.Flags().StringVarP(&workspaceUploadDir, "workspace-upload-path", "P", "", "upload directory for local files")
	rootCmd.Flags().BoolVarP(&overwrite, "overwrite", "f", false, "overwrite existing files")
	rootCmd.Flags().BoolVar(&dryRun, "dry-run", false, "validate but don't submit")

	// Input options
	rootCmd.Flags().StringVar(&fasta, "fasta", "", "FASTA input file of individual viral DNA sequences (required)")
	rootCmd.Flags().StringVar(&metadata, "metadata", "", "CSV file containing the submission metadata (required)")

	// Submitter information
	rootCmd.Flags().StringVar(&affiliation, "affiliation", "", "name of the submitter's affiliated institution")
	rootCmd.Flags().StringVar(&firstName, "first-name", "", "first name of the submitter (required)")
	rootCmd.Flags().StringVar(&lastName, "last-name", "", "last name of the submitter (required)")
	rootCmd.Flags().StringVar(&email, "email", "", "email address of the submitter (required)")
	rootCmd.Flags().StringVar(&consortium, "consortium", "", "name of the consortium on behalf of which the submission is made")
	rootCmd.Flags().StringVar(&country, "country", "", "country in which the submitter is located")
	rootCmd.Flags().StringVar(&phone, "phone", "", "the submitter's phone number")
	rootCmd.Flags().StringVar(&street, "street", "", "the submitter's street address")
	rootCmd.Flags().StringVar(&postalCode, "postal-code", "", "the submitter's postal code")
	rootCmd.Flags().StringVar(&city, "city", "", "city in which the submitter is located")
	rootCmd.Flags().StringVar(&state, "state", "", "state or province in which the submitter is located")
}

func run(cmd *cobra.Command, args []string) error {
	outputPath := args[0]
	outputName := args[1]

	// Validate required inputs (mirrors the Perl GetOptions validation).
	if fasta == "" {
		return fmt.Errorf("a FASTA file is required")
	}
	if metadata == "" {
		return fmt.Errorf("a metadata file is required")
	}
	if firstName == "" || lastName == "" || email == "" {
		return fmt.Errorf("first name, last name, and email are required submitter information")
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
		"affiliation": affiliation,
		"first_name":  firstName,
		"last_name":   lastName,
		"email":       email,
		"consortium":  consortium,
		"country":     country,
		"phoneNumber": phone,
		"street":      street,
		"postal_code": postalCode,
		"city":        city,
		"state":       state,
	}

	// Validate and upload (if necessary) the FASTA input file.
	fastaFile, err := processFilename(ws, fasta, "contigs", token)
	if err != nil {
		return err
	}
	params["input_fasta_file"] = fastaFile
	params["input_source"] = "fasta_file"

	// Validate and upload (if necessary) the metadata input file.
	metadataFile, err := processFilename(ws, metadata, "csv", token)
	if err != nil {
		return err
	}
	params["metadata"] = metadataFile

	startParams := appservice.StartParams{}

	if dryRun {
		fmt.Println("Would submit with data:")
		paramsJSON, _ := json.MarshalIndent(params, "", "  ")
		fmt.Println(string(paramsJSON))
		return nil
	}

	// Submit the job.
	task, err := app.StartApp2("SequenceSubmission", params, startParams)
	if err != nil {
		return fmt.Errorf("submitting sequence submission: %w", err)
	}

	fmt.Printf("Submitted sequence submission with id %s\n", task.GetID())
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
