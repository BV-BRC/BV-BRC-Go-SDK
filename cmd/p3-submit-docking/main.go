// Command p3-submit-docking submits a protein-ligand docking job.
//
// Usage:
//
//	p3-submit-docking [options] output-path output-name
//
// This script submits a request to attempt docking of ligands against a
// protein. The protein is specified as a PDB file or a PDB ID, and the ligands
// can either be in a SMILES file or one of three pre-defined named libraries.
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

// ligandLibMap maps the user-facing named library options to the internal
// library identifiers, mirroring the Perl LIGAND_LIB_MAP constant.
var ligandLibMap = map[string]string{
	"exemplar":     "small_db",
	"approved":     "approved-drugs",
	"experimental": "experimental_drugs",
}

var (
	workspacePrefix    string
	workspaceUploadDir string
	overwrite          bool
	dryRun             bool

	// Protein input options.
	pdbFile string
	pdbID   string

	// Ligand input options.
	ligandsFile string
	ligandsLib  string

	// Tuning options.
	samplesPerComplex int
	inferenceSteps    int
)

var rootCmd = &cobra.Command{
	Use:   "p3-submit-docking [options] output-path output-name",
	Short: "Submit a protein-ligand docking job to BV-BRC",
	Long: `Submit a request to attempt docking of ligands against a protein.

The protein is specified as a PDB file or a PDB ID, and the ligands can either
be in a SMILES file or one of three pre-defined named libraries.

Examples:

  # Dock an approved-drugs library against a PDB ID
  p3-submit-docking --pdb-id 1abc --ligands-lib approved \
    /username@patricbrc.org/home/docking MyDocking

  # Dock a local SMILES file against a local PDB file
  p3-submit-docking --pdb-file protein.pdb --ligands-file ligands.txt \
    /username@patricbrc.org/home/docking MyDocking`,
	Args: cobra.ExactArgs(2),
	RunE: run,
}

func init() {
	rootCmd.Flags().StringVarP(&workspacePrefix, "workspace-path-prefix", "p", "", "prefix for workspace pathnames")
	rootCmd.Flags().StringVarP(&workspaceUploadDir, "workspace-upload-path", "P", "", "upload directory for local files")
	rootCmd.Flags().BoolVarP(&overwrite, "overwrite", "f", false, "overwrite existing files")
	rootCmd.Flags().BoolVar(&dryRun, "dry-run", false, "validate but don't submit")

	// Protein input options.
	rootCmd.Flags().StringVar(&pdbFile, "pdb-file", "", "PDB input file for the protein (local or ws:; type pdb)")
	rootCmd.Flags().StringVar(&pdbID, "pdb-id", "", "PDB ID of the protein to be used for docking")

	// Ligand input options.
	rootCmd.Flags().StringVar(&ligandsFile, "ligands-file", "", "SMILES file for the ligands (local or ws:; type txt)")
	rootCmd.Flags().StringVar(&ligandsLib, "ligands-lib", "", "named ligand library: exemplar, approved, or experimental")

	// Tuning options.
	rootCmd.Flags().IntVar(&samplesPerComplex, "samples-per-complex", 10, "number of pose samples per protein-ligand pair")
	rootCmd.Flags().IntVar(&inferenceSteps, "inference-steps", 20, "number of diffusion steps for pose generation")
}

func run(cmd *cobra.Command, args []string) error {
	outputPath := args[0]
	outputName := args[1]

	// Validate the tuning parameters.
	if samplesPerComplex < 1 {
		return fmt.Errorf("invalid samples per complex specified-- must be greater than 0")
	}
	if inferenceSteps < 1 {
		return fmt.Errorf("invalid inference steps specified-- must be greater than 0")
	}

	// Get auth token.
	token, err := auth.GetToken()
	if err != nil {
		return fmt.Errorf("getting token: %w", err)
	}
	if token == nil {
		return fmt.Errorf("you must be logged in to BV-BRC via the p3-login command to submit jobs")
	}

	// Create clients.
	ws := workspace.New(workspace.WithToken(token))
	app := appservice.New(appservice.WithToken(token))

	// Clean output path.
	outputPath = strings.TrimPrefix(outputPath, "ws:")
	outputPath = expandWorkspacePath(outputPath)
	outputPath = strings.TrimSuffix(outputPath, "/")

	if !dryRun {
		if err := ws.RequireFolder(outputPath); err != nil {
			return err
		}
	}

	// Set upload path default.
	if workspaceUploadDir == "" {
		workspaceUploadDir = outputPath
	}

	// Build the parameter structure.
	params := map[string]interface{}{
		"samples_per_complex": samplesPerComplex,
		"inference_steps":     inferenceSteps,
		"batch_size":          10,
		"output_path":         outputPath,
		"output_file":         outputName,
	}

	// Handle the protein input. Either a file or a PDB ID.
	if pdbFile != "" {
		fixed, err := processFilename(ws, pdbFile, "pdb", token)
		if err != nil {
			return err
		}
		params["user_pdb_file"] = []string{fixed}
		params["protein_input_type"] = "user_pdb_file"
	} else if pdbID != "" {
		params["input_pdb"] = []string{pdbID}
		params["protein_input_type"] = "input_pdb"
	} else {
		return fmt.Errorf("no protein specified-- either --pdb-file or --pdb-id must be provided")
	}

	// Handle the ligand input. Either a SMILES file or a named library.
	if ligandsFile != "" {
		fixed, err := processFilename(ws, ligandsFile, "txt", token)
		if err != nil {
			return err
		}
		params["ligand_ws_file"] = fixed
		params["ligand_library_type"] = "ws_file"
	} else if ligandsLib != "" {
		libID, ok := ligandLibMap[ligandsLib]
		if !ok {
			return fmt.Errorf("invalid ligand library specified-- must be one of exemplar, approved, experimental")
		}
		params["ligand_named_library"] = libID
		params["ligand_library_type"] = "named_library"
	} else {
		return fmt.Errorf("no ligands specified-- either --ligands-file or --ligands-lib must be provided")
	}

	startParams := appservice.StartParams{}

	if dryRun {
		fmt.Println("Would submit with data:")
		paramsJSON, _ := json.MarshalIndent(params, "", "  ")
		fmt.Println(string(paramsJSON))
		return nil
	}

	// Submit the job.
	task, err := app.StartApp2("Docking", params, startParams)
	if err != nil {
		return fmt.Errorf("submitting docking: %w", err)
	}

	fmt.Printf("Submitted protein-ligand docking with id %s\n", task.GetID())
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
