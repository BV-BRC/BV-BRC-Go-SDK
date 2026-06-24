package workspace

import "fmt"

// RequireFolder verifies that the given workspace path exists and is a folder.
// It mirrors the Perl UploadSpec output-path check, which stats the output path
// and dies with "Output path ... does not exist" if it is missing or not a
// directory. Submit commands call this on their resolved output path so the Go
// CLI fails as early and clearly as the Perl CLI does.
func (c *Client) RequireFolder(path string) error {
	meta, err := c.Stat(path, false)
	if err != nil || meta == nil {
		return fmt.Errorf("output path %s does not exist", path)
	}
	if !meta.IsFolder() {
		return fmt.Errorf("output path %s is not a folder", path)
	}
	return nil
}
