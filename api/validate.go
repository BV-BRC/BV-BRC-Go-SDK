package api

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// ValidateGenomeIDs checks that every supplied genome ID exists in BV-BRC and
// returns the list of IDs that were not found (empty when all are valid). This
// mirrors the Perl GenomeIdSpec::validate_genomes behavior, which queries the
// data API genome collection and reports any IDs missing from the result.
func (c *Client) ValidateGenomeIDs(ctx context.Context, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	q := NewQuery().Select("genome_id").In("genome_id", ids...)
	rows, err := c.Query(ctx, "genome", q)
	if err != nil {
		return nil, err
	}
	found := make(map[string]bool, len(rows))
	for _, r := range rows {
		if g, ok := r["genome_id"].(string); ok {
			found[g] = true
		}
	}
	var missing []string
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		if !found[id] {
			missing = append(missing, id)
		}
	}
	sort.Strings(missing)
	return missing, nil
}

// RequireGenomeIDs validates the genome IDs and returns an error naming any that
// do not exist, matching the Perl scripts that die on an invalid genome ID.
func (c *Client) RequireGenomeIDs(ctx context.Context, ids []string) error {
	missing, err := c.ValidateGenomeIDs(ctx, ids)
	if err != nil {
		return fmt.Errorf("validating genome IDs: %w", err)
	}
	if len(missing) > 0 {
		return fmt.Errorf("invalid genome ID(s): %s", strings.Join(missing, ", "))
	}
	return nil
}
