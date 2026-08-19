package seeddir

import (
	"fmt"
	"strings"
)

// standardGeneticCode is NCBI translation table 1, and the base every other
// table here is a handful of edits away from. It is SeedUtils::standard_genetic_code.
var standardGeneticCode = map[string]byte{
	"AAA": 'K', "AAC": 'N', "AAG": 'K', "AAT": 'N',
	"ACA": 'T', "ACC": 'T', "ACG": 'T', "ACT": 'T',
	"AGA": 'R', "AGC": 'S', "AGG": 'R', "AGT": 'S',
	"ATA": 'I', "ATC": 'I', "ATG": 'M', "ATT": 'I',
	"CAA": 'Q', "CAC": 'H', "CAG": 'Q', "CAT": 'H',
	"CCA": 'P', "CCC": 'P', "CCG": 'P', "CCT": 'P',
	"CGA": 'R', "CGC": 'R', "CGG": 'R', "CGT": 'R',
	"CTA": 'L', "CTC": 'L', "CTG": 'L', "CTT": 'L',
	"GAA": 'E', "GAC": 'D', "GAG": 'E', "GAT": 'D',
	"GCA": 'A', "GCC": 'A', "GCG": 'A', "GCT": 'A',
	"GGA": 'G', "GGC": 'G', "GGG": 'G', "GGT": 'G',
	"GTA": 'V', "GTC": 'V', "GTG": 'V', "GTT": 'V',
	"TAA": '*', "TAC": 'Y', "TAG": '*', "TAT": 'Y',
	"TCA": 'S', "TCC": 'S', "TCG": 'S', "TCT": 'S',
	"TGA": '*', "TGC": 'C', "TGG": 'W', "TGT": 'C',
	"TTA": 'L', "TTC": 'F', "TTG": 'L', "TTT": 'F',
}

// geneticCodeEdits is what each supported table changes about table 1.
//
// The supported set is SeedUtils::genetic_code's: 1, 2, 3, 4 and 11, with
// anything else an error rather than a silent mistranslation. The edits for 2
// and 3 differ from that function's, which spells its overrides in RNA (AUA,
// UGA, CUU...) while the table it edits and the DNA it translates are both
// spelled in DNA -- so in Perl every U-bearing override is looked up under a
// key that can never occur, and codes 2 and 3 quietly translate as code 1.
// These are the real NCBI tables. Nothing in the BV-BRC annotation pipeline
// uses either code, so this changes no output in practice; codes 1, 4 and 11
// are identical to the Perl.
var geneticCodeEdits = map[int]map[string]byte{
	1:  {},
	11: {}, // Differs from table 1 only in its start codons, which are not used here.
	2:  {"AGA": '*', "AGG": '*', "ATA": 'M', "TGA": 'W'},
	3:  {"ATA": 'M', "CTT": 'T', "CTC": 'T', "CTA": 'T', "CTG": 'T', "TGA": 'W'},
	4:  {"TGA": 'W'},
}

// GeneticCode returns the codon table for an NCBI translation table number.
func GeneticCode(number int) (map[string]byte, error) {
	edits, ok := geneticCodeEdits[number]
	if !ok {
		return nil, fmt.Errorf("genetic code %d is not supported; only codes 1, 2, 3, 4 and 11 are", number)
	}

	code := make(map[string]byte, len(standardGeneticCode))
	for codon, aa := range standardGeneticCode {
		code[codon] = aa
	}
	for codon, aa := range edits {
		code[codon] = aa
	}
	return code, nil
}

// Translate turns DNA into protein, one codon at a time, as
// SeedUtils::translate does.
//
// A codon that is not in the table -- one holding an N, most often -- becomes
// an X rather than an error. A trailing partial codon is dropped. When start is
// true an initial GTG or TTG is read as the alternative start codon it is and
// becomes an M; note that this is the whole of the Perl's start handling, so
// the other alternative starts are not special-cased here either.
func Translate(dna string, code map[string]byte, start bool) string {
	dna = strings.ToUpper(dna)

	protein := make([]byte, 0, len(dna)/3)
	for i := 0; i+3 <= len(dna); i += 3 {
		aa, ok := code[dna[i:i+3]]
		if !ok {
			aa = 'X'
		}
		protein = append(protein, aa)
	}

	if start && len(protein) > 0 && (strings.HasPrefix(dna, "GTG") || strings.HasPrefix(dna, "TTG")) {
		protein[0] = 'M'
	}

	return string(protein)
}

// complement is SeedUtils::rev_comp's tr///, IUPAC ambiguity codes and all. A
// base it does not know is left alone.
var complement = strings.NewReplacer(
	"a", "t", "c", "g", "g", "c", "t", "a", "u", "a",
	"m", "k", "r", "y", "w", "w", "s", "s", "y", "r", "k", "m",
	"b", "v", "d", "h", "h", "d", "v", "b",
	"A", "T", "C", "G", "G", "C", "T", "A", "U", "A",
	"M", "K", "R", "Y", "W", "W", "S", "S", "Y", "R", "K", "M",
	"B", "V", "D", "H", "H", "D", "V", "B",
)

// ReverseComplement is SeedUtils::reverse_comp.
func ReverseComplement(dna string) string {
	reversed := make([]byte, len(dna))
	for i := 0; i < len(dna); i++ {
		reversed[i] = dna[len(dna)-1-i]
	}
	return complement.Replace(string(reversed))
}
