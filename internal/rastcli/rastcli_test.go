package rastcli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadInputFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "genome.gto")
	body := `{"id":"83333.1","features":[]}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := LoadInput(&Common{Input: path})
	if err != nil {
		t.Fatalf("LoadInput: %v", err)
	}
	if string(got) != body {
		t.Errorf("got %q, want the bytes unchanged: %q", got, body)
	}
}

func TestLoadInputMissingFile(t *testing.T) {
	_, err := LoadInput(&Common{Input: filepath.Join(t.TempDir(), "nope.gto")})
	if err == nil {
		t.Fatal("want an error for a missing input file")
	}
	if !strings.Contains(err.Error(), "nope.gto") {
		t.Errorf("error should name the file: %v", err)
	}
}

func TestLoadInputRejectsNonJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "notjson")
	if err := os.WriteFile(path, []byte("<html>Error 1010</html>"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := LoadInput(&Common{Input: path})
	if err == nil {
		t.Fatal("want an error for input that is not JSON")
	}
	if !strings.Contains(err.Error(), "valid JSON") {
		t.Errorf("got %v", err)
	}
}

func TestPrettyPreservesLargeIntegers(t *testing.T) {
	// The reason GTOs travel as raw bytes: decoding into interface{} would
	// turn this into a float64 and print it back as 1.2345678901234568e+16.
	raw := json.RawMessage(`{"n":12345678901234567}`)
	got, err := Pretty(raw)
	if err != nil {
		t.Fatalf("Pretty: %v", err)
	}
	if !strings.Contains(string(got), "12345678901234567") {
		t.Errorf("got %s, want the integer intact", got)
	}
}

func TestPrettyPreservesKeyOrder(t *testing.T) {
	raw := json.RawMessage(`{"zebra":1,"apple":2,"middle":3}`)
	got, err := Pretty(raw)
	if err != nil {
		t.Fatalf("Pretty: %v", err)
	}
	s := string(got)
	if strings.Index(s, "zebra") > strings.Index(s, "apple") {
		t.Errorf("keys were reordered:\n%s", s)
	}
}

func TestPrettyUsesThreeSpaceIndent(t *testing.T) {
	got, err := Pretty(json.RawMessage(`{"a":1,"b":[1,2]}`))
	if err != nil {
		t.Fatalf("Pretty: %v", err)
	}
	// Three spaces per level, matching JSON::XS->new->pretty -- which is what
	// the Perl tools have always written, and so what anything downstream that
	// greps a GTO expects. (JSON::XS also spaces around the colon; Go does not,
	// and no reader cares.)
	want := "{\n   \"a\": 1,\n   \"b\": [\n      1,\n      2\n   ]\n}"
	if string(got) != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestWriteOutputToFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.gto")
	if err := WriteOutput(json.RawMessage(`{"a":1}`), &Common{Output: path}); err != nil {
		t.Fatalf("WriteOutput: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(data), "\n") {
		t.Error("output should end with a newline")
	}
	var v map[string]int
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if v["a"] != 1 {
		t.Errorf("got %v", v)
	}
}

func TestWriteTextToFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.txt")
	if err := WriteText("hello\n", &Common{Output: path}); err != nil {
		t.Fatalf("WriteText: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello\n" {
		t.Errorf("got %q", data)
	}
}

func TestReadContigsFasta(t *testing.T) {
	path := filepath.Join(t.TempDir(), "contigs.fa")
	if err := os.WriteFile(path, []byte(">c1 first\nACGT\nTTTT\n>c2\nGGGG\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	contigs, err := ReadContigsFasta(path)
	if err != nil {
		t.Fatalf("ReadContigsFasta: %v", err)
	}
	if len(contigs) != 2 {
		t.Fatalf("got %d contigs, want 2", len(contigs))
	}
	if contigs[0].ID != "c1" || contigs[0].DNA != "ACGTTTTT" {
		t.Errorf("contig 0 = %+v", contigs[0])
	}
	if contigs[1].ID != "c2" || contigs[1].DNA != "GGGG" {
		t.Errorf("contig 1 = %+v", contigs[1])
	}

	// The description is dropped: add_contigs takes only id and dna.
	body, err := json.Marshal(contigs[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "first") {
		t.Errorf("description leaked into the wire form: %s", body)
	}
}

func TestReadContigsFastaMissingFile(t *testing.T) {
	_, err := ReadContigsFasta(filepath.Join(t.TempDir(), "nope.fa"))
	if err == nil {
		t.Fatal("want an error for a missing contigs file")
	}
	if !strings.Contains(err.Error(), "nope.fa") {
		t.Errorf("error should name the file: %v", err)
	}
}
