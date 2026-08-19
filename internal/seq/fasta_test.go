package seq

import (
	"strings"
	"testing"
)

func TestReadFasta(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []Record
	}{
		{
			name:  "single record",
			input: ">contig1\nACGT\n",
			want:  []Record{{ID: "contig1", Seq: "ACGT"}},
		},
		{
			name:  "id and description split on first whitespace",
			input: ">contig1 a long  description\nACGT\n",
			want:  []Record{{ID: "contig1", Desc: "a long  description", Seq: "ACGT"}},
		},
		{
			name:  "tab between id and description",
			input: ">contig1\tdescription\nACGT\n",
			want:  []Record{{ID: "contig1", Desc: "description", Seq: "ACGT"}},
		},
		{
			name:  "wrapped sequence is joined",
			input: ">c\nACGT\nTTTT\nGG\n",
			want:  []Record{{ID: "c", Seq: "ACGTTTTTGG"}},
		},
		{
			name:  "spaces and tabs are stripped from sequence lines",
			input: ">c\nAC GT\tTT\n",
			want:  []Record{{ID: "c", Seq: "ACGTTT"}},
		},
		{
			name:  "carriage returns are stripped",
			input: ">c\r\nACGT\r\n",
			want:  []Record{{ID: "c", Seq: "ACGT"}},
		},
		{
			name:  "multiple records",
			input: ">a\nAAAA\n>b desc\nCCCC\n",
			want:  []Record{{ID: "a", Seq: "AAAA"}, {ID: "b", Desc: "desc", Seq: "CCCC"}},
		},
		{
			// gjoseqlib yields nothing for a header with no residues under
			// it, and callers rely on that: a stray ">" in a file must not
			// become an empty contig the service then chokes on.
			name:  "header with no sequence yields no record",
			input: ">empty\n>a\nAAAA\n",
			want:  []Record{{ID: "a", Seq: "AAAA"}},
		},
		{
			name:  "no trailing newline",
			input: ">a\nACGT",
			want:  []Record{{ID: "a", Seq: "ACGT"}},
		},
		{
			name:  "text before the first header is ignored",
			input: "junk\n>a\nACGT\n",
			want:  []Record{{ID: "a", Seq: "ACGT"}},
		},
		{
			name:  "empty input",
			input: "",
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadFasta(strings.NewReader(tt.input))
			if err != nil {
				t.Fatalf("ReadFasta: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d records, want %d: %+v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("record %d = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestWriteFasta(t *testing.T) {
	tests := []struct {
		name string
		rec  Record
		want string
	}{
		{
			name: "short sequence",
			rec:  Record{ID: "a", Seq: "ACGT"},
			want: ">a\nACGT\n",
		},
		{
			name: "description is included",
			rec:  Record{ID: "a", Desc: "some genome", Seq: "ACGT"},
			want: ">a some genome\nACGT\n",
		},
		{
			name: "exactly one line",
			rec:  Record{ID: "a", Seq: strings.Repeat("A", 60)},
			want: ">a\n" + strings.Repeat("A", 60) + "\n",
		},
		{
			name: "wraps at 60 columns",
			rec:  Record{ID: "a", Seq: strings.Repeat("A", 61)},
			want: ">a\n" + strings.Repeat("A", 60) + "\nA\n",
		},
		{
			name: "empty sequence prints only the title",
			rec:  Record{ID: "a"},
			want: ">a\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b strings.Builder
			if err := WriteFasta(&b, tt.rec); err != nil {
				t.Fatalf("WriteFasta: %v", err)
			}
			if b.String() != tt.want {
				t.Errorf("got %q, want %q", b.String(), tt.want)
			}
		})
	}
}

func TestRoundTrip(t *testing.T) {
	want := []Record{
		{ID: "contig1", Desc: "first", Seq: strings.Repeat("ACGT", 40)},
		{ID: "contig2", Seq: "TTTT"},
	}

	var b strings.Builder
	for _, r := range want {
		if err := WriteFasta(&b, r); err != nil {
			t.Fatalf("WriteFasta: %v", err)
		}
	}

	got, err := ReadFasta(strings.NewReader(b.String()))
	if err != nil {
		t.Fatalf("ReadFasta: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("record %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}
