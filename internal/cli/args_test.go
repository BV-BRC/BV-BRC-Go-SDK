package cli

import (
	"reflect"
	"testing"
)

func TestNormalizePairedEndLibArgs(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{
			"two-argument form is joined",
			[]string{"cmd", "--paired-end-lib", "r1.fq", "r2.fq", "/ws/out", "name"},
			[]string{"cmd", "--paired-end-lib", "r1.fq,r2.fq", "/ws/out", "name"},
		},
		{
			"comma form passes through",
			[]string{"cmd", "--paired-end-lib", "r1.fq,r2.fq", "/ws/out", "name"},
			[]string{"cmd", "--paired-end-lib", "r1.fq,r2.fq", "/ws/out", "name"},
		},
		{
			"equals form passes through",
			[]string{"cmd", "--paired-end-lib=r1.fq,r2.fq", "/ws/out", "name"},
			[]string{"cmd", "--paired-end-lib=r1.fq,r2.fq", "/ws/out", "name"},
		},
		{
			"repeated libraries in both forms",
			[]string{"cmd", "--paired-end-lib", "a1.fq", "a2.fq", "--paired-end-lib", "b1.fq,b2.fq", "/ws/out", "name"},
			[]string{"cmd", "--paired-end-lib", "a1.fq,a2.fq", "--paired-end-lib", "b1.fq,b2.fq", "/ws/out", "name"},
		},
		{
			"a following flag is not consumed as read2",
			[]string{"cmd", "--paired-end-lib", "r1.fq,r2.fq", "--single-end-lib", "s.fq"},
			[]string{"cmd", "--paired-end-lib", "r1.fq,r2.fq", "--single-end-lib", "s.fq"},
		},
		{
			"plural alias is rewritten to the singular",
			[]string{"cmd", "--paired-end-libs", "r1.fq", "r2.fq", "/ws/out", "name"},
			[]string{"cmd", "--paired-end-lib", "r1.fq,r2.fq", "/ws/out", "name"},
		},
		{
			"plural alias with equals is rewritten too",
			[]string{"cmd", "--paired-end-libs=r1.fq,r2.fq"},
			[]string{"cmd", "--paired-end-lib=r1.fq,r2.fq"},
		},
		{
			"arguments after -- are left alone",
			[]string{"cmd", "--", "--paired-end-lib", "r1.fq", "r2.fq"},
			[]string{"cmd", "--", "--paired-end-lib", "r1.fq", "r2.fq"},
		},
		{
			"other arguments are untouched",
			[]string{"cmd", "--srr-id", "SRR123", "/ws/out", "name"},
			[]string{"cmd", "--srr-id", "SRR123", "/ws/out", "name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizePairedEndLibArgs(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got  %q\nwant %q", got, tt.want)
			}
		})
	}
}

func TestSplitPairedEndLib(t *testing.T) {
	ok := []struct{ in, r1, r2 string }{
		{"r1.fq,r2.fq", "r1.fq", "r2.fq"},
		{"r1.fq, r2.fq", "r1.fq", "r2.fq"},                    // whitespace around the comma
		{" ws:/u@x/r1.fq , r2.fq ", "ws:/u@x/r1.fq", "r2.fq"}, // and around the whole value
		{"/a b/r1.fq,/a b/r2.fq", "/a b/r1.fq", "/a b/r2.fq"}, // spaces inside a path are kept
	}
	for _, tt := range ok {
		r1, r2, err := SplitPairedEndLib(tt.in)
		if err != nil {
			t.Errorf("SplitPairedEndLib(%q): %v", tt.in, err)
			continue
		}
		if r1 != tt.r1 || r2 != tt.r2 {
			t.Errorf("SplitPairedEndLib(%q) = %q, %q; want %q, %q", tt.in, r1, r2, tt.r1, tt.r2)
		}
	}

	bad := []string{"", "r1.fq", "r1.fq,", ",r2.fq", " , ", "r1.fq,r2.fq,r3.fq"}
	for _, in := range bad {
		if _, _, err := SplitPairedEndLib(in); err == nil {
			t.Errorf("SplitPairedEndLib(%q) succeeded, want an error", in)
		}
	}
}
