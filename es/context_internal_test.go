package es

import (
	"maps"
	"testing"
)

func TestMergeMetadata(t *testing.T) {
	tests := []struct {
		name           string
		base, override Metadata
		want           Metadata
	}{
		{"both empty", nil, nil, nil},
		{"only base", Metadata{"a": "1"}, nil, Metadata{"a": "1"}},
		{"only override", nil, Metadata{"a": "1"}, Metadata{"a": "1"}},
		{"disjoint", Metadata{"a": "1"}, Metadata{"b": "2"}, Metadata{"a": "1", "b": "2"}},
		{"override wins on collision", Metadata{"a": "base"}, Metadata{"a": "over"}, Metadata{"a": "over"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := mergeMetadata(tc.base, tc.override)
			if !maps.Equal(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
