package helper

import "testing"

func TestChunkSliceWithSize(t *testing.T) {
	tests := []struct {
		name     string
		input    []int
		size     int
		expected int
	}{
		{"empty", []int{}, 3, 0},
		{"single chunk", []int{1, 2}, 5, 1},
		{"exact", []int{1, 2, 3, 4}, 2, 2},
		{"remainder", []int{1, 2, 3, 4, 5}, 2, 3},
		{"zero size", []int{1, 2, 3}, 0, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ChunkSliceWithSize(tt.input, tt.size)
			if len(got) != tt.expected {
				t.Fatalf("expected %d chunks, got %d", tt.expected, len(got))
			}
		})
	}
}
