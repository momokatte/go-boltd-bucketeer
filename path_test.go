package bucketeer

import (
	"testing"
)

func TestPath(t *testing.T) {

	p := NewPath("root")

	if actual := len(p); actual != 1 {
		t.Fatalf("Expected length 1, got '%d'", actual)
	}

	p = p.Nest("branch")

	if actual := len(p); actual != 2 {
		t.Fatalf("Expected length 2, got '%d'", actual)
	}

	p = p.Nest("leaf")

	if actual := len(p); actual != 3 {
		t.Fatalf("Expected length 3, got '%d'", actual)
	}
}
