package count_min_sketch

import "testing"

func TestAddAndGet(t *testing.T) {
	cms := newCMS(0.01, 0.01, 42)
	element := "example_element"

	cms.add(element)
	count := cms.get(element)

	if count != 1 {
		t.Errorf("Expected count of %s to be 1, but got %d", element, count)
	}
}

func TestCalculateM(t *testing.T) {
	epsilon := 0.01
	expectedCols := uint(272) // This is an example value; adjust it according to your calculations.

	cols := CalculateM(epsilon)

	if cols != expectedCols {
		t.Errorf("Expected cols to be %d, but got %d", expectedCols, cols)
	}
}

func TestCalculateK(t *testing.T) {
	delta := 0.01
	expectedRows := uint(6) // This is an example value; adjust it according to your calculations.

	rows := CalculateK(delta)

	if rows != expectedRows {
		t.Errorf("Expected rows to be %d, but got %d", expectedRows, rows)
	}
}
