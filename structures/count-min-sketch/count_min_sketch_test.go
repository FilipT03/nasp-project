package count_min_sketch

import (
	"testing"
)

func TestAddAndGet(t *testing.T) {
	cms := NewCMS(0.01, 0.01)
	element := "example_element"

	cms.Add(element)
	count := cms.Get(element)

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
func TestSerializationAndDeserialization(t *testing.T) {
	// Create a new CMS instance
	cms1 := NewCMS(0.01, 0.001)

	// Add some data to the CMS
	cms1.Add("apple")
	cms1.Add("banana")
	cms1.Add("cherry")

	// Serialize the CMS
	serializedData := cms1.Serialize()

	// Deserialize the CMS
	cms2 := Deserialize(serializedData)
	// Verify that the matrix dimensions are the same
	if cms1.rows != cms2.rows || cms1.cols != cms2.cols {
		t.Errorf("Matrix dimensions are different. Original: %dx%d, Deserialized: %dx%d", cms1.rows, cms1.cols, cms2.rows, cms2.cols)
	}

	// Check that the seeds are the same
	for i := uint(0); i < cms1.rows; i++ {
		if string(cms1.seeds[i].Seed) != string(cms2.seeds[i].Seed) {
			t.Errorf("Seeds are different for row %d. Original: %s, Deserialized: %s", i, cms1.seeds[i].Seed, cms2.seeds[i].Seed)
		}
	}

	// Verify that the counts are the same
	for _, element := range []string{"apple", "banana", "cherry"} {
		count1 := cms1.Get(element)
		count2 := cms2.Get(element)

		if count1 != count2 {
			t.Errorf("Counts for element %s are different. Original: %d, Deserialized: %d", element, count1, count2)
		}
	}
}
