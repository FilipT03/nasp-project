package sim_hash

import (
	"testing"
)

var (
	text1 = `Look again at that dot. That's here. That's home. That's us. 
	On it everyone you love, everyone you know, everyone you ever heard of, every human being who ever was, 
	lived out their lives. The aggregate of our joy and suffering, thousands of confident religions, ideologies, 
	and economic doctrines, every hunter and forager, every hero and coward, every creator and destroyer of civilization, 
	every king and peasant, every young couple in love, every mother and father, hopeful child, inventor and explorer, 
	every teacher of morals, every corrupt politician, every "superstar," every "supreme leader," 
	every saint and sinner in the history of our species lived there-on a mote of dust suspended in a sunbeam.`

	text2 = `If you wish to make an apple pie from scratch, you must first invent the universe.`

	similarToText2 = `If you wish to make a pecan pie from scratch, you must first invent the universe.`

	text3 = `Every one of us is, in the cosmic perspective, precious. If a human disagrees with you, let him live. 
	In a hundred billion galaxies, you will not find another.`
)

func TestSerializeAndDeserialize(t *testing.T) {
	print1, err1 := SimHashText(text1)
	print2, err2 := SimHashText(text3)

	if (err1 != nil) || (err2 != nil) {
		t.Errorf("Failed to compute simhash. \nprint1 = 0x%x; err = %s\nprint2 = 0x%x; err = %s\n", print1, err1, print2, err2)
	}

	deserialized1, deserialized2 := Deserialize(print1.Serialize()), Deserialize(print2.Serialize())

	if print1 != deserialized1 {
		t.Errorf("Expected fingerprint to be 0x%x, got 0x%x.", print1, deserialized1)
	}

	if print2 != deserialized2 {
		t.Errorf("Expected fingerprint to be 0x%x, got 0x%x.", print2, deserialized2)
	}
}

func TestSimHashText(t *testing.T) {
	print1, err := SimHashText(text2)

	if err != nil {
		t.Errorf("Failed to compute simhash. [%s]", err)
	}

	print2, err := SimHashText(similarToText2)

	if err != nil {
		t.Errorf("Failed to compute simhash. [%s]", err)
	}

	print3, err := SimHashText(text3)

	if err != nil {
		t.Errorf("Failed to compute simhash. [%s]", err)
	}

	distance := print1.HammingDistanceFrom(print1)
	if distance != 0 {
		t.Errorf("Expected hamming distance from fingerprint to itself to be 0, got %d.", distance)
	}

	distance12 := HammingDistance(print1, print2)
	distance13 := HammingDistance(print1, print3)

	if distance12 > distance13 {
		t.Errorf("Expected distance between more similar texts [%d] to be smaller than less similar texts [%d].", distance12, distance13)
	}
}
