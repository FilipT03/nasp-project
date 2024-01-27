package memtable

import (
	"bytes"
	"nasp-project/model"
	"nasp-project/util"
	"testing"
)

func add(mts *Memtables) {
	err := mts.Add(&model.Record{
		Key:       []byte("1"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
	err = mts.Add(&model.Record{
		Key:       []byte("5"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
	err = mts.Add(&model.Record{
		Key:       []byte("7"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
	err = mts.Add(&model.Record{
		Key:       []byte("8"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
	err = mts.Add(&model.Record{
		Key:       []byte("4"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
	err = mts.Add(&model.Record{
		Key:       []byte("2"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
}

func testLogicalDelete(t *testing.T, structure string) {
	util.GetConfig().Memtable.Structure = structure
	mts := CreateMemtables(&util.GetConfig().Memtable)
	add(mts)
	err := mts.Delete([]byte("5"))
	if err != nil {
		t.Errorf("error: [%s] '5' should be in structure", structure)
	}

	record, _ := mts.Get([]byte("5"))
	if err != nil {
		t.Errorf("error: [%s] '5' should be logically deleted", structure)
	}
	if !record.Tombstone {
		t.Errorf("error: [%s] '5' should be logically deleted", structure)
	}
	mts.Clear()
}

func TestLogicalDelete(t *testing.T) {
	testLogicalDelete(t, "SkipList")
	testLogicalDelete(t, "BTree")
	testLogicalDelete(t, "HashMap")
}

func TestTableSwitch(t *testing.T) {
	util.GetConfig().Memtable.Instances = 4
	util.GetConfig().Memtable.MaxSize = 3
	mts := CreateMemtables(&util.GetConfig().Memtable)
	add(mts)
	err := mts.Add(&model.Record{
		Key:       []byte("a"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})

	if err != nil {
		t.Errorf("error: [%s]", err.Error())
	}

	if mts.currentIndex != 2 {
		t.Errorf("error: expected current table to be %d, but got %d", 2, mts.currentIndex)
	}
}

func addPrefix(mts *Memtables) {
	_ = mts.Add(&model.Record{
		Key:       []byte("aaa"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 15,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("aab"),
		Value:     nil,
		Tombstone: true,
		Timestamp: 12,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("aabbc"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 13,
	})

	_ = mts.Add(&model.Record{
		Key:       []byte("aabbc"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 16,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("aabaa"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("aabaca"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 9,
	})

	_ = mts.Add(&model.Record{
		Key:       []byte("aabbccdd"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 10,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("aacda"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 14,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("csdasd"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 5,
	})

	_ = mts.Add(&model.Record{
		Key:       []byte("aab"),
		Value:     nil,
		Tombstone: true,
		Timestamp: 5,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("aabcay"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 4,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("aabbs"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 3,
	})

	_ = mts.Add(&model.Record{
		Key:       []byte("aabacav"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 2,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("adsadf"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 1,
	})

}

func addRange(mts *Memtables) {
	_ = mts.Add(&model.Record{
		Key:       []byte("1"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("2"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("4"),
		Value:     nil,
		Tombstone: true,
		Timestamp: 0,
	})

	_ = mts.Add(&model.Record{
		Key:       []byte("3"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 3,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("5"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("8"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})

	_ = mts.Add(&model.Record{
		Key:       []byte("1"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("2"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("3"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 5,
	})

	_ = mts.Add(&model.Record{
		Key:       []byte("1"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("5"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 4,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("8"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 3,
	})

	_ = mts.Add(&model.Record{
		Key:       []byte("3"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 2,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("6"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 1,
	})
}

func testValidRangeScan(structure string, t *testing.T) {
	util.GetConfig().Memtable.MaxSize = 3
	util.GetConfig().Memtable.Instances = 5
	util.GetConfig().Memtable.Structure = structure
	mts := CreateMemtables(&util.GetConfig().Memtable)
	addRange(mts)

	records := mts.RangeScan([]byte("2"), []byte("8"))
	sol := []model.Record{
		{
			Key:       []byte("2"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 0,
		},
		{
			Key:       []byte("3"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 5,
		},
		{
			Key:       []byte("5"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 4,
		},
		{
			Key:       []byte("6"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 1,
		},
		{
			Key:       []byte("8"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 3,
		},
	}

	if len(records) != 5 {
		t.Errorf("error: [%s] records size to be 5, got %d", structure, len(records))
		return
	}

	for i, record := range records {
		if bytes.Compare(record.Key, sol[i].Key) != 0 {
			t.Errorf("error: [%s] keys are not sorted correcly at %d", structure, i)
		}
		if record.Timestamp != sol[i].Timestamp {
			t.Errorf("error: [%s] timestamp is not correct at %d", structure, i)
		}
	}
}

func TestValidRangeScan(t *testing.T) {
	testValidRangeScan("HashMap", t)
	testValidRangeScan("BTree", t)
	testValidRangeScan("SkipList", t)
}

func testInvalidRangeScan(structure string, t *testing.T) {
	util.GetConfig().Memtable.MaxSize = 3
	util.GetConfig().Memtable.Instances = 5
	util.GetConfig().Memtable.Structure = structure
	mts := CreateMemtables(&util.GetConfig().Memtable)
	addRange(mts)

	records := mts.RangeScan([]byte("A"), []byte("F"))

	if len(records) != 0 {
		t.Errorf("error: [%s] expected records size to be 0, got %d", structure, len(records))
	}
}

func TestInvalidRangeScan(t *testing.T) {
	testInvalidRangeScan("HashMap", t)
	testInvalidRangeScan("BTree", t)
	testInvalidRangeScan("SkipList", t)
}

func testValidPrefixScan(structure string, t *testing.T) {
	util.GetConfig().Memtable.MaxSize = 3
	util.GetConfig().Memtable.Instances = 5
	util.GetConfig().Memtable.Structure = structure
	mts := CreateMemtables(&util.GetConfig().Memtable)
	addPrefix(mts)

	records := mts.PrefixScan([]byte("aab"))
	sol := []model.Record{
		{
			Key:       []byte("aabaa"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 0,
		},
		{
			Key:       []byte("aabaca"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 9,
		},
		{
			Key:       []byte("aabacav"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 2,
		},
		{
			Key:       []byte("aabbc"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 16,
		},
		{
			Key:       []byte("aabbccdd"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 10,
		},
		{
			Key:       []byte("aabbs"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 3,
		},
		{
			Key:       []byte("aabcay"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 4,
		},
	}

	if len(records) != 7 {
		t.Errorf("error: [%s] expected records size to be 7, got %d", structure, len(records))
	}

	for i, record := range records {
		if bytes.Compare(record.Key, sol[i].Key) != 0 {
			t.Errorf("error: [%s] keys are not sorted correcly at %d", structure, i)
		}
		if record.Timestamp != sol[i].Timestamp {
			t.Errorf("error: [%s] timestamp is not correct at %d", structure, i)
		}
	}
}

func TestValidPrefixScan(t *testing.T) {
	testValidPrefixScan("HashMap", t)
	testValidPrefixScan("BTree", t)
	testValidPrefixScan("SkipList", t)
}

func testInvalidPrefixScan(structure string, t *testing.T) {
	util.GetConfig().Memtable.MaxSize = 3
	util.GetConfig().Memtable.Instances = 5
	util.GetConfig().Memtable.Structure = structure
	mts := CreateMemtables(&util.GetConfig().Memtable)
	addRange(mts)

	records := mts.PrefixScan([]byte("xyz"))

	if len(records) != 0 {
		t.Errorf("error: [%s] expected records size to be 0, got %d", structure, len(records))
	}
}

func TestInvalidPrefixScan(t *testing.T) {
	testInvalidPrefixScan("HashMap", t)
	testInvalidPrefixScan("BTree", t)
	testInvalidPrefixScan("SkipList", t)
}

func addReserved(mts *Memtables) {
	_ = mts.Add(&model.Record{
		Key:       []byte(util.BloomFilterPrefix + "mojfilter"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte(util.HyperLogLogPrefix + "mojhajperloglog"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("__a"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})

	_ = mts.Add(&model.Record{
		Key:       []byte(util.SimHashPrefix + "mojsimhes"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 3,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("__Hll"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte(util.CountMinSketchPrefix + "mojcms"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})

	_ = mts.Add(&model.Record{
		Key:       []byte("adasda"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("babva"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("352523"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 5,
	})

	_ = mts.Add(&model.Record{
		Key:       []byte("dasda"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("basd"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 4,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte(util.RateLimiterKey),
		Value:     nil,
		Tombstone: false,
		Timestamp: 3,
	})

	_ = mts.Add(&model.Record{
		Key:       []byte("_a_a"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 2,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("_CMS"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 1,
	})

}
func testReservedScan(structure string, t *testing.T) {
	util.GetConfig().Memtable.MaxSize = 3
	util.GetConfig().Memtable.Instances = 5
	util.GetConfig().Memtable.Structure = structure
	mts := CreateMemtables(&util.GetConfig().Memtable)
	addReserved(mts)

	recordsPrefix := mts.PrefixScan([]byte("_"))
	recordsRange := mts.RangeScan([]byte("_"), []byte("b"))

	if len(recordsPrefix) != 4 {
		t.Errorf("error: [Prefix Scan] [%s] expected records size to be 4, got %d", structure, len(recordsPrefix))
	}

	if len(recordsRange) != 5 {
		t.Errorf("error: [Range Scan] [%s] expected records size to be 4, got %d", structure, len(recordsRange))
	}
}

func TestReservedScan(t *testing.T) {
	testReservedScan("HashMap", t)
	testReservedScan("BTree", t)
	testReservedScan("SkipList", t)
}
