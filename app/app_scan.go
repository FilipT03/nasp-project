package app

import (
	"bytes"
	"errors"
	"nasp-project/model"
	"nasp-project/structures/lsm"
)

type Record struct {
	Key   string
	Value []byte
}

func (kvs *KeyValueStore) RangeScan(minKey, maxKey string, pageNumber, pageSize int) ([]Record, error) {
	if block, err := kvs.rateLimitReached(); block {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("rate limit reached")
	}

	compressionDict, err := kvs.getCompressionDict("") // dict with all keys loaded
	if err != nil {
		return nil, err
	}

	memtableScan := kvs.memtables.RangeScan([]byte(minKey), []byte(maxKey))

	sstableScan, err := lsm.RangeScan([]byte(minKey), []byte(maxKey), -1, compressionDict, kvs.config)
	if err != nil {
		return nil, err
	}

	scan := mergeScans(memtableScan, sstableScan)

	recs := make([]Record, len(scan))
	for i, rec := range scan {
		recs[i] = Record{Key: string(rec.Key), Value: rec.Value}
	}

	return paginate(recs, pageNumber, pageSize), nil
}

func (kvs *KeyValueStore) PrefixScan(prefix string, pageNumber, pageSize int) ([]Record, error) {
	if block, err := kvs.rateLimitReached(); block {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("rate limit reached")
	}

	compressionDict, err := kvs.getCompressionDict("") // dict with all keys loaded
	if err != nil {
		return nil, err
	}

	memtableScan := kvs.memtables.PrefixScan([]byte(prefix))

	sstableScan, err := lsm.PrefixScan([]byte(prefix), -1, compressionDict, kvs.config)
	if err != nil {
		return nil, err
	}

	scan := mergeScans(memtableScan, sstableScan)

	recs := make([]Record, len(scan))
	for i, rec := range scan {
		recs[i] = Record{Key: string(rec.Key), Value: rec.Value}
	}

	return paginate(recs, pageNumber, pageSize), nil
}

func paginate(recs []Record, pageNumber, pageSize int) []Record {
	if (pageNumber+1)*pageSize <= len(recs) {
		return recs[pageNumber*pageSize : (pageNumber+1)*pageSize]
	}
	if pageNumber*pageSize <= len(recs) {
		return recs[pageNumber*pageSize:]
	}
	return nil
}

func mergeScans(scan1, scan2 []*model.Record) []*model.Record {
	var merged []*model.Record
	i, j := 0, 0
	for i < len(scan1) && j < len(scan2) {
		rec1, rec2 := scan1[i], scan2[j]
		if bytes.Compare(rec1.Key, rec2.Key) < 0 {
			if !rec1.Tombstone {
				merged = append(merged, rec1)
			}
			i++
		} else if bytes.Compare(rec1.Key, rec2.Key) > 0 {
			if !rec2.Tombstone {
				merged = append(merged, rec2)
			}
			j++
		} else if rec1.Timestamp > rec2.Timestamp {
			if !rec1.Tombstone {
				merged = append(merged, rec1)
			}
			i++
			j++
		} else {
			if !rec2.Tombstone {
				merged = append(merged, rec2)
			}
			i++
			j++
		}
	}
	for i < len(scan1) {
		if !scan1[i].Tombstone {
			merged = append(merged, scan1[i])
		}
		i++
	}
	for j < len(scan2) {
		if !scan2[j].Tombstone {
			merged = append(merged, scan2[j])
		}
		j++
	}
	return merged
}
