package lsm

import (
	"bytes"
	"nasp-project/model"
	"nasp-project/structures/compression"
	"nasp-project/util"
)

func RangeScan(startKey, endKey []byte, maxRecords int, compressionDict *compression.Dictionary, config *util.Config) ([]*model.Record, error) {
	var scans [][]*model.Record
	for lvl := 1; lvl <= config.LSMTree.MaxLevel; lvl++ {
		tables, err := GetSSTablesForLevel(lvl, config)
		if err != nil {
			return nil, err
		}
		for _, table := range tables {
			scan, err := table.RangeScan(startKey, endKey, maxRecords, compressionDict)
			if err != nil {
				return nil, err
			}
			scans = append(scans, scan)
		}
	}

	scan := mergeAllScans(scans)
	if maxRecords != -1 && len(scan) > maxRecords {
		scan = scan[:maxRecords]
	}
	return scan, nil
}

func PrefixScan(prefix []byte, maxRecords int, compressionDict *compression.Dictionary, config *util.Config) ([]*model.Record, error) {
	var scans [][]*model.Record
	for lvl := 1; lvl <= config.LSMTree.MaxLevel; lvl++ {
		tables, err := GetSSTablesForLevel(lvl, config)
		if err != nil {
			return nil, err
		}
		for _, table := range tables {
			scan, err := table.PrefixScan(prefix, maxRecords, compressionDict)
			if err != nil {
				return nil, err
			}
			scans = append(scans, scan)
		}
	}

	scan := mergeAllScans(scans)
	if maxRecords != -1 && len(scan) > maxRecords {
		scan = scan[:maxRecords]
	}
	return scan, nil
}

func mergeAllScans(scans [][]*model.Record) []*model.Record {
	for len(scans) > 1 {
		var newScans [][]*model.Record
		for i := 0; i+1 < len(scans); i += 2 {
			newScans = append(newScans, mergeTwoScans(scans[i], scans[i+1]))
		}
		if len(scans)%2 != 0 {
			newScans = append(newScans, scans[len(scans)-1])
		}
		scans = newScans
	}
	return scans[0]
}

func mergeTwoScans(scan1 []*model.Record, scan2 []*model.Record) []*model.Record {
	var merged []*model.Record
	i, j := 0, 0
	for i < len(scan1) && j < len(scan2) {
		rec1, rec2 := scan1[i], scan2[j]
		if bytes.Compare(rec1.Key, rec2.Key) < 0 {
			merged = append(merged, rec1)
			i++
		} else if bytes.Compare(rec1.Key, rec2.Key) > 0 {
			merged = append(merged, rec2)
			j++
		} else if rec1.Timestamp > rec2.Timestamp {
			merged = append(merged, rec1)
			i++
		} else {
			merged = append(merged, rec2)
			j++
		}
	}
	for i < len(scan1) {
		merged = append(merged, scan1[i])
		i++
	}
	for j < len(scan2) {
		merged = append(merged, scan2[j])
		j++
	}
	return merged
}
