package util

import "nasp-project/model"

type Iterator interface {
	Next() bool
	Value() *model.Record
}

// IsInvalidKey checks if the key is a reserved word.
func IsInvalidKey(iter Iterator) bool {
	return iter != nil && iter.Value() != nil && IsReservedKey(iter.Value().Key)
}
