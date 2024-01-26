package util

import "nasp-project/model"

type Iterator interface {
	Next() bool
	Value() *model.Record
}
