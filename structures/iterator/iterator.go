package iterator

type Iterator interface {
	Next() bool
	Value() []byte
}
