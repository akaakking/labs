package util

type arraylist struct {
	array []any
}

type List interface {
	Add(element any)
	insert(element any)
	Delete(element any)
	Size(size int)
}

type Queue interface {
	Size(size int)
	Push(element any)
	Pop()
	Empty()
}
