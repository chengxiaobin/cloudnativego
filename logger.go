package main

type EventType byte

const (
	_                     = iota // iota == 0; ignore this value
	EventDelete EventType = iota // iota == 1
	EventPut                     // iota == 2; implicitly repeat last
)

type Event struct {
	Sequence  uint64
	EventType EventType
	Key       string
	Value     string
}

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error

	LastSequence() uint64

	Run()
	Wait()
	Close() error

	ReadEvents() (<-chan Event, <-chan error)
}
