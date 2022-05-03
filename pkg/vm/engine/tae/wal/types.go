package wal

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

const (
	GroupC uint32 = iota + 10
	GroupUC
)

type Index struct {
	LSN  uint64
	CSN  uint32
	Size uint32
}

type LogEntry entry.Entry

type Driver interface {
	Checkpoint(indexes []*Index) (LogEntry, error)
	AppendEntry(uint32, LogEntry) (uint64, error)
	LoadEntry(groupId uint32, lsn uint64) (LogEntry, error)
	GetCheckpointed() uint64
	Compact() error
	Close() error
}

func (index *Index) String() string {
	if index == nil {
		return "<nil index>"
	}
	return fmt.Sprintf("<Index[%d:%d]>", index.LSN, index.CSN)
}
