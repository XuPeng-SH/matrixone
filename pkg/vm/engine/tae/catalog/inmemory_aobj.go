// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

import (
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

// inMemoryAobj represents a shared in-memory appendable object.
type inMemoryAobj struct {
	entry   *ObjectEntry
	nextRow atomic.Uint32
	maxRows uint32
	frozen  atomic.Bool
}

// GetOrCreateInMemoryAobj returns the current in-memory aobj or creates a new one.
// It returns the ObjectEntry and the nextRow offset.
func (entry *TableEntry) GetOrCreateInMemoryAobj(isTombstone bool) (*ObjectEntry, uint32) {
	entry.aobjMu.RLock()
	current := entry.currentDataAobj
	if isTombstone {
		current = entry.currentTombstoneAobj
	}

	// Check if current aobj is usable
	if current != nil && !current.frozen.Load() {
		entry.aobjMu.RUnlock()
		return current.entry, current.nextRow.Load()
	}
	entry.aobjMu.RUnlock()

	// Create new in-memory aobj
	entry.aobjMu.Lock()
	defer entry.aobjMu.Unlock()

	// Double check
	current = entry.currentDataAobj
	if isTombstone {
		current = entry.currentTombstoneAobj
	}
	if current != nil && !current.frozen.Load() {
		return current.entry, current.nextRow.Load()
	}

	// Create new ObjectEntry
	objEntry := NewInMemoryObject(entry, types.BuildTS(time.Now().UnixNano(), 0), isTombstone)
	entry.AddEntryLocked(objEntry)

	// Create inMemoryAobj
	current = &inMemoryAobj{
		entry:   objEntry,
		maxRows: entry.GetLastestSchemaLocked(isTombstone).Extra.BlockMaxRows,
	}

	if isTombstone {
		entry.currentTombstoneAobj = current
	} else {
		entry.currentDataAobj = current
	}

	return objEntry, 0
}

// AllocateRows atomically allocates row range in the current in-memory aobj.
// Returns (startRow, count, needSwitch).
func (entry *TableEntry) AllocateRows(isTombstone bool, count uint32) (startRow, allocated uint32, needSwitch bool) {
	entry.aobjMu.RLock()
	current := entry.currentDataAobj
	if isTombstone {
		current = entry.currentTombstoneAobj
	}
	entry.aobjMu.RUnlock()

	if current == nil || current.frozen.Load() {
		return 0, 0, true
	}

	// Atomic allocation
	for {
		currentRow := current.nextRow.Load()
		available := current.maxRows - currentRow

		if available == 0 {
			return 0, 0, true
		}

		toAlloc := count
		if toAlloc > available {
			toAlloc = available
		}

		if current.nextRow.CompareAndSwap(currentRow, currentRow+toAlloc) {
			return currentRow, toAlloc, false
		}
	}
}

// FreezeCurrentInMemoryAobj freezes the current in-memory aobj.
func (entry *TableEntry) FreezeCurrentInMemoryAobj(isTombstone bool) {
	entry.aobjMu.Lock()
	defer entry.aobjMu.Unlock()

	var current *inMemoryAobj
	if isTombstone {
		current = entry.currentTombstoneAobj
		entry.currentTombstoneAobj = nil
	} else {
		current = entry.currentDataAobj
		entry.currentDataAobj = nil
	}

	if current != nil {
		current.frozen.Store(true)
		// Also freeze the underlying aobject
		if obj := current.entry.GetObjectData(); obj != nil {
			obj.(interface{ FreezeAppend() }).FreezeAppend()
		}
	}
}

// GetSharedAppender creates a SharedAppender for the transaction.
func (entry *TableEntry) GetSharedAppender(txn txnif.AsyncTxn, isTombstone bool) data.SharedAppender {
	// TODO: implement sharedAppender
	// return newSharedAppender(entry, txn, isTombstone)
	return nil
}
