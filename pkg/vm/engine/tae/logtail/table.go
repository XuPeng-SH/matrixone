// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtail

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"go.uber.org/zap"
)

type RowT = *txnRow
type BlockT = *txnBlock

type summary struct {
	hasCatalogChanges bool
	// TODO
	// table ids
	// maxLsn
}

type archiveEntry struct {
	memo   *txnif.TxnMemo
	txnCnt int
	maxLSN uint64
}

type txnRow struct {
	txnif.AsyncTxn
}

func (row *txnRow) Length() int             { return 1 }
func (row *txnRow) Window(_, _ int) *txnRow { return nil }

type txnBlock struct {
	sync.RWMutex
	bornTS       types.TS
	rows         []*txnRow
	summary      atomic.Pointer[summary]
	archiveEntry atomic.Pointer[archiveEntry]
}

func (blk *txnBlock) GetArchiveEntry() *archiveEntry {
	return blk.archiveEntry.Load()
}

func (blk *txnBlock) TryArchive() *archiveEntry {
	entry := blk.GetArchiveEntry()
	if entry != nil {
		return entry
	}
	blk.RLock()
	full := len(blk.rows) == cap(blk.rows)
	blk.RUnlock()
	if !full {
		return nil
	}
	blk.Lock()
	if !blk.AllCommittedLocked() {
		blk.Unlock()
		return nil
	}

	if entry = blk.GetArchiveEntry(); entry != nil {
		blk.Unlock()
		return entry
	}
	entry = new(archiveEntry)
	entry.memo = txnif.NewTxnMemo()
	for _, row := range blk.rows {
		rowMemo := row.GetMemo()
		if rowMemo.HasCatalogChanges() {
			entry.memo.AddCatalogChange()
		}
		if row.GetLSN() > entry.maxLSN {
			entry.maxLSN = row.GetLSN()
		}
		entry.memo.Merge(rowMemo.GetDirty())
	}
	entry.txnCnt = len(blk.rows)
	blk.rows = nil
	blk.archiveEntry.Store(entry)
	blk.Unlock()
	return entry
}

func (blk *txnBlock) AllCommittedLocked() bool {
	for _, row := range blk.rows {
		state := row.GetTxnState(false)
		if state != txnif.TxnStateCommitted && state != txnif.TxnStateRollbacked {
			return false
		}
	}
	return true
}

func (blk *txnBlock) Length() int {
	blk.RLock()
	defer blk.RUnlock()
	return len(blk.rows)
}

func (blk *txnBlock) IsAppendable() bool {
	return blk != nil
}

func (blk *txnBlock) Append(row *txnRow) (err error) {
	blk.Lock()
	defer blk.Unlock()
	blk.rows = append(blk.rows, row)
	return
}

func (blk *txnBlock) Close() {
	blk.Lock()
	defer blk.Unlock()
	blk.bornTS = types.TS{}
	blk.rows = make([]*txnRow, 0)
}

func (blk *txnBlock) trySumary() {
	summary := new(summary)
	for _, row := range blk.rows {
		if row.GetMemo().HasCatalogChanges() {
			summary.hasCatalogChanges = true
			break
		}
	}
	blk.summary.CompareAndSwap(nil, summary)
}

func (blk *txnBlock) ForeachRowInBetween(
	from, to types.TS,
	rowOp func(row RowT) (goNext bool),
) (outOfRange bool, readRows int) {
	var rows []*txnRow
	if blk.summary.Load() == nil {
		blk.RLock()
		rows = blk.rows[:len(blk.rows)]
		capacity := cap(blk.rows)
		blk.RUnlock()
		if capacity == len(rows) && blk.summary.Load() == nil {
			blk.trySumary()
		}
	} else {
		rows = blk.rows
	}
	for _, row := range rows {
		readRows += 1
		ts := row.GetPrepareTS()
		if ts.IsEmpty() || ts.Greater(to) {
			outOfRange = true
			return
		}
		if ts.Less(from) {
			continue
		}

		if !rowOp(row) {
			outOfRange = true
			return
		}
	}
	return
}

func (blk *txnBlock) String() string {
	length := blk.Length()
	var buf bytes.Buffer
	_, _ = buf.WriteString(
		fmt.Sprintf("TXNBLK-[%s][Len=%d]", blk.bornTS.ToString(), length))
	return buf.String()
}

type TxnTable struct {
	*model.AOT[BlockT, RowT]
}

func (blk *txnBlock) Less(b BlockT) bool {
	return blk.bornTS.Less(b.bornTS)
}

func timeBasedTruncateFactory(ts types.TS) func(b BlockT) bool {
	return func(b BlockT) bool {
		return b.bornTS.GreaterEq(ts)
	}
}

func NewTxnTable(blockSize int, nowClock func() types.TS) *TxnTable {
	factory := func(row RowT) BlockT {
		ts := row.GetPrepareTS()
		if ts == txnif.UncommitTS {
			ts = nowClock()
		}
		return &txnBlock{
			bornTS: ts,
			rows:   make([]*txnRow, 0, blockSize),
		}
	}
	return &TxnTable{
		AOT: model.NewAOT(
			blockSize,
			factory,
			(*txnBlock).Less,
		),
	}
}

func (table *TxnTable) AddTxn(txn txnif.AsyncTxn) (err error) {
	row := &txnRow{
		AsyncTxn: txn,
	}
	err = table.Append(row)
	return
}

func (table *TxnTable) TruncateByTimeStamp(ts types.TS) (cnt int) {
	filter := timeBasedTruncateFactory(ts)
	return table.Truncate(filter)
}

func (table *TxnTable) ForeachRowInBetween(
	from, to types.TS,
	skipBlkOp func(blk BlockT) bool,
	rowOp func(row RowT) (goNext bool),
	blkOp func(blk BlockT) (goNext, goRecr bool),
) (readRows int) {
	snapshot := table.Snapshot()
	pivot := &txnBlock{bornTS: from}
	outOfLeft := true
	snapshot.Descend(pivot, func(blk BlockT) bool {
		pivot.bornTS = blk.bornTS
		outOfLeft = false
		return false
	})

	// from is smaller than the very first block and it is not special like 0-0, 0-1, 1-0
	if outOfLeft && from.Greater(types.BuildTS(1, 1)) {
		minTs := types.TS{}
		snapshot.Ascend(&txnBlock{}, func(blk *txnBlock) bool {
			minTs = blk.bornTS
			return false
		})
		logutil.Info("[logtail] fetch with too small ts", zap.String("ts", from.ToString()), zap.String("minTs", minTs.ToString()))
	}
	snapshot.Ascend(pivot, func(blk BlockT) bool {
		if blk.bornTS.Greater(to) {
			return false
		}

		if skipBlkOp != nil && skipBlkOp(blk) {
			return blk.rows[len(blk.rows)-1].GetPrepareTS().LessEq(to)
		}
		if blkOp != nil {
			goNext, goRecr := blkOp(blk)
			if !goNext {
				return false
			}
			if !goRecr {
				return true
			}
		}

		outOfRange, cnt := blk.ForeachRowInBetween(
			from,
			to,
			rowOp,
		)
		readRows += cnt

		return !outOfRange
	})
	return
}
