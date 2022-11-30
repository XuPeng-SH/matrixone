package logtail

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
)

func TestTxnTable1(t *testing.T) {
	txnCnt := 22
	blockSize := 10
	idAlloc := common.NewTxnIDAllocator()
	tsAlloc := types.GlobalTsAlloctor
	table := NewTxnTable(blockSize)
	for i := 0; i < txnCnt; i++ {
		txn := new(txnbase.Txn)
		txn.TxnCtx = txnbase.NewTxnCtx(idAlloc.Alloc(), tsAlloc.Alloc(), nil)
		txn.PrepareTS = tsAlloc.Alloc()
		assert.NoError(t, table.AddTxn(txn))
	}
	t.Log(table.BlockCount())
	t.Log(table.String())
	timestamps := make([]types.TS, 0)
	fn1 := func(block *txnBlock) bool {
		timestamps = append(timestamps, block.bornTS)
		return true
	}
	table.Scan(fn1)
	assert.Equal(t, 3, len(timestamps))

	cnt := 0

	op := func(row RowT) (goNext bool) {
		t.Log(row.String())
		cnt++
		return true
	}

	table.ForeachRowInBetween(
		timestamps[0].Prev(),
		types.MaxTs(),
		op,
	)
	assert.Equal(t, txnCnt, cnt)
	cnt = 0
	t.Log("==")
	table.ForeachRowInBetween(
		timestamps[1].Next(),
		types.MaxTs(),
		op,
	)
	assert.Equal(t, txnCnt-blockSize, cnt)
	cnt = 0
	t.Log("==")
	table.ForeachRowInBetween(
		timestamps[2].Next(),
		types.MaxTs(),
		op,
	)
	assert.Equal(t, txnCnt-2*blockSize, cnt)
	cnt = 0

	ckp := timestamps[0].Prev()
	cnt = table.TruncateByTimeStamp(ckp)
	assert.Equal(t, 0, cnt)

	ckp = timestamps[0].Next()
	cnt = table.TruncateByTimeStamp(ckp)
	assert.Equal(t, 0, cnt)

	ckp = timestamps[1].Prev()
	cnt = table.TruncateByTimeStamp(ckp)
	assert.Equal(t, 0, cnt)

	t.Log(table.String())
	ckp = timestamps[1].Next()
	cnt = table.TruncateByTimeStamp(ckp)
	assert.Equal(t, 1, cnt)
}
