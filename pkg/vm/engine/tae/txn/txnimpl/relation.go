package txnimpl

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type txnRelationIt struct {
	*sync.RWMutex
	txn    txnif.AsyncTxn
	linkIt *common.LinkIt
	curr   *catalog.TableEntry
}

func newRelationIt(txn txnif.AsyncTxn, meta *catalog.DBEntry) *txnRelationIt {
	it := &txnRelationIt{
		RWMutex: meta.RWMutex,
		txn:     txn,
		linkIt:  meta.MakeTableIt(true),
	}
	if it.linkIt.Valid() {
		it.curr = it.linkIt.Get().GetPayload().(*catalog.TableEntry)
	}
	return it
}

func (it *txnRelationIt) Close() error { return nil }

func (it *txnRelationIt) Valid() bool { return it.linkIt.Valid() }

func (it *txnRelationIt) Next() {
	valid := true
	for {
		it.linkIt.Next()
		node := it.linkIt.Get()
		if node == nil {
			it.curr = nil
			break
		}
		entry := node.GetPayload().(*catalog.TableEntry)
		entry.RLock()
		valid = entry.TxnCanRead(it.txn, entry.RWMutex)
		entry.RUnlock()
		if valid {
			it.curr = entry
			break
		}
	}
}

func (it *txnRelationIt) GetRelation() handle.Relation {
	return newRelation(it.txn, it.curr)
}

type txnRelation struct {
	*txnbase.TxnRelation
	entry *catalog.TableEntry
}

func newRelation(txn txnif.AsyncTxn, meta *catalog.TableEntry) *txnRelation {
	rel := &txnRelation{
		TxnRelation: &txnbase.TxnRelation{
			Txn: txn,
		},
		entry: meta,
	}
	return rel
}

func (h *txnRelation) ID() uint64     { return h.entry.GetID() }
func (h *txnRelation) String() string { return h.entry.String() }

func (h *txnRelation) GetMeta() interface{}   { return h.entry }
func (h *txnRelation) GetSchema() interface{} { return h.entry.GetSchema() }

func (h *txnRelation) Close() error                     { return nil }
func (h *txnRelation) Rows() int64                      { return 0 }
func (h *txnRelation) Size(attr string) int64           { return 0 }
func (h *txnRelation) GetCardinality(attr string) int64 { return 0 }
func (h *txnRelation) MakeReader() handle.Reader        { return nil }

func (h *txnRelation) BatchDedup(col *vector.Vector) error {
	return h.Txn.GetStore().BatchDedup(h.entry.GetID(), col)
}

func (h *txnRelation) Append(data *batch.Batch) error {
	return h.Txn.GetStore().Append(h.entry.GetID(), data)
}

func (h *txnRelation) CreateSegment() (seg handle.Segment, err error) {
	return h.Txn.GetStore().CreateSegment(h.entry.GetID())
}

func (h *txnRelation) MakeSegmentIt() handle.SegmentIt {
	return newSegmentIt(h.Txn, h.entry)
}

func (h *txnRelation) MakeBlockIt() handle.BlockIt {
	return newRelationBlockIt(h)
}
