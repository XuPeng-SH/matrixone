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

package tables

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type object struct {
	*baseObject
}

func newObject(
	meta *catalog.ObjectEntry,
	rt *dbutils.Runtime,
) *object {
	obj := &object{}
	obj.baseObject = newBaseObject(obj, meta, rt)
	pnode := newPersistedNode(obj.baseObject)
	node := NewNode(pnode)
	node.Ref()
	obj.node.Store(node)
	return obj
}

func (obj *object) Init() (err error) {
	return
}

func (obj *object) PrepareCompact() bool {
	prepareCompact := obj.meta.Load().PrepareCompact()
	if !prepareCompact && obj.meta.Load().CheckPrintPrepareCompact() {
		obj.meta.Load().PrintPrepareCompactDebugLog()
	}
	return prepareCompact
}

func (obj *object) PrepareCompactInfo() (result bool, reason string) {
	return obj.meta.Load().PrepareCompact(), ""
}

func (obj *object) FreezeAppend() {}

func (obj *object) Pin() *common.PinnedItem[*object] {
	obj.Ref()
	return &common.PinnedItem[*object]{
		Val: obj,
	}
}

<<<<<<< HEAD
=======
func (obj *object) GetColumnDataByIds(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	blkID uint16,
	colIdxes []int,
	mp *mpool.MPool,
) (view *containers.Batch, err error) {
	node := obj.PinNode()
	defer node.Unref()
	schema := readSchema.(*catalog.Schema)
	return obj.ResolvePersistedColumnDatas(
		ctx, txn, schema, blkID, colIdxes, false, mp,
	)
}

// GetColumnDataById Get the snapshot at txn's start timestamp of column data.
// Notice that for non-appendable object, if it is visible to txn,
// then all the object data pointed by meta location also be visible to txn;
func (obj *object) GetColumnDataById(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	blkID uint16,
	col int,
	mp *mpool.MPool,
) (view *containers.Batch, err error) {
	schema := readSchema.(*catalog.Schema)
	return obj.ResolvePersistedColumnData(
		ctx,
		txn,
		schema,
		blkID,
		col,
		false,
		mp,
	)
}
>>>>>>> main
func (obj *object) CoarseCheckAllRowsCommittedBefore(ts types.TS) bool {
	creatTS := obj.meta.Load().GetCreatedAt()
	return creatTS.Less(&ts)
}

func (obj *object) GetDuplicatedRows(
	ctx context.Context,
	txn txnif.TxnReader,
	keys containers.Vector,
	keysZM index.ZM,
	precommit bool,
	checkWWConflict bool,
	rowIDs containers.Vector,
	mp *mpool.MPool,
) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			logutil.Infof("BatchDedup %s (%v)obj-%s: %v",
				obj.meta.Load().GetTable().GetLastestSchemaLocked(false).Name,
				obj.IsAppendable(),
				obj.meta.Load().ID().String(),
				err)
		}
	}()
	return obj.persistedGetDuplicatedRows(
		ctx,
		txn,
		precommit,
		keys,
		keysZM,
		rowIDs,
		false,
		0,
		mp,
	)
}
func (obj *object) GetMaxRowByTS(ts types.TS) (uint32, error) {
	panic("not support")
}
func (obj *object) Contains(
	ctx context.Context,
	txn txnif.TxnReader,
	isCommitting bool,
	keys containers.Vector,
	keysZM index.ZM,
	mp *mpool.MPool) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			logutil.Infof("BatchDedup %s (%v)obj-%s: %v",
				obj.meta.Load().GetTable().GetLastestSchemaLocked(false).Name,
				obj.IsAppendable(),
				obj.meta.Load().ID().String(),
				err)
		}
	}()
	return obj.persistedContains(
		ctx,
		txn,
		isCommitting,
		keys,
		keysZM,
		false,
		mp,
	)
}

func (obj *object) RunCalibration() (score int, err error) {
	score, _ = obj.estimateRawScore()
	return
}

func (obj *object) estimateRawScore() (score int, dropped bool) {
	return 0, obj.meta.Load().HasDropCommitted()
}

func (obj *object) EstimateMemSize() (int, int) {
	return 0, 0
}

func (obj *object) GetRowsOnReplay() uint64 {
	stats := obj.meta.Load().GetObjectStats()
	fileRows := uint64(stats.Rows())
	return fileRows
}
