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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

func (entry *TableEntry) CollectTombstoneInRange(
	ctx context.Context,
	start, end types.TS,
	objectID objectio.ObjectId,
	mp *mpool.MPool,
	vpool *containers.VectorPool,
) (bat *containers.Batch, err error) {
	it := entry.MakeObjectIt(false, true)
	for ; it.Valid(); it.Next() {
		node := it.Get()
		tombstone := node.GetPayload()
		tombstone.RLock()
		skip := tombstone.IsCreatingOrAbortedLocked() || tombstone.HasDropCommittedLocked()
		tombstone.RUnlock()
		if skip {
			continue
		}
		visible := tombstone.IsVisibleInRange(start, end)
		if !visible {
			continue
		}
		if tombstone.HasCommittedPersistedData() {
			zm := tombstone.GetSortKeyZonemap()
			if !zm.PrefixEq(objectID[:]) {
				continue
			}
			// TODO: Bloomfilter
		}
		err = tombstone.GetObjectData().CollectObjectTombstoneInRange(ctx, start, end, &objectID, &bat, mp, vpool)
		if err != nil {
			return nil, err
		}
	}
	return
}

func (entry *TableEntry) IsDeleted(
	ctx context.Context,
	txn txnif.TxnReader,
	rowID types.Rowid,
	vp *containers.VectorPool,
	mp *mpool.MPool) (deleted bool, err error) {
	rowIDs := vp.GetVector(&objectio.RowidType)
	rowIDs.Append(rowID, false)
	defer rowIDs.Close()
	rowIDZM := index.NewZM(objectio.RowidType.Oid, objectio.RowidType.Scale)
	if err = index.BatchUpdateZM(rowIDZM, rowIDs.GetDownstreamVector()); err != nil {
		return
	}

	it := entry.MakeObjectIt(false, true)
	for ; it.Valid(); it.Next() {
		node := it.Get()
		tombstone := node.GetPayload()
		tombstone.RLock()
		visible, err := tombstone.IsVisibleWithLock(txn, tombstone.RWMutex)
		tombstone.RUnlock()
		if err != nil {
			return false, err
		}
		if !visible {
			continue
		}
		err = tombstone.GetObjectData().Contains(ctx, txn, rowIDs, rowIDZM, nil, mp)
		if err != nil {
			return false, err
		}
		if rowIDs.IsNull(0) {
			return true, nil
		}
	}
	return
}

func (entry *TableEntry) OnApplyDelete(
	deleted uint64,
	ts types.TS) (err error) {
	entry.RemoveRows(deleted)
	return
}

func (entry *TableEntry) HybridScan(
	txn txnif.TxnReader,
	bat **containers.Batch,
	readSchema *Schema,
	colIdxs []int,
	blkID *objectio.Blockid,
	mp *mpool.MPool,
) error {
	dataObject, err := entry.GetObjectByID(blkID.Object(), false)
	if err != nil {
		return err
	}
	_, offset := blkID.Offsets()
	err = dataObject.GetObjectData().Scan(bat, txn, readSchema, offset, colIdxs, mp)
	if err != nil {
		return err
	}
	if *bat == nil {
		return nil
	}
	it := entry.MakeObjectIt(false, true)
	for ; it.Valid(); it.Next() {
		tombstone := it.Get().GetPayload()
		err := tombstone.GetObjectData().FillBlockTombstones(txn, blkID, &(*bat).Deletes, mp)
		if err != nil {
			return err
		}
	}
	id := dataObject.AsCommonID()
	id.BlockID = *blkID
	err = txn.GetStore().FillInWorkspaceDeletes(id, &(*bat).Deletes)
	return err
}
