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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type SharedAppender interface {
	Append(node txnif.AppendableNode) error
	Close()

	// Test interfaces
	GetCurrentAobj() *aobject
	GetRefedAobjs() []*aobject
}

type sharedAppender struct {
	table       *catalog.TableEntry
	txn         txnif.AsyncTxn
	rt          *dbutils.Runtime
	isTombstone bool

	currentEntry *catalog.ObjectEntry
	currentAobj  *aobject
	nextRow      uint32
	maxRows      uint32
	refedAobjs   []*aobject
}

func NewSharedAppender(
	table *catalog.TableEntry,
	txn txnif.AsyncTxn,
	rt *dbutils.Runtime,
	isTombstone bool,
) SharedAppender {
	return &sharedAppender{
		table:       table,
		txn:         txn,
		rt:          rt,
		isTombstone: isTombstone,
		refedAobjs:  make([]*aobject, 0),
	}
}

func (app *sharedAppender) Append(node txnif.AppendableNode) error {
	if node == nil {
		return nil
	}

	totalRows := node.Rows()
	if totalRows == 0 {
		return nil
	}

	remaining := totalRows
	srcOffset := uint32(0)

	for remaining > 0 {
		objEntry, aobj, err := app.ensureAobj()
		if err != nil {
			return err
		}

		startRow, allocated := app.allocateRows(remaining)

		if err := app.createAppendNode(aobj, startRow, allocated); err != nil {
			return err
		}

		if err := app.generatePhyAddr(node, objEntry, srcOffset, allocated, startRow); err != nil {
			return err
		}

		if err := app.writeData(node, aobj, srcOffset, allocated); err != nil {
			return err
		}

		node.AddApplyInfo(srcOffset, allocated, startRow, allocated, objEntry.AsCommonID())

		srcOffset += allocated
		remaining -= allocated
	}

	return nil
}

func (app *sharedAppender) Close() {
	for _, aobj := range app.refedAobjs {
		aobj.Unref()
	}
	app.refedAobjs = nil
}

func (app *sharedAppender) ensureAobj() (*catalog.ObjectEntry, *aobject, error) {
	if app.currentAobj != nil && app.nextRow < app.maxRows && !app.currentAobj.IsAppendFrozen() {
		return app.currentEntry, app.currentAobj, nil
	}

	objEntry := catalog.NewInMemoryObject(
		app.table,
		types.BuildTS(time.Now().UnixNano(), 0),
		app.isTombstone,
	)

	app.table.Lock()
	app.table.AddEntryLocked(objEntry)
	app.table.Unlock()

	aobj := newAObject(objEntry, app.rt, app.isTombstone)
	aobj.Ref()
	app.refedAobjs = append(app.refedAobjs, aobj)

	app.currentEntry = objEntry
	app.currentAobj = aobj
	app.nextRow = 0
	app.maxRows = app.table.GetLastestSchema(app.isTombstone).Extra.BlockMaxRows

	return objEntry, aobj, nil
}

func (app *sharedAppender) allocateRows(count uint32) (startRow, allocated uint32) {
	available := app.maxRows - app.nextRow
	allocated = count
	if allocated > available {
		allocated = available
	}
	startRow = app.nextRow
	app.nextRow += allocated
	return
}

func (app *sharedAppender) createAppendNode(aobj *aobject, startRow, count uint32) error {
	aobj.Lock()
	defer aobj.Unlock()
	_, _ = aobj.appendMVCC.AddAppendNodeLocked(app.txn, startRow, startRow+count)
	return nil
}

func (app *sharedAppender) generatePhyAddr(
	node txnif.AppendableNode,
	objEntry *catalog.ObjectEntry,
	srcOffset, count, destOffset uint32,
) error {
	if app.isTombstone {
		return nil
	}

	schema := app.table.GetLastestSchema(app.isTombstone)
	phyAddrIdx := schema.PhyAddrKey.Idx

	blkID := objectio.NewBlockidWithObjectID(objEntry.ID(), 0)
	col := app.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	defer col.Close()

	if err := objectio.ConstructRowidColumnTo(
		col.GetDownstreamVector(),
		&blkID,
		destOffset,
		count,
		col.GetAllocator(),
	); err != nil {
		return err
	}

	data := node.GetData()
	if data == nil {
		return moerr.NewInternalErrorNoCtx("node data is nil")
	}

	// Update PhyAddr column in-place instead of extending
	phyAddrVec := data.Vecs[phyAddrIdx]
	for i := uint32(0); i < count; i++ {
		rowid := col.Get(int(i))
		phyAddrVec.Update(int(srcOffset+i), rowid, false)
	}

	return nil
}

func (app *sharedAppender) writeData(
	node txnif.AppendableNode,
	aobj *aobject,
	srcOffset, count uint32,
) error {
	data := node.GetData()
	if data == nil {
		return moerr.NewInternalErrorNoCtx("node data is nil")
	}

	bat := data.Window(int(srcOffset), int(count))
	defer bat.Close()

	aobj.Lock()
	defer aobj.Unlock()

	n := aobj.PinNode()
	defer n.Unref()

	if !n.IsPersisted() {
		mnode := n.MustMNode()
		_, err := mnode.ApplyAppendLocked(bat)
		return err
	}

	return moerr.NewInternalErrorNoCtx("cannot append to persisted node")
}

func (app *sharedAppender) GetCurrentAobj() *aobject {
	return app.currentAobj
}

func (app *sharedAppender) GetRefedAobjs() []*aobject {
	return app.refedAobjs
}
