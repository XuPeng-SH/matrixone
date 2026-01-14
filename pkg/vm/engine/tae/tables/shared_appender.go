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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type SharedAppender interface {
	// PrepareAppend: 分配空间、创建 AppendNode、生成 RowID
	// 返回创建的 AppendNode 列表（供 tableSpace 注册到 txnEntries）
	PrepareAppend(node txnif.AppendableNode) ([]txnif.TxnEntry, error)

	// ApplyAppend: 写入数据
	ApplyAppend() error

	// Close: 释放资源
	Close()

	// Append: 简化接口，用于测试（内部调用 PrepareAppend + ApplyAppend）
	Append(node txnif.AppendableNode) error

	// Test interfaces
	GetCurrentAobj() *aobject
	GetRefedAobjs() []*aobject
}

type appendContext struct {
	objEntry *catalog.ObjectEntry
	aobj     *aobject
	srcStart uint32
	srcCount uint32
	destRow  uint32
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

	// 记录创建的 AppendNode（用于返回给 tableSpace）
	createdAppendNodes []txnif.TxnEntry

	// For two-phase commit
	preparedNode     txnif.AppendableNode
	preparedContexts []*appendContext
}

func NewSharedAppender(
	table *catalog.TableEntry,
	txn txnif.AsyncTxn,
	rt *dbutils.Runtime,
	isTombstone bool,
) SharedAppender {
	return &sharedAppender{
		table:            table,
		txn:              txn,
		rt:               rt,
		isTombstone:      isTombstone,
		refedAobjs:       make([]*aobject, 0),
		preparedContexts: make([]*appendContext, 0),
	}
}

// Append is a simple one-step interface for testing (not used in production)
// In production, use PrepareAppend + ApplyAppend separately
func (app *sharedAppender) Append(node txnif.AppendableNode) error {
	_, err := app.PrepareAppend(node)
	if err != nil {
		return err
	}
	return app.ApplyAppend()
}

// PrepareAppend allocates space, creates AppendNodes, and generates RowIDs
// Returns the list of created AppendNodes for tableSpace to register to txnEntries
func (app *sharedAppender) PrepareAppend(node txnif.AppendableNode) ([]txnif.TxnEntry, error) {
	if node == nil {
		return nil, nil
	}

	data := node.GetData()
	if data == nil {
		return nil, moerr.NewInternalErrorNoCtx("node data is nil")
	}

	totalRows := node.Rows()
	if totalRows == 0 {
		return nil, nil
	}

	// Save node for ApplyAppend
	app.preparedNode = node
	app.createdAppendNodes = make([]txnif.TxnEntry, 0)
	app.preparedContexts = make([]*appendContext, 0)

	// Generate RowIDs and create AppendNodes
	schema := app.table.GetLastestSchema(app.isTombstone)

	// Handle PhyAddr column (only for data table, not tombstone)
	var phyAddrVec containers.Vector
	var phyAddrIdx int
	if !app.isTombstone {
		phyAddrIdx = schema.PhyAddrKey.Idx
		phyAddrVec = app.rt.VectorPool.Small.GetVector(&objectio.RowidType)
		defer func() {
			data.Vecs[phyAddrIdx].Close()
			data.Vecs[phyAddrIdx] = phyAddrVec
		}()
	}

	remaining := totalRows
	srcOffset := uint32(0)

	for remaining > 0 {
		objEntry, aobj, err := app.ensureAobj()
		if err != nil {
			return nil, err
		}

		startRow, allocated := app.allocateRows(remaining)

		// Create AppendNode in MVCC
		appendNode, created, err := app.createAppendNode(aobj, startRow, allocated)
		if err != nil {
			return nil, err
		}
		
		// If created, add to the list for tableSpace to register
		if created && appendNode != nil {
			app.createdAppendNodes = append(app.createdAppendNodes, appendNode)
		}

		// Generate RowIDs
		if err := app.generatePhyAddr(phyAddrVec, objEntry, allocated, startRow); err != nil {
			return nil, err
		}

		// Save context for ApplyAppend
		app.preparedContexts = append(app.preparedContexts, &appendContext{
			objEntry: objEntry,
			aobj:     aobj,
			srcStart: srcOffset,
			srcCount: allocated,
			destRow:  startRow,
		})

		// Register object to txn (warChecker, GetMemo)
		app.registerObjectToTxn(objEntry)

		// Notify node about the mapping
		node.AddApplyInfo(srcOffset, allocated, startRow, allocated, objEntry.AsCommonID())

		srcOffset += allocated
		remaining -= allocated
	}

	return app.createdAppendNodes, nil
}

// ApplyAppend writes the actual data to aobjects
func (app *sharedAppender) ApplyAppend() error {
	if app.preparedNode == nil {
		return nil
	}

	data := app.preparedNode.GetData()
	if data == nil {
		return moerr.NewInternalErrorNoCtx("prepared node data is nil")
	}

	// Write data for each context
	for _, ctx := range app.preparedContexts {
		if err := app.writeDataToAobj(data, ctx); err != nil {
			return err
		}
	}

	// Clear prepared state
	app.preparedNode = nil
	app.preparedContexts = nil

	return nil
}

func (app *sharedAppender) writeDataToAobj(data *containers.Batch, ctx *appendContext) error {
	bat := data.Window(int(ctx.srcStart), int(ctx.srcCount))
	defer bat.Close()

	ctx.aobj.Lock()
	defer ctx.aobj.Unlock()

	n := ctx.aobj.PinNode()
	defer n.Unref()

	if !n.IsPersisted() {
		mnode := n.MustMNode()
		_, err := mnode.ApplyAppendLocked(bat)
		return err
	}

	return moerr.NewInternalErrorNoCtx("cannot append to persisted node")
}

func (app *sharedAppender) Close() {
	for _, aobj := range app.refedAobjs {
		aobj.Unref()
	}
	app.refedAobjs = nil
	app.preparedNode = nil
	app.preparedContexts = nil
}

func (app *sharedAppender) ensureAobj() (*catalog.ObjectEntry, *aobject, error) {
	if app.currentAobj != nil && app.nextRow < app.maxRows && !app.currentAobj.IsAppendFrozen() {
		return app.currentEntry, app.currentAobj, nil
	}

	// Create ObjectEntry with txn's StartTS
	// The ObjectEntry itself is visible once created, but data visibility is controlled by AppendNodes
	objEntry := catalog.NewInMemoryObject(app.table, app.txn.GetStartTS(), app.isTombstone)
	app.table.Lock()
	app.table.AddEntryLocked(objEntry)
	app.table.Unlock()

	// Initialize ObjectData using DataFactory (if available)
	dataFactory := app.table.GetDB().GetCatalog().DataFactory
	if dataFactory != nil {
		objEntry.InitData(dataFactory)
	}

	aobj := objEntry.GetObjectData().(*aobject)
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

func (app *sharedAppender) createAppendNode(aobj *aobject, startRow, count uint32) (txnif.TxnEntry, bool, error) {
	aobj.Lock()
	defer aobj.Unlock()
	node, created := aobj.appendMVCC.AddAppendNodeLocked(app.txn, startRow, startRow+count)
	return node, created, nil
}

func (app *sharedAppender) generatePhyAddr(
	phyAddrVec containers.Vector,
	objEntry *catalog.ObjectEntry,
	count, destOffset uint32,
) error {
	if app.isTombstone {
		return nil
	}

	blkID := objectio.NewBlockidWithObjectID(objEntry.ID(), 0)
	col := app.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	defer col.Close()

	// Construct rowids to temporary col
	if err := objectio.ConstructRowidColumnTo(
		col.GetDownstreamVector(),
		&blkID,
		destOffset,
		count,
		col.GetAllocator(),
	); err != nil {
		return err
	}

	// Extend to phyAddrVec (accumulate all rowids)
	return phyAddrVec.ExtendVec(col.GetDownstreamVector())
}

func (app *sharedAppender) registerObjectToTxn(objEntry *catalog.ObjectEntry) {
	// Register to GetMemo
	id := objEntry.AsCommonID()
	app.txn.GetMemo().AddObject(
		app.table.GetDB().ID,
		id.TableID,
		id.ObjectID(),
		app.isTombstone,
	)
	
	// Note: warChecker.Insert will be handled by tableSpace
	// because warChecker is not accessible from SharedAppender
}

func (app *sharedAppender) GetCurrentAobj() *aobject {
	return app.currentAobj
}

func (app *sharedAppender) GetRefedAobjs() []*aobject {
	return app.refedAobjs
}
