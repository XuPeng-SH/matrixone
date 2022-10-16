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

package txnimpl

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type replayTxnStore struct {
	txnbase.NoopTxnStore
	Cmd         *txnbase.TxnCmd
	Observer    wal.ReplayObserver
	catalog     *catalog.Catalog
	dataFactory *tables.DataFactory
	cache       *bytes.Buffer
	wal         wal.Driver
}

func MakeReplayTxn(
	mgr *txnbase.TxnManager,
	ctx *txnbase.TxnCtx,
	lsn uint64,
	cmd *txnbase.TxnCmd,
	observer wal.ReplayObserver,
	catalog *catalog.Catalog,
	dataFactory *tables.DataFactory,
	cache *bytes.Buffer,
	wal wal.Driver) *txnbase.Txn {
	store := &replayTxnStore{
		Cmd:         cmd,
		Observer:    observer,
		catalog:     catalog,
		dataFactory: dataFactory,
		cache:       cache,
		wal:         wal,
	}
	txn := txnbase.NewPersistedTxn(
		mgr,
		ctx,
		store,
		lsn,
		store.prepareCommit,
		store.prepareRollback,
		store.applyCommit,
		store.applyRollback)
	return txn
}

func (store *replayTxnStore) IsReadonly() bool { return false }

func (store *replayTxnStore) prepareCommit(txn txnif.AsyncTxn) (err error) {
	logutil.Infof("TODO PreparCommit %s", txn.String())
	// TODO
	// PrepareCommit all commands
	// Check idempotent of each command
	// Record all idempotent error commands and skip apply|rollback later
	idxCtx := &wal.Index{LSN: txn.GetLSN()}
	idxCtx.Size = store.Cmd.CmdSize
	internalCnt := uint32(0)
	store.Observer.OnTimeStamp(txn.GetPrepareTS())
	for i, command := range store.Cmd.Cmds {
		command.SetReplayTxn(txn)
		if command.GetType() == CmdAppend {
			internalCnt++
			store.prepareCmd(command, nil)
		} else {
			idx := idxCtx.Clone()
			idx.CSN = uint32(i) - internalCnt
			store.prepareCmd(command, idx)
		}
	}
	return
}

func (store *replayTxnStore) applyCommit(txn txnif.AsyncTxn) (err error) {
	store.Cmd.ApplyCommit()
	return
}

func (store *replayTxnStore) applyRollback(txn txnif.AsyncTxn) (err error) {
	store.Cmd.ApplyRollback()
	return
}

func (store *replayTxnStore) prepareRollback(txn txnif.AsyncTxn) (err error) {
	panic(moerr.NewInternalError("cannot prepareRollback rollback replay txn: %s",
		txn.String()))
}

func (store *replayTxnStore) prepareCmd(txncmd txnif.TxnCmd, idxCtx *wal.Index) {
	if idxCtx != nil && idxCtx.Size > 0 {
		logutil.Debug("", common.OperationField("replay-cmd"),
			common.OperandField(txncmd.Desc()),
			common.AnyField("index", idxCtx.String()))
	}
	var err error
	switch cmd := txncmd.(type) {
	case *catalog.EntryCommand:
		store.catalog.ReplayCmd(txncmd, store.dataFactory, idxCtx, store.Observer, store.cache)
	case *AppendCmd:
		store.AppendData(cmd, store.Observer)
	case *updates.UpdateCmd:
		store.Update(cmd, idxCtx, store.Observer)
	}
	if err != nil {
		panic(err)
	}
}

func (store *replayTxnStore) AppendData(cmd *AppendCmd, observer wal.ReplayObserver) {
	hasActive := false
	for _, info := range cmd.Infos {
		database, err := store.catalog.GetDatabaseByID(info.GetDBID())
		if err != nil {
			panic(err)
		}
		id := info.GetDest()
		blk, err := database.GetBlockEntryByID(id)
		if err != nil {
			panic(err)
		}
		if !blk.IsActive() {
			continue
		}
		if observer != nil {
			observer.OnTimeStamp(blk.GetBlockData().GetMaxCheckpointTS())
		}
		if !blk.GetBlockData().GetMaxCheckpointTS().IsEmpty() {
			continue
		}
		hasActive = true
	}

	if !hasActive {
		return
	}

	var data *containers.Batch

	for _, subTxnCmd := range cmd.Cmds {
		switch subCmd := subTxnCmd.(type) {
		case *txnbase.BatchCmd:
			data = subCmd.Bat
		case *txnbase.PointerCmd:
			batEntry, err := store.wal.LoadEntry(subCmd.Group, subCmd.Lsn)
			if err != nil {
				panic(err)
			}
			r := bytes.NewBuffer(batEntry.GetPayload())
			txnCmd, _, err := txnbase.BuildCommandFrom(r)
			if err != nil {
				panic(err)
			}
			data = txnCmd.(*txnbase.BatchCmd).Bat
			batEntry.Free()
		}
	}
	if data != nil {
		defer data.Close()
	}

	for _, info := range cmd.Infos {
		database, err := store.catalog.GetDatabaseByID(info.GetDBID())
		if err != nil {
			panic(err)
		}
		id := info.GetDest()
		blk, err := database.GetBlockEntryByID(id)
		if err != nil {
			panic(err)
		}
		if !blk.IsActive() {
			continue
		}
		if observer != nil {
			observer.OnTimeStamp(blk.GetBlockData().GetMaxCheckpointTS())
		}
		if cmd.Ts.LessEq(blk.GetBlockData().GetMaxCheckpointTS()) {
			continue
		}
		start := info.GetSrcOff()
		bat := data.CloneWindow(int(start), int(info.GetSrcLen()))
		bat.Compact()
		defer bat.Close()
		if err = blk.GetBlockData().OnReplayAppendPayload(bat); err != nil {
			panic(err)
		}
	}
}

func (store *replayTxnStore) Update(cmd *updates.UpdateCmd, idxCtx *wal.Index, observer wal.ReplayObserver) {
	switch cmd.GetType() {
	case txnbase.CmdAppend:
		store.AppendReplay(cmd, idxCtx, observer)
	case txnbase.CmdDelete:
		store.Delete(cmd, idxCtx, observer)
	}
}

func (store *replayTxnStore) Delete(cmd *updates.UpdateCmd, idxCtx *wal.Index, observer wal.ReplayObserver) {
	database, err := store.catalog.GetDatabaseByID(cmd.GetDBID())
	if err != nil {
		panic(err)
	}
	deleteNode := cmd.GetDeleteNode()
	deleteNode.SetLogIndex(idxCtx)
	if deleteNode.Is1PC() {
		if _, err := deleteNode.TxnMVCCNode.ApplyCommit(nil); err != nil {
			panic(err)
		}
	}
	id := deleteNode.GetID()
	blk, err := database.GetBlockEntryByID(id)
	if err != nil {
		panic(err)
	}
	if !blk.IsActive() {
		observer.OnStaleIndex(idxCtx)
		return
	}
	blkData := blk.GetBlockData()
	err = blkData.OnReplayDelete(deleteNode)
	if err != nil {
		panic(err)
	}

}

func (store *replayTxnStore) AppendReplay(cmd *updates.UpdateCmd, idxCtx *wal.Index, observer wal.ReplayObserver) {
	database, err := store.catalog.GetDatabaseByID(cmd.GetDBID())
	if err != nil {
		panic(err)
	}
	appendNode := cmd.GetAppendNode()
	appendNode.SetLogIndex(idxCtx)
	if appendNode.Is1PC() {
		if _, err := appendNode.TxnMVCCNode.ApplyCommit(nil); err != nil {
			panic(err)
		}
	}
	id := appendNode.GetID()
	blk, err := database.GetBlockEntryByID(id)
	if err != nil {
		panic(err)
	}
	if !blk.IsActive() {
		observer.OnStaleIndex(idxCtx)
		return
	}
	if appendNode.GetCommitTS().LessEq(blk.GetBlockData().GetMaxCheckpointTS()) {
		observer.OnStaleIndex(idxCtx)
		return
	}
	if err = blk.GetBlockData().OnReplayAppend(appendNode); err != nil {
		panic(err)
	}
}
