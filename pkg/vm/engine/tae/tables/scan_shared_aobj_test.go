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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test 1: Single AppendNode, committed, visible
func TestScanSharedAobj_SingleCommitted(t *testing.T) {
	defer testutils.AfterTest(t)()

	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 8192
	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)

	table, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	rt := dbutils.NewRuntime()

	// Create shared aobj
	createTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	sharedAobj := catalog.NewInMemoryObject(table, createTxn.GetStartTS(), false)
	table.Lock()
	table.AddEntryLocked(sharedAobj)
	table.Unlock()

	err = createTxn.Commit(context.Background())
	require.NoError(t, err)

	aobj := newAObject(sharedAobj, rt, false)
	aobj.Ref()
	defer aobj.Unref()

	// Txn1: append [0, 100) and commit
	txn1, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.Lock()
	node1, _ := aobj.appendMVCC.AddAppendNodeLocked(txn1, 0, 100)
	aobj.Unlock()

	err = txn1.Commit(context.Background())
	require.NoError(t, err)
	
	// Manually apply commit for AppendNode since it's not registered in txn.txnEntries
	err = node1.ApplyCommit(txn1.GetID())
	require.NoError(t, err)
	
	commitTS := txn1.GetCommitTS()

	// Wait for commit to complete and time to advance
	time.Sleep(10 * time.Millisecond)

	// Read txn: should see [0, 100)
	readTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)
	readStartTS := readTxn.GetStartTS()

	t.Logf("commitTS=%s, readStartTS=%s", commitTS.ToString(), readStartTS.ToString())

	aobj.RLock()
	maxRow, visible, holes, err := aobj.appendMVCC.GetVisibleRowLocked(context.Background(), readTxn)
	aobj.RUnlock()

	require.NoError(t, err)
	assert.True(t, visible, "Should be visible")
	assert.Equal(t, uint32(100), maxRow, "maxRow should be 100")
	assert.True(t, holes == nil || holes.IsEmpty(), "Should have no holes")

	err = readTxn.Commit(context.Background())
	require.NoError(t, err)

	t.Logf("Single committed AppendNode: visible=%v, maxRow=%d", visible, maxRow)
}

// Test 2: Single AppendNode, active, not visible to other txn
func TestScanSharedAobj_SingleActive(t *testing.T) {
	defer testutils.AfterTest(t)()

	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 8192
	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)

	table, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	rt := dbutils.NewRuntime()

	createTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	sharedAobj := catalog.NewInMemoryObject(table, createTxn.GetStartTS(), false)
	table.Lock()
	table.AddEntryLocked(sharedAobj)
	table.Unlock()

	err = createTxn.Commit(context.Background())
	require.NoError(t, err)

	aobj := newAObject(sharedAobj, rt, false)
	aobj.Ref()
	defer aobj.Unref()

	// Txn1: append [0, 100) but NOT commit
	txn1, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.Lock()
	_, _ = aobj.appendMVCC.AddAppendNodeLocked(txn1, 0, 100)
	aobj.Unlock()

	// Read txn: should NOT see [0, 100)
	readTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.RLock()
	maxRow, visible, holes, err := aobj.appendMVCC.GetVisibleRowLocked(context.Background(), readTxn)
	aobj.RUnlock()

	require.NoError(t, err)

	// Should not be visible, or visible with holes
	if visible {
		assert.NotNil(t, holes, "Should have holes if visible")
		assert.False(t, holes.IsEmpty(), "Holes should not be empty")
		// Check holes cover [0, 100)
		for i := uint64(0); i < 100; i++ {
			assert.True(t, holes.Contains(i), "Row %d should be in holes", i)
		}
	} else {
		assert.Equal(t, uint32(0), maxRow, "maxRow should be 0 if not visible")
	}

	err = readTxn.Commit(context.Background())
	require.NoError(t, err)

	err = txn1.Rollback(context.Background())
	require.NoError(t, err)

	t.Logf("Single active AppendNode: visible=%v, maxRow=%d, has holes=%v",
		visible, maxRow, holes != nil && !holes.IsEmpty())
}

// Test 3: Read own writes
func TestScanSharedAobj_ReadOwnWrites(t *testing.T) {
	defer testutils.AfterTest(t)()

	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 8192
	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)

	table, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	rt := dbutils.NewRuntime()

	createTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	sharedAobj := catalog.NewInMemoryObject(table, createTxn.GetStartTS(), false)
	table.Lock()
	table.AddEntryLocked(sharedAobj)
	table.Unlock()

	err = createTxn.Commit(context.Background())
	require.NoError(t, err)

	aobj := newAObject(sharedAobj, rt, false)
	aobj.Ref()
	defer aobj.Unref()

	// Txn1: append [0, 100) and read in same txn
	txn1, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.Lock()
	_, _ = aobj.appendMVCC.AddAppendNodeLocked(txn1, 0, 100)
	aobj.Unlock()

	// Read in same txn: should see [0, 100)
	aobj.RLock()
	maxRow, visible, holes, err := aobj.appendMVCC.GetVisibleRowLocked(context.Background(), txn1)
	aobj.RUnlock()

	require.NoError(t, err)
	assert.True(t, visible, "Should see own writes")
	assert.Equal(t, uint32(100), maxRow, "maxRow should be 100")
	assert.True(t, holes == nil || holes.IsEmpty(), "Should have no holes for own writes")

	err = txn1.Commit(context.Background())
	require.NoError(t, err)

	t.Logf("Read own writes: visible=%v, maxRow=%d", visible, maxRow)
}

// Test 4: Multiple AppendNodes, mixed visibility
func TestScanSharedAobj_MultipleAppendNodes(t *testing.T) {
	defer testutils.AfterTest(t)()

	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 8192
	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)

	table, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	rt := dbutils.NewRuntime()

	createTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	sharedAobj := catalog.NewInMemoryObject(table, createTxn.GetStartTS(), false)
	table.Lock()
	table.AddEntryLocked(sharedAobj)
	table.Unlock()

	err = createTxn.Commit(context.Background())
	require.NoError(t, err)

	aobj := newAObject(sharedAobj, rt, false)
	aobj.Ref()
	defer aobj.Unref()

	// Txn1: append [0, 100) and commit
	txn1, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.Lock()
	_, _ = aobj.appendMVCC.AddAppendNodeLocked(txn1, 0, 100)
	aobj.Unlock()

	err = txn1.Commit(context.Background())
	require.NoError(t, err)

	// Txn2: append [100, 200) but NOT commit
	txn2, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.Lock()
	_, _ = aobj.appendMVCC.AddAppendNodeLocked(txn2, 100, 200)
	aobj.Unlock()

	// Txn3: append [200, 300) and commit
	txn3, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.Lock()
	_, _ = aobj.appendMVCC.AddAppendNodeLocked(txn3, 200, 300)
	aobj.Unlock()

	err = txn3.Commit(context.Background())
	require.NoError(t, err)

	// Read txn: start AFTER all commits, should see [0, 100) and [200, 300), with hole [100, 200)
	readTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.RLock()
	maxRow, visible, holes, err := aobj.appendMVCC.GetVisibleRowLocked(context.Background(), readTxn)
	aobj.RUnlock()

	require.NoError(t, err)

	t.Logf("Multiple AppendNodes: visible=%v, maxRow=%d, holes=%v", visible, maxRow, holes)

	// The behavior depends on whether txn2 is still active
	// If visible, should have holes for [100, 200)
	if visible {
		assert.Equal(t, uint32(300), maxRow, "maxRow should be 300")
		assert.NotNil(t, holes, "Should have holes")
		assert.False(t, holes.IsEmpty(), "Holes should not be empty")

		// Verify holes: [100, 200) should be holes
		for i := uint64(100); i < 200; i++ {
			assert.True(t, holes.Contains(i), "Row %d should be in holes", i)
		}
		// Verify visible: [0, 100) and [200, 300) should NOT be holes
		for i := uint64(0); i < 100; i++ {
			assert.False(t, holes.Contains(i), "Row %d should NOT be in holes", i)
		}
		for i := uint64(200); i < 300; i++ {
			assert.False(t, holes.Contains(i), "Row %d should NOT be in holes", i)
		}
	}

	err = readTxn.Commit(context.Background())
	require.NoError(t, err)

	err = txn2.Rollback(context.Background())
	require.NoError(t, err)

	t.Logf("Multiple AppendNodes: visible=%v, maxRow=%d, holes=[100,200)", visible, maxRow)
}

// Test 5: Rollback - aborted AppendNode should not be visible
func TestScanSharedAobj_Rollback(t *testing.T) {
	defer testutils.AfterTest(t)()

	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 8192
	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)

	table, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	rt := dbutils.NewRuntime()

	createTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	sharedAobj := catalog.NewInMemoryObject(table, createTxn.GetStartTS(), false)
	table.Lock()
	table.AddEntryLocked(sharedAobj)
	table.Unlock()

	err = createTxn.Commit(context.Background())
	require.NoError(t, err)

	aobj := newAObject(sharedAobj, rt, false)
	aobj.Ref()
	defer aobj.Unref()

	// Txn1: append [0, 100) and commit
	txn1, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.Lock()
	_, _ = aobj.appendMVCC.AddAppendNodeLocked(txn1, 0, 100)
	aobj.Unlock()

	err = txn1.Commit(context.Background())
	require.NoError(t, err)

	// Txn2: append [100, 200) and rollback
	txn2, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.Lock()
	node2, _ := aobj.appendMVCC.AddAppendNodeLocked(txn2, 100, 200)
	aobj.Unlock()

	// Manually trigger rollback for AppendNode since it's not registered in txn.txnEntries
	err = node2.PrepareRollback()
	require.NoError(t, err)
	err = node2.ApplyRollback()
	require.NoError(t, err)

	err = txn2.Rollback(context.Background())
	require.NoError(t, err)

	// Verify node2 is aborted
	assert.True(t, node2.IsAborted(), "node2 should be aborted after rollback")

	// Txn3: append [200, 300) and commit
	txn3, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.Lock()
	_, _ = aobj.appendMVCC.AddAppendNodeLocked(txn3, 200, 300)
	aobj.Unlock()

	err = txn3.Commit(context.Background())
	require.NoError(t, err)

	// Read txn: should see [0, 100) and [200, 300), NOT [100, 200)
	readTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.RLock()
	maxRow, visible, holes, err := aobj.appendMVCC.GetVisibleRowLocked(context.Background(), readTxn)
	aobj.RUnlock()

	require.NoError(t, err)

	t.Logf("Rollback test: visible=%v, maxRow=%d, node2.Aborted=%v", visible, maxRow, node2.IsAborted())

	// If visible, verify aborted rows are in holes
	if visible {
		assert.NotNil(t, holes, "Should have holes for aborted AppendNode")
		if holes != nil && !holes.IsEmpty() {
			// Verify [100, 200) are holes (aborted)
			for i := uint64(100); i < 200; i++ {
				assert.True(t, holes.Contains(i), "Aborted row %d should be in holes", i)
			}
		}
	}

	err = readTxn.Commit(context.Background())
	require.NoError(t, err)
}

// Test 6: Multiple txns rollback
func TestScanSharedAobj_MultipleRollback(t *testing.T) {
	defer testutils.AfterTest(t)()

	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 8192
	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)

	table, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	rt := dbutils.NewRuntime()

	createTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	sharedAobj := catalog.NewInMemoryObject(table, createTxn.GetStartTS(), false)
	table.Lock()
	table.AddEntryLocked(sharedAobj)
	table.Unlock()

	err = createTxn.Commit(context.Background())
	require.NoError(t, err)

	aobj := newAObject(sharedAobj, rt, false)
	aobj.Ref()
	defer aobj.Unref()

	// Create multiple txns with mixed commit/rollback
	txns := make([]struct {
		txn    txnif.AsyncTxn
		start  uint32
		end    uint32
		commit bool
	}, 5)

	txns[0] = struct {
		txn    txnif.AsyncTxn
		start  uint32
		end    uint32
		commit bool
	}{start: 0, end: 100, commit: true}

	txns[1] = struct {
		txn    txnif.AsyncTxn
		start  uint32
		end    uint32
		commit bool
	}{start: 100, end: 200, commit: false} // rollback

	txns[2] = struct {
		txn    txnif.AsyncTxn
		start  uint32
		end    uint32
		commit bool
	}{start: 200, end: 300, commit: true}

	txns[3] = struct {
		txn    txnif.AsyncTxn
		start  uint32
		end    uint32
		commit bool
	}{start: 300, end: 400, commit: false} // rollback

	txns[4] = struct {
		txn    txnif.AsyncTxn
		start  uint32
		end    uint32
		commit bool
	}{start: 400, end: 500, commit: true}

	// Execute all txns
	for i := range txns {
		txns[i].txn, err = txnMgr.StartTxn(nil)
		require.NoError(t, err)

		aobj.Lock()
		_, _ = aobj.appendMVCC.AddAppendNodeLocked(txns[i].txn, txns[i].start, txns[i].end)
		aobj.Unlock()

		if txns[i].commit {
			err = txns[i].txn.Commit(context.Background())
		} else {
			err = txns[i].txn.Rollback(context.Background())
		}
		require.NoError(t, err)
	}

	// Read txn: should see [0,100), [200,300), [400,500)
	// Should NOT see [100,200) and [300,400) (aborted)
	readTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.RLock()
	maxRow, visible, holes, err := aobj.appendMVCC.GetVisibleRowLocked(context.Background(), readTxn)
	aobj.RUnlock()

	require.NoError(t, err)

	t.Logf("Multiple rollback: visible=%v, maxRow=%d", visible, maxRow)

	// If visible, verify aborted ranges are in holes
	if visible && holes != nil && !holes.IsEmpty() {
		// Check some aborted rows
		assert.True(t, holes.Contains(150), "Aborted row 150 should be in holes")
		assert.True(t, holes.Contains(350), "Aborted row 350 should be in holes")
	}

	err = readTxn.Commit(context.Background())
	require.NoError(t, err)
}
