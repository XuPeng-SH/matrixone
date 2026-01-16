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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test 1: Verify IsSharedAobj() correctly identifies shared aobj
func TestSharedAobj_Identification(t *testing.T) {
	defer testutils.AfterTest(t)()

	schema := catalog.MockSchema(2, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	txn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	db, err := c.CreateDBEntry("db", "", "", txn)
	require.NoError(t, err)

	table, err := db.CreateTableEntry(schema, txn, nil)
	require.NoError(t, err)

	createTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	// 创建共享 aobj (UUID v7)
	sharedAobj := catalog.NewInMemoryObject(table, createTxn.GetStartTS(), false)

	// 验证是共享 aobj
	assert.True(t, sharedAobj.IsSharedAobj(), "NewInMemoryObject should create shared aobj with UUID v7")

	err = createTxn.Commit(context.Background())
	require.NoError(t, err)
}

// Test 2: Verify GetMinCommitTS() returns correct value
func TestSharedAobj_GetMinCommitTS(t *testing.T) {
	defer testutils.AfterTest(t)()

	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 8192
	c := catalog.MockCatalog(nil)
	defer c.Close()

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	txn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	db, err := c.CreateDBEntry("db", "", "", txn)
	require.NoError(t, err)

	table, err := db.CreateTableEntry(schema, txn, nil)
	require.NoError(t, err)

	rt := dbutils.NewRuntime()

	createTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	// 创建共享 aobj
	sharedAobj := catalog.NewInMemoryObject(table, createTxn.GetStartTS(), false)
	table.Lock()
	table.AddEntryLocked(sharedAobj)
	table.Unlock()

	err = createTxn.Commit(context.Background())
	require.NoError(t, err)

	// 创建 aobject
	aobj := newAObject(sharedAobj, rt, false)
	aobj.Ref()
	defer aobj.Unref()

	// 模拟多个 txn append
	txn1, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.Lock()
	node1, _ := aobj.appendMVCC.AddAppendNodeLocked(txn1, 0, 100)
	aobj.Unlock()

	err = txn1.Commit(context.Background())
	require.NoError(t, err)
	commitTS1 := node1.GetEnd()

	txn2, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.Lock()
	node2, _ := aobj.appendMVCC.AddAppendNodeLocked(txn2, 100, 200)
	aobj.Unlock()

	err = txn2.Commit(context.Background())
	require.NoError(t, err)
	commitTS2 := node2.GetEnd()

	// 验证 GetMinCommitTS() - 直接从 aobject 调用
	minCommitTS := aobj.GetMinCommitTS()

	// 计算期望值
	expectedMinTS := commitTS1
	if commitTS2.LT(&commitTS1) {
		expectedMinTS = commitTS2
	}

	// 验证返回值正确
	assert.False(t, minCommitTS.IsEmpty(), "GetMinCommitTS should return non-empty timestamp")
	assert.True(t, minCommitTS.EQ(&expectedMinTS),
		"GetMinCommitTS should return min(commitTS1, commitTS2), got %v, expected %v",
		minCommitTS, expectedMinTS)

	t.Logf("GetMinCommitTS: %v", minCommitTS)
	t.Logf("Expected min(commitTS1, commitTS2): %v", expectedMinTS)
}

// TestAppendMVCCGetMaxCommitTS tests GetMaxCommitTS returns correct values
func TestAppendMVCCGetMaxCommitTS(t *testing.T) {
	defer testutils.AfterTest(t)()

	objEntry := &catalog.ObjectEntry{}
	appendMVCC := updates.NewAppendMVCCHandle(objEntry)

	// Empty appendMVCC should return empty TS
	maxTS := appendMVCC.GetMaxCommitTS()
	require.True(t, maxTS.IsEmpty(), "Empty appendMVCC should return empty TS")

	// Add uncommitted append
	t1 := types.BuildTS(time.Now().UnixNano(), 0)
	mockTxn := updates.MockTxnWithStartTS(t1)

	appendMVCC.AddAppendNodeLocked(mockTxn, 0, 10)

	// Uncommitted should return empty
	maxTS = appendMVCC.GetMaxCommitTS()
	require.True(t, maxTS.IsEmpty(), "Uncommitted append should return empty maxCommitTS")

	// Simulate commit
	time.Sleep(time.Millisecond)
	t2 := types.BuildTS(time.Now().UnixNano(), 0)
	mockTxn.CommitTS = t2

	// Get the node and commit it
	node := appendMVCC.MVCC()[0]
	node.PrepareCommit()
	node.ApplyCommit("")

	maxTS = appendMVCC.GetMaxCommitTS()
	require.Equal(t, t2, maxTS, "MaxCommitTS should equal commit timestamp")
}

// TestAppendMVCCGetMaxCommitTSMultipleNodes tests with multiple append nodes
func TestAppendMVCCGetMaxCommitTSMultipleNodes(t *testing.T) {
	defer testutils.AfterTest(t)()

	objEntry := &catalog.ObjectEntry{}
	appendMVCC := updates.NewAppendMVCCHandle(objEntry)

	// Add first append and commit
	t1 := types.BuildTS(time.Now().UnixNano(), 0)
	txn1 := updates.MockTxnWithStartTS(t1)
	appendMVCC.AddAppendNodeLocked(txn1, 0, 10)

	time.Sleep(time.Millisecond)
	commitTS1 := types.BuildTS(time.Now().UnixNano(), 0)
	txn1.CommitTS = commitTS1
	appendMVCC.MVCC()[0].PrepareCommit()
	appendMVCC.MVCC()[0].ApplyCommit("")

	// Add second append and commit
	time.Sleep(time.Millisecond)
	t2 := types.BuildTS(time.Now().UnixNano(), 0)
	txn2 := updates.MockTxnWithStartTS(t2)
	appendMVCC.AddAppendNodeLocked(txn2, 10, 20)

	time.Sleep(time.Millisecond)
	commitTS2 := types.BuildTS(time.Now().UnixNano(), 0)
	txn2.CommitTS = commitTS2
	appendMVCC.MVCC()[1].PrepareCommit()
	appendMVCC.MVCC()[1].ApplyCommit("")

	// MaxCommitTS should be the later one
	maxTS := appendMVCC.GetMaxCommitTS()
	require.Equal(t, commitTS2, maxTS, "MaxCommitTS should be the latest commit")
	require.True(t, maxTS.GT(&commitTS1), "commitTS2 should be greater than commitTS1")
}

// TestSharedAobjIncrementalDedupScenario demonstrates the core issue:
// A shared aobj created at T0 may have data committed at T1 > T0.
// Incremental dedup checking [T0+1, T2] should NOT skip this object
// based on CreatedAt alone.
func TestSharedAobjIncrementalDedupScenario(t *testing.T) {
	defer testutils.AfterTest(t)()

	// Simulate timestamps
	t0 := types.BuildTS(time.Now().UnixNano(), 0)
	time.Sleep(time.Millisecond)
	t1 := types.BuildTS(time.Now().UnixNano(), 0) // T1 > T0
	time.Sleep(time.Millisecond)
	t2 := types.BuildTS(time.Now().UnixNano(), 0) // T2 > T1

	// Create ObjectEntry with CreatedAt = T0
	objEntry := &catalog.ObjectEntry{}
	objEntry.CreatedAt = t0

	// Create appendMVCC
	appendMVCC := updates.NewAppendMVCCHandle(objEntry)

	// Simulate: Txn1 appended rows and committed at T1
	txn1 := updates.MockTxnWithStartTS(t0)
	appendMVCC.AddAppendNodeLocked(txn1, 0, 10)
	txn1.CommitTS = t1
	appendMVCC.MVCC()[0].PrepareCommit()
	appendMVCC.MVCC()[0].ApplyCommit("")

	// Dedup check range: from = T0+1, to = T2
	from := t0.Next()
	to := t2

	// Key assertions:
	// 1. CreatedAt (T0) < from (T0+1) - old logic would skip based on this
	require.True(t, objEntry.CreatedAt.LT(&from),
		"CreatedAt should be less than from")

	// 2. But maxCommitTS (T1) >= from - so we should NOT skip
	maxCommitTS := appendMVCC.GetMaxCommitTS()
	require.False(t, maxCommitTS.LT(&from),
		"MaxCommitTS should NOT be less than from, object must be checked")

	// 3. maxCommitTS should be in range [from, to]
	require.True(t, maxCommitTS.GE(&from) && maxCommitTS.LE(&to),
		"MaxCommitTS should be in dedup check range")

	t.Logf("CreatedAt=%s, from=%s, to=%s, maxCommitTS=%s",
		objEntry.CreatedAt.ToString(), from.ToString(), to.ToString(), maxCommitTS.ToString())
}

// TestAppendMVCCGetMaxCommitTS tests GetMaxCommitTS returns correct values
func TestAobjCreateNodeStart(t *testing.T) {
	defer testutils.AfterTest(t)()

	schema := catalog.MockSchema(2, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	txn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	db, err := c.CreateDBEntry("db", "", "", txn)
	require.NoError(t, err)

	tableEntry, err := db.CreateTableEntry(schema, txn, nil)
	require.NoError(t, err)

	err = txn.Commit(context.Background())
	require.NoError(t, err)

	// Create two transactions
	txn1, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)
	txn1StartTS := txn1.GetStartTS()

	txn2, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)
	txn2StartTS := txn2.GetStartTS()

	// txn2 creates aobj
	objEntry := catalog.NewObjectEntry(
		tableEntry,
		txn2,
		objectio.ObjectStats{},
		nil,
		false,
	)

	// Check CreateNode.Start
	createNodeStart := objEntry.CreateNode.Start

	t.Logf("aobj CreateNode.Start = %s", createNodeStart.ToString())
	t.Logf("txn2.StartTS = %s", txn2StartTS.ToString())
	t.Logf("txn1.StartTS = %s", txn1StartTS.ToString())

	// CreateNode.Start should equal txn2.StartTS
	require.Equal(t, txn2StartTS, createNodeStart,
		"aobj CreateNode.Start should be txn2.StartTS")

	txn1.Commit(context.Background())
	txn2.Commit(context.Background())
}

// Test visibility with committed aobj
func TestAobjVisibilityAfterCommit(t *testing.T) {
	defer testutils.AfterTest(t)()

	schema := catalog.MockSchema(2, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	txn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	db, err := c.CreateDBEntry("db", "", "", txn)
	require.NoError(t, err)

	tableEntry, err := db.CreateTableEntry(schema, txn, nil)
	require.NoError(t, err)

	err = txn.Commit(context.Background())
	require.NoError(t, err)

	// Create txn1 first
	txn1, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)
	txn1StartTS := txn1.GetStartTS()

	// Create txn2 and create aobj
	txn2, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	objEntry := catalog.NewObjectEntry(tableEntry, txn2, objectio.ObjectStats{}, nil, false)

	// Commit txn2
	err = txn2.Commit(context.Background())
	require.NoError(t, err)

	// Check visibility to txn1
	// txn1.startTS < txn2.commitTS, so aobj should NOT be visible to txn1
	visible := objEntry.IsVisible(txn1)

	t.Logf("aobj.CreateNode.Start = %s", objEntry.CreateNode.Start.ToString())
	t.Logf("aobj.CreateNode.End = %s", objEntry.CreateNode.End.ToString())
	t.Logf("txn1.StartTS = %s", txn1StartTS.ToString())
	t.Logf("IsVisible(txn1) = %v", visible)

	require.False(t, visible,
		"aobj should NOT be visible to txn1 (txn1.startTS < txn2.commitTS)")

	txn1.Commit(context.Background())
}
