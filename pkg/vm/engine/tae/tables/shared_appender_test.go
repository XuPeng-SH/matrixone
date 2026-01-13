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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockAppendableNode implements txnif.AppendableNode for testing
type mockAppendableNode struct {
	data    *containers.Batch
	appends []mockAppendInfo
}

type mockAppendInfo struct {
	srcOff  uint32
	srcLen  uint32
	destOff uint32
	destLen uint32
	dest    common.ID
}

func (n *mockAppendableNode) Rows() uint32 {
	return uint32(n.data.Length())
}

func (n *mockAppendableNode) GetData() *containers.Batch {
	return n.data
}

func (n *mockAppendableNode) AddApplyInfo(srcOff, srcLen, destOff, destLen uint32, dest *common.ID) {
	n.appends = append(n.appends, mockAppendInfo{
		srcOff:  srcOff,
		srcLen:  srcLen,
		destOff: destOff,
		destLen: destLen,
		dest:    *dest,
	})
}

func createMockNode(schema *catalog.Schema, rows int) *mockAppendableNode {
	// Use AllTypes and AllNames to include PhyAddr column
	bat := containers.MockBatchWithAttrs(schema.AllTypes(), schema.AllNames(), rows, schema.GetPrimaryKey().Idx, nil)
	return &mockAppendableNode{
		data:    bat,
		appends: make([]mockAppendInfo, 0),
	}
}

// Helper function to create test environment
func setupTest(t *testing.T) (*catalog.Catalog, *catalog.TableEntry, txnif.AsyncTxn, *txnbase.TxnManager, *dbutils.Runtime) {
	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 8192
	c := catalog.MockCatalog(nil)

	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)

	table, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())

	txn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	rt := dbutils.NewRuntime()

	return c, table, txn, txnMgr, rt
}

// Test 1: Single aobj, single append (100 rows)
func TestSharedAppender_SingleAppend(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := NewSharedAppender(table, txn, rt, false)
	defer appender.Close()

	node := createMockNode(table.GetLastestSchema(false), 100)
	defer node.data.Close()

	err := appender.Append(node)
	require.NoError(t, err)

	// Verify: 1 append info
	assert.Equal(t, 1, len(node.appends))
	assert.Equal(t, uint32(0), node.appends[0].srcOff)
	assert.Equal(t, uint32(100), node.appends[0].srcLen)
	assert.Equal(t, uint32(0), node.appends[0].destOff)
	assert.Equal(t, uint32(100), node.appends[0].destLen)
}

// Test 2: Single aobj, multiple appends
func TestSharedAppender_MultipleAppends(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := NewSharedAppender(table, txn, rt, false)
	defer appender.Close()

	schema := table.GetLastestSchema(false)

	// First append: 100 rows
	node1 := createMockNode(schema, 100)
	defer node1.data.Close()
	err := appender.Append(node1)
	require.NoError(t, err)

	// Second append: 200 rows
	node2 := createMockNode(schema, 200)
	defer node2.data.Close()
	err = appender.Append(node2)
	require.NoError(t, err)

	// Verify node1: [0, 100)
	assert.Equal(t, 1, len(node1.appends))
	assert.Equal(t, uint32(0), node1.appends[0].destOff)
	assert.Equal(t, uint32(100), node1.appends[0].destLen)

	// Verify node2: [100, 300)
	assert.Equal(t, 1, len(node2.appends))
	assert.Equal(t, uint32(100), node2.appends[0].destOff)
	assert.Equal(t, uint32(200), node2.appends[0].destLen)

	// Verify: same ObjectEntry
	assert.Equal(t, node1.appends[0].dest, node2.appends[0].dest)
}

// Test 3: Cross aobj append (8000 + 500 rows)
func TestSharedAppender_CrossAobj(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := NewSharedAppender(table, txn, rt, false)
	defer appender.Close()

	schema := table.GetLastestSchema(false)

	// Append 8000 rows
	node1 := createMockNode(schema, 8000)
	defer node1.data.Close()
	err := appender.Append(node1)
	require.NoError(t, err)

	// Append 500 more rows (should trigger new aobj)
	node2 := createMockNode(schema, 500)
	defer node2.data.Close()
	err = appender.Append(node2)
	require.NoError(t, err)

	// Verify node1: [0, 8000)
	assert.Equal(t, 1, len(node1.appends))
	assert.Equal(t, uint32(0), node1.appends[0].destOff)
	assert.Equal(t, uint32(8000), node1.appends[0].destLen)

	// Verify node2: split into 2 parts
	assert.Equal(t, 2, len(node2.appends))
	// First part: [8000, 8192) = 192 rows
	assert.Equal(t, uint32(0), node2.appends[0].srcOff)
	assert.Equal(t, uint32(192), node2.appends[0].srcLen)
	assert.Equal(t, uint32(8000), node2.appends[0].destOff)
	assert.Equal(t, uint32(192), node2.appends[0].destLen)
	// Second part: [0, 308) = 308 rows in new aobj
	assert.Equal(t, uint32(192), node2.appends[1].srcOff)
	assert.Equal(t, uint32(308), node2.appends[1].srcLen)
	assert.Equal(t, uint32(0), node2.appends[1].destOff)
	assert.Equal(t, uint32(308), node2.appends[1].destLen)

	// Verify: different ObjectEntry
	assert.NotEqual(t, node2.appends[0].dest, node2.appends[1].dest)
}

// Test 4: Exactly fill aobj (8192 rows)
func TestSharedAppender_ExactlyFillAobj(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := NewSharedAppender(table, txn, rt, false)
	defer appender.Close()

	schema := table.GetLastestSchema(false)

	node := createMockNode(schema, 8192)
	defer node.data.Close()
	err := appender.Append(node)
	require.NoError(t, err)

	// Verify: [0, 8192)
	assert.Equal(t, 1, len(node.appends))
	assert.Equal(t, uint32(0), node.appends[0].destOff)
	assert.Equal(t, uint32(8192), node.appends[0].destLen)

	// Next append should create new aobj
	node2 := createMockNode(schema, 100)
	defer node2.data.Close()
	err = appender.Append(node2)
	require.NoError(t, err)

	// Verify: new aobj [0, 100)
	assert.Equal(t, 1, len(node2.appends))
	assert.Equal(t, uint32(0), node2.appends[0].destOff)
	assert.Equal(t, uint32(100), node2.appends[0].destLen)

	// Verify: different ObjectEntry
	assert.NotEqual(t, node.appends[0].dest, node2.appends[0].dest)
}

// Test 5: Empty node
func TestSharedAppender_EmptyNode(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := NewSharedAppender(table, txn, rt, false)
	defer appender.Close()

	node := createMockNode(table.GetLastestSchema(false), 0)
	defer node.data.Close()
	err := appender.Append(node)
	require.NoError(t, err)

	// Verify: no append info
	assert.Equal(t, 0, len(node.appends))
}

// Test 6: Nil node
func TestSharedAppender_NilNode(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := NewSharedAppender(table, txn, rt, false)
	defer appender.Close()

	err := appender.Append(nil)
	require.NoError(t, err)
}

// Test 7: Tombstone appender
func TestSharedAppender_Tombstone(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := NewSharedAppender(table, txn, rt, true) // isTombstone = true
	defer appender.Close()

	node := createMockNode(table.GetLastestSchema(true), 100)
	defer node.data.Close()
	err := appender.Append(node)
	require.NoError(t, err)

	// Verify: append info recorded
	assert.Equal(t, 1, len(node.appends))
}

// Test 8: Concurrent appends (multiple txns)
func TestSharedAppender_Concurrent(t *testing.T) {
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

	// 10 concurrent txns, each appends 100 rows
	concurrency := 10
	rowsPerTxn := 100

	var wg sync.WaitGroup
	nodes := make([]*mockAppendableNode, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			txn, err := txnMgr.StartTxn(nil)
			require.NoError(t, err)

			appender := NewSharedAppender(table, txn, rt, false)
			defer appender.Close()

			node := createMockNode(schema, rowsPerTxn)
			nodes[idx] = node

			err = appender.Append(node)
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Verify: all nodes have append info
	for i := 0; i < concurrency; i++ {
		assert.Equal(t, 1, len(nodes[i].appends))
		assert.Equal(t, uint32(rowsPerTxn), nodes[i].appends[0].destLen)
		nodes[i].data.Close()
	}

	// Verify: row ranges don't overlap
	ranges := make(map[string][]uint32) // objectID -> []offsets
	for i := 0; i < concurrency; i++ {
		info := nodes[i].appends[0]
		objID := info.dest.String()
		if ranges[objID] == nil {
			ranges[objID] = make([]uint32, 0)
		}
		for row := info.destOff; row < info.destOff+info.destLen; row++ {
			ranges[objID] = append(ranges[objID], row)
		}
	}

	// Check no duplicates in each object
	for objID, offsets := range ranges {
		seen := make(map[uint32]bool)
		for _, offset := range offsets {
			assert.False(t, seen[offset], "duplicate offset %d in object %s", offset, objID)
			seen[offset] = true
		}
	}
}

// Test 9: Large batch spanning multiple aobjs
func TestSharedAppender_LargeBatch(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := NewSharedAppender(table, txn, rt, false)
	defer appender.Close()

	// Append 20000 rows (spans 3 aobjs)
	node := createMockNode(table.GetLastestSchema(false), 20000)
	defer node.data.Close()
	err := appender.Append(node)
	require.NoError(t, err)

	// Verify: 3 append infos
	assert.Equal(t, 3, len(node.appends))

	// First aobj: [0, 8192)
	assert.Equal(t, uint32(0), node.appends[0].srcOff)
	assert.Equal(t, uint32(8192), node.appends[0].srcLen)
	assert.Equal(t, uint32(0), node.appends[0].destOff)

	// Second aobj: [8192, 16384)
	assert.Equal(t, uint32(8192), node.appends[1].srcOff)
	assert.Equal(t, uint32(8192), node.appends[1].srcLen)
	assert.Equal(t, uint32(0), node.appends[1].destOff)

	// Third aobj: [16384, 20000)
	assert.Equal(t, uint32(16384), node.appends[2].srcOff)
	assert.Equal(t, uint32(3616), node.appends[2].srcLen)
	assert.Equal(t, uint32(0), node.appends[2].destOff)

	// Verify: 3 different ObjectEntry
	assert.NotEqual(t, node.appends[0].dest, node.appends[1].dest)
	assert.NotEqual(t, node.appends[1].dest, node.appends[2].dest)
}

// Test 10: Aobj with insufficient remaining space
func TestSharedAppender_InsufficientSpace(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := NewSharedAppender(table, txn, rt, false)
	defer appender.Close()

	schema := table.GetLastestSchema(false)

	// Fill to 8190 rows
	node1 := createMockNode(schema, 8190)
	defer node1.data.Close()
	err := appender.Append(node1)
	require.NoError(t, err)

	// Append 100 more rows (only 2 rows fit in current aobj)
	node2 := createMockNode(schema, 100)
	defer node2.data.Close()
	err = appender.Append(node2)
	require.NoError(t, err)

	// Verify node2: split into 2 parts
	assert.Equal(t, 2, len(node2.appends))
	// First part: [8190, 8192) = 2 rows
	assert.Equal(t, uint32(0), node2.appends[0].srcOff)
	assert.Equal(t, uint32(2), node2.appends[0].srcLen)
	assert.Equal(t, uint32(8190), node2.appends[0].destOff)
	assert.Equal(t, uint32(2), node2.appends[0].destLen)
	// Second part: [0, 98) = 98 rows in new aobj
	assert.Equal(t, uint32(2), node2.appends[1].srcOff)
	assert.Equal(t, uint32(98), node2.appends[1].srcLen)
	assert.Equal(t, uint32(0), node2.appends[1].destOff)
	assert.Equal(t, uint32(98), node2.appends[1].destLen)

	// Verify: different ObjectEntry
	assert.NotEqual(t, node2.appends[0].dest, node2.appends[1].dest)
}

// Test 11: Concurrent appends triggering aobj switch
func TestSharedAppender_ConcurrentAobjSwitch(t *testing.T) {
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

	// 5 concurrent txns, each appends 2000 rows
	concurrency := 5
	rowsPerTxn := 2000

	var wg sync.WaitGroup
	nodes := make([]*mockAppendableNode, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			txn, err := txnMgr.StartTxn(nil)
			require.NoError(t, err)

			appender := NewSharedAppender(table, txn, rt, false)
			defer appender.Close()

			node := createMockNode(schema, rowsPerTxn)
			nodes[idx] = node

			err = appender.Append(node)
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Verify: total 10000 rows, should span 2 aobjs
	totalRows := 0
	objIDs := make(map[string]bool)
	for i := 0; i < concurrency; i++ {
		for _, info := range nodes[i].appends {
			totalRows += int(info.destLen)
			objIDs[info.dest.String()] = true
		}
		nodes[i].data.Close()
	}

	assert.Equal(t, 10000, totalRows)
	assert.GreaterOrEqual(t, len(objIDs), 2, "should span at least 2 aobjs")
}

// Test 12: Data and tombstone independent allocation
func TestSharedAppender_DataAndTombstone(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	schema := table.GetLastestSchema(false)

	// Append data
	dataAppender := NewSharedAppender(table, txn, rt, false)
	defer dataAppender.Close()

	dataNode := createMockNode(schema, 100)
	defer dataNode.data.Close()
	err := dataAppender.Append(dataNode)
	require.NoError(t, err)

	// Append tombstone
	tombstoneAppender := NewSharedAppender(table, txn, rt, true)
	defer tombstoneAppender.Close()

	tombstoneNode := createMockNode(table.GetLastestSchema(true), 50)
	defer tombstoneNode.data.Close()
	err = tombstoneAppender.Append(tombstoneNode)
	require.NoError(t, err)

	// Verify: data and tombstone have different ObjectEntry
	assert.NotEqual(t, dataNode.appends[0].dest, tombstoneNode.appends[0].dest)

	// Verify: both allocated correctly
	assert.Equal(t, uint32(100), dataNode.appends[0].destLen)
	assert.Equal(t, uint32(50), tombstoneNode.appends[0].destLen)
}
