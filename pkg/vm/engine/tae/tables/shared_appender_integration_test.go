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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/require"
)

// Test 1: Verify IsLocal correctness for different ObjectEntry types
// NewStandaloneObject (IsLocal=true) should not be in catalog
// NewInMemoryObject (IsLocal=false) should be in catalog
func TestSharedAppender_TxnInternalScan(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	// Verify: NewStandaloneObject creates entry with IsLocal=true
	tableSpaceEntry := catalog.NewStandaloneObject(table, txn.GetStartTS(), false)
	require.True(t, tableSpaceEntry.IsLocal, "tableSpace.entry must be IsLocal=true")

	// Verify: tableSpaceEntry not in catalog (because IsLocal=true)
	_, err := table.GetObjectByID(tableSpaceEntry.ID(), false)
	require.Error(t, err, "tableSpace.entry should not be in catalog")

	// Use SharedAppender to create object
	appender := NewSharedAppender(table, txn, rt, false)
	defer appender.Close()

	schema := table.GetLastestSchema(false)
	node := createMockNode(schema, 100)
	defer node.data.Close()

	err = appender.Append(node)
	require.NoError(t, err)

	// Verify: SharedAppender created object is in catalog (because IsLocal=false)
	aobj := appender.GetCurrentAobj()
	require.NotNil(t, aobj)
	objEntry := aobj.meta.Load()
	require.False(t, objEntry.IsLocal, "SharedAppender created object must be IsLocal=false (globally visible)")

	foundObj, err := table.GetObjectByID(objEntry.ID(), false)
	require.NoError(t, err)
	require.Equal(t, objEntry.ID(), foundObj.ID())
}

// Test 2: PrepareAppend + ApplyAppend workflow
func TestSharedAppender_PrepareApplyNodeIntegration(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	schema := table.GetLastestSchema(false)

	appender := NewSharedAppender(table, txn, rt, false)
	defer appender.Close()

	node := createMockNode(schema, 200)
	defer node.data.Close()

	// Step 1: PrepareAppend
	appendNodes, err := appender.PrepareAppend(node)
	require.NoError(t, err)
	require.NotEmpty(t, appendNodes)

	// Step 2: Verify AppendNodes
	for _, an := range appendNodes {
		require.NotNil(t, an)
	}

	// Step 3: ApplyAppend
	err = appender.ApplyAppend()
	require.NoError(t, err)

	// Verify aobj created
	aobj := appender.GetCurrentAobj()
	require.NotNil(t, aobj)
}

// Test 3: allocateRows returns 0 when aobj is full
// Verify new aobj is created when current aobj reaches maxRows
func TestSharedAppender_AllocateRowsZero(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	schema := table.GetLastestSchema(false)
	appender := NewSharedAppender(table, txn, rt, false)
	defer appender.Close()

	// Fill first aobj (8192 rows)
	node1 := createMockNode(schema, 8192)
	defer node1.data.Close()

	appendNodes1, err := appender.PrepareAppend(node1)
	require.NoError(t, err)
	require.NotEmpty(t, appendNodes1)

	err = appender.ApplyAppend()
	require.NoError(t, err)

	firstAobj := appender.GetCurrentAobj()
	require.NotNil(t, firstAobj)

	// Append 100 more rows, should create new aobj
	node2 := createMockNode(schema, 100)
	defer node2.data.Close()

	appendNodes2, err := appender.PrepareAppend(node2)
	require.NoError(t, err)
	require.NotEmpty(t, appendNodes2)

	err = appender.ApplyAppend()
	require.NoError(t, err)

	secondAobj := appender.GetCurrentAobj()
	require.NotEqual(t, firstAobj.meta.Load().ID(), secondAobj.meta.Load().ID(), "should create new aobj")

	// Verify: both aobjs are in catalog
	_, err = table.GetObjectByID(firstAobj.meta.Load().ID(), false)
	require.NoError(t, err)
	_, err = table.GetObjectByID(secondAobj.meta.Load().ID(), false)
	require.NoError(t, err)
}

// Test 4: Verify freezelock usage in createAppendNode
func TestSharedAppender_FreezeLock(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	schema := table.GetLastestSchema(false)
	appender := NewSharedAppender(table, txn, rt, false)
	defer appender.Close()

	node := createMockNode(schema, 100)
	defer node.data.Close()

	// PrepareAppend calls createAppendNode which should use freezelock
	appendNodes, err := appender.PrepareAppend(node)
	require.NoError(t, err)
	require.NotEmpty(t, appendNodes)

	aobj := appender.GetCurrentAobj()
	require.NotNil(t, aobj)

	// Verify: aobj is not frozen
	require.False(t, aobj.frozen.Load())

	// ApplyAppend
	err = appender.ApplyAppend()
	require.NoError(t, err)
}

// Test 5: Cross-aobj append (8500 rows spanning 2 aobjs)
// Verify PrepareAppend returns multiple AppendNodes
func TestSharedAppender_CrossAobjComplete(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	schema := table.GetLastestSchema(false)
	appender := NewSharedAppender(table, txn, rt, false)
	defer appender.Close()

	// Append 8500 rows (will span 2 aobjs)
	node := createMockNode(schema, 8500)
	defer node.data.Close()

	appendNodes, err := appender.PrepareAppend(node)
	require.NoError(t, err)
	require.Len(t, appendNodes, 2, "should create 2 AppendNodes for cross-aobj append")

	// Verify AddApplyInfo was called twice
	require.Len(t, node.appends, 2)

	// First append: [0, 8192)
	require.Equal(t, uint32(0), node.appends[0].srcOff)
	require.Equal(t, uint32(8192), node.appends[0].srcLen)
	require.Equal(t, uint32(0), node.appends[0].destOff)
	require.Equal(t, uint32(8192), node.appends[0].destLen)

	// Second append: [8192, 8500)
	require.Equal(t, uint32(8192), node.appends[1].srcOff)
	require.Equal(t, uint32(308), node.appends[1].srcLen)
	require.Equal(t, uint32(0), node.appends[1].destOff)
	require.Equal(t, uint32(308), node.appends[1].destLen)

	// Verify: two different ObjectEntries
	require.NotEqual(t, node.appends[0].dest, node.appends[1].dest)

	err = appender.ApplyAppend()
	require.NoError(t, err)

	// Verify: current aobj is the second one
	currentAobj := appender.GetCurrentAobj()
	require.NotNil(t, currentAobj)
}

// Test 6: Multiple PrepareAppend + ApplyAppend calls
func TestSharedAppender_MultiplePrepareApply(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	schema := table.GetLastestSchema(false)
	appender := NewSharedAppender(table, txn, rt, false)
	defer appender.Close()

	// First: 100 rows
	node1 := createMockNode(schema, 100)
	defer node1.data.Close()

	appendNodes1, err := appender.PrepareAppend(node1)
	require.NoError(t, err)
	require.Len(t, appendNodes1, 1)

	err = appender.ApplyAppend()
	require.NoError(t, err)

	firstAobj := appender.GetCurrentAobj()
	require.NotNil(t, firstAobj)

	// Second: 200 rows (same aobj)
	node2 := createMockNode(schema, 200)
	defer node2.data.Close()

	_, err = appender.PrepareAppend(node2)
	require.NoError(t, err)
	// May not create new AppendNode if in same range

	err = appender.ApplyAppend()
	require.NoError(t, err)

	// Verify: using same aobj
	require.Equal(t, firstAobj.meta.Load().ID(), appender.GetCurrentAobj().meta.Load().ID())

	// Third: 8000 rows (will span aobjs)
	node3 := createMockNode(schema, 8000)
	defer node3.data.Close()

	appendNodes3, err := appender.PrepareAppend(node3)
	require.NoError(t, err)
	require.NotEmpty(t, appendNodes3, "should create AppendNodes")

	err = appender.ApplyAppend()
	require.NoError(t, err)

	// Verify: created new aobj (exceeded first aobj capacity)
	require.NotEqual(t, firstAobj.meta.Load().ID(), appender.GetCurrentAobj().meta.Load().ID())
}
