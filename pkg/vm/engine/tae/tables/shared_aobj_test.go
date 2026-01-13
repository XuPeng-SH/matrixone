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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
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

	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)

	table, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

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

	t.Logf("✅ GetMinCommitTS: %v", minCommitTS)
	t.Logf("✅ Expected min(commitTS1, commitTS2): %v", expectedMinTS)
}
