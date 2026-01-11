// Copyright 2024 Matrix Origin
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

package updates

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAppendNode_PrepareRollback_KeepsNode verifies node is kept on rollback
func TestAppendNode_PrepareRollback_KeepsNode(t *testing.T) {
	db := catalog.MockDBEntryWithAccInfo(0, 1)
	tbl := catalog.MockTableEntryWithDB(db, 2)
	stats := objectio.NewObjectStats()
	obj := catalog.MockObjectEntry(tbl, stats, false, nil, types.BuildTS(100, 0))
	mvcc := NewAppendMVCCHandle(obj)
	
	txn := MockTxnWithStartTS(types.BuildTS(100, 0))
	node, created := mvcc.AddAppendNodeLocked(txn, 0, 10)
	assert.True(t, created)
	assert.NotNil(t, node)
	assert.Equal(t, uint32(0), node.GetStartRow())
	assert.Equal(t, uint32(10), node.GetMaxRow())
	
	assert.False(t, mvcc.appends.IsEmpty())
	
	err := node.PrepareRollback()
	assert.NoError(t, err)
	
	// Node should still be in MVCC chain (not deleted)
	assert.False(t, mvcc.appends.IsEmpty())
	
	// Node should be marked as aborted
	assert.True(t, node.IsAborted())
	
	// Should be able to find the node by row
	foundNode := mvcc.GetAppendNodeByRowLocked(5)
	assert.NotNil(t, foundNode)
	assert.Equal(t, node, foundNode)
	assert.True(t, foundNode.IsAborted())
}

// TestFillInCommitTS_FillsZeroForAborted verifies FillInCommitTSVecLocked fills zero TS for aborted nodes
func TestFillInCommitTS_FillsZeroForAborted(t *testing.T) {
	db := catalog.MockDBEntryWithAccInfo(0, 1)
	tbl := catalog.MockTableEntryWithDB(db, 2)
	stats := objectio.NewObjectStats()
	obj := catalog.MockObjectEntry(tbl, stats, false, nil, types.BuildTS(100, 0))
	mvcc := NewAppendMVCCHandle(obj)
	
	// Create 3 AppendNodes
	txn1 := MockTxnWithStartTS(types.BuildTS(100, 0))
	node1, _ := mvcc.AddAppendNodeLocked(txn1, 0, 5)
	
	txn2 := MockTxnWithStartTS(types.BuildTS(200, 0))
	node2, _ := mvcc.AddAppendNodeLocked(txn2, 5, 10)
	
	txn3 := MockTxnWithStartTS(types.BuildTS(300, 0))
	node3, _ := mvcc.AddAppendNodeLocked(txn3, 10, 15)
	
	// Commit node1 and node3
	err := node1.ApplyCommit("txn1")
	require.NoError(t, err)
	err = node3.ApplyCommit("txn3")
	require.NoError(t, err)
	
	// Rollback node2
	err = node2.PrepareRollback()
	require.NoError(t, err)
	err = node2.ApplyRollback()
	require.NoError(t, err)
	
	assert.True(t, node2.IsAborted())
	
	// Fill commit timestamps
	commitTSVec := containers.MakeVector(types.T_TS.ToType(), nil)
	defer commitTSVec.Close()
	
	mvcc.Lock()
	mvcc.FillInCommitTSVecLocked(commitTSVec, 15, nil)
	mvcc.Unlock()
	
	// Should have all 15 rows (node1: 5 + node2: 5 + node3: 5)
	assert.Equal(t, 15, commitTSVec.Length())
	
	// Verify commit timestamps
	tss := vector.MustFixedColWithTypeCheck[types.TS](commitTSVec.GetDownstreamVector())
	// First 5 rows should be node1's commitTS
	for i := 0; i < 5; i++ {
		assert.Equal(t, node1.GetCommitTS(), tss[i])
	}
	// Middle 5 rows should be MaxTS (aborted)
	maxTS := types.MaxTs()
	for i := 5; i < 10; i++ {
		assert.Equal(t, maxTS, tss[i], "Row %d should have MaxTS", i)
	}
	// Last 5 rows should be node3's commitTS
	for i := 10; i < 15; i++ {
		assert.Equal(t, node3.GetCommitTS(), tss[i])
	}
}

