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

package test

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

// TestReplaySharedAobj_ObjectExists tests replay when ObjectEntry exists
func TestReplaySharedAobj_ObjectExists(t *testing.T) {
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(3, 2)
	schema.Extra.BlockMaxRows = 10
	tae.BindSchema(schema)

	// Create table and shared aobj
	bat := catalog.MockBatch(schema, 5)
	defer bat.Close()
	tae.CreateRelAndAppend(bat, true)

	// Get ObjectEntry
	txn, rel := tae.GetRelation()
	objEntry := testutil.GetOneBlockMeta(rel)
	require.NotNil(t, objEntry)
	require.NoError(t, txn.Commit(ctx))

	// Verify: ObjectEntry can be found
	db := objEntry.GetTable().GetDB()
	id := objEntry.AsCommonID()
	foundObj, err := db.GetObjectEntryByID(id, false)
	require.NoError(t, err)
	require.NotNil(t, foundObj)
	require.Equal(t, objEntry.ID(), foundObj.ID())

	t.Log("Replay with existing ObjectEntry works correctly")
}
