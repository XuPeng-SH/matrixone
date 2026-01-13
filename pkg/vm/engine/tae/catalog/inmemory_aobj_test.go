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

package catalog

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetOrCreateInMemoryAobj tests the basic creation and retrieval of in-memory aobj
func TestGetOrCreateInMemoryAobj(t *testing.T) {
	catalog := MockCatalog(nil)
	defer catalog.Close()

	db, err := catalog.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)

	schema := MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 8192
	tbl, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)

	// Test 1: First call should create new aobj
	obj1, offset1 := tbl.GetOrCreateInMemoryAobj(false)
	assert.NotNil(t, obj1)
	assert.Equal(t, uint32(0), offset1)
	assert.True(t, obj1.IsInMemory())

	// Test 2: Second call should return the same aobj
	obj2, offset2 := tbl.GetOrCreateInMemoryAobj(false)
	assert.Equal(t, obj1.ID(), obj2.ID())
	assert.Equal(t, uint32(0), offset2)

	// Test 3: Tombstone should be separate
	tombObj, tombOffset := tbl.GetOrCreateInMemoryAobj(true)
	assert.NotNil(t, tombObj)
	assert.NotEqual(t, obj1.ID(), tombObj.ID())
	assert.Equal(t, uint32(0), tombOffset)
}

// TestAllocateRows tests atomic row allocation
func TestAllocateRows(t *testing.T) {
	catalog := MockCatalog(nil)
	defer catalog.Close()

	db, err := catalog.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)

	schema := MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 100 // Small for testing
	tbl, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)

	// Create initial aobj
	_, _ = tbl.GetOrCreateInMemoryAobj(false)

	// Test 1: Allocate 10 rows
	start1, count1, needSwitch1 := tbl.AllocateRows(false, 10)
	assert.Equal(t, uint32(0), start1)
	assert.Equal(t, uint32(10), count1)
	assert.False(t, needSwitch1)

	// Test 2: Allocate another 20 rows
	start2, count2, needSwitch2 := tbl.AllocateRows(false, 20)
	assert.Equal(t, uint32(10), start2)
	assert.Equal(t, uint32(20), count2)
	assert.False(t, needSwitch2)

	// Test 3: Allocate more than available (100 - 30 = 70 available)
	start3, count3, needSwitch3 := tbl.AllocateRows(false, 80)
	assert.Equal(t, uint32(30), start3)
	assert.Equal(t, uint32(70), count3) // Only 70 available
	assert.False(t, needSwitch3)

	// Test 4: Now it's full, should need switch
	start4, count4, needSwitch4 := tbl.AllocateRows(false, 10)
	assert.Equal(t, uint32(0), start4)
	assert.Equal(t, uint32(0), count4)
	assert.True(t, needSwitch4)
}

// TestAllocateRowsConcurrent tests concurrent row allocation
func TestAllocateRowsConcurrent(t *testing.T) {
	catalog := MockCatalog(nil)
	defer catalog.Close()

	db, err := catalog.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)

	schema := MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 1000
	tbl, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)

	// Create initial aobj
	_, _ = tbl.GetOrCreateInMemoryAobj(false)

	// Concurrent allocation
	const numGoroutines = 10
	const rowsPerGoroutine = 50
	var wg sync.WaitGroup
	results := make([]struct {
		start uint32
		count uint32
	}, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			start, count, needSwitch := tbl.AllocateRows(false, rowsPerGoroutine)
			assert.False(t, needSwitch)
			results[idx].start = start
			results[idx].count = count
		}(i)
	}

	wg.Wait()

	// Verify no overlapping ranges
	for i := 0; i < numGoroutines; i++ {
		for j := i + 1; j < numGoroutines; j++ {
			r1 := results[i]
			r2 := results[j]
			// Check no overlap: [r1.start, r1.start+r1.count) and [r2.start, r2.start+r2.count)
			assert.True(t,
				r1.start+r1.count <= r2.start || r2.start+r2.count <= r1.start,
				"Overlapping ranges: [%d, %d) and [%d, %d)",
				r1.start, r1.start+r1.count, r2.start, r2.start+r2.count)
		}
	}

	// Verify total allocated
	totalAllocated := uint32(0)
	for i := 0; i < numGoroutines; i++ {
		totalAllocated += results[i].count
	}
	assert.Equal(t, uint32(numGoroutines*rowsPerGoroutine), totalAllocated)
}

// TestFreezeCurrentInMemoryAobj tests freezing aobj
func TestFreezeCurrentInMemoryAobj(t *testing.T) {
	catalog := MockCatalog(nil)
	defer catalog.Close()

	db, err := catalog.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)

	schema := MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 8192
	tbl, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)

	// Create initial aobj
	obj1, _ := tbl.GetOrCreateInMemoryAobj(false)

	// Allocate some rows
	start1, count1, _ := tbl.AllocateRows(false, 100)
	assert.Equal(t, uint32(0), start1)
	assert.Equal(t, uint32(100), count1)

	// Freeze current aobj
	tbl.FreezeCurrentInMemoryAobj(false)

	// After freeze, allocation should need switch
	_, _, needSwitch := tbl.AllocateRows(false, 10)
	assert.True(t, needSwitch)

	// GetOrCreate should create new aobj
	obj2, offset2 := tbl.GetOrCreateInMemoryAobj(false)
	assert.NotEqual(t, obj1.ID(), obj2.ID())
	assert.Equal(t, uint32(0), offset2)

	// New aobj should be allocatable
	start2, count2, needSwitch2 := tbl.AllocateRows(false, 10)
	assert.Equal(t, uint32(0), start2)
	assert.Equal(t, uint32(10), count2)
	assert.False(t, needSwitch2)
}

// TestSeparateDataAndTombstone tests data and tombstone aobjs are separate
func TestSeparateDataAndTombstone(t *testing.T) {
	catalog := MockCatalog(nil)
	defer catalog.Close()

	db, err := catalog.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)

	schema := MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 8192
	tbl, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)

	// Create data aobj
	dataObj, _ := tbl.GetOrCreateInMemoryAobj(false)
	dataStart, dataCount, _ := tbl.AllocateRows(false, 100)

	// Create tombstone aobj
	tombObj, _ := tbl.GetOrCreateInMemoryAobj(true)
	tombStart, tombCount, _ := tbl.AllocateRows(true, 50)

	// They should be different objects
	assert.NotEqual(t, dataObj.ID(), tombObj.ID())

	// Allocations should be independent
	assert.Equal(t, uint32(0), dataStart)
	assert.Equal(t, uint32(100), dataCount)
	assert.Equal(t, uint32(0), tombStart)
	assert.Equal(t, uint32(50), tombCount)

	// Freeze data should not affect tombstone
	tbl.FreezeCurrentInMemoryAobj(false)

	// Data allocation should need switch
	_, _, needSwitchData := tbl.AllocateRows(false, 10)
	assert.True(t, needSwitchData)

	// Tombstone allocation should still work
	tombStart2, tombCount2, needSwitchTomb := tbl.AllocateRows(true, 10)
	assert.Equal(t, uint32(50), tombStart2)
	assert.Equal(t, uint32(10), tombCount2)
	assert.False(t, needSwitchTomb)
}
