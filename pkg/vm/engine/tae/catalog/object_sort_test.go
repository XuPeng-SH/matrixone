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
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/assert"
)

// Helper to create test ObjectEntry
// isInMemory: appendable=true, no extent (simulates in-memory aobj)
// isCommitted: appendable=false, has extent (simulates committed nobj)
// isUncommitted: appendable=false, no extent, IsLocal=true (simulates uncommitted nobj)
func makeTestObject(createdAt, deletedAt int64, isInMemory, isCommitted, isUncommitted bool) *ObjectEntry {
	var objectID objectio.ObjectId
	if isInMemory {
		// Use UUID v7 for in-memory
		id := uuid.Must(uuid.NewV7())
		copy(objectID[:], id[:])
	} else {
		objectID = objectio.NewObjectid()
	}

	appendable := isInMemory
	stats := objectio.NewObjectStatsWithObjectID(&objectID, appendable, false, false)

	// Committed objects have extent
	if isCommitted {
		extent := objectio.NewRandomExtent()
		objectio.SetObjectStatsExtent(stats, extent)
	}

	entry := &ObjectEntry{
		ObjectNode: ObjectNode{
			IsLocal: isUncommitted,
		},
		EntryMVCCNode: EntryMVCCNode{
			CreatedAt: types.BuildTS(createdAt, 0),
			DeletedAt: types.BuildTS(deletedAt, 0),
		},
		ObjectMVCCNode: ObjectMVCCNode{
			ObjectStats: *stats,
		},
	}
	return entry
}

func TestLess2Sorting(t *testing.T) {
	// Create test objects
	committed1 := makeTestObject(100, 0, false, true, false)   // committed nobj, CreatedAt=100
	committed2 := makeTestObject(200, 250, false, true, false) // committed nobj, DeletedAt=250
	inMemory1 := makeTestObject(150, 0, true, false, false)    // in-memory aobj, CreatedAt=150
	inMemory2 := makeTestObject(180, 0, true, false, false)    // in-memory aobj, CreatedAt=180
	uncommitted1 := makeTestObject(300, 0, false, false, true) // uncommitted nobj, CreatedAt=300
	uncommitted2 := makeTestObject(350, 0, false, false, true) // uncommitted nobj, CreatedAt=350

	// Verify IsInMemory works
	assert.True(t, inMemory1.IsInMemory(), "inMemory1 should be in-memory")
	assert.True(t, inMemory2.IsInMemory(), "inMemory2 should be in-memory")
	assert.False(t, committed1.IsInMemory(), "committed1 should not be in-memory")
	assert.False(t, uncommitted1.IsInMemory(), "uncommitted1 should not be in-memory")

	objects := []*ObjectEntry{
		uncommitted2, committed2, inMemory2, uncommitted1, committed1, inMemory1,
	}

	// Sort using Less2
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Less2(objects[j])
	})

	// Expected order:
	// 1. committed1 (100)
	// 2. committed2 (250, by DeletedAt)
	// 3. inMemory1 (150)
	// 4. inMemory2 (180)
	// 5. uncommitted1 (300)
	// 6. uncommitted2 (350)

	assert.Equal(t, committed1, objects[0], "committed1 should be first")
	assert.Equal(t, committed2, objects[1], "committed2 should be second")
	assert.Equal(t, inMemory1, objects[2], "inMemory1 should be third")
	assert.Equal(t, inMemory2, objects[3], "inMemory2 should be fourth")
	assert.Equal(t, uncommitted1, objects[4], "uncommitted1 should be fifth")
	assert.Equal(t, uncommitted2, objects[5], "uncommitted2 should be sixth")
}

func TestEarlyBreakScenario(t *testing.T) {
	// Scenario: Query range [from=200, to=300]
	// Early break should happen when we hit an in-memory aobj with CreatedAt < from

	from := types.BuildTS(200, 0)

	// Create objects (no uncommitted, only committed + in-memory)
	inMemory1 := makeTestObject(250, 0, true, false, false)  // in-memory aobj, in range
	inMemory2 := makeTestObject(180, 0, true, false, false)  // in-memory aobj, CreatedAt < from, EARLY BREAK
	committed1 := makeTestObject(220, 0, false, true, false) // committed nobj
	committed2 := makeTestObject(100, 0, false, true, false) // committed nobj

	objects := []*ObjectEntry{
		inMemory1, inMemory2, committed1, committed2,
	}

	// Sort using Less2
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Less2(objects[j])
	})

	t.Logf("Sorted order:")
	for i, obj := range objects {
		t.Logf("  [%d] CreatedAt=%d, IsAppendable=%v, IsInMemory=%v",
			i, obj.CreatedAt.Physical(), obj.IsAppendable(), obj.IsInMemory())
	}

	// Simulate iteration (Last -> Prev, newest to oldest)
	var visited []*ObjectEntry
	earlyBreak := false

	for i := len(objects) - 1; i >= 0; i-- {
		obj := objects[i]

		t.Logf("Visiting [%d]: CreatedAt=%d, IsAppendable=%v",
			i, obj.CreatedAt.Physical(), obj.IsAppendable())

		// Early break logic: if appendable && CreatedAt < from
		if obj.IsAppendable() && obj.CreatedAt.LT(&from) {
			t.Logf("  -> Early break!")
			earlyBreak = true
			break
		}

		visited = append(visited, obj)
	}

	assert.True(t, earlyBreak, "Should trigger early break")
	t.Logf("Visited %d objects", len(visited))

	// After sorting: committed2, committed1, inMemory2, inMemory1
	// Iterate backwards: inMemory1 (250), inMemory2 (180 < from, break)
	assert.Equal(t, 1, len(visited), "Should visit 1 object before early break")
	assert.Equal(t, inMemory1, visited[0])
}

func TestEarlyBreakWithCommittedAobj(t *testing.T) {
	// Test early break with committed appendable objects (persisted aobj)
	from := types.BuildTS(200, 0)

	// Create committed appendable objects (simulating flushed aobj)
	committedAobj1 := makeTestObject(250, 0, true, false, false)
	committedAobj1.ObjectNode.forcePNode = true // Mark as persisted

	committedAobj2 := makeTestObject(180, 0, true, false, false)
	committedAobj2.ObjectNode.forcePNode = true // Mark as persisted, CreatedAt < from

	inMemory1 := makeTestObject(220, 0, true, false, false)  // in-memory aobj
	committed1 := makeTestObject(150, 0, false, true, false) // committed nobj

	objects := []*ObjectEntry{
		committedAobj1, committedAobj2, inMemory1, committed1,
	}

	// Sort using Less2
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Less2(objects[j])
	})

	t.Logf("Sorted order:")
	for i, obj := range objects {
		t.Logf("  [%d] CreatedAt=%d, IsAppendable=%v, forcePNode=%v",
			i, obj.CreatedAt.Physical(), obj.IsAppendable(), obj.ObjectNode.forcePNode)
	}

	// Verify sorting: committed nobj, then committed aobj (by CreatedAt), then in-memory aobj
	assert.Equal(t, committed1, objects[0], "committed nobj first")
	assert.Equal(t, committedAobj2, objects[1], "committed aobj2 (180)")
	assert.Equal(t, inMemory1, objects[2], "in-memory aobj (220)")
	assert.Equal(t, committedAobj1, objects[3], "committed aobj1 (250)")

	// Simulate early break
	var visited []*ObjectEntry
	earlyBreak := false

	for i := len(objects) - 1; i >= 0; i-- {
		obj := objects[i]

		if obj.IsAppendable() && obj.CreatedAt.LT(&from) {
			earlyBreak = true
			break
		}

		visited = append(visited, obj)
	}

	assert.True(t, earlyBreak, "Should trigger early break")
	// Should visit: committedAobj1 (250), inMemory1 (220), then committedAobj2 (180 < from, break)
	assert.Equal(t, 2, len(visited), "Should visit 2 objects")
}

func TestEarlyBreakMonotonicCreatedAt(t *testing.T) {
	// Critical test: Verify appendable objects are monotonic by CreatedAt
	// This is required for early break correctness

	from := types.BuildTS(200, 0)

	// Mix of committed and in-memory appendable objects
	inMemory1 := makeTestObject(300, 0, true, false, false)
	inMemory2 := makeTestObject(250, 0, true, false, false)
	inMemory3 := makeTestObject(150, 0, true, false, false) // < from

	committedAobj1 := makeTestObject(280, 0, true, false, false)
	committedAobj1.ObjectNode.forcePNode = true

	committedAobj2 := makeTestObject(220, 0, true, false, false)
	committedAobj2.ObjectNode.forcePNode = true

	committedAobj3 := makeTestObject(180, 0, true, false, false) // < from
	committedAobj3.ObjectNode.forcePNode = true

	objects := []*ObjectEntry{
		inMemory1, inMemory2, inMemory3, committedAobj1, committedAobj2, committedAobj3,
	}

	// Sort using Less2
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Less2(objects[j])
	})

	// Verify all appendable objects are sorted by CreatedAt (monotonic)
	var appendableObjects []*ObjectEntry
	for _, obj := range objects {
		if obj.IsAppendable() {
			appendableObjects = append(appendableObjects, obj)
		}
	}

	// Check monotonic CreatedAt
	for i := 1; i < len(appendableObjects); i++ {
		prev := appendableObjects[i-1]
		curr := appendableObjects[i]
		assert.True(t, prev.CreatedAt.LE(&curr.CreatedAt),
			"Appendable objects must be monotonic by CreatedAt: prev=%d, curr=%d",
			prev.CreatedAt.Physical(), curr.CreatedAt.Physical())
	}

	// Verify early break works correctly
	var visited []*ObjectEntry
	earlyBreak := false

	for i := len(objects) - 1; i >= 0; i-- {
		obj := objects[i]

		if obj.IsAppendable() && obj.CreatedAt.LT(&from) {
			earlyBreak = true
			break
		}

		visited = append(visited, obj)
	}

	assert.True(t, earlyBreak, "Should trigger early break")

	// Verify we didn't visit any appendable object with CreatedAt < from
	for _, obj := range visited {
		if obj.IsAppendable() {
			assert.True(t, obj.CreatedAt.GE(&from),
				"Should not visit appendable object with CreatedAt < from")
		}
	}
}

func TestEarlyBreakAllCombinations(t *testing.T) {
	// Comprehensive test: all object types mixed together
	from := types.BuildTS(200, 0)

	// Create all types of objects
	// 1. Committed non-appendable (nobj)
	committedNobj1 := makeTestObject(100, 150, false, true, false) // DeletedAt=150 < from
	committedNobj2 := makeTestObject(180, 250, false, true, false) // DeletedAt=250, in range
	committedNobj3 := makeTestObject(220, 0, false, true, false)   // CreatedAt=220, in range

	// 2. Committed appendable (persisted aobj)
	committedAobj1 := makeTestObject(170, 0, true, false, false) // < from
	committedAobj1.ObjectNode.forcePNode = true

	committedAobj2 := makeTestObject(210, 0, true, false, false) // in range
	committedAobj2.ObjectNode.forcePNode = true

	committedAobj3 := makeTestObject(280, 0, true, false, false) // in range
	committedAobj3.ObjectNode.forcePNode = true

	// 3. In-memory appendable (in-memory aobj)
	inMemory1 := makeTestObject(190, 0, true, false, false) // < from
	inMemory2 := makeTestObject(230, 0, true, false, false) // in range
	inMemory3 := makeTestObject(270, 0, true, false, false) // in range

	// 4. Uncommitted non-appendable
	uncommitted1 := makeTestObject(350, 0, false, false, true)
	uncommitted2 := makeTestObject(400, 0, false, false, true)

	objects := []*ObjectEntry{
		committedNobj1, committedNobj2, committedNobj3,
		committedAobj1, committedAobj2, committedAobj3,
		inMemory1, inMemory2, inMemory3,
		uncommitted1, uncommitted2,
	}

	// Sort using Less2
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Less2(objects[j])
	})

	t.Logf("Sorted order:")
	for i, obj := range objects {
		t.Logf("  [%d] CreatedAt=%d, DeletedAt=%d, IsAppendable=%v, IsLocal=%v, IsInMemory=%v",
			i, obj.CreatedAt.Physical(), obj.DeletedAt.Physical(),
			obj.IsAppendable(), obj.IsLocal, obj.IsInMemory())
	}

	// Verify sorting order:
	// 1. Non-appendable committed (by max(CreatedAt, DeletedAt))
	// 2. All appendable (by CreatedAt, monotonic)
	// 3. Uncommitted non-appendable (by CreatedAt)

	// Find boundaries
	var nonAppendableCommitted, appendable, uncommitted []*ObjectEntry
	for _, obj := range objects {
		if obj.IsLocal && !obj.IsInMemory() {
			uncommitted = append(uncommitted, obj)
		} else if obj.IsAppendable() {
			appendable = append(appendable, obj)
		} else {
			nonAppendableCommitted = append(nonAppendableCommitted, obj)
		}
	}

	t.Logf("Non-appendable committed: %d, Appendable: %d, Uncommitted: %d",
		len(nonAppendableCommitted), len(appendable), len(uncommitted))

	// Verify appendable objects are monotonic by CreatedAt
	for i := 1; i < len(appendable); i++ {
		prev := appendable[i-1]
		curr := appendable[i]
		assert.True(t, prev.CreatedAt.LE(&curr.CreatedAt),
			"Appendable objects must be monotonic: prev=%d, curr=%d",
			prev.CreatedAt.Physical(), curr.CreatedAt.Physical())
	}

	// Simulate early break
	var visited []*ObjectEntry
	earlyBreak := false

	for i := len(objects) - 1; i >= 0; i-- {
		obj := objects[i]

		// Early break logic: if appendable && CreatedAt < from
		if obj.IsAppendable() && obj.CreatedAt.LT(&from) {
			t.Logf("Early break at object: CreatedAt=%d", obj.CreatedAt.Physical())
			earlyBreak = true
			break
		}

		visited = append(visited, obj)
	}

	assert.True(t, earlyBreak, "Should trigger early break")

	// Verify correctness: no appendable object with CreatedAt < from should be visited
	for _, obj := range visited {
		if obj.IsAppendable() {
			assert.True(t, obj.CreatedAt.GE(&from),
				"Visited appendable object with CreatedAt=%d < from=%d",
				obj.CreatedAt.Physical(), from.Physical())
		}
	}

	// Verify we visited all appendable objects with CreatedAt >= from
	visitedAppendable := 0
	for _, obj := range visited {
		if obj.IsAppendable() {
			visitedAppendable++
		}
	}

	expectedAppendable := 0
	for _, obj := range appendable {
		if obj.CreatedAt.GE(&from) {
			expectedAppendable++
		}
	}

	assert.Equal(t, expectedAppendable, visitedAppendable,
		"Should visit all appendable objects with CreatedAt >= from")
}

func TestEarlyBreakEdgeCases(t *testing.T) {
	// Test edge cases

	t.Run("No appendable objects", func(t *testing.T) {
		from := types.BuildTS(200, 0)

		committed1 := makeTestObject(100, 0, false, true, false)
		committed2 := makeTestObject(250, 0, false, true, false)
		uncommitted := makeTestObject(300, 0, false, false, true)

		objects := []*ObjectEntry{committed1, committed2, uncommitted}
		sort.Slice(objects, func(i, j int) bool {
			return objects[i].Less2(objects[j])
		})

		earlyBreak := false
		for i := len(objects) - 1; i >= 0; i-- {
			obj := objects[i]
			if obj.IsAppendable() && obj.CreatedAt.LT(&from) {
				earlyBreak = true
				break
			}
		}

		assert.False(t, earlyBreak, "Should not trigger early break without appendable objects")
	})

	t.Run("All appendable objects above from", func(t *testing.T) {
		from := types.BuildTS(100, 0)

		inMemory1 := makeTestObject(200, 0, true, false, false)
		inMemory2 := makeTestObject(300, 0, true, false, false)

		objects := []*ObjectEntry{inMemory1, inMemory2}
		sort.Slice(objects, func(i, j int) bool {
			return objects[i].Less2(objects[j])
		})

		earlyBreak := false
		visitedCount := 0
		for i := len(objects) - 1; i >= 0; i-- {
			obj := objects[i]
			if obj.IsAppendable() && obj.CreatedAt.LT(&from) {
				earlyBreak = true
				break
			}
			visitedCount++
		}

		assert.False(t, earlyBreak, "Should not trigger early break")
		assert.Equal(t, 2, visitedCount, "Should visit all objects")
	})

	t.Run("First appendable object triggers early break", func(t *testing.T) {
		from := types.BuildTS(200, 0)

		inMemory1 := makeTestObject(150, 0, true, false, false) // < from
		committed1 := makeTestObject(250, 0, false, true, false)

		objects := []*ObjectEntry{inMemory1, committed1}
		sort.Slice(objects, func(i, j int) bool {
			return objects[i].Less2(objects[j])
		})

		// After sorting: committed1(250, non-appendable), inMemory1(150, appendable)
		// Iterate backwards: inMemory1 (break immediately)

		earlyBreak := false
		visitedCount := 0
		for i := len(objects) - 1; i >= 0; i-- {
			obj := objects[i]
			if obj.IsAppendable() && obj.CreatedAt.LT(&from) {
				earlyBreak = true
				break
			}
			visitedCount++
		}

		assert.True(t, earlyBreak, "Should trigger early break")
		assert.Equal(t, 0, visitedCount, "Should break immediately on first appendable object")
	})
}
