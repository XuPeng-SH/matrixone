// Copyright 2026 Matrix Origin
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

package malloc

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestSimpleCAllocator() *SimpleCAllocator {
	return NewSimpleCAllocator(nil, nil, nil, nil, nil)
}

const simpleCAllocatorTestSize = 128 << 10

func TestSimpleCAllocatorAllocateAndDeallocate(t *testing.T) {
	for _, size := range []uint64{
		1,
		simpleCAllocatorTestSize - 1,
		simpleCAllocatorTestSize,
		simpleCAllocatorTestSize + 1,
	} {
		t.Run(testNameForSize(size), func(t *testing.T) {
			allocator := newTestSimpleCAllocator()

			slice, err := allocator.Allocate(size)
			require.NoError(t, err)
			require.Len(t, slice, int(size))
			require.Equal(t, int(size), cap(slice))
			require.Equal(t, make([]byte, size), slice)
			require.Equal(t, int64(size), allocator.currentInuse.Load())

			slice[0] = 1
			slice[len(slice)-1] = 2
			allocator.Deallocate(slice, size)
			require.Zero(t, allocator.currentInuse.Load())
		})
	}
}

func TestSimpleCAllocatorZeroSize(t *testing.T) {
	allocator := newTestSimpleCAllocator()

	slice, err := allocator.Malloc(0)
	require.NoError(t, err)
	require.Nil(t, slice)

	slice, err = allocator.Allocate(0)
	require.NoError(t, err)
	require.Nil(t, slice)
	require.Zero(t, allocator.currentInuse.Load())
}

func TestSimpleCAllocatorMallocAndDeallocate(t *testing.T) {
	for _, size := range []uint64{
		simpleCAllocatorTestSize - 1,
		simpleCAllocatorTestSize,
	} {
		t.Run(testNameForSize(size), func(t *testing.T) {
			allocator := newTestSimpleCAllocator()

			slice, err := allocator.Malloc(size)
			require.NoError(t, err)
			require.Len(t, slice, int(size))
			require.Equal(t, int(size), cap(slice))
			require.Equal(t, int64(size), allocator.currentInuse.Load())

			slice[0] = 1
			slice[len(slice)-1] = 2
			allocator.Deallocate(slice, size)
			require.Zero(t, allocator.currentInuse.Load())
		})
	}
}

func TestSimpleCAllocatorReallocZeroTransitions(t *testing.T) {
	testCases := []struct {
		name    string
		oldSize uint64
		newSize uint64
	}{
		{
			name:    "small-to-small",
			oldSize: simpleCAllocatorTestSize / 2,
			newSize: simpleCAllocatorTestSize - 1,
		},
		{
			name:    "small-to-large",
			oldSize: simpleCAllocatorTestSize - 1,
			newSize: simpleCAllocatorTestSize,
		},
		{
			name:    "large-to-large",
			oldSize: simpleCAllocatorTestSize,
			newSize: simpleCAllocatorTestSize * 2,
		},
		{
			name:    "large-to-small",
			oldSize: simpleCAllocatorTestSize,
			newSize: simpleCAllocatorTestSize / 2,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			allocator := newTestSimpleCAllocator()
			old, err := allocator.Allocate(testCase.oldSize)
			require.NoError(t, err)
			for i := range old {
				old[i] = byte(i%251 + 1)
			}

			resized, err := allocator.ReallocZero(
				old,
				testCase.oldSize,
				testCase.newSize,
			)
			require.NoError(t, err)
			require.Len(t, resized, int(testCase.newSize))
			require.Equal(t, int(testCase.newSize), cap(resized))
			require.Equal(t, int64(testCase.newSize), allocator.currentInuse.Load())

			preserved := min(testCase.oldSize, testCase.newSize)
			for i := uint64(0); i < preserved; i++ {
				require.Equal(t, byte(i%251+1), resized[i])
			}
			for i := testCase.oldSize; i < testCase.newSize; i++ {
				require.Zero(t, resized[i])
			}

			allocator.Deallocate(resized, testCase.newSize)
			require.Zero(t, allocator.currentInuse.Load())
		})
	}
}

func TestSimpleCAllocatorReallocZeroReducedCapacityTransitions(t *testing.T) {
	testCases := []struct {
		name          string
		oldSize       uint64
		logicalLength uint64
		newSize       uint64
	}{
		{
			name:          "small-to-large",
			oldSize:       simpleCAllocatorTestSize - 1,
			logicalLength: simpleCAllocatorTestSize / 2,
			newSize:       simpleCAllocatorTestSize,
		},
		{
			name:          "large-to-small",
			oldSize:       simpleCAllocatorTestSize,
			logicalLength: simpleCAllocatorTestSize / 4,
			newSize:       simpleCAllocatorTestSize / 2,
		},
		{
			name:          "large-to-large",
			oldSize:       simpleCAllocatorTestSize * 2,
			logicalLength: simpleCAllocatorTestSize / 2,
			newSize:       simpleCAllocatorTestSize * 3,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			allocator := newTestSimpleCAllocator()
			old, err := allocator.Allocate(testCase.oldSize)
			require.NoError(t, err)
			for i := range old {
				old[i] = 0x7f
			}

			// The three-index slice deliberately removes the allocator capacity
			// from the caller-visible view.
			reducedView := old[:testCase.logicalLength:testCase.logicalLength]
			resized, err := allocator.ReallocZero(
				reducedView,
				testCase.oldSize,
				testCase.newSize,
			)
			require.NoError(t, err)
			require.Equal(t, int64(testCase.newSize), allocator.currentInuse.Load())
			require.Equal(
				t,
				makeRepeatedByteSlice(int(testCase.logicalLength), 0x7f),
				resized[:testCase.logicalLength],
			)
			require.Equal(
				t,
				make([]byte, testCase.newSize-testCase.logicalLength),
				resized[testCase.logicalLength:],
			)

			allocator.Deallocate(resized, testCase.newSize)
			require.Zero(t, allocator.currentInuse.Load())
		})
	}
}

func TestSimpleCAllocatorReallocZeroFromAndToEmpty(t *testing.T) {
	allocator := newTestSimpleCAllocator()

	slice, err := allocator.ReallocZero(nil, 0, simpleCAllocatorTestSize)
	require.NoError(t, err)
	require.Len(t, slice, simpleCAllocatorTestSize)
	require.Equal(t, int64(simpleCAllocatorTestSize), allocator.currentInuse.Load())

	slice, err = allocator.ReallocZero(slice, simpleCAllocatorTestSize, 0)
	require.NoError(t, err)
	require.Nil(t, slice)
	require.Zero(t, allocator.currentInuse.Load())

	slice, err = allocator.ReallocZero(nil, 0, 0)
	require.NoError(t, err)
	require.Nil(t, slice)
	require.Zero(t, allocator.currentInuse.Load())
}

func TestSimpleCAllocatorDeallocateSizeMismatch(t *testing.T) {
	allocator := newTestSimpleCAllocator()

	require.Panics(t, func() {
		allocator.Deallocate(nil, 1)
	})

	slice, err := allocator.Allocate(simpleCAllocatorTestSize)
	require.NoError(t, err)
	require.Panics(t, func() {
		allocator.Deallocate(slice, simpleCAllocatorTestSize-1)
	})
	allocator.Deallocate(slice, simpleCAllocatorTestSize)
}

func TestSimpleCAllocatorConcurrentTransitions(t *testing.T) {
	allocator := newTestSimpleCAllocator()
	const goroutines = 8
	const iterations = 32

	var waitGroup sync.WaitGroup
	errs := make(chan error, goroutines)
	waitGroup.Add(goroutines)
	for goroutine := 0; goroutine < goroutines; goroutine++ {
		go func() {
			defer waitGroup.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				slice, err := allocator.Allocate(simpleCAllocatorTestSize - 1)
				if err != nil {
					errs <- err
					return
				}
				slice[0] = 1

				slice, err = allocator.ReallocZero(
					slice,
					simpleCAllocatorTestSize-1,
					simpleCAllocatorTestSize,
				)
				if err != nil {
					errs <- err
					return
				}
				if slice[0] != 1 {
					errs <- fmt.Errorf("data not preserved: got %d", slice[0])
					return
				}

				allocator.Deallocate(slice, simpleCAllocatorTestSize)
			}
		}()
	}
	waitGroup.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Zero(t, allocator.currentInuse.Load())
}

func testNameForSize(size uint64) string {
	switch size {
	case simpleCAllocatorTestSize - 1:
		return "below-test-size"
	case simpleCAllocatorTestSize:
		return "at-test-size"
	case simpleCAllocatorTestSize + 1:
		return "above-test-size"
	default:
		return "size-" + strconv.FormatUint(size, 10)
	}
}

func makeRepeatedByteSlice(size int, value byte) []byte {
	slice := make([]byte, size)
	for i := range slice {
		slice[i] = value
	}
	return slice
}
