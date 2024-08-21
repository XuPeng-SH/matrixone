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

package bitmap

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

const (
	Rows          = 10
	BenchmarkRows = 8192
)

func newBm(n int) *Bitmap {
	var bm Bitmap
	bm.InitWithSize(int64(n))
	return &bm
}

func TestNulls(t *testing.T) {
	np := newBm(Rows)
	np.AddRange(0, 0)
	np.AddRange(1, 10)
	require.Equal(t, 9, np.Count())
	np.Reset()

	ok := np.IsEmpty()
	require.Equal(t, true, ok)
	np.TryExpandWithSize(10)
	np.Add(0)
	ok = np.Contains(0)
	require.Equal(t, true, ok)
	require.Equal(t, 1, np.Count())

	itr := np.Iterator()
	require.Equal(t, uint64(0), itr.PeekNext())
	require.Equal(t, true, itr.HasNext())
	require.Equal(t, uint64(0), itr.Next())

	np.Remove(0)
	ok = np.IsEmpty()
	require.Equal(t, true, ok)

	np.AddMany([]uint64{1, 2, 3})
	require.Equal(t, 3, np.Count())
	np.RemoveRange(1, 3)
	require.Equal(t, 0, np.Count())

	np.AddMany([]uint64{1, 2, 3})
	np.Filter([]int64{0})
	fmt.Printf("%v\n", np.String())
	fmt.Printf("size: %v\n", np.Size())
	fmt.Printf("numbers: %v\n", np.Count())

	nq := newBm(Rows)
	nq.Unmarshal(np.Marshal())

	require.Equal(t, np.ToArray(), nq.ToArray())

	np.Reset()
}

func BenchmarkAdd(b *testing.B) {
	np := newBm(BenchmarkRows)
	for i := 0; i < b.N; i++ {
		for j := 0; j < BenchmarkRows; j++ {
			np.Add(uint64(j))
		}
		for j := 0; j < BenchmarkRows; j++ {
			np.Contains(uint64(j))
		}
		for j := 0; j < BenchmarkRows; j++ {
			np.Remove(uint64(j))
		}
	}
}

func TestC(t *testing.T) {
	bm3 := newBm(10000)
	bm5 := newBm(10000)

	require.True(t, bm3.IsEmpty())
	require.True(t, bm3.C_IsEmpty())
	require.True(t, bm3.Count() == 0)
	require.True(t, bm3.C_Count() == 0)

	for i := 0; i < 10000; i += 3 {
		bm3.Add(uint64(i))
	}
	for i := 0; i < 10000; i += 5 {
		bm3.Add(uint64(i))
	}

	require.False(t, bm3.IsEmpty())
	require.False(t, bm3.C_IsEmpty())

	bm3Copy := bm3.Clone()
	require.True(t, bm3.IsSame(bm3Copy))
	require.True(t, bm3.C_Count() == bm3Copy.C_Count())
	bm5Copy := bm5.Clone()
	require.True(t, bm5.IsSame(bm5Copy))
	require.True(t, bm5.C_Count() == bm5Copy.C_Count())

	bm3CopyAgain := bm3.Clone()

	bm3.Or(bm5)
	bm3Copy.C_Or(bm5)
	require.True(t, bm3.IsSame(bm3Copy))
	require.True(t, bm3.Count() == int(bm3Copy.C_Count()))

	bm5.And(bm3CopyAgain)
	bm5Copy.C_And(bm3CopyAgain)
	require.True(t, bm5.IsSame(bm5Copy))
	require.True(t, bm5.Count() == int(bm5Copy.C_Count()))
}

func TestBitmapIterator_Next(t *testing.T) {
	np := newBm(BenchmarkRows)
	np.AddRange(0, 64)

	// | 63 -- 0 | 127 -- 64 | 191 -- 128 | 255 -- 192 | 319 -- 256 | 383 -- 320 | ... |
	rows := []uint64{127, 192, 320} // add some boundary case to check if loops over word
	np.AddMany(rows)

	itr := np.Iterator()
	i := uint64(0)
	for itr.HasNext() {
		r := itr.Next()
		if r < uint64(64) {
			require.Equal(t, i, r)
			i++
		} else {
			t.Logf("r now is %d\n", r)
		}
	}
}

func TestFixedSizeBitmap(t *testing.T) {
	var bm FixedSizeBitmap
	require.True(t, bm.IsEmpty())
	require.Equal(t, 0, bm.Count())

	bm.Add(1022)
	require.False(t, bm.IsEmpty())
	require.Equal(t, 1, bm.Count())

	bm.Reset()
	require.True(t, bm.IsEmpty())
	require.Equal(t, 0, bm.Count())

	bm.Add(0)
	require.False(t, bm.IsEmpty())
	require.Equal(t, 1, bm.Count())
	bm.Remove(0)
	require.True(t, bm.IsEmpty())
	require.Equal(t, 0, bm.Count())

	for i := 0; i < FixedSizeBitmapBits; i++ {
		bm.Add(uint64(i))
		require.Equal(t, i+1, bm.Count())
	}

	for i := 0; i < FixedSizeBitmapBits; i++ {
		bm.Remove(uint64(i))
		require.Equal(t, FixedSizeBitmapBits-i-1, bm.Count())
	}
	require.True(t, bm.IsEmpty())

	var expects []uint64
	step := rand.Intn(10) + 1
	for i := 0; i < FixedSizeBitmapBits; i += step {
		bm.Add(uint64(i))
		expects = append(expects, uint64(i))
	}

	require.Equal(t, len(expects), bm.Count())
	actualUints := bm.ToArray()
	require.Equal(t, expects, actualUints)
	actualInts := bm.ToI64Arrary()
	for i, v := range actualInts {
		require.Equal(t, int64(expects[i]), v)
	}
}
