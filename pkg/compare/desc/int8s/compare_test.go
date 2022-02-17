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

package int8s

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNew(t *testing.T) {
	require.Equal(t, &compare{xs: make([][]int8, 2),
		ns: make([]*nulls.Nulls, 2),
		vs: make([]*vector.Vector, 2)}, New())
}

func TestCompare_Vector(t *testing.T) {
	c := New()
	c.vs[0] = vector.New(types.Type{Oid: types.T(types.T_int8)})
	require.Equal(t, vector.New(types.Type{Oid: types.T(types.T_int8)}), c.Vector())
}

func TestCompare_Set(t *testing.T) {
	c := New()
	vector := vector.New(types.Type{Oid: types.T(types.T_int8)})
	c.Set(1, vector)
	require.Equal(t, vector, c.vs[1])
}

func TestCompare_Compare(t *testing.T) {
	c := New()
	c.xs[0] = []int8{5, 6}
	c.xs[1] = []int8{7, 8}
	result := c.Compare(0, 1, 0, 0)
	require.Equal(t, 1, result)
	c.xs[1] = []int8{5, 6}
	result = c.Compare(0, 1, 0, 0)
	require.Equal(t, 0, result)
	c.xs[1] = []int8{3, 4}
	result = c.Compare(0, 1, 0, 0)
	require.Equal(t, -1, result)
}
