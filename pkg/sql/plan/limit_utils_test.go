// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"math"
	"slices"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestBuildCandidateLimit(t *testing.T) {
	tests := []struct {
		name       string
		limit      *planpb.Expr
		offset     *planpb.Expr
		want       uint64
		wantUsable bool
	}{
		{name: "limit only", limit: makePlan2Uint64ConstExprWithType(10), want: 10, wantUsable: true},
		{name: "limit plus offset", limit: makePlan2Uint64ConstExprWithType(10), offset: makePlan2Uint64ConstExprWithType(5), want: 15, wantUsable: true},
		{name: "zero offset", limit: makePlan2Uint64ConstExprWithType(10), offset: makePlan2Uint64ConstExprWithType(0), want: 10, wantUsable: true},
		{name: "overflow", limit: makePlan2Uint64ConstExprWithType(math.MaxUint64), offset: makePlan2Uint64ConstExprWithType(1)},
		{name: "dynamic offset", limit: makePlan2Uint64ConstExprWithType(10), offset: &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}},
		{name: "dynamic limit without offset", limit: &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}, wantUsable: true},
		{name: "null literal offset", limit: makePlan2Uint64ConstExprWithType(10), offset: &planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true, Value: &planpb.Literal_U64Val{U64Val: 5}}}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, usable := buildCandidateLimit(tc.limit, tc.offset)
			require.Equal(t, tc.wantUsable, usable)
			if !usable {
				require.Nil(t, got)
				return
			}
			if value, literal := getLiteralUint64(got); literal {
				require.Equal(t, tc.want, value)
			} else {
				require.NotSame(t, tc.limit, got)
				require.NotNil(t, got.GetP())
			}
		})
	}
}

func TestComposePaginationMatchesSequentialWindows(t *testing.T) {
	type window struct {
		limit  *uint64
		offset *uint64
	}
	value := func(v uint64) *uint64 { return &v }
	limitValues := []*uint64{nil, value(0), value(1), value(3), value(8)}
	offsetValues := []*uint64{nil, value(0), value(1), value(2), value(9)}
	windows := make([]window, 0, len(limitValues)*len(offsetValues))
	for _, limit := range limitValues {
		for _, offset := range offsetValues {
			windows = append(windows, window{limit: limit, offset: offset})
		}
	}
	input := []int{0, 1, 2, 3, 4, 5, 6, 7}
	apply := func(rows []int, pagination window) []int {
		start := uint64(0)
		if pagination.offset != nil {
			start = *pagination.offset
		}
		if start >= uint64(len(rows)) {
			return nil
		}
		end := uint64(len(rows))
		if pagination.limit != nil && *pagination.limit < end-start {
			end = start + *pagination.limit
		}
		if end == start {
			return nil
		}
		return slices.Clone(rows[start:end])
	}
	expr := func(v *uint64) *planpb.Expr {
		if v == nil {
			return nil
		}
		return makePlan2Uint64ConstExprWithType(*v)
	}

	for innerIndex, inner := range windows {
		for outerIndex, outer := range windows {
			limitExpr, offsetExpr, ok := composePagination(
				expr(inner.limit), expr(inner.offset), expr(outer.limit), expr(outer.offset),
			)
			if !ok {
				outerExhaustsInner := inner.limit != nil && *inner.limit > 0 &&
					outer.offset != nil && *outer.offset >= *inner.limit &&
					(outer.limit == nil || *outer.limit > 0)
				require.Truef(t, outerExhaustsInner,
					"literal windows %d then %d unexpectedly refused composition", innerIndex, outerIndex)
				continue
			}

			composed := window{}
			if limitExpr != nil {
				limit, literal := getLiteralUint64(limitExpr)
				require.True(t, literal)
				composed.limit = value(limit)
			}
			if offsetExpr != nil {
				offset, literal := getLiteralUint64(offsetExpr)
				require.True(t, literal)
				composed.offset = value(offset)
			}

			sequential := apply(apply(input, inner), outer)
			combined := apply(input, composed)
			require.Equalf(t, sequential, combined,
				"literal windows %d then %d changed the selected rows", innerIndex, outerIndex)
		}
	}
}
