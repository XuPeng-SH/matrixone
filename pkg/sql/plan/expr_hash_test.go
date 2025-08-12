// Copyright 2022 Matrix Origin
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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestExprHashKey(t *testing.T) {
	// 测试空表达式
	if key := ExprHashKey(nil); key != "nil" {
		t.Errorf("Expected 'nil' for nil expr, got %s", key)
	}

	// 测试列引用
	colExpr := &plan.Expr{
		Typ: plan.Type{Id: 1, Width: 10, Scale: 0, NotNullable: true},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: 2,
				Name:   "test_col",
			},
		},
	}
	key1 := ExprHashKey(colExpr)
	if key1 == "" {
		t.Error("Expected non-empty key for column expression")
	}

	// 测试相同表达式产生相同键
	key2 := ExprHashKey(colExpr)
	if key1 != key2 {
		t.Error("Same expression should produce same key")
	}

	// 测试字面量
	litExpr := &plan.Expr{
		Typ: plan.Type{Id: 2, Width: 8, Scale: 0, NotNullable: true},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Isnull: false,
				Value:  &plan.Literal_I64Val{I64Val: 123},
			},
		},
	}
	key3 := ExprHashKey(litExpr)
	if key3 == "" {
		t.Error("Expected non-empty key for literal expression")
	}

	// 测试函数表达式
	funcExpr := &plan.Expr{
		Typ: plan.Type{Id: 3, Width: 4, Scale: 0, NotNullable: true},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "add"},
				Args: []*plan.Expr{colExpr, litExpr},
			},
		},
	}
	key4 := ExprHashKey(funcExpr)
	if key4 == "" {
		t.Error("Expected non-empty key for function expression")
	}
}

func TestExprHashKeyPerformance(t *testing.T) {
	// 创建一个复杂的表达式用于性能测试
	complexExpr := &plan.Expr{
		Typ: plan.Type{Id: 1, Width: 10, Scale: 0, NotNullable: true},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "complex_function"},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{Id: 2, Width: 8, Scale: 0, NotNullable: true},
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: 1,
								ColPos: 2,
								Name:   "col1",
							},
						},
					},
					{
						Typ: plan.Type{Id: 3, Width: 4, Scale: 0, NotNullable: true},
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Isnull: false,
								Value:  &plan.Literal_I64Val{I64Val: 456},
							},
						},
					},
				},
			},
		},
	}

	// 测试 ExprHashKey 性能
	iterations := 10000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		ExprHashKey(complexExpr)
	}
	hashDuration := time.Since(start)

	// 测试 expr.String() 性能
	start = time.Now()
	for i := 0; i < iterations; i++ {
		complexExpr.String()
	}
	stringDuration := time.Since(start)

	t.Logf("ExprHashKey took %v for %d iterations", hashDuration, iterations)
	t.Logf("expr.String() took %v for %d iterations", stringDuration, iterations)
	t.Logf("Performance improvement: %.2fx faster", float64(stringDuration)/float64(hashDuration))

	// 验证性能提升
	if hashDuration >= stringDuration {
		t.Errorf("Expected ExprHashKey to be faster than expr.String(), but got %v vs %v", hashDuration, stringDuration)
	}
}

func TestExprHashKeyConsistency(t *testing.T) {
	// 测试哈希键的一致性
	expr1 := &plan.Expr{
		Typ: plan.Type{Id: 1, Width: 10, Scale: 0, NotNullable: true},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: 2,
				Name:   "test_col",
			},
		},
	}

	expr2 := &plan.Expr{
		Typ: plan.Type{Id: 1, Width: 10, Scale: 0, NotNullable: true},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: 2,
				Name:   "test_col",
			},
		},
	}

	key1 := ExprHashKey(expr1)
	key2 := ExprHashKey(expr2)

	if key1 != key2 {
		t.Errorf("Identical expressions should produce identical keys: %s vs %s", key1, key2)
	}

	// 测试不同表达式产生不同键
	expr3 := &plan.Expr{
		Typ: plan.Type{Id: 1, Width: 10, Scale: 0, NotNullable: true},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: 3, // 不同的列位置
				Name:   "test_col",
			},
		},
	}

	key3 := ExprHashKey(expr3)
	if key1 == key3 {
		t.Error("Different expressions should produce different keys")
	}
}

func BenchmarkExprHashKey(b *testing.B) {
	expr := &plan.Expr{
		Typ: plan.Type{Id: 1, Width: 10, Scale: 0, NotNullable: true},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "add"},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{Id: 2, Width: 8, Scale: 0, NotNullable: true},
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: 1,
								ColPos: 2,
								Name:   "col1",
							},
						},
					},
					{
						Typ: plan.Type{Id: 3, Width: 4, Scale: 0, NotNullable: true},
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Isnull: false,
								Value:  &plan.Literal_I64Val{I64Val: 123},
							},
						},
					},
				},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExprHashKey(expr)
	}
}

func BenchmarkExprString(b *testing.B) {
	expr := &plan.Expr{
		Typ: plan.Type{Id: 1, Width: 10, Scale: 0, NotNullable: true},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "add"},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{Id: 2, Width: 8, Scale: 0, NotNullable: true},
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: 1,
								ColPos: 2,
								Name:   "col1",
							},
						},
					},
					{
						Typ: plan.Type{Id: 3, Width: 4, Scale: 0, NotNullable: true},
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Isnull: false,
								Value:  &plan.Literal_I64Val{I64Val: 123},
							},
						},
					},
				},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		expr.String()
	}
}
