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
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// ExprHashKey 为表达式生成高效的哈希键，替代 expr.String()
// 这个函数比 expr.String() 快很多，因为它避免了完整的字符串序列化
func ExprHashKey(expr *plan.Expr) string {
	if expr == nil {
		return "nil"
	}

	// 使用 FNV-1a 哈希算法，它比 MD5 更快
	h := fnv.New64a()

	// 写入表达式类型信息
	binary.Write(h, binary.LittleEndian, expr.Typ.Id)
	binary.Write(h, binary.LittleEndian, expr.Typ.Width)
	binary.Write(h, binary.LittleEndian, expr.Typ.Scale)
	binary.Write(h, binary.LittleEndian, expr.Typ.NotNullable)

	// 根据表达式类型写入关键信息
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		// 列引用：写入表位置和列位置
		binary.Write(h, binary.LittleEndian, e.Col.RelPos)
		binary.Write(h, binary.LittleEndian, e.Col.ColPos)
		if e.Col.Name != "" {
			h.Write([]byte(e.Col.Name))
		}

	case *plan.Expr_Lit:
		// 字面量：写入类型和值
		if e.Lit.Isnull {
			h.Write([]byte("null"))
		} else {
			writeLiteralValue(h, e.Lit.Value)
		}

	case *plan.Expr_F:
		// 函数：写入函数名和参数数量
		if e.F.Func != nil {
			h.Write([]byte(e.F.Func.ObjName))
		}
		binary.Write(h, binary.LittleEndian, int32(len(e.F.Args)))
		// 递归处理参数
		for _, arg := range e.F.Args {
			h.Write([]byte(ExprHashKey(arg)))
		}

	case *plan.Expr_P:
		// 参数引用：写入参数位置
		binary.Write(h, binary.LittleEndian, e.P.Pos)

	case *plan.Expr_V:
		// 变量引用：写入变量名
		if e.V.Name != "" {
			h.Write([]byte(e.V.Name))
		}

	case *plan.Expr_Sub:
		// 子查询：写入节点ID
		if e.Sub.NodeId != 0 {
			binary.Write(h, binary.LittleEndian, e.Sub.NodeId)
		}

	case *plan.Expr_List:
		// 列表：写入列表长度和每个元素
		binary.Write(h, binary.LittleEndian, int32(len(e.List.List)))
		for _, item := range e.List.List {
			h.Write([]byte(ExprHashKey(item)))
		}

	case *plan.Expr_T:
		// 类型转换：TargetType 是空结构体，只写入类型标识
		h.Write([]byte("type_cast"))

	case *plan.Expr_Vec:
		// 向量：写入向量长度
		binary.Write(h, binary.LittleEndian, e.Vec.Len)

	case *plan.Expr_Fold:
		// 折叠：写入折叠ID
		binary.Write(h, binary.LittleEndian, e.Fold.Id)

	default:
		// 对于未知类型，使用完整的字符串表示（作为后备方案）
		h.Write([]byte(fmt.Sprintf("%T", expr.Expr)))
	}

	// 返回十六进制字符串作为键
	return fmt.Sprintf("%016x", h.Sum64())
}

// writeLiteralValue 将字面量值写入哈希器
func writeLiteralValue(h hash.Hash64, value interface{}) {
	switch v := value.(type) {
	case *plan.Literal_I8Val:
		binary.Write(h, binary.LittleEndian, v.I8Val)
	case *plan.Literal_I16Val:
		binary.Write(h, binary.LittleEndian, v.I16Val)
	case *plan.Literal_I32Val:
		binary.Write(h, binary.LittleEndian, v.I32Val)
	case *plan.Literal_I64Val:
		binary.Write(h, binary.LittleEndian, v.I64Val)
	case *plan.Literal_U8Val:
		binary.Write(h, binary.LittleEndian, v.U8Val)
	case *plan.Literal_U16Val:
		binary.Write(h, binary.LittleEndian, v.U16Val)
	case *plan.Literal_U32Val:
		binary.Write(h, binary.LittleEndian, v.U32Val)
	case *plan.Literal_U64Val:
		binary.Write(h, binary.LittleEndian, v.U64Val)
	case *plan.Literal_Fval:
		binary.Write(h, binary.LittleEndian, v.Fval)
	case *plan.Literal_Dval:
		binary.Write(h, binary.LittleEndian, v.Dval)
	case *plan.Literal_Dateval:
		binary.Write(h, binary.LittleEndian, v.Dateval)
	case *plan.Literal_Datetimeval:
		binary.Write(h, binary.LittleEndian, v.Datetimeval)
	case *plan.Literal_Timeval:
		binary.Write(h, binary.LittleEndian, v.Timeval)
	case *plan.Literal_Timestampval:
		binary.Write(h, binary.LittleEndian, v.Timestampval)
	case *plan.Literal_Sval:
		h.Write([]byte(v.Sval))
	case *plan.Literal_Bval:
		binary.Write(h, binary.LittleEndian, v.Bval)
	case *plan.Literal_EnumVal:
		binary.Write(h, binary.LittleEndian, v.EnumVal)
	case *plan.Literal_Decimal64Val:
		binary.Write(h, binary.LittleEndian, v.Decimal64Val.A)
	case *plan.Literal_Decimal128Val:
		binary.Write(h, binary.LittleEndian, v.Decimal128Val.A)
		binary.Write(h, binary.LittleEndian, v.Decimal128Val.B)
	default:
		// 对于未知的字面量类型，写入类型名称
		h.Write([]byte(fmt.Sprintf("%T", value)))
	}
}

// ExprHashKeyMD5 使用 MD5 生成哈希键（备选方案，比 FNV 稍慢但更安全）
func ExprHashKeyMD5(expr *plan.Expr) string {
	if expr == nil {
		return "nil"
	}

	// 使用 protobuf 的 Marshal 方法获取字节数据
	data, err := expr.Marshal()
	if err != nil {
		// 如果序列化失败，回退到字符串方法
		return expr.String()
	}

	// 计算 MD5 哈希
	hash := md5.Sum(data)
	return fmt.Sprintf("%x", hash)
}

// ExprHashKeySimple 生成简单的哈希键，基于表达式的关键特征
// 这个版本比 ExprHashKey 更快，但可能产生更多冲突
func ExprHashKeySimple(expr *plan.Expr) string {
	if expr == nil {
		return "nil"
	}

	var key string

	// 基于表达式类型生成简单键
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		key = fmt.Sprintf("col_%d_%d", e.Col.RelPos, e.Col.ColPos)

	case *plan.Expr_Lit:
		if e.Lit.Isnull {
			key = "lit_null"
		} else {
			key = fmt.Sprintf("lit_%T", e.Lit.Value)
		}

	case *plan.Expr_F:
		if e.F.Func != nil {
			key = fmt.Sprintf("func_%s_%d", e.F.Func.ObjName, len(e.F.Args))
		} else {
			key = "func_unknown"
		}

	case *plan.Expr_P:
		key = fmt.Sprintf("param_%d", e.P.Pos)

	case *plan.Expr_V:
		key = fmt.Sprintf("var_%s", e.V.Name)

	case *plan.Expr_Sub:
		key = fmt.Sprintf("sub_%d", e.Sub.NodeId)

	case *plan.Expr_List:
		key = fmt.Sprintf("list_%d", len(e.List.List))

	case *plan.Expr_T:
		key = "type_cast"

	case *plan.Expr_Vec:
		key = fmt.Sprintf("vec_%d", e.Vec.Len)

	case *plan.Expr_Fold:
		key = fmt.Sprintf("fold_%d", e.Fold.Id)

	default:
		key = fmt.Sprintf("unknown_%T", expr.Expr)
	}

	return key
}
