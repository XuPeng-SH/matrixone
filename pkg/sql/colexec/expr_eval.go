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

package colexec

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	constBType          = types.T_bool.ToType()
	constI8Type         = types.T_int8.ToType()
	constI16Type        = types.T_int16.ToType()
	constI32Type        = types.T_int32.ToType()
	constI64Type        = types.T_int64.ToType()
	constU8Type         = types.T_uint8.ToType()
	constU16Type        = types.T_uint16.ToType()
	constU32Type        = types.T_uint32.ToType()
	constU64Type        = types.T_uint64.ToType()
	constFType          = types.T_float32.ToType()
	constDType          = types.T_float64.ToType()
	constSType          = types.T_varchar.ToType()
	constBinType        = types.T_blob.ToType()
	constDateType       = types.T_date.ToType()
	constTimeType       = types.T_time.ToType()
	constDatetimeType   = types.T_datetime.ToType()
	constTimestampTypes = []types.Type{
		types.New(types.T_timestamp, 0, 0),
		types.New(types.T_timestamp, 0, 1),
		types.New(types.T_timestamp, 0, 2),
		types.New(types.T_timestamp, 0, 3),
		types.New(types.T_timestamp, 0, 4),
		types.New(types.T_timestamp, 0, 5),
		types.New(types.T_timestamp, 0, 6),
	}
)

func getConstVecInList(ctx context.Context, proc *process.Process, exprs []*plan.Expr) (*vector.Vector, error) {
	lenList := len(exprs)
	vec, err := proc.AllocVectorOfRows(types.New(types.T(exprs[0].Typ.Id), exprs[0].Typ.Width, exprs[0].Typ.Scale), lenList, nil)
	if err != nil {
		panic(moerr.NewOOM(proc.Ctx))
	}
	for i := 0; i < lenList; i++ {
		expr := exprs[i]
		t, ok := expr.Expr.(*plan.Expr_C)
		if !ok {
			return nil, moerr.NewInternalError(proc.Ctx, "args in list must be constant")
		}
		if t.C.GetIsnull() {
			vec.GetNulls().Set(uint64(i))
		} else {
			switch t.C.GetValue().(type) {
			case *plan.Const_Bval:
				veccol := vector.MustFixedCol[bool](vec)
				veccol[i] = t.C.GetBval()
			case *plan.Const_I8Val:
				veccol := vector.MustFixedCol[int8](vec)
				veccol[i] = int8(t.C.GetI8Val())
			case *plan.Const_I16Val:
				veccol := vector.MustFixedCol[int16](vec)
				veccol[i] = int16(t.C.GetI16Val())
			case *plan.Const_I32Val:
				veccol := vector.MustFixedCol[int32](vec)
				veccol[i] = t.C.GetI32Val()
			case *plan.Const_I64Val:
				veccol := vector.MustFixedCol[int64](vec)
				veccol[i] = t.C.GetI64Val()
			case *plan.Const_U8Val:
				veccol := vector.MustFixedCol[uint8](vec)
				veccol[i] = uint8(t.C.GetU8Val())
			case *plan.Const_U16Val:
				veccol := vector.MustFixedCol[uint16](vec)
				veccol[i] = uint16(t.C.GetU16Val())
			case *plan.Const_U32Val:
				veccol := vector.MustFixedCol[uint32](vec)
				veccol[i] = t.C.GetU32Val()
			case *plan.Const_U64Val:
				veccol := vector.MustFixedCol[uint64](vec)
				veccol[i] = t.C.GetU64Val()
			case *plan.Const_Fval:
				veccol := vector.MustFixedCol[float32](vec)
				veccol[i] = t.C.GetFval()
			case *plan.Const_Dval:
				veccol := vector.MustFixedCol[float64](vec)
				veccol[i] = t.C.GetDval()
			case *plan.Const_Dateval:
				veccol := vector.MustFixedCol[types.Date](vec)
				veccol[i] = types.Date(t.C.GetDateval())
			case *plan.Const_Timeval:
				veccol := vector.MustFixedCol[types.Time](vec)
				veccol[i] = types.Time(t.C.GetTimeval())
			case *plan.Const_Datetimeval:
				veccol := vector.MustFixedCol[types.Datetime](vec)
				veccol[i] = types.Datetime(t.C.GetDatetimeval())
			case *plan.Const_Decimal64Val:
				cd64 := t.C.GetDecimal64Val()
				d64 := types.Decimal64(cd64.A)
				veccol := vector.MustFixedCol[types.Decimal64](vec)
				veccol[i] = d64
			case *plan.Const_Decimal128Val:
				cd128 := t.C.GetDecimal128Val()
				d128 := types.Decimal128{B0_63: uint64(cd128.A), B64_127: uint64(cd128.B)}
				veccol := vector.MustFixedCol[types.Decimal128](vec)
				veccol[i] = d128
			case *plan.Const_Timestampval:
				scale := expr.Typ.Scale
				if scale < 0 || scale > 6 {
					return nil, moerr.NewInternalError(proc.Ctx, "invalid timestamp scale")
				}
				veccol := vector.MustFixedCol[types.Timestamp](vec)
				veccol[i] = types.Timestamp(t.C.GetTimestampval())
			case *plan.Const_Sval:
				sval := t.C.GetSval()
				vector.SetStringAt(vec, i, sval, proc.Mp())
			case *plan.Const_Defaultval:
				defaultVal := t.C.GetDefaultval()
				veccol := vector.MustFixedCol[bool](vec)
				veccol[i] = defaultVal
			default:
				return nil, moerr.NewNYI(ctx, fmt.Sprintf("const expression %v", t.C.GetValue()))
			}
			vec.SetIsBin(t.C.IsBin)
		}
	}
	return vec, nil
}

func getConstVec(ctx context.Context, proc *process.Process, expr *plan.Expr, length int) (*vector.Vector, error) {
	var vec *vector.Vector
	t := expr.Expr.(*plan.Expr_C)
	if t.C.GetIsnull() {
		vec = vector.NewConstNull(types.New(types.T(expr.Typ.Id), expr.Typ.Width, expr.Typ.Scale), length, proc.Mp())
	} else {
		switch t.C.GetValue().(type) {
		case *plan.Const_Bval:
			vec = vector.NewConstFixed(constBType, t.C.GetBval(), length, proc.Mp())
		case *plan.Const_I8Val:
			vec = vector.NewConstFixed(constI8Type, int8(t.C.GetI8Val()), length, proc.Mp())
		case *plan.Const_I16Val:
			vec = vector.NewConstFixed(constI16Type, int16(t.C.GetI16Val()), length, proc.Mp())
		case *plan.Const_I32Val:
			vec = vector.NewConstFixed(constI32Type, int32(t.C.GetI32Val()), length, proc.Mp())
		case *plan.Const_I64Val:
			vec = vector.NewConstFixed(constI64Type, int64(t.C.GetI64Val()), length, proc.Mp())
		case *plan.Const_U8Val:
			vec = vector.NewConstFixed(constU8Type, uint8(t.C.GetU8Val()), length, proc.Mp())
		case *plan.Const_U16Val:
			vec = vector.NewConstFixed(constU16Type, uint16(t.C.GetU16Val()), length, proc.Mp())
		case *plan.Const_U32Val:
			vec = vector.NewConstFixed(constU32Type, uint32(t.C.GetU32Val()), length, proc.Mp())
		case *plan.Const_U64Val:
			vec = vector.NewConstFixed(constU64Type, uint64(t.C.GetU64Val()), length, proc.Mp())
		case *plan.Const_Fval:
			vec = vector.NewConstFixed(constFType, t.C.GetFval(), length, proc.Mp())
		case *plan.Const_Dval:
			vec = vector.NewConstFixed(constDType, t.C.GetDval(), length, proc.Mp())
		case *plan.Const_Dateval:
			vec = vector.NewConstFixed(constDateType, types.Date(t.C.GetDateval()), length, proc.Mp())
		case *plan.Const_Timeval:
			vec = vector.NewConstFixed(constTimeType, types.Time(t.C.GetTimeval()), length, proc.Mp())
		case *plan.Const_Datetimeval:
			vec = vector.NewConstFixed(constDatetimeType, types.Datetime(t.C.GetDatetimeval()), length, proc.Mp())
		case *plan.Const_Decimal64Val:
			cd64 := t.C.GetDecimal64Val()
			d64 := types.Decimal64(cd64.A)
			typ := types.New(types.T_decimal64, expr.Typ.Width, expr.Typ.Scale)
			vec = vector.NewConstFixed(typ, d64, length, proc.Mp())
		case *plan.Const_Decimal128Val:
			cd128 := t.C.GetDecimal128Val()
			d128 := types.Decimal128{B0_63: uint64(cd128.A), B64_127: uint64(cd128.B)}
			typ := types.New(types.T_decimal128, expr.Typ.Width, expr.Typ.Scale)
			vec = vector.NewConstFixed(typ, d128, length, proc.Mp())
		case *plan.Const_Timestampval:
			scale := expr.Typ.Scale
			if scale < 0 || scale > 6 {
				return nil, moerr.NewInternalError(proc.Ctx, "invalid timestamp scale")
			}
			vec = vector.NewConstFixed(constTimestampTypes[scale], types.Timestamp(t.C.GetTimestampval()), length, proc.Mp())
		case *plan.Const_Sval:
			sval := t.C.GetSval()
			// Distingush binary with non-binary string.
			if expr.Typ != nil {
				if expr.Typ.Id == int32(types.T_binary) || expr.Typ.Id == int32(types.T_varbinary) || expr.Typ.Id == int32(types.T_blob) {
					vec = vector.NewConstBytes(constBinType, []byte(sval), length, proc.Mp())
				} else {
					vec = vector.NewConstBytes(constSType, []byte(sval), length, proc.Mp())
				}
			} else {
				vec = vector.NewConstBytes(constSType, []byte(sval), length, proc.Mp())
			}
		case *plan.Const_Defaultval:
			defaultVal := t.C.GetDefaultval()
			vec = vector.NewConstFixed(constBType, defaultVal, length, proc.Mp())
		default:
			return nil, moerr.NewNYI(ctx, fmt.Sprintf("const expression %v", t.C.GetValue()))
		}
		vec.SetIsBin(t.C.IsBin)
	}
	return vec, nil
}

func EvalExpr(bat *batch.Batch, proc *process.Process, expr *plan.Expr) (*vector.Vector, error) {
	var length = len(bat.Zs)
	if length == 0 {
		return vector.NewConstNull(types.New(types.T(expr.Typ.Id), expr.Typ.Width, expr.Typ.Scale), length, proc.Mp()), nil
	}

	e := expr.Expr
	switch t := e.(type) {
	case *plan.Expr_C:
		return getConstVec(proc.Ctx, proc, expr, length)
	case *plan.Expr_T:
		// return a vector recorded type information but without real data
		return vector.NewConstNull(types.New(types.T(t.T.Typ.GetId()), t.T.Typ.GetWidth(), t.T.Typ.GetScale()), length, proc.Mp()), nil
	case *plan.Expr_Col:
		vec := bat.Vecs[t.Col.ColPos]
		if vec.IsConstNull() {
			vec.SetType(types.New(types.T(expr.Typ.Id), expr.Typ.Width, expr.Typ.Scale))
		}
		return vec, nil
	case *plan.Expr_List:
		return getConstVecInList(proc.Ctx, proc, t.List.List)
	case *plan.Expr_F:
		var result *vector.Vector

		fid := t.F.GetFunc().GetObj()
		f, err := function.GetFunctionByID(proc.Ctx, fid)
		if err != nil {
			return nil, err
		}

		functionParameters := make([]*vector.Vector, len(t.F.Args))
		for i := range functionParameters {
			functionParameters[i], err = EvalExpr(bat, proc, t.F.Args[i])
			if err != nil {
				break
			}
		}
		if err != nil {
			cleanVectorsExceptList(proc, functionParameters, bat.Vecs)
			return nil, err
		}

		result, err = evalFunction(proc, f, functionParameters, length)
		cleanVectorsExceptList(proc, functionParameters, append(bat.Vecs, result))
		if err != nil {
			return nil, err
		}
		return result, nil
	default:
		// *plan.Expr_Corr, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Sub
		return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("unsupported eval expr '%v'", t))
	}
}

func JoinFilterEvalExpr(r, s *batch.Batch, rRow int, proc *process.Process, expr *plan.Expr) (*vector.Vector, error) {
	length := len(s.Zs)
	e := expr.Expr
	switch t := e.(type) {
	case *plan.Expr_C:
		return getConstVec(proc.Ctx, proc, expr, length)
	case *plan.Expr_T:
		// return a vector recorded type information but without real data
		return vector.NewConstNull(types.New(types.T(t.T.Typ.GetId()), t.T.Typ.GetWidth(), t.T.Typ.GetScale()), length, proc.Mp()), nil
	case *plan.Expr_Col:
		if t.Col.RelPos == 0 {
			return r.Vecs[t.Col.ColPos].ToConst(rRow, length, proc.Mp()), nil
		}
		return s.Vecs[t.Col.ColPos], nil
	case *plan.Expr_List:
		return getConstVecInList(proc.Ctx, proc, t.List.List)
	case *plan.Expr_F:
		var result *vector.Vector

		fid := t.F.GetFunc().GetObj()
		f, err := function.GetFunctionByID(proc.Ctx, fid)
		if err != nil {
			return nil, err
		}

		functionParameters := make([]*vector.Vector, len(t.F.Args))
		for i := range functionParameters {
			functionParameters[i], err = JoinFilterEvalExpr(r, s, rRow, proc, t.F.Args[i])
			if err != nil {
				break
			}
		}
		if err != nil {
			cleanVectorsExceptList(proc, functionParameters, append(r.Vecs, s.Vecs...))
			return nil, err
		}

		result, err = evalFunction(proc, f, functionParameters, length)
		cleanVectorsExceptList(proc, functionParameters, append(append(r.Vecs, s.Vecs...), result))
		if err != nil {
			return nil, err
		}
		return result, nil
	default:
		// *plan.Expr_Corr, *plan.Expr_List, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Sub
		return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("eval expr '%v'", t))
	}
}

func EvalFilterExprWithMinMax(
	ctx context.Context,
	expr *plan.Expr,
	input *batch.Batch,
	proc *process.Process,
) (outVec *vector.Vector, stopped bool) {
	if stopped {
		return
	}
	var err error
	switch t := expr.Expr.(type) {
	case *plan.Expr_C:
		if outVec, err = getConstVec(ctx, proc, expr, input.Length()); err != nil {
			stopped = true
		}
		return
	case *plan.Expr_Col:
		outVec = input.Vecs[t.Col.ColPos]
		return
	case *plan.Expr_F:
		f, err := function.GetFunctionByID(ctx, t.F.GetFunc().GetObj())
		if err != nil {
			stopped = true
			return
		}
		vecs := make([]*vector.Vector, len(t.F.Args))
		for i := range vecs {
			if vecs[i], stopped = EvalFilterExprWithMinMax(ctx, t.F.Args[i], input, proc); stopped {
				break
			}
		}
		if stopped {
			cleanVectorsExceptList(proc, vecs, input.Vecs)
			return
		}

		var compType uint8
		switch t.F.Func.ObjName {
		case ">":
			outVec = vector.NewVec(types.T_bool.ToType())
			compType = 0
		case "<":
			outVec = vector.NewVec(types.T_bool.ToType())
			compType = 1
		case ">=":
			outVec = vector.NewVec(types.T_bool.ToType())
			compType = 2
		case "<=":
			outVec = vector.NewVec(types.T_bool.ToType())
			compType = 3
		case "=":
			outVec = vector.NewVec(types.T_bool.ToType())
			compType = 4
		}
		if outVec != nil {
			if err = vector.CompareWithMinMax(vecs[0], vecs[1], outVec, compType, proc.Mp()); err != nil {
				outVec.Free(proc.Mp())
				outVec = nil
				stopped = true
			}
			cleanVectorsExceptList(proc, vecs, input.Vecs)
			return
		} else {
			outVec, err = evalFunction(proc, f, vecs, len(input.Zs))
			if err != nil {
				// TODO: outVec maybe leak
				cleanVectorsExceptList(proc, append(vecs, outVec), input.Vecs)
				stopped = true
			} else {
				cleanVectorsExceptList(proc, vecs, append(input.Vecs, outVec))
			}
			return
		}
	}
	return
}

func EvalExprByZonemapBat(ctx context.Context, bat *batch.Batch, proc *process.Process, expr *plan.Expr) (*vector.Vector, error) {
	length := len(bat.Zs)
	if length == 0 {
		return vector.NewConstNull(types.New(types.T(expr.Typ.Id), expr.Typ.Width, expr.Typ.Scale), 1, proc.Mp()), nil
	}

	e := expr.Expr
	switch t := e.(type) {
	case *plan.Expr_C:
		return getConstVec(ctx, proc, expr, length)
	case *plan.Expr_T:
		// return a vector recorded type information but without real data
		return vector.NewConstNull(types.New(types.T(t.T.Typ.GetId()), t.T.Typ.GetWidth(), t.T.Typ.GetScale()), length, proc.Mp()), nil
	case *plan.Expr_Col:
		vec := bat.Vecs[t.Col.ColPos]
		if vec.IsConstNull() {
			vec.SetType(types.New(types.T(expr.Typ.Id), expr.Typ.Width, expr.Typ.Scale))
		}
		return vec, nil
	case *plan.Expr_F:
		var result *vector.Vector

		fid := t.F.GetFunc().GetObj()
		f, err := function.GetFunctionByID(proc.Ctx, fid)
		if err != nil {
			return nil, err
		}

		functionParameters := make([]*vector.Vector, len(t.F.Args))
		for i := range functionParameters {
			functionParameters[i], err = EvalExprByZonemapBat(ctx, bat, proc, t.F.Args[i])
			if err != nil {
				break
			}
		}
		if err != nil {
			cleanVectorsExceptList(proc, functionParameters, bat.Vecs)
			return nil, err
		}

		compareAndReturn := func(isTrue bool, err error) (*vector.Vector, error) {
			if err != nil {
				// if it can't compare, just return true.
				// that means we don't know this filter expr's return, so you must readBlock
				return vector.NewConstFixed(types.T_bool.ToType(), true, 1, proc.Mp()), nil
			}
			return vector.NewConstFixed(types.T_bool.ToType(), isTrue, 1, proc.Mp()), nil
		}

		switch t.F.Func.ObjName {
		case ">":
			// if someone in left > someone in right, that will be true
			return compareAndReturn(functionParameters[0].CompareAndCheckAnyResultIsTrue(ctx, functionParameters[1], ">"))
		case "<":
			// if someone in left < someone in right, that will be true
			return compareAndReturn(functionParameters[0].CompareAndCheckAnyResultIsTrue(ctx, functionParameters[1], "<"))
		case "=":
			// if left intersect right, that will be true
			return compareAndReturn(functionParameters[0].CompareAndCheckIntersect(functionParameters[1]))
		case ">=":
			// if someone in left >= someone in right, that will be true
			return compareAndReturn(functionParameters[0].CompareAndCheckAnyResultIsTrue(ctx, functionParameters[1], ">="))
		case "<=":
			// if someone in left <= someone in right, that will be true
			return compareAndReturn(functionParameters[0].CompareAndCheckAnyResultIsTrue(ctx, functionParameters[1], "<="))
		case "and":
			// if left has one true and right has one true, that will be true
			cols1 := vector.MustFixedCol[bool](functionParameters[0])
			cols2 := vector.MustFixedCol[bool](functionParameters[1])

			for _, leftHasTrue := range cols1 {
				if leftHasTrue {
					for _, rightHasTrue := range cols2 {
						if rightHasTrue {
							return vector.NewConstFixed(types.T_bool.ToType(), true, 1, proc.Mp()), nil
						}
					}
					break
				}
			}
			return vector.NewConstFixed(types.T_bool.ToType(), false, 1, proc.Mp()), nil
		case "or":
			// if someone is true in left/right, that will be true
			cols1 := vector.MustFixedCol[bool](functionParameters[0])
			cols2 := vector.MustFixedCol[bool](functionParameters[1])
			for _, flag := range cols1 {
				if flag {
					return vector.NewConstFixed(types.T_bool.ToType(), true, 1, proc.Mp()), nil
				}
			}
			for _, flag := range cols2 {
				if flag {
					return vector.NewConstFixed(types.T_bool.ToType(), true, 1, proc.Mp()), nil
				}
			}
			return vector.NewConstFixed(types.T_bool.ToType(), false, 1, proc.Mp()), nil
		}

		result, err = evalFunction(proc, f, functionParameters, len(bat.Zs))
		cleanVectorsExceptList(proc, functionParameters, append(bat.Vecs, result))
		if err != nil {
			return nil, err
		}
		return result, nil
	default:
		// *plan.Expr_Corr,  *plan.Expr_P, *plan.Expr_V, *plan.Expr_Sub
		return nil, moerr.NewNYI(ctx, fmt.Sprintf("unsupported eval expr '%v'", t))
	}
}

func JoinFilterEvalExprInBucket(r, s *batch.Batch, rRow, sRow int, proc *process.Process, expr *plan.Expr) (*vector.Vector, error) {
	e := expr.Expr
	switch t := e.(type) {
	case *plan.Expr_C:
		return getConstVec(proc.Ctx, proc, expr, 1)
	case *plan.Expr_T:
		// return a vector recorded type information but without real data
		return vector.NewConstNull(types.New(types.T(t.T.Typ.GetId()), t.T.Typ.GetWidth(), t.T.Typ.GetScale()), 1, proc.Mp()), nil
	case *plan.Expr_Col:
		if t.Col.RelPos == 0 {
			return r.Vecs[t.Col.ColPos].ToConst(rRow, 1, proc.Mp()), nil
		}
		return s.Vecs[t.Col.ColPos].ToConst(sRow, 1, proc.Mp()), nil
	case *plan.Expr_F:
		var result *vector.Vector

		fid := t.F.GetFunc().GetObj()
		f, err := function.GetFunctionByID(proc.Ctx, fid)
		if err != nil {
			return nil, err
		}

		functionParameters := make([]*vector.Vector, len(t.F.Args))
		for i := range functionParameters {
			functionParameters[i], err = JoinFilterEvalExprInBucket(r, s, rRow, sRow, proc, t.F.Args[i])
			if err != nil {
				break
			}
		}
		if err != nil {
			cleanVectorsExceptList(proc, functionParameters, append(r.Vecs, s.Vecs...))
			return nil, err
		}

		result, err = evalFunction(proc, f, functionParameters, 1)
		cleanVectorsExceptList(proc, functionParameters, append(append(r.Vecs, s.Vecs...), result))
		if err != nil {
			return nil, err
		}
		return result, nil
	default:
		// *plan.Expr_Corr, *plan.Expr_List, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Sub
		return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("eval expr '%v'", t))
	}
}

func evalFunction(proc *process.Process, f *function.Function, args []*vector.Vector, length int) (*vector.Vector, error) {
	if !f.UseNewFramework {
		v, err := f.VecFn(args, proc)
		if err != nil {
			return nil, err
		}
		v.SetLength(length)
		return v, nil
	}
	var resultWrapper vector.FunctionResultWrapper
	var err error

	var parameterTypes []types.Type
	if f.FlexibleReturnType != nil {
		parameterTypes = make([]types.Type, len(args))
		for i := range args {
			parameterTypes[i] = *args[i].GetType()
		}
	}
	rTyp, _ := f.ReturnType(parameterTypes)
	numScalar := 0
	// If any argument is `NULL`, return NULL.
	// If all arguments are scalar, return scalar.
	for i := range args {
		if args[i].IsConst() {
			numScalar++
		} else {
			if len(f.ParameterMustScalar) > i && f.ParameterMustScalar[i] {
				return nil, moerr.NewInternalError(proc.Ctx,
					fmt.Sprintf("the %dth parameter of function can only be constant", i+1))
			}
		}
	}

	if !f.Volatile && numScalar == len(args) {
		resultWrapper = vector.NewFunctionResultWrapper(rTyp, proc.Mp(), true, length)
		// XXX only evaluate the first row.
		err = f.NewFn(args, resultWrapper, proc, 1)
	} else {
		resultWrapper = vector.NewFunctionResultWrapper(rTyp, proc.Mp(), false, length)
		err = f.NewFn(args, resultWrapper, proc, length)
	}
	if err != nil {
		resultWrapper.Free()
		return nil, err
	}
	rvec := resultWrapper.GetResultVector()
	rvec.SetLength(length)
	return rvec, nil
}

func cleanVectorsExceptList(proc *process.Process, vs []*vector.Vector, excepts []*vector.Vector) {
	mp := proc.Mp()
	for i := range vs {
		if vs[i] == nil {
			continue
		}
		needClean := true
		for j := range excepts {
			if excepts[j] == vs[i] {
				needClean = false
				break
			}
		}
		if needClean {
			vs[i].Free(mp)
		}
	}
}

// RewriteFilterExprList will convert an expression list to be an AndExpr
func RewriteFilterExprList(list []*plan.Expr) *plan.Expr {
	l := len(list)
	if l == 0 {
		return nil
	} else if l == 1 {
		return list[0]
	} else {
		left := list[0]
		right := RewriteFilterExprList(list[1:])
		return &plan.Expr{
			Typ:  left.Typ,
			Expr: makeAndExpr(left, right),
		}
	}
}

func SplitAndExprs(list []*plan.Expr) []*plan.Expr {
	exprs := make([]*plan.Expr, 0, len(list))
	for i := range list {
		exprs = append(exprs, splitAndExpr(list[i])...)
	}
	return exprs
}

func splitAndExpr(expr *plan.Expr) []*plan.Expr {
	exprs := make([]*plan.Expr, 0, 1)
	if e, ok := expr.Expr.(*plan.Expr_F); ok {
		fid, _ := function.DecodeOverloadID(e.F.Func.GetObj())
		if fid == function.AND {
			exprs = append(exprs, splitAndExpr(e.F.Args[0])...)
			exprs = append(exprs, splitAndExpr(e.F.Args[1])...)
			return exprs
		}
	}
	exprs = append(exprs, expr)
	return exprs
}

func makeAndExpr(left, right *plan.Expr) *plan.Expr_F {
	return &plan.Expr_F{
		F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.AndFunctionEncodedID, ObjName: function.AndFunctionName},
			Args: []*plan.Expr{left, right},
		},
	}
}
