// Copyright 2021 - 2022 Matrix Origin
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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Concat(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtyp := types.T_varchar.ToType()
	// If any binary type exists return binary type.
	for _, v := range ivecs {
		if v.GetType().Oid == types.T_binary || v.GetType().Oid == types.T_varbinary || v.GetType().Oid == types.T_blob {
			rtyp = types.T_blob.ToType()
			break
		}
	}
	isAllConst := true

	for i := range ivecs {
		if ivecs[i].IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		if !ivecs[i].IsConst() {
			isAllConst = false
		}
	}
	if isAllConst {
		return concatWithAllConst(ivecs, proc, rtyp)
	}
	return concatWithSomeCols(ivecs, proc, rtyp)
}

func concatWithAllConst(ivecs []*vector.Vector, proc *process.Process, vct types.Type) (*vector.Vector, error) {
	//length := vectors[0].Length()
	res := ""
	for i := range ivecs {
		res += ivecs[i].UnsafeGetStringAt(0)
	}
	return vector.NewConstBytes(vct, []byte(res), ivecs[0].Length(), proc.Mp()), nil
}

func concatWithSomeCols(ivecs []*vector.Vector, proc *process.Process, rtyp types.Type) (*vector.Vector, error) {
	length := ivecs[0].Length()
	rvec := vector.NewVec(rtyp)
	for i := range ivecs {
		nulls.Or(ivecs[i].GetNulls(), rvec.GetNulls(), rvec.GetNulls())
	}
	val := make([]string, length)
	for i := 0; i < length; i++ {
		if nulls.Contains(rvec.GetNulls(), uint64(i)) {
			continue
		}
		for j := range ivecs {
			if ivecs[j].IsConst() {
				val[i] += ivecs[j].UnsafeGetStringAt(0)
			} else {
				val[i] += ivecs[j].UnsafeGetStringAt(i)
			}
		}
	}
	vector.AppendStringList(rvec, val, nil, proc.Mp())
	return rvec, nil
}
