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

package main

import (
	"context"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/panjf2000/ants/v2"
)

var sampleDir = "/tmp/sample2"
var dbName = "db"
var cpuprofile = "/tmp/sample2/cpuprofile"
var memprofile = "/tmp/sample2/memprofile"

func init() {
	os.RemoveAll(sampleDir)
}

func startProfile() {
	f, _ := os.Create(cpuprofile)
	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
}

func stopProfile() {
	pprof.StopCPUProfile()
	memf, _ := os.Create(memprofile)
	defer memf.Close()
	_ = pprof.Lookup("heap").WriteTo(memf, 0)
}

func main() {
	ctx := context.Background()

	tae, _ := db.Open(ctx, sampleDir, nil)
	defer tae.Close()

	schema := catalog.MockSchemaAll(10, 3)
	schema.BlockMaxRows = 1000
	schema.ObjectMaxBlocks = 10
	batchCnt := uint32(10)
	batchRows := schema.BlockMaxRows * 1 / 2 * batchCnt
	{
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.CreateDatabase(dbName, "", "")
		_, _ = db.CreateRelation(schema)
		if err := txn.Commit(context.Background()); err != nil {
			panic(err)
		}
	}
	bat := catalog.MockBatch(schema, int(batchRows))
	defer bat.Close()
	bats := bat.Split(int(batchCnt))
	var wg sync.WaitGroup
	doAppend := func(b *containers.Batch) func() {
		return func() {
			defer wg.Done()
			txn, _ := tae.StartTxn(nil)
			db, err := txn.GetDatabase(dbName)
			if err != nil {
				panic(err)
			}
			rel, err := db.GetRelationByName(schema.Name)
			if err != nil {
				panic(err)
			}
			if err := rel.Append(context.Background(), b); err != nil {
				panic(err)
			}
			if err := txn.Commit(context.Background()); err != nil {
				panic(err)
			}
		}
	}
	p, _ := ants.NewPool(4)
	now := time.Now()
	startProfile()
	for _, b := range bats {
		wg.Add(1)
		_ = p.Submit(doAppend(b))
	}
	wg.Wait()
	stopProfile()
	logutil.Infof("Append takes: %s", time.Since(now))

	{
		txn, _ := tae.StartTxn(nil)
		db, err := txn.GetDatabase(dbName)
		if err != nil {
			panic(err)
		}
		rel, err := db.GetRelationByName(schema.Name)
		if err != nil {
			panic(err)
		}
		objIt := rel.MakeObjectIt(false)
		for objIt.Valid() {
			obj := objIt.GetObject()
			logutil.Info(obj.String())
			for i := 0; i < obj.BlkCnt(); i++ {
				view, err := obj.GetColumnDataById(context.Background(), uint16(i), 0, common.DefaultAllocator)
				logutil.Infof("Block %s Rows %d", obj.Fingerprint().BlockString(), view.Length())
				if err != nil {
					panic(err)
				}
				defer view.Close()
			}
			objIt.Next()
		}
		if err = txn.Commit(context.Background()); err != nil {
			panic(err)
		}
	}
	logutil.Info(tae.Catalog.SimplePPString(common.PPL1))
}
