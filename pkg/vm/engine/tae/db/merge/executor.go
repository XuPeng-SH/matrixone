// Copyright 2023 Matrix Origin
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

package merge

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

type activeTaskStats map[uint64]struct {
	blk      int
	estBytes int
}

// MergeExecutor consider resources to decide to merge or not.
type MergeExecutor struct {
	tableName           string
	rt                  *dbutils.Runtime
	cnSched             CNMergeScheduler
	memAvail            int
	memSpare            int // 15% of total memory
	cpuPercent          float64
	activeMergeBlkCount int32
	activeEstimateBytes int64
	taskConsume         struct {
		sync.Mutex
		o map[objectio.ObjectId]struct{}
		m activeTaskStats
	}
}

func NewMergeExecutor(rt *dbutils.Runtime, sched CNMergeScheduler) *MergeExecutor {
	return &MergeExecutor{
		rt:      rt,
		cnSched: sched,
	}
}

func (e *MergeExecutor) RefreshMemInfo() {
	if stats, err := mem.VirtualMemory(); err == nil {
		e.memAvail = int(stats.Available)
		if e.memSpare == 0 {
			e.memSpare = int(float32(stats.Total) * 0.15)
		}
	}
	if percents, err := cpu.Percent(0, false); err == nil {
		e.cpuPercent = percents[0]
	}
}

func (e *MergeExecutor) PrintStats() {
	cnt := atomic.LoadInt32(&e.activeMergeBlkCount)
	if cnt == 0 && e.MemAvailBytes() > 512*common.Const1MBytes {
		return
	}

	logutil.Infof(
		"Mergeblocks avail mem: %v(%v reserved), active mergeing size: %v, active merging blk cnt: %d",
		common.HumanReadableBytes(e.memAvail),
		common.HumanReadableBytes(e.memSpare),
		common.HumanReadableBytes(int(atomic.LoadInt64(&e.activeEstimateBytes))), cnt,
	)
}

func (e *MergeExecutor) AddActiveTask(taskId uint64, blkn, esize int) {
	atomic.AddInt64(&e.activeEstimateBytes, int64(esize))
	atomic.AddInt32(&e.activeMergeBlkCount, int32(blkn))
	e.taskConsume.Lock()
	if e.taskConsume.m == nil {
		e.taskConsume.m = make(activeTaskStats)
	}
	e.taskConsume.m[taskId] = struct {
		blk      int
		estBytes int
	}{blkn, esize}
	e.taskConsume.Unlock()
}

func (e *MergeExecutor) OnExecDone(v any) {
	task := v.(tasks.MScopedTask)

	e.taskConsume.Lock()
	stat := e.taskConsume.m[task.ID()]
	delete(e.taskConsume.m, task.ID())
	e.taskConsume.Unlock()

	atomic.AddInt32(&e.activeMergeBlkCount, -int32(stat.blk))
	atomic.AddInt64(&e.activeEstimateBytes, -int64(stat.estBytes))
}

// TODO: remove manually merge on dn
func (e *MergeExecutor) ManuallyExecute(entry *catalog.TableEntry, objs []*catalog.ObjectEntry) error {
	mem := e.MemAvailBytes()
	if mem > constMaxMemCap {
		mem = constMaxMemCap
	}
	osize, esize, _ := estimateMergeConsume(objs)
	if esize > 2*mem/3 {
		return moerr.NewInternalErrorNoCtx("no enough mem to merge. osize %d, mem %d", osize, mem)
	}

	objCnt := len(objs)

	scopes := make([]common.ID, objCnt)
	for i, obj := range objs {
		scopes[i] = *obj.AsCommonID()
	}

	factory := func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return jobs.NewMergeObjectsTask(ctx, txn, objs, e.rt, false) //TODO
	}
	task, err := e.rt.Scheduler.ScheduleMultiScopedTxnTask(tasks.WaitableCtx, tasks.DataCompactionTask, scopes, factory)
	if err == tasks.ErrScheduleScopeConflict {
		return moerr.NewInternalErrorNoCtx("conflict with running merging jobs, try later")
	} else if err != nil {
		return moerr.NewInternalErrorNoCtx("schedule error: %v", err)
	}

	blkn := 0
	for _, obj := range objs {
		blkn += obj.BlockCnt()
	}
	logMergeTask(entry.GetLastestSchemaLocked(false).Name, task.ID(), objs, blkn, osize, esize)
	if err = task.WaitDone(context.Background()); err != nil {
		return moerr.NewInternalErrorNoCtx("merge error: %v", err)
	}
	return nil
}

func (e *MergeExecutor) ExecuteFor(entry *catalog.TableEntry, policy Policy) {
	e.tableName = fmt.Sprintf("%v-%v", entry.ID, entry.GetLastestSchema(false).Name)

	mobjs, kind := policy.Revise(int64(e.cpuPercent), int64(e.MemAvailBytes()))
	if len(mobjs) < 2 {
		return
	}

	if ActiveCNObj.CheckOverlapOnCNActive(mobjs) {
		return
	}

	if kind == TaskHostCN {
		osize, esize, _ := estimateMergeConsume(mobjs)
		blkCnt := 0
		for _, obj := range mobjs {
			blkCnt += obj.BlockCnt()
		}
		stats := make([][]byte, 0, len(mobjs))
		cids := make([]common.ID, 0, len(mobjs))
		for _, obj := range mobjs {
			stat := obj.GetObjectStats()
			stats = append(stats, stat.Clone().Marshal())
			cids = append(cids, *obj.AsCommonID())
		}
		if e.rt.Scheduler.CheckAsyncScopes(cids) != nil {
			return
		}
		schema := entry.GetLastestSchema(false)
		cntask := &api.MergeTaskEntry{
			AccountId:         schema.AcInfo.TenantID,
			UserId:            schema.AcInfo.UserID,
			RoleId:            schema.AcInfo.RoleID,
			TblId:             entry.ID,
			DbId:              entry.GetDB().GetID(),
			TableName:         entry.GetLastestSchema(false).Name,
			DbName:            entry.GetDB().GetName(),
			ToMergeObjs:       stats,
			EstimatedMemUsage: uint64(esize),
		}
		if err := e.cnSched.SendMergeTask(context.TODO(), cntask); err == nil {
			ActiveCNObj.AddActiveCNObj(mobjs)
			logMergeTask(e.tableName, math.MaxUint64, mobjs, blkCnt, osize, esize)
		} else {
			logutil.Warnf("mergeblocks send to cn error: %v", err)
			return
		}
		entry.Stats.AddMerge(osize, len(mobjs), blkCnt)
	} else {
		objScopes := make([]common.ID, 0)
		tombstoneScopes := make([]common.ID, 0)
		objs := make([]*catalog.ObjectEntry, 0)
		tombstones := make([]*catalog.ObjectEntry, 0)
		objectBlkCnt := 0
		tombstoneBlkCnt := 0
		for _, obj := range mobjs {
			if obj.IsTombstone {
				tombstoneBlkCnt += obj.BlockCnt()
				tombstones = append(tombstones, obj)
				tombstoneScopes = append(tombstoneScopes, *obj.AsCommonID())
			} else {
				objectBlkCnt += obj.BlockCnt()
				objs = append(objs, obj)
				objScopes = append(objScopes, *obj.AsCommonID())
			}
		}

		e.scheduleMergeObjects(objScopes, objs, objectBlkCnt, entry, false)
		e.scheduleMergeObjects(tombstoneScopes, tombstones, tombstoneBlkCnt, entry, true)
	}

}
func (e *MergeExecutor) scheduleMergeObjects(scopes []common.ID, mobjs []*catalog.ObjectEntry, blkCnt int, entry *catalog.TableEntry, isTombstone bool) {
	osize, esize, _ := estimateMergeConsume(mobjs)
	factory := func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return jobs.NewMergeObjectsTask(ctx, txn, mobjs, e.rt, isTombstone)
	}
	task, err := e.rt.Scheduler.ScheduleMultiScopedTxnTask(nil, tasks.DataCompactionTask, scopes, factory)
	if err != nil {
		if err != tasks.ErrScheduleScopeConflict {
			logutil.Infof("[Mergeblocks] Schedule error info=%v", err)
		}
		return
	}
	e.AddActiveTask(task.ID(), blkCnt, esize)
	task.AddObserver(e)
	logMergeTask(e.tableName, task.ID(), mobjs, blkCnt, osize, esize)
	entry.Stats.AddMerge(osize, len(mobjs), blkCnt)

}
func (e *MergeExecutor) MemAvailBytes() int {
	merging := int(atomic.LoadInt64(&e.activeEstimateBytes))
	avail := e.memAvail - e.memSpare - merging
	if avail < 0 {
		avail = 0
	}
	return avail
}

func logMergeTask(name string, taskId uint64, merges []*catalog.ObjectEntry, blkn, osize, esize int) {
	rows := 0
	infoBuf := &bytes.Buffer{}
	for _, obj := range merges {
		r := obj.GetRemainingRows()
		rows += r
		infoBuf.WriteString(fmt.Sprintf(" %d(%s)", r, common.ShortObjId(obj.ID)))
	}
	platform := fmt.Sprintf("t%d", taskId)
	if taskId == math.MaxUint64 {
		platform = "CN"
		v2.TaskCNMergeScheduledByCounter.Inc()
		v2.TaskCNMergedBlocksCounter.Add(float64(blkn))
		v2.TaskCNMergedSizeCounter.Add(float64(osize))
	} else {
		v2.TaskDNMergeScheduledByCounter.Inc()
		v2.TaskDNMergedBlocksCounter.Add(float64(blkn))
		v2.TaskDNMergedSizeCounter.Add(float64(osize))
	}
	logutil.Infof(
		"[Mergeblocks] Scheduled %v [%v|on%d,bn%d|%s,%s], merged(%v): %s", name,
		platform, len(merges), blkn,
		common.HumanReadableBytes(osize), common.HumanReadableBytes(esize),
		rows,
		infoBuf.String(),
	)
}
