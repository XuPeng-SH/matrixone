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

package model

import (
	"fmt"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type TransferTable struct {
	sync.RWMutex
	ttl   time.Duration
	pages map[common.ID]*common.PinnedItem[*TransferPage]
}

type TransferPage struct {
	common.RefHelper
	bornTS  time.Time
	src     *common.ID
	dest    *common.ID
	offsets containers.Vector
}

func NewTransferTable(ttl time.Duration) *TransferTable {
	return &TransferTable{
		ttl:   ttl,
		pages: make(map[common.ID]*common.PinnedItem[*TransferPage]),
	}
}

func (table *TransferTable) Pin(id common.ID) (pinned *common.PinnedItem[*TransferPage], err error) {
	table.RLock()
	defer table.RUnlock()
	var found bool
	if pinned, found = table.pages[id]; !found {
		err = moerr.GetOkExpectedEOB()
	} else {
		pinned = pinned.Item().Pin()
	}
	return
}
func (table *TransferTable) Len() int {
	table.RLock()
	defer table.RUnlock()
	return len(table.pages)
}
func (table *TransferTable) prepareTTL(now time.Time) (items []*common.PinnedItem[*TransferPage]) {
	table.RLock()
	defer table.RUnlock()
	for _, page := range table.pages {
		if page.Item().TTL(now, table.ttl) {
			items = append(items, page)
		}
	}
	return
}

func (table *TransferTable) executeTTL(items []*common.PinnedItem[*TransferPage]) {
	if len(items) == 0 {
		return
	}
	table.Lock()
	for _, pinned := range items {
		delete(table.pages, *pinned.Item().GetSrc())
	}
	table.Unlock()
	for _, pinned := range items {
		pinned.Close()
	}
}

func (table *TransferTable) RunTTL(now time.Time) {
	items := table.prepareTTL(now)
	table.executeTTL(items)
}

func (table *TransferTable) AddPage(page *TransferPage) (dup bool) {
	pinned := page.Pin()
	defer func() {
		if dup {
			pinned.Close()
		}
	}()
	table.Lock()
	defer table.Unlock()
	id := *page.GetSrc()
	if _, found := table.pages[id]; found {
		dup = true
		return
	}
	table.pages[id] = pinned
	return
}

func (table *TransferTable) Close() {
	table.Lock()
	defer table.Unlock()
	for _, item := range table.pages {
		item.Close()
	}
	table.pages = make(map[common.ID]*common.PinnedItem[*TransferPage])
}

func NewRowIDVector() containers.Vector {
	return containers.MakeVector(
		types.T_Rowid.ToType(),
		false,
		&containers.Options{
			Allocator: common.CacheAllocator,
		})
}

func NewTransferPage(
	bornTS time.Time,
	src, dest *common.ID,
	offsets containers.Vector) *TransferPage {
	page := &TransferPage{
		bornTS:  bornTS,
		src:     src,
		dest:    dest,
		offsets: offsets,
	}
	page.OnZeroCB = page.Close
	return page
}

func (page *TransferPage) Close() {
	logutil.Infof("Closing %s", page.String())
	if page.offsets != nil {
		page.offsets.Close()
		page.offsets = nil
	} else {
		panic(moerr.NewInternalError("page was closed more than once"))
	}
}

func (page *TransferPage) GetBornTS() time.Time { return page.bornTS }
func (page *TransferPage) GetSrc() *common.ID   { return page.src }
func (page *TransferPage) GetDest() *common.ID  { return page.dest }

func (page *TransferPage) TTL(now time.Time, ttl time.Duration) bool {
	return now.After(page.bornTS.Add(ttl))
}

func (page *TransferPage) String() string {
	return fmt.Sprintf("page[%s->%s][%s][Len=%d]",
		page.src.BlockString(),
		page.dest.BlockString(),
		page.bornTS.String(),
		page.offsets.Length())
}

func (page *TransferPage) Pin() *common.PinnedItem[*TransferPage] {
	page.Ref()
	return &common.PinnedItem[*TransferPage]{
		Val: page,
	}
}

func (page *TransferPage) TransferOne(srcOff uint32) types.Rowid {
	return page.offsets.Get(int(srcOff)).(types.Rowid)
}

func (page *TransferPage) TransferMany(srcOffs ...uint32) (dest containers.Vector) {
	dest = NewRowIDVector()
	slice := page.offsets.Slice().([]types.Rowid)
	for _, off := range srcOffs {
		dest.Append(slice[off])
	}
	return
}
