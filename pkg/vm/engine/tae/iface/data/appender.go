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

package data

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

// SharedAppender is used by a transaction to append data to shared in-memory aobjects.
// A transaction should get one SharedAppender per table and close it after use.
type SharedAppender interface {
	// PrepareAppend allocates row ranges and creates AppendNodes.
	// It may span multiple aobjects if needed.
	// Returns a list of appendCtx for later ApplyAppend.
	// The node parameter is the transaction's private anode.
	PrepareAppend(node interface{}) ([]AppendContext, error)

	// ApplyAppend writes data to the aobject.
	ApplyAppend(ctx AppendContext) error

	// Close releases all resources (Unref all aobjects).
	Close()
}

// AppendContext contains information for applying append.
type AppendContext interface {
	// GetNode returns the source node (anode).
	GetNode() interface{}

	// GetAppendNode returns the MVCC AppendNode.
	GetAppendNode() txnif.AppendNode

	// GetSrcOffset returns the source offset in anode.
	GetSrcOffset() uint32

	// GetCount returns the number of rows.
	GetCount() uint32

	// GetDestOffset returns the destination offset in aobject.
	GetDestOffset() uint32
}
