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

package tables

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	// "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type NodeT interface {
	common.IRef

	IsPersisted() bool

	Contains(
		ctx context.Context,
		keys containers.Vector,
		keysZM index.ZM,
		bf objectio.BloomFilter,
		txn txnif.TxnReader,
		isCommitting bool,
		mp *mpool.MPool,
	) (err error)
	GetDuplicatedRows(
		ctx context.Context,
		txn txnif.TxnReader,
		maxVisibleRow uint32,
		keys containers.Vector,
		keysZM index.ZM,
		rowIDs containers.Vector,
		bf objectio.BloomFilter,
		isCommitting bool,
		checkWWConflict bool,
		mp *mpool.MPool,
	) (err error)

	Rows() (uint32, error)

	GetRowByFilter(ctx context.Context, txn txnif.TxnReader, filter *handle.Filter, mp *mpool.MPool,
		vpool *containers.VectorPool) (bid uint16, row uint32, err error)
	CollectAppendInRange(
		start, end types.TS, withAborted bool, mp *mpool.MPool,
	) (batWithVer *containers.BatchWithVersion, err error)
}

type Node struct {
	NodeT
}

func NewNode(node NodeT) *Node {
	return &Node{
		NodeT: node,
	}
}

func (n *Node) MustMNode() *memoryNode {
	return n.NodeT.(*memoryNode)
}

func (n *Node) MustPNode() *persistedNode {
	return n.NodeT.(*persistedNode)
}
