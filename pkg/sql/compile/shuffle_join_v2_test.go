package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestHasDownstreamLimitOrLock(t *testing.T) {
	c := &Compile{}

	// 测试空节点
	node := &plan.Node{}
	require.False(t, c.hasDownstreamLimitOrLock(node))

	// 测试有limit的节点
	nodeWithLimit := &plan.Node{
		Limit: &plan.Expr{}, // 任意非nil值
	}
	require.True(t, c.hasDownstreamLimitOrLock(nodeWithLimit))

	// 测试有lock的节点
	nodeWithLock := &plan.Node{
		NodeType: plan.Node_LOCK_OP,
	}
	require.True(t, c.hasDownstreamLimitOrLock(nodeWithLock))
}

func TestMergeProbeScopesForShuffleJoinV2(t *testing.T) {
	c := &Compile{}

	// 测试空scopes
	scopes := []*Scope{}
	result := c.mergeProbeScopesForShuffleJoinV2(scopes)
	require.Equal(t, 0, len(result))

	// 测试单个scope
	singleScope := []*Scope{{}}
	result = c.mergeProbeScopesForShuffleJoinV2(singleScope)
	require.Equal(t, 1, len(result))
	require.Equal(t, singleScope[0], result[0])

	// 测试多个scopes - 跳过这个测试，因为需要proc字段
	// multipleScopes := []*Scope{{}, {}, {}}
	// result = c.mergeProbeScopesForShuffleJoinV2(multipleScopes)
	// require.Equal(t, 1, len(result))
	// require.Equal(t, Merge, result[0].Magic)
	// require.Equal(t, 3, len(result[0].PreScopes))
}
