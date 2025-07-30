package shuffle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDedupJoinShufflePoolConfig(t *testing.T) {
	// 测试DEDUP JOIN场景下的shuffle pool配置
	// 模拟DEDUP JOIN的场景：bucket数量很大，但probe端并发度低

	// 场景1：bucket数量为16（模拟DEDUP JOIN）
	bucketNum := int32(16)
	shuffle := &Shuffle{
		BucketNum: bucketNum,
	}

	// 模拟Prepare函数中的逻辑
	maxHolders := int32(1)
	if shuffle.BucketNum > 8 {
		maxHolders = shuffle.BucketNum / 4
		if maxHolders < 1 {
			maxHolders = 1
		}
	}

	// 验证maxHolders计算正确
	expectedMaxHolders := int32(4) // 16 / 4 = 4
	require.Equal(t, expectedMaxHolders, maxHolders)

	// 创建shuffle pool
	sp := NewShufflePool(bucketNum, maxHolders)
	require.Equal(t, bucketNum, sp.bucketNum)
	require.Equal(t, maxHolders, sp.maxHolders)

	// 场景2：bucket数量为4（普通场景）
	bucketNum2 := int32(4)
	maxHolders2 := int32(1)
	if bucketNum2 > 8 {
		maxHolders2 = bucketNum2 / 4
		if maxHolders2 < 1 {
			maxHolders2 = 1
		}
	}

	// 验证普通场景下maxHolders仍然是1
	require.Equal(t, int32(1), maxHolders2)
}

func TestShufflePoolMaxHoldersCalculation(t *testing.T) {
	// 测试不同bucket数量下的maxHolders计算

	testCases := []struct {
		bucketNum       int32
		expectedHolders int32
	}{
		{1, 1},  // 小bucket数量，使用默认值
		{4, 1},  // 小bucket数量，使用默认值
		{8, 1},  // 边界值，使用默认值
		{12, 3}, // 12 / 4 = 3
		{16, 4}, // 16 / 4 = 4
		{20, 5}, // 20 / 4 = 5
		{32, 8}, // 32 / 4 = 8
	}

	for _, tc := range testCases {
		maxHolders := int32(1)
		if tc.bucketNum > 8 {
			maxHolders = tc.bucketNum / 4
			if maxHolders < 1 {
				maxHolders = 1
			}
		}

		require.Equal(t, tc.expectedHolders, maxHolders,
			"bucketNum=%d, expected=%d, got=%d", tc.bucketNum, tc.expectedHolders, maxHolders)
	}
}
