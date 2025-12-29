# IVF Entries 表大小预估方法说明

## 概述

在 IVF entries 表执行类型优化中，表大小的预估是关键决策因素之一。本文档详细说明表大小是如何预估的。

## 预估方法

### 1. 主要方法：使用 SizeMap（更准确）

**位置**: `pkg/sql/plan/stats.go:1592-1597`

```go
// Calculate estimated size in MB from SizeMap (more accurate)
var totalSizeMB uint64
for _, v := range s.SizeMap {
    totalSizeMB += v
}
stats.EstimatedSizeMb = float64(totalSizeMB) / (1024 * 1024)
```

**原理**:
- `SizeMap` 是一个 `map[string]uint64`，键是列名，值是该列的**持久化字节数**
- 通过累加所有列的大小，得到表的**总持久化大小**
- 除以 `1024 * 1024` 转换为 MB

**SizeMap 的数据来源**:

1. **收集阶段** (`pkg/vm/engine/disttae/stats.go:671-672`):
   ```go
   info.ColumnSize[idx] = int64(meta.BlockHeader().ZoneMapArea().Length() +
       meta.BlockHeader().BFExtent().Length() + objColMeta.Location().Length())
   ```
   - `ZoneMapArea().Length()`: ZoneMap 区域的长度
   - `BFExtent().Length()`: Bloom Filter 扩展区域的长度  
   - `objColMeta.Location().Length()`: 列数据的实际存储位置长度

2. **更新到 StatsInfo** (`pkg/sql/plan/stats.go:311`):
   ```go
   s.SizeMap[colName] = uint64(info.ColumnSize[i])
   ```

**SizeMap 的含义**:
- `SizeMap[columnName]` 存储的是该列在整个表中的**近似持久化字节数**
- 这是从存储层（TAE）收集的实际数据大小统计
- 包含了列数据、ZoneMap、Bloom Filter 等元数据的存储大小

### 2. 后备方法：使用 Rowsize * TableCnt

**位置**: `pkg/sql/plan/stats.go:1599-1601`

```go
// Fallback: use Rowsize * TableCnt if SizeMap is empty
if stats.EstimatedSizeMb == 0 && stats.TableCnt > 0 {
    stats.EstimatedSizeMb = (stats.Rowsize * stats.TableCnt) / (1024 * 1024)
}
```

**使用场景**:
- 当 `SizeMap` 为空或所有值为 0 时使用
- 作为后备方案，确保即使没有详细的列大小信息也能估算

**Rowsize 的计算** (`pkg/sql/plan/stats.go:1571-1582`):
```go
// estimate average row size from collected table stats: sum(SizeMap)/TableCnt
// SizeMap stores approximate persisted bytes per column; divide by total rows to get bytes/row
{
    var totalSize uint64
    for _, v := range s.SizeMap {
        totalSize += v
    }
    if stats.TableCnt > 0 {
        stats.Rowsize = float64(totalSize) / stats.TableCnt
    } else {
        stats.Rowsize = 0
    }
}
```

**原理**:
- `Rowsize` = `sum(SizeMap) / TableCnt`，即平均每行的字节数
- `EstimatedSizeMb` = `Rowsize * TableCnt / (1024 * 1024)`
- 实际上等价于 `sum(SizeMap) / (1024 * 1024)`，与主要方法结果相同

## 数据流

```
存储层 (TAE)
    ↓
收集列大小信息 (disttae/stats.go)
    - ZoneMapArea 大小
    - BFExtent 大小  
    - 列数据 Location 大小
    ↓
InfoFromZoneMap.ColumnSize[]
    ↓
UpdateStatsInfo() → StatsInfo.SizeMap[columnName]
    ↓
calcScanStats() → plan.Stats.EstimatedSizeMb
    ↓
GetExecType() 使用 EstimatedSizeMb 进行决策
```

## 示例

假设一个 IVF entries 表有 3 列，SizeMap 如下：

```go
SizeMap = {
    "id":     50 * 1024 * 1024,   // 50MB
    "version": 10 * 1024 * 1024,   // 10MB
    "data":    40 * 1024 * 1024,   // 40MB
}
```

**计算过程**:
1. `totalSizeMB = 50MB + 10MB + 40MB = 100MB`
2. `EstimatedSizeMb = 100.0` MB

**决策**:
- `EstimatedSizeMb >= 100.0` → 满足最小阈值，可以触发优化
- `EstimatedSizeMb >= 500.0` → 满足 MultiCN 阈值，使用多 CN 执行

## 注意事项

1. **SizeMap 的准确性**:
   - SizeMap 来自存储层的实际统计，相对准确
   - 包含了元数据（ZoneMap、Bloom Filter）的大小
   - 反映的是**持久化存储大小**，不是内存大小

2. **后备方案**:
   - 当 SizeMap 不可用时，使用 `Rowsize * TableCnt`
   - 这确保了即使统计信息不完整也能进行估算

3. **单位转换**:
   - 所有大小计算最终转换为 MB（除以 `1024 * 1024`）
   - 与阈值比较时使用相同的单位

4. **实时性**:
   - SizeMap 来自统计缓存，可能不是最新的
   - 但对于执行类型决策，近似值已经足够

## 相关代码位置

| 组件 | 文件 | 行号 | 说明 |
|------|------|------|------|
| 列大小收集 | `pkg/vm/engine/disttae/stats.go` | 671-672 | 从存储层收集列大小 |
| 更新到 StatsInfo | `pkg/sql/plan/stats.go` | 311 | 填充 SizeMap |
| 计算 Rowsize | `pkg/sql/plan/stats.go` | 1571-1582 | 从 SizeMap 计算平均行大小 |
| 计算 EstimatedSizeMb | `pkg/sql/plan/stats.go` | 1592-1601 | 预估表大小（MB） |
| 使用预估大小 | `pkg/sql/plan/stats.go` | 2001, 2006 | 在 GetExecType 中使用 |

---

**最后更新**: 2025-01-XX
