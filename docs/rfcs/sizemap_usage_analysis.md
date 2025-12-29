# SizeMap 使用分析

## SizeMap 的定义

`SizeMap` 是 `StatsInfo` 中的一个字段，类型为 `map[string]uint64`，键是列名，值是该列的**持久化字节数**。

**位置**: `pkg/pb/statsinfo/statsinfo.proto` 和 `pkg/pb/statsinfo/statsinfo.pb.go`

```go
type StatsInfo struct {
    // ...
    SizeMap map[string]uint64  // 列名 -> 该列的持久化字节数
    // ...
}
```

## SizeMap 的填充

**位置**: `pkg/vm/engine/disttae/stats.go:671-672, 718` 和 `pkg/sql/plan/stats.go:311`

```go
// 在 updateInfoFromZoneMap() 中填充
info.ColumnSize[idx] = int64(meta.BlockHeader().ZoneMapArea().Length() +
    meta.BlockHeader().BFExtent().Length() + objColMeta.Location().Length())

// 在 UpdateStatsInfo() 中更新到 StatsInfo
s.SizeMap[colName] = uint64(info.ColumnSize[i])
```

**注意**：对于 IVF entries 表，`Location().Length()` 可能只是压缩后的大小或元数据大小，不是实际向量数据大小。已修复为使用 `obj.OriginSize()`。

## SizeMap 的使用场景

### 1. 计算平均行大小 (Rowsize)

**位置**: `pkg/sql/plan/stats.go:1571-1583`

```go
// estimate average row size from collected table stats: sum(SizeMap)/TableCnt
// SizeMap stores approximate persisted bytes per column; divide by total rows to get bytes/row
var totalSize uint64
for _, v := range s.SizeMap {
    totalSize += v
}
if stats.TableCnt > 0 {
    stats.Rowsize = float64(totalSize) / stats.TableCnt
}
```

**用途**：
- 估算每行的平均字节数
- 用于查询成本估算
- 用于判断是否使用向量索引（`RowSizeThreshold = 128`）

### 2. 计算表的总大小（用于 IVF 优化）

**位置**: `pkg/sql/plan/stats.go:1597-1605`

```go
// Calculate estimated size in MB from SizeMap (more accurate)
var totalSizeMB uint64
for colName, v := range s.SizeMap {
    totalSizeMB += v
}
stats.EstimatedSizeMb = float64(totalSizeMB) / (1024 * 1024)
```

**用途**：
- **IVF entries 表优化**：判断表大小是否 >= 100MB，决定是否使用 AP 执行
- 用于执行类型决策（TP/AP_ONECN/AP_MULTICN）

### 3. JOIN 成本估算

**位置**: `pkg/sql/plan/stats.go:1810-1823`

```go
if leftChild.NodeType == plan.Node_TABLE_SCAN && rightChild.NodeType == plan.Node_TABLE_SCAN {
    w1 := builder.getStatsInfoByTableID(leftChild.TableDef.TblId)
    w2 := builder.getStatsInfoByTableID(rightChild.TableDef.TblId)
    if w1 != nil && w2 != nil {
        var t1size, t2size uint64
        for _, v := range w1.Stats.SizeMap {
            t1size += v
        }
        factor1 = math.Pow(float64(t1size), 0.1)
        for _, v := range w2.Stats.SizeMap {
            t2size += v
        }
        factor2 = math.Pow(float64(t2size), 0.1)
    }
}
```

**用途**：
- 计算 JOIN 的成本因子
- 基于表大小调整 JOIN 策略

### 4. 表大小查询（用户可见）

**位置**: `pkg/vm/engine/disttae/txn_table.go:280-294`

```go
s, _ := tbl.Stats(ctx, true)
if s == nil {
    return szInPart, nil
}
if columnName == AllColumns {
    var ret uint64
    for _, z := range s.SizeMap {
        ret += z  // 累加所有列的大小
    }
    return ret + szInPart, nil
}
sz, ok := s.SizeMap[columnName]
if !ok {
    return 0, moerr.NewInvalidInputf(ctx, "bad input column name %v", columnName)
}
return sz + szInPart, nil
```

**用途**：
- 返回表的总大小（所有列）
- 返回特定列的大小
- 用于 `SHOW TABLE STATUS` 等查询

## SizeMap 的数据来源

### 收集阶段 (`pkg/vm/engine/disttae/stats.go`)

```go
// 第一次对象
info.ColumnSize[idx] = int64(meta.BlockHeader().ZoneMapArea().Length() +
    meta.BlockHeader().BFExtent().Length() + objColMeta.Location().Length())

// 后续对象（累加）
info.ColumnSize[idx] += int64(objColMeta.Location().Length())
```

**组成部分**：
1. **ZoneMapArea().Length()**: ZoneMap 区域的长度（元数据）
2. **BFExtent().Length()**: Bloom Filter 扩展区域的长度（元数据）
3. **Location().Length()**: 列数据的 Location 长度（可能是压缩大小或元数据大小）

**问题**：对于 IVF entries 表，`Location().Length()` 可能不是实际向量数据大小。

### 更新到 StatsInfo (`pkg/sql/plan/stats.go:311`)

```go
s.SizeMap[colName] = uint64(info.ColumnSize[i])
```

## 对于 IVF Entries 表的特殊处理

### 问题

- `Location().Length()` 可能只是压缩后的大小或元数据大小
- 实际向量数据可能远大于 `SizeMap` 中的值
- 导致 `EstimatedSizeMb` 被低估（如 93.47MB vs 实际 2GB+）

### 修复

**位置**: `pkg/vm/engine/disttae/stats.go:644-672, 718`

```go
// 对于 entries 表，使用对象的实际大小
if isEntriesTable {
    objActualSize = obj.OriginSize()  // 使用 OriginSize() 而不是 Location().Length()
    info.ColumnSize[idx] = int64(objActualSize) / int64(lenCols)
}
```

**原理**：
- `obj.OriginSize()` 返回对象的**原始大小**（未压缩）
- 将对象大小按列数平均分配到各列
- 这样 `SizeMap` 的总和会更接近实际数据大小

## 总结

| 用途 | 位置 | 说明 |
|------|------|------|
| 计算 Rowsize | `stats.go:1571-1583` | `Rowsize = sum(SizeMap) / TableCnt` |
| IVF 优化 | `stats.go:1597-1605` | `EstimatedSizeMb = sum(SizeMap) / (1024*1024)` |
| JOIN 成本 | `stats.go:1810-1823` | 基于表大小计算 JOIN 因子 |
| 表大小查询 | `txn_table.go:280-294` | 返回表或列的大小给用户 |

**关键点**：
- SizeMap 是**按列统计**的持久化字节数
- 对于普通表，`Location().Length()` 可能足够准确
- 对于 IVF entries 表（存储大量向量数据），需要使用 `OriginSize()` 获取实际大小

---

**最后更新**: 2025-01-XX
