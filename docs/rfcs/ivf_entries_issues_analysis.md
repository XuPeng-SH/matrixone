# IVF Entries 优化问题分析

## 问题 1: IndexReaderParam.OrderBy 为空

### 问题描述
从日志看到：`has OrderBy: false (len=0), has Limit: true`

### 根本原因分析

查看 `apply_indices_ivfflat.go:319-323`：
```go
tableFuncNode.IndexReaderParam = &plan.IndexReaderParam{
    Limit:        limitExpr,
    OrigFuncName: ivfCtx.origFuncName,
    DistRange:    distRange,
    // 注意：这里没有设置 OrderBy！
}
```

**OrderBy 实际上是在后面创建的 SORT 节点中**（`apply_indices_ivfflat.go:564-585`）：
```go
orderByScore := []*OrderBySpec{
    {
        Expr: &plan.Expr{
            Typ: tableFuncNode.TableDef.Cols[1].Typ, // score column
            Expr: &plan.Expr_Col{
                Col: &plan.ColRef{
                    RelPos: tableFuncTag,
                    ColPos: 1, // score column
                },
            },
        },
        Flag: vecCtx.sortDirection,
    },
}

sortByID := builder.appendNode(&plan.Node{
    NodeType: plan.Node_SORT,
    Children: []int32{joinRootID},
    OrderBy:  orderByScore,  // OrderBy 在这里！
    Limit:    limit,
}, ctx)
```

### 解决方案

已修改 `hasIvfIndexQueryInPlan()` 函数，不仅检查 `IndexReaderParam.OrderBy`，还检查查询中是否有 SORT 节点。

---

## 问题 2: EstimatedSizeMb 只有 93.47MB，但实际 objects 超过 2GB

### 问题描述
- 日志显示：`EstimatedSizeMb=93.47`
- 实际：entries 表的所有 objects 加起来超过 2GB

### 根本原因分析

查看 `pkg/vm/engine/disttae/stats.go:671-672` 和 `718`：
```go
// 第一次对象
info.ColumnSize[idx] = int64(meta.BlockHeader().ZoneMapArea().Length() +
    meta.BlockHeader().BFExtent().Length() + objColMeta.Location().Length())

// 后续对象
info.ColumnSize[idx] += int64(objColMeta.Location().Length())
```

**问题**：
- `Location().Length()` 可能只是**压缩后的大小**或**元数据大小**，不是实际数据大小
- 对于 entries 表，它存储的是**向量数据**，每个向量可能很大（如 128 维 float32 = 512 字节）
- 但 `SizeMap` 统计的可能只是元数据/压缩大小，所以只有 93.47MB

### 正确的对象大小获取方式

查看 `pkg/vm/engine/disttae/txn_table.go:476-477`：
```go
CompressSize: int64(obj.ObjectStats.Size()),      // 压缩后大小
OriginSize:   int64(obj.ObjectStats.OriginSize()), // 原始大小（未压缩）
```

**应该使用 `obj.ObjectStats.OriginSize()` 来获取对象的实际大小！**

### 解决方案

需要修改 `calcScanStats()` 或 `updateInfoFromZoneMap()`，对于 IVF entries 表：
1. 使用 `obj.ObjectStats.OriginSize()` 而不是 `Location().Length()`
2. 或者累加所有对象的 `ObjectStats.OriginSize()` 来计算总大小

### 当前 SizeMap 的含义

`SizeMap[columnName]` 当前统计的是：
- ZoneMap 区域大小
- BloomFilter 区域大小  
- **列数据的 Location().Length()**（可能是压缩大小或元数据大小）

**对于 entries 表，这不是实际存储的向量数据大小！**

### 建议的修复

1. **方案 A**：在 `updateInfoFromZoneMap()` 中，对于 entries 表，使用 `obj.ObjectStats.OriginSize()` 累加
2. **方案 B**：在 `calcScanStats()` 中，对于 entries 表，从 `StatsInfo` 获取对象的实际大小（如果有的话）
3. **方案 C**：使用 `ObjectNumber * 平均对象大小` 来估算（如果知道平均对象大小）

---

## 下一步行动

1. ✅ **已修复**：OrderBy 检测问题（通过检查 SORT 节点）
2. ⏳ **待修复**：EstimatedSizeMb 计算问题（需要使用对象的实际大小）

需要查看：
- `obj.ObjectStats.OriginSize()` 是否可以在 `updateInfoFromZoneMap()` 中获取
- 或者是否有其他方式获取 entries 表的实际总大小
