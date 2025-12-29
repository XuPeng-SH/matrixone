# IVF Entries 优化性能开销分析

## 问题 1: 这个过程重不重（性能开销）？

### 答案：**非常轻量，几乎没有额外开销**

### 原因分析：

#### 1. **复用已有逻辑，无额外数据收集**

我们添加的代码**完全复用**了已有的统计信息收集机制：

**已有逻辑**（计算 Rowsize）:
```go
// pkg/sql/plan/stats.go:1571-1582
var totalSize uint64
for _, v := range s.SizeMap {
    totalSize += v
}
stats.Rowsize = float64(totalSize) / stats.TableCnt
```

**新增逻辑**（计算 EstimatedSizeMb）:
```go
// pkg/sql/plan/stats.go:1592-1597
var totalSizeMB uint64
for _, v := range s.SizeMap {
    totalSizeMB += v
}
stats.EstimatedSizeMb = float64(totalSizeMB) / (1024 * 1024)
```

**关键点**:
- `SizeMap` 已经存在，用于计算 `Rowsize`
- 我们只是**复用同一个 SizeMap**，做一次简单的循环累加
- **没有额外的存储层访问**，没有额外的 I/O 操作

#### 2. **数据已缓存，无需实时计算**

```go
// pkg/sql/plan/stats.go:1522
s, err := builder.compCtx.Stats(node.ObjRef, scanSnapshot)
```

- `StatsInfo`（包含 `SizeMap`）来自**统计信息缓存**
- 缓存由存储层维护，在表统计信息更新时刷新
- 查询计划阶段直接使用缓存，**无运行时开销**

#### 3. **计算复杂度极低**

- **时间复杂度**: O(n)，n = 列数（通常 < 100）
- **空间复杂度**: O(1)，只是累加一个 uint64
- **操作**: 简单的循环累加和除法

#### 4. **执行时机：查询计划阶段**

- 在**查询计划阶段**（compile time）执行，不在运行时（runtime）
- 只执行一次，结果缓存在 `plan.Stats` 中
- 不影响查询执行性能

### 性能开销对比

| 操作 | 开销 | 说明 |
|------|------|------|
| 存储层收集 SizeMap | **已有** | 原本就存在，用于计算 Rowsize |
| 循环累加 SizeMap | **O(n)** | n = 列数，通常 < 100，微秒级 |
| 除法运算 | **O(1)** | 单次浮点除法，纳秒级 |
| 缓存访问 | **已有** | StatsInfo 已缓存，无额外 I/O |

**结论**: 新增代码的开销可以忽略不计，主要是：
- 一次简单的循环（复用已有数据）
- 一次除法运算
- 无额外存储访问、无额外 I/O

---

## 问题 2: 所有查询都会这样，还是只有 IVF search entries 表会这样？

### 答案：**SizeMap 收集对所有表都进行，但优化逻辑只用于 IVF entries 表**

### 详细说明：

#### 1. **SizeMap 收集：所有表都会进行**

**调用链**:
```
所有 TABLE_SCAN 节点
  ↓
calcScanStats()  // pkg/sql/plan/stats.go:1500
  ↓
builder.compCtx.Stats()  // 获取 StatsInfo（包含 SizeMap）
  ↓
计算 Rowsize（已有逻辑）
  ↓
计算 EstimatedSizeMb（新增逻辑）
```

**适用范围**:
- ✅ **所有表**的扫描节点都会调用 `calcScanStats()`
- ✅ **所有表**都会计算 `EstimatedSizeMb`（即使不使用）
- ✅ **所有表**的 `SizeMap` 都会被收集（原本就存在，用于计算 Rowsize）

**为什么所有表都计算？**
- `calcScanStats()` 是通用的统计信息计算函数
- `EstimatedSizeMb` 的计算非常简单（复用 SizeMap）
- 即使不使用，计算开销也极小（可忽略）
- 保持代码一致性，未来可能用于其他优化

#### 2. **优化逻辑：只用于 IVF entries 表**

**决策逻辑** (`pkg/sql/plan/stats.go:1994-2016`):
```go
if node.NodeType == plan.Node_TABLE_SCAN &&
    (node.TableDef.TableType == catalog.SystemSI_IVFFLAT_TblType_Entries || 
     node.TableDef.TableType == catalog.Hnsw_TblType_Storage) {
    
    // NEW: Enhanced optimization for IVF entries table
    if node.TableDef.TableType == catalog.SystemSI_IVFFLAT_TblType_Entries &&
        isIvfIndexQuery &&
        stats.ObjectNumber > IvfEntriesMinObjectCount &&
        stats.EstimatedSizeMb >= IvfEntriesMinSizeMB {
        // 使用新的优化逻辑
    } else {
        // 使用现有的 BlockNum 逻辑
    }
}
```

**条件检查**:
1. ✅ 必须是 `TABLE_SCAN` 节点
2. ✅ 表类型必须是 `SystemSI_IVFFLAT_TblType_Entries`（IVF entries 表）
3. ✅ 必须是 IVF 索引查询（`isIvfIndexQuery == true`）
4. ✅ 对象数量 > 2
5. ✅ 预估大小 >= 100MB

**只有同时满足所有条件，才会使用新的优化逻辑**

#### 3. **其他表的行为**

| 表类型 | SizeMap 收集 | EstimatedSizeMb 计算 | 优化逻辑使用 |
|--------|-------------|-------------------|------------|
| IVF entries 表 | ✅ | ✅ | ✅（满足条件时） |
| HNSW 表 | ✅ | ✅ | ❌（使用现有逻辑） |
| 普通表 | ✅ | ✅ | ❌（不使用） |
| 内部表 | ❌ | ❌ | ❌（跳过统计） |

### 总结

1. **性能开销**:
   - ✅ **极轻量**：复用已有数据，简单循环累加
   - ✅ **无额外 I/O**：使用缓存数据
   - ✅ **计划阶段执行**：不影响运行时性能

2. **适用范围**:
   - ✅ **SizeMap 收集**：所有表（原本就存在）
   - ✅ **EstimatedSizeMb 计算**：所有表（开销极小）
   - ✅ **优化逻辑使用**：**仅 IVF entries 表**（满足条件时）

3. **设计考虑**:
   - 保持代码通用性，未来可扩展
   - 计算开销极小，对所有表计算也无影响
   - 优化逻辑严格限制，避免误用

---

**最后更新**: 2025-01-XX
