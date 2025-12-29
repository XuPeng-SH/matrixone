# IVF Entries 优化调试日志说明

## 日志前缀

所有调试日志使用前缀：`[IVF_OPT]`

## 日志位置

### 1. GetExecType 函数

**位置**: `pkg/sql/plan/stats.go:1996-2075`

**日志内容**:
- 函数入口：是否强制多CN
- IVF 查询检测结果
- 节点总数
- 向量索引表扫描检测
- IVF entries 表的优化条件检查
- 各个条件的值（ObjectNumber, EstimatedSizeMb）
- MultiCN 条件判断
- 最终决策结果

**示例日志**:
```
[IVF_OPT] GetExecType: isIvfIndexQuery=true, totalNodes=5
[IVF_OPT] GetExecType: Found vector index table scan, tableType=ivfflat_entries, tableName=...
[IVF_OPT] GetExecType: IVF entries table detected, checking optimization conditions...
[IVF_OPT] GetExecType: isIvfIndexQuery=true, ObjectNumber=15 (threshold=2), EstimatedSizeMb=600.00 (threshold=100.00)
[IVF_OPT] GetExecType: Basic optimization conditions met, checking multi-CN conditions...
[IVF_OPT] GetExecType: MultiCN check - ObjectNumber=15 > 10: true, EstimatedSizeMb=600.00 >= 500.00: true
[IVF_OPT] GetExecType: ✅ MultiCN optimization triggered! ObjectNumber=15, EstimatedSizeMb=600.00
[IVF_OPT] GetExecType: Final decision: 2 (TP=0, AP_ONECN=1, AP_MULTICN=2)
```

### 2. hasIvfIndexQueryInPlan 函数

**位置**: `pkg/sql/plan/stats.go:1607-1633`

**日志内容**:
- 查询是否为空
- 节点总数
- 每个 FUNCTION_SCAN 节点的检查
- IndexReaderParam 是否存在
- OrderBy 和 Limit 是否存在
- 是否检测到 IVF 索引查询

**示例日志**:
```
[IVF_OPT] hasIvfIndexQueryInPlan: checking 5 nodes
[IVF_OPT] hasIvfIndexQueryInPlan: node[2] is FUNCTION_SCAN, IndexReaderParam=true
[IVF_OPT] hasIvfIndexQueryInPlan: node[2] has OrderBy: true (len=1), has Limit: true
[IVF_OPT] hasIvfIndexQueryInPlan: ✅ IVF index query detected in node[2]
```

### 3. calcScanStats 函数

**位置**: `pkg/sql/plan/stats.go:1585-1603`

**日志内容**:
- 仅针对 IVF entries 表输出
- AccurateObjectNumber 和 ApproxObjectNumber 的值
- 最终的 ObjectNumber
- SizeMap 中每列的大小
- 预估大小计算（如果使用后备方法）
- 完整的统计信息摘要

**示例日志**:
```
[IVF_OPT] calcScanStats: IVF entries table - AccurateObjectNumber=15, ApproxObjectNumber=0, final ObjectNumber=15
[IVF_OPT] calcScanStats: SizeMap[id]=52428800 bytes
[IVF_OPT] calcScanStats: SizeMap[version]=10485760 bytes
[IVF_OPT] calcScanStats: SizeMap[data]=41943040 bytes
[IVF_OPT] calcScanStats: IVF entries table stats - ObjectNumber=15, EstimatedSizeMb=100.00, TableCnt=1000000.00, Rowsize=104.86, BlockNum=65
```

## 如何查看日志

### 1. 查看所有 IVF 优化相关日志

```bash
# 查看日志文件
grep "\[IVF_OPT\]" /path/to/log/file

# 实时查看
tail -f /path/to/log/file | grep "\[IVF_OPT\]"
```

### 2. 查看特定查询的日志

```bash
# 结合时间戳或其他标识
grep "\[IVF_OPT\]" /path/to/log/file | grep "your-query-id"
```

### 3. 查看关键决策点

```bash
# 查看最终决策
grep "\[IVF_OPT\].*Final decision" /path/to/log/file

# 查看 MultiCN 触发
grep "\[IVF_OPT\].*MultiCN optimization triggered" /path/to/log/file

# 查看条件检查
grep "\[IVF_OPT\].*checking optimization conditions" /path/to/log/file
```

## 调试检查清单

当查询没有使用多CN时，按以下顺序检查日志：

### 1. 检查是否检测到 IVF 查询
```
[IVF_OPT] hasIvfIndexQueryInPlan: ✅ IVF index query detected
```
- ❌ 如果没有这个日志，说明查询没有被识别为 IVF 索引查询
- 检查：是否有 FUNCTION_SCAN 节点，是否有 IndexReaderParam，是否有 OrderBy 和 Limit

### 2. 检查是否检测到 IVF entries 表
```
[IVF_OPT] GetExecType: Found vector index table scan, tableType=ivfflat_entries
```
- ❌ 如果没有这个日志，说明表类型不是 `SystemSI_IVFFLAT_TblType_Entries`
- 检查：表类型是否正确

### 3. 检查统计信息是否正确
```
[IVF_OPT] calcScanStats: IVF entries table stats - ObjectNumber=15, EstimatedSizeMb=600.00
```
- ❌ 如果 ObjectNumber=0 或 EstimatedSizeMb=0，说明统计信息未正确收集
- 检查：StatsInfo 是否正确返回，SizeMap 是否为空

### 4. 检查优化条件
```
[IVF_OPT] GetExecType: isIvfIndexQuery=true, ObjectNumber=15 (threshold=2), EstimatedSizeMb=600.00 (threshold=100.00)
```
- ❌ 如果条件不满足，会输出详细的失败原因
- 检查：每个条件是否满足

### 5. 检查 MultiCN 条件
```
[IVF_OPT] GetExecType: MultiCN check - ObjectNumber=15 > 10: true, EstimatedSizeMb=600.00 >= 500.00: true
```
- ❌ 如果都是 false，说明不满足 MultiCN 条件
- 检查：ObjectNumber 是否 > 10，EstimatedSizeMb 是否 >= 500

### 6. 检查最终决策
```
[IVF_OPT] GetExecType: Final decision: 2 (TP=0, AP_ONECN=1, AP_MULTICN=2)
```
- 0 = TP
- 1 = AP_ONECN
- 2 = AP_MULTICN

## 常见问题排查

### 问题1: isIvfIndexQuery=false

**可能原因**:
- 查询不是 IVF 索引查询
- FUNCTION_SCAN 节点没有 IndexReaderParam
- IndexReaderParam 缺少 OrderBy 或 Limit

**检查**:
```bash
grep "\[IVF_OPT\].*hasIvfIndexQueryInPlan" /path/to/log/file
```

### 问题2: ObjectNumber=0 或 EstimatedSizeMb=0

**可能原因**:
- StatsInfo 未正确返回
- SizeMap 为空
- 统计信息未更新

**检查**:
```bash
grep "\[IVF_OPT\].*calcScanStats.*IVF entries table stats" /path/to/log/file
```

### 问题3: 条件都满足但仍然是 TP

**可能原因**:
- 其他逻辑覆盖了决策（如 BlockNum 检查）
- 查询中有其他节点影响了决策

**检查**:
```bash
grep "\[IVF_OPT\].*GetExecType" /path/to/log/file | tail -20
```

---

**最后更新**: 2025-01-XX
