# IVF Entries Table Execution Type Optimization Proposal

**Author:** Development Team  
**Date:** 2025-01-XX  
**Status:** Proposal  
**Related Issues:** N/A

---

## 1. Executive Summary

This proposal optimizes the execution type decision (TP/AP_ONECN/AP_MULTICN) for IVF (Inverted File) entries table queries by considering actual data volume (object count and estimated size) instead of relying solely on block selectivity estimates. This improves query performance for large-scale vector search operations.

---

## 2. Background

### 2.1 Current Implementation

The execution type for IVF entries tables is determined in `GetExecType()` (`pkg/sql/plan/stats.go:1917-1959`):

```go
func GetExecType(qry *plan.Query, txnHaveDDL bool, isPrepare bool) ExecType {
    if GetForceScanOnMultiCN() {
        return ExecTypeAP_MULTICN
    }
    ret := ExecTypeTP
    for _, node := range qry.GetNodes() {
        // ... general logic ...
        
        // Vector index table specific logic
        if node.NodeType == plan.Node_TABLE_SCAN &&
            (node.TableDef.TableType == catalog.SystemSI_IVFFLAT_TblType_Entries || 
             node.TableDef.TableType == catalog.Hnsw_TblType_Storage) &&
            stats.Rowsize > RowSizeThreshold &&
            stats.BlockNum > LargeBlockThresholdForOneCN {
            ret = ExecTypeAP_ONECN
            if stats.BlockNum > LargeBlockThresholdForMultiCN {
                ret = ExecTypeAP_MULTICN
            }
        }
    }
    return ret
}
```

**Current Thresholds** (`pkg/sql/plan/stats.go:45-61`):

| Constant | Value | Description |
|----------|-------|-------------|
| `blockThresholdForTpQuery` | 32 | General TP query threshold |
| `BlockThresholdForOneCN` | 512 | General AP_ONECN threshold |
| `costThresholdForOneCN` | 160000 | Cost threshold for AP_ONECN |
| `costThresholdForTpQuery` | 240000 | Cost threshold for TP query |
| `RowSizeThreshold` | 128 | Row size threshold for vector tables |
| `LargeBlockThresholdForOneCN` | 4 | Vector table AP_ONECN threshold |
| `LargeBlockThresholdForMultiCN` | 32 | Vector table AP_MULTICN threshold |

### 2.2 Problem Statement

The current approach has limitations:

1. **Conservative Block Selectivity**: Due to sort order caps in `estimateFilterBlockSelectivity()`, the estimated `BlockNum` is often underestimated:
   - First PK column: capped at 0.2 selectivity
   - Second PK column: capped at 0.5 selectivity
   - Example: A table with 65 blocks may estimate `BlockNum = 14`

2. **Insufficient Data Volume Consideration**: The decision doesn't consider:
   - Actual object count (`AccurateObjectNumber`/`ApproxObjectNumber` from `StatsInfo`)
   - Total estimated size (`SizeMap` from `StatsInfo`)
   - Whether the query is an IVF index search (has `IndexReaderParam` with `OrderBy` and `Limit`)

3. **Performance Impact**: Large IVF entries tables may not utilize multi-CN execution, leading to:
   - Slower query execution
   - Underutilization of distributed resources
   - Poor scalability for large-scale vector search

### 2.3 IVF Query Structure

IVF index queries have a specific structure after `applyIndicesForFiltersUsingIVFFlatIndex()`:

```
FUNCTION_SCAN (ivf_search table function)
  └── IndexReaderParam:
        ├── Limit: LIMIT N expression
        ├── OrderBy: ORDER BY distance expressions
        └── DistRange: distance range filter
```

The `IndexReaderParam` is set in `apply_indices_ivfflat.go:319`:
```go
tableFuncNode.IndexReaderParam = &plan.IndexReaderParam{
    Limit:        limitExpr,
    OrigFuncName: ivfCtx.origFuncName,
    DistRange:    distRange,
}
```

---

## 3. Requirements

### 3.1 Functional Requirements

1. **IVF Index Query Detection**
   - Detect queries using IVF index entries table
   - Verify query has `ORDER BY ... LIMIT` clause (indicating vector search)
   - Check: `node.IndexReaderParam != nil && len(node.IndexReaderParam.OrderBy) > 0 && node.IndexReaderParam.Limit != nil`

2. **Object Count Threshold**
   - Check if entries table has more than 2 objects
   - Use `statsInfo.AccurateObjectNumber > 2` or `statsInfo.ApproxObjectNumber > 2`

3. **Size Threshold**
   - Check if estimated total size >= 100MB
   - Calculate from `SizeMap` in `StatsInfo` or `stats.Rowsize * stats.TableCnt`

4. **Execution Type Decision**
   - If all conditions met: Use `ExecTypeAP_ONECN` minimum
   - Consider `ExecTypeAP_MULTICN` for very large tables (>10 objects or >500MB)
   - Otherwise: Follow existing logic

### 3.2 Non-Functional Requirements

1. **Backward Compatibility**: Must not break existing queries
2. **Performance**: Optimization check should be lightweight
3. **Maintainability**: Code should be clear and well-documented

---

## 4. Detailed Design

### 4.1 Architecture Overview

```
GetExecType()
    ↓
For each node in query:
    ↓
Check if IVF entries table scan (Node_TABLE_SCAN + SystemSI_IVFFLAT_TblType_Entries)
    ↓
Check if related FUNCTION_SCAN has IndexReaderParam with OrderBy + Limit
    ↓
Get statistics (ObjectNumber, Size) from StatsInfo
    ↓
Apply new optimization logic
    ↓
Return execution type
```

### 4.2 Implementation Details

#### 4.2.1 New Constants

```go
const (
    // IVF entries optimization thresholds
    IvfEntriesMinObjectCount     = 2      // Minimum object count to trigger optimization
    IvfEntriesMinSizeMB          = 100.0  // Minimum size in MB to trigger optimization
    IvfEntriesMultiCNObjectCount = 10     // Object count threshold for multi-CN
    IvfEntriesMultiCNSizeMB      = 500.0  // Size threshold for multi-CN
)
```

#### 4.2.2 Helper Function: Check IVF Index Query

```go
// hasIvfIndexQueryInPlan checks if the query contains an IVF index search
// with ORDER BY and LIMIT clause
func hasIvfIndexQueryInPlan(qry *plan.Query) bool {
    for _, node := range qry.GetNodes() {
        if node.NodeType == plan.Node_FUNCTION_SCAN &&
            node.IndexReaderParam != nil &&
            len(node.IndexReaderParam.OrderBy) > 0 &&
            node.IndexReaderParam.Limit != nil {
            return true
        }
    }
    return false
}
```

#### 4.2.3 Helper Function: Get IVF Entries Statistics

```go
// getIvfEntriesStats retrieves object count and size for IVF entries table
// Returns: (objectNumber, estimatedSizeMB, ok)
func getIvfEntriesStats(node *plan.Node, s *pb.StatsInfo) (int64, float64, bool) {
    if s == nil {
        return 0, 0, false
    }
    
    // Get object number (prefer AccurateObjectNumber, fallback to ApproxObjectNumber)
    objectNumber := s.AccurateObjectNumber
    if objectNumber == 0 {
        objectNumber = s.ApproxObjectNumber
    }
    
    // Calculate estimated size from SizeMap
    var totalSize uint64
    for _, size := range s.SizeMap {
        totalSize += size
    }
    estimatedSizeMB := float64(totalSize) / (1024 * 1024)
    
    // Fallback: use Rowsize * TableCnt if SizeMap is empty
    if estimatedSizeMB == 0 && node.Stats != nil && node.Stats.TableCnt > 0 {
        estimatedSizeMB = (node.Stats.Rowsize * node.Stats.TableCnt) / (1024 * 1024)
    }
    
    return objectNumber, estimatedSizeMB, true
}
```

#### 4.2.4 Modified GetExecType Function

**Option A: No Signature Change (Preferred)**

Store statistics in node during `calcScanStats()` and use them in `GetExecType()`:

```go
func GetExecType(qry *plan.Query, txnHaveDDL bool, isPrepare bool) ExecType {
    if GetForceScanOnMultiCN() {
        return ExecTypeAP_MULTICN
    }
    ret := ExecTypeTP
    
    // Check if this is an IVF index query
    isIvfIndexQuery := hasIvfIndexQueryInPlan(qry)
    
    for _, node := range qry.GetNodes() {
        switch node.NodeType {
        case plan.Node_RECURSIVE_CTE, plan.Node_RECURSIVE_SCAN:
            ret = ExecTypeAP_ONECN
        }
        stats := node.Stats
        if stats == nil || stats.BlockNum > int32(BlockThresholdForOneCN) && stats.Cost > float64(costThresholdForOneCN) {
            if txnHaveDDL {
                return ExecTypeAP_ONECN
            } else {
                return ExecTypeAP_MULTICN
            }
        }
        if isPrepare {
            if stats.BlockNum > blockThresholdForTpQuery*4 || stats.Cost > costThresholdForTpQuery*4 {
                ret = ExecTypeAP_ONECN
            }
        } else {
            if stats.BlockNum > blockThresholdForTpQuery || stats.Cost > costThresholdForTpQuery {
                ret = ExecTypeAP_ONECN
            }
        }
        
        // Vector index table specific logic
        if node.NodeType == plan.Node_TABLE_SCAN &&
            (node.TableDef.TableType == catalog.SystemSI_IVFFLAT_TblType_Entries || 
             node.TableDef.TableType == catalog.Hnsw_TblType_Storage) {
            
            // NEW: Enhanced optimization for IVF index queries
            if isIvfIndexQuery && stats.ObjectNumber > IvfEntriesMinObjectCount && 
               stats.EstimatedSizeMB >= IvfEntriesMinSizeMB {
                ret = ExecTypeAP_ONECN
                if stats.ObjectNumber > IvfEntriesMultiCNObjectCount || 
                   stats.EstimatedSizeMB >= IvfEntriesMultiCNSizeMB {
                    ret = ExecTypeAP_MULTICN
                }
            } else if stats.Rowsize > RowSizeThreshold && 
                      stats.BlockNum > LargeBlockThresholdForOneCN {
                // Existing logic as fallback
                ret = ExecTypeAP_ONECN
                if stats.BlockNum > LargeBlockThresholdForMultiCN {
                    ret = ExecTypeAP_MULTICN
                }
            }
        }
        
        if node.NodeType != plan.Node_TABLE_SCAN && stats.HashmapStats != nil && stats.HashmapStats.Shuffle {
            ret = ExecTypeAP_ONECN
        }
    }
    return ret
}
```

**Required Changes to plan.Stats proto:**

Add new fields to `plan.Stats`:
```protobuf
message Stats {
    // ... existing fields ...
    int64 ObjectNumber = 15;      // Object count from StatsInfo
    double EstimatedSizeMB = 16;  // Estimated size in MB
}
```

**Required Changes to calcScanStats():**

```go
func calcScanStats(node *plan.Node, builder *QueryBuilder) *plan.Stats {
    // ... existing code ...
    
    s, err := builder.compCtx.Stats(node.ObjRef, scanSnapshot)
    if err != nil || s == nil {
        return DefaultStats()
    }

    stats := new(plan.Stats)
    stats.TableCnt = s.TableCnt
    
    // NEW: Store object count and size for IVF optimization
    stats.ObjectNumber = s.AccurateObjectNumber
    if stats.ObjectNumber == 0 {
        stats.ObjectNumber = s.ApproxObjectNumber
    }
    
    // Calculate estimated size
    var totalSize uint64
    for _, v := range s.SizeMap {
        totalSize += v
    }
    stats.EstimatedSizeMB = float64(totalSize) / (1024 * 1024)
    
    // ... rest of existing code ...
}
```

**Option B: With Signature Change**

If proto changes are not desired, pass `builder` to `GetExecType()`:

```go
func GetExecType(qry *plan.Query, txnHaveDDL bool, isPrepare bool, builder *QueryBuilder) ExecType
```

This requires updating the call site in `compile.go` and passing `nil` for backward compatibility.

### 4.3 Decision Matrix

| Condition | Object Count | Size (MB) | Execution Type |
|-----------|--------------|-----------|----------------|
| IVF query | > 10 | any | AP_MULTICN |
| IVF query | any | >= 500 | AP_MULTICN |
| IVF query | > 2 | >= 100 | AP_ONECN |
| IVF query | <= 2 | < 100 | Existing logic |
| Non-IVF | any | any | Existing logic |

---

## 5. Implementation Plan

### Phase 1: Core Implementation (Week 1)

1. **Proto Changes** (if Option A)
   - [ ] Add `ObjectNumber` and `EstimatedSizeMB` to `plan.Stats`
   - [ ] Regenerate proto files

2. **Stats Collection**
   - [ ] Modify `calcScanStats()` to populate new fields
   - [ ] Add helper function `hasIvfIndexQueryInPlan()`

3. **GetExecType Enhancement**
   - [ ] Add new constants for IVF thresholds
   - [ ] Implement enhanced optimization logic
   - [ ] Ensure backward compatibility

### Phase 2: Testing (Week 2)

1. **Unit Tests**
   - [ ] Test `hasIvfIndexQueryInPlan()`
   - [ ] Test `GetExecType` with new logic
   - [ ] Test edge cases

2. **Integration Tests**
   - [ ] Test with actual IVF index queries
   - [ ] Verify execution type is correctly determined

### Phase 3: Documentation (Week 3)

1. **Code Comments**
2. **Update developer guide if needed**

---

## 6. Testing Strategy

### 6.1 Unit Tests

```go
func TestHasIvfIndexQueryInPlan(t *testing.T) {
    tests := []struct {
        name     string
        query    *plan.Query
        expected bool
    }{
        {
            name: "FUNCTION_SCAN with OrderBy and Limit",
            query: &plan.Query{
                Nodes: []*plan.Node{{
                    NodeType: plan.Node_FUNCTION_SCAN,
                    IndexReaderParam: &plan.IndexReaderParam{
                        OrderBy: []*plan.OrderBySpec{{Expr: &plan.Expr{}}},
                        Limit:   &plan.Expr{},
                    },
                }},
            },
            expected: true,
        },
        {
            name: "FUNCTION_SCAN without OrderBy",
            query: &plan.Query{
                Nodes: []*plan.Node{{
                    NodeType:         plan.Node_FUNCTION_SCAN,
                    IndexReaderParam: &plan.IndexReaderParam{},
                }},
            },
            expected: false,
        },
        {
            name: "TABLE_SCAN only",
            query: &plan.Query{
                Nodes: []*plan.Node{{
                    NodeType: plan.Node_TABLE_SCAN,
                }},
            },
            expected: false,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got := hasIvfIndexQueryInPlan(tt.query)
            require.Equal(t, tt.expected, got)
        })
    }
}

func TestGetExecType_IvfEntriesOptimization(t *testing.T) {
    tests := []struct {
        name           string
        tableType      string
        objectNumber   int64
        estimatedSizeMB float64
        hasIvfQuery    bool
        expected       ExecType
    }{
        {
            name:           "Large IVF table with IVF query -> MultiCN",
            tableType:      catalog.SystemSI_IVFFLAT_TblType_Entries,
            objectNumber:   15,
            estimatedSizeMB: 600,
            hasIvfQuery:    true,
            expected:       ExecTypeAP_MULTICN,
        },
        {
            name:           "Medium IVF table with IVF query -> OneCN",
            tableType:      catalog.SystemSI_IVFFLAT_TblType_Entries,
            objectNumber:   5,
            estimatedSizeMB: 200,
            hasIvfQuery:    true,
            expected:       ExecTypeAP_ONECN,
        },
        {
            name:           "Small IVF table -> existing logic",
            tableType:      catalog.SystemSI_IVFFLAT_TblType_Entries,
            objectNumber:   1,
            estimatedSizeMB: 50,
            hasIvfQuery:    true,
            expected:       ExecTypeTP, // depends on existing logic
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            q := makeQueryWithIvfStats(tt.tableType, tt.objectNumber, tt.estimatedSizeMB, tt.hasIvfQuery)
            got := GetExecType(q, false, false)
            require.Equal(t, tt.expected, got)
        })
    }
}
```

### 6.2 Integration Tests

1. **Large IVF Entries Table**: Create IVF index with >10 objects, >500MB → verify `ExecTypeAP_MULTICN`
2. **Medium IVF Entries Table**: Create IVF index with 3-5 objects, 100-200MB → verify `ExecTypeAP_ONECN`
3. **Small IVF Entries Table**: Create IVF index with 1-2 objects, <100MB → verify existing logic

---

## 7. Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Breaking existing queries | Low | High | Comprehensive testing, fallback to existing logic |
| Performance overhead | Low | Medium | Statistics already cached in `calcScanStats()` |
| Incorrect execution type | Medium | Medium | Conservative thresholds, thorough testing |

### Rollback Plan

1. Revert proto changes (if any)
2. Remove new optimization logic
3. Restore original `GetExecType()` behavior

---

## 8. Alternatives Considered

### Alternative 1: Adjust Block Selectivity Caps

**Approach:** Increase sort order caps for IVF entries table

**Pros:** Simpler, no function signature change

**Cons:** Less accurate, may affect other tables

### Alternative 2: Configuration-Based Thresholds

**Approach:** Make thresholds configurable via session variables

**Pros:** Flexible, easy to tune

**Cons:** More complex, requires documentation

**Decision:** Proposed approach is preferred because it:
- Considers actual data volume
- Is specific to IVF index queries
- Maintains backward compatibility
- Provides better accuracy

---

## 9. Success Criteria

1. ✅ IVF index queries on large entries tables (>2 objects, >100MB) use AP execution
2. ✅ Multi-CN execution triggered for very large tables (>10 objects or >500MB)
3. ✅ No regression in existing query behavior
4. ✅ Performance improvement for large-scale vector search
5. ✅ Code is well-tested and documented

---

## 10. References

| Resource | Location |
|----------|----------|
| `GetExecType` function | `pkg/sql/plan/stats.go:1917` |
| `calcScanStats` function | `pkg/sql/plan/stats.go:1492` |
| IVF index application | `pkg/sql/plan/apply_indices_ivfflat.go` |
| Compile call site | `pkg/sql/compile/compile.go:856` |
| StatsInfo proto | `pkg/pb/statsinfo/statsinfo.pb.go:276` |
| Existing tests | `pkg/sql/plan/stats_test.go` |

---

## 11. Appendix

### 11.1 StatsInfo Structure

From `pkg/pb/statsinfo/statsinfo.pb.go`:

```go
type StatsInfo struct {
    NdvMap               map[string]float64
    MinValMap            map[string]float64
    MaxValMap            map[string]float64
    DataTypeMap          map[string]uint64
    NullCntMap           map[string]uint64
    SizeMap              map[string]uint64        // Column sizes
    ShuffleRangeMap      map[string]*ShuffleRange
    BlockNumber          int64
    AccurateObjectNumber int64                    // Accurate object count
    ApproxObjectNumber   int64                    // Approximate object count
    TableCnt             float64
    TableName            string
}
```

### 11.2 Example IVF Query Plan

```sql
-- Original query
SELECT id, content FROM documents
ORDER BY l2_distance(embedding, '[0.1,0.2,...]') ASC
LIMIT 10;

-- After IVF index application, plan contains:
-- 1. TABLE_SCAN on entries table (SystemSI_IVFFLAT_TblType_Entries)
-- 2. FUNCTION_SCAN with IndexReaderParam:
--    - Limit: 10 (or over-fetched value)
--    - OrderBy: l2_distance expression
--    - DistRange: optional distance filter
```

---

**End of Proposal**
