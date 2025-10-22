# MO_CTL Commands Documentation

This document describes all available `mo_ctl` commands in MatrixOne, their usage, parameters, and return values.

## Overview

The `mo_ctl` function is a control interface for managing and debugging MatrixOne services. It follows the format:

```sql
SELECT mo_ctl('<service>', '<command>', '<parameter>');
```

- `<service>`: Target service type - `'cn'` (Compute Node) or `'dn'` (Data Node/TN)
- `<command>`: Command name to execute
- `<parameter>`: Command-specific parameters (optional for some commands)

## Service Types

- **CN (Compute Node)**: Handles query processing and computation
- **DN (Data Node/TN)**: Handles data storage and transaction management

## Commands Reference

### 1. PING

**Description**: Tests connectivity to DN services.

**Service**: `dn`

**Usage**:
```sql
-- Ping all DN shards
SELECT mo_ctl('dn', 'ping', '');

-- Ping specific DN shard by ID
SELECT mo_ctl('dn', 'ping', '<shard_id>');
```

**Parameters**:
- Empty string: Ping all DN shards
- `<shard_id>`: Specific DN shard ID (uint64)

**Returns**: JSON object with ping responses from DN services.

---

### 2. FLUSH

**Description**: Flushes table data to disk.

**Service**: `dn`

**Usage**:
```sql
-- Flush by table ID
SELECT mo_ctl('dn', 'flush', '<table_id>');

-- Flush by database and table name
SELECT mo_ctl('dn', 'flush', '<db_name>.<table_name>');

-- Flush with specific account ID
SELECT mo_ctl('dn', 'flush', '<db_name>.<table_name>.<account_id>');
```

**Parameters**:
- `<table_id>`: Table ID (uint64)
- `<db_name>.<table_name>`: Database and table name
- `<db_name>.<table_name>.<account_id>`: With specific account ID

**Returns**: JSON object with flush operation result from DN.

---

### 3. CHECKPOINT

**Description**: Triggers a checkpoint operation on DN services.

**Service**: `dn`

**Usage**:
```sql
-- Immediate checkpoint
SELECT mo_ctl('dn', 'checkpoint', '');

-- Checkpoint with flush duration
SELECT mo_ctl('dn', 'checkpoint', '<duration>');
```

**Parameters**:
- Empty string: Immediate checkpoint
- `<duration>`: Flush duration (e.g., `'10s'`, `'1m'`, `'1h'`)

**Returns**: JSON object with checkpoint operation status.

---

### 4. GLOBALCHECKPOINT

**Description**: Triggers a global checkpoint across all DN services.

**Service**: `dn`

**Usage**:
```sql
-- Global checkpoint with default settings
SELECT mo_ctl('dn', 'globalcheckpoint', '');

-- Global checkpoint with flush duration and timeout
SELECT mo_ctl('dn', 'globalcheckpoint', '<flush_duration>;<timeout>');
```

**Parameters**:
- Empty string: Default settings
- `<flush_duration>;<timeout>`: Both as duration strings (e.g., `'10s;30s'`)

**Returns**: JSON object with global checkpoint result.

---

### 5. FORCEGC

**Description**: Forces garbage collection to free memory.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'forcegc', '');
```

**Parameters**: None (empty string)

**Returns**: JSON object with `"OK"` on success.

---

### 6. INSPECT

**Description**: Inspects DN internal state for debugging.

**Service**: `dn`

**Usage**:
```sql
SELECT mo_ctl('dn', 'inspect', '<operation>');
```

**Parameters**:
- `<operation>`: Inspection operation name (implementation-specific)

**Returns**: Console-formatted string with inspection results.

**Note**: Requires appropriate account privileges.

---

### 7. GETSNAPSHOT

**Description**: Gets the current snapshot timestamp.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'getsnapshot', '');
```

**Parameters**: None (empty string)

**Returns**: JSON object with current timestamp in debug string format.

---

### 8. USESNAPSHOT

**Description**: Sets a snapshot timestamp for subsequent transactions.

**Service**: `cn`

**Usage**:
```sql
-- Use current timestamp
SELECT mo_ctl('cn', 'usesnapshot', '');

-- Use specific timestamp
SELECT mo_ctl('cn', 'usesnapshot', '<timestamp>');
```

**Parameters**:
- Empty string: Use current timestamp
- `<timestamp>`: Specific timestamp string

**Returns**: JSON object with `"OK"` on success.

---

### 9. TASK

**Description**: Manages task framework and executes tasks.

**Service**: `cn`

**Usage**:
```sql
-- Disable task framework
SELECT mo_ctl('cn', 'task', 'disable');

-- Enable task framework
SELECT mo_ctl('cn', 'task', 'enable');

-- Get task table user
SELECT mo_ctl('cn', 'task', 'getuser');

-- Run specific task on CN
SELECT mo_ctl('cn', 'task', '<cn_uuid>:<task_name>');
```

**Parameters**:
- `'disable'`: Disable task framework
- `'enable'`: Enable task framework
- `'getuser'`: Get task table user info
- `<cn_uuid>:<task_name>`: Execute task (e.g., `'uuid:storageusage'`)

**Returns**: JSON object with task execution result.

---

### 10. LABEL

**Description**: Sets CN service labels for workload management.

**Service**: `cn`

**Usage**:
```sql
-- Set single label value
SELECT mo_ctl('cn', 'label', '<cn_uuid>:<key>:<value>');

-- Set multiple label values
SELECT mo_ctl('cn', 'label', '<cn_uuid>:<key>:[<v1>,<v2>,<v3>]');
```

**Parameters**:
- `<cn_uuid>:<key>:<value>`: Single label value
- `<cn_uuid>:<key>:[v1,v2,...]`: Multiple label values (comma-separated in brackets)

**Returns**: JSON object with `"OK"` on success.

---

### 11. WORKSTATE

**Description**: Sets CN service work state.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'workstate', '<cn_uuid>:<state>');
```

**Parameters**:
- `<cn_uuid>:<state>`: CN UUID and numeric state value

**Returns**: JSON object with `"OK"` on success.

---

### 12. SYNCCOMMIT

**Description**: Synchronizes commit timestamps across all CN services.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'synccommit', '');
```

**Parameters**: None (empty string)

**Returns**: JSON object with sync result and max commit timestamp.

---

### 13. ADDFAULTPOINT

**Description**: Adds a fault injection point for testing.

**Service**: `dn`

**Usage**:
```sql
SELECT mo_ctl('dn', 'addfaultpoint', '<fault_point_spec>');
```

**Parameters**:
- `<fault_point_spec>`: Fault point specification string

**Returns**: JSON object with operation result.

---

### 14. BACKUP

**Description**: Triggers a backup operation.

**Service**: `dn`

**Usage**:
```sql
SELECT mo_ctl('dn', 'backup', '<backup_params>');
```

**Parameters**:
- `<backup_params>`: Backup operation parameters

**Returns**: JSON object with backup operation status.

---

### 15. TRACESPAN

**Description**: Controls distributed tracing spans.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'tracespan', '<span_command>');
```

**Parameters**:
- `<span_command>`: Trace span control command

**Returns**: JSON object with tracing operation result.

---

### 16. COREDUMP

**Description**: Triggers a core dump for debugging.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'coredump', '<dump_params>');
```

**Parameters**:
- `<dump_params>`: Core dump parameters

**Returns**: JSON object with core dump operation result.

---

### 17. INTERCEPTCOMMIT

**Description**: Intercepts commit operations for testing.

**Service**: `dn`

**Usage**:
```sql
SELECT mo_ctl('dn', 'interceptcommit', '<intercept_spec>');
```

**Parameters**:
- `<intercept_spec>`: Intercept specification

**Returns**: JSON object with operation status.

---

### 18. MERGEOBJECTS

**Description**: Merges data objects to optimize storage.

**Service**: `cn`

**Usage**:
```sql
-- Merge specific objects by table ID
SELECT mo_ctl('cn', 'mergeobjects', 'o:<table_id>[.<account_id>]:<obj1>,<obj2>,...[:<target_size>]');

-- Merge table objects with filter
SELECT mo_ctl('cn', 'mergeobjects', 't:<db_name>.<table_name>[.<account_id>][:<filter>][:<target_size>]');
```

**Parameters**:
- Object merge: `o:<table_id>.<account_id>:<obj1>,<obj2>:100M`
- Table merge: `t:<db_name>.<table_name>[:filter][:target_size]`
  - `filter`: `'overlap'`, `'small'`, or `'small(size)'` (e.g., `'small(110M)'`)
  - `target_size`: Target object size (e.g., `'120M'`, `'1G'`)

**Returns**: Raw byte data with merge operation details (not JSON formatted).

**Note**: Cannot be executed within an explicit transaction.

---

### 19. DISKCLEANER

**Description**: Triggers disk cleanup operations.

**Service**: `dn`

**Usage**:
```sql
SELECT mo_ctl('dn', 'diskcleaner', '<cleanup_params>');
```

**Parameters**:
- `<cleanup_params>`: Cleanup operation parameters

**Returns**: JSON object with cleanup result.

---

### 20. GETPROTOCOLVERSION

**Description**: Gets the current RPC protocol version.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'getprotocolversion', '');
```

**Parameters**: None (empty string)

**Returns**: JSON object with protocol version information.

---

### 21. SETPROTOCOLVERSION

**Description**: Sets the RPC protocol version.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'setprotocolversion', '<version>');
```

**Parameters**:
- `<version>`: Protocol version number

**Returns**: JSON object with operation result.

---

### 22. REMOTEREMOTELLOCKTABLE

**Description**: Removes remote lock table entries.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'remoteremotellocktable', '<table_spec>');
```

**Parameters**:
- `<table_spec>`: Lock table specification

**Returns**: JSON object with operation status.

---

### 23. GETLATESTBIND

**Description**: Gets the latest bind information.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'getlatestbind', '<bind_params>');
```

**Parameters**:
- `<bind_params>`: Bind query parameters

**Returns**: JSON object with bind information.

---

### 24. UNSUBSCRIBE_TABLE

**Description**: Unsubscribes from a table's change notifications.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'unsubscribe_table', '<db_name>:<table_name>');
```

**Parameters**:
- `<db_name>:<table_name>`: Database and table name separated by colon

**Returns**: JSON object with unsubscribe result.

---

### 25. TXN-TRACE

**Description**: Controls transaction tracing.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'txn-trace', '<trace_command>');
```

**Parameters**:
- `<trace_command>`: Transaction trace control command

**Returns**: JSON object with tracing information.

---

### 26. RELOAD-AUTO-INCREMENT-CACHE

**Description**: Reloads the auto-increment cache.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'reload-auto-increment-cache', '<table_spec>');
```

**Parameters**:
- `<table_spec>`: Table specification for cache reload

**Returns**: JSON object with reload result.

---

### 27. READER

**Description**: Controls reader configuration on CN services.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'reader', '<reader_command>:<params>');
```

**Parameters**:
- `<reader_command>:<params>`: Reader control command and parameters

**Returns**: JSON object with reader configuration for all CN services.

**Note**: This command is distributed to all CN services in the cluster.

---

### 28. GET-TABLE-SHARDS

**Description**: Gets shard information for a table.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'get-table-shards', '<table_spec>');
```

**Parameters**:
- `<table_spec>`: Table specification

**Returns**: JSON object with table shard information.

---

### 29. MOTABLESTATS

**Description**: Gets statistics for MatrixOne internal tables.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'motablestats', '<stats_params>');
```

**Parameters**:
- `<stats_params>`: Statistics query parameters

**Returns**: JSON object with table statistics.

---

### 30. WORKSPACETHRESHOLD

**Description**: Manages workspace memory threshold settings.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'workspacethreshold', '<threshold_params>');
```

**Parameters**:
- `<threshold_params>`: Threshold configuration parameters

**Returns**: JSON object with threshold settings.

---

### 31. TABLE-EXTRA

**Description**: Gets extra information about a table.

**Service**: `cn`

**Usage**:
```sql
SELECT mo_ctl('cn', 'table-extra', '<table_spec>');
```

**Parameters**:
- `<table_spec>`: Table specification

**Returns**: JSON object with extra table information.

---

### 32. SHUFFLE_MONITOR

**Description**: Monitors and controls shuffle locality statistics for query optimization. This command helps analyze how well data is distributed across CN nodes and identify potential performance issues related to data shuffling.

**Service**: `cn`

**Usage**:
```sql
-- Enable statistics collection
SELECT mo_ctl('cn', 'shuffle_monitor', 'enable');

-- Query current statistics
SELECT mo_ctl('cn', 'shuffle_monitor', 'query');

-- Check if monitoring is enabled
SELECT mo_ctl('cn', 'shuffle_monitor', 'status');

-- Reset statistics to zero (without disabling)
SELECT mo_ctl('cn', 'shuffle_monitor', 'reset');

-- Disable statistics collection and clear data
SELECT mo_ctl('cn', 'shuffle_monitor', 'disable');
```

**Parameters**:
- `'enable'`: Enable statistics collection on all CN nodes
- `'query'`: Get current shuffle locality statistics from all CNs
- `'status'`: Check if collection is enabled/disabled
- `'reset'`: Reset statistics to zero (keeps monitoring enabled)
- `'disable'`: Disable statistics collection and clear all collected data

**When Statistics Are Collected**:
Statistics are only collected when:
1. Monitoring is enabled via the `enable` command
2. A query involves shuffle operations (multi-CN queries with data redistribution)
3. The query processor evaluates object locality during shuffle phase
4. Conditions met: `rsp != nil && rsp.CNCNT > 1 && rsp.Node != nil`

This means:
- **Low overhead**: Only queries that actually need shuffle are monitored
- **No single-CN impact**: Single-CN queries are not affected
- **Selective monitoring**: Only tables involved in shuffle operations are tracked

**Performance and Memory Impact**:

1. **Memory Usage**: Minimal (~100 bytes per CN node)
   - Only stores 8 int64 counters
   - No per-table or per-object metadata stored
   - Memory usage is constant regardless of table count

2. **Performance Impact**: Negligible
   - Simple atomic counter increments with read-write lock
   - No disk I/O or network calls
   - Overhead: ~10-50 nanoseconds per object check
   - Only active during shuffle evaluation phase

3. **Scalability**:
   - Linear relationship with shuffle operations, not table count
   - If 1000 objects need shuffle → 1000 counter increments
   - No performance degradation over time
   - Suitable for production use with high shuffle frequency

**Understanding the Results**:

The `query` command returns detailed statistics for each CN node. Here's how to interpret them:

**Example Output Structure**:
```json
{
  "method": "SHUFFLE_MONITOR",
  "result": {
    "cn-uuid-1": {
      "success": true,
      "message": "query success",
      "stats": {
        "range_local": 1500,
        "range_remote": 500,
        "hash_local": 3000,
        "hash_remote": 1000,
        "appendable_local": 200,
        "appendable_remote": 50,
        "no_shuffle": 100,
        "total": 6350,
        "enabled": true
      },
      "locality_info": {
        "range_locality_rate": "75.00%",
        "range_local_objects": 1500,
        "range_remote_objects": 500,
        "range_total_objects": 2000,
        "hash_locality_rate": "75.00%",
        "hash_local_objects": 3000,
        "hash_remote_objects": 1000,
        "hash_total_objects": 4000,
        "appendable_locality_rate": "80.00%",
        "appendable_local_objects": 200,
        "appendable_remote_objects": 50,
        "appendable_total_objects": 250,
        "overall_locality_rate": "75.20%",
        "total_local_objects": 4700,
        "total_remote_objects": 1550,
        "total_shuffled_objects": 6250,
        "no_shuffle_objects": 100,
        "total_objects_processed": 6350
      }
    },
    "cn-uuid-2": { ... }
  }
}
```

**Key Metrics Explained**:

1. **Range Shuffle Statistics** (`range_*`):
   - Objects shuffled using range partitioning (typically for sorted data)
   - High locality (>70%) is good - means data is well-distributed by range
   - Low locality (<50%) may indicate poor range distribution

2. **Hash Shuffle Statistics** (`hash_*`):
   - Objects shuffled using hash partitioning (typical for joins/aggregations)
   - Expected locality: ~1/N where N is number of CNs (e.g., 50% for 2 CNs)
   - Much higher locality suggests data skew

3. **Appendable Objects** (`appendable_*`):
   - Recently inserted data (not yet compacted)
   - Usually has high local locality (appendable objects prefer local CN)
   - Low values are normal for stable workloads

4. **Overall Locality Rate**:
   - **75%+ locality**: Excellent - most objects processed locally
   - **50-75% locality**: Good - balanced distribution
   - **30-50% locality**: Fair - consider data distribution optimization
   - **<30% locality**: Poor - significant shuffle overhead, investigate data skew

5. **No Shuffle Objects**:
   - Objects that don't require shuffling (single-CN queries, no redistribution needed)
   - High values indicate many single-CN queries

**What to Look For**:

1. **Low Overall Locality (<50%)**:
   - Problem: High network shuffle overhead
   - Action: Check data distribution, consider repartitioning

2. **Imbalanced CN Statistics**:
   - One CN has much higher total_objects than others
   - Problem: Data skew or hotspot
   - Action: Analyze table distribution keys

3. **High Remote Ratio for Appendable**:
   - Problem: Recent data not on local CN
   - Action: Check load balancer, review insert patterns

4. **Growing Total Without Locality Change**:
   - Normal: Indicates consistent shuffle patterns
   - If locality degrades over time: investigate data growth patterns

**Handling Multiple Tables**:

Since this monitors at the object level (not table level), statistics aggregate across all tables:

1. **For Detailed Table-Level Analysis**:
   - Run targeted queries on specific tables
   - Reset statistics before each test: `mo_ctl('cn', 'shuffle_monitor', 'reset')`
   - Run test query on specific table
   - Immediately query results: `mo_ctl('cn', 'shuffle_monitor', 'query')`

2. **For Overall System Health**:
   - Keep monitoring enabled continuously
   - Periodically query and record results
   - Look for trends over time

3. **Best Practices**:
   ```sql
   -- Test specific table
   SELECT mo_ctl('cn', 'shuffle_monitor', 'reset');
   SELECT * FROM db1.table1 WHERE ...; -- Your query
   SELECT mo_ctl('cn', 'shuffle_monitor', 'query');
   
   -- Test multiple tables sequentially
   SELECT mo_ctl('cn', 'shuffle_monitor', 'reset');
   SELECT * FROM db1.table1 JOIN db1.table2 ...;
   SELECT mo_ctl('cn', 'shuffle_monitor', 'query');
   ```

**Disable Behavior**:

When you run `mo_ctl('cn', 'shuffle_monitor', 'disable')`:
1. ✓ Statistics collection stops immediately
2. ✓ All collected data is cleared (reset to zero)
3. ✓ Memory is freed
4. ✓ No further overhead on queries

This ensures:
- Clean state when re-enabling
- No stale data confusion
- Zero overhead when disabled

**Handling Partial Failures (Important)**:

Since commands are distributed to all CN nodes, partial failures can occur:

**Scenario 1: Some CNs succeed, some fail (enable/disable/reset)**

Example output when 1 out of 3 CNs fails:
```json
{
  "method": "SHUFFLE_MONITOR",
  "result": {
    "summary": {
      "total_cns": 3,
      "success_count": 2,
      "failed_count": 1,
      "all_successful": false,
      "partial_success": true,
      "all_failed": false,
      "warning": "Command executed on 2/3 CNs. 1 CNs failed. Cluster state is inconsistent!"
    },
    "cn_details": {
      "cn-uuid-1": {
        "success": true,
        "message": "shuffle stats enabled",
        "enabled": true
      },
      "cn-uuid-2": {
        "success": true,
        "message": "shuffle stats enabled",
        "enabled": true
      },
      "cn-uuid-3": {
        "success": false,
        "message": "transfer failed: context deadline exceeded"
      }
    }
  }
}
```

**What this means**:
- ⚠️ **Inconsistent state**: Some CNs are enabled, others are not
- 📊 **Partial data**: `query` will show data only from enabled CNs
- 🔧 **Action required**: Retry the failed command or investigate the failure

**Scenario 2: Status command with inconsistent state**

```json
{
  "summary": {
    "total_cns": 3,
    "success_count": 3,
    "failed_count": 0,
    "all_successful": true
  },
  "cn_details": {
    "cn-uuid-1": { "enabled": true, ... },
    "cn-uuid-2": { "enabled": true, ... },
    "cn-uuid-3": { "enabled": false, ... }  // Inconsistent!
  }
}
```

**What to do**:
1. Check `summary.all_successful` first
2. If `partial_success: true`, check `warning` message
3. Review `cn_details` to see which CNs failed
4. Retry the command for consistency

**Scenario 3: Disable behavior with partial failures**

If `disable` partially fails:
```
- CN1: Disabled + data cleared ✓
- CN2: Disabled + data cleared ✓  
- CN3: Still enabled (command failed) ✗
```

**Consequences**:
- CN3 continues collecting statistics
- `query` will show data from CN3 only
- Creates confusion about monitoring state

**Recommended recovery**:
```sql
-- 1. Check current status
SELECT mo_ctl('cn', 'shuffle_monitor', 'status');

-- 2. If inconsistent, retry disable
SELECT mo_ctl('cn', 'shuffle_monitor', 'disable');

-- 3. Verify all CNs are disabled
SELECT mo_ctl('cn', 'shuffle_monitor', 'status');
```

**Best Practices for Consistency**:

1. **Always check summary**:
   ```sql
   -- Look for "all_successful": true in results
   SELECT mo_ctl('cn', 'shuffle_monitor', 'enable');
   ```

2. **Use status to verify**:
   ```sql
   -- After enable/disable, verify state
   SELECT mo_ctl('cn', 'shuffle_monitor', 'status');
   ```

3. **Retry on partial failure**:
   - If you see `partial_success: true`, retry the command
   - Individual CN operations are idempotent (safe to retry)

4. **Handle inconsistent states**:
   - For monitoring: Can still use data, but be aware some CNs may be missing
   - For disable: Retry until all CNs are disabled
   - For enable: Retry until all CNs are enabled

5. **Production deployment**:
   - Consider scripting with retry logic
   - Monitor for `partial_success` warnings
   - Alert on failed CN operations

**Example: Robust enable with verification**:
```sql
-- Enable
SELECT mo_ctl('cn', 'shuffle_monitor', 'enable');
-- Check result for "all_successful": true

-- Verify
SELECT mo_ctl('cn', 'shuffle_monitor', 'status');
-- All CNs should show "enabled": true

-- If inconsistent, retry
SELECT mo_ctl('cn', 'shuffle_monitor', 'enable');
```

**Why partial failures happen**:
- Network timeouts (default: 5 seconds)
- CN node temporarily unavailable
- RPC service issues
- CN node restarting

**Note**: This command is automatically distributed to all CN services in the cluster. The command returns results from all CNs, including both successes and failures, allowing you to detect and handle inconsistent cluster states.

---

## Return Value Format

Most commands return JSON-formatted results with the following structure:

```json
{
  "method": "<COMMAND_NAME>",
  "result": <command_specific_data>
}
```

**Exceptions**:
- `INSPECT`: Returns console-formatted string
- `MERGEOBJECTS`: Returns raw byte data

## Command Distribution

Some commands are automatically distributed to multiple services:

- **CN Commands**: Most CN commands execute on all CN services in the cluster
- **DN Commands**: DN commands can target specific shards or all shards depending on parameters

## Error Handling

If a command fails, the function returns an error message. Common error scenarios:

- Invalid service type
- Unsupported command
- Invalid parameter format
- Service unavailable
- Permission denied (for commands requiring privileges)
- Transaction context restrictions (e.g., MERGEOBJECTS)

## Notes

1. Command names are case-insensitive
2. Service type can be `'cn'`, `'CN'`, `'dn'`, or `'DN'`
3. Some commands require specific account privileges
4. Commands like MERGEOBJECTS cannot run inside explicit transactions
5. Most CN commands are distributed to all CN services automatically
6. Duration parameters accept formats like `'10s'`, `'1m'`, `'1h'`, etc.
7. Size parameters accept formats like `'1K'`, `'1M'`, `'1G'`, etc.

## Examples

```sql
-- Check DN connectivity
SELECT mo_ctl('dn', 'ping', '');

-- Force garbage collection on CN
SELECT mo_ctl('cn', 'forcegc', '');

-- Flush a specific table
SELECT mo_ctl('dn', 'flush', 'mydb.mytable');

-- Create checkpoint with 10 second flush duration
SELECT mo_ctl('dn', 'checkpoint', '10s');

-- Get current snapshot timestamp
SELECT mo_ctl('cn', 'getsnapshot', '');

-- Merge table objects with small filter
SELECT mo_ctl('cn', 'mergeobjects', 't:mydb.mytable:small:120M');

-- Get shuffle statistics
SELECT mo_ctl('cn', 'shuffle_monitor', 'get');

-- Enable shuffle monitoring
SELECT mo_ctl('cn', 'shuffle_monitor', 'enable');
```

## See Also

- Source code: `pkg/sql/plan/function/ctl/`
- Tests: `pkg/sql/plan/function/ctl/*_test.go`
- Protobuf definitions: `proto/query.proto`, `proto/api.proto`

