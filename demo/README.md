# AI Dataset Demo - Git for Data with MatrixOne

这个演示展示了 MatrixOne 的 **Git for Data** 能力，结合 AI 数据管道的完整工作流程。

## 🎯 演示目标

- **Time Travel**: 数据级版本控制，像 Git 分支/提交一样管理变更历史
- **AI 数据管道**: 集成 AI 模型标注和人类审核流程
- **向量搜索**: 支持 KNN 搜索相似 embeddings
- **可重现性**: 确保 AI 数据管道的完整追踪和回溯

## 🏗️ 架构设计

### 数据表结构

```sql
CREATE TABLE ai_dataset (
    id INT PRIMARY KEY,
    features vec32(128),  -- 128 维向量，用于 AI embeddings
    label VARCHAR(50) DEFAULT 'unlabeled',  -- 初始标签
    metadata JSON,  -- 标注元数据（如标注者、置信度）
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- 记录更新时间
);
```

### 工作流程

1. **数据初始化**: 生成指定行数的模拟向量数据
2. **AI 模型标注**: 批量标注数据，记录置信度
3. **人类审核**: 纠正 AI 模型的错误标注
4. **版本控制**: 每次更新都保留历史记录
5. **时间旅行**: 查询任意时间点的数据状态
6. **向量搜索**: 基于相似度的数据检索

## 🚀 快速开始

### 前置条件

1. MatrixOne 服务正在运行
2. Go 1.19+ 环境
3. MySQL 客户端（用于连接测试）

### 运行演示

#### 1. 使用默认配置
```bash
# 启动 MatrixOne
./mo-service -cfg etc/launch/launch.toml

# 运行演示
cd demo
make run
```

#### 2. 使用环境变量
```bash
# 设置环境变量
export MO_HOST=192.168.1.100
export MO_PORT=6001
export MO_USER=root
export MO_PASSWORD=111
export MO_DATABASE=test

# 运行演示
make run
```

#### 3. 使用命令行参数
```bash
# 直接指定参数
./ai_dataset_demo -host 192.168.1.100 -port 6001 -user root -password 111 -database test

# 使用完整 DSN
./ai_dataset_demo -dsn "root:111@tcp(192.168.1.100:6001)/test"
```

#### 4. 使用 Makefile 变量
```bash
# 指定 host 运行
make run-host HOST=192.168.1.100 PORT=6001 USER=root PASSWORD=111 DATABASE=test
```

### 交互式模式

```bash
# 默认配置
make interactive

# 指定 host
make interactive-host HOST=192.168.1.100 PORT=6001

# 命令行参数
./ai_dataset_demo -interactive -host 192.168.1.100 -port 6001
```

## 📊 演示功能

### 1. 数据生成

```go
// 生成 1000 行模拟数据
demo.MockData(1000)
```

### 2. AI 模型标注

```go
aiAnnotations := []AnnotationResult{
    {ID: 1, Label: "cat", Confidence: 0.95, Annotator: "AI_model_v1"},
    {ID: 2, Label: "dog", Confidence: 0.85, Annotator: "AI_model_v1"},
    // ...
}
demo.AIModelAnnotation("AI_model_v1", aiAnnotations)
```

### 3. 人类审核

```go
humanAnnotations := []AnnotationResult{
    {ID: 2, Label: "wolf", Reason: "corrected from dog - AI misidentified"},
    // ...
}
demo.HumanAnnotation(humanAnnotations)
```

### 4. 数据时间点比较

```go
// 比较两个时间点的数据差异
demo.CompareTimePoints("2025-09-09 13:06:20", "2025-09-09 13:06:24")
```

**详细模式输出示例**:
```
🔄 RECORD MODIFIED - ID: 1
   📍 Time Points: 2025-09-09 13:06:20 → 2025-09-09 13:06:24
   🔄 Label: 'unlabeled' → 'cat'
   🔄 Annotator: 'N/A' → 'AI_model_v1'
   🔄 Confidence: 'N/A' → '0.95'
   📋 Metadata Details:
      Time 1: Annotator='N/A', Confidence='N/A', Reason='N/A'
      Time 2: Annotator='AI_model_v1', Confidence='0.95', Reason='N/A'
   ⏰ Timestamps: 2025-09-09 13:06:20 → 2025-09-09 13:06:24
```

**统计模式输出**:
- 📊 新增/删除/修改记录数量
- 🏷️ 标签分布变化统计
- 📈 快速概览，无详细记录

### 5. 快照管理

```go
// 创建快照
demo.CreateSnapshot("initial")

// 查看所有快照
demo.ShowSnapshots()

// 删除快照
demo.DropSnapshot("ai_dataset_20250909_143022_initial")

// 比较两个快照
demo.CompareSnapshots("snapshot1", "snapshot2")
```

**快照命名规则**:
- 格式: `ai_dataset_YYYYMMDD_HHMMSS_suffix`
- 示例: `ai_dataset_20250909_143022_initial`
- 用户只需提供后缀，系统自动生成完整名称

### 6. 时间旅行查询

```sql
-- 查询特定时间点的数据状态
-- 使用 MatrixOne 的 Time Travel 语法
SELECT * FROM ai_dataset {MO_TS=1757424004000000000};
```

**时间格式转换**:
- 用户输入: `2025-09-09 13:20:04`
- 系统转换: `1757424004000000000` (MatrixOne TS 物理时间戳)
- 执行查询: `SELECT * FROM ai_dataset {MO_TS=1757424004000000000}`

### 6. 数据时间点比较

```sql
-- 比较两个时间点的数据差异
-- 时间点 1: 2025-09-09 13:06:20
-- 时间点 2: 2025-09-09 13:06:24

-- 查询时间点 1 的数据
SELECT id, label, 
       JSON_EXTRACT(metadata, '$.annotator') as annotator,
       JSON_EXTRACT(metadata, '$.confidence') as confidence
FROM ai_dataset {MO_TS=1757424004000000000};

-- 查询时间点 2 的数据  
SELECT id, label, 
       JSON_EXTRACT(metadata, '$.annotator') as annotator,
       JSON_EXTRACT(metadata, '$.confidence') as confidence
FROM ai_dataset {MO_TS=1757424004000000000};
```

**比较功能特性**:

**📋 详细模式 (默认)**:
- 🔍 逐记录详细差异分析
- 📝 具体变化: Label 'A' → 'B', Annotator 'X' → 'Y'
- 📋 完整 metadata 信息 (annotator, confidence, reason)
- ⏰ 每个变化的时间戳
- 🆔 显示具体的主键 ID

**📊 统计模式**:
- 📈 变化统计概览
- 🏷️ 标签分布变化
- 📋 新增/删除/修改记录数量
- 🔄 快速概览，无详细记录

### 7. 快照管理

```sql
-- 创建快照
CREATE SNAPSHOT ai_dataset_20250909_143022_initial FOR TABLE test ai_dataset;

-- 查看所有快照
SHOW SNAPSHOTS;

-- 删除快照
DROP SNAPSHOT ai_dataset_20250909_143022_initial;

-- 查询快照数据
SELECT * FROM ai_dataset {Snapshot = "ai_dataset_20250909_143022_initial"};
```

**快照功能特性**:
- 📸 自动命名: `ai_dataset_YYYYMMDD_HHMMSS_suffix`
- 🔄 快照比较: 类似时间戳比较，支持详细和统计模式
- 📋 版本管理: 数据管道的版本控制
- 🗑️ 快照清理: 删除不需要的快照

### 8. 向量相似度搜索

```sql
-- 查找与 ID=1 最相似的 5 条记录
SELECT id, label, L2_DISTANCE(features, query_vector) as distance
FROM ai_dataset 
WHERE id != 1
ORDER BY distance 
LIMIT 5;
```

## 🔍 SQL 查询示例

### 快照管理查询

```sql
-- 创建快照
CREATE SNAPSHOT ai_dataset_20250909_143022_initial FOR TABLE test ai_dataset;

-- 查看所有快照
SHOW SNAPSHOTS;

-- 查询快照数据
SELECT id, label, 
       JSON_EXTRACT(metadata, '$.annotator') as annotator,
       JSON_EXTRACT(metadata, '$.confidence') as confidence
FROM ai_dataset {Snapshot = "ai_dataset_20250909_143022_initial"};

-- 删除快照
DROP SNAPSHOT ai_dataset_20250909_143022_initial;
```

### 查看当前数据状态

```sql
SELECT id, label, 
       JSON_EXTRACT(metadata, '$.annotator') as annotator,
       JSON_EXTRACT(metadata, '$.confidence') as confidence,
       timestamp
FROM ai_dataset 
ORDER BY id;
```

### 统计标注情况

```sql
SELECT 
    JSON_EXTRACT(metadata, '$.annotator') as annotator,
    COUNT(*) as count,
    AVG(JSON_EXTRACT(metadata, '$.confidence')) as avg_confidence
FROM ai_dataset 
WHERE label != 'unlabeled'
GROUP BY JSON_EXTRACT(metadata, '$.annotator');
```

### 查找 AI 模型标注的记录

```sql
SELECT * FROM ai_dataset 
WHERE JSON_EXTRACT(metadata, '$.annotator') = 'AI_model_v1';
```

### 查找人类纠正的记录

```sql
SELECT * FROM ai_dataset 
WHERE JSON_EXTRACT(metadata, '$.annotator') = 'human_reviewer';
```

## 🎨 自定义演示

### 修改数据量

```go
// 在 main.go 中修改
demo.MockData(500)  // 生成 500 行数据
```

### 添加新的标注模型

```go
// 添加新的 AI 模型标注
newAnnotations := []AnnotationResult{
    {ID: 10, Label: "elephant", Confidence: 0.92, Annotator: "AI_model_v2"},
    {ID: 11, Label: "lion", Confidence: 0.88, Annotator: "AI_model_v2"},
}
demo.AIModelAnnotation("AI_model_v2", newAnnotations)
```

### 批量人类标注

```go
// 批量人类审核
batchAnnotations := []AnnotationResult{
    {ID: 6, Label: "tiger", Reason: "corrected classification"},
    {ID: 7, Label: "leopard", Reason: "corrected classification"},
    {ID: 8, Label: "cheetah", Reason: "corrected classification"},
}
demo.HumanAnnotation(batchAnnotations)
```

## 🔧 配置选项

### 配置选项

#### 环境变量
- `MO_HOST`: MatrixOne 主机地址 (默认: 127.0.0.1)
- `MO_PORT`: MatrixOne 端口 (默认: 6001)
- `MO_USER`: 数据库用户名 (默认: root)
- `MO_PASSWORD`: 数据库密码 (默认: 111)
- `MO_DATABASE`: 数据库名称 (默认: test)
- `MO_DSN`: 完整的数据库连接字符串 (覆盖其他选项)

#### 命令行参数
- `-host`: MatrixOne 主机地址
- `-port`: MatrixOne 端口
- `-user`: 数据库用户名
- `-password`: 数据库密码
- `-database`: 数据库名称
- `-dsn`: 完整的 DSN 连接字符串
- `-interactive`: 运行交互式模式

### 数据库配置

确保 MatrixOne 配置支持：
- 向量数据类型 (`vec32`)
- JSON 数据类型
- Time Travel 功能（如果可用）

## 📈 性能优化

### 批量操作

- 使用批量插入减少网络往返
- 批量更新提高标注效率

### 索引建议

```sql
-- 为常用查询创建索引
CREATE INDEX idx_label ON ai_dataset(label);
CREATE INDEX idx_timestamp ON ai_dataset(timestamp);
CREATE INDEX idx_annotator ON ai_dataset((JSON_EXTRACT(metadata, '$.annotator')));
```

## 🐛 故障排除

### 常见问题

1. **连接失败**
   ```
   Error: failed to connect to database
   ```
   - 检查 MatrixOne 是否运行
   - 验证连接字符串和端口

2. **向量类型不支持**
   ```
   Error: unknown column type 'vec32'
   ```
   - 确认 MatrixOne 版本支持向量类型
   - 检查数据库配置

3. **Time Travel 查询失败**
   ```
   Warning: Time Travel query failed
   ```
   - 这是正常现象，功能可能未启用
   - 演示会回退到当前状态查询

### 调试模式

```bash
# 启用详细日志
export MO_LOG_LEVEL=debug
./ai_dataset_demo
```

## 🤝 贡献

欢迎提交 Issue 和 Pull Request 来改进这个演示！

### 开发指南

1. Fork 项目
2. 创建功能分支
3. 提交更改
4. 创建 Pull Request

## 📄 许可证

本项目遵循 MatrixOne 的许可证条款。
