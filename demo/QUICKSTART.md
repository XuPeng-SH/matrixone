# AI Dataset Demo - 快速开始指南

## 🚀 一键启动

### 默认配置
```bash
# 1. 确保 MatrixOne 正在运行
./mo-service -cfg etc/launch/launch.toml

# 2. 运行完整演示
cd demo
make run

# 3. 或者运行交互式演示
make interactive
```

### 自定义 Host 地址
```bash
# 使用环境变量
export MO_HOST=192.168.1.100
export MO_PORT=6001
make run

# 使用命令行参数
./ai_dataset_demo -host 192.168.1.100 -port 6001

# 使用 Makefile 变量
make run-host HOST=192.168.1.100 PORT=6001
```

## 📋 功能概览

### 🎯 核心功能
- **Git for Data**: Time Travel 查询，数据版本控制
- **AI 数据管道**: 自动化标注 + 人类审核
- **向量搜索**: KNN 相似度搜索
- **版本追踪**: 完整的标注历史记录

### 🛠️ 使用方式

#### 1. 完整演示模式
```bash
./ai_dataset_demo
```
自动运行完整的演示流程，包括：
- 创建数据表
- 生成 100 行模拟数据
- AI 模型标注
- 人类审核
- 显示结果统计

#### 2. 交互式模式
```bash
./ai_dataset_demo interactive
```
提供交互式菜单，支持：
- 自定义数据量
- 手动标注
- 实时查询
- 时间旅行查询

#### 3. SQL 脚本模式
```bash
make sql
```
直接执行 SQL 脚本，适合数据库管理员使用。

## 🔧 配置选项

### 环境变量
```bash
# 分别设置各个参数
export MO_HOST=192.168.1.100
export MO_PORT=6001
export MO_USER=root
export MO_PASSWORD=111
export MO_DATABASE=test

# 或者使用完整 DSN
export MO_DSN="root:111@tcp(192.168.1.100:6001)/test"
```

### 命令行参数
```bash
# 分别指定参数
./ai_dataset_demo -host 192.168.1.100 -port 6001 -user root -password 111 -database test

# 使用完整 DSN
./ai_dataset_demo -dsn "root:111@tcp(192.168.1.100:6001)/test"

# 交互式模式
./ai_dataset_demo -interactive -host 192.168.1.100 -port 6001
```

### Makefile 变量
```bash
# 指定 host 运行
make run-host HOST=192.168.1.100 PORT=6001 USER=root PASSWORD=111 DATABASE=test

# 交互式模式
make interactive-host HOST=192.168.1.100 PORT=6001
```

### 配置优先级
1. 命令行参数 (最高优先级)
2. 环境变量
3. 默认值 (最低优先级)

### 自定义配置
- 复制 `config.example` 为 `.env` 文件
- 修改 `main.go` 中的默认连接字符串
- 调整 `MockData()` 函数中的数据生成逻辑
- 自定义 AI 模型和人类标注流程

## 📊 演示数据

### 表结构
```sql
CREATE TABLE ai_dataset (
    id INT PRIMARY KEY,
    features vec32(128),  -- 128 维向量
    label VARCHAR(50) DEFAULT 'unlabeled',
    metadata JSON,        -- 标注元数据
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 示例数据流程
1. **初始状态**: 100 行未标注数据
2. **AI 标注**: 5 条记录被 AI 模型标注
3. **人类审核**: 2 条记录被人类纠正
4. **版本控制**: 每次更新都保留历史记录

## 🔍 查询示例

### 查看当前状态
```sql
SELECT id, label, 
       JSON_EXTRACT(metadata, '$.annotator') as annotator,
       JSON_EXTRACT(metadata, '$.confidence') as confidence
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

### Time Travel 查询
```sql
SELECT * FROM ai_dataset AT(TIMESTAMP => '2024-01-01 10:00:00');
```

### 向量相似度搜索
```sql
SELECT id, label, 
       VECTOR_DISTANCE(features, query_vector) as distance
FROM ai_dataset 
WHERE id != 1
ORDER BY distance 
LIMIT 5;
```

## 🎨 自定义演示

### 修改数据量
```go
// 在 main.go 的 RunDemo() 函数中
demo.MockData(1000)  // 改为 1000 行
```

### 添加新的 AI 模型
```go
aiAnnotations := []AnnotationResult{
    {ID: 10, Label: "elephant", Confidence: 0.92, Annotator: "AI_model_v2"},
    {ID: 11, Label: "lion", Confidence: 0.88, Annotator: "AI_model_v2"},
}
demo.AIModelAnnotation("AI_model_v2", aiAnnotations)
```

### 批量人类标注
```go
humanAnnotations := []AnnotationResult{
    {ID: 6, Label: "tiger", Reason: "corrected classification"},
    {ID: 7, Label: "leopard", Reason: "corrected classification"},
}
demo.HumanAnnotation(humanAnnotations)
```

## 🐛 故障排除

### 常见问题

1. **连接失败**
   ```
   Error: failed to connect to database
   ```
   - 检查 MatrixOne 是否运行在端口 6001
   - 验证用户名密码是否正确

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

## 📈 性能优化

### 批量操作
- 使用 `MockData()` 批量插入数据
- 使用 `AIModelAnnotation()` 批量更新标注

### 索引建议
```sql
CREATE INDEX idx_label ON ai_dataset(label);
CREATE INDEX idx_timestamp ON ai_dataset(timestamp);
CREATE INDEX idx_annotator ON ai_dataset((JSON_EXTRACT(metadata, '$.annotator')));
```

## 🤝 扩展开发

### 添加新功能
1. 在 `AIDatasetDemo` 结构体中添加新方法
2. 在交互式菜单中添加新选项
3. 更新 `RunDemo()` 函数包含新功能

### 集成外部 AI 模型
```go
func (d *AIDatasetDemo) ExternalAIModelAnnotation(modelAPI string, records []int) error {
    // 调用外部 AI 模型 API
    // 处理返回结果
    // 更新数据库
}
```

## 📚 相关资源

- [MatrixOne 官方文档](https://docs.matrixorigin.io/)
- [Go SQL 驱动文档](https://github.com/go-sql-driver/mysql)
- [JSON 函数参考](https://dev.mysql.com/doc/refman/8.0/en/json-functions.html)

## 🎉 开始使用

现在你已经了解了所有功能，开始你的 AI 数据管道演示之旅吧！

```bash
cd demo
make run
```

享受 Git for Data 的强大功能！ 🚀
