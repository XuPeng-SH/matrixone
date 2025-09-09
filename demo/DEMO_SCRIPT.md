# AI Dataset Demo - 演示脚本

## 🎬 演示流程脚本

### 开场介绍 (30秒)
```
欢迎来到 MatrixOne AI Dataset Demo！

今天我们将展示 MatrixOne 的 Git for Data 能力，
结合 AI 数据管道的完整工作流程。

主要功能包括：
- Time Travel: 数据级版本控制
- AI 数据管道: 自动化标注 + 人类审核  
- 向量搜索: KNN 相似度搜索
- 版本追踪: 完整的标注历史记录
```

### 1. 环境准备 (1分钟)
```bash
# 检查 MatrixOne 状态
nc -z 127.0.0.1 6001 && echo "✅ MatrixOne is running" || echo "❌ Please start MatrixOne"

# 启动演示
cd demo
make run
```

**解说词**: "首先确保 MatrixOne 服务正在运行，然后启动我们的演示程序。"

### 2. 数据表创建 (30秒)
```
✅ Connected to MatrixOne database successfully!
✅ Created ai_dataset table successfully!
```

**解说词**: "程序自动连接到 MatrixOne 数据库，并创建了包含向量列和 JSON 元数据的 AI 数据集表。"

### 3. 模拟数据生成 (1分钟)
```
🔄 Generating 100 rows of mock data...
📊 Inserted rows 1-100
✅ Successfully generated 100 rows of mock data!
```

**解说词**: "我们生成了 100 行模拟数据，每行包含 128 维的向量特征。这些数据代表未标注的 AI 训练样本。"

### 4. 初始状态展示 (1分钟)
```
📊 Current Dataset State:
============================================================
ID   Label        Annotator       Confidence Reason                Timestamp
---- ------------ --------------- ---------- -------------------- --------------------
1    unlabeled    NULL            N/A        NULL                 2024-01-01 10:00:00
2    unlabeled    NULL            N/A        NULL                 2024-01-01 10:00:00
...

📈 Statistics: 100 total records, 0 labeled (0.0%)
```

**解说词**: "可以看到所有数据都是未标注状态，这正是 AI 数据管道的起点。"

### 5. AI 模型标注 (2分钟)
```
🤖 AI Model 'AI_model_v1' is annotating 5 records...
  📝 Record 1: cat (confidence: 0.95)
  📝 Record 2: dog (confidence: 0.85)
  📝 Record 3: bird (confidence: 0.92)
  📝 Record 4: fish (confidence: 0.78)
  📝 Record 5: cat (confidence: 0.88)
✅ AI model annotation completed!
```

**解说词**: "现在 AI 模型开始标注数据。注意每个标注都包含置信度信息，这些元数据对于后续的质量控制非常重要。"

### 6. 人类审核 (2分钟)
```
👤 Human reviewer is annotating 2 records...
  ✏️  Record 2: wolf (reason: corrected from dog - AI misidentified)
  ✏️  Record 4: shark (reason: corrected from fish - more specific classification)
✅ Human annotation completed!
```

**解说词**: "人类审核员发现了 AI 模型的错误，进行了纠正。这种人工审核机制确保了数据质量，同时保留了完整的变更历史。"

### 7. 最终状态展示 (1分钟)
```
📊 Current Dataset State:
============================================================
ID   Label        Annotator       Confidence Reason                Timestamp
---- ------------ --------------- ---------- -------------------- --------------------
1    cat          AI_model_v1     0.95       NULL                 2024-01-01 10:01:00
2    wolf         human_reviewer  N/A        corrected from dog   2024-01-01 10:02:00
3    bird         AI_model_v1     0.92       NULL                 2024-01-01 10:01:00
4    shark        human_reviewer  N/A        corrected from fish  2024-01-01 10:02:00
5    cat          AI_model_v1     0.88       NULL                 2024-01-01 10:01:00

📈 Statistics: 100 total records, 5 labeled (5.0%)
```

**解说词**: "现在可以看到标注的完整历史：AI 模型的原始标注、人类审核员的纠正，以及每次变更的时间戳。这就是 Git for Data 的核心价值。"

### 8. 向量相似度搜索 (1分钟)
```
🔍 Vector Similarity Search - Query ID: 1, Top K: 5
============================================================
ID   Label        Distance  Annotator
---- ------------ --------- ---------------
2    wolf         0.1234    human_reviewer
3    bird         0.2345    AI_model_v1
4    shark        0.3456    human_reviewer
5    cat          0.4567    AI_model_v1
6    unlabeled    0.5678    NULL
```

**解说词**: "向量相似度搜索展示了 AI 数据管道的另一个重要功能：基于特征相似性找到相关的数据样本，这对于数据增强和模型训练非常有用。"

### 9. 交互式演示 (3分钟)
```bash
# 启动交互式模式
./ai_dataset_demo interactive
```

**演示菜单操作**:
1. 选择 "1" - 生成更多模拟数据
2. 选择 "2" - 添加新的 AI 模型标注
3. 选择 "4" - 查看当前状态
4. 选择 "5" - 尝试时间旅行查询
5. 选择 "8" - 退出

**解说词**: "交互式模式让用户可以自定义演示流程，体验不同的功能组合。"

### 10. SQL 查询演示 (2分钟)
```sql
-- 连接到数据库
mysql -h 127.0.0.1 -P 6001 -u root -p111 test

-- 查看标注统计
SELECT 
    JSON_EXTRACT(metadata, '$.annotator') as annotator,
    COUNT(*) as count,
    AVG(JSON_EXTRACT(metadata, '$.confidence')) as avg_confidence
FROM ai_dataset 
WHERE label != 'unlabeled'
GROUP BY JSON_EXTRACT(metadata, '$.annotator');

-- 查看变更历史
SELECT id, label, timestamp,
       JSON_EXTRACT(metadata, '$.annotator') as annotator
FROM ai_dataset 
WHERE label != 'unlabeled'
ORDER BY timestamp;
```

**解说词**: "通过 SQL 查询，我们可以深入分析标注质量、变更历史，以及数据管道的各种统计信息。"

### 11. 总结 (1分钟)
```
🎉 Demo completed successfully!
💡 Key Features Demonstrated:
   • Git for Data: Time Travel queries (when available)
   • AI Data Pipeline: Automated and human annotations
   • Vector Search: Similarity-based retrieval
   • Version Control: Metadata tracking for reproducibility
```

**解说词**: "这个演示展示了 MatrixOne 如何将 Git 的版本控制理念应用到数据管理，为 AI 数据管道提供了强大的可重现性和可追溯性。无论是研究团队还是生产环境，这种能力都能显著提升数据质量和模型可靠性。"

## 🎯 演示要点总结

### 核心价值
1. **数据版本控制**: 像 Git 一样管理数据变更
2. **AI 数据管道**: 自动化 + 人工审核的混合模式
3. **可重现性**: 完整的标注历史和元数据追踪
4. **向量搜索**: 基于相似性的数据检索

### 技术亮点
1. **JSON 元数据**: 灵活的标注信息存储
2. **向量数据类型**: 原生支持 AI embeddings
3. **Time Travel**: 查询任意时间点的数据状态
4. **批量操作**: 高效的批量标注和更新

### 应用场景
1. **机器学习**: 训练数据的版本管理
2. **数据标注**: 多轮标注和审核流程
3. **A/B 测试**: 不同版本数据集的对比
4. **合规审计**: 数据变更的完整记录

## 📝 演示准备清单

### 环境准备
- [ ] MatrixOne 服务运行正常
- [ ] 网络连接稳定
- [ ] 演示环境干净（无旧数据）

### 演示材料
- [ ] 演示脚本打印版
- [ ] 备用 SQL 查询语句
- [ ] 故障排除指南
- [ ] 观众问题准备

### 技术准备
- [ ] 测试完整演示流程
- [ ] 准备交互式演示场景
- [ ] 验证 SQL 查询语句
- [ ] 检查网络和性能

## 🎤 演示技巧

### 节奏控制
- 每个功能演示后暂停，让观众理解
- 重点强调 Git for Data 的独特价值
- 适时与观众互动，解答问题

### 重点强调
- 版本控制对 AI 数据管道的重要性
- 元数据追踪的价值
- 向量搜索的实际应用场景
- 与传统数据库的差异

### 问题准备
- "Time Travel 查询的性能如何？"
- "如何处理大规模数据集的版本控制？"
- "向量搜索的精度如何保证？"
- "如何集成现有的 AI 模型？"

这个演示脚本将帮助您全面展示 MatrixOne 的 Git for Data 能力！🚀
