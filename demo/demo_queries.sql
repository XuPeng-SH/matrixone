-- AI Dataset Demo SQL Queries
-- 演示 Git for Data 和 AI 数据管道的 SQL 查询

-- ===========================================
-- 1. 表结构创建
-- ===========================================

CREATE TABLE IF NOT EXISTS ai_dataset (
    id INT PRIMARY KEY,
    features vecf32(128),
    label VARCHAR(50) DEFAULT 'unlabeled',
    metadata JSON,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===========================================
-- 2. 数据插入示例
-- ===========================================

-- 插入示例数据
INSERT INTO ai_dataset (id, features, label, metadata) VALUES
    (1, '[0.12, 0.34, 0.56, 0.78, 0.90, 0.11, 0.22, 0.33, 0.44, 0.55, 0.66, 0.77, 0.88, 0.99, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.20, 0.21, 0.22, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.29, 0.30, 0.31, 0.32, 0.33, 0.34, 0.35, 0.36, 0.37, 0.38, 0.39, 0.40, 0.41, 0.42, 0.43, 0.44, 0.45, 0.46, 0.47, 0.48, 0.49, 0.50, 0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.57, 0.58, 0.59, 0.60, 0.61, 0.62, 0.63, 0.64, 0.65, 0.66, 0.67, 0.68, 0.69, 0.70, 0.71, 0.72, 0.73, 0.74, 0.75, 0.76, 0.77, 0.78, 0.79, 0.80, 0.81, 0.82, 0.83, 0.84, 0.85, 0.86, 0.87, 0.88, 0.89, 0.90, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 1.00, 1.01, 1.02, 1.03, 1.04, 1.05, 1.06, 1.07]', 'unlabeled', NULL),
    (2, '[0.23, 0.45, 0.67, 0.89, 0.01, 0.12, 0.23, 0.34, 0.45, 0.56, 0.67, 0.78, 0.89, 0.90, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 0.00, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.20, 0.21, 0.22, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.29, 0.30, 0.31, 0.32, 0.33, 0.34, 0.35, 0.36, 0.37, 0.38, 0.39, 0.40, 0.41, 0.42, 0.43, 0.44, 0.45, 0.46, 0.47, 0.48, 0.49, 0.50, 0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.57, 0.58, 0.59, 0.60, 0.61, 0.62, 0.63, 0.64, 0.65, 0.66, 0.67, 0.68, 0.69, 0.70, 0.71, 0.72, 0.73, 0.74, 0.75, 0.76, 0.77, 0.78, 0.79, 0.80, 0.81, 0.82, 0.83, 0.84, 0.85, 0.86, 0.87, 0.88, 0.89, 0.90, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 1.00]', 'unlabeled', NULL),
    (3, '[0.34, 0.56, 0.78, 0.90, 0.12, 0.23, 0.34, 0.45, 0.56, 0.67, 0.78, 0.89, 0.90, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 0.00, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.20, 0.21, 0.22, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.29, 0.30, 0.31, 0.32, 0.33, 0.34, 0.35, 0.36, 0.37, 0.38, 0.39, 0.40, 0.41, 0.42, 0.43, 0.44, 0.45, 0.46, 0.47, 0.48, 0.49, 0.50, 0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.57, 0.58, 0.59, 0.60, 0.61, 0.62, 0.63, 0.64, 0.65, 0.66, 0.67, 0.68, 0.69, 0.70, 0.71, 0.72, 0.73, 0.74, 0.75, 0.76, 0.77, 0.78, 0.79, 0.80, 0.81, 0.82, 0.83, 0.84, 0.85, 0.86, 0.87, 0.88, 0.89, 0.90, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 1.00, 1.01]', 'unlabeled', NULL);

-- ===========================================
-- 3. AI 模型标注示例
-- ===========================================

-- AI 模型 v1 标注
UPDATE ai_dataset
SET label = 'cat', 
    metadata = JSON '{"annotator": "AI_model_v1", "confidence": 0.95}', 
    timestamp = CURRENT_TIMESTAMP
WHERE id = 1;

UPDATE ai_dataset
SET label = 'dog', 
    metadata = JSON '{"annotator": "AI_model_v1", "confidence": 0.85}', 
    timestamp = CURRENT_TIMESTAMP
WHERE id = 2;

UPDATE ai_dataset
SET label = 'bird', 
    metadata = JSON '{"annotator": "AI_model_v1", "confidence": 0.92}', 
    timestamp = CURRENT_TIMESTAMP
WHERE id = 3;

-- ===========================================
-- 4. 人类标注示例
-- ===========================================

-- 人类审核员纠正 AI 的错误
UPDATE ai_dataset
SET label = 'wolf', 
    metadata = JSON '{"annotator": "human_reviewer", "reason": "corrected from dog - AI misidentified"}', 
    timestamp = CURRENT_TIMESTAMP
WHERE id = 2;

-- ===========================================
-- 5. 数据查询示例
-- ===========================================

-- 查看所有数据
SELECT id, label, 
       JSON_EXTRACT(metadata, '$.annotator') as annotator,
       JSON_EXTRACT(metadata, '$.confidence') as confidence,
       JSON_EXTRACT(metadata, '$.reason') as reason,
       timestamp
FROM ai_dataset 
ORDER BY id;

-- 统计标注情况
SELECT 
    JSON_EXTRACT(metadata, '$.annotator') as annotator,
    COUNT(*) as count,
    AVG(JSON_EXTRACT(metadata, '$.confidence')) as avg_confidence
FROM ai_dataset 
WHERE label != 'unlabeled'
GROUP BY JSON_EXTRACT(metadata, '$.annotator');

-- 查找 AI 模型标注的记录
SELECT * FROM ai_dataset 
WHERE JSON_EXTRACT(metadata, '$.annotator') = 'AI_model_v1';

-- 查找人类纠正的记录
SELECT * FROM ai_dataset 
WHERE JSON_EXTRACT(metadata, '$.annotator') = 'human_reviewer';

-- 按标签分组统计
SELECT label, COUNT(*) as count
FROM ai_dataset 
GROUP BY label
ORDER BY count DESC;

-- ===========================================
-- 6. 快照管理示例 (Git for Data)
-- ===========================================

-- 创建快照
CREATE SNAPSHOT ai_dataset_20250909_143022_initial FOR TABLE test ai_dataset;

-- 查看所有快照
SHOW SNAPSHOTS;

-- 查询快照数据
SELECT id, label, 
       JSON_EXTRACT(metadata, '$.annotator') as annotator,
       JSON_EXTRACT(metadata, '$.confidence') as confidence,
       timestamp
FROM ai_dataset {Snapshot = "ai_dataset_20250909_143022_initial"}
ORDER BY id;

-- 删除快照
DROP SNAPSHOT ai_dataset_20250909_143022_initial;

-- ===========================================
-- 7. Time Travel 查询示例 (Git for Data)
-- ===========================================

-- 查询特定时间点的数据状态
-- 使用 MatrixOne 的 Time Travel 语法
-- 注意：需要将时间字符串转换为 TS 物理时间戳格式
-- 例如：2025-09-09 13:20:04 -> 1757424004000000000
SELECT id, label, 
       JSON_EXTRACT(metadata, '$.annotator') as annotator,
       timestamp
FROM ai_dataset {MO_TS=1757424004000000000}
ORDER BY id;

-- 查询数据变更历史
-- 这个查询展示了 Git for Data 的核心能力
SELECT id, label, 
       JSON_EXTRACT(metadata, '$.annotator') as annotator,
       timestamp
FROM ai_dataset 
WHERE timestamp <= '2024-01-01 10:00:00'
ORDER BY id, timestamp;

-- ===========================================
-- 8. 向量相似度搜索示例
-- ===========================================

-- 查找与指定记录最相似的记录
-- 使用 MatrixOne 的 L2_DISTANCE 函数
SELECT id, label, 
       L2_DISTANCE(features, (SELECT features FROM ai_dataset WHERE id = 1)) as distance,
       JSON_EXTRACT(metadata, '$.annotator') as annotator
FROM ai_dataset 
WHERE id != 1
ORDER BY distance 
LIMIT 5;

-- 基于相似度的标签预测
-- 使用 KNN 方法预测未标注数据的标签
WITH similar_records AS (
    SELECT id, label, 
           L2_DISTANCE(features, (SELECT features FROM ai_dataset WHERE id = 1)) as distance
    FROM ai_dataset 
    WHERE id != 1 AND label != 'unlabeled'
    ORDER BY distance 
    LIMIT 3
)
SELECT id, 
       (SELECT label FROM similar_records ORDER BY distance LIMIT 1) as predicted_label,
       AVG(distance) as avg_distance
FROM similar_records;

-- ===========================================
-- 8. 高级分析查询
-- ===========================================

-- 标注质量分析
SELECT 
    JSON_EXTRACT(metadata, '$.annotator') as annotator,
    COUNT(*) as total_annotations,
    COUNT(CASE WHEN JSON_EXTRACT(metadata, '$.reason') IS NOT NULL THEN 1 END) as corrections,
    AVG(JSON_EXTRACT(metadata, '$.confidence')) as avg_confidence,
    MIN(JSON_EXTRACT(metadata, '$.confidence')) as min_confidence,
    MAX(JSON_EXTRACT(metadata, '$.confidence')) as max_confidence
FROM ai_dataset 
WHERE label != 'unlabeled'
GROUP BY JSON_EXTRACT(metadata, '$.annotator');

-- 时间序列分析 - 标注活动趋势
SELECT 
    DATE(timestamp) as date,
    JSON_EXTRACT(metadata, '$.annotator') as annotator,
    COUNT(*) as annotations_count
FROM ai_dataset 
WHERE label != 'unlabeled'
GROUP BY DATE(timestamp), JSON_EXTRACT(metadata, '$.annotator')
ORDER BY date, annotator;

-- 数据质量检查
SELECT 
    'Total Records' as metric, COUNT(*) as value
FROM ai_dataset
UNION ALL
SELECT 
    'Labeled Records', COUNT(*)
FROM ai_dataset 
WHERE label != 'unlabeled'
UNION ALL
SELECT 
    'AI Annotated', COUNT(*)
FROM ai_dataset 
WHERE JSON_EXTRACT(metadata, '$.annotator') LIKE '%AI%'
UNION ALL
SELECT 
    'Human Annotated', COUNT(*)
FROM ai_dataset 
WHERE JSON_EXTRACT(metadata, '$.annotator') = 'human_reviewer'
UNION ALL
SELECT 
    'Average Confidence', ROUND(AVG(JSON_EXTRACT(metadata, '$.confidence')), 3)
FROM ai_dataset 
WHERE JSON_EXTRACT(metadata, '$.confidence') IS NOT NULL;

-- ===========================================
-- 9. 索引创建建议
-- ===========================================

-- 为常用查询创建索引
CREATE INDEX IF NOT EXISTS idx_ai_dataset_label ON ai_dataset(label);
CREATE INDEX IF NOT EXISTS idx_ai_dataset_timestamp ON ai_dataset(timestamp);
CREATE INDEX IF NOT EXISTS idx_ai_dataset_annotator ON ai_dataset((JSON_EXTRACT(metadata, '$.annotator')));

-- ===========================================
-- 10. 清理脚本
-- ===========================================

-- 清理所有数据（谨慎使用）
-- DELETE FROM ai_dataset;

-- 删除表（谨慎使用）
-- DROP TABLE IF EXISTS ai_dataset;
