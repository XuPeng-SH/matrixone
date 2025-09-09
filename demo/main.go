package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Config 配置结构体
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	DSN      string
}

// AIDatasetDemo 演示工具结构
type AIDatasetDemo struct {
	db *sql.DB
}

// AnnotationResult 标注结果
type AnnotationResult struct {
	ID         int
	Label      string
	Confidence float64
	Annotator  string
	Reason     string
}

// parseConfig 解析配置
func parseConfig() *Config {
	config := &Config{
		Host:     "127.0.0.1",
		Port:     6001,
		User:     "root",
		Password: "111",
		Database: "test",
	}

	// 定义命令行参数
	host := flag.String("host", "", "MatrixOne host address (default: 127.0.0.1)")
	port := flag.Int("port", 0, "MatrixOne port (default: 6001)")
	user := flag.String("user", "", "Database username (default: root)")
	password := flag.String("password", "", "Database password (default: 111)")
	database := flag.String("database", "", "Database name (default: test)")
	dsn := flag.String("dsn", "", "Complete DSN connection string (overrides other options)")
	interactive := flag.Bool("interactive", false, "Run in interactive mode")

	flag.Parse()

	// 如果设置了 DSN，直接使用
	if *dsn != "" {
		config.DSN = *dsn
		return config
	}

	// 从环境变量获取配置
	if envHost := os.Getenv("MO_HOST"); envHost != "" {
		config.Host = envHost
	}
	if envPort := os.Getenv("MO_PORT"); envPort != "" {
		if p, err := strconv.Atoi(envPort); err == nil {
			config.Port = p
		}
	}
	if envUser := os.Getenv("MO_USER"); envUser != "" {
		config.User = envUser
	}
	if envPassword := os.Getenv("MO_PASSWORD"); envPassword != "" {
		config.Password = envPassword
	}
	if envDatabase := os.Getenv("MO_DATABASE"); envDatabase != "" {
		config.Database = envDatabase
	}

	// 命令行参数覆盖环境变量
	if *host != "" {
		config.Host = *host
	}
	if *port != 0 {
		config.Port = *port
	}
	if *user != "" {
		config.User = *user
	}
	if *password != "" {
		config.Password = *password
	}
	if *database != "" {
		config.Database = *database
	}

	// 构建 DSN
	config.DSN = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		config.User, config.Password, config.Host, config.Port, config.Database)

	// 检查是否要运行交互式模式
	if *interactive {
		os.Args = append([]string{os.Args[0], "interactive"}, os.Args[1:]...)
	}

	return config
}

// NewAIDatasetDemo 创建新的演示工具实例
func NewAIDatasetDemo() *AIDatasetDemo {
	return &AIDatasetDemo{}
}

// Connect 连接到数据库
func (d *AIDatasetDemo) Connect(dsn string) error {
	var err error
	d.db, err = sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	// 测试连接
	if err = d.db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %v", err)
	}

	fmt.Println("✅ Connected to MatrixOne database successfully!")
	return nil
}

// Close 关闭数据库连接
func (d *AIDatasetDemo) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

// CreateTable 创建 AI 数据集表
func (d *AIDatasetDemo) CreateTable() error {
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS ai_dataset (
		id INT PRIMARY KEY,
		features vecf32(128),
		label VARCHAR(50) DEFAULT 'unlabeled',
		metadata JSON,
		timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`

	_, err := d.db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	fmt.Println("✅ Created ai_dataset table successfully!")
	return nil
}

// generateRandomVector 生成随机向量
func (d *AIDatasetDemo) generateRandomVector(dim int) string {
	var values []string
	for i := 0; i < dim; i++ {
		// 生成 0-1 之间的随机浮点数
		value := rand.Float64()
		values = append(values, fmt.Sprintf("%.2f", value))
	}
	return "[" + strings.Join(values, ", ") + "]"
}

// MockData 生成指定行数的模拟数据
func (d *AIDatasetDemo) MockData(rowCount int) error {
	// 清空现有数据
	_, err := d.db.Exec("DELETE FROM ai_dataset")
	if err != nil {
		return fmt.Errorf("failed to clear existing data: %v", err)
	}

	fmt.Printf("🔄 Generating %d rows of mock data...\n", rowCount)

	// 批量插入数据
	batchSize := 100
	for i := 0; i < rowCount; i += batchSize {
		end := i + batchSize
		if end > rowCount {
			end = rowCount
		}

		var values []string
		for j := i; j < end; j++ {
			vector := d.generateRandomVector(128)
			values = append(values, fmt.Sprintf("(%d, '%s', 'unlabeled', NULL, CURRENT_TIMESTAMP)", j+1, vector))
		}

		insertSQL := fmt.Sprintf("INSERT INTO ai_dataset (id, features, label, metadata, timestamp) VALUES %s",
			strings.Join(values, ", "))

		_, err := d.db.Exec(insertSQL)
		if err != nil {
			return fmt.Errorf("failed to insert batch data: %v", err)
		}

		fmt.Printf("📊 Inserted rows %d-%d\n", i+1, end)
	}

	fmt.Printf("✅ Successfully generated %d rows of mock data!\n", rowCount)
	return nil
}

// AIModelAnnotation AI 模型批量标注
func (d *AIDatasetDemo) AIModelAnnotation(modelName string, annotations []AnnotationResult) error {
	fmt.Printf("🤖 AI Model '%s' is annotating %d records...\n", modelName, len(annotations))

	for _, annotation := range annotations {
		metadata := fmt.Sprintf(`{"annotator": "%s", "confidence": %.2f}`,
			modelName, annotation.Confidence)

		updateSQL := `
			UPDATE ai_dataset 
			SET label = ?, metadata = ?, timestamp = CURRENT_TIMESTAMP 
			WHERE id = ?`

		_, err := d.db.Exec(updateSQL, annotation.Label, metadata, annotation.ID)
		if err != nil {
			return fmt.Errorf("failed to update record %d: %v", annotation.ID, err)
		}

		fmt.Printf("  📝 Record %d: %s (confidence: %.2f)\n",
			annotation.ID, annotation.Label, annotation.Confidence)
	}

	fmt.Println("✅ AI model annotation completed!")
	return nil
}

// HumanAnnotation 人类标注
func (d *AIDatasetDemo) HumanAnnotation(annotations []AnnotationResult) error {
	fmt.Printf("👤 Human reviewer is annotating %d records...\n", len(annotations))

	for _, annotation := range annotations {
		metadata := fmt.Sprintf(`{"annotator": "human_reviewer", "reason": "%s"}`,
			annotation.Reason)

		updateSQL := `
			UPDATE ai_dataset 
			SET label = ?, metadata = ?, timestamp = CURRENT_TIMESTAMP 
			WHERE id = ?`

		_, err := d.db.Exec(updateSQL, annotation.Label, metadata, annotation.ID)
		if err != nil {
			return fmt.Errorf("failed to update record %d: %v", annotation.ID, err)
		}

		fmt.Printf("  ✏️  Record %d: %s (reason: %s)\n",
			annotation.ID, annotation.Label, annotation.Reason)
	}

	fmt.Println("✅ Human annotation completed!")
	return nil
}

// ShowCurrentState 显示当前数据状态
func (d *AIDatasetDemo) ShowCurrentState() error {
	fmt.Println("\n📊 Current Dataset State:")
	fmt.Println(strings.Repeat("=", 60))

	query := `
		SELECT id, label, 
		       JSON_EXTRACT(metadata, '$.annotator') as annotator,
		       JSON_EXTRACT(metadata, '$.confidence') as confidence,
		       JSON_EXTRACT(metadata, '$.reason') as reason,
		       timestamp
		FROM ai_dataset 
		ORDER BY id 
		LIMIT 10`

	rows, err := d.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query data: %v", err)
	}
	defer rows.Close()

	fmt.Printf("%-4s %-12s %-15s %-10s %-20s %-20s\n",
		"ID", "Label", "Annotator", "Confidence", "Reason", "Timestamp")
	fmt.Println(strings.Repeat("-", 100))

	for rows.Next() {
		var id int
		var label, timestamp string
		var annotator, reason sql.NullString
		var confidence sql.NullFloat64

		err := rows.Scan(&id, &label, &annotator, &confidence, &reason, &timestamp)
		if err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		confStr := "N/A"
		if confidence.Valid {
			confStr = fmt.Sprintf("%.2f", confidence.Float64)
		}

		annotatorStr := "N/A"
		if annotator.Valid {
			annotatorStr = strings.Trim(annotator.String, `"`)
		}

		reasonStr := "N/A"
		if reason.Valid {
			reasonStr = strings.Trim(reason.String, `"`)
		}

		fmt.Printf("%-4d %-12s %-15s %-10s %-20s %-20s\n",
			id, label, annotatorStr, confStr, reasonStr, timestamp)
	}

	// 显示统计信息
	var totalCount, labeledCount int
	d.db.QueryRow("SELECT COUNT(*) FROM ai_dataset").Scan(&totalCount)
	d.db.QueryRow("SELECT COUNT(*) FROM ai_dataset WHERE label != 'unlabeled'").Scan(&labeledCount)

	fmt.Printf("\n📈 Statistics: %d total records, %d labeled (%.1f%%)\n",
		totalCount, labeledCount, float64(labeledCount)/float64(totalCount)*100)

	return nil
}

// parseTimeToTS 将时间字符串转换为 MatrixOne TS 格式
func parseTimeToTS(timeStr string) (string, error) {
	// 解析时间字符串
	parsedTime, err := time.ParseInLocation("2006-01-02 15:04:05", timeStr, time.Local)
	if err != nil {
		return "", fmt.Errorf("invalid time format, expected: 2006-01-02 15:04:05, got: %s", timeStr)
	}

	// 转换为纳秒时间戳（MatrixOne 使用纳秒作为物理时间）
	nanos := parsedTime.UnixNano()

	// MatrixOne TS 格式：直接使用物理时间戳
	ts := fmt.Sprintf("%d", nanos)

	return ts, nil
}

// TimeTravelQuery 时间旅行查询 - 查询指定时间点的数据状态
func (d *AIDatasetDemo) TimeTravelQuery(targetTime string) error {
	fmt.Printf("⏰ Time Travel Query - Target Time: %s\n", targetTime)
	fmt.Println(strings.Repeat("=", 60))

	// 将时间字符串转换为 MatrixOne TS 格式
	ts, err := parseTimeToTS(targetTime)
	if err != nil {
		fmt.Printf("❌ Time format error: %v\n", err)
		fmt.Println("📊 Showing current state instead:")
		return d.ShowCurrentState()
	}

	fmt.Printf("🕐 Converted to TS: %s\n", ts)

	// 使用 MatrixOne 的 Time Travel 语法
	query := fmt.Sprintf(`
		SELECT id, label, 
		       JSON_EXTRACT(metadata, '$.annotator') as annotator,
		       JSON_EXTRACT(metadata, '$.confidence') as confidence,
		       timestamp
		FROM ai_dataset {MO_TS=%s}
		ORDER BY id 
		LIMIT 10`, ts)

	rows, err := d.db.Query(query)
	if err != nil {
		// 如果 Time Travel 查询失败，显示当前状态
		fmt.Printf("⚠️  Time Travel query failed (feature may not be available): %v\n", err)
		fmt.Println("📊 Showing current state instead:")
		return d.ShowCurrentState()
	}
	defer rows.Close()

	fmt.Printf("%-4s %-12s %-15s %-10s %-20s\n",
		"ID", "Label", "Annotator", "Confidence", "Timestamp")
	fmt.Println(strings.Repeat("-", 80))

	for rows.Next() {
		var id int
		var label, timestamp string
		var annotator sql.NullString
		var confidence sql.NullFloat64

		err := rows.Scan(&id, &label, &annotator, &confidence, &timestamp)
		if err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		confStr := "N/A"
		if confidence.Valid {
			confStr = fmt.Sprintf("%.2f", confidence.Float64)
		}

		annotatorStr := "N/A"
		if annotator.Valid {
			annotatorStr = strings.Trim(annotator.String, `"`)
		}

		fmt.Printf("%-4d %-12s %-15s %-10s %-20s\n",
			id, label, annotatorStr, confStr, timestamp)
	}

	return nil
}

// CompareTimePoints 比较两个时间点的数据差异
func (d *AIDatasetDemo) CompareTimePoints(time1, time2 string) error {
	return d.CompareTimePointsWithMode(time1, time2, true) // 默认显示详细差异
}

// CompareTimePointsWithMode 比较两个时间点的数据差异，可选择显示模式
func (d *AIDatasetDemo) CompareTimePointsWithMode(time1, time2 string, showDetailed bool) error {
	fmt.Printf("🔄 Data Comparison - Time Point 1: %s vs Time Point 2: %s\n", time1, time2)
	fmt.Println(strings.Repeat("=", 80))

	// 转换时间格式
	ts1, err1 := parseTimeToTS(time1)
	if err1 != nil {
		return fmt.Errorf("invalid time format for time1: %v", err1)
	}

	ts2, err2 := parseTimeToTS(time2)
	if err2 != nil {
		return fmt.Errorf("invalid time format for time2: %v", err2)
	}

	fmt.Printf("🕐 Time Point 1 TS: %s\n", ts1)
	fmt.Printf("🕐 Time Point 2 TS: %s\n", ts2)
	fmt.Println()

	// 获取两个时间点的数据
	data1, err := d.getDataAtTime(ts1)
	if err != nil {
		return fmt.Errorf("failed to get data at time1: %v", err)
	}

	data2, err := d.getDataAtTime(ts2)
	if err != nil {
		return fmt.Errorf("failed to get data at time2: %v", err)
	}

	// 比较数据差异
	if showDetailed {
		d.compareDataDetailed(data1, data2, time1, time2)
	} else {
		d.compareDataSummary(data1, data2, time1, time2)
	}
	return nil
}

// DataRecord 数据结构
type DataRecord struct {
	ID         int
	Label      string
	Annotator  string
	Confidence string
	Reason     string
	Timestamp  string
}

// getDataAtTime 获取指定时间点的数据
func (d *AIDatasetDemo) getDataAtTime(ts string) (map[int]DataRecord, error) {
	query := fmt.Sprintf(`
		SELECT id, label, 
		       JSON_EXTRACT(metadata, '$.annotator') as annotator,
		       JSON_EXTRACT(metadata, '$.confidence') as confidence,
		       JSON_EXTRACT(metadata, '$.reason') as reason,
		       timestamp
		FROM ai_dataset {MO_TS=%s}
		ORDER BY id`, ts)

	rows, err := d.db.Query(query)
	if err != nil {
		// 如果 Time Travel 查询失败，使用当前数据
		fmt.Printf("⚠️  Time Travel query failed, using current data: %v\n", err)
		query = `
			SELECT id, label, 
			       JSON_EXTRACT(metadata, '$.annotator') as annotator,
			       JSON_EXTRACT(metadata, '$.confidence') as confidence,
			       JSON_EXTRACT(metadata, '$.reason') as reason,
			       timestamp
			FROM ai_dataset 
			ORDER BY id`
		rows, err = d.db.Query(query)
		if err != nil {
			return nil, fmt.Errorf("failed to query data: %v", err)
		}
	}
	defer rows.Close()

	data := make(map[int]DataRecord)
	for rows.Next() {
		var id int
		var label, timestamp string
		var annotator, reason sql.NullString
		var confidence sql.NullFloat64

		err := rows.Scan(&id, &label, &annotator, &confidence, &reason, &timestamp)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		annotatorStr := "N/A"
		if annotator.Valid {
			annotatorStr = strings.Trim(annotator.String, `"`)
		}

		confStr := "N/A"
		if confidence.Valid {
			confStr = fmt.Sprintf("%.2f", confidence.Float64)
		}

		reasonStr := "N/A"
		if reason.Valid {
			reasonStr = strings.Trim(reason.String, `"`)
		}

		data[id] = DataRecord{
			ID:         id,
			Label:      label,
			Annotator:  annotatorStr,
			Confidence: confStr,
			Reason:     reasonStr,
			Timestamp:  timestamp,
		}
	}

	return data, nil
}

// compareDataDetailed 比较两个时间点的数据差异（详细模式）
func (d *AIDatasetDemo) compareDataDetailed(data1, data2 map[int]DataRecord, time1, time2 string) {
	fmt.Printf("📊 Data Comparison Results:\n")
	fmt.Println(strings.Repeat("-", 80))

	// 统计信息
	added := 0
	removed := 0
	modified := 0
	unchanged := 0

	// 检查所有记录
	allIDs := make(map[int]bool)
	for id := range data1 {
		allIDs[id] = true
	}
	for id := range data2 {
		allIDs[id] = true
	}

	// 显示详细差异
	fmt.Printf("🔍 Detailed Changes:\n")
	fmt.Println(strings.Repeat("=", 100))

	for id := range allIDs {
		record1, exists1 := data1[id]
		record2, exists2 := data2[id]

		if !exists1 {
			// 新增记录
			added++
			fmt.Printf("🆕 RECORD ADDED - ID: %d\n", id)
			fmt.Printf("   📍 Time Point: %s\n", time2)
			fmt.Printf("   🏷️  Label: %s\n", record2.Label)
			fmt.Printf("   👤 Annotator: %s\n", record2.Annotator)
			fmt.Printf("   📊 Confidence: %s\n", record2.Confidence)
			if record2.Reason != "N/A" {
				fmt.Printf("   💭 Reason: %s\n", record2.Reason)
			}
			fmt.Printf("   ⏰ Timestamp: %s\n", record2.Timestamp)
			fmt.Println(strings.Repeat("-", 50))
		} else if !exists2 {
			// 删除记录
			removed++
			fmt.Printf("🗑️  RECORD REMOVED - ID: %d\n", id)
			fmt.Printf("   📍 Time Point: %s\n", time1)
			fmt.Printf("   🏷️  Label: %s\n", record1.Label)
			fmt.Printf("   👤 Annotator: %s\n", record1.Annotator)
			fmt.Printf("   📊 Confidence: %s\n", record1.Confidence)
			if record1.Reason != "N/A" {
				fmt.Printf("   💭 Reason: %s\n", record1.Reason)
			}
			fmt.Printf("   ⏰ Timestamp: %s\n", record1.Timestamp)
			fmt.Println(strings.Repeat("-", 50))
		} else {
			// 比较记录
			hasChanges := false
			changes := []string{}

			if record1.Label != record2.Label {
				hasChanges = true
				changes = append(changes, fmt.Sprintf("Label: '%s' → '%s'", record1.Label, record2.Label))
			}
			if record1.Annotator != record2.Annotator {
				hasChanges = true
				changes = append(changes, fmt.Sprintf("Annotator: '%s' → '%s'", record1.Annotator, record2.Annotator))
			}
			if record1.Confidence != record2.Confidence {
				hasChanges = true
				changes = append(changes, fmt.Sprintf("Confidence: '%s' → '%s'", record1.Confidence, record2.Confidence))
			}
			if record1.Reason != record2.Reason {
				hasChanges = true
				changes = append(changes, fmt.Sprintf("Reason: '%s' → '%s'", record1.Reason, record2.Reason))
			}

			if hasChanges {
				modified++
				fmt.Printf("🔄 RECORD MODIFIED - ID: %d\n", id)
				fmt.Printf("   📍 Time Points: %s → %s\n", time1, time2)

				for _, change := range changes {
					fmt.Printf("   🔄 %s\n", change)
				}

				// 显示完整的 metadata 信息
				fmt.Printf("   📋 Metadata Details:\n")
				fmt.Printf("      Time 1: Annotator='%s', Confidence='%s', Reason='%s'\n",
					record1.Annotator, record1.Confidence, record1.Reason)
				fmt.Printf("      Time 2: Annotator='%s', Confidence='%s', Reason='%s'\n",
					record2.Annotator, record2.Confidence, record2.Reason)
				fmt.Printf("   ⏰ Timestamps: %s → %s\n", record1.Timestamp, record2.Timestamp)
				fmt.Println(strings.Repeat("-", 50))
			} else {
				unchanged++
			}
		}
	}

	// 显示统计摘要
	fmt.Printf("📈 Summary:\n")
	fmt.Println(strings.Repeat("-", 50))
	fmt.Printf("  🆕 Added records: %d\n", added)
	fmt.Printf("  🗑️  Removed records: %d\n", removed)
	fmt.Printf("  🔄 Modified records: %d\n", modified)
	fmt.Printf("  ✅ Unchanged records: %d\n", unchanged)
	fmt.Printf("  📊 Total records at %s: %d\n", time1, len(data1))
	fmt.Printf("  📊 Total records at %s: %d\n", time2, len(data2))

	// 显示标签变化统计
	d.showLabelChanges(data1, data2, time1, time2)
}

// compareDataSummary 比较两个时间点的数据差异（统计模式）
func (d *AIDatasetDemo) compareDataSummary(data1, data2 map[int]DataRecord, time1, time2 string) {
	fmt.Printf("📊 Data Comparison Summary:\n")
	fmt.Println(strings.Repeat("-", 80))

	// 统计信息
	added := 0
	removed := 0
	modified := 0
	unchanged := 0

	// 检查所有记录
	allIDs := make(map[int]bool)
	for id := range data1 {
		allIDs[id] = true
	}
	for id := range data2 {
		allIDs[id] = true
	}

	// 统计变化
	for id := range allIDs {
		record1, exists1 := data1[id]
		record2, exists2 := data2[id]

		if !exists1 {
			added++
		} else if !exists2 {
			removed++
		} else {
			// 比较记录
			if record1.Label != record2.Label || record1.Annotator != record2.Annotator ||
				record1.Confidence != record2.Confidence || record1.Reason != record2.Reason {
				modified++
			} else {
				unchanged++
			}
		}
	}

	// 显示统计摘要
	fmt.Printf("📈 Summary:\n")
	fmt.Println(strings.Repeat("-", 50))
	fmt.Printf("  🆕 Added records: %d\n", added)
	fmt.Printf("  🗑️  Removed records: %d\n", removed)
	fmt.Printf("  🔄 Modified records: %d\n", modified)
	fmt.Printf("  ✅ Unchanged records: %d\n", unchanged)
	fmt.Printf("  📊 Total records at %s: %d\n", time1, len(data1))
	fmt.Printf("  📊 Total records at %s: %d\n", time2, len(data2))

	// 显示标签变化统计
	d.showLabelChanges(data1, data2, time1, time2)
}

// showLabelChanges 显示标签变化统计
func (d *AIDatasetDemo) showLabelChanges(data1, data2 map[int]DataRecord, time1, time2 string) {
	fmt.Println("\n🏷️  Label Change Analysis:")
	fmt.Println(strings.Repeat("-", 60))

	// 统计标签分布
	labels1 := make(map[string]int)
	labels2 := make(map[string]int)

	for _, record := range data1 {
		labels1[record.Label]++
	}
	for _, record := range data2 {
		labels2[record.Label]++
	}

	// 显示标签变化
	allLabels := make(map[string]bool)
	for label := range labels1 {
		allLabels[label] = true
	}
	for label := range labels2 {
		allLabels[label] = true
	}

	fmt.Printf("%-15s %-8s %-8s %-10s\n", "Label", "Count1", "Count2", "Change")
	fmt.Println(strings.Repeat("-", 50))

	for label := range allLabels {
		count1 := labels1[label]
		count2 := labels2[label]
		change := count2 - count1

		changeStr := "="
		if change > 0 {
			changeStr = fmt.Sprintf("+%d", change)
		} else if change < 0 {
			changeStr = fmt.Sprintf("%d", change)
		}

		fmt.Printf("%-15s %-8d %-8d %-10s\n", label, count1, count2, changeStr)
	}
}

// CreateSnapshot 创建快照
func (d *AIDatasetDemo) CreateSnapshot(suffix string) error {
	// 生成快照名称：前缀 + 时间戳 + 用户后缀
	timestamp := time.Now().Format("20060102_150405")
	snapshotName := fmt.Sprintf("ai_dataset_%s_%s", timestamp, suffix)

	fmt.Printf("📸 Creating Snapshot: %s\n", snapshotName)
	fmt.Println(strings.Repeat("=", 60))

	// 创建快照的 SQL
	createSQL := fmt.Sprintf("CREATE SNAPSHOT %s FOR TABLE test ai_dataset", snapshotName)

	_, err := d.db.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}

	fmt.Printf("✅ Snapshot '%s' created successfully!\n", snapshotName)
	fmt.Printf("📋 SQL: %s\n", createSQL)

	return nil
}

// ShowSnapshots 显示所有快照
func (d *AIDatasetDemo) ShowSnapshots() error {
	fmt.Println("📸 Available Snapshots:")
	fmt.Println(strings.Repeat("=", 80))

	query := "SHOW SNAPSHOTS"
	rows, err := d.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query snapshots: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var snapshotName, timestamp, snapshotLevel, accountName, databaseName, tableName string
		err := rows.Scan(&snapshotName, &timestamp, &snapshotLevel, &accountName, &databaseName, &tableName)
		if err != nil {
			return fmt.Errorf("failed to scan snapshot row: %v", err)
		}

		// 美化输出，突出快照名称和时间
		fmt.Printf("📸 %s\n", strings.Repeat("=", 76))
		fmt.Printf("🏷️  Name: %s\n", snapshotName)
		fmt.Printf("⏰ Time:  %s\n", timestamp)
		fmt.Printf("📊 Level: %s | Account: %s | Database: %s | Table: %s\n", 
			snapshotLevel, accountName, databaseName, tableName)
		fmt.Println()
		count++
	}

	if count == 0 {
		fmt.Println("❌ No snapshots found.")
	} else {
		fmt.Printf("📊 Total snapshots: %d\n", count)
	}

	return nil
}

// DropSnapshot 删除快照
func (d *AIDatasetDemo) DropSnapshot(snapshotName string) error {
	fmt.Printf("🗑️  Dropping Snapshot: %s\n", snapshotName)
	fmt.Println(strings.Repeat("=", 60))

	dropSQL := fmt.Sprintf("DROP SNAPSHOT %s", snapshotName)

	_, err := d.db.Exec(dropSQL)
	if err != nil {
		return fmt.Errorf("failed to drop snapshot: %v", err)
	}

	fmt.Printf("✅ Snapshot '%s' dropped successfully!\n", snapshotName)
	fmt.Printf("📋 SQL: %s\n", dropSQL)

	return nil
}

// DropAllSnapshots 删除所有快照
func (d *AIDatasetDemo) DropAllSnapshots() error {
	fmt.Println("🗑️🗑️  Dropping All Snapshots")
	fmt.Println(strings.Repeat("=", 60))

	// 首先获取所有快照
	query := "SHOW SNAPSHOTS"
	rows, err := d.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query snapshots: %v", err)
	}
	defer rows.Close()

	var snapshotNames []string
	for rows.Next() {
		var snapshotName, timestamp, snapshotLevel, accountName, databaseName, tableName string
		err := rows.Scan(&snapshotName, &timestamp, &snapshotLevel, &accountName, &databaseName, &tableName)
		if err != nil {
			return fmt.Errorf("failed to scan snapshot row: %v", err)
		}
		snapshotNames = append(snapshotNames, snapshotName)
	}

	if len(snapshotNames) == 0 {
		fmt.Println("ℹ️  No snapshots found to delete.")
		return nil
	}

	fmt.Printf("📋 Found %d snapshots to delete:\n", len(snapshotNames))
	for i, name := range snapshotNames {
		fmt.Printf("  %d. %s\n", i+1, name)
	}
	fmt.Println()

	// 删除所有快照
	successCount := 0
	failedCount := 0

	for _, snapshotName := range snapshotNames {
		dropSQL := fmt.Sprintf("DROP SNAPSHOT %s", snapshotName)
		_, err := d.db.Exec(dropSQL)
		if err != nil {
			fmt.Printf("❌ Failed to drop snapshot '%s': %v\n", snapshotName, err)
			failedCount++
		} else {
			fmt.Printf("✅ Dropped snapshot: %s\n", snapshotName)
			successCount++
		}
	}

	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("📊 Summary: %d successful, %d failed\n", successCount, failedCount)

	if failedCount == 0 {
		fmt.Println("🎉 All snapshots deleted successfully!")
	} else {
		fmt.Printf("⚠️  %d snapshots failed to delete\n", failedCount)
	}

	return nil
}

// CompareSnapshots 比较两个快照
func (d *AIDatasetDemo) CompareSnapshots(snapshot1, snapshot2 string) error {
	return d.CompareSnapshotsWithMode(snapshot1, snapshot2, true) // 默认显示详细差异
}

// CompareSnapshotWithTimestamp 比较快照和时间戳
func (d *AIDatasetDemo) CompareSnapshotWithTimestamp(snapshotName, timestamp string, showDetailed bool) error {
	fmt.Printf("🔄 Snapshot vs Timestamp Comparison - Snapshot: %s vs Timestamp: %s\n", snapshotName, timestamp)
	fmt.Println(strings.Repeat("=", 80))

	// 获取快照数据
	data1, err := d.getDataFromSnapshot(snapshotName)
	if err != nil {
		return fmt.Errorf("failed to get data from snapshot: %v", err)
	}

	// 转换时间戳格式
	ts, err := parseTimeToTS(timestamp)
	if err != nil {
		return fmt.Errorf("invalid timestamp format: %v", err)
	}

	// 获取时间戳数据
	data2, err := d.getDataAtTime(ts)
	if err != nil {
		return fmt.Errorf("failed to get data at timestamp: %v", err)
	}

	// 比较数据差异
	if showDetailed {
		d.compareDataDetailed(data1, data2, fmt.Sprintf("Snapshot: %s", snapshotName), fmt.Sprintf("Timestamp: %s", timestamp))
	} else {
		d.compareDataSummary(data1, data2, fmt.Sprintf("Snapshot: %s", snapshotName), fmt.Sprintf("Timestamp: %s", timestamp))
	}
	return nil
}

// CompareSnapshotsWithMode 比较两个快照，可选择显示模式
func (d *AIDatasetDemo) CompareSnapshotsWithMode(snapshot1, snapshot2 string, showDetailed bool) error {
	fmt.Printf("🔄 Snapshot Comparison - Snapshot 1: %s vs Snapshot 2: %s\n", snapshot1, snapshot2)
	fmt.Println(strings.Repeat("=", 80))

	// 获取两个快照的数据
	data1, err := d.getDataFromSnapshot(snapshot1)
	if err != nil {
		return fmt.Errorf("failed to get data from snapshot1: %v", err)
	}

	data2, err := d.getDataFromSnapshot(snapshot2)
	if err != nil {
		return fmt.Errorf("failed to get data from snapshot2: %v", err)
	}

	// 比较数据差异
	if showDetailed {
		d.compareDataDetailed(data1, data2, snapshot1, snapshot2)
	} else {
		d.compareDataSummary(data1, data2, snapshot1, snapshot2)
	}
	return nil
}

// getDataFromSnapshot 从快照获取数据
func (d *AIDatasetDemo) getDataFromSnapshot(snapshotName string) (map[int]DataRecord, error) {
	query := fmt.Sprintf(`
		SELECT id, label, 
		       JSON_EXTRACT(metadata, '$.annotator') as annotator,
		       JSON_EXTRACT(metadata, '$.confidence') as confidence,
		       JSON_EXTRACT(metadata, '$.reason') as reason,
		       timestamp
		FROM ai_dataset {Snapshot = "%s"}
		ORDER BY id`, snapshotName)

	rows, err := d.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshot data: %v", err)
	}
	defer rows.Close()

	data := make(map[int]DataRecord)
	for rows.Next() {
		var id int
		var label, timestamp string
		var annotator, reason sql.NullString
		var confidence sql.NullFloat64

		err := rows.Scan(&id, &label, &annotator, &confidence, &reason, &timestamp)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		annotatorStr := "N/A"
		if annotator.Valid {
			annotatorStr = strings.Trim(annotator.String, `"`)
		}

		confStr := "N/A"
		if confidence.Valid {
			confStr = fmt.Sprintf("%.2f", confidence.Float64)
		}

		reasonStr := "N/A"
		if reason.Valid {
			reasonStr = strings.Trim(reason.String, `"`)
		}

		data[id] = DataRecord{
			ID:         id,
			Label:      label,
			Annotator:  annotatorStr,
			Confidence: confStr,
			Reason:     reasonStr,
			Timestamp:  timestamp,
		}
	}

	return data, nil
}

// VectorSimilaritySearch 向量相似度搜索
func (d *AIDatasetDemo) VectorSimilaritySearch(queryID int, topK int) error {
	fmt.Printf("🔍 Vector Similarity Search - Query ID: %d, Top K: %d\n", queryID, topK)
	fmt.Println(strings.Repeat("=", 60))

	// 获取查询向量
	var queryVector string
	err := d.db.QueryRow("SELECT features FROM ai_dataset WHERE id = ?", queryID).Scan(&queryVector)
	if err != nil {
		return fmt.Errorf("failed to get query vector: %v", err)
	}

	// 执行向量相似度搜索
	// 注意：这里需要根据 MatrixOne 的实际向量搜索语法调整
	query := fmt.Sprintf(`
		SELECT id, label, 
		       L2_DISTANCE(features, '%s') as distance,
		       JSON_EXTRACT(metadata, '$.annotator') as annotator
		FROM ai_dataset 
		WHERE id != %d
		ORDER BY distance 
		LIMIT %d`, queryVector, queryID, topK)

	rows, err := d.db.Query(query)
	if err != nil {
		// 如果向量搜索失败，显示简单的 ID 搜索
		fmt.Printf("⚠️  Vector similarity search failed (feature may not be available): %v\n", err)
		fmt.Println("📊 Showing simple ID-based search instead:")

		simpleQuery := fmt.Sprintf(`
			SELECT id, label, 
			       JSON_EXTRACT(metadata, '$.annotator') as annotator
			FROM ai_dataset 
			WHERE id != %d
			ORDER BY id 
			LIMIT %d`, queryID, topK)

		rows, err = d.db.Query(simpleQuery)
		if err != nil {
			return fmt.Errorf("failed to execute simple query: %v", err)
		}
		defer rows.Close()

		fmt.Printf("%-4s %-12s %-15s\n", "ID", "Label", "Annotator")
		fmt.Println(strings.Repeat("-", 40))

		for rows.Next() {
			var id int
			var label string
			var annotator sql.NullString

			err := rows.Scan(&id, &label, &annotator)
			if err != nil {
				return fmt.Errorf("failed to scan row: %v", err)
			}

			annotatorStr := "N/A"
			if annotator.Valid {
				annotatorStr = strings.Trim(annotator.String, `"`)
			}
			fmt.Printf("%-4d %-12s %-15s\n", id, label, annotatorStr)
		}
		return nil
	}
	defer rows.Close()

	fmt.Printf("%-4s %-12s %-10s %-15s\n", "ID", "Label", "Distance", "Annotator")
	fmt.Println(strings.Repeat("-", 50))

	for rows.Next() {
		var id int
		var label string
		var annotator sql.NullString
		var distance sql.NullFloat64

		err := rows.Scan(&id, &label, &distance, &annotator)
		if err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		distStr := "N/A"
		if distance.Valid {
			distStr = fmt.Sprintf("%.4f", distance.Float64)
		}

		annotatorStr := "N/A"
		if annotator.Valid {
			annotatorStr = strings.Trim(annotator.String, `"`)
		}

		fmt.Printf("%-4d %-12s %-10s %-15s\n", id, label, distStr, annotatorStr)
	}

	return nil
}

// RunDemo 运行完整演示
func (d *AIDatasetDemo) RunDemo() error {
	fmt.Println("🚀 Starting AI Dataset Demo with Git for Data capabilities...")
	fmt.Println(strings.Repeat("=", 80))

	// 1. 创建表
	if err := d.CreateTable(); err != nil {
		return err
	}

	// 2. 生成模拟数据
	if err := d.MockData(100); err != nil {
		return err
	}

	// 3. 显示初始状态
	if err := d.ShowCurrentState(); err != nil {
		return err
	}

	// 4. AI 模型标注 - 标注 30 条记录
	aiAnnotations := []AnnotationResult{
		{ID: 1, Label: "cat", Confidence: 0.95, Annotator: "AI_model_v1"},
		{ID: 2, Label: "dog", Confidence: 0.85, Annotator: "AI_model_v1"},
		{ID: 3, Label: "bird", Confidence: 0.92, Annotator: "AI_model_v1"},
		{ID: 4, Label: "fish", Confidence: 0.78, Annotator: "AI_model_v1"},
		{ID: 5, Label: "cat", Confidence: 0.88, Annotator: "AI_model_v1"},
		{ID: 6, Label: "elephant", Confidence: 0.91, Annotator: "AI_model_v1"},
		{ID: 7, Label: "lion", Confidence: 0.87, Annotator: "AI_model_v1"},
		{ID: 8, Label: "tiger", Confidence: 0.89, Annotator: "AI_model_v1"},
		{ID: 9, Label: "bear", Confidence: 0.83, Annotator: "AI_model_v1"},
		{ID: 10, Label: "wolf", Confidence: 0.86, Annotator: "AI_model_v1"},
		{ID: 11, Label: "eagle", Confidence: 0.94, Annotator: "AI_model_v1"},
		{ID: 12, Label: "shark", Confidence: 0.82, Annotator: "AI_model_v1"},
		{ID: 13, Label: "dolphin", Confidence: 0.90, Annotator: "AI_model_v1"},
		{ID: 14, Label: "penguin", Confidence: 0.88, Annotator: "AI_model_v1"},
		{ID: 15, Label: "giraffe", Confidence: 0.85, Annotator: "AI_model_v1"},
		{ID: 16, Label: "zebra", Confidence: 0.87, Annotator: "AI_model_v1"},
		{ID: 17, Label: "monkey", Confidence: 0.89, Annotator: "AI_model_v1"},
		{ID: 18, Label: "snake", Confidence: 0.84, Annotator: "AI_model_v1"},
		{ID: 19, Label: "frog", Confidence: 0.81, Annotator: "AI_model_v1"},
		{ID: 20, Label: "butterfly", Confidence: 0.93, Annotator: "AI_model_v1"},
		{ID: 21, Label: "spider", Confidence: 0.79, Annotator: "AI_model_v1"},
		{ID: 22, Label: "ant", Confidence: 0.76, Annotator: "AI_model_v1"},
		{ID: 23, Label: "bee", Confidence: 0.88, Annotator: "AI_model_v1"},
		{ID: 24, Label: "ladybug", Confidence: 0.92, Annotator: "AI_model_v1"},
		{ID: 25, Label: "dragonfly", Confidence: 0.85, Annotator: "AI_model_v1"},
		{ID: 26, Label: "cricket", Confidence: 0.78, Annotator: "AI_model_v1"},
		{ID: 27, Label: "grasshopper", Confidence: 0.80, Annotator: "AI_model_v1"},
		{ID: 28, Label: "caterpillar", Confidence: 0.83, Annotator: "AI_model_v1"},
		{ID: 29, Label: "moth", Confidence: 0.77, Annotator: "AI_model_v1"},
		{ID: 30, Label: "beetle", Confidence: 0.86, Annotator: "AI_model_v1"},
	}

	if err := d.AIModelAnnotation("AI_model_v1", aiAnnotations); err != nil {
		return err
	}

	// 等待一秒以创建时间差异
	time.Sleep(1 * time.Second)

	// 5. 人类标注（纠正 AI 的错误）- 审核 20 条记录
	humanAnnotations := []AnnotationResult{
		{ID: 2, Label: "wolf", Reason: "corrected from dog - AI misidentified"},
		{ID: 4, Label: "shark", Reason: "corrected from fish - more specific classification"},
		{ID: 6, Label: "elephant", Reason: "confirmed AI annotation - correct"},
		{ID: 7, Label: "lion", Reason: "confirmed AI annotation - correct"},
		{ID: 8, Label: "tiger", Reason: "confirmed AI annotation - correct"},
		{ID: 9, Label: "bear", Reason: "confirmed AI annotation - correct"},
		{ID: 10, Label: "wolf", Reason: "confirmed AI annotation - correct"},
		{ID: 11, Label: "eagle", Reason: "confirmed AI annotation - correct"},
		{ID: 12, Label: "shark", Reason: "confirmed AI annotation - correct"},
		{ID: 13, Label: "dolphin", Reason: "confirmed AI annotation - correct"},
		{ID: 14, Label: "penguin", Reason: "confirmed AI annotation - correct"},
		{ID: 15, Label: "giraffe", Reason: "confirmed AI annotation - correct"},
		{ID: 16, Label: "zebra", Reason: "confirmed AI annotation - correct"},
		{ID: 17, Label: "monkey", Reason: "confirmed AI annotation - correct"},
		{ID: 18, Label: "snake", Reason: "confirmed AI annotation - correct"},
		{ID: 19, Label: "frog", Reason: "confirmed AI annotation - correct"},
		{ID: 20, Label: "butterfly", Reason: "confirmed AI annotation - correct"},
		{ID: 21, Label: "spider", Reason: "confirmed AI annotation - correct"},
		{ID: 22, Label: "ant", Reason: "confirmed AI annotation - correct"},
		{ID: 23, Label: "bee", Reason: "confirmed AI annotation - correct"},
	}

	if err := d.HumanAnnotation(humanAnnotations); err != nil {
		return err
	}

	// 6. 显示最终状态
	if err := d.ShowCurrentState(); err != nil {
		return err
	}

	// 7. 向量相似度搜索演示
	if err := d.VectorSimilaritySearch(1, 5); err != nil {
		return err
	}

	fmt.Println("\n🎉 Demo completed successfully!")
	fmt.Println("💡 Key Features Demonstrated:")
	fmt.Println("   • Git for Data: Time Travel queries (when available)")
	fmt.Println("   • AI Data Pipeline: Automated and human annotations")
	fmt.Println("   • Vector Search: Similarity-based retrieval")
	fmt.Println("   • Version Control: Metadata tracking for reproducibility")

	return nil
}

func main() {
	// 解析配置
	config := parseConfig()

	// 检查是否要运行交互式模式
	if len(os.Args) > 1 && os.Args[1] == "interactive" {
		runInteractiveDemo(config)
		return
	}

	// 显示连接信息
	fmt.Printf("🔗 Connecting to MatrixOne at %s:%d\n", config.Host, config.Port)
	fmt.Printf("📊 Database: %s, User: %s\n", config.Database, config.User)

	demo := NewAIDatasetDemo()
	defer demo.Close()

	// 连接数据库
	if err := demo.Connect(config.DSN); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	// 运行演示
	if err := demo.RunDemo(); err != nil {
		log.Fatalf("Demo failed: %v", err)
	}
}

// runInteractiveDemo 运行交互式演示
func runInteractiveDemo(config *Config) {
	fmt.Println("🎮 Interactive AI Dataset Demo")
	fmt.Println("==============================")
	fmt.Printf("🔗 Connecting to MatrixOne at %s:%d\n", config.Host, config.Port)
	fmt.Printf("📊 Database: %s, User: %s\n", config.Database, config.User)

	demo := NewAIDatasetDemo()
	defer demo.Close()

	if err := demo.Connect(config.DSN); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	// 创建表
	if err := demo.CreateTable(); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	reader := bufio.NewReader(os.Stdin)

	for {
		showInteractiveMenu()
		fmt.Print("请选择操作 (1-8): ")

		choice, _ := reader.ReadString('\n')
		choice = strings.TrimSpace(choice)

		switch choice {
		case "1":
			if err := mockDataMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "2":
			if err := aiAnnotationMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "3":
			if err := humanAnnotationMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "4":
			if err := demo.ShowCurrentState(); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "5":
			if err := timeTravelMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "6":
			if err := unifiedCompareMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "7":
			if err := snapshotMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "8":
			if err := vectorSearchMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "9":
			if err := demo.RunDemo(); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "10":
			fmt.Println("👋 感谢使用 AI Dataset Demo!")
			return
		default:
			fmt.Println("❌ 无效选择，请重新输入")
		}

		fmt.Println("\n按回车键继续...")
		reader.ReadString('\n')
	}
}

// showInteractiveMenu 显示交互式菜单
func showInteractiveMenu() {
	fmt.Println("\n" + strings.Repeat("=", 50))
	fmt.Println("🎯 AI Dataset Demo - 交互式菜单")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Println("1. 📊 生成模拟数据")
	fmt.Println("2. 🤖 AI 模型标注")
	fmt.Println("3. 👤 人类标注")
	fmt.Println("4. 📈 查看当前状态")
	fmt.Println("5. ⏰ 时间旅行查询")
	fmt.Println("6. 🔄 数据比较 (时间点/快照)")
	fmt.Println("7. 📸 快照管理")
	fmt.Println("8. 🔍 向量相似度搜索")
	fmt.Println("9. 🎬 运行完整演示")
	fmt.Println("10. 🚪 退出")
	fmt.Println(strings.Repeat("=", 50))
}

// mockDataMenu 模拟数据菜单
func mockDataMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	fmt.Print("请输入要生成的数据行数 (默认 100): ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	rowCount := 100
	if input != "" {
		if count, err := strconv.Atoi(input); err == nil && count > 0 {
			rowCount = count
		}
	}

	fmt.Printf("🔄 正在生成 %d 行模拟数据...\n", rowCount)
	return demo.MockData(rowCount)
}

// aiAnnotationMenu AI 标注菜单
func aiAnnotationMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	fmt.Print("请输入 AI 模型名称 (默认 AI_model_v1): ")
	modelName, _ := reader.ReadString('\n')
	modelName = strings.TrimSpace(modelName)
	if modelName == "" {
		modelName = "AI_model_v1"
	}

	fmt.Print("请输入要标注的记录 ID (用逗号分隔，如 1,2,3): ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	if input == "" {
		return fmt.Errorf("请输入至少一个记录 ID")
	}

	ids := strings.Split(input, ",")
	var annotations []AnnotationResult

	for _, idStr := range ids {
		id, err := strconv.Atoi(strings.TrimSpace(idStr))
		if err != nil {
			return fmt.Errorf("无效的 ID: %s", idStr)
		}

		fmt.Printf("记录 %d 的标签: ", id)
		label, _ := reader.ReadString('\n')
		label = strings.TrimSpace(label)

		fmt.Printf("记录 %d 的置信度 (0-1): ", id)
		confStr, _ := reader.ReadString('\n')
		confStr = strings.TrimSpace(confStr)

		confidence := 0.9
		if conf, err := strconv.ParseFloat(confStr, 64); err == nil {
			confidence = conf
		}

		annotations = append(annotations, AnnotationResult{
			ID:         id,
			Label:      label,
			Confidence: confidence,
			Annotator:  modelName,
		})
	}

	return demo.AIModelAnnotation(modelName, annotations)
}

// humanAnnotationMenu 人类标注菜单
func humanAnnotationMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	fmt.Print("请输入要标注的记录 ID (用逗号分隔，如 1,2,3): ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	if input == "" {
		return fmt.Errorf("请输入至少一个记录 ID")
	}

	ids := strings.Split(input, ",")
	var annotations []AnnotationResult

	for _, idStr := range ids {
		id, err := strconv.Atoi(strings.TrimSpace(idStr))
		if err != nil {
			return fmt.Errorf("无效的 ID: %s", idStr)
		}

		fmt.Printf("记录 %d 的标签: ", id)
		label, _ := reader.ReadString('\n')
		label = strings.TrimSpace(label)

		fmt.Printf("记录 %d 的标注原因: ", id)
		reason, _ := reader.ReadString('\n')
		reason = strings.TrimSpace(reason)

		annotations = append(annotations, AnnotationResult{
			ID:        id,
			Label:     label,
			Annotator: "human_reviewer",
			Reason:    reason,
		})
	}

	return demo.HumanAnnotation(annotations)
}

// timeTravelMenu 时间旅行菜单
func timeTravelMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	fmt.Print("请输入目标时间 (格式: 2024-01-01 10:00:00): ")
	targetTime, _ := reader.ReadString('\n')
	targetTime = strings.TrimSpace(targetTime)

	if targetTime == "" {
		targetTime = "2024-01-01 10:00:00"
	}

	return demo.TimeTravelQuery(targetTime)
}

// compareTimeMenu 比较时间点菜单
func compareTimeMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	fmt.Print("请输入第一个时间点 (格式: 2024-01-01 10:00:00): ")
	time1, _ := reader.ReadString('\n')
	time1 = strings.TrimSpace(time1)

	if time1 == "" {
		time1 = "2024-01-01 10:00:00"
	}

	fmt.Print("请输入第二个时间点 (格式: 2024-01-01 11:00:00): ")
	time2, _ := reader.ReadString('\n')
	time2 = strings.TrimSpace(time2)

	if time2 == "" {
		time2 = "2024-01-01 11:00:00"
	}

	fmt.Print("选择显示模式 (1=详细差异, 2=统计摘要, 默认=1): ")
	mode, _ := reader.ReadString('\n')
	mode = strings.TrimSpace(mode)

	showDetailed := true
	if mode == "2" {
		showDetailed = false
	}

	return demo.CompareTimePointsWithMode(time1, time2, showDetailed)
}

// snapshotMenu 快照管理菜单
func snapshotMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	for {
		fmt.Println("\n" + strings.Repeat("=", 40))
		fmt.Println("📸 快照管理")
		fmt.Println(strings.Repeat("=", 40))
		fmt.Println("1. 📸 创建快照")
		fmt.Println("2. 📋 查看所有快照")
		fmt.Println("3. 🗑️  删除快照")
		fmt.Println("4. 🗑️🗑️ 删除所有快照")
		fmt.Println("5. 🔙 返回主菜单")
		fmt.Println(strings.Repeat("=", 40))

		fmt.Print("请选择操作 (1-5): ")
		choice, _ := reader.ReadString('\n')
		choice = strings.TrimSpace(choice)

		switch choice {
		case "1":
			if err := createSnapshotMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "2":
			if err := demo.ShowSnapshots(); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "3":
			if err := dropSnapshotMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "4":
			if err := dropAllSnapshotsMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "5":
			return nil
		default:
			fmt.Println("❌ 无效选择，请重新输入")
		}

		fmt.Println("\n按回车键继续...")
		reader.ReadString('\n')
	}
}

// createSnapshotMenu 创建快照菜单
func createSnapshotMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	fmt.Print("请输入快照后缀名称 (如: initial, after_ai, after_human): ")
	suffix, _ := reader.ReadString('\n')
	suffix = strings.TrimSpace(suffix)

	if suffix == "" {
		suffix = "manual"
	}

	return demo.CreateSnapshot(suffix)
}

// dropSnapshotMenu 删除快照菜单
func dropSnapshotMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	fmt.Print("请输入要删除的快照名称: ")
	snapshotName, _ := reader.ReadString('\n')
	snapshotName = strings.TrimSpace(snapshotName)

	if snapshotName == "" {
		return fmt.Errorf("快照名称不能为空")
	}

	return demo.DropSnapshot(snapshotName)
}

// dropAllSnapshotsMenu 删除所有快照菜单
func dropAllSnapshotsMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	fmt.Println("⚠️  警告：此操作将删除所有快照，且无法撤销！")
	fmt.Print("确认删除所有快照吗？(输入 'yes' 确认): ")
	confirmation, _ := reader.ReadString('\n')
	confirmation = strings.TrimSpace(confirmation)

	if confirmation != "yes" {
		fmt.Println("❌ 操作已取消")
		return nil
	}

	return demo.DropAllSnapshots()
}

// getSnapshotList 获取快照列表
func (d *AIDatasetDemo) getSnapshotList() ([]string, error) {
	query := "SHOW SNAPSHOTS"
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshots: %v", err)
	}
	defer rows.Close()

	var snapshotNames []string
	for rows.Next() {
		var snapshotName, timestamp, snapshotLevel, accountName, databaseName, tableName string
		err := rows.Scan(&snapshotName, &timestamp, &snapshotLevel, &accountName, &databaseName, &tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to scan snapshot row: %v", err)
		}
		snapshotNames = append(snapshotNames, snapshotName)
	}

	return snapshotNames, nil
}

// unifiedCompareMenu 统一的数据比较菜单
func unifiedCompareMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	for {
		fmt.Println("\n" + strings.Repeat("=", 60))
		fmt.Println("🔄 数据比较中心")
		fmt.Println(strings.Repeat("=", 60))
		fmt.Println("1. 📸 快照 vs 📸 快照")
		fmt.Println("2. 📸 快照 vs ⏰ 时间戳")
		fmt.Println("3. ⏰ 时间戳 vs ⏰ 时间戳")
		fmt.Println("4. 🔙 返回主菜单")
		fmt.Println(strings.Repeat("=", 60))

		fmt.Print("请选择比较类型 (1-4): ")
		choice, _ := reader.ReadString('\n')
		choice = strings.TrimSpace(choice)

		switch choice {
		case "1":
			if err := compareSnapshotToSnapshot(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "2":
			if err := compareSnapshotToTimestamp(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "3":
			if err := compareTimestampToTimestamp(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "4":
			return nil
		default:
			fmt.Println("❌ 无效选择，请重新输入")
		}

		fmt.Println("\n按回车键继续...")
		reader.ReadString('\n')
	}
}


// compareSnapshotToSnapshot 快照 vs 快照比较
func compareSnapshotToSnapshot(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 获取快照列表
	snapshots, err := demo.getSnapshotList()
	if err != nil {
		return fmt.Errorf("获取快照列表失败: %v", err)
	}

	if len(snapshots) == 0 {
		return fmt.Errorf("没有找到任何快照")
	}

	// 显示候选快照（最多5个）
	fmt.Println("📋 可用的快照:")
	maxShow := 5
	if len(snapshots) < maxShow {
		maxShow = len(snapshots)
	}
	
	for i := 0; i < maxShow; i++ {
		fmt.Printf("  %d. %s\n", i+1, snapshots[i])
	}
	if len(snapshots) > maxShow {
		fmt.Printf("  ... 还有 %d 个快照\n", len(snapshots)-maxShow)
	}
	fmt.Println()

	// 选择第一个快照
	fmt.Print("请输入第一个快照名称 (或输入序号): ")
	input1, _ := reader.ReadString('\n')
	input1 = strings.TrimSpace(input1)

	snapshot1 := input1
	if num, err := strconv.Atoi(input1); err == nil && num >= 1 && num <= len(snapshots) {
		snapshot1 = snapshots[num-1]
		fmt.Printf("✅ 选择快照: %s\n", snapshot1)
	}

	if snapshot1 == "" {
		return fmt.Errorf("快照名称不能为空")
	}

	// 选择第二个快照
	fmt.Print("请输入第二个快照名称 (或输入序号): ")
	input2, _ := reader.ReadString('\n')
	input2 = strings.TrimSpace(input2)

	snapshot2 := input2
	if num, err := strconv.Atoi(input2); err == nil && num >= 1 && num <= len(snapshots) {
		snapshot2 = snapshots[num-1]
		fmt.Printf("✅ 选择快照: %s\n", snapshot2)
	}

	if snapshot2 == "" {
		return fmt.Errorf("快照名称不能为空")
	}

	// 选择显示模式
	fmt.Print("选择显示模式 (1=详细差异, 2=统计摘要, 默认=1): ")
	mode, _ := reader.ReadString('\n')
	mode = strings.TrimSpace(mode)

	showDetailed := true
	if mode == "2" {
		showDetailed = false
	}

	return demo.CompareSnapshotsWithMode(snapshot1, snapshot2, showDetailed)
}

// compareSnapshotToTimestamp 快照 vs 时间戳比较
func compareSnapshotToTimestamp(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 获取快照列表
	snapshots, err := demo.getSnapshotList()
	if err != nil {
		return fmt.Errorf("获取快照列表失败: %v", err)
	}

	if len(snapshots) == 0 {
		return fmt.Errorf("没有找到任何快照")
	}

	// 显示候选快照（最多5个）
	fmt.Println("📋 可用的快照:")
	maxShow := 5
	if len(snapshots) < maxShow {
		maxShow = len(snapshots)
	}
	
	for i := 0; i < maxShow; i++ {
		fmt.Printf("  %d. %s\n", i+1, snapshots[i])
	}
	if len(snapshots) > maxShow {
		fmt.Printf("  ... 还有 %d 个快照\n", len(snapshots)-maxShow)
	}
	fmt.Println()

	// 选择快照
	fmt.Print("请输入快照名称 (或输入序号): ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	snapshot := input
	if num, err := strconv.Atoi(input); err == nil && num >= 1 && num <= len(snapshots) {
		snapshot = snapshots[num-1]
		fmt.Printf("✅ 选择快照: %s\n", snapshot)
	}

	if snapshot == "" {
		return fmt.Errorf("快照名称不能为空")
	}

	// 输入时间戳
	fmt.Print("请输入时间戳 (格式: 2024-01-01 10:00:00): ")
	timestamp, _ := reader.ReadString('\n')
	timestamp = strings.TrimSpace(timestamp)

	if timestamp == "" {
		return fmt.Errorf("时间戳不能为空")
	}

	// 选择显示模式
	fmt.Print("选择显示模式 (1=详细差异, 2=统计摘要, 默认=1): ")
	mode, _ := reader.ReadString('\n')
	mode = strings.TrimSpace(mode)

	showDetailed := true
	if mode == "2" {
		showDetailed = false
	}

	return demo.CompareSnapshotWithTimestamp(snapshot, timestamp, showDetailed)
}

// compareTimestampToTimestamp 时间戳 vs 时间戳比较
func compareTimestampToTimestamp(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 输入第一个时间戳
	fmt.Print("请输入第一个时间戳 (格式: 2024-01-01 10:00:00): ")
	timestamp1, _ := reader.ReadString('\n')
	timestamp1 = strings.TrimSpace(timestamp1)

	if timestamp1 == "" {
		return fmt.Errorf("第一个时间戳不能为空")
	}

	// 输入第二个时间戳
	fmt.Print("请输入第二个时间戳 (格式: 2024-01-01 11:00:00): ")
	timestamp2, _ := reader.ReadString('\n')
	timestamp2 = strings.TrimSpace(timestamp2)

	if timestamp2 == "" {
		return fmt.Errorf("第二个时间戳不能为空")
	}

	// 选择显示模式
	fmt.Print("选择显示模式 (1=详细差异, 2=统计摘要, 默认=1): ")
	mode, _ := reader.ReadString('\n')
	mode = strings.TrimSpace(mode)

	showDetailed := true
	if mode == "2" {
		showDetailed = false
	}

	return demo.CompareTimePointsWithMode(timestamp1, timestamp2, showDetailed)
}

// vectorSearchMenu 向量搜索菜单
func vectorSearchMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	fmt.Print("请输入查询记录 ID: ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	queryID := 1
	if id, err := strconv.Atoi(input); err == nil {
		queryID = id
	}

	fmt.Print("请输入返回结果数量 (默认 5): ")
	input, _ = reader.ReadString('\n')
	input = strings.TrimSpace(input)

	topK := 5
	if k, err := strconv.Atoi(input); err == nil && k > 0 {
		topK = k
	}

	return demo.VectorSimilaritySearch(queryID, topK)
}
