package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
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
	// 首先确保mo_branches数据库存在
	if err := d.ensureBranchesDatabase(); err != nil {
		return fmt.Errorf("failed to create branches database: %v", err)
	}

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS ai_dataset (
		id INT PRIMARY KEY,
		features vecf32(128),
		label VARCHAR(50) DEFAULT 'unlabeled',
		description TEXT NOT NULL,
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

// ensureBranchesDatabase 确保mo_branches数据库存在
func (d *AIDatasetDemo) ensureBranchesDatabase() error {
	_, err := d.db.Exec("CREATE DATABASE IF NOT EXISTS mo_branches")
	if err != nil {
		return fmt.Errorf("failed to create mo_branches database: %v", err)
	}

	// 确保分支管理表存在
	if err := d.ensureBranchManagementTable(); err != nil {
		return fmt.Errorf("failed to create branch management table: %v", err)
	}

	return nil
}

// ensureBranchManagementTable 确保分支管理表存在
func (d *AIDatasetDemo) ensureBranchManagementTable() error {
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS mo_branches.branch_management (
		id INT AUTO_INCREMENT PRIMARY KEY,
		event_type VARCHAR(50) NOT NULL,
		source_database VARCHAR(100) NOT NULL,
		source_table VARCHAR(100) NOT NULL,
		branch_name VARCHAR(100) NOT NULL,
		target_branch VARCHAR(100),
		snapshot_name VARCHAR(200),
		merge_conflicts INT DEFAULT 0,
		merge_resolved INT DEFAULT 0,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		INDEX idx_branch_name (branch_name),
		INDEX idx_created_at (created_at)
	);`

	_, err := d.db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create branch_management table: %v", err)
	}
	return nil
}

// CreateTableBranch 创建表分支（必须基于快照）
func (d *AIDatasetDemo) CreateTableBranch(branchName, snapshotName string) error {
	// 确保mo_branches数据库存在
	if err := d.ensureBranchesDatabase(); err != nil {
		return fmt.Errorf("failed to create branches database: %v", err)
	}

	// 生成分支表名：test_ai_dataset_$branchname
	branchTableName := fmt.Sprintf("test_ai_dataset_%s", branchName)

	// 使用CLONE语法创建表分支，基于指定快照
	cloneSQL := fmt.Sprintf("CREATE TABLE mo_branches.%s CLONE test.ai_dataset {Snapshot = '%s'}", branchTableName, snapshotName)

	_, err := d.db.Exec(cloneSQL)
	if err != nil {
		return fmt.Errorf("failed to create table branch: %v", err)
	}

	// 记录分支创建事件到管理表
	if err := d.recordBranchEvent("CREATE", "test", "ai_dataset", branchName, snapshotName); err != nil {
		fmt.Printf("⚠️  Warning: Failed to record branch event: %v\n", err)
		// 不因为记录失败而停止分支创建
	}

	fmt.Printf("✅ Table branch '%s' created successfully based on snapshot '%s'\n", branchName, snapshotName)
	return nil
}

// recordBranchEvent 记录分支事件到管理表
func (d *AIDatasetDemo) recordBranchEvent(eventType, sourceDB, sourceTable, branchName, snapshotName string) error {
	insertSQL := `
		INSERT INTO mo_branches.branch_management 
		(event_type, source_database, source_table, branch_name, snapshot_name) 
		VALUES (?, ?, ?, ?, ?)`

	_, err := d.db.Exec(insertSQL, eventType, sourceDB, sourceTable, branchName, snapshotName)
	if err != nil {
		return fmt.Errorf("failed to record branch event: %v", err)
	}
	return nil
}

// recordMergeEvent 记录merge事件
func (d *AIDatasetDemo) recordMergeEvent(sourceBranch, targetBranch string, conflicts, resolved int) error {
	insertSQL := `
		INSERT INTO mo_branches.branch_management 
		(event_type, source_database, source_table, branch_name, target_branch, merge_conflicts, merge_resolved) 
		VALUES (?, ?, ?, ?, ?, ?, ?)`

	_, err := d.db.Exec(insertSQL, "MERGE", "test", "ai_dataset", sourceBranch, targetBranch, conflicts, resolved)
	if err != nil {
		return fmt.Errorf("failed to record merge event: %v", err)
	}
	return nil
}

// ListTableBranches 列出所有表分支
func (d *AIDatasetDemo) ListTableBranches() error {
	branches, err := d.getTableBranches()
	if err != nil {
		return err
	}

	fmt.Println("🌿 表分支列表:")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("%-4s %-20s %-30s %-20s\n", "序号", "分支名称", "基于快照", "创建时间")
	fmt.Println(strings.Repeat("-", 80))

	if len(branches) == 0 {
		fmt.Println("📋 没有找到任何分支")
		return nil
	}

	for i, branch := range branches {
		// 查询分支管理表获取快照信息
		snapshotInfo := d.getBranchSnapshotInfo(branch)
		fmt.Printf("%-4d %-20s %-30s %-20s\n",
			i+1,
			branch,
			snapshotInfo.SnapshotName,
			snapshotInfo.CreatedAt)
	}

	fmt.Printf("\n📊 总计: %d 个分支\n", len(branches))
	return nil
}

// BranchSnapshotInfo 分支快照信息
type BranchSnapshotInfo struct {
	SnapshotName string
	CreatedAt    string
}

// ConflictRecord 冲突记录（按行级别）
type ConflictRecord struct {
	ID                int
	SourceLabel       string
	SourceDescription string
	SourceAnnotator   string
	SourceConfidence  string
	SourceReason      string
	TargetLabel       string
	TargetDescription string
	TargetAnnotator   string
	TargetConfidence  string
	TargetReason      string
}

// MergeResult merge结果
type MergeResult struct {
	Conflicts         []ConflictRecord
	TotalConflicts    int
	ResolvedConflicts []ConflictRecord
	ResolutionChoice  map[int]string // ID -> "main" or "branch"
}

// getBranchSnapshotInfo 获取分支的快照信息
func (d *AIDatasetDemo) getBranchSnapshotInfo(branchName string) BranchSnapshotInfo {
	query := `
		SELECT snapshot_name, created_at
		FROM mo_branches.branch_management
		WHERE branch_name = ? AND event_type = 'CREATE'
		ORDER BY created_at DESC
		LIMIT 1`

	var snapshotName sql.NullString
	var createdAt string
	err := d.db.QueryRow(query, branchName).Scan(&snapshotName, &createdAt)
	if err != nil {
		return BranchSnapshotInfo{
			SnapshotName: "未知",
			CreatedAt:    "未知",
		}
	}

	// 格式化时间
	if createdAt != "" {
		if t, err := time.Parse("2006-01-02 15:04:05", createdAt); err == nil {
			createdAt = t.Format("01-02 15:04")
		}
	}

	snapshotNameStr := "未知"
	if snapshotName.Valid && snapshotName.String != "" {
		snapshotNameStr = snapshotName.String
	}

	return BranchSnapshotInfo{
		SnapshotName: snapshotNameStr,
		CreatedAt:    createdAt,
	}
}

// getTableBranches 获取所有表分支名称列表
func (d *AIDatasetDemo) getTableBranches() ([]string, error) {
	// 确保mo_branches数据库存在
	if err := d.ensureBranchesDatabase(); err != nil {
		return nil, fmt.Errorf("failed to create branches database: %v", err)
	}

	// 查询mo_branches数据库中的所有表
	query := "SHOW TABLES FROM mo_branches"
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query table branches: %v", err)
	}
	defer rows.Close()

	var branches []string
	var tableName string
	for rows.Next() {
		err := rows.Scan(&tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to scan table name: %v", err)
		}

		// 只处理以test_ai_dataset_开头的表
		if strings.HasPrefix(tableName, "test_ai_dataset_") {
			branchName := strings.TrimPrefix(tableName, "test_ai_dataset_")
			branches = append(branches, branchName)
		}
	}

	return branches, nil
}

// DropTableBranch 删除表分支
func (d *AIDatasetDemo) DropTableBranch(branchName string) error {
	// 生成分支表名
	branchTableName := fmt.Sprintf("test_ai_dataset_%s", branchName)

	// 删除表分支
	dropSQL := fmt.Sprintf("DROP TABLE mo_branches.%s", branchTableName)

	_, err := d.db.Exec(dropSQL)
	if err != nil {
		return fmt.Errorf("failed to drop table branch: %v", err)
	}

	// 记录分支删除事件到管理表
	if err := d.recordBranchEvent("DROP", "test", "ai_dataset", branchName, ""); err != nil {
		fmt.Printf("⚠️  Warning: Failed to record branch event: %v\n", err)
		// 不因为记录失败而停止分支删除
	}

	fmt.Printf("✅ Table branch '%s' dropped successfully\n", branchName)
	return nil
}

// ShowBranchHistory 显示分支历史记录（类似git log）
func (d *AIDatasetDemo) ShowBranchHistory() error {
	// 确保mo_branches数据库存在
	if err := d.ensureBranchesDatabase(); err != nil {
		return fmt.Errorf("failed to create branches database: %v", err)
	}

	query := `
		SELECT id, event_type, source_database, source_table, branch_name, target_branch, snapshot_name, created_at
		FROM mo_branches.branch_management 
		ORDER BY created_at DESC 
		LIMIT 50`

	rows, err := d.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query branch history: %v", err)
	}
	defer rows.Close()

	fmt.Println("📜 Branch History (类似 git log):")
	fmt.Println(strings.Repeat("=", 80))

	var id int
	var eventType, sourceDB, sourceTable, branchName, createdAt string
	var targetBranch, snapshotName sql.NullString
	recordCount := 0

	for rows.Next() {
		err := rows.Scan(&id, &eventType, &sourceDB, &sourceTable, &branchName, &targetBranch, &snapshotName, &createdAt)
		if err != nil {
			return fmt.Errorf("failed to scan branch history row: %v", err)
		}

		// 格式化显示
		eventIcon := "➕"
		if eventType == "DROP" {
			eventIcon = "🗑️"
		} else if eventType == "MERGE" {
			eventIcon = "🔀"
		}

		if eventType == "MERGE" {
			// Merge事件显示更详细的信息
			targetBranchStr := "main"
			if targetBranch.Valid && targetBranch.String != "" {
				targetBranchStr = targetBranch.String
			}
			fmt.Printf("%s %s | Source: %s → Target: %s\n",
				eventIcon, eventType, branchName, targetBranchStr)
		} else {
			// 其他事件显示原有格式
			fmt.Printf("%s %s | Branch: %s | Source: %s.%s\n",
				eventIcon, eventType, branchName, sourceDB, sourceTable)
		}

		if snapshotName.Valid && snapshotName.String != "" {
			fmt.Printf("   📸 Based on snapshot: %s\n", snapshotName.String)
		}

		fmt.Printf("   ⏰ %s\n", createdAt)
		fmt.Println(strings.Repeat("-", 60))
		recordCount++
	}

	if recordCount == 0 {
		fmt.Println("No branch history found.")
	} else {
		fmt.Printf("\n📊 Total records: %d\n", recordCount)
	}

	return nil
}

// CompareBranches 比较两个分支的数据
func (d *AIDatasetDemo) CompareBranches(branch1Name, branch2Name string, showDetailed bool) error {
	// 生成分支表名
	branch1Table := fmt.Sprintf("mo_branches.test_ai_dataset_%s", branch1Name)
	branch2Table := fmt.Sprintf("mo_branches.test_ai_dataset_%s", branch2Name)

	// 查询两个分支的数据
	query1 := fmt.Sprintf(`
		SELECT id, label, description,
		       JSON_EXTRACT(metadata, '$.annotator') as annotator,
		       JSON_EXTRACT(metadata, '$.confidence') as confidence,
		       JSON_EXTRACT(metadata, '$.reason') as reason,
		       timestamp
		FROM %s 
		ORDER BY id 
		LIMIT 10`, branch1Table)

	query2 := fmt.Sprintf(`
		SELECT id, label, description,
		       JSON_EXTRACT(metadata, '$.annotator') as annotator,
		       JSON_EXTRACT(metadata, '$.confidence') as confidence,
		       JSON_EXTRACT(metadata, '$.reason') as reason,
		       timestamp
		FROM %s 
		ORDER BY id 
		LIMIT 10`, branch2Table)

	// 获取第一个分支的数据
	rows1, err := d.db.Query(query1)
	if err != nil {
		return fmt.Errorf("failed to query branch1 data: %v", err)
	}
	defer rows1.Close()

	var data1 []map[string]interface{}
	for rows1.Next() {
		var id int
		var label, description, timestamp string
		var annotator, reason sql.NullString
		var confidence sql.NullFloat64

		err := rows1.Scan(&id, &label, &description, &annotator, &confidence, &reason, &timestamp)
		if err != nil {
			return fmt.Errorf("failed to scan branch1 row: %v", err)
		}

		row := map[string]interface{}{
			"id":          id,
			"label":       label,
			"description": description,
			"annotator":   annotator.String,
			"confidence":  confidence.Float64,
			"reason":      reason.String,
			"timestamp":   timestamp,
		}
		data1 = append(data1, row)
	}

	// 获取第二个分支的数据
	rows2, err := d.db.Query(query2)
	if err != nil {
		return fmt.Errorf("failed to query branch2 data: %v", err)
	}
	defer rows2.Close()

	var data2 []map[string]interface{}
	for rows2.Next() {
		var id int
		var label, description, timestamp string
		var annotator, reason sql.NullString
		var confidence sql.NullFloat64

		err := rows2.Scan(&id, &label, &description, &annotator, &confidence, &reason, &timestamp)
		if err != nil {
			return fmt.Errorf("failed to scan branch2 row: %v", err)
		}

		row := map[string]interface{}{
			"id":          id,
			"label":       label,
			"description": description,
			"annotator":   annotator.String,
			"confidence":  confidence.Float64,
			"reason":      reason.String,
			"timestamp":   timestamp,
		}
		data2 = append(data2, row)
	}

	// 比较数据
	fmt.Printf("🔄 Branch Comparison: %s vs %s\n", branch1Name, branch2Name)
	fmt.Println(strings.Repeat("=", 80))

	// 转换为DataRecord格式
	records1 := make(map[int]DataRecord)
	records2 := make(map[int]DataRecord)

	for _, row := range data1 {
		id := row["id"].(int)
		confidence := "N/A"
		if conf, ok := row["confidence"].(float64); ok {
			confidence = fmt.Sprintf("%.2f", conf)
		}
		records1[id] = DataRecord{
			ID:         id,
			Label:      row["label"].(string),
			Annotator:  row["annotator"].(string),
			Confidence: confidence,
			Reason:     row["reason"].(string),
			Timestamp:  row["timestamp"].(string),
		}
	}

	for _, row := range data2 {
		id := row["id"].(int)
		confidence := "N/A"
		if conf, ok := row["confidence"].(float64); ok {
			confidence = fmt.Sprintf("%.2f", conf)
		}
		records2[id] = DataRecord{
			ID:         id,
			Label:      row["label"].(string),
			Annotator:  row["annotator"].(string),
			Confidence: confidence,
			Reason:     row["reason"].(string),
			Timestamp:  row["timestamp"].(string),
		}
	}

	if showDetailed {
		d.compareDataDetailed(records1, records2, fmt.Sprintf("Branch: %s", branch1Name), fmt.Sprintf("Branch: %s", branch2Name))
	} else {
		d.compareDataSummary(records1, records2, fmt.Sprintf("Branch: %s", branch1Name), fmt.Sprintf("Branch: %s", branch2Name))
	}

	return nil
}

// CompareBranchWithSnapshot 比较分支和快照的数据
func (d *AIDatasetDemo) CompareBranchWithSnapshot(branchName, snapshotName string, showDetailed bool) error {
	// 生成分支表名
	branchTable := fmt.Sprintf("mo_branches.test_ai_dataset_%s", branchName)

	// 查询分支数据
	branchQuery := fmt.Sprintf(`
		SELECT id, label, description,
		       JSON_EXTRACT(metadata, '$.annotator') as annotator,
		       JSON_EXTRACT(metadata, '$.confidence') as confidence,
		       JSON_EXTRACT(metadata, '$.reason') as reason,
		       timestamp
		FROM %s 
		ORDER BY id 
		LIMIT 10`, branchTable)

	// 查询快照数据
	snapshotQuery := fmt.Sprintf(`
		SELECT id, label, description,
		       JSON_EXTRACT(metadata, '$.annotator') as annotator,
		       JSON_EXTRACT(metadata, '$.confidence') as confidence,
		       JSON_EXTRACT(metadata, '$.reason') as reason,
		       timestamp
		FROM ai_dataset {Snapshot = '%s'}
		ORDER BY id 
		LIMIT 10`, snapshotName)

	// 获取分支数据
	rows1, err := d.db.Query(branchQuery)
	if err != nil {
		return fmt.Errorf("failed to query branch data: %v", err)
	}
	defer rows1.Close()

	var data1 []map[string]interface{}
	for rows1.Next() {
		var id int
		var label, description, timestamp string
		var annotator, reason sql.NullString
		var confidence sql.NullFloat64

		err := rows1.Scan(&id, &label, &description, &annotator, &confidence, &reason, &timestamp)
		if err != nil {
			return fmt.Errorf("failed to scan branch row: %v", err)
		}

		row := map[string]interface{}{
			"id":          id,
			"label":       label,
			"description": description,
			"annotator":   annotator.String,
			"confidence":  confidence.Float64,
			"reason":      reason.String,
			"timestamp":   timestamp,
		}
		data1 = append(data1, row)
	}

	// 获取快照数据
	rows2, err := d.db.Query(snapshotQuery)
	if err != nil {
		return fmt.Errorf("failed to query snapshot data: %v", err)
	}
	defer rows2.Close()

	var data2 []map[string]interface{}
	for rows2.Next() {
		var id int
		var label, description, timestamp string
		var annotator, reason sql.NullString
		var confidence sql.NullFloat64

		err := rows2.Scan(&id, &label, &description, &annotator, &confidence, &reason, &timestamp)
		if err != nil {
			return fmt.Errorf("failed to scan snapshot row: %v", err)
		}

		row := map[string]interface{}{
			"id":          id,
			"label":       label,
			"description": description,
			"annotator":   annotator.String,
			"confidence":  confidence.Float64,
			"reason":      reason.String,
			"timestamp":   timestamp,
		}
		data2 = append(data2, row)
	}

	// 比较数据
	fmt.Printf("🔄 Branch vs Snapshot Comparison: %s vs %s\n", branchName, snapshotName)
	fmt.Println(strings.Repeat("=", 80))

	// 转换为DataRecord格式
	records1 := make(map[int]DataRecord)
	records2 := make(map[int]DataRecord)

	for _, row := range data1 {
		id := row["id"].(int)
		confidence := "N/A"
		if conf, ok := row["confidence"].(float64); ok {
			confidence = fmt.Sprintf("%.2f", conf)
		}
		records1[id] = DataRecord{
			ID:         id,
			Label:      row["label"].(string),
			Annotator:  row["annotator"].(string),
			Confidence: confidence,
			Reason:     row["reason"].(string),
			Timestamp:  row["timestamp"].(string),
		}
	}

	for _, row := range data2 {
		id := row["id"].(int)
		confidence := "N/A"
		if conf, ok := row["confidence"].(float64); ok {
			confidence = fmt.Sprintf("%.2f", conf)
		}
		records2[id] = DataRecord{
			ID:         id,
			Label:      row["label"].(string),
			Annotator:  row["annotator"].(string),
			Confidence: confidence,
			Reason:     row["reason"].(string),
			Timestamp:  row["timestamp"].(string),
		}
	}

	if showDetailed {
		d.compareDataDetailed(records1, records2, fmt.Sprintf("Branch: %s", branchName), fmt.Sprintf("Snapshot: %s", snapshotName))
	} else {
		d.compareDataSummary(records1, records2, fmt.Sprintf("Branch: %s", branchName), fmt.Sprintf("Snapshot: %s", snapshotName))
	}

	return nil
}

// CompareBranchWithMainTable 比较分支和主表最新数据
func (d *AIDatasetDemo) CompareBranchWithMainTable(branchName string, showDetailed bool) error {
	// 生成分支表名
	branchTable := fmt.Sprintf("mo_branches.test_ai_dataset_%s", branchName)

	// 查询分支数据
	branchQuery := fmt.Sprintf(`
		SELECT id, label, description,
		       JSON_EXTRACT(metadata, '$.annotator') as annotator,
		       JSON_EXTRACT(metadata, '$.confidence') as confidence,
		       JSON_EXTRACT(metadata, '$.reason') as reason,
		       timestamp
		FROM %s 
		ORDER BY id 
		LIMIT 10`, branchTable)

	// 查询主表最新数据
	mainQuery := `
		SELECT id, label, description,
		       JSON_EXTRACT(metadata, '$.annotator') as annotator,
		       JSON_EXTRACT(metadata, '$.confidence') as confidence,
		       JSON_EXTRACT(metadata, '$.reason') as reason,
		       timestamp
		FROM ai_dataset 
		ORDER BY id 
		LIMIT 10`

	// 获取分支数据
	rows1, err := d.db.Query(branchQuery)
	if err != nil {
		return fmt.Errorf("failed to query branch data: %v", err)
	}
	defer rows1.Close()

	var data1 []map[string]interface{}
	for rows1.Next() {
		var id int
		var label, description, timestamp string
		var annotator, reason sql.NullString
		var confidence sql.NullFloat64

		err := rows1.Scan(&id, &label, &description, &annotator, &confidence, &reason, &timestamp)
		if err != nil {
			return fmt.Errorf("failed to scan branch row: %v", err)
		}

		row := map[string]interface{}{
			"id":          id,
			"label":       label,
			"description": description,
			"annotator":   annotator.String,
			"confidence":  confidence.Float64,
			"reason":      reason.String,
			"timestamp":   timestamp,
		}
		data1 = append(data1, row)
	}

	// 获取主表数据
	rows2, err := d.db.Query(mainQuery)
	if err != nil {
		return fmt.Errorf("failed to query main table data: %v", err)
	}
	defer rows2.Close()

	var data2 []map[string]interface{}
	for rows2.Next() {
		var id int
		var label, description, timestamp string
		var annotator, reason sql.NullString
		var confidence sql.NullFloat64

		err := rows2.Scan(&id, &label, &description, &annotator, &confidence, &reason, &timestamp)
		if err != nil {
			return fmt.Errorf("failed to scan main table row: %v", err)
		}

		row := map[string]interface{}{
			"id":          id,
			"label":       label,
			"description": description,
			"annotator":   annotator.String,
			"confidence":  confidence.Float64,
			"reason":      reason.String,
			"timestamp":   timestamp,
		}
		data2 = append(data2, row)
	}

	// 比较数据 - 主表作为baseline
	fmt.Printf("🔄 Branch vs Main Table Comparison (Main Table as Baseline)\n")
	fmt.Printf("📊 Baseline: Main Table | 🌿 Branch: %s\n", branchName)
	fmt.Println(strings.Repeat("=", 80))

	// 转换为DataRecord格式 - 主表作为records1 (baseline)，分支作为records2 (comparison)
	baselineRecords := make(map[int]DataRecord)   // 主表作为baseline
	comparisonRecords := make(map[int]DataRecord) // 分支作为比较对象

	// 主表数据作为baseline (records1)
	for _, row := range data2 {
		id := row["id"].(int)
		confidence := "N/A"
		if conf, ok := row["confidence"].(float64); ok {
			confidence = fmt.Sprintf("%.2f", conf)
		}
		baselineRecords[id] = DataRecord{
			ID:         id,
			Label:      row["label"].(string),
			Annotator:  row["annotator"].(string),
			Confidence: confidence,
			Reason:     row["reason"].(string),
			Timestamp:  row["timestamp"].(string),
		}
	}

	// 分支数据作为比较对象 (records2)
	for _, row := range data1 {
		id := row["id"].(int)
		confidence := "N/A"
		if conf, ok := row["confidence"].(float64); ok {
			confidence = fmt.Sprintf("%.2f", conf)
		}
		comparisonRecords[id] = DataRecord{
			ID:         id,
			Label:      row["label"].(string),
			Annotator:  row["annotator"].(string),
			Confidence: confidence,
			Reason:     row["reason"].(string),
			Timestamp:  row["timestamp"].(string),
		}
	}

	if showDetailed {
		d.compareDataDetailed(baselineRecords, comparisonRecords, "📊 Main Table (Baseline)", fmt.Sprintf("🌿 Branch: %s", branchName))
	} else {
		d.compareDataSummary(baselineRecords, comparisonRecords, "📊 Main Table (Baseline)", fmt.Sprintf("🌿 Branch: %s", branchName))
	}

	return nil
}

// DetectConflicts 检测两个分支之间的冲突
func (d *AIDatasetDemo) DetectConflicts(sourceBranch, targetBranch string) (*MergeResult, error) {
	var sourceTable, targetTable string

	if sourceBranch == "main" {
		// 源分支是主表
		sourceTable = "ai_dataset"
	} else {
		// 源分支是另一个分支
		sourceTable = fmt.Sprintf("mo_branches.test_ai_dataset_%s", sourceBranch)
	}

	if targetBranch == "main" {
		// 目标分支是主表
		targetTable = "ai_dataset"
	} else {
		// 目标分支是另一个分支
		targetTable = fmt.Sprintf("mo_branches.test_ai_dataset_%s", targetBranch)
	}

	query := fmt.Sprintf(`
		SELECT 
			s.id,
			s.label as source_label,
			s.description as source_description,
			JSON_EXTRACT(s.metadata, '$.annotator') as source_annotator,
			JSON_EXTRACT(s.metadata, '$.confidence') as source_confidence,
			JSON_EXTRACT(s.metadata, '$.reason') as source_reason,
			t.label as target_label,
			t.description as target_description,
			JSON_EXTRACT(t.metadata, '$.annotator') as target_annotator,
			JSON_EXTRACT(t.metadata, '$.confidence') as target_confidence,
			JSON_EXTRACT(t.metadata, '$.reason') as target_reason
		FROM %s s
		INNER JOIN %s t ON s.id = t.id
		WHERE s.label != t.label 
		   OR s.description != t.description
		   OR JSON_EXTRACT(s.metadata, '$.annotator') != JSON_EXTRACT(t.metadata, '$.annotator')
		   OR JSON_EXTRACT(s.metadata, '$.confidence') != JSON_EXTRACT(t.metadata, '$.confidence')
		   OR JSON_EXTRACT(s.metadata, '$.reason') != JSON_EXTRACT(t.metadata, '$.reason')
		ORDER BY s.id`, sourceTable, targetTable)

	rows, err := d.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to detect conflicts: %v", err)
	}
	defer rows.Close()

	var conflicts []ConflictRecord
	for rows.Next() {
		var id int
		var sourceLabel, sourceDescription, targetLabel, targetDescription string
		var sourceAnnotator, sourceReason, targetAnnotator, targetReason sql.NullString
		var sourceConfidence, targetConfidence sql.NullFloat64

		err := rows.Scan(&id, &sourceLabel, &sourceDescription, &sourceAnnotator, &sourceConfidence, &sourceReason,
			&targetLabel, &targetDescription, &targetAnnotator, &targetConfidence, &targetReason)
		if err != nil {
			return nil, fmt.Errorf("failed to scan conflict row: %v", err)
		}

		// 处理NULL值
		sourceAnnotatorStr := "N/A"
		if sourceAnnotator.Valid {
			sourceAnnotatorStr = sourceAnnotator.String
		}
		sourceReasonStr := "N/A"
		if sourceReason.Valid {
			sourceReasonStr = sourceReason.String
		}
		targetAnnotatorStr := "N/A"
		if targetAnnotator.Valid {
			targetAnnotatorStr = targetAnnotator.String
		}
		targetReasonStr := "N/A"
		if targetReason.Valid {
			targetReasonStr = targetReason.String
		}

		sourceConfidenceStr := "N/A"
		if sourceConfidence.Valid {
			sourceConfidenceStr = fmt.Sprintf("%.2f", sourceConfidence.Float64)
		}
		targetConfidenceStr := "N/A"
		if targetConfidence.Valid {
			targetConfidenceStr = fmt.Sprintf("%.2f", targetConfidence.Float64)
		}

		// 按行级别创建冲突记录（同一ID的所有差异算作一个冲突）
		conflicts = append(conflicts, ConflictRecord{
			ID:                id,
			SourceLabel:       sourceLabel,
			SourceDescription: sourceDescription,
			SourceAnnotator:   sourceAnnotatorStr,
			SourceConfidence:  sourceConfidenceStr,
			SourceReason:      sourceReasonStr,
			TargetLabel:       targetLabel,
			TargetDescription: targetDescription,
			TargetAnnotator:   targetAnnotatorStr,
			TargetConfidence:  targetConfidenceStr,
			TargetReason:      targetReasonStr,
		})
	}

	return &MergeResult{
		Conflicts:         conflicts,
		TotalConflicts:    len(conflicts),
		ResolvedConflicts: []ConflictRecord{},
		ResolutionChoice:  make(map[int]string),
	}, nil
}

// ShowConflicts 显示冲突列表（按行级别）
func (d *AIDatasetDemo) ShowConflicts(conflicts []ConflictRecord, startIndex int, sourceBranch, targetBranch string) {
	fmt.Printf("\n🔍 冲突列表 (显示 %d-%d 条，共 %d 条冲突)\n",
		startIndex+1, min(startIndex+5, len(conflicts)), len(conflicts))
	fmt.Println(strings.Repeat("=", 120))
	fmt.Printf("%-4s %-15s %-15s %-15s %-15s %-15s %-15s\n",
		"ID", "源分支Label", "目标分支Label", "源分支描述", "目标分支描述", "源分支标注者", "目标分支标注者")
	fmt.Println(strings.Repeat("-", 120))

	endIndex := min(startIndex+5, len(conflicts))
	for i := startIndex; i < endIndex; i++ {
		conflict := conflicts[i]
		sourceLabel := truncateText(conflict.SourceLabel, 13)
		targetLabel := truncateText(conflict.TargetLabel, 13)
		sourceDesc := truncateText(conflict.SourceDescription, 13)
		targetDesc := truncateText(conflict.TargetDescription, 13)
		sourceAnnotator := truncateText(conflict.SourceAnnotator, 13)
		targetAnnotator := truncateText(conflict.TargetAnnotator, 13)

		fmt.Printf("%-4d %-15s %-15s %-15s %-15s %-15s %-15s\n",
			conflict.ID, sourceLabel, targetLabel, sourceDesc, targetDesc, sourceAnnotator, targetAnnotator)
	}

	if len(conflicts) > startIndex+5 {
		fmt.Printf("\n按 'n' 继续查看，按 'e' 结束扫描\n")
	} else {
		fmt.Printf("\n已显示所有冲突\n")
	}
}

// min 辅助函数
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ResolveConflicts 冲突解决界面
func (d *AIDatasetDemo) ResolveConflicts(mergeResult *MergeResult, sourceBranch, targetBranch string, reader *bufio.Reader) error {
	for {
		fmt.Println("\n🔧 冲突解决面板")
		fmt.Println(strings.Repeat("=", 50))
		fmt.Printf("源分支: %s\n", sourceBranch)
		fmt.Printf("目标分支: %s\n", targetBranch)
		fmt.Printf("总冲突数: %d\n", mergeResult.TotalConflicts)
		fmt.Printf("已解决: %d\n", len(mergeResult.ResolutionChoice))
		fmt.Printf("待解决: %d\n", mergeResult.TotalConflicts-len(mergeResult.ResolutionChoice))
		fmt.Println(strings.Repeat("=", 50))
		fmt.Println("1. 📋 查看所有冲突")
		fmt.Printf("2. ✅ 全部接受源分支版本 (%s)\n", sourceBranch)
		fmt.Printf("3. ✅ 全部接受目标分支版本 (%s)\n", targetBranch)
		fmt.Println("4. 🎯 选择性解决冲突")
		fmt.Println("5. 🚀 执行 Merge")
		fmt.Println("6. ❌ 退出 (不执行任何操作)")
		fmt.Print("请选择操作 (1-6): ")

		choice, _ := reader.ReadString('\n')
		choice = strings.TrimSpace(choice)

		switch choice {
		case "1":
			d.showAllConflicts(mergeResult.Conflicts, sourceBranch, targetBranch, reader)
		case "2":
			d.acceptAllSource(mergeResult)
		case "3":
			d.acceptAllTarget(mergeResult)
		case "4":
			d.selectiveResolve(mergeResult, reader)
		case "5":
			return d.executeMerge(mergeResult, sourceBranch, targetBranch)
		case "6":
			fmt.Println("❌ 已取消 Merge 操作")
			return nil
		default:
			fmt.Println("❌ 无效选择，请重新输入")
		}
	}
}

// showAllConflicts 显示所有冲突
func (d *AIDatasetDemo) showAllConflicts(conflicts []ConflictRecord, sourceBranch, targetBranch string, reader *bufio.Reader) {
	startIndex := 0
	for {
		d.ShowConflicts(conflicts, startIndex, sourceBranch, targetBranch)

		if startIndex+5 >= len(conflicts) {
			break
		}

		fmt.Print("按 'n' 继续，按 'e' 结束: ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if strings.ToLower(input) == "e" {
			break
		} else if strings.ToLower(input) == "n" {
			startIndex += 5
		}
	}
}

// acceptAllSource 全部接受源分支版本
func (d *AIDatasetDemo) acceptAllSource(mergeResult *MergeResult) {
	// 清空之前的解决记录
	mergeResult.ResolvedConflicts = []ConflictRecord{}
	mergeResult.ResolutionChoice = make(map[int]string)

	// 设置所有冲突为接受源分支版本
	for _, conflict := range mergeResult.Conflicts {
		mergeResult.ResolutionChoice[conflict.ID] = "source"
		mergeResult.ResolvedConflicts = append(mergeResult.ResolvedConflicts, conflict)
	}
	fmt.Println("✅ 已设置全部接受源分支版本")
}

// acceptAllTarget 全部接受目标分支版本
func (d *AIDatasetDemo) acceptAllTarget(mergeResult *MergeResult) {
	// 清空之前的解决记录
	mergeResult.ResolvedConflicts = []ConflictRecord{}
	mergeResult.ResolutionChoice = make(map[int]string)

	// 设置所有冲突为接受目标分支版本
	for _, conflict := range mergeResult.Conflicts {
		mergeResult.ResolutionChoice[conflict.ID] = "target"
		mergeResult.ResolvedConflicts = append(mergeResult.ResolvedConflicts, conflict)
	}
	fmt.Println("✅ 已设置全部接受目标分支版本")
}

// selectiveResolve 选择性解决冲突
func (d *AIDatasetDemo) selectiveResolve(mergeResult *MergeResult, reader *bufio.Reader) error {
	fmt.Println("\n🎯 选择性解决冲突")
	fmt.Println(strings.Repeat("=", 50))

	startIndex := 0
	for {
		// 显示当前批次的冲突
		endIndex := min(startIndex+5, len(mergeResult.Conflicts))
		fmt.Printf("\n处理冲突 %d-%d (共 %d 个)\n", startIndex+1, endIndex, len(mergeResult.Conflicts))

		for i := startIndex; i < endIndex; i++ {
			conflict := mergeResult.Conflicts[i]

			// 检查是否已经解决
			if _, resolved := mergeResult.ResolutionChoice[conflict.ID]; resolved {
				fmt.Printf("✅ ID %d - 已解决\n", conflict.ID)
				continue
			}

			fmt.Printf("\n🔍 冲突 ID %d - 整行冲突\n", conflict.ID)
			fmt.Printf("📊 源分支: Label=%s, 描述=%s, 标注者=%s, 置信度=%s, 原因=%s\n",
				conflict.SourceLabel, conflict.SourceDescription, conflict.SourceAnnotator,
				conflict.SourceConfidence, conflict.SourceReason)
			fmt.Printf("🌿 目标分支: Label=%s, 描述=%s, 标注者=%s, 置信度=%s, 原因=%s\n",
				conflict.TargetLabel, conflict.TargetDescription, conflict.TargetAnnotator,
				conflict.TargetConfidence, conflict.TargetReason)
			fmt.Print("选择: (s)源分支整行, (t)目标分支整行, (k)跳过: ")

			choice, _ := reader.ReadString('\n')
			choice = strings.TrimSpace(strings.ToLower(choice))

			switch choice {
			case "s":
				mergeResult.ResolutionChoice[conflict.ID] = "source"
				// 添加到已解决列表（如果还没有的话）
				found := false
				for _, resolved := range mergeResult.ResolvedConflicts {
					if resolved.ID == conflict.ID {
						found = true
						break
					}
				}
				if !found {
					mergeResult.ResolvedConflicts = append(mergeResult.ResolvedConflicts, conflict)
				}
				fmt.Println("✅ 已选择源分支版本")
			case "t":
				mergeResult.ResolutionChoice[conflict.ID] = "target"
				// 添加到已解决列表（如果还没有的话）
				found := false
				for _, resolved := range mergeResult.ResolvedConflicts {
					if resolved.ID == conflict.ID {
						found = true
						break
					}
				}
				if !found {
					mergeResult.ResolvedConflicts = append(mergeResult.ResolvedConflicts, conflict)
				}
				fmt.Println("✅ 已选择目标分支版本")
			case "k":
				fmt.Println("⏭️ 跳过此冲突")
			default:
				fmt.Println("❌ 无效选择，跳过")
			}
		}

		if endIndex >= len(mergeResult.Conflicts) {
			break
		}

		fmt.Print("\n按 'n' 继续下一批，按 'e' 结束: ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(strings.ToLower(input))

		if input == "e" {
			break
		} else if input == "n" {
			startIndex += 5
		}
	}

	return nil
}

// executeMerge 执行merge操作
func (d *AIDatasetDemo) executeMerge(mergeResult *MergeResult, sourceBranch, targetBranch string) error {
	if len(mergeResult.ResolutionChoice) == 0 {
		return fmt.Errorf("没有解决任何冲突，无法执行 merge")
	}

	fmt.Printf("\n🚀 正在执行 Merge 操作...\n")
	fmt.Printf("源分支: %s\n", sourceBranch)
	fmt.Printf("目标分支: %s\n", targetBranch)
	fmt.Printf("解决冲突数: %d/%d\n", len(mergeResult.ResolutionChoice), mergeResult.TotalConflicts)

	var sourceTable, targetTable string

	if sourceBranch == "main" {
		sourceTable = "ai_dataset"
	} else {
		sourceTable = fmt.Sprintf("mo_branches.test_ai_dataset_%s", sourceBranch)
	}

	if targetBranch == "main" {
		targetTable = "ai_dataset"
	} else {
		targetTable = fmt.Sprintf("mo_branches.test_ai_dataset_%s", targetBranch)
	}
	successCount := 0
	errorCount := 0

	for conflictID, choice := range mergeResult.ResolutionChoice {
		var updateQuery string
		var err error

		if choice == "source" {
			// 使用源分支的整行数据更新目标分支
			updateQuery = fmt.Sprintf(`
				UPDATE %s 
				SET label = (SELECT label FROM %s WHERE id = ?),
				    description = (SELECT description FROM %s WHERE id = ?),
				    metadata = (SELECT metadata FROM %s WHERE id = ?),
				    timestamp = (SELECT timestamp FROM %s WHERE id = ?)
				WHERE id = ?`, targetTable, sourceTable, sourceTable, sourceTable, sourceTable)
		} else {
			// choice == "target" - 保持目标分支的值不变
			continue
		}

		_, err = d.db.Exec(updateQuery, conflictID, conflictID, conflictID, conflictID, conflictID)
		if err != nil {
			fmt.Printf("❌ 更新记录 %d 失败: %v\n", conflictID, err)
			errorCount++
		} else {
			successCount++
		}
	}

	fmt.Printf("\n📊 Merge 执行结果:\n")
	fmt.Printf("✅ 成功更新: %d 条记录\n", successCount)
	if errorCount > 0 {
		fmt.Printf("❌ 失败: %d 条记录\n", errorCount)
	}

	if errorCount == 0 {
		fmt.Println("🎉 Merge 操作完成！")
	} else {
		fmt.Println("⚠️ Merge 操作部分完成，请检查错误信息")
	}

	// 记录merge事件
	if err := d.recordMergeEvent(sourceBranch, targetBranch, mergeResult.TotalConflicts, len(mergeResult.ResolutionChoice)); err != nil {
		fmt.Printf("⚠️  Warning: Failed to record merge event: %v\n", err)
	}

	// 如果目标分支是主表，自动生成新快照
	if targetBranch == "main" {
		snapshotName := fmt.Sprintf("merge_%s_to_main_%s", sourceBranch, time.Now().Format("20060102_150405"))
		if err := d.CreateSnapshot(snapshotName); err != nil {
			fmt.Printf("⚠️  Warning: Failed to create snapshot after merge: %v\n", err)
		} else {
			fmt.Printf("📸 已自动创建快照: %s\n", snapshotName)
		}
	}

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

// generateAnimalDescription 生成动物描述文字
func (d *AIDatasetDemo) generateAnimalDescription(id int) string {
	// 预定义的动物描述（不包含动物名称，每个都有不同的开头）
	descriptions := []string{
		// 第一个必须是snake的描述
		"Slithers gracefully with elongated body and no limbs, using muscular contractions to move across surfaces. Possesses heat-sensing abilities to detect temperature changes in the environment.",
		// 第三个必须是cat的描述
		"Features sharp claws and acute hearing, capable of navigating in complete darkness. Prefers resting in elevated positions and demonstrates exceptional balance and agility.",
		// 其他30个不同的描述
		"Displays powerful limbs and keen sense of smell, excels at navigating complex terrain. Typically lives in social groups with strong territorial instincts.",
		"Exhibits thick fur and powerful jaws, survives harsh environmental conditions. Undergoes hibernation with significantly reduced metabolic rates.",
		"Demonstrates long neck and strong legs, capable of rapid running speeds. Feeds primarily on vegetation with complex digestive systems.",
		"Possesses sharp beak and powerful wings, capable of extended aerial flight. Shows excellent navigational skills for long-distance migration.",
		"Shows streamlined body and powerful tail fin, excels at rapid swimming. Has acute hearing to detect vibrations in water.",
		"Presents thick skin and strong limbs, adapts to both aquatic and terrestrial environments. Demonstrates excellent swimming abilities and enjoys mud baths.",
		"Reveals long trunk and massive size, represents the largest land mammal. Exhibits remarkable memory to remember water sources and food locations.",
		"Displays black and white coloration with rounded body shape, primarily feeds on bamboo. Shows gentle temperament and prefers solitary living.",
		"Manifests long neck and spotted pattern, stands as the world's tallest animal. Features strong legs capable of powerful kicks against predators.",
		"Shows striped pattern and powerful jumping ability, excels at rapid running across grasslands. Possesses keen vision to spot prey from great distances.",
		"Carries thick blubber layer and white coloration, survives in polar environments. Demonstrates excellent swimming skills and feeds mainly on fish.",
		"Displays long tail and strong limbs, moves agilely through tree canopies. Has acute vision and hearing for forest foraging.",
		"Equips sharp teeth and powerful jaws, serves as apex predator in marine environments. Possesses keen sense of smell to detect blood in water.",
		"Features massive wingspan and robust skeletal structure, capable of extended high-altitude soaring. Has exceptional vision to spot small animals from great heights.",
		"Utilizes superior night vision and silent flight techniques, serves as perfect nocturnal hunter. Features rotatable head with wide field of view.",
		"Employs unique echolocation system for precise navigation in complete darkness. Has flexible wing membranes for precise flight control.",
		"Adopts hard shell and retractable head, withdraws into protective ball when threatened. Moves slowly but steadily with lifespan reaching several decades.",
		"Uses variable coloration and soft tentacles for perfect environmental camouflage. Possesses highly developed nervous system and learning capabilities.",
		"Configures sharp horns and powerful hind legs, delivers powerful kicks when threatened. Shows exceptional jumping ability over great distances.",
		"Equips heavy armor and powerful claws, survives in both aquatic and terrestrial environments. Has complex social structure and territorial behavior.",
		"Blooms with colorful plumage and powerful flight capability, performs complex aerial maneuvers. Demonstrates high intelligence and vocal mimicry skills.",
		"Carries sensitive antennae and hard shell, detects subtle environmental changes. Features spiral shell for complete body protection.",
		"Maintains slimy skin and powerful regeneration ability, regrows lost limbs. Has unique respiratory system for both aquatic and terrestrial life.",
		"Equips sharp spines and bright warning colors to deter potential predators. Possesses special venom glands for toxin release.",
		"Uses extremely long tongue and sticky secretions, specializes in catching small insects. Has color-changing ability to match environment.",
		"Demonstrates powerful chewing ability and complex social organization, builds intricate nest structures. Shows high level of teamwork and cooperation.",
		"Maintains transparent body and graceful swimming posture, appears like floating spirit in water. Has simple but effective neural network system.",
		"Features multiple hearts and blue blood, possesses extremely high intelligence and learning ability. Can use tools and shows complex problem-solving skills.",
		"Exerts powerful visual system and rapid running speed, serves as speed king of grasslands. Has sophisticated group hunting tactics and cooperation.",
		"Uses sharp claws and strong arms, excels at rapid movement through tree canopies. Possesses complex social behavior and emotional expression.",
		"Adopts thick scales and powerful defense capability, resists most external attacks. Has ancient lineage and long evolutionary history.",
	}

	// 确保第一个和第三个是特定的描述
	if id == 1 {
		return descriptions[0] // snake描述
	} else if id == 3 {
		return descriptions[1] // cat描述
	} else {
		// 其他随机选择
		rand.Seed(time.Now().UnixNano() + int64(id))
		return descriptions[rand.Intn(len(descriptions))]
	}
}

// truncateText 截断文本并添加省略号
func truncateText(text string, maxLen int) string {
	if len(text) <= maxLen {
		return text
	}
	return text[:maxLen] + fmt.Sprintf("(...%d字)", len(text)-maxLen)
}

// MockData 生成指定行数的模拟数据
func (d *AIDatasetDemo) MockData(rowCount int) error {
	// 确保3小时PITR存在
	if err := d.ensurePITRExists(); err != nil {
		fmt.Printf("⚠️  Warning: Failed to create PITR: %v\n", err)
		// 继续执行，不因为PITR创建失败而停止数据生成
	}

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
			description := d.generateAnimalDescription(j + 1)
			// 转义单引号
			description = strings.ReplaceAll(description, "'", "''")
			values = append(values, fmt.Sprintf("(%d, '%s', 'unlabeled', '%s', NULL, CURRENT_TIMESTAMP)", j+1, vector, description))
		}

		insertSQL := fmt.Sprintf("INSERT INTO ai_dataset (id, features, label, description, metadata, timestamp) VALUES %s",
			strings.Join(values, ", "))

		_, err := d.db.Exec(insertSQL)
		if err != nil {
			return fmt.Errorf("failed to insert batch data: %v", err)
		}

		fmt.Printf("📊 Inserted rows %d-%d\n", i+1, end)
	}

	fmt.Printf("✅ Successfully generated %d rows of mock data!\n", rowCount)

	// 自动创建初始化快照
	fmt.Println("📸 Creating initial snapshot...")
	if err := d.createInitialSnapshot(); err != nil {
		fmt.Printf("⚠️  Warning: Failed to create initial snapshot: %v\n", err)
		// 继续执行，不因为快照创建失败而停止
	}

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

	// 自动创建标注后快照
	fmt.Println("📸 Creating annotation snapshot...")
	sequence := d.getNextSequenceNumber(modelName)
	if err := d.createAnnotationSnapshot(modelName, sequence); err != nil {
		fmt.Printf("⚠️  Warning: Failed to create annotation snapshot: %v\n", err)
		// 继续执行，不因为快照创建失败而停止
	}

	return nil
}

// AIModelAnnotationOnBranch 在分支上进行AI模型标注
func (d *AIDatasetDemo) AIModelAnnotationOnBranch(branchName, modelName string, annotations []AnnotationResult) error {
	branchTable := fmt.Sprintf("mo_branches.test_ai_dataset_%s", branchName)

	fmt.Printf("🤖 正在分支 %s 上进行 AI 标注...\n", branchName)

	for _, annotation := range annotations {
		// 构建metadata JSON字符串，与主表标注保持一致
		metadata := fmt.Sprintf(`{"annotator": "%s", "confidence": %.2f}`,
			annotation.Annotator, annotation.Confidence)

		// 更新分支表中的记录
		updateQuery := fmt.Sprintf(`
			UPDATE %s 
			SET label = ?, metadata = ?, timestamp = CURRENT_TIMESTAMP 
			WHERE id = ?`, branchTable)

		_, err := d.db.Exec(updateQuery, annotation.Label, metadata, annotation.ID)

		if err != nil {
			return fmt.Errorf("failed to update branch record %d: %v", annotation.ID, err)
		}

		fmt.Printf("✅ 记录 %d 已标注: %s (置信度: %.2f)\n",
			annotation.ID, annotation.Label, annotation.Confidence)
	}

	fmt.Printf("🎉 分支 %s 上的 AI 标注完成！\n", branchName)
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

	// 自动创建标注后快照
	fmt.Println("📸 Creating annotation snapshot...")
	sequence := d.getNextSequenceNumber("human")
	if err := d.createAnnotationSnapshot("human", sequence); err != nil {
		fmt.Printf("⚠️  Warning: Failed to create annotation snapshot: %v\n", err)
		// 继续执行，不因为快照创建失败而停止
	}

	return nil
}

// HumanAnnotationOnBranch 在分支上进行人类标注
func (d *AIDatasetDemo) HumanAnnotationOnBranch(branchName string, annotations []AnnotationResult) error {
	branchTable := fmt.Sprintf("mo_branches.test_ai_dataset_%s", branchName)

	fmt.Printf("👤 正在分支 %s 上进行人类标注...\n", branchName)

	for _, annotation := range annotations {
		// 构建metadata JSON字符串，与主表标注保持一致
		metadata := fmt.Sprintf(`{"annotator": "human_reviewer", "reason": "%s"}`,
			annotation.Reason)

		// 更新分支表中的记录
		updateQuery := fmt.Sprintf(`
			UPDATE %s 
			SET label = ?, metadata = ?, timestamp = CURRENT_TIMESTAMP 
			WHERE id = ?`, branchTable)

		_, err := d.db.Exec(updateQuery, annotation.Label, metadata, annotation.ID)

		if err != nil {
			return fmt.Errorf("failed to update branch record %d: %v", annotation.ID, err)
		}

		fmt.Printf("✅ 记录 %d 已标注: %s (原因: %s)\n",
			annotation.ID, annotation.Label, annotation.Reason)
	}

	fmt.Printf("🎉 分支 %s 上的人类标注完成！\n", branchName)
	return nil
}

// ShowCurrentState 显示当前数据状态
func (d *AIDatasetDemo) ShowCurrentState() error {
	fmt.Println("\n📊 Current Dataset State:")
	fmt.Println(strings.Repeat("=", 60))

	query := `
		SELECT id, features, label, description,
		       JSON_EXTRACT(metadata, '$.annotator') as annotator,
		       JSON_EXTRACT(metadata, '$.confidence') as confidence,
		       JSON_EXTRACT(metadata, '$.reason') as reason,
		       timestamp
		FROM ai_dataset 
		ORDER BY id 
		LIMIT 5`

	rows, err := d.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query data: %v", err)
	}
	defer rows.Close()

	fmt.Printf("%-4s %-10s %-12s %-35s %-15s %-10s %-20s %-20s\n",
		"ID", "Vector", "Label", "Description", "Annotator", "Confidence", "Reason", "Timestamp")
	fmt.Println(strings.Repeat("-", 150))

	for rows.Next() {
		var id int
		var features, label, description, timestamp string
		var annotator, reason sql.NullString
		var confidence sql.NullFloat64

		err := rows.Scan(&id, &features, &label, &description, &annotator, &confidence, &reason, &timestamp)
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

		// 截断长文本
		descStr := truncateText(description, 30)
		reasonStr = truncateText(reasonStr, 20)
		featuresStr := truncateText(features, 8) // 向量显示8个字符

		fmt.Printf("%-4d %-10s %-12s %-35s %-15s %-10s %-20s %-20s\n",
			id, featuresStr, label, descStr, annotatorStr, confStr, reasonStr, timestamp)
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

// ShowSnapshotState 显示快照状态
func (d *AIDatasetDemo) ShowSnapshotState(snapshotName string) error {
	fmt.Printf("\n📸 快照状态: %s\n", snapshotName)
	fmt.Println(strings.Repeat("=", 60))

	query := fmt.Sprintf(`
		SELECT id, features, label, description,
		       JSON_EXTRACT(metadata, '$.annotator') as annotator,
		       JSON_EXTRACT(metadata, '$.confidence') as confidence,
		       JSON_EXTRACT(metadata, '$.reason') as reason,
		       timestamp
		FROM ai_dataset {Snapshot = '%s'}
		ORDER BY id 
		LIMIT 20`, snapshotName)

	rows, err := d.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query snapshot data: %v", err)
	}
	defer rows.Close()

	fmt.Printf("%-4s %-10s %-8s %-35s %-15s %-8s %-20s %-20s\n",
		"ID", "Vector", "Label", "Description", "Annotator", "Conf", "Reason", "Timestamp")
	fmt.Println(strings.Repeat("-", 120))

	recordCount := 0
	for rows.Next() {
		var id int
		var features, label, description, timestamp string
		var annotator, reason sql.NullString
		var confidence sql.NullFloat64

		err := rows.Scan(&id, &features, &label, &description, &annotator, &confidence, &reason, &timestamp)
		if err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		// 截断长文本
		description = truncateText(description, 30)
		features = truncateText(features, 8)
		reasonText := "N/A"
		if reason.Valid {
			reasonText = truncateText(reason.String, 20)
		}

		confText := "N/A"
		if confidence.Valid {
			confText = fmt.Sprintf("%.2f", confidence.Float64)
		}

		annotatorText := "N/A"
		if annotator.Valid {
			annotatorText = annotator.String
		}

		fmt.Printf("%-4d %-10s %-8s %-35s %-15s %-8s %-20s %-20s\n",
			id, features, label, description, annotatorText, confText, reasonText, timestamp)
		recordCount++
	}

	fmt.Printf("\n📊 快照 %s 包含 %d 条记录\n", snapshotName, recordCount)
	return nil
}

// ShowBranchState 显示分支状态
func (d *AIDatasetDemo) ShowBranchState(branchName string) error {
	fmt.Printf("\n🌿 分支状态: %s\n", branchName)
	fmt.Println(strings.Repeat("=", 60))

	branchTable := fmt.Sprintf("mo_branches.test_ai_dataset_%s", branchName)
	query := fmt.Sprintf(`
		SELECT id, features, label, description,
		       JSON_EXTRACT(metadata, '$.annotator') as annotator,
		       JSON_EXTRACT(metadata, '$.confidence') as confidence,
		       JSON_EXTRACT(metadata, '$.reason') as reason,
		       timestamp
		FROM %s
		ORDER BY id 
		LIMIT 20`, branchTable)

	rows, err := d.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query branch data: %v", err)
	}
	defer rows.Close()

	fmt.Printf("%-4s %-10s %-8s %-35s %-15s %-8s %-20s %-20s\n",
		"ID", "Vector", "Label", "Description", "Annotator", "Conf", "Reason", "Timestamp")
	fmt.Println(strings.Repeat("-", 120))

	recordCount := 0
	for rows.Next() {
		var id int
		var features, label, description, timestamp string
		var annotator, reason sql.NullString
		var confidence sql.NullFloat64

		err := rows.Scan(&id, &features, &label, &description, &annotator, &confidence, &reason, &timestamp)
		if err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		// 截断长文本
		description = truncateText(description, 30)
		features = truncateText(features, 8)
		reasonText := "N/A"
		if reason.Valid {
			reasonText = truncateText(reason.String, 20)
		}

		confText := "N/A"
		if confidence.Valid {
			confText = fmt.Sprintf("%.2f", confidence.Float64)
		}

		annotatorText := "N/A"
		if annotator.Valid {
			annotatorText = annotator.String
		}

		fmt.Printf("%-4d %-10s %-8s %-35s %-15s %-8s %-20s %-20s\n",
			id, features, label, description, annotatorText, confText, reasonText, timestamp)
		recordCount++
	}

	fmt.Printf("\n📊 分支 %s 包含 %d 条记录\n", branchName, recordCount)
	return nil
}

// TimeTravelQuery 时间旅行查询 - 查询指定时间点的数据状态
func (d *AIDatasetDemo) TimeTravelQuery(targetTime string) error {
	return d.TimeTravelQueryWithMode(targetTime, false, "")
}

// TimeTravelQueryWithMode 时间旅行查询 - 支持快照和时间戳查询
func (d *AIDatasetDemo) TimeTravelQueryWithMode(target string, useSnapshot bool, snapshotName string) error {
	if useSnapshot {
		return d.TimeTravelQueryFromSnapshot(snapshotName)
	} else {
		return d.TimeTravelQueryFromTimestamp(target)
	}
}

// TimeTravelQueryFromSnapshot 从快照进行时间旅行查询
func (d *AIDatasetDemo) TimeTravelQueryFromSnapshot(snapshotName string) error {
	fmt.Printf("⏰ Time Travel Query from Snapshot: %s\n", snapshotName)
	fmt.Println(strings.Repeat("=", 60))

	// 使用快照查询
	query := fmt.Sprintf(`
		SELECT id, label, description,
		       JSON_EXTRACT(metadata, '$.annotator') as annotator,
		       JSON_EXTRACT(metadata, '$.confidence') as confidence,
		       JSON_EXTRACT(metadata, '$.reason') as reason,
		       timestamp
		FROM ai_dataset {Snapshot = "%s"}
		ORDER BY id 
		LIMIT 10`, snapshotName)

	rows, err := d.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query snapshot data: %v", err)
	}
	defer rows.Close()

	fmt.Printf("%-4s %-12s %-35s %-15s %-10s %-20s %-20s\n",
		"ID", "Label", "Description", "Annotator", "Confidence", "Reason", "Timestamp")
	fmt.Println(strings.Repeat("-", 140))

	for rows.Next() {
		var id int
		var label, description, timestamp string
		var annotator, reason sql.NullString
		var confidence sql.NullFloat64

		err := rows.Scan(&id, &label, &description, &annotator, &confidence, &reason, &timestamp)
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

		// 截断长文本
		descStr := truncateText(description, 30)
		reasonStr = truncateText(reasonStr, 20)

		fmt.Printf("%-4d %-12s %-35s %-15s %-10s %-20s %-20s\n",
			id, label, descStr, annotatorStr, confStr, reasonStr, timestamp)
	}

	return nil
}

// TimeTravelQueryFromTimestamp 从时间戳进行时间旅行查询
func (d *AIDatasetDemo) TimeTravelQueryFromTimestamp(targetTime string) error {
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
		SELECT id, label, description,
		       JSON_EXTRACT(metadata, '$.annotator') as annotator,
		       JSON_EXTRACT(metadata, '$.confidence') as confidence,
		       JSON_EXTRACT(metadata, '$.reason') as reason,
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

	fmt.Printf("%-4s %-12s %-35s %-15s %-10s %-20s %-20s\n",
		"ID", "Label", "Description", "Annotator", "Confidence", "Reason", "Timestamp")
	fmt.Println(strings.Repeat("-", 140))

	for rows.Next() {
		var id int
		var label, description, timestamp string
		var annotator, reason sql.NullString
		var confidence sql.NullFloat64

		err := rows.Scan(&id, &label, &description, &annotator, &confidence, &reason, &timestamp)
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

		// 截断长文本
		descStr := truncateText(description, 30)
		reasonStr = truncateText(reasonStr, 20)

		fmt.Printf("%-4d %-12s %-35s %-15s %-10s %-20s %-20s\n",
			id, label, descStr, annotatorStr, confStr, reasonStr, timestamp)
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

// createInitialSnapshot 创建初始化快照
func (d *AIDatasetDemo) createInitialSnapshot() error {
	timestamp := time.Now().Format("20060102_150405")
	snapshotName := fmt.Sprintf("ai_dataset_%s_initial", timestamp)

	createSQL := fmt.Sprintf("CREATE SNAPSHOT %s FOR TABLE test ai_dataset", snapshotName)

	_, err := d.db.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("failed to create initial snapshot: %v", err)
	}

	fmt.Printf("✅ Initial snapshot '%s' created successfully!\n", snapshotName)
	return nil
}

// createAnnotationSnapshot 创建标注后快照
func (d *AIDatasetDemo) createAnnotationSnapshot(annotator string, sequence int) error {
	timestamp := time.Now().Format("20060102_150405")
	snapshotName := fmt.Sprintf("ai_dataset_%s_%s_%d", timestamp, annotator, sequence)

	createSQL := fmt.Sprintf("CREATE SNAPSHOT %s FOR TABLE test ai_dataset", snapshotName)

	_, err := d.db.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("failed to create annotation snapshot: %v", err)
	}

	fmt.Printf("✅ Annotation snapshot '%s' created successfully!\n", snapshotName)
	return nil
}

// getNextSequenceNumber 获取下一个序列号
func (d *AIDatasetDemo) getNextSequenceNumber(annotator string) int {
	snapshots, err := d.getSnapshotList()
	if err != nil {
		return 1
	}

	maxSeq := 0
	pattern := fmt.Sprintf("_%s_", annotator)

	for _, snapshotName := range snapshots {
		if strings.Contains(snapshotName, pattern) {
			// 提取序列号
			parts := strings.Split(snapshotName, "_")
			if len(parts) >= 3 {
				if seq, err := strconv.Atoi(parts[len(parts)-1]); err == nil {
					if seq > maxSeq {
						maxSeq = seq
					}
				}
			}
		}
	}

	return maxSeq + 1
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

// ShowSnapshots 显示所有快照（按时间戳升序排列）
func (d *AIDatasetDemo) ShowSnapshots() error {
	fmt.Println("📸 Available Snapshots (按时间戳升序排列):")
	fmt.Println(strings.Repeat("=", 80))

	snapshots, err := d.getSnapshotInfoList()
	if err != nil {
		return fmt.Errorf("failed to get snapshots: %v", err)
	}

	count := 0
	for _, snapshot := range snapshots {
		// 美化输出，突出快照名称和时间
		fmt.Printf("📸 %s\n", strings.Repeat("=", 76))
		fmt.Printf("🏷️  Name: %s\n", snapshot.Name)
		fmt.Printf("⏰ Time:  %s\n", snapshot.Timestamp)
		fmt.Printf("📊 Level: %s | Account: %s | Database: %s | Table: %s\n",
			snapshot.Level, snapshot.Account, snapshot.Database, snapshot.Table)
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

// CreatePITR 创建PITR
func (d *AIDatasetDemo) CreatePITR(pitrName string, duration string) error {
	fmt.Printf("🕐 Creating PITR: %s (Duration: %s)\n", pitrName, duration)
	fmt.Println(strings.Repeat("=", 60))

	// 创建PITR的SQL
	createSQL := fmt.Sprintf("CREATE PITR %s FOR TABLE test ai_dataset RANGE %s", pitrName, duration)

	_, err := d.db.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("failed to create PITR: %v", err)
	}

	fmt.Printf("✅ PITR '%s' created successfully!\n", pitrName)
	fmt.Printf("📋 SQL: %s\n", createSQL)

	return nil
}

// ShowPITRs 显示所有PITR
func (d *AIDatasetDemo) ShowPITRs() error {
	fmt.Println("🕐 Available PITRs:")
	fmt.Println(strings.Repeat("=", 80))

	query := "SHOW PITR"
	rows, err := d.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query PITRs: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var pitrName, createdTime, modifiedTime, pitrLevel, accountName, databaseName, tableName, pitrLength, pitrUnit string
		err := rows.Scan(&pitrName, &createdTime, &modifiedTime, &pitrLevel, &accountName, &databaseName, &tableName, &pitrLength, &pitrUnit)
		if err != nil {
			return fmt.Errorf("failed to scan PITR row: %v", err)
		}

		// 美化输出，突出PITR名称和时间
		fmt.Printf("🕐 %s\n", strings.Repeat("=", 76))
		fmt.Printf("🏷️  Name: %s\n", pitrName)
		fmt.Printf("⏰ Created:  %s\n", createdTime)
		fmt.Printf("🔄 Modified: %s\n", modifiedTime)
		fmt.Printf("📊 Level: %s | Account: %s | Database: %s | Table: %s\n",
			pitrLevel, accountName, databaseName, tableName)
		fmt.Printf("⏱️  Duration: %s %s\n", pitrLength, pitrUnit)
		fmt.Println()
		count++
	}

	if count == 0 {
		fmt.Println("❌ No PITRs found.")
	} else {
		fmt.Printf("📊 Total PITRs: %d\n", count)
	}

	return nil
}

// DropPITR 删除PITR
func (d *AIDatasetDemo) DropPITR(pitrName string) error {
	fmt.Printf("🗑️  Dropping PITR: %s\n", pitrName)
	fmt.Println(strings.Repeat("=", 60))

	dropSQL := fmt.Sprintf("DROP PITR %s", pitrName)

	_, err := d.db.Exec(dropSQL)
	if err != nil {
		return fmt.Errorf("failed to drop PITR: %v", err)
	}

	fmt.Printf("✅ PITR '%s' dropped successfully!\n", pitrName)
	fmt.Printf("📋 SQL: %s\n", dropSQL)

	return nil
}

// getPITRList 获取PITR列表
func (d *AIDatasetDemo) getPITRList() ([]string, error) {
	query := "SHOW PITR"
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query PITRs: %v", err)
	}
	defer rows.Close()

	var pitrNames []string
	for rows.Next() {
		var pitrName, createdTime, modifiedTime, pitrLevel, accountName, databaseName, tableName, pitrLength, pitrUnit string
		err := rows.Scan(&pitrName, &createdTime, &modifiedTime, &pitrLevel, &accountName, &databaseName, &tableName, &pitrLength, &pitrUnit)
		if err != nil {
			return nil, fmt.Errorf("failed to scan PITR row: %v", err)
		}
		pitrNames = append(pitrNames, pitrName)
	}

	return pitrNames, nil
}

// RestoreFromSnapshot 从快照恢复数据
func (d *AIDatasetDemo) RestoreFromSnapshot(snapshotName string) error {
	fmt.Printf("🔄 Restoring data from snapshot: %s\n", snapshotName)
	fmt.Println(strings.Repeat("=", 60))

	// 恢复数据的SQL
	restoreSQL := fmt.Sprintf("RESTORE ACCOUNT sys DATABASE test TABLE ai_dataset FROM SNAPSHOT %s", snapshotName)

	_, err := d.db.Exec(restoreSQL)
	if err != nil {
		return fmt.Errorf("failed to restore from snapshot: %v", err)
	}

	fmt.Printf("✅ Data restored from snapshot '%s' successfully!\n", snapshotName)
	fmt.Printf("📋 SQL: %s\n", restoreSQL)

	return nil
}

// RestoreFromPITR 从PITR时间点恢复数据
func (d *AIDatasetDemo) RestoreFromPITR(pitrName, timestamp string) error {
	fmt.Printf("🔄 Restoring data from PITR: %s at %s\n", pitrName, timestamp)
	fmt.Println(strings.Repeat("=", 60))

	// 恢复数据的SQL
	restoreSQL := fmt.Sprintf("RESTORE DATABASE test TABLE ai_dataset FROM PITR '%s' '%s'", pitrName, timestamp)

	_, err := d.db.Exec(restoreSQL)
	if err != nil {
		return fmt.Errorf("failed to restore from PITR: %v", err)
	}

	fmt.Printf("✅ Data restored from PITR '%s' at %s successfully!\n", pitrName, timestamp)
	fmt.Printf("📋 SQL: %s\n", restoreSQL)

	return nil
}

// getDefaultPITRName 获取默认PITR名称
func (d *AIDatasetDemo) getDefaultPITRName() string {
	return "ai_dataset_3h_pitr"
}

// CleanupAllDemoData 一键清空所有demo相关数据
func (d *AIDatasetDemo) CleanupAllDemoData() error {
	fmt.Println("🧹 一键清空")
	fmt.Println(strings.Repeat("=", 60))

	// 统计信息
	snapshotCount := 0
	pitrCount := 0
	branchCount := 0
	dataCount := 0
	errorCount := 0

	// 1. 删除所有demo相关的快照
	fmt.Println("📸 正在删除所有demo相关快照...")
	snapshots, err := d.getSnapshotList()
	if err != nil {
		fmt.Printf("⚠️  获取快照列表失败: %v\n", err)
	} else {
		for _, snapshotName := range snapshots {
			if strings.Contains(snapshotName, "ai_dataset") {
				err := d.DropSnapshot(snapshotName)
				if err != nil {
					fmt.Printf("❌ 删除快照 '%s' 失败: %v\n", snapshotName, err)
					errorCount++
				} else {
					snapshotCount++
				}
			}
		}
	}

	// 2. 删除所有demo相关的PITR
	fmt.Println("\n🕐 正在删除所有demo相关PITR...")
	pitrList, err := d.getPITRList()
	if err != nil {
		fmt.Printf("⚠️  获取PITR列表失败: %v\n", err)
	} else {
		for _, pitrName := range pitrList {
			if strings.Contains(pitrName, "ai_dataset") {
				err := d.DropPITR(pitrName)
				if err != nil {
					fmt.Printf("❌ 删除PITR '%s' 失败: %v\n", pitrName, err)
					errorCount++
				} else {
					pitrCount++
				}
			}
		}
	}

	// 3. 删除所有demo相关的表分支
	fmt.Println("\n🌿 正在删除所有demo相关表分支...")
	branches, err := d.getTableBranches()
	if err != nil {
		fmt.Printf("⚠️  获取分支列表失败: %v\n", err)
	} else {
		for _, branchName := range branches {
			err := d.DropTableBranch(branchName)
			if err != nil {
				fmt.Printf("❌ 删除分支 '%s' 失败: %v\n", branchName, err)
				errorCount++
			} else {
				branchCount++
			}
		}
	}

	// 4. 清空分支历史记录
	fmt.Println("\n📜 正在清空分支历史记录...")
	_, err = d.db.Exec("DELETE FROM mo_branches.branch_management")
	if err != nil {
		fmt.Printf("❌ 清空分支历史失败: %v\n", err)
		errorCount++
	} else {
		fmt.Println("✅ 分支历史记录已清空")
	}

	// 5. 清空ai_dataset表数据
	fmt.Println("\n🗑️  正在清空ai_dataset表数据...")
	// 先获取数据量
	dataCount = d.getDataCount()
	_, err = d.db.Exec("DELETE FROM ai_dataset")
	if err != nil {
		fmt.Printf("❌ 清空表数据失败: %v\n", err)
		errorCount++
	}

	// 显示清理结果
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("📊 清理结果:")
	fmt.Printf("  📸 删除快照: %d 个\n", snapshotCount)
	fmt.Printf("  🕐 删除PITR: %d 个\n", pitrCount)
	fmt.Printf("  🌿 删除分支: %d 个\n", branchCount)
	fmt.Printf("  🗑️  清空数据: %d 行数据已删除\n", dataCount)

	if errorCount > 0 {
		fmt.Printf("  ❌ 错误数量: %d 个\n", errorCount)
		fmt.Println("⚠️  部分清理操作失败，请检查错误信息")
	} else {
		fmt.Println("✅ 所有demo数据清理完成！")
	}

	return nil
}

// getDemoSnapshotCount 获取demo相关快照数量
func (d *AIDatasetDemo) getDemoSnapshotCount() int {
	snapshots, err := d.getSnapshotList()
	if err != nil {
		return 0
	}

	count := 0
	for _, snapshotName := range snapshots {
		if strings.Contains(snapshotName, "ai_dataset") {
			count++
		}
	}
	return count
}

// getDemoPITRCount 获取demo相关PITR数量
func (d *AIDatasetDemo) getDemoPITRCount() int {
	pitrList, err := d.getPITRList()
	if err != nil {
		return 0
	}

	count := 0
	for _, pitrName := range pitrList {
		if strings.Contains(pitrName, "ai_dataset") {
			count++
		}
	}
	return count
}

// getDataCount 获取ai_dataset表数据行数
func (d *AIDatasetDemo) getDataCount() int {
	var count int
	err := d.db.QueryRow("SELECT COUNT(*) FROM ai_dataset").Scan(&count)
	if err != nil {
		return 0
	}
	return count
}

// ensurePITRExists 确保3小时PITR存在
func (d *AIDatasetDemo) ensurePITRExists() error {
	pitrName := "ai_dataset_3h_pitr"

	// 检查PITR是否已存在
	pitrList, err := d.getPITRList()
	if err != nil {
		return fmt.Errorf("failed to check existing PITRs: %v", err)
	}

	// 检查是否已存在3小时PITR
	for _, name := range pitrList {
		if name == pitrName {
			fmt.Printf("ℹ️  PITR '%s' already exists, skipping creation\n", pitrName)
			return nil
		}
	}

	// 创建3小时PITR
	fmt.Println("🕐 Creating 3-hour PITR for data protection...")
	return d.CreatePITR(pitrName, "3 'h'")
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
		fmt.Print("请选择操作 (1-14): ")

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
			if err := showCurrentStateMenu(demo, reader); err != nil {
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
			if err := pitrMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "9":
			if err := restoreMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "10":
			if err := cleanupMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "11":
			if err := vectorSearchMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "12":
			if err := tableBranchMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "13":
			if err := mergeMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "14":
			if err := demo.RunDemo(); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "15":
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
	fmt.Println("8. 🕐 PITR管理")
	fmt.Println("9. 🔄 数据恢复")
	fmt.Println("10. 🧹 一键清空Demo数据")
	fmt.Println("11. 🔍 向量相似度搜索")
	fmt.Println("12. 🌿 表分支管理")
	fmt.Println("13. 🔀 分支 Merge")
	fmt.Println("14. 🎬 运行完整演示")
	fmt.Println("15. 🚪 退出")
	fmt.Println(strings.Repeat("=", 50))
}

// tableBranchMenu 表分支管理菜单
func tableBranchMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	fmt.Println("\n🌿 表分支管理")
	fmt.Println("1. 📋 查看所有分支")
	fmt.Println("2. ➕ 创建新分支")
	fmt.Println("3. 🗑️ 删除分支")
	fmt.Println("4. 📜 查看分支历史")
	fmt.Print("请选择操作 (1-4): ")

	choice, _ := reader.ReadString('\n')
	choice = strings.TrimSpace(choice)

	switch choice {
	case "1":
		return demo.ListTableBranches()
	case "2":
		return createBranchMenu(demo, reader)
	case "3":
		return deleteBranchMenu(demo, reader)
	case "4":
		return demo.ShowBranchHistory()
	default:
		fmt.Println("❌ 无效选择")
		return nil
	}
}

// createBranchMenu 创建分支菜单
func createBranchMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 获取可用快照列表
	snapshots, err := demo.getSnapshotInfoList()
	if err != nil {
		return fmt.Errorf("failed to get snapshots: %v", err)
	}

	if len(snapshots) == 0 {
		return fmt.Errorf("没有可用的快照，请先创建快照")
	}

	// 显示可用快照
	fmt.Println("📸 可用的快照:")
	fmt.Println(strings.Repeat("=", 50))
	for i, snapshot := range snapshots {
		if i >= 10 { // 最多显示10个快照
			break
		}
		fmt.Printf("%d. %s (创建时间: %s)\n", i+1, snapshot.Name, snapshot.Timestamp)
	}

	fmt.Print("\n请选择快照 (输入序号): ")
	snapshotInput, _ := reader.ReadString('\n')
	snapshotInput = strings.TrimSpace(snapshotInput)

	var snapshotName string
	if num, err := strconv.Atoi(snapshotInput); err == nil && num >= 1 && num <= len(snapshots) {
		snapshotName = snapshots[num-1].Name
		fmt.Printf("✅ 选择快照: %s\n", snapshotName)
	} else {
		return fmt.Errorf("无效的快照序号")
	}

	fmt.Print("请输入分支名称: ")
	branchName, _ := reader.ReadString('\n')
	branchName = strings.TrimSpace(branchName)

	if branchName == "" {
		return fmt.Errorf("分支名称不能为空")
	}

	return demo.CreateTableBranch(branchName, snapshotName)
}

// branchVsBranchMenu 分支与分支比较菜单
func branchVsBranchMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 获取所有分支列表
	branches, err := demo.getTableBranches()
	if err != nil {
		return fmt.Errorf("failed to get branches: %v", err)
	}

	if len(branches) < 2 {
		return fmt.Errorf("至少需要2个分支才能进行比较")
	}

	// 显示所有分支
	fmt.Println("🌿 可用的分支:")
	fmt.Println(strings.Repeat("=", 30))
	for i, branch := range branches {
		fmt.Printf("%d. 📋 %s\n", i+1, branch)
	}

	// 选择第一个分支
	fmt.Print("\n请选择第一个分支 (序号): ")
	input1, _ := reader.ReadString('\n')
	input1 = strings.TrimSpace(input1)

	var branch1Name string
	if num, err := strconv.Atoi(input1); err == nil && num >= 1 && num <= len(branches) {
		branch1Name = branches[num-1]
		fmt.Printf("✅ 选择分支1: %s\n", branch1Name)
	} else {
		return fmt.Errorf("无效的分支序号")
	}

	// 选择第二个分支
	fmt.Print("请选择第二个分支 (序号): ")
	input2, _ := reader.ReadString('\n')
	input2 = strings.TrimSpace(input2)

	var branch2Name string
	if num, err := strconv.Atoi(input2); err == nil && num >= 1 && num <= len(branches) {
		branch2Name = branches[num-1]
		fmt.Printf("✅ 选择分支2: %s\n", branch2Name)
	} else {
		return fmt.Errorf("无效的分支序号")
	}

	if branch1Name == branch2Name {
		return fmt.Errorf("不能比较同一个分支")
	}

	// 选择显示模式
	fmt.Print("显示详细比较? (y/N): ")
	detailed, _ := reader.ReadString('\n')
	detailed = strings.TrimSpace(detailed)
	showDetailed := strings.ToLower(detailed) == "y" || strings.ToLower(detailed) == "yes"

	return demo.CompareBranches(branch1Name, branch2Name, showDetailed)
}

// branchVsSnapshotMenu 分支与快照比较菜单
func branchVsSnapshotMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 获取所有分支列表
	branches, err := demo.getTableBranches()
	if err != nil {
		return fmt.Errorf("failed to get branches: %v", err)
	}

	if len(branches) == 0 {
		return fmt.Errorf("没有可用的分支")
	}

	// 显示所有分支
	fmt.Println("🌿 可用的分支:")
	fmt.Println(strings.Repeat("=", 30))
	for i, branch := range branches {
		fmt.Printf("%d. 📋 %s\n", i+1, branch)
	}

	// 选择分支
	fmt.Print("\n请选择分支 (序号): ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	var branchName string
	if num, err := strconv.Atoi(input); err == nil && num >= 1 && num <= len(branches) {
		branchName = branches[num-1]
		fmt.Printf("✅ 选择分支: %s\n", branchName)
	} else {
		return fmt.Errorf("无效的分支序号")
	}

	// 获取可用快照列表
	snapshots, err := demo.getSnapshotInfoList()
	if err != nil {
		return fmt.Errorf("failed to get snapshots: %v", err)
	}

	if len(snapshots) == 0 {
		return fmt.Errorf("没有可用的快照")
	}

	// 显示可用快照
	fmt.Println("\n📸 可用的快照:")
	fmt.Println(strings.Repeat("=", 50))
	for i, snapshot := range snapshots {
		if i >= 10 { // 最多显示10个快照
			break
		}
		fmt.Printf("%d. %s (创建时间: %s)\n", i+1, snapshot.Name, snapshot.Timestamp)
	}

	// 选择快照
	fmt.Print("\n请选择快照 (序号): ")
	snapshotInput, _ := reader.ReadString('\n')
	snapshotInput = strings.TrimSpace(snapshotInput)

	var snapshotName string
	if num, err := strconv.Atoi(snapshotInput); err == nil && num >= 1 && num <= len(snapshots) {
		snapshotName = snapshots[num-1].Name
		fmt.Printf("✅ 选择快照: %s\n", snapshotName)
	} else {
		return fmt.Errorf("无效的快照序号")
	}

	// 选择显示模式
	fmt.Print("显示详细比较? (y/N): ")
	detailed, _ := reader.ReadString('\n')
	detailed = strings.TrimSpace(detailed)
	showDetailed := strings.ToLower(detailed) == "y" || strings.ToLower(detailed) == "yes"

	return demo.CompareBranchWithSnapshot(branchName, snapshotName, showDetailed)
}

// branchVsMainTableMenu 分支与主表比较菜单
func branchVsMainTableMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 获取所有分支列表
	branches, err := demo.getTableBranches()
	if err != nil {
		return fmt.Errorf("failed to get branches: %v", err)
	}

	if len(branches) == 0 {
		return fmt.Errorf("没有可用的分支")
	}

	// 显示所有分支
	fmt.Println("🌿 可用的分支:")
	fmt.Println(strings.Repeat("=", 30))
	for i, branch := range branches {
		fmt.Printf("%d. 📋 %s\n", i+1, branch)
	}

	// 选择分支
	fmt.Print("\n请选择分支 (序号): ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	var branchName string
	if num, err := strconv.Atoi(input); err == nil && num >= 1 && num <= len(branches) {
		branchName = branches[num-1]
		fmt.Printf("✅ 选择分支: %s\n", branchName)
	} else {
		return fmt.Errorf("无效的分支序号")
	}

	// 选择显示模式
	fmt.Print("显示详细比较? (y/N): ")
	detailed, _ := reader.ReadString('\n')
	detailed = strings.TrimSpace(detailed)
	showDetailed := strings.ToLower(detailed) == "y" || strings.ToLower(detailed) == "yes"

	return demo.CompareBranchWithMainTable(branchName, showDetailed)
}

// mergeMenu merge菜单
func mergeMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 获取所有分支列表
	branches, err := demo.getTableBranches()
	if err != nil {
		return fmt.Errorf("failed to get branches: %v", err)
	}

	if len(branches) == 0 {
		return fmt.Errorf("没有可用的分支")
	}

	// 显示所有分支
	fmt.Println("🌿 可用的分支:")
	fmt.Println(strings.Repeat("=", 50))
	for i, branch := range branches {
		fmt.Printf("%d. 📋 %s\n", i+1, branch)
	}
	fmt.Printf("%d. 📊 main (主表)\n", len(branches)+1)

	// 选择源分支
	fmt.Print("\n🔀 请选择源分支 (要合并的分支) (序号): ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	var sourceBranch string
	if num, err := strconv.Atoi(input); err == nil && num >= 1 && num <= len(branches)+1 {
		if num == len(branches)+1 {
			sourceBranch = "main"
		} else {
			sourceBranch = branches[num-1]
		}
		fmt.Printf("✅ 源分支: %s\n", sourceBranch)
	} else {
		return fmt.Errorf("无效的分支序号")
	}

	// 选择目标分支
	fmt.Print("\n🎯 请选择目标分支 (接收合并的分支) (序号): ")
	input, _ = reader.ReadString('\n')
	input = strings.TrimSpace(input)

	var targetBranch string
	if num, err := strconv.Atoi(input); err == nil && num >= 1 && num <= len(branches)+1 {
		if num == len(branches)+1 {
			targetBranch = "main"
		} else {
			targetBranch = branches[num-1]
		}
		fmt.Printf("✅ 目标分支: %s\n", targetBranch)
	} else {
		return fmt.Errorf("无效的分支序号")
	}

	if sourceBranch == targetBranch {
		return fmt.Errorf("源分支和目标分支不能相同")
	}

	// 显示merge操作摘要
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Printf("🔀 Merge 操作摘要:\n")
	fmt.Printf("   源分支 (Source): %s\n", sourceBranch)
	fmt.Printf("   目标分支 (Target): %s\n", targetBranch)
	fmt.Printf("   操作: 将 %s 的更改合并到 %s\n", sourceBranch, targetBranch)
	fmt.Println(strings.Repeat("=", 60))

	// 检测冲突
	fmt.Printf("\n🔍 正在检测分支 %s 与 %s 的冲突...\n", sourceBranch, targetBranch)
	mergeResult, err := demo.DetectConflicts(sourceBranch, targetBranch)
	if err != nil {
		return fmt.Errorf("failed to detect conflicts: %v", err)
	}

	if mergeResult.TotalConflicts == 0 {
		fmt.Println("✅ 没有发现冲突，可以直接merge")
		fmt.Print("是否执行merge? (y/N): ")
		confirm, _ := reader.ReadString('\n')
		confirm = strings.TrimSpace(strings.ToLower(confirm))

		if confirm == "y" || confirm == "yes" {
			// 直接执行merge，没有冲突
			return demo.executeDirectMerge(sourceBranch, targetBranch)
		} else {
			fmt.Println("❌ 已取消merge操作")
			return nil
		}
	} else {
		fmt.Printf("⚠️ 发现 %d 个冲突，需要解决\n", mergeResult.TotalConflicts)

		// 显示前5个冲突
		demo.ShowConflicts(mergeResult.Conflicts, 0, sourceBranch, targetBranch)

		// 进入冲突解决界面
		return demo.ResolveConflicts(mergeResult, sourceBranch, targetBranch, reader)
	}
}

// executeDirectMerge 执行直接merge（无冲突情况）
func (d *AIDatasetDemo) executeDirectMerge(sourceBranch, targetBranch string) error {
	fmt.Printf("\n🚀 正在执行直接 Merge 操作...\n")
	fmt.Printf("源分支: %s\n", sourceBranch)
	fmt.Printf("目标分支: %s\n", targetBranch)

	var sourceTable, targetTable string

	if sourceBranch == "main" {
		sourceTable = "ai_dataset"
	} else {
		sourceTable = fmt.Sprintf("mo_branches.test_ai_dataset_%s", sourceBranch)
	}

	if targetBranch == "main" {
		targetTable = "ai_dataset"
	} else {
		targetTable = fmt.Sprintf("mo_branches.test_ai_dataset_%s", targetBranch)
	}

	// 直接使用源分支数据更新目标分支
	updateQuery := fmt.Sprintf(`
		UPDATE %s 
		SET label = (SELECT label FROM %s WHERE %s.id = %s.id),
		    description = (SELECT description FROM %s WHERE %s.id = %s.id),
		    metadata = (SELECT metadata FROM %s WHERE %s.id = %s.id)
		WHERE EXISTS (SELECT 1 FROM %s WHERE %s.id = %s.id)`,
		targetTable, sourceTable, targetTable, sourceTable,
		sourceTable, targetTable, sourceTable,
		sourceTable, targetTable, sourceTable,
		sourceTable, targetTable, sourceTable)

	result, err := d.db.Exec(updateQuery)
	if err != nil {
		return fmt.Errorf("failed to execute merge: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()
	fmt.Printf("✅ 成功更新 %d 条记录\n", rowsAffected)
	fmt.Println("🎉 直接 Merge 操作完成！")

	// 记录merge事件
	if err := d.recordMergeEvent(sourceBranch, targetBranch, 0, 0); err != nil {
		fmt.Printf("⚠️  Warning: Failed to record merge event: %v\n", err)
	}

	// 如果目标分支是主表，自动生成新快照
	if targetBranch == "main" {
		snapshotName := fmt.Sprintf("merge_%s_to_main_%s", sourceBranch, time.Now().Format("20060102_150405"))
		if err := d.CreateSnapshot(snapshotName); err != nil {
			fmt.Printf("⚠️  Warning: Failed to create snapshot after merge: %v\n", err)
		} else {
			fmt.Printf("📸 已自动创建快照: %s\n", snapshotName)
		}
	}

	return nil
}

// deleteBranchMenu 删除分支菜单
func deleteBranchMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 获取所有分支列表
	branches, err := demo.getTableBranches()
	if err != nil {
		return fmt.Errorf("failed to get branches: %v", err)
	}

	if len(branches) == 0 {
		fmt.Println("❌ 没有可删除的分支")
		return nil
	}

	// 显示所有分支
	fmt.Println("🌿 可删除的分支:")
	fmt.Println(strings.Repeat("=", 30))
	for i, branch := range branches {
		fmt.Printf("%d. 📋 %s\n", i+1, branch)
	}

	fmt.Print("\n请输入要删除的分支 (序号或分支名称): ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	if input == "" {
		return fmt.Errorf("输入不能为空")
	}

	var branchName string
	// 尝试解析为序号
	if num, err := strconv.Atoi(input); err == nil && num >= 1 && num <= len(branches) {
		branchName = branches[num-1]
		fmt.Printf("✅ 选择分支: %s\n", branchName)
	} else {
		// 作为分支名称处理
		branchName = input
		// 验证分支是否存在
		found := false
		for _, branch := range branches {
			if branch == branchName {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("分支 '%s' 不存在", branchName)
		}
	}

	// 确认删除
	fmt.Printf("确认删除分支 '%s'? (y/N): ", branchName)
	confirm, _ := reader.ReadString('\n')
	confirm = strings.TrimSpace(confirm)

	if strings.ToLower(confirm) == "y" || strings.ToLower(confirm) == "yes" {
		return demo.DropTableBranch(branchName)
	} else {
		fmt.Println("❌ 取消删除操作")
		return nil
	}
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
	fmt.Println("\n🤖 AI 标注")
	fmt.Println("1. 📊 基于主表标注")
	fmt.Println("2. 🌿 基于分支标注")
	fmt.Print("请选择标注方式 (1-2): ")

	choice, _ := reader.ReadString('\n')
	choice = strings.TrimSpace(choice)

	switch choice {
	case "1":
		return aiAnnotationOnMainTable(demo, reader)
	case "2":
		return aiAnnotationOnBranch(demo, reader)
	default:
		fmt.Println("❌ 无效选择，使用主表标注")
		return aiAnnotationOnMainTable(demo, reader)
	}
}

// aiAnnotationOnMainTable 在主表上进行AI标注
func aiAnnotationOnMainTable(demo *AIDatasetDemo, reader *bufio.Reader) error {
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

// aiAnnotationOnBranch 在分支上进行AI标注
func aiAnnotationOnBranch(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 获取所有分支列表
	branches, err := demo.getTableBranches()
	if err != nil {
		return fmt.Errorf("failed to get branches: %v", err)
	}

	if len(branches) == 0 {
		return fmt.Errorf("没有可用的分支")
	}

	// 显示所有分支
	fmt.Println("🌿 可用的分支:")
	fmt.Println(strings.Repeat("=", 30))
	for i, branch := range branches {
		fmt.Printf("%d. 📋 %s\n", i+1, branch)
	}

	// 选择分支
	fmt.Print("\n请选择分支 (序号): ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	var branchName string
	if num, err := strconv.Atoi(input); err == nil && num >= 1 && num <= len(branches) {
		branchName = branches[num-1]
		fmt.Printf("✅ 选择分支: %s\n", branchName)
	} else {
		return fmt.Errorf("无效的分支序号")
	}

	fmt.Print("请输入 AI 模型名称 (默认 AI_model_v1): ")
	modelName, _ := reader.ReadString('\n')
	modelName = strings.TrimSpace(modelName)
	if modelName == "" {
		modelName = "AI_model_v1"
	}

	fmt.Print("请输入要标注的记录 ID (用逗号分隔，如 1,2,3): ")
	idInput, _ := reader.ReadString('\n')
	idInput = strings.TrimSpace(idInput)

	if idInput == "" {
		return fmt.Errorf("请输入至少一个记录 ID")
	}

	ids := strings.Split(idInput, ",")
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

	return demo.AIModelAnnotationOnBranch(branchName, modelName, annotations)
}

// humanAnnotationMenu 人类标注菜单
func humanAnnotationMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	fmt.Println("\n👤 人类标注")
	fmt.Println("1. 📊 基于主表标注")
	fmt.Println("2. 🌿 基于分支标注")
	fmt.Print("请选择标注方式 (1-2): ")

	choice, _ := reader.ReadString('\n')
	choice = strings.TrimSpace(choice)

	switch choice {
	case "1":
		return humanAnnotationOnMainTable(demo, reader)
	case "2":
		return humanAnnotationOnBranch(demo, reader)
	default:
		fmt.Println("❌ 无效选择，使用主表标注")
		return humanAnnotationOnMainTable(demo, reader)
	}
}

// humanAnnotationOnMainTable 在主表上进行人类标注
func humanAnnotationOnMainTable(demo *AIDatasetDemo, reader *bufio.Reader) error {
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

// humanAnnotationOnBranch 在分支上进行人类标注
func humanAnnotationOnBranch(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 获取所有分支列表
	branches, err := demo.getTableBranches()
	if err != nil {
		return fmt.Errorf("failed to get branches: %v", err)
	}

	if len(branches) == 0 {
		return fmt.Errorf("没有可用的分支")
	}

	// 显示所有分支
	fmt.Println("🌿 可用的分支:")
	fmt.Println(strings.Repeat("=", 30))
	for i, branch := range branches {
		fmt.Printf("%d. 📋 %s\n", i+1, branch)
	}

	// 选择分支
	fmt.Print("\n请选择分支 (序号): ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	var branchName string
	if num, err := strconv.Atoi(input); err == nil && num >= 1 && num <= len(branches) {
		branchName = branches[num-1]
		fmt.Printf("✅ 选择分支: %s\n", branchName)
	} else {
		return fmt.Errorf("无效的分支序号")
	}

	fmt.Print("请输入要标注的记录 ID (用逗号分隔，如 1,2,3): ")
	idInput, _ := reader.ReadString('\n')
	idInput = strings.TrimSpace(idInput)

	if idInput == "" {
		return fmt.Errorf("请输入至少一个记录 ID")
	}

	ids := strings.Split(idInput, ",")
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

	return demo.HumanAnnotationOnBranch(branchName, annotations)
}

// showCurrentStateMenu 查看当前状态菜单
func showCurrentStateMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	fmt.Println("\n📊 查看数据状态")
	fmt.Println("1. 📊 主表状态")
	fmt.Println("2. 📸 快照状态")
	fmt.Println("3. 🌿 分支状态")
	fmt.Print("请选择查看方式 (1-3): ")

	choice, _ := reader.ReadString('\n')
	choice = strings.TrimSpace(choice)

	switch choice {
	case "1":
		return demo.ShowCurrentState()
	case "2":
		return showSnapshotStateMenu(demo, reader)
	case "3":
		return showBranchStateMenu(demo, reader)
	default:
		fmt.Println("❌ 无效选择，显示主表状态")
		return demo.ShowCurrentState()
	}
}

// showSnapshotStateMenu 显示快照状态菜单
func showSnapshotStateMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 获取可用快照列表
	snapshots, err := demo.getSnapshotInfoList()
	if err != nil {
		return fmt.Errorf("failed to get snapshots: %v", err)
	}

	if len(snapshots) == 0 {
		return fmt.Errorf("没有可用的快照")
	}

	// 显示可用快照
	fmt.Println("\n📸 可用的快照:")
	fmt.Println(strings.Repeat("=", 50))
	for i, snapshot := range snapshots {
		if i >= 10 { // 最多显示10个快照
			break
		}
		fmt.Printf("%d. %s (创建时间: %s)\n", i+1, snapshot.Name, snapshot.Timestamp)
	}

	// 选择快照
	fmt.Print("\n请选择快照 (序号): ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	var snapshotName string
	if num, err := strconv.Atoi(input); err == nil && num >= 1 && num <= len(snapshots) {
		snapshotName = snapshots[num-1].Name
		fmt.Printf("✅ 选择快照: %s\n", snapshotName)
	} else {
		return fmt.Errorf("无效的快照序号")
	}

	return demo.ShowSnapshotState(snapshotName)
}

// showBranchStateMenu 显示分支状态菜单
func showBranchStateMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 获取所有分支列表
	branches, err := demo.getTableBranches()
	if err != nil {
		return fmt.Errorf("failed to get branches: %v", err)
	}

	if len(branches) == 0 {
		return fmt.Errorf("没有可用的分支")
	}

	// 显示所有分支
	fmt.Println("\n🌿 可用的分支:")
	fmt.Println(strings.Repeat("=", 30))
	for i, branch := range branches {
		fmt.Printf("%d. 📋 %s\n", i+1, branch)
	}

	// 选择分支
	fmt.Print("\n请选择分支 (序号): ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	var branchName string
	if num, err := strconv.Atoi(input); err == nil && num >= 1 && num <= len(branches) {
		branchName = branches[num-1]
		fmt.Printf("✅ 选择分支: %s\n", branchName)
	} else {
		return fmt.Errorf("无效的分支序号")
	}

	return demo.ShowBranchState(branchName)
}

// timeTravelMenu 时间旅行菜单
func timeTravelMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	fmt.Println("⏰ 时间旅行查询")
	fmt.Println("1. 📸 从快照查询")
	fmt.Println("2. 🕐 从时间戳查询")
	fmt.Print("请选择查询方式 (1-2): ")

	choice, _ := reader.ReadString('\n')
	choice = strings.TrimSpace(choice)

	switch choice {
	case "1":
		return timeTravelFromSnapshotMenu(demo, reader)
	case "2":
		return timeTravelFromTimestampMenu(demo, reader)
	default:
		fmt.Println("❌ 无效选择，使用默认时间戳查询")
		return timeTravelFromTimestampMenu(demo, reader)
	}
}

// timeTravelFromSnapshotMenu 从快照进行时间旅行查询菜单
func timeTravelFromSnapshotMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 获取快照列表
	snapshots, err := demo.getSnapshotInfoList()
	if err != nil {
		return fmt.Errorf("failed to get snapshots: %v", err)
	}

	if len(snapshots) == 0 {
		fmt.Println("❌ 没有可用的快照")
		return nil
	}

	fmt.Println("📸 可用的快照:")
	for i, snapshot := range snapshots {
		if i >= 5 { // 最多显示5个
			break
		}
		fmt.Printf("%d. %s (创建时间: %s)\n", i+1, snapshot.Name, snapshot.Timestamp)
	}

	fmt.Print("请选择快照 (输入序号或快照名称): ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	var snapshotName string
	if num, err := strconv.Atoi(input); err == nil && num >= 1 && num <= len(snapshots) {
		snapshotName = snapshots[num-1].Name
	} else {
		snapshotName = input
	}

	if snapshotName == "" {
		return fmt.Errorf("快照名称不能为空")
	}

	return demo.TimeTravelQueryFromSnapshot(snapshotName)
}

// timeTravelFromTimestampMenu 从时间戳进行时间旅行查询菜单
func timeTravelFromTimestampMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	fmt.Print("请输入目标时间 (格式: 2024-01-01 10:00:00): ")
	targetTime, _ := reader.ReadString('\n')
	targetTime = strings.TrimSpace(targetTime)

	if targetTime == "" {
		targetTime = "2024-01-01 10:00:00"
	}

	return demo.TimeTravelQueryFromTimestamp(targetTime)
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

// SnapshotInfo 快照信息结构
type SnapshotInfo struct {
	Name      string
	Timestamp string
	Level     string
	Account   string
	Database  string
	Table     string
}

// getSnapshotList 获取快照列表（按时间戳升序排列）
func (d *AIDatasetDemo) getSnapshotList() ([]string, error) {
	snapshots, err := d.getSnapshotInfoList()
	if err != nil {
		return nil, err
	}

	var snapshotNames []string
	for _, snapshot := range snapshots {
		snapshotNames = append(snapshotNames, snapshot.Name)
	}

	return snapshotNames, nil
}

// getSnapshotInfoList 获取快照信息列表（按时间戳升序排列）
func (d *AIDatasetDemo) getSnapshotInfoList() ([]SnapshotInfo, error) {
	query := "SHOW SNAPSHOTS"
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshots: %v", err)
	}
	defer rows.Close()

	var snapshots []SnapshotInfo
	for rows.Next() {
		var snapshot SnapshotInfo
		err := rows.Scan(&snapshot.Name, &snapshot.Timestamp, &snapshot.Level,
			&snapshot.Account, &snapshot.Database, &snapshot.Table)
		if err != nil {
			return nil, fmt.Errorf("failed to scan snapshot row: %v", err)
		}
		snapshots = append(snapshots, snapshot)
	}

	// 在应用层按时间戳升序排序
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Timestamp < snapshots[j].Timestamp
	})

	return snapshots, nil
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
		fmt.Println("4. 🌿 分支 vs 🌿 分支")
		fmt.Println("5. 🌿 分支 vs 📸 快照")
		fmt.Println("6. 🌿 分支 vs 📊 主表")
		fmt.Println("7. 🔙 返回主菜单")
		fmt.Println(strings.Repeat("=", 60))

		fmt.Print("请选择比较类型 (1-7): ")
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
			if err := branchVsBranchMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "5":
			if err := branchVsSnapshotMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "6":
			if err := branchVsMainTableMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "7":
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
	// 获取快照信息列表（按时间戳升序排列）
	snapshots, err := demo.getSnapshotInfoList()
	if err != nil {
		return fmt.Errorf("获取快照列表失败: %v", err)
	}

	if len(snapshots) == 0 {
		return fmt.Errorf("没有找到任何快照")
	}

	// 显示候选快照（最多5个，按时间戳升序）
	fmt.Println("📋 可用的快照 (按时间戳升序排列):")
	maxShow := 5
	if len(snapshots) < maxShow {
		maxShow = len(snapshots)
	}

	for i := 0; i < maxShow; i++ {
		fmt.Printf("  %d. %s (%s)\n", i+1, snapshots[i].Name, snapshots[i].Timestamp)
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
		snapshot1 = snapshots[num-1].Name
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
		snapshot2 = snapshots[num-1].Name
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
	// 获取快照信息列表（按时间戳升序排列）
	snapshots, err := demo.getSnapshotInfoList()
	if err != nil {
		return fmt.Errorf("获取快照列表失败: %v", err)
	}

	if len(snapshots) == 0 {
		return fmt.Errorf("没有找到任何快照")
	}

	// 显示候选快照（最多5个，按时间戳升序）
	fmt.Println("📋 可用的快照 (按时间戳升序排列):")
	maxShow := 5
	if len(snapshots) < maxShow {
		maxShow = len(snapshots)
	}

	for i := 0; i < maxShow; i++ {
		fmt.Printf("  %d. %s (%s)\n", i+1, snapshots[i].Name, snapshots[i].Timestamp)
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
		snapshot = snapshots[num-1].Name
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

// pitrMenu PITR管理菜单
func pitrMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	for {
		fmt.Println("\n" + strings.Repeat("=", 40))
		fmt.Println("🕐 PITR管理")
		fmt.Println(strings.Repeat("=", 40))
		fmt.Println("1. 🕐 创建PITR")
		fmt.Println("2. 📋 查看所有PITR")
		fmt.Println("3. 🗑️  删除PITR")
		fmt.Println("4. 🔙 返回主菜单")
		fmt.Println(strings.Repeat("=", 40))

		fmt.Print("请选择操作 (1-4): ")
		choice, _ := reader.ReadString('\n')
		choice = strings.TrimSpace(choice)

		switch choice {
		case "1":
			if err := createPITRMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "2":
			if err := demo.ShowPITRs(); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "3":
			if err := dropPITRMenu(demo, reader); err != nil {
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

// createPITRMenu 创建PITR菜单
func createPITRMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	fmt.Print("请输入PITR名称 (默认: ai_dataset_3h_pitr): ")
	pitrName, _ := reader.ReadString('\n')
	pitrName = strings.TrimSpace(pitrName)

	if pitrName == "" {
		pitrName = "ai_dataset_3h_pitr"
	}

	fmt.Print("请输入持续时间 (默认: 3 'h'): ")
	duration, _ := reader.ReadString('\n')
	duration = strings.TrimSpace(duration)

	if duration == "" {
		duration = "3 'h'"
	}

	return demo.CreatePITR(pitrName, duration)
}

// dropPITRMenu 删除PITR菜单
func dropPITRMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 获取PITR列表
	pitrList, err := demo.getPITRList()
	if err != nil {
		return fmt.Errorf("获取PITR列表失败: %v", err)
	}

	if len(pitrList) == 0 {
		return fmt.Errorf("没有找到任何PITR")
	}

	// 显示候选PITR（最多5个）
	fmt.Println("📋 可用的PITR:")
	maxShow := 5
	if len(pitrList) < maxShow {
		maxShow = len(pitrList)
	}

	for i := 0; i < maxShow; i++ {
		fmt.Printf("  %d. %s\n", i+1, pitrList[i])
	}
	if len(pitrList) > maxShow {
		fmt.Printf("  ... 还有 %d 个PITR\n", len(pitrList)-maxShow)
	}
	fmt.Println()

	fmt.Print("请输入要删除的PITR名称 (或输入序号): ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	pitrName := input
	if num, err := strconv.Atoi(input); err == nil && num >= 1 && num <= len(pitrList) {
		pitrName = pitrList[num-1]
		fmt.Printf("✅ 选择PITR: %s\n", pitrName)
	}

	if pitrName == "" {
		return fmt.Errorf("PITR名称不能为空")
	}

	return demo.DropPITR(pitrName)
}

// restoreMenu 数据恢复菜单
func restoreMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	for {
		fmt.Println("\n" + strings.Repeat("=", 50))
		fmt.Println("🔄 数据恢复")
		fmt.Println(strings.Repeat("=", 50))
		fmt.Println("1. 📸 从快照恢复")
		fmt.Println("2. 🕐 从PITR时间点恢复")
		fmt.Println("3. 🔙 返回主菜单")
		fmt.Println(strings.Repeat("=", 50))

		fmt.Print("请选择恢复类型 (1-3): ")
		choice, _ := reader.ReadString('\n')
		choice = strings.TrimSpace(choice)

		switch choice {
		case "1":
			if err := restoreFromSnapshotMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "2":
			if err := restoreFromPITRMenu(demo, reader); err != nil {
				fmt.Printf("❌ 错误: %v\n", err)
			}
		case "3":
			return nil
		default:
			fmt.Println("❌ 无效选择，请重新输入")
		}

		fmt.Println("\n按回车键继续...")
		reader.ReadString('\n')
	}
}

// restoreFromSnapshotMenu 从快照恢复菜单
func restoreFromSnapshotMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 获取快照信息列表（按时间戳升序排列）
	snapshots, err := demo.getSnapshotInfoList()
	if err != nil {
		return fmt.Errorf("获取快照列表失败: %v", err)
	}

	if len(snapshots) == 0 {
		return fmt.Errorf("没有找到任何快照")
	}

	// 显示候选快照（最多5个，按时间戳升序）
	fmt.Println("📋 可用的快照 (按时间戳升序排列):")
	maxShow := 5
	if len(snapshots) < maxShow {
		maxShow = len(snapshots)
	}

	for i := 0; i < maxShow; i++ {
		fmt.Printf("  %d. %s (%s)\n", i+1, snapshots[i].Name, snapshots[i].Timestamp)
	}
	if len(snapshots) > maxShow {
		fmt.Printf("  ... 还有 %d 个快照\n", len(snapshots)-maxShow)
	}
	fmt.Println()

	// 选择快照
	fmt.Print("请输入快照名称 (或输入序号): ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	snapshotName := input
	if num, err := strconv.Atoi(input); err == nil && num >= 1 && num <= len(snapshots) {
		snapshotName = snapshots[num-1].Name
		fmt.Printf("✅ 选择快照: %s\n", snapshotName)
	}

	if snapshotName == "" {
		return fmt.Errorf("快照名称不能为空")
	}

	// 确认恢复操作
	fmt.Printf("⚠️  警告：此操作将恢复数据到快照 '%s' 的状态，当前数据将被覆盖！\n", snapshotName)
	fmt.Print("确认恢复吗？(输入 'yes' 确认): ")
	confirmation, _ := reader.ReadString('\n')
	confirmation = strings.TrimSpace(confirmation)

	if confirmation != "yes" {
		fmt.Println("❌ 操作已取消")
		return nil
	}

	return demo.RestoreFromSnapshot(snapshotName)
}

// restoreFromPITRMenu 从PITR时间点恢复菜单
func restoreFromPITRMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 使用默认PITR
	pitrName := demo.getDefaultPITRName()
	fmt.Printf("🕐 使用默认PITR: %s\n", pitrName)

	// 输入时间戳
	fmt.Print("请输入恢复时间点 (格式: 2025-09-09 13:20:04.123456，留空使用当前时间): ")
	timestamp, _ := reader.ReadString('\n')
	timestamp = strings.TrimSpace(timestamp)

	if timestamp == "" {
		timestamp = "now"
		fmt.Printf("✅ 使用当前时间: %s\n", timestamp)
	}

	// 确认恢复操作
	fmt.Printf("⚠️  警告：此操作将恢复数据到PITR '%s' 在时间点 '%s' 的状态，当前数据将被覆盖！\n", pitrName, timestamp)
	fmt.Print("确认恢复吗？(输入 'yes' 确认): ")
	confirmation, _ := reader.ReadString('\n')
	confirmation = strings.TrimSpace(confirmation)

	if confirmation != "yes" {
		fmt.Println("❌ 操作已取消")
		return nil
	}

	return demo.RestoreFromPITR(pitrName, timestamp)
}

// cleanupMenu 清空数据菜单
func cleanupMenu(demo *AIDatasetDemo, reader *bufio.Reader) error {
	// 显示当前状态
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("🧹 一键清空Demo数据")
	fmt.Println(strings.Repeat("=", 60))

	// 获取当前状态
	snapshotCount := demo.getDemoSnapshotCount()
	pitrCount := demo.getDemoPITRCount()
	dataCount := demo.getDataCount()

	fmt.Println("📊 当前Demo数据状态:")
	fmt.Printf("  📸 Demo相关快照: %d 个\n", snapshotCount)
	fmt.Printf("  🕐 Demo相关PITR: %d 个\n", pitrCount)
	fmt.Printf("  🗑️  ai_dataset表数据: %d 行\n", dataCount)
	fmt.Println()

	if snapshotCount == 0 && pitrCount == 0 && dataCount == 0 {
		fmt.Println("ℹ️  没有找到需要清理的Demo数据")
		return nil
	}

	// 警告信息
	fmt.Println("⚠️  警告：此操作将删除所有Demo相关数据，包括：")
	fmt.Println("  • 所有包含 'ai_dataset' 的快照")
	fmt.Println("  • 所有包含 'ai_dataset' 的PITR")
	fmt.Println("  • ai_dataset表中的所有数据")
	fmt.Println("  • 此操作无法撤销！")
	fmt.Println()

	// 确认操作
	fmt.Print("确认要清空所有Demo数据吗？(输入 'CLEANUP' 确认): ")
	confirmation, _ := reader.ReadString('\n')
	confirmation = strings.TrimSpace(confirmation)

	if confirmation != "CLEANUP" {
		fmt.Println("❌ 操作已取消")
		return nil
	}

	// 执行清理
	return demo.CleanupAllDemoData()
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
