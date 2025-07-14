// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mockdata

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/cmd/mo-tool/common"
	"github.com/spf13/cobra"
)

var case1DropTableSQL = []string{
	`DROP TABLE IF EXISTS case1_table_0`,
}

var case1CreateTableSQL = []string{
	`
	CREATE TABLE IF NOT EXISTS case1_table_0 (
		id BIGINT PRIMARY KEY,
		col1 TINYINT,
		col2 SMALLINT,
		col3 INT,
		col4 BIGINT,
		col5 TINYINT UNSIGNED,
		col6 SMALLINT UNSIGNED,
		col7 INT UNSIGNED,
		col8 BIGINT UNSIGNED,
		col9 FLOAT,
		col10 DOUBLE,
		col11 VARCHAR(255),
		col12 DATE,
		col13 DATETIME,
		col14 TIMESTAMP,
		col15 BOOL,
		col16 DECIMAL(16,6),
		col17 TEXT,
		col18 JSON,
		col19 BLOB,
		col20 BINARY(255),
		col21 VARBINARY(255),
		col22 VECF32(3),
		col23 VECF32(3),
		col24 VECF64(3),
		col25 VECF64(3)
	)`,
}

var case1InsertSQL = []string{
	`
	INSERT INTO case1_table_0 (
		id, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
		col11, col12, col13, col14, col15, col16, col17, col18, col19,
		col20, col21, col22, col23, col24, col25
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
}

func dropCase1Table(db *sql.DB, idx int) error {
	_, err := db.Exec(case1DropTableSQL[idx])
	return err
}

func createCase1Table(db *sql.DB, idx int) error {
	_, err := db.Exec(case1CreateTableSQL[idx])
	return err
}

func generateMockData(values *[]any, caseID int, id int64) {
	switch caseID {
	case 0:
		generateCase1_0_Data(values, id)
	default:
		panic(fmt.Sprintf("invalid caseID: %d", caseID))
	}
}

func generateCase1_0_Data(values *[]any, id int64) {
	*values = append(*values, id)                  // id
	*values = append(*values, 42)                  // col1
	*values = append(*values, 1234)                // col2
	*values = append(*values, 123456)              // col3
	*values = append(*values, 123456789)           // col4
	*values = append(*values, 128)                 // col5
	*values = append(*values, 12345)               // col6
	*values = append(*values, 1234567)             // col7
	*values = append(*values, 1234567890123456789) // col8
	*values = append(*values, 3.14159)
	*values = append(*values, 2.718281828459045)
	*values = append(*values, "mock_string_value")
	*values = append(*values, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	*values = append(*values, time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC))
	*values = append(*values, time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC))
	*values = append(*values, true)
	*values = append(*values, "123456.789012")
	*values = append(*values, "mock_text_content")
	*values = append(*values, `{"key": "value", "number": 42}`)
	*values = append(*values, []byte("mock_blob_data"))
	*values = append(*values, []byte("mock_binary_data"))
	*values = append(*values, []byte("mock_varbinary_data"))
	*values = append(*values, []byte("[1, 2, 3]"))
	*values = append(*values, []byte("[1, 2, 3]"))
	*values = append(*values, []byte("[1, 2, 3]"))
	*values = append(*values, []byte("[1, 2, 3]"))
}

func insertCase1Batch(
	ctx context.Context,
	db *sql.DB,
	caseID int,
	batchID int,
	batchSize int,
	values *[]any,
	errChan chan error,
) {
	if caseID < 0 || caseID >= len(case1InsertSQL) {
		errChan <- fmt.Errorf("invalid idx: %d", caseID)
		return
	}

	insertSQL := case1InsertSQL[caseID]

	stmt, err := db.Prepare(insertSQL)
	if err != nil {
		errChan <- fmt.Errorf("failed to prepare statement: %v", err)
		return
	}
	defer stmt.Close()

	tx, err := db.Begin()
	if err != nil {
		errChan <- fmt.Errorf("failed to begin transaction: %v", err)
		return
	}

	startKey := int64(batchID * batchSize)
	for i := 0; i < batchSize; i++ {
		*values = (*values)[:0]
		generateMockData(values, caseID, startKey+int64(i))
		select {
		case <-ctx.Done():
			tx.Rollback()
			log.Printf("Batch %d cancelled", batchID)
			return
		default:
		}

		_, err := tx.Stmt(stmt).Exec(*values...)
		if err != nil {
			tx.Rollback()
			errChan <- fmt.Errorf("failed to insert row %d in batch %d: %v", i, batchID, err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		errChan <- fmt.Errorf("failed to commit batch %d: %v", batchID, err)
		return
	}

	log.Printf("Batch %d completed: inserted %d rows", batchID, batchSize)
}

func case1Main(cmd *cobra.Command, caseID int) {
	url, user, password, dbName, err := common.DBFlagValues(cmd)
	if err != nil {
		log.Fatalf("Failed to get db flag values: %v", err)
	}

	db, err := sql.Open("mysql", common.GetDBUrl(url, user, password, dbName))
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	log.Println("Connected to database successfully")

	clean, _ := cmd.Flags().GetBool("clean")
	if clean {
		if err := dropCase1Table(db, caseID); err != nil {
			log.Fatalf("Failed to drop table: %v", err)
		}
	}

	if err := createCase1Table(db, caseID); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	log.Println("Tables created successfully")
	parallel, _ := cmd.Flags().GetInt("parallel")
	batchSize, _ := cmd.Flags().GetInt("batch-size")
	totalBatches, _ := cmd.Flags().GetInt("total-batches")
	db.SetMaxOpenConns(parallel + 5)
	db.SetMaxIdleConns(parallel)
	db.SetConnMaxLifetime(time.Hour)

	var wg sync.WaitGroup
	errChan := make(chan error, parallel)
	batchChan := make(chan int, parallel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			values := make([]any, 0, 26)
			for batchID := range batchChan {
				select {
				case <-ctx.Done():
					log.Printf("Worker %d cancelled", workerID)
					return
				default:
				}

				values = values[:0]
				insertCase1Batch(ctx, db, caseID, batchID, batchSize, &values, errChan)
			}
		}(i)
	}

	go func() {
		log.Printf("Starting to send %d batches to workers...", totalBatches)
		for i := 0; i < totalBatches; i++ {
			batchChan <- i
		}
		close(batchChan)
		log.Printf("All batches sent to workers")
	}()

	go func() {
		for err := range errChan {
			log.Printf("Error: %v", err)
			cancel()
		}
	}()

	wg.Wait()
	close(errChan)

	log.Printf("Completed! Inserted %d batches with %d rows each (total: %d rows)",
		totalBatches, batchSize, totalBatches*batchSize)
}
