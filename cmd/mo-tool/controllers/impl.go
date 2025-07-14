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

package controllers

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func forceCheckPoint(
	db *sql.DB,
	verbose bool,
) {
	checkpointSql := "select mo_ctl('dn', 'checkpoint', '1');"
	_, err := db.Exec(checkpointSql)
	if err != nil {
		log.Fatal(err)
	}
	if verbose {
		log.Println("force checkpoint done")
	}
}

func forceCheckpointLoop(
	ctx context.Context,
	period time.Duration,
	db *sql.DB,
	verbose bool,
	wg *sync.WaitGroup,
) {
	if wg != nil {
		defer wg.Done()
	}
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Println("force checkpoint loop done")
			return
		case <-ticker.C:
			forceCheckPoint(db, verbose)
		}
	}
}

func forceFlush(
	db *sql.DB,
	dbName, tableName string,
	verbose bool,
	mustSuccess bool,
) (err error) {
	flushSql := fmt.Sprintf("select mo_ctl('dn', 'flush', '%s.%s');", dbName, tableName)
	if verbose {
		log.Println(flushSql)
	}
	_, err = db.Exec(flushSql)
	if mustSuccess && err != nil {
		log.Fatal(err)
	}
	return
}

func forceFlushLoop(
	ctx context.Context,
	period time.Duration,
	db *sql.DB,
	dbName, tableName string,
	wg *sync.WaitGroup,
	verbose bool,
	onEachFlush func(int, error) bool,
) {
	if wg != nil {
		defer wg.Done()
	}
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	cnt := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := forceFlush(db, dbName, tableName, false, false)
			if onEachFlush != nil {
				if !onEachFlush(cnt, err) {
					return
				}
			}
			cnt++
			if verbose {
				log.Println("force flush", dbName, tableName, cnt, err)
			}
		}
	}
}
