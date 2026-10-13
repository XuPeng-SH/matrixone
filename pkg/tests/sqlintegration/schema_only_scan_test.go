// Copyright 2026 Matrix Origin
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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestSchemaOnlyScanMetadataAndPreparedDependencies(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists schema_only_scan")
		exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
		columns := func(q string, want []string) {
			t.Helper()
			rows, err := conn.QueryContext(ctx, q)
			require.NoError(t, err, q)
			defer rows.Close()
			got, err := rows.Columns()
			require.NoError(t, err)
			require.Equal(t, want, got)
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}
		exec("create database schema_only_scan")
		exec("use schema_only_scan")
		exec("create table t(k int primary key,v varchar(16),index iv(v))")
		exec("insert into t values(1,'Alpha'),(2,'Beta')")
		columns("select * from t limit 0", []string{"k", "v"})
		exec("create view viewed as select * from t")
		columns("select * from viewed limit 0", []string{"k", "v"})
		exec("create temporary table temporary_t(k int primary key,alias_value int)")
		columns("select * from temporary_t limit 0", []string{"k", "alias_value"})

		exec("prepare empty_plan from 'select * from t limit 0'")
		defer conn.ExecContext(context.Background(), "deallocate prepare empty_plan")
		columns("execute empty_plan", []string{"k", "v"})
		exec("alter table t add column added int")
		columns("execute empty_plan", []string{"k", "v", "added"})
		// CREATE VIEW expands its star at creation; later columns are not outputs.
		columns("select * from viewed limit 0", []string{"k", "v"})
		exec("drop view viewed")
		exec("drop table t")
		exec("create view t as select 1 as replacement")
		columns("execute empty_plan", []string{"replacement"})
		var unused any
		err = conn.QueryRowContext(ctx, "select * from missing_table limit 0").Scan(&unused)
		require.ErrorContains(t, err, "missing_table")
		columns("select * from t limit 0", []string{"replacement"})
	})
}
