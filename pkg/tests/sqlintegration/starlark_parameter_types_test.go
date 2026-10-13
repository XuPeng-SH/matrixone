// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestStarlarkVectorParameterReuse(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const schema = "starlark_parameter_reuse"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
		exec("drop database if exists " + schema)
		exec("create database " + schema)
		exec("use " + schema)
		for _, typ := range []string{"vecf32", "vecf64", "vecbf16", "vecf16", "vecint8", "vecuint8"} {
			t.Run(typ, func(t *testing.T) {
				exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
				exec("create procedure reuse_v(inout v " + typ + "(2)) language 'starlark' $$out_v = v$$")
				exec("set @v = '[1,2]'")
				for call := 0; call < 2; call++ {
					exec("call reuse_v(@v)")
				}
				var equal bool
				require.NoError(t, conn.QueryRowContext(ctx, "select @v = cast('[1,2]' as "+typ+"(2))").Scan(&equal))
				require.True(t, equal)
				exec("create procedure bad_v(inout v " + typ + "(2)) language 'starlark' $$out_v = '[1,2,3]'$$")
				_, err := conn.ExecContext(ctx, "call bad_v(@v)")
				require.Error(t, err)
				exec("call reuse_v(@v)")
				require.NoError(t, conn.QueryRowContext(ctx, "select @v = cast('[1,2]' as "+typ+"(2))").Scan(&equal))
				require.True(t, equal)
				exec("drop procedure bad_v")
				exec("drop procedure reuse_v")
			})
		}
		for _, tc := range []struct{ typ, value, query, want string }{
			{"decimal(38,2)", "'123456789012345678901234567890.25'", "select @v - cast('123456789012345678901234567890.25' as decimal(38,2))", "0.00"},
			{"bigint unsigned", "18446744073709551615", "select cast(@v as char)", "18446744073709551615"},
			{"double", "1.25", "select @v = cast(1.25 as double)", "1"},
			{"boolean", "True", "select @v is true", "1"},
			{"date", "'2026-10-14'", "select @v = cast('2026-10-14' as date)", "1"},
			{"datetime(3)", "'2026-10-14 12:34:56.125'", "select @v = cast('2026-10-14 12:34:56.125' as datetime(3))", "1"},
			{"time(3)", "'12:34:56.125'", "select @v = cast('12:34:56.125' as time(3))", "1"},
			{"json", "'{\"a\":1}'", "select json_extract(@v,'$.a')", "1"},
			{"uuid", "'00000000-0000-0000-0000-000000000001'", "select @v = cast('00000000-0000-0000-0000-000000000001' as uuid)", "1"},
			{"binary(3)", "'a'", "select hex(@v)", "610000"},
			{"varbinary(3)", "'abc'", "select hex(@v)", "616263"},
		} {
			t.Run(tc.typ, func(t *testing.T) {
				exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
				exec("create procedure scalar_v(inout v " + tc.typ + ") language 'starlark' $$out_v = " + tc.value + "$$")
				exec("set @v = 1")
				for call := 0; call < 2; call++ {
					exec("call scalar_v(@v)")
				}
				var got string
				require.NoError(t, conn.QueryRowContext(ctx, tc.query).Scan(&got))
				require.Equal(t, tc.want, got)
				if tc.typ == "decimal(38,2)" || tc.typ == "varbinary(3)" {
					query := "select cast(? as decimal(38,2)) - cast('123456789012345678901234567890.25' as decimal(38,2))"
					if tc.typ == "varbinary(3)" {
						query = "select hex(?)"
					}
					exec("prepare typed_output from '" + strings.ReplaceAll(query, "'", "''") + "'")
					require.NoError(t, conn.QueryRowContext(ctx, "execute typed_output using @v").Scan(&got))
					require.Equal(t, tc.want, got)
					exec("deallocate prepare typed_output")
				}
				exec("drop procedure scalar_v")
			})
		}

	})
}
