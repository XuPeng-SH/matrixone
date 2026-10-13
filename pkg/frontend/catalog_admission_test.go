// Copyright 2024 Matrix Origin
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

package frontend

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	planner "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Exercise the production frontend context and planner, mocking only catalog
// and engine I/O. A partial decoder or a mock Resolve must not hide metadata.
func TestFrontendCatalogAdmissionBeforeSchemaOnlyScan(t *testing.T) {
	for _, tc := range []struct {
		name                                                                         string
		column, columnRevision, defaultID, defaultRevision, tableFormat, indexFormat uint32
		allowed                                                                      bool
	}{
		{name: "legacy0", allowed: true},
		{name: "binary1", column: 1, allowed: true},
		{name: "legacy_bin2", column: 2, allowed: true},
		{name: "legacy_ci3", column: 3, defaultID: 3, allowed: true},
		{name: "uca400_9", column: 9, columnRevision: 1, defaultID: 9, defaultRevision: 1, allowed: true},
		{name: "uca400_10", column: 10, columnRevision: 1, defaultID: 10, defaultRevision: 1, allowed: true},
		{name: "native_column4", column: 4, columnRevision: 1},
		{name: "native_column5", column: 5, columnRevision: 1},
		{name: "wide_column", column: 260, columnRevision: 1},
		{name: "unknown_column_revision", column: 3, columnRevision: 2},
		{name: "native_default", column: 3, defaultID: 4, defaultRevision: 1},
		{name: "wide_default", column: 3, defaultID: 260, defaultRevision: 1},
		{name: "unknown_default_revision", column: 3, defaultID: 3, defaultRevision: 2},
		{name: "native_table_format", column: 3, tableFormat: 1},
		{name: "wide_table_format", column: 3, tableFormat: 256},
		{name: "native_index_format", column: 3, indexFormat: 1},
		{name: "wide_index_format", column: 3, indexFormat: 256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			proc := testutil.NewProcess(t)
			fixture := planner.NewMockCompilerContext(false, proc)
			_, catalogDef, err := fixture.Resolve("tpch", "nation", nil)
			require.NoError(t, err)
			catalogDef.TblId, catalogDef.DbId, catalogDef.Version = 42, 10, 7
			col := planner.FindColumn(catalogDef.Cols, "n_name")
			require.NotNil(t, col)
			col.Typ.Charset, col.Typ.CollationVersion = tc.column, tc.columnRevision
			catalogDef.DefaultCharset, catalogDef.CollationVersion = tc.defaultID, tc.defaultRevision
			catalogDef.KeyFormat = tc.tableFormat
			catalogDef.Indexes = []*pbplan.IndexDef{{IndexName: "idx", KeyFormat: tc.indexFormat}}

			txnOp := mock_frontend.NewMockTxnOperator(ctrl)
			txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
			txnOp.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).AnyTimes()
			txnOp.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
			storage := mock_frontend.NewMockEngine(ctrl)
			database := mock_frontend.NewMockDatabase(ctrl)
			relation := mock_frontend.NewMockRelation(ctrl)
			storage.EXPECT().Database(gomock.Any(), "tpch", txnOp).Return(database, nil).AnyTimes()
			database.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
			database.EXPECT().GetDatabaseId(gomock.Any()).Return("10").AnyTimes()
			database.EXPECT().Relation(gomock.Any(), "nation", nil).Return(relation, nil).AnyTimes()
			resolveCalls, statsCalls := 0, 0
			relation.EXPECT().GetTableDef(gomock.Any()).DoAndReturn(func(context.Context) *pbplan.TableDef { resolveCalls++; return catalogDef }).AnyTimes()
			relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(42)).AnyTimes()
			relation.EXPECT().Stats(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, bool) (*pb.StatsInfo, error) { statsCalls++; return nil, nil }).AnyTimes()

			ses := newTestSession(t, ctrl)
			defer ses.Close()
			ses.SetDatabaseName("tpch")
			ses.txnHandler = InitTxnHandler("", storage, proc.Ctx, txnOp)
			getPu("").StorageEngine = storage
			tcc := InitTxnCompilerContext("tpch")
			tcc.execCtx = &ExecCtx{reqCtx: proc.Ctx, ses: ses, proc: proc}
			defer tcc.Close()
			build := func(sql string, allowed bool) {
				t.Helper()
				before, err := catalogDef.Marshal()
				require.NoError(t, err)
				resolveCalls, statsCalls = 0, 0
				tcc.execCtx.rootSQLOverride = &sql
				stmt, err := parsers.ParseOne(proc.Ctx, dialect.MYSQL, sql, 1)
				require.NoError(t, err)
				p, err := planner.BuildPlan(tcc, stmt, false)
				stmt.Free()
				if allowed {
					require.NoError(t, err, sql)
					require.NotNil(t, p)
					require.Len(t, p.GetQuery().CatalogDependencies, 1)
					dep := p.GetQuery().CatalogDependencies[0]
					require.Equal(t, int64(42), dep.Obj)
					require.Equal(t, int64(7), dep.Server)
					if sql == "select * from nation limit 0" {
						require.Len(t, planner.GetResultColumnsFromPlan(p), 4)
						require.Zero(t, statsCalls, "schema-only scan must not read statistics")
					}
				} else {
					require.Error(t, err, sql)
					require.Nil(t, p)
				}
				require.Equal(t, 1, resolveCalls, "complete metadata must resolve once")
				after, err := catalogDef.Marshal()
				require.NoError(t, err)
				require.Equal(t, before, after, "planner must not mutate catalog definitions")
			}
			for _, sql := range []string{"select 1 from nation", "select * from nation limit 0"} {
				build(sql, tc.allowed)
			}
			// A rejected statement cannot poison the same context or a later empty scan.
			col.Typ.Charset, col.Typ.CollationVersion = uint32(types.CharsetUTF8), 0
			catalogDef.DefaultCharset, catalogDef.CollationVersion, catalogDef.KeyFormat = 3, 0, 0
			catalogDef.Indexes[0].KeyFormat = 0
			build("select * from nation limit 0", true)
		})
	}
}
