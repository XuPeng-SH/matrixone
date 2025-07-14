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
	"database/sql"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/cmd/mo-tool/common"
	"github.com/spf13/cobra"
)

func PrepareCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ctr",
		Short: "Mo ctr helper",
		Long:  "Mo ctr helper",
	}

	cmd.AddCommand(forceFlushLoopCommand())

	return cmd
}

func forceFlushLoopCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "force-flush",
		Short: "Force flush loop",
		Long:  "Force flush loop",
	}

	common.AddDBFlags(cmd)
	cmd.Flags().String("table", "", "Table name")
	cmd.Flags().Int("period", 5, "Period in second, default 5")
	cmd.Flags().Bool("verbose", false, "Verbose, default false")
	cmd.Flags().Bool("must-success", false, "Must success, default false")

	cmd.Run = func(cmd *cobra.Command, args []string) {
		url, user, password, dbName, err := common.DBFlagValues(cmd)
		if err != nil {
			log.Fatal(err)
		}
		fullUrl := common.GetDBUrl(url, user, password, dbName)
		tableName, _ := cmd.Flags().GetString("table")
		period, _ := cmd.Flags().GetInt("period")
		verbose, _ := cmd.Flags().GetBool("verbose")
		mustSuccess, _ := cmd.Flags().GetBool("must-success")

		db, err := sql.Open("mysql", fullUrl)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		var wg sync.WaitGroup
		wg.Add(1)

		go forceFlushLoop(
			cmd.Context(),
			time.Duration(period)*time.Second,
			db,
			dbName,
			tableName,
			&wg,
			verbose,
			func(cnt int, err error) bool {
				if mustSuccess {
					log.Println("force flush break", dbName, tableName, cnt, err)
					return false
				}
				return true
			},
		)

		wg.Wait()
		log.Println("force flush loop done")
	}

	return cmd
}
