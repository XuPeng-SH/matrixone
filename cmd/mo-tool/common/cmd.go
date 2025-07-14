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

package common

import (
	"fmt"

	"github.com/spf13/cobra"
)

func DBFlagValues(cmd *cobra.Command) (
	url string,
	user string,
	password string,
	db string,
	err error,
) {
	if url, err = cmd.Flags().GetString("url"); err != nil {
		return
	}
	if user, err = cmd.Flags().GetString("user"); err != nil {
		return
	}
	if password, err = cmd.Flags().GetString("password"); err != nil {
		return
	}
	if db, err = cmd.Flags().GetString("db"); err != nil {
		return
	}
	return url, user, password, db, nil
}

func AddDBFlags(cmd *cobra.Command) {
	cmd.Flags().String("url", "127.0.0.1:6001", "database url")
	cmd.Flags().String("user", "dump", "database user")
	cmd.Flags().String("password", "111", "database password")
	cmd.Flags().String("db", "", "database name")
}

func GetDBUrl(
	url string,
	user string,
	password string,
	db string,
) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", user, password, url, db)
}
