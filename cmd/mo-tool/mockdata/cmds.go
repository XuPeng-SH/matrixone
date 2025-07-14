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
	"github.com/matrixorigin/matrixone/cmd/mo-tool/common"
	"github.com/spf13/cobra"
)

func PrepareCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mockdata",
		Short: "Mock data",
		Long:  "Mock data",
	}

	cmd.AddCommand(mockBigDataCase1Command())

	return cmd
}

func mockBigDataCase1Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bd-case1",
		Short: "Mock big data case1",
		Long:  "Mock big data case1",
	}

	common.AddDBFlags(cmd)
	cmd.Flags().Int("batch-size", 1000, "Batch size, default 1000")
	cmd.Flags().Int("total-batches", 2000, "Total batches, default 2000")
	cmd.Flags().Int("parallel", 10, "Parallel, default 10")
	cmd.Flags().Int("case-id", 0, "Case id, default 0")
	cmd.Flags().Bool("clean", false, "Clean, default false")

	cmd.Run = func(cmd *cobra.Command, args []string) {
		caseID, _ := cmd.Flags().GetInt("case-id")
		case1Main(cmd, caseID)
	}

	return cmd
}
