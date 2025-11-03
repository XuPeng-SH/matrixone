// Copyright 2025 Matrix Origin
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

package dispatch

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// TestSendToAnyRemoteFuncReturnsNilOnCancel catches Bug #4
// Directly calls sendToAnyRemoteFunc() with cancelled context
func TestSendToAnyRemoteFuncReturnsNilOnCancel(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Cancel context
	ctx, cancel := context.WithCancel(proc.Ctx)
	cancel()
	proc.Ctx = ctx

	// Setup minimal dispatch (prepared=true to skip network wait)
	d := &Dispatch{
		ctr: &container{
			prepared:      true,
			remoteRegsCnt: 1,
			aliveRegCnt:   1,
			sendCnt:       0,
		},
	}

	// Create test batch
	bat := batch.NewWithSize(1)
	bat.SetRowCount(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(bat.Vecs[0], int64(1), false, proc.Mp())

	// Call actual sendToAnyRemoteFunc() - this is the target function
	end, err := sendToAnyRemoteFunc(bat, d, proc)

	// Bug #4: Before fix, err == nil
	// After fix: err == context.Canceled
	require.True(t, end, "should return end=true")
	if err == nil {
		t.Error("BUG #4 DETECTED: sendToAnyRemoteFunc returns nil when context cancelled")
	}
	require.Error(t, err, "sendToAnyRemoteFunc should return context error, not nil")
}

// TestSendToAllRemoteFuncWithContextCancel tests sendToAllRemoteFunc
// This function iterates through remoteReceivers, need proper setup
func TestSendToAllRemoteFuncWithContextCancel(t *testing.T) {
	proc := testutil.NewProcess(t)

	ctx, cancel := context.WithCancel(proc.Ctx)
	cancel()
	proc.Ctx = ctx

	// sendToAllRemoteFunc doesn't have the context check bug (no select on ctx.Done)
	// It will fail when trying to marshal/send, but won't return nil on context cancel
	// So this test just documents that sendToAllRemoteFunc has different behavior

	d := &Dispatch{
		ctr: &container{
			prepared:        true,
			remoteReceivers: []*process.WrapCs{}, // Empty to avoid network calls
			remoteRegsCnt:   0,
			aliveRegCnt:     0,
		},
	}

	bat := batch.NewWithSize(1)
	bat.SetRowCount(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(bat.Vecs[0], int64(1), false, proc.Mp())

	// With remoteRegsCnt=0, returns (false, nil) not (true, nil)
	end, err := sendToAllRemoteFunc(bat, d, proc)

	// sendToAllRemoteFunc returns false when finished normally with empty receivers
	require.False(t, end)
	require.NoError(t, err)
}
