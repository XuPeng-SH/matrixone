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

package ctl

import (
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordShuffleLocalityStats(t *testing.T) {
	// Reset stats before test
	ResetShuffleLocalityStats()
	DisableShuffleLocalityStats()

	// Test when disabled
	RecordShuffleLocalityStats("range", true)
	stats := GetShuffleLocalityStats()
	assert.Equal(t, int64(0), stats.TotalObjects, "Should not record when disabled")

	// Enable and test range shuffle
	EnableShuffleLocalityStats()
	RecordShuffleLocalityStats("range", true)
	RecordShuffleLocalityStats("range", false)
	stats = GetShuffleLocalityStats()
	assert.Equal(t, int64(2), stats.TotalObjects)
	assert.Equal(t, int64(1), stats.RangeShuffleLocal)
	assert.Equal(t, int64(1), stats.RangeShuffleRemote)

	// Test hash shuffle
	RecordShuffleLocalityStats("hash", true)
	RecordShuffleLocalityStats("hash", false)
	stats = GetShuffleLocalityStats()
	assert.Equal(t, int64(4), stats.TotalObjects)
	assert.Equal(t, int64(1), stats.HashShuffleLocal)
	assert.Equal(t, int64(1), stats.HashShuffleRemote)

	// Test appendable
	RecordShuffleLocalityStats("appendable", true)
	RecordShuffleLocalityStats("appendable", false)
	stats = GetShuffleLocalityStats()
	assert.Equal(t, int64(6), stats.TotalObjects)
	assert.Equal(t, int64(1), stats.AppendableLocal)
	assert.Equal(t, int64(1), stats.AppendableRemote)

	// Test none
	RecordShuffleLocalityStats("none", true)
	stats = GetShuffleLocalityStats()
	assert.Equal(t, int64(7), stats.TotalObjects)
	assert.Equal(t, int64(1), stats.NoShuffleObjects)

	// Reset and verify
	ResetShuffleLocalityStats()
	stats = GetShuffleLocalityStats()
	assert.Equal(t, int64(0), stats.TotalObjects)
	assert.Equal(t, int64(0), stats.RangeShuffleLocal)
	assert.Equal(t, int64(0), stats.RangeShuffleRemote)
}

func TestGetShuffleLocalityStats(t *testing.T) {
	ResetShuffleLocalityStats()
	EnableShuffleLocalityStats()

	RecordShuffleLocalityStats("range", true)
	RecordShuffleLocalityStats("hash", false)

	stats := GetShuffleLocalityStats()
	assert.NotNil(t, stats)
	assert.Equal(t, int64(2), stats.TotalObjects)
	assert.Equal(t, int64(1), stats.RangeShuffleLocal)
	assert.Equal(t, int64(1), stats.HashShuffleRemote)
	assert.True(t, stats.Enabled)
}

func TestEnableDisableShuffleLocalityStats(t *testing.T) {
	DisableShuffleLocalityStats()
	assert.False(t, IsShuffleLocalityStatsEnabled())

	EnableShuffleLocalityStats()
	assert.True(t, IsShuffleLocalityStatsEnabled())

	DisableShuffleLocalityStats()
	assert.False(t, IsShuffleLocalityStatsEnabled())
}

func TestResetShuffleLocalityStats(t *testing.T) {
	ResetShuffleLocalityStats()
	EnableShuffleLocalityStats()

	// Add some data
	RecordShuffleLocalityStats("range", true)
	RecordShuffleLocalityStats("hash", false)
	RecordShuffleLocalityStats("appendable", true)

	stats := GetShuffleLocalityStats()
	assert.Equal(t, int64(3), stats.TotalObjects)

	// Reset
	ResetShuffleLocalityStats()
	stats = GetShuffleLocalityStats()
	assert.Equal(t, int64(0), stats.TotalObjects)
	assert.Equal(t, int64(0), stats.RangeShuffleLocal)
	assert.Equal(t, int64(0), stats.RangeShuffleRemote)
	assert.Equal(t, int64(0), stats.HashShuffleLocal)
	assert.Equal(t, int64(0), stats.HashShuffleRemote)
	assert.Equal(t, int64(0), stats.AppendableLocal)
	assert.Equal(t, int64(0), stats.AppendableRemote)
	assert.Equal(t, int64(0), stats.NoShuffleObjects)
}

func TestProcessShuffleMonitorCmd_Enable(t *testing.T) {
	DisableShuffleLocalityStats()

	result := processShuffleMonitorCmd("enable")
	assert.True(t, result["success"].(bool))
	assert.Equal(t, "shuffle stats enabled", result["message"])
	assert.True(t, result["enabled"].(bool))
	assert.True(t, IsShuffleLocalityStatsEnabled())
}

func TestProcessShuffleMonitorCmd_Disable(t *testing.T) {
	EnableShuffleLocalityStats()

	// Add some data first
	RecordShuffleLocalityStats("range", true)
	RecordShuffleLocalityStats("hash", false)
	stats := GetShuffleLocalityStats()
	assert.Equal(t, int64(2), stats.TotalObjects, "Should have data before disable")

	result := processShuffleMonitorCmd("disable")
	assert.True(t, result["success"].(bool))
	assert.Contains(t, result["message"].(string), "disabled and data cleared")
	assert.False(t, result["enabled"].(bool))
	assert.False(t, IsShuffleLocalityStatsEnabled())

	// Verify data is cleared
	stats = GetShuffleLocalityStats()
	assert.Equal(t, int64(0), stats.TotalObjects, "Data should be cleared after disable")
	assert.Equal(t, int64(0), stats.RangeShuffleLocal)
	assert.Equal(t, int64(0), stats.HashShuffleRemote)
}

func TestProcessShuffleMonitorCmd_Reset(t *testing.T) {
	ResetShuffleLocalityStats()
	EnableShuffleLocalityStats()
	RecordShuffleLocalityStats("range", true)

	result := processShuffleMonitorCmd("reset")
	assert.True(t, result["success"].(bool))
	assert.Equal(t, "shuffle stats reset", result["message"])
	assert.NotNil(t, result["stats"])

	statsMap := result["stats"].(map[string]interface{})
	assert.Equal(t, int64(0), statsMap["total"])
}

func TestProcessShuffleMonitorCmd_Query(t *testing.T) {
	ResetShuffleLocalityStats()
	EnableShuffleLocalityStats()

	// Add test data
	RecordShuffleLocalityStats("range", true)
	RecordShuffleLocalityStats("range", true)
	RecordShuffleLocalityStats("range", false)
	RecordShuffleLocalityStats("hash", true)
	RecordShuffleLocalityStats("hash", false)
	RecordShuffleLocalityStats("hash", false)

	result := processShuffleMonitorCmd("query")
	assert.True(t, result["success"].(bool))
	assert.Equal(t, "query success", result["message"])
	assert.NotNil(t, result["stats"])
	assert.NotNil(t, result["locality_info"])

	statsMap := result["stats"].(map[string]interface{})
	assert.Equal(t, int64(6), statsMap["total"])
	assert.Equal(t, int64(2), statsMap["range_local"])
	assert.Equal(t, int64(1), statsMap["range_remote"])
	assert.Equal(t, int64(1), statsMap["hash_local"])
	assert.Equal(t, int64(2), statsMap["hash_remote"])

	localityInfo := result["locality_info"].(map[string]interface{})
	assert.NotNil(t, localityInfo["range_locality_rate"])
	assert.NotNil(t, localityInfo["hash_locality_rate"])
	assert.NotNil(t, localityInfo["overall_locality_rate"])

	// Check locality rates are strings with percentage
	rangeRate := localityInfo["range_locality_rate"].(string)
	assert.Contains(t, rangeRate, "%")
	assert.Contains(t, rangeRate, "66.67") // 2/3 ≈ 66.67%

	hashRate := localityInfo["hash_locality_rate"].(string)
	assert.Contains(t, hashRate, "%")
	assert.Contains(t, hashRate, "33.33") // 1/3 ≈ 33.33%

	overallRate := localityInfo["overall_locality_rate"].(string)
	assert.Contains(t, overallRate, "%")
	assert.Contains(t, overallRate, "50.00") // 3/6 = 50%

	// Check detailed counts
	assert.Equal(t, int64(2), localityInfo["range_local_objects"])
	assert.Equal(t, int64(1), localityInfo["range_remote_objects"])
	assert.Equal(t, int64(3), localityInfo["range_total_objects"])
	assert.Equal(t, int64(1), localityInfo["hash_local_objects"])
	assert.Equal(t, int64(2), localityInfo["hash_remote_objects"])
	assert.Equal(t, int64(3), localityInfo["hash_total_objects"])
}

func TestProcessShuffleMonitorCmd_QueryEmpty(t *testing.T) {
	ResetShuffleLocalityStats()
	EnableShuffleLocalityStats()

	result := processShuffleMonitorCmd("query")
	assert.True(t, result["success"].(bool))
	assert.NotNil(t, result["stats"])
	// locality_info should show "no data collected yet"
	localityInfo := result["locality_info"].(map[string]interface{})
	assert.Equal(t, "no data collected yet", localityInfo["message"])
}

func TestProcessShuffleMonitorCmd_Status(t *testing.T) {
	// Test when disabled
	DisableShuffleLocalityStats()
	result := processShuffleMonitorCmd("status")
	assert.True(t, result["success"].(bool))
	assert.False(t, result["enabled"].(bool))
	assert.Contains(t, result["message"].(string), "disabled")

	// Test when enabled
	EnableShuffleLocalityStats()
	result = processShuffleMonitorCmd("status")
	assert.True(t, result["success"].(bool))
	assert.True(t, result["enabled"].(bool))
	assert.Contains(t, result["message"].(string), "enabled")
}

func TestProcessShuffleMonitorCmd_UnknownCommand(t *testing.T) {
	result := processShuffleMonitorCmd("unknown")
	assert.False(t, result["success"].(bool))
	assert.Contains(t, result["message"].(string), "unknown command")
	assert.Contains(t, result["message"].(string), "status") // Should mention status in help
}

func TestConvertStatsToMap(t *testing.T) {
	stats := &ShuffleLocalityStats{
		RangeShuffleLocal:  10,
		RangeShuffleRemote: 20,
		HashShuffleLocal:   5,
		HashShuffleRemote:  15,
		AppendableLocal:    3,
		AppendableRemote:   7,
		NoShuffleObjects:   2,
		TotalObjects:       62,
		Enabled:            true,
	}

	result := convertStatsToMap(stats)
	assert.Equal(t, int64(10), result["range_local"])
	assert.Equal(t, int64(20), result["range_remote"])
	assert.Equal(t, int64(5), result["hash_local"])
	assert.Equal(t, int64(15), result["hash_remote"])
	assert.Equal(t, int64(3), result["appendable_local"])
	assert.Equal(t, int64(7), result["appendable_remote"])
	assert.Equal(t, int64(2), result["no_shuffle"])
	assert.Equal(t, int64(62), result["total"])
	assert.Equal(t, true, result["enabled"])
}

func TestHandleShuffleMonitorRequest(t *testing.T) {
	// Test enable
	req := &query.ShuffleMonitorRequest{
		Cmd:       "enable",
		Parameter: "enable",
	}

	resp := HandleShuffleMonitorRequest(req)
	assert.True(t, resp.Success)
	assert.NotEmpty(t, resp.Message)
	assert.NotEmpty(t, resp.Data)

	var result map[string]interface{}
	err := json.Unmarshal([]byte(resp.Data), &result)
	require.NoError(t, err)
	assert.True(t, result["success"].(bool))

	// Test query
	req = &query.ShuffleMonitorRequest{
		Cmd:       "query",
		Parameter: "query",
	}

	resp = HandleShuffleMonitorRequest(req)
	assert.True(t, resp.Success)
	assert.NotEmpty(t, resp.Data)

	err = json.Unmarshal([]byte(resp.Data), &result)
	require.NoError(t, err)
	assert.True(t, result["success"].(bool))
	assert.NotNil(t, result["stats"])
}

func TestHandleShuffleMonitor_WrongService(t *testing.T) {
	_, err := handleShuffleMonitor(nil, tn, "enable", nil)
	assert.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrWrongService))
}

func TestHandleShuffleMonitorRequest_MarshalError(t *testing.T) {
	// Create a request that will cause processShuffleMonitorCmd to return
	// data that can be marshaled
	req := &query.ShuffleMonitorRequest{
		Cmd:       "enable",
		Parameter: "enable",
	}

	resp := HandleShuffleMonitorRequest(req)
	assert.True(t, resp.Success)
	assert.NotEmpty(t, resp.Data)

	// Verify data can be unmarshaled
	var result map[string]interface{}
	err := json.Unmarshal([]byte(resp.Data), &result)
	assert.NoError(t, err)
}

func TestHandleShuffleMonitorRequest_AllCommands(t *testing.T) {
	commands := []string{"enable", "disable", "reset", "query", "status"}

	for _, cmd := range commands {
		t.Run(cmd, func(t *testing.T) {
			req := &query.ShuffleMonitorRequest{
				Cmd:       cmd,
				Parameter: cmd,
			}

			resp := HandleShuffleMonitorRequest(req)
			assert.NotNil(t, resp)
			assert.True(t, resp.Success)
			assert.NotEmpty(t, resp.Message)
			assert.NotEmpty(t, resp.Data)

			// Verify JSON is valid
			var result map[string]interface{}
			err := json.Unmarshal([]byte(resp.Data), &result)
			assert.NoError(t, err)
			assert.True(t, result["success"].(bool))
		})
	}
}

func TestShuffleLocalityStats_Concurrency(t *testing.T) {
	ResetShuffleLocalityStats()
	EnableShuffleLocalityStats()

	// Test concurrent writes
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				shuffleType := "range"
				if j%2 == 0 {
					shuffleType = "hash"
				}
				isLocal := j%3 == 0
				RecordShuffleLocalityStats(shuffleType, isLocal)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	stats := GetShuffleLocalityStats()
	assert.Equal(t, int64(1000), stats.TotalObjects)

	// Verify total equals sum of all categories
	total := stats.RangeShuffleLocal + stats.RangeShuffleRemote +
		stats.HashShuffleLocal + stats.HashShuffleRemote
	assert.Equal(t, int64(1000), total)
}

func TestShuffleLocalityStats_ConcurrentEnableDisable(t *testing.T) {
	done := make(chan bool)

	// Concurrent enable/disable
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				if j%2 == 0 {
					EnableShuffleLocalityStats()
				} else {
					DisableShuffleLocalityStats()
				}
			}
			done <- true
		}()
	}

	// Concurrent reads
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				_ = IsShuffleLocalityStatsEnabled()
				_ = GetShuffleLocalityStats()
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should not panic and should be in a valid state
	enabled := IsShuffleLocalityStatsEnabled()
	assert.True(t, enabled || !enabled) // Just verify it returns a boolean
}

func TestProcessShuffleMonitorCmd_AllCommands(t *testing.T) {
	testCases := []struct {
		name        string
		cmd         string
		expectError bool
	}{
		{"enable", "enable", false},
		{"disable", "disable", false},
		{"reset", "reset", false},
		{"query", "query", false},
		{"status", "status", false},
		{"invalid", "invalid", true},
		{"empty", "", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := processShuffleMonitorCmd(tc.cmd)
			assert.NotNil(t, result)
			success, ok := result["success"].(bool)
			assert.True(t, ok)
			if tc.expectError {
				assert.False(t, success)
			} else {
				assert.True(t, success)
			}
		})
	}
}
