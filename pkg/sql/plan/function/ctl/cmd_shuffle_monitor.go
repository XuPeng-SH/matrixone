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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ShuffleLocalityStats is used to track object shuffle locality statistics
type ShuffleLocalityStats struct {
	mu sync.RWMutex
	// Statistics categorized by shuffle type
	RangeShuffleLocal  int64 // Number of local objects for range shuffle
	RangeShuffleRemote int64 // Number of remote objects for range shuffle
	HashShuffleLocal   int64 // Number of local objects for hash shuffle
	HashShuffleRemote  int64 // Number of remote objects for hash shuffle
	AppendableLocal    int64 // Number of local appendable objects
	AppendableRemote   int64 // Number of remote appendable objects
	NoShuffleObjects   int64 // Number of objects that don't need shuffle
	TotalObjects       int64 // Total number of objects
	Enabled            bool  // Whether statistics collection is enabled
}

var (
	globalShuffleLocalityStats = &ShuffleLocalityStats{}
)

// RecordShuffleLocalityStats records shuffle locality statistics
// This function is called from ShouldSkipObjByShuffle
func RecordShuffleLocalityStats(shuffleType string, isLocal bool) {
	if !globalShuffleLocalityStats.Enabled {
		return
	}

	globalShuffleLocalityStats.mu.Lock()
	defer globalShuffleLocalityStats.mu.Unlock()

	globalShuffleLocalityStats.TotalObjects++

	switch shuffleType {
	case "range":
		if isLocal {
			globalShuffleLocalityStats.RangeShuffleLocal++
		} else {
			globalShuffleLocalityStats.RangeShuffleRemote++
		}
	case "hash":
		if isLocal {
			globalShuffleLocalityStats.HashShuffleLocal++
		} else {
			globalShuffleLocalityStats.HashShuffleRemote++
		}
	case "appendable":
		if isLocal {
			globalShuffleLocalityStats.AppendableLocal++
		} else {
			globalShuffleLocalityStats.AppendableRemote++
		}
	case "none":
		globalShuffleLocalityStats.NoShuffleObjects++
	}
}

// GetShuffleLocalityStats returns current statistics
func GetShuffleLocalityStats() *ShuffleLocalityStats {
	globalShuffleLocalityStats.mu.RLock()
	defer globalShuffleLocalityStats.mu.RUnlock()

	return &ShuffleLocalityStats{
		RangeShuffleLocal:  globalShuffleLocalityStats.RangeShuffleLocal,
		RangeShuffleRemote: globalShuffleLocalityStats.RangeShuffleRemote,
		HashShuffleLocal:   globalShuffleLocalityStats.HashShuffleLocal,
		HashShuffleRemote:  globalShuffleLocalityStats.HashShuffleRemote,
		AppendableLocal:    globalShuffleLocalityStats.AppendableLocal,
		AppendableRemote:   globalShuffleLocalityStats.AppendableRemote,
		NoShuffleObjects:   globalShuffleLocalityStats.NoShuffleObjects,
		TotalObjects:       globalShuffleLocalityStats.TotalObjects,
		Enabled:            globalShuffleLocalityStats.Enabled,
	}
}

// ResetShuffleLocalityStats resets statistics
func ResetShuffleLocalityStats() {
	globalShuffleLocalityStats.mu.Lock()
	defer globalShuffleLocalityStats.mu.Unlock()

	globalShuffleLocalityStats.RangeShuffleLocal = 0
	globalShuffleLocalityStats.RangeShuffleRemote = 0
	globalShuffleLocalityStats.HashShuffleLocal = 0
	globalShuffleLocalityStats.HashShuffleRemote = 0
	globalShuffleLocalityStats.AppendableLocal = 0
	globalShuffleLocalityStats.AppendableRemote = 0
	globalShuffleLocalityStats.NoShuffleObjects = 0
	globalShuffleLocalityStats.TotalObjects = 0
}

// EnableShuffleLocalityStats enables statistics collection
func EnableShuffleLocalityStats() {
	globalShuffleLocalityStats.mu.Lock()
	defer globalShuffleLocalityStats.mu.Unlock()
	globalShuffleLocalityStats.Enabled = true
}

// DisableShuffleLocalityStats disables statistics collection
func DisableShuffleLocalityStats() {
	globalShuffleLocalityStats.mu.Lock()
	defer globalShuffleLocalityStats.mu.Unlock()
	globalShuffleLocalityStats.Enabled = false
}

// IsShuffleLocalityStatsEnabled checks if statistics collection is enabled
func IsShuffleLocalityStatsEnabled() bool {
	globalShuffleLocalityStats.mu.RLock()
	defer globalShuffleLocalityStats.mu.RUnlock()
	return globalShuffleLocalityStats.Enabled
}

// handleShuffleMonitor handles shuffle monitoring commands
// Command format:
//
//	mo_ctl("cn", "shuffle_monitor", "enable")  - Enable statistics for all CNs
//	mo_ctl("cn", "shuffle_monitor", "disable") - Disable statistics for all CNs
//	mo_ctl("cn", "shuffle_monitor", "reset")   - Reset statistics for all CNs
//	mo_ctl("cn", "shuffle_monitor", "query")   - Query statistics from all CNs
func handleShuffleMonitor(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender,
) (Result, error) {
	if service != cn {
		return Result{}, moerr.NewWrongServiceNoCtx("only cn supported", string(service))
	}

	parameter = strings.ToLower(strings.TrimSpace(parameter))
	cmd := parameter

	// Get all CN nodes
	cns := make([]string, 0)
	clusterservice.GetMOCluster(proc.GetService()).GetCNService(
		clusterservice.Selector{},
		func(cn metadata.CNService) bool {
			cns = append(cns, cn.ServiceID)
			return true
		},
	)

	info := make(map[string]interface{})

	for idx := range cns {
		cnID := cns[idx]

		// Process directly on current CN
		if cnID == proc.GetQueryClient().ServiceID() {
			result := processShuffleMonitorCmd(cmd)
			info[cnID] = result
		} else {
			// Process on other CNs via RPC
			request := proc.GetQueryClient().NewRequest(query.CmdMethod_ShuffleMonitor)
			request.ShuffleMonitorRequest = &query.ShuffleMonitorRequest{
				Cmd:       cmd,
				Parameter: parameter,
			}

			_, cancel := context.WithTimeoutCause(
				context.Background(),
				time.Second*5,
				moerr.CauseTransferRequest2OtherCNs,
			)
			defer cancel()

			resp, err := TransferRequest2OtherCNs(proc, cnID, request)
			if resp == nil || err != nil {
				info[cnID] = map[string]interface{}{
					"success": false,
					"message": fmt.Sprintf("transfer failed: %v", err),
				}
			} else if resp.ShuffleMonitorResponse != nil {
				var respData map[string]interface{}
				if err := json.Unmarshal([]byte(resp.ShuffleMonitorResponse.Data), &respData); err == nil {
					info[cnID] = respData
				} else {
					info[cnID] = map[string]interface{}{
						"success": resp.ShuffleMonitorResponse.Success,
						"message": resp.ShuffleMonitorResponse.Message,
						"data":    resp.ShuffleMonitorResponse.Data,
					}
				}
			}
		}
	}

	return Result{
		Method: ShuffleMonitorMethod,
		Data:   info,
	}, nil
}

// processShuffleMonitorCmd processes shuffle monitor command on current CN
func processShuffleMonitorCmd(cmd string) map[string]interface{} {
	result := make(map[string]interface{})

	switch cmd {
	case "enable":
		EnableShuffleLocalityStats()
		result["success"] = true
		result["message"] = "shuffle stats enabled"
		result["enabled"] = true

	case "disable":
		DisableShuffleLocalityStats()
		result["success"] = true
		result["message"] = "shuffle stats disabled"
		result["enabled"] = false

	case "reset":
		ResetShuffleLocalityStats()
		result["success"] = true
		result["message"] = "shuffle stats reset"
		stats := GetShuffleLocalityStats()
		result["stats"] = convertStatsToMap(stats)

	case "query":
		stats := GetShuffleLocalityStats()
		result["success"] = true
		result["message"] = "query success"
		result["stats"] = convertStatsToMap(stats)

		// Calculate locality ratios
		if stats.TotalObjects > 0 {
			localityInfo := make(map[string]interface{})

			// Range shuffle locality
			rangeTotal := stats.RangeShuffleLocal + stats.RangeShuffleRemote
			if rangeTotal > 0 {
				localityInfo["range_locality_rate"] = float64(stats.RangeShuffleLocal) / float64(rangeTotal)
			}

			// Hash shuffle locality
			hashTotal := stats.HashShuffleLocal + stats.HashShuffleRemote
			if hashTotal > 0 {
				localityInfo["hash_locality_rate"] = float64(stats.HashShuffleLocal) / float64(hashTotal)
			}

			// Overall locality
			totalLocal := stats.RangeShuffleLocal + stats.HashShuffleLocal + stats.AppendableLocal
			totalRemote := stats.RangeShuffleRemote + stats.HashShuffleRemote + stats.AppendableRemote
			totalShuffled := totalLocal + totalRemote
			if totalShuffled > 0 {
				localityInfo["overall_locality_rate"] = float64(totalLocal) / float64(totalShuffled)
			}

			result["locality_info"] = localityInfo
		}

	default:
		result["success"] = false
		result["message"] = fmt.Sprintf("unknown command: %s, supported: enable, disable, reset, query", cmd)
	}

	return result
}

// convertStatsToMap converts statistics to map format
func convertStatsToMap(stats *ShuffleLocalityStats) map[string]interface{} {
	return map[string]interface{}{
		"range_local":       stats.RangeShuffleLocal,
		"range_remote":      stats.RangeShuffleRemote,
		"hash_local":        stats.HashShuffleLocal,
		"hash_remote":       stats.HashShuffleRemote,
		"appendable_local":  stats.AppendableLocal,
		"appendable_remote": stats.AppendableRemote,
		"no_shuffle":        stats.NoShuffleObjects,
		"total":             stats.TotalObjects,
		"enabled":           stats.Enabled,
	}
}

// HandleShuffleMonitorRequest handles ShuffleMonitor requests from other CNs
// This function is called in CN's query service
func HandleShuffleMonitorRequest(req *query.ShuffleMonitorRequest) *query.ShuffleMonitorResponse {
	result := processShuffleMonitorCmd(req.Cmd)

	data, err := json.Marshal(result)
	if err != nil {
		return &query.ShuffleMonitorResponse{
			Success: false,
			Message: fmt.Sprintf("marshal result failed: %v", err),
			Data:    "",
		}
	}

	success, _ := result["success"].(bool)
	message, _ := result["message"].(string)

	return &query.ShuffleMonitorResponse{
		Success: success,
		Message: message,
		Data:    string(data),
	}
}
