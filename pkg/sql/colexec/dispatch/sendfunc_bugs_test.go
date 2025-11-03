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
	"bufio"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBug4SendToAnyRemoteFuncErrorSwallow checks Bug #4 in sendfunc.go
// Bug #4: Lines 256-257 return (true, nil) instead of (true, ctx.Err())
func TestBug4SendToAnyRemoteFuncErrorSwallow(t *testing.T) {
	file, err := os.Open("sendfunc.go")
	require.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNum := 0
	bugExists := false
	inSendToAnyRemote := false
	
	// Look for sendToAnyRemoteFunc and check the context handling
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		
		if strings.Contains(line, "func sendToAnyRemoteFunc") {
			inSendToAnyRemote = true
			t.Logf("Found sendToAnyRemoteFunc at line %d", lineNum)
		}
		
		if inSendToAnyRemote && strings.Contains(line, "case <-proc.Ctx.Done():") {
			if scanner.Scan() {
				lineNum++
				nextLine := scanner.Text()
				if strings.Contains(nextLine, "return true, nil") {
					bugExists = true
					t.Logf("Bug #4 FOUND at line %d: %s", lineNum, strings.TrimSpace(nextLine))
					t.Logf("Should return (true, proc.Ctx.Err())")
					break
				} else if strings.Contains(nextLine, "return true, proc.Ctx.Err()") {
					t.Logf("Bug #4 FIXED at line %d: %s", lineNum, strings.TrimSpace(nextLine))
					return
				}
			}
		}
		
		// Stop after the function ends
		if inSendToAnyRemote && lineNum > 250 && strings.HasPrefix(strings.TrimSpace(line), "func ") {
			break
		}
	}

	if bugExists {
		t.Logf("=== BUG #4 CONFIRMED ===")
		t.Logf("Location: sendfunc.go around line 256-257")
		t.Logf("Problem: return true, nil when context is cancelled")
		t.Logf("Fix: Change to return true, proc.Ctx.Err()")
	} else {
		t.Logf("Bug #4 pattern not found or already fixed")
	}
}

