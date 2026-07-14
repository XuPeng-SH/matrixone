# Operator And Pipeline Reference

## 1. Operator Lifecycle Contracts

### Call() vs Reset()

| Method | Purpose | Must NOT Do |
|--------|---------|-------------|
| `Call()` | Process one batch. Receive from upstream, compute, send downstream. | Send terminal signals (`End`, `Error`, `Abort`). That is `Reset()`'s job. |
| `Reset()` | Cleanup. Notify downstream the operator is done. | Block waiting for receiver acknowledgment. Use `Abort()`, not `CloseWithTimeout()`. |

This is the most common source of subtle operator bugs: premature pipeline termination, spurious timeouts, dead receivers.

### PipelineSpool Lifecycle

| Method | Behavior | Use When |
|--------|----------|----------|
| `Close()` | Wait for all consumers to acknowledge end of stream by consuming nil batches from spool | Graceful completion path |
| `CloseWithTimeout()` | Same as `Close()` but with timeout | Legacy cleanup; deprecated for typed signals |
| `ForceCleanup()` / `Abort()` | Synchronous release of all un-consumed slots. No waiting. Idempotent (`sync.Once`) | Cleanup path with explicit terminal signals |

Critical rule: if implicit nil-batch end-of-stream was replaced with explicit typed signals (`EventEnd`, `EventError`, `EventAbort`), use `Abort()` instead of `CloseWithTimeout()`. `CloseWithTimeout()` waits for nil batches that will never arrive.

### Pipeline Signal Types

| Signal | Constructor | Meaning |
|--------|-------------|---------|
| `EventData` | `NewDataSignal(batch)` | Normal data batch |
| `EventEnd` | `NewEndSignal()` | Graceful end of stream |
| `EventError` | `NewErrorSignal(err)` | Operator encountered an error |
| `EventAbort` | `NewAbortSignal()` | Forceful termination |

Dual-protocol compatibility: `GetNextBatch` handles both explicit typed signals and legacy nil-batch convention (`content == nil`). Old operators using implicit nil-batch continue to work.

## 2. Post-Modification Verification

### Test Freshness

Test output must be from the current turn:

1. `go test` command appears in current turn's tool calls.
2. Exit code is 0.
3. Timestamp is after the last edit.

For concurrency/lifecycle regressions, also apply
[the regression-test design gate](../../mo-self-review/references/regression-test-design.md).
Encode observable partial order rather than scheduler timing, assert the complete
terminal state, and keep test cleanup safe on assertion failure.

### Completion Gate

Before declaring a change done, all boxes must be checked:

```
□ go build ./pkg/.../modified_package...    -> exit 0
□ go vet ./pkg/.../modified_package...      -> exit 0
□ go test -v -count=1 ./pkg/.../...         -> exit 0, no hangs
□ git diff --stat                            -> inspected, no unintended files
□ Regression: at least one test from dependent package passes
```

An actual hang is a failure. No-output duration alone is not proof: first identify
whether Go is compiling, CGo is linking, the test binary is starting, or the test
body is blocked. Use timeout stacks and the concrete wait chain to diagnose a test
hang; use a CI-appropriate command timeout only as the final guard.

## 3. Fault Diagnosis

| Symptom | Look At |
|---------|---------|
| Test hangs exactly 30s | `CloseWithTimeout` waiting for nil-batch that never arrives. Check if typed signals are being sent instead. |
| Test body starts, then stops making progress | Check timeout stacks, `done` closure, and blocking sends after excluding compile/link/startup latency. |
| `context deadline exceeded` after 30s | `WaitingEndWithTimeout` timed out. Check whether all senders called `Reset()` and sent typed terminal signals. |
| `CGO_CFLAGS` not working | Run `go env CGO_CFLAGS` to verify. Use `export` if the package has sub-packages. |

## 4. Common Pitfalls

### Structural Changes Need Both Ends

When changing communication protocol between operators, such as connector <-> merge or dispatch <-> merge, update both sender and receiver. A sender-only change can deadlock.

Identification: exact 30s timeout in cleanup usually points to `CloseWithTimeout` + typed signal mismatch.

### CGo Link Errors Are Usually Environment

CGo link errors (`Undefined symbols`, `cannot find -lmo`) are environment issues until proven otherwise. The C shared libraries (`libmo.dylib`, `libusearch_c.dylib`) must be pre-built via `make cgo` and `make thirdparties`.

### Pipeline Cleanup: Abort, Do Not Wait

During pipeline cleanup, operators should use non-blocking or timeout-gated communication. Never block waiting for a receiver that may have already exited.

### Channel Full Edge Cases

When sending terminal signals into a bounded channel, the send may fail because the channel is full. Ensure the terminal state is still recorded even when channel send fails; the `done` channel must close regardless of delivery success.

## 5. Forbidden Patterns

1. Never send terminal signals (`End`, `Error`, `Abort`) from `Call()`. Only `Reset()` sends terminal signals.
2. Never call `sp.CloseWithTimeout()` after switching to explicit typed terminal signals. Use `sp.Abort()`.
3. Never claim "pre-existing" without `git stash` proof.
4. Never declare done without fresh test output.
5. Never assume `go build` success means `go test` will pass.
6. Never skip bottom-up testing.
7. Never add per-algorithm `switch`/`if` on index algo names in the SQL layer. Route through `indexplugin.Get(algo)`.
8. Never use `time.Sleep` or a short timeout to establish concurrent ordering; use observable lifecycle state or synchronization events.
