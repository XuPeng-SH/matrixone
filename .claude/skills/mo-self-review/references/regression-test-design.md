# Regression Test Design Reference

Use this reference for correctness, concurrency, lifecycle, cancellation, restart,
and cleanup regressions. The goal is to preserve the contract under realistic
interleavings without coupling the test to one implementation shape.

## Contents

- [1. Start From The Contract](#1-start-from-the-contract)
- [2. Build A Deterministic Partial Order](#2-build-a-deterministic-partial-order)
- [3. Resist Overfitting](#3-resist-overfitting)
- [4. Close The State Machine](#4-close-the-state-machine)
- [5. Make Test Failure Safe](#5-make-test-failure-safe)
- [6. Separate Correctness From Performance](#6-separate-correctness-from-performance)
- [7. Validate In Layers](#7-validate-in-layers)

## 1. Start From The Contract

Write this design record before writing test mechanics:

```text
Invariant:
Old concrete failure:
Observable pause point:
Required partial order:
Release event:
Terminal assertions:
Opposite ordering:
Generation or reuse assertions:
CI timeout role:
```

Name the state transition or ownership invariant, not the line that happened to be
wrong. A good regression remains valid after an equivalent refactor.

## 2. Build A Deterministic Partial Order

Encode the minimum causal order needed to expose the bug:

```text
A reaches an observable lifecycle state
  -> the test confirms that state
  -> B closes, cancels, aborts, or restarts
  -> the test releases A
  -> both operations reach terminal states
```

Use channels, barriers, existing pause facilities, observable state, or real
state-machine stages. Do not use `time.Sleep` to guess that A probably ran first.
A timeout may stop a broken test from hanging CI, but it must not establish order.

Prefer the least invasive observation mechanism:

1. Public behavior or exported state.
2. Real internal lifecycle transitions already used by production.
3. An existing dependency-injection or fault-injection seam.
4. A new seam only when it is also a legitimate production design boundary and
   the decision is recorded; never add a production hook solely for one test.

## 3. Resist Overfitting

Ask before accepting the test:

- Would a different correct implementation pass?
- Does the test assert the contract, or an incidental line, lock, goroutine, or
  container shape?
- Did production code change to improve the design, or merely to make the test
  schedulable?
- Is an internal field asserted because it represents ownership/terminal state,
  or because it is convenient to inspect?
- Does the test cover the bug class, or only the first reported interleaving?

Do not turn useful techniques into universal constants. Avoid fixed sleep
durations, repetition counts, timeout values, and internal-field checklists. Scale
stress and timeout budgets to determinism, package cost, and CI conditions.

## 4. Close The State Machine

For two competing operations, test both meaningful linearization orders:

| Order | Required proof |
|-------|----------------|
| A becomes terminal before B starts | B observes the terminal state and does not recreate ownership |
| B starts while A is in flight | A cannot publish or retain ownership after B becomes terminal |

For restartable or reusable objects, also cross the generation boundary:

- Old-generation flags, tokens, waiters, quota, and resources do not leak forward.
- The new generation starts from its documented initial state.
- A late old-generation completion cannot mutate the new generation.
- A subsequent valid operation succeeds, proving capacity and gates were restored.

Assert all applicable terminal evidence: returned result, closed/aborted state,
ownership maps and counts, quota, wait queues, helper goroutine exit, and successful
reuse. Avoid asserting unrelated representation details.

## 5. Make Test Failure Safe

The regression test must not introduce its own Q1/Q2/Q3 failure:

- Arrange unlock/cleanup immediately after acquiring a resource. If the test must
  hold a lock across a phase, snapshot state, release the lock, then assert.
- Do not call `require`/`Fatal` while holding a lock or while cleanup depends on the
  current goroutine continuing.
- Give every helper goroutine a terminal path and collect its result.
- Use bounded result receives. Keep the timeout generous and report which phase
  failed so a slow CI machine is distinguishable from a broken causal chain.
- Avoid `Eventually` polling when an event can be observed directly. When polling
  is unavoidable, poll a contract state and keep the overall wait bounded.

## 6. Separate Correctness From Performance

Functional tests prove state and ownership invariants, not scheduler speed. Remove
upper-bound assertions such as "operation completes within a small duration" unless
the requirement is an explicit SLO and the test runs in a calibrated environment.

Classify silence before calling a test hung: Go may be compiling, CGo may be
linking, the test binary may be starting, or the test body may be blocked. Use the
process phase, final exit status, Go timeout stack, and wait-chain evidence. Silence
alone is not a verdict.

## 7. Validate In Layers

Run validation after the final edit, in this order where applicable:

1. Prove the new regression fails against the old implementation, or record why a
   safe red-before check is impractical.
2. Run the focused regression once.
3. Repeat the focused regression enough to expose scheduling dependence; repetition
   is a flake detector, not proof of determinism.
4. Run the focused package with `-race` when concurrency changed.
5. Run the full modified package, then build/vet checks and a dependent package.
6. Use the required CGo environment for packages whose test binary links CGo.

Do not mix unrelated baseline failures or generated dependency changes into the
fix. Prove any "pre-existing" failure against a clean baseline with the exact same
command and environment.
