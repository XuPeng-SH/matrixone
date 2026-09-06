# Ordered `ON DUPLICATE KEY UPDATE` Semantics

Status: In progress

## Problem

`INSERT ... ON DUPLICATE KEY UPDATE` is both an ordered logical program and a
physical table mutation. Collapsing assignments or duplicate input keys loses
observable SQL semantics, while treating every logical action as a physical
write needlessly rewrites base/index data and fires implicit `ON UPDATE`
expressions.

The same statement can also cross planner, DEDUP, distributed pipeline,
`MULTI_UPDATE`, regular and irregular index maintenance, partition/direct/S3
writes, client affected-row reporting, and rolling-upgrade protocol boundaries.
Those consumers need one explicit contract rather than independently inferring
"changed" from the final batch.

## Invariants

1. Assignment expressions execute left-to-right. A repeated target is not
   deduplicated; each RHS observes the row image produced by the prior RHS.
2. Every duplicate input row is a logical action. Constraints that can be
   affected by ODKU are validated for every action before any non-final action
   is discarded.
3. Logical affected rows and physical mutation are independent:
   a changed duplicate contributes 2, a no-op contributes 0 or 1 under
   `CLIENT_FOUND_ROWS`, while storage/index writers receive only the final row
   and only when its physical image changed.
4. A pure no-op restores the stored row image before CHECK/FK/index/RETURNING
   consumers. It must not leak an implicit timestamp or regenerated value.
5. FK validation is eligible only for an inserted row or an action that changed
   that FK tuple. An unrelated update must not revalidate historical orphan
   data created while `foreign_key_checks=0`.
6. Value-change detection follows the target SQL type: NULL-aware, CHAR PAD
   SPACE, JSON structural comparison, declared FLOAT scale, signed zero and NaN
   peers, and element-wise vector semantics. The hot path performs no
   `interface{}` conversion or heap allocation.
7. Malformed/missing metadata fails closed. A mixed-version CN that cannot
   interpret the action/count/physical markers must not execute the plan.

## Plan and execution model

The planner retains an ordered `(target-column, expression)` stream. DEDUP
replays it against a stable row image and emits:

- the materialized current row;
- an accumulated affected-row weight;
- a physical-change marker for the final image;
- when action validation is required, an action-final marker and constraint
  eligibility markers.

Action validation is enabled only when a changed target can affect CHECK, FK,
or NOT NULL semantics. CHECK/FK/NOT NULL assertions are barriered before the
action-final filter. The filter then reduces each key group to its final row.
`MULTI_UPDATE` consumes the count marker independently and applies the physical
marker uniformly to base, regular-index, irregular-index, partition, direct,
and S3 writers.

Self-referencing FKs retain their existing statement-level post-write check in
this change. Their parent domain can include rows created by the same statement,
so moving them into the row-scoped parent scan requires a separately specified
statement-local parent-key source; treating only the pre-statement table as the
parent domain would reject valid self-reference inserts. The ordered action
work must not claim row-scoped self-FK semantics until that source exists.

## Ownership and unhappy paths

- DEDUP owns its stable expression-result vectors until `Free`; swapping a
  logical row image transfers vector references but never drops the owned pool.
- A compiled materialized source is closed once per pipeline attempt by the
  attempt owner. A receiver-less SINK rejected during compilation releases only
  its newly compiled producer scopes; it does not assume ownership of a source
  registered elsewhere.
- Errors from action validation abort the statement before physical writes are
  committed. The final-action barrier cannot suppress a prior action error.
- ALTER/DROP INDEX owns hidden child-relation deletion under the parent DDL
  lifecycle. It must not recursively enter SQL DDL and acquire child metadata
  locks in the inverse order of concurrent DML.
- All action buffers are bounded by the input statement/bucket. No goroutine,
  retry loop, or unbounded retained history is introduced.

## Performance model

Tables whose ODKU targets cannot affect an action-level constraint retain the
one-row-per-key-group fast path. Constraint-bearing statements pay for action
rows because discarding them would be incorrect. Fixed scalar comparisons use
typed vector reads with zero allocation; varlen values compare their existing
bytes, and JSON decoding is limited to JSON columns.

Required performance evidence covers no-conflict, distinct conflict, hot-key,
pure no-op, CHECK/FK/NOT NULL action validation, and a wide varlen row. Compare
the same binary/mode/data on the same machine and report medians rather than an
individual run.

## Validation matrix

| Contract | White-box proof | SQL-visible proof |
|---|---|---|
| ordered and repeated assignments | DEDUP replay tests | dependent/repeated SET cases |
| action validation precedes final filtering | plan barrier/metadata tests | invalid-then-valid CHECK/FK/NOT NULL rollback |
| no-op does not write | change/count marker tests | ROW_COUNT, timestamp, base and forced-index state |
| type-aware equality | scalar/vector comparator tests and allocation oracle | CHAR/JSON/scaled FLOAT no-op cases |
| distributed compatibility | encode/decode and version-fence tests | mixed-version rejection coverage |
| DDL child ownership | deterministic barrier/fake-engine test | concurrent DML versus ALTER DROP INDEX harness |

Every failure case also checks durable table/index state after rollback. Tests
use barriers or direct typed state; sleeps and probabilistic retries are not
correctness oracles.
