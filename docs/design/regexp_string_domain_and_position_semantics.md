# REGEXP string-domain and position semantics

Status: implementation review

Owning issues: #25299, #27217, #28202, #28207, #28219

Implementation: #28267

## Problem and compatibility boundary

MatrixOne uses Go RE2 while MySQL 8.4 uses ICU. Full regexp grammar
compatibility is therefore not a goal of this change. The required contract is
limited to behavior MatrixOne can implement independently of the regexp engine:

- SQL text values use UTF-8 code-point positions;
- BINARY, VARBINARY, BLOB, and runtime binary parameters use byte positions;
- statically known text and binary regexp operands are rejected with MySQL
  error 3995;
- a parameter marker owns its string domain at EXECUTE time, including when it
  is nested in a domain-preserving expression;
- `ORD` interprets its input in the selected row's effective string domain;
  `REGEXP_SUBSTR` and `REGEXP_REPLACE` also preserve that domain on their
  string result;
- `pos` does not have one common anchor contract across all regexp functions.

The last two items cross planner, prepared-statement, vector, and execution
boundaries. A prepared plan may describe a parameter as TEXT while a later
execution supplies a BLOB. Static type alone is therefore not sufficient
evidence of the runtime string domain.

## Invariants

### String-domain lattice

Every string expression has one of these planner-visible domain sets:

| Domain set | Meaning |
|---|---|
| `{text}` | statically text for every execution |
| `{binary}` | statically binary for every execution |
| `{text,binary}` | owned by the current prepared execution |

An explicit cast is a domain boundary and always produces a singleton set.
Untyped parameter markers and user variables can produce `{text,binary}`.
Functions with fixed textual output, such as `HEX`, collapse their input to
`{text}`. Domain-preserving functions propagate their input set according to
their documented return-type rule.

`REGEXP_SUBSTR` and `REGEXP_REPLACE` are special only in that their executor
already derives the result domain from all semantic string operands. Their
planner transfer function must express the same rule:

- if a statically binary operand fixes execution to binary, the only possible
  runtime result is binary;
- otherwise a runtime-owned semantic operand makes both text and binary
  possible;
- the prepared result type remains the ordinary overload result, with runtime
  provenance carrying a differing row domain.

The analysis is compositional. It must not enumerate the Cartesian product of
parameter values or infer a nested function's domain by changing only one child
while holding incompatible siblings fixed.

Static regexp validation compares every singleton operand. Runtime-owned
operands are deferred, but known operands are still compared with one another.
This preserves early 3995 errors without rejecting a statement whose legal
domain is determined only at EXECUTE time.

### Position and anchor policies

`pos` is one-based in SQL. Text positions count UTF-8 code points and binary
positions count bytes. After conversion to a byte offset, each function applies
its own search policy:

| Function | Matcher subject | Meaning of `^` and word boundary at `pos > 1` |
|---|---|---|
| `REGEXP_INSTR` | suffix beginning at `pos` | relative to the suffix |
| `REGEXP_SUBSTR` | complete original subject | relative to the original subject |
| `REGEXP_REPLACE` | complete original subject | relative to the original subject |

This follows MySQL 8.4's `Regexp_facade`: `Find`, used by INSTR, resets the
matcher with the suffix, whereas SUBSTR and REPLACE reset with the complete
subject and pass a start position to the matcher.

Occurrences after the first remain relative to the same matcher subject. ICU
retains an empty match at the end of a preceding nonempty match; Go's `FindAll`
suppresses it. The iterator therefore emits every selected empty match and then
advances by exactly one text code point / binary byte. This preserves the ICU
sequence while guaranteeing termination. `REGEXP_REPLACE` keeps MySQL's
function-level exception that an empty subject is returned unchanged.

## Data flow and ownership

1. The binder records which regexp operands have a runtime-owned domain.
2. Execute-time rebinding replaces parameters in a copied plan. The cached
   prepared plan remains immutable.
3. COM_STMT and SQL EXECUTE rebuild parameter metadata on every execution;
   binary-to-text-to-binary reuse must not retain a stale domain.
4. Vectors carry static type plus optional row-level runtime provenance.
5. REGEXP execution selects byte mode if any semantic string operand is
   effectively binary for the current row.
6. SUBSTR and REPLACE attach the selected effective result domain to their
   output vector.

Metadata slices are bounded by statement parameter count and are cleared before
reuse. The prepared statement/session remains their owner. No new goroutine,
wait, retry, or external resource is introduced.

## Binary matching representation

Go RE2 has no byte-mode switch. Binary execution maps each non-ASCII byte to a
distinct private-use rune while preserving ASCII regexp grammar. Numeric byte
escapes are mapped to the same alphabet. Unicode properties and code-point
escapes above `0xff` are rejected in binary mode so callers cannot address the
internal alphabet.

The regexp cache is operator-owned, limited to 100 entries, and clones pattern
keys whose source vector may be reused. Its syntax-derived zero-width metadata
uses the same keys and is evicted atomically with each matcher, so it cannot grow
independently. Per-row encoded subjects and results are lexically scoped and
become unreachable after evaluation.

Cost model:

- ASCII binary subjects remain zero-copy;
- a non-ASCII binary subject is encoded in `O(n)` time with at most three UTF-8
  bytes per source byte;
- positional matching streams occurrences and retains constant match-index
  memory rather than materializing `FindAll` results;
- replace-all patterns that cannot consume zero input units retain RE2's native
  optimized path; only nullable/empty-width syntax uses the ICU-compatible
  iterator;
- INSTR encodes only the suffix starting at `pos`; SUBSTR and REPLACE retain the
  complete subject because their anchor contract requires it;
- the uniform-domain predicate path stays vectorized and allocation-free.

## Alternatives

### Slice every subject at `pos`

This is simple and correct for INSTR, but rebases anchors incorrectly for
SUBSTR and REPLACE. It is rejected as a shared implementation.

### Preserve complete-subject context for every function

This is correct for SUBSTR and REPLACE, but changes INSTR's documented suffix
semantics and does unnecessary prefix work. It is rejected as a shared
implementation.

### Enumerate prepared parameter type combinations

Cartesian enumeration can find correlated domain changes but grows
exponentially with argument count and couples planning cost to statement shape.
Changing one argument at a time is bounded but misses correlated legal states.
Both are rejected. A small domain lattice and function transfer rules express
the actual contract directly.

### Add a second byte-regexp engine

A separate engine could avoid encoding, but adds grammar, dependency, security,
and deployment differences far beyond these compatibility fixes. The bounded
RE2 encoding is retained.

## Validation matrix

| Contract | White-box oracle | Public oracle |
|---|---|---|
| INSTR suffix anchors | direct matcher tests for `^`, multiline `^`, `\\b`, `$` | BVT SQL at `pos > 1` |
| SUBSTR/REPLACE original anchors | start-aware iterator tests | existing BVT positional cases |
| nested dynamic result domain | binder accepts correlated runtime domains and rejects fixed controls | SQL PREPARE and COM_STMT binary/text/binary reuse |
| static mismatch | function resolver returns 3995 for known mixed operands | existing BVT matrix |
| byte positions/results | binary vectors without legacy `SetIsBin` | BINARY/VARBINARY/BLOB BVT |
| zero-width sequence/progress | adjacent nonempty/empty, anchor, boundary, empty-subject, and cache-bound unit tests | SQL results compared with MySQL 8.4 |
| hot-path cost | allocation benchmarks for ASCII, high-byte, and near-end positions | performance evidence, not BVT timing assertions |

The regression tests use minimum strings and no sleeps, retries, ambient state,
or large fixtures. A same-statement binary-to-text-to-binary transition is the
reset/reuse oracle; a fresh statement is the control.

## Rollback and residual compatibility

The change adds no persisted, catalog, or wire field. Rolling back code restores
the former SQL behavior without migration. Mixed-version remote execution uses
the existing prepared-parameter transport; runtime binary provenance remains a
local process concern.

RE2/ICU grammar differences remain explicit non-goals. Errors caused solely by
unsupported ICU constructs are not evidence that the positional or string-domain
contract above failed.
