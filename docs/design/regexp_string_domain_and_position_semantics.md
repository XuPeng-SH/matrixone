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
- a statically known binary regexp operand cannot be combined with a text
  operand and is rejected with MySQL error 3995;
- a direct parameter marker defers its domain at PREPARE and retains MySQL's
  `PARAM_ITEM` provenance at EXECUTE: a binary marker does not itself trigger
  3995, but a text marker remains incompatible with a statically binary peer;
- a parameter nested in a domain-preserving expression is no longer a direct
  marker. Its current result domain is checked normally at EXECUTE;
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

Compatibility checking keeps provenance separate from that current domain:

| Check mode | Compatibility meaning |
|---|---|
| known | current text participates; current binary is a binary trigger |
| deferred | PREPARE-time runtime-owned domain is omitted until EXECUTE |
| direct marker | current text participates; current binary is compatible but does not trigger 3995 |
| domainless | a bare untyped NULL contributes no domain |

The modes are not a second type system. Ordinary argument types remain the
source of overload selection, result metadata, and byte/text execution mode.
They answer only whether that current domain may trigger the regexp charset
restriction.

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
At EXECUTE, the four-mode table is applied at every nested regexp boundary; a
nested function must not accidentally re-run the ordinary static checker and
erase the direct-marker exception of its own children. A known binary operand
is incompatible with any participating text operand. A binary direct marker is
not a trigger, so two differently typed direct markers are legal and select
binary execution if either current value is binary. This preserves early 3995
errors without rejecting valid `PARAM_ITEM` combinations.

The defer mode is PREPARE-only: execute-time rebinding supplies an explicit
resolved mode vector, so cached ParamRefs cannot defer a second time after the
current values and domains are known. A bare NULL literal contributes no
domain. A prepared marker whose current value is NULL retains its PREPARE-time
domain, matching MySQL's parameter contract instead of silently bypassing a
text/binary mismatch. A NULL-valued function result likewise retains its
declared domain; it is not equivalent to a bare NULL literal. A failed
execution leaves the cached plan unchanged and reusable.

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

MySQL has one additional INSTR-only boundary: for an empty subject, every
positive `pos` is accepted. A zero-width anchor match is reported at that
requested position, while a consuming pattern or a later occurrence returns
zero. SUBSTR and REPLACE retain their ordinary `pos <= length` validation.

A present pattern is validated before a later NULL or range shortcut can
determine the result. This includes both the special empty-pattern error 3685
and ordinary malformed-pattern compilation failures for every predicate,
INSTR, SUBSTR, and REPLACE arity. A present REGEXP_LIKE `match_type` is likewise
validated before a NULL subject or pattern. REGEXP_LIKE still returns NULL
first when `match_type` itself is NULL, matching MySQL's argument semantics.

## Data flow and ownership

1. The binder records deferred regexp operands at PREPARE and reconstructs the
   known/direct-marker/domainless modes at every function boundary at EXECUTE.
2. Execute-time rebinding replaces parameters in a copied plan. Materialized
   parameters retain their execute-time binary type even when a different
   sibling expression triggered specialization, so `ORD(?)` and other string
   consumers cannot fall back to text semantics. The cached prepared plan
   remains immutable.
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

### Prepared `BIT_COUNT` type evolution

`BIT_COUNT(?)` has a separate asymmetric contract. An unresolved marker begins
in the binary-string overload. The first numeric value reparses that marker and
records MySQL's canonical parameter category: signed or unsigned `LONGLONG`,
`DOUBLE`, or `DECIMAL(65,30)`. Later text or BLOB values use that category until
a newer numeric execution changes it. The source's physical width and decimal
precision are deliberately not retained, because they must not constrain a
later string after reprepare. NULL neither requires a specialized plan for its
own execution nor clears the recorded type. Multiple markers evolve independently.

The state is owned by `PrepareStmt`, bounded by the statement parameter count,
and cleared when metadata rebuilds the prepared-plan generation. It is not part
of the reusable plan: execute-time rebinding still copies the immutable prepared
plan and restores typed runtime literals to parameter references before caching
the specialized compile. Explicit casts remain fixed type boundaries and do not
participate in this evolution.

## Binary matching representation

Go RE2 has no byte-mode switch. Binary execution maps each non-ASCII byte to a
distinct private-use rune while preserving ASCII regexp grammar. Numeric byte
escapes are mapped to the same alphabet. Unicode properties and code-point
escapes above `0xff` are rejected in binary mode so callers cannot address the
internal alphabet.

Binary operands establish a case-sensitive default, but explicit `i` and `c`
flags override it and the rightmost flag wins. ASCII bytes can therefore fold
under `i`; encoded high bytes remain distinct private-use runes and are not
subject to Unicode case folding.

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
- the uniform-domain predicate path stays vectorized and allocation-free when
  the subject has no NULLs; batches containing NULL subjects use the row-aware
  path so an empty non-NULL pattern still raises 3685.

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
| nested dynamic result domain | binder accepts correlated runtime domains, propagates binary from every semantic operand, and rejects fixed controls | SQL PREPARE and COM_STMT binary/text/binary reuse |
| static mismatch | function resolver returns 3995 for known mixed operands | existing BVT matrix |
| execute-time marker semantics | exhaustive known/deferred/direct-marker/domainless matrix | mixed direct markers, fixed-binary controls, nested-result controls, and cached-plan reuse |
| byte positions/results | binary vectors without legacy `SetIsBin` | BINARY/VARBINARY/BLOB BVT |
| binary match flags | ASCII `i`, rightmost `ci`/`ic`, and non-ASCII byte controls | REGEXP_LIKE BVT compared with MySQL 8.4 |
| pattern and match-type precedence | empty and malformed patterns across every arity and later NULL boundary; invalid/present/NULL match types | public SQL for LIKE/INSTR/SUBSTR/REPLACE |
| empty INSTR subject | arbitrary positive position, anchor/no-match/second-occurrence and binary controls | public SQL at `pos = 5` |
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
