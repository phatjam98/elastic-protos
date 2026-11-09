# 0007. Zero-downtime reindex: copy, catch up, swap, freeze, reconcile

- Status: Accepted
- Date: 2026-11-09

## Context

A breaking mapping change is applied by copying the index behind an alias into a new index and
swapping the alias. Until now the migrator write-blocked the old index for the whole copy, so every
write through the alias failed for minutes on a large index. Applications that bootstrap at startup
saw that window on every breaking deploy.

The engines give us these building blocks, on Elasticsearch 8.x/9.x and OpenSearch 2.x/3.x alike:

- `_reindex` with `version_type: external` keeps the source versions and skips a document whose
  destination version is equal or higher, so a second pass copies only what changed since the
  first. It compares version numbers, not time: once the destination takes its own writes, a source
  document with a higher version number can overwrite a newer write.
- Alias updates are atomic.
- A point in time (PIT) is a consistent snapshot that can be searched with `version` and
  `seq_no_primary_term`, by `ids` and paged with `search_after`.
- Index and delete accept `if_seq_no`/`if_primary_term`: the write fails with 409 when the document
  changed (or vanished) since that sequence number.
- `PUT _mapping` replaces `_meta` as a whole when the request carries one, and leaves it alone
  otherwise (verified on ES 8.19/9.5 and OpenSearch 2.19/3.9).

## Decision

`ReindexMode.ZERO_DOWNTIME` is the default; `ReindexMode.BLOCKING` keeps the previous behaviour.

1. Create (or reuse) the new index and mark it `_meta.elastic_protos.pending = {action: reindex,
   from, phase: copying}`.
2. Copy with external versioning, then
3. copy again (catch-up).
4. Refresh the new index, open a PIT over it (the snapshot) and record its id in the marker.
5. Swap the alias.
6. After a grace period, write-block the old index (the writes that resolved the alias just before
   the swap land first).
7. Reconcile: for every document of the frozen old index, look it up in the snapshot. Missing:
   `create` it. Different version: write it with the snapshot's `if_seq_no`/`if_primary_term`.
   A 409 means a write through the alias after the swap won, and it stays.
8. Sweep: delete, on the snapshot's sequence number, every snapshot document the old index no
   longer has.
9. Remove the marker; retain or delete the old index.

A migration script also applies in step 7, through a staging index, and must not change `_id`.
Recovery reads only the alias and the marker: before the swap the run restarts at step 2 (external
versioning makes that idempotent); after it, at step 6, never running `_reindex` again (conditional
writes make steps 7 and 8 idempotent); without the PIT, step 7 creates missing documents only and
reports the rest as warnings.

Additions that existing documents were never indexed into (`unindexedAdditions`) are no longer a
reindex: `BACKFILL` puts the mapping and runs `_update_by_query` in place, under a `pending =
{action: backfill}` marker.

A rollback runs the same procedure from the new index back into the old one (`REVERSE_DELTA`),
comparing sources instead of versions, since versions are not comparable in that direction.

## Alternatives rejected

- **Dual alias with `is_write_index`** (alias on both indices, writes to the new one): reads through
  the alias would return every document twice until the old index left the alias, and a partial
  `_update` of a document not yet copied fails with 404 on the write index.
- **A short write block for the delta only** (copy live, then block, copy the delta, swap): writes
  still fail during the delta copy, which is O(changes) and on a busy index O(N) in practice; the
  failure window shrinks but does not go away.
- **A plain external-version delta after the swap** (`_reindex` old to new with `version_type:
  external` once the alias moved): version numbers do not order writes across two indices. A
  document updated twice in the old index after the copy has a higher version than the same
  document updated once in the new index after the swap, so the delta would overwrite the newer
  write. Conditional writes against a snapshot taken before the swap order them correctly.

## Consequences

- Writes through the alias never fail during a reindex, except a straggler that resolved the alias
  before the swap and arrives after the grace period: it gets a retryable `BLOCKED` error rather
  than landing in an index nobody reads.
- Reads after the swap can miss writes made to the old index after the catch-up pass until the
  reconcile reaches them; a partial `_update` in that window applies to the stale copy. Last writer
  wins per document.
- A reindex costs two copy passes, a PIT and a scan of the old index; document counts of the two
  indices may legitimately differ afterwards, so a difference is a warning, not a failure.
- `IndexPlan` gains `resume`, `MigrationResult.Outcome` gains `reconcile`, `rollback` returns an
  `Outcome`, and `IndexMigrator.Builder.listener` reports phases (an exception from the listener
  simulates a crash in tests).
