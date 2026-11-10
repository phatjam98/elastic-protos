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

Review refinements (same decision): the old index's sequence number checkpoint is recorded before
the first copy, and documents written after it are reconciled first and compared regardless of
version, since a version restarts at 1 once a delete's tombstone is gone (`index.gc_deletes`); a
script is probed for `_id` changes before the swap; the freeze uses `_block/write`, which waits for
writes in flight; and without the snapshot the reconcile only reports (`INCOMPLETE`) unless the
operator asks it to create missing documents, which would bring back deletes made after the swap.

## Consequences

- Full-document writes and deletes through the alias never fail during a reindex, except a
  straggler that resolved the alias before the swap and arrives after the grace period: it gets a
  retryable `BLOCKED` error rather than landing in an index nobody reads. *(Qualified in the second
  addendum: a partial `_update` can fail with 409 at the swap.)*
- Reads after the swap can miss writes made to the old index after the catch-up pass until the
  reconcile reaches them, and return documents deleted from the old index after the first copy
  started until the sweep; a partial `_update` in that window applies to the stale copy. Last
  writer wins per document.
- A reindex costs two copy passes, a PIT and a scan of the old index; document counts of the two
  indices may legitimately differ afterwards, so a difference is a warning, not a failure.
- `IndexPlan` gains `resume`, `MigrationResult.Outcome` gains `reconcile`, `rollback` returns an
  `Outcome`, and `IndexMigrator.Builder.listener` reports phases (an exception from the listener
  simulates a crash in tests).

## Addendum (2026-11-10): reconcile by sequence number range

The cold pass of step 7 and the sweep of step 8 each read every document, so the stale-read window
after the swap grew with the index, not with the writes made during the copy. They are now skipped
where that is provably sound (`ReconcileStrategy.SEQ_NO_RANGE`, recorded in `ReconcileStats`).

- **Checkpoint.** The prepare step records the global checkpoint of the old index (the lowest
  `seq_no.global_checkpoint` over its primaries, `SearchEngine.globalCheckpoint`) instead of the
  lowest `max_seq_no` (still the fallback for an engine without it). Every operation at or below a
  shard's global checkpoint has been processed by every in-sync copy, and no later operation gets a
  sequence number at or below it, even after a primary fails over (the new primary holds every
  operation up to the global checkpoint and continues above its own `max_seq_no`). `max_seq_no`
  promises neither: a number can be taken before its operation is in Lucene, and above the global
  checkpoint a failover can roll operations back and hand their numbers to new writes. The
  checkpoint is the minimum over the shards, so for the other shards it is a lower bound and the
  hot pass reads a superset.
- **Skipping the cold pass.** The copy refreshes the old index after the checkpoint is read, so a
  document at or below it is visible to the first copy and is copied with its version. When the
  run created the new index itself (no reused leftover), no script runs, and it is not a rollback,
  nothing else writes the new index before the snapshot, so every such document is in the snapshot
  unchanged (premise P). The hot pass (`range` on `_seq_no` above the checkpoint) is then the whole
  reconcile. A checkpoint of -1 (a shard without operations) would make the range the whole index
  and is a full scan instead. OpenSearch segment replication on either index is excluded, because
  a replica can serve a search (the copy's scroll, or the snapshot) from older segments after a
  refresh.
- **Skipping the sweep.** A delete leaves nothing a range query can find, so the sweep is decided
  by counting. With S the snapshot, A the frozen old index and H its documents above the
  checkpoint, P gives `A \ H ⊆ S`, so `|S \ A| = |S| - |A| + |H \ S|`, where `|H \ S|` falls out
  of the hot pass. Zero ghosts skips the sweep; a positive count runs it.
- **Checking P, not assuming it** (review). If P fails for k documents, the count reads
  `ghosts - k`, so a violation that loses as many documents as there are real ghosts reads 0 and
  silently skips both the sweep and the cold pass. The negative branch (cold pass and sweep, with
  a warning) is therefore only a last-resort inconsistency detector, not a guard. P is checked
  where it is cheap, before the swap, and the range is dropped (full scan) when any check fails:
  - the checkpoint is not above the engine's own lowest `max_seq_no` (no valid global checkpoint
    is);
  - right after the refresh that precedes the first pass, the old index's documents at or below
    the checkpoint are counted (C1); the first pass must read at least C1 documents and create
    exactly every document it read: no failure, version conflict (on a fresh index: duplicate ids
    under custom routing), no-op (a dropping ingest pipeline), update or delete;
  - the catch-up deletes nothing;
  - the snapshot holds exactly the number of documents the two passes created (nothing else
    wrote or deleted the fresh index before the swap);
  - at the freeze, the documents at or below the checkpoint are counted again and must not exceed
    C1 (no later operation can add one).

  The decision and C1 are written into the marker only at `phase: swapping` (`seq_no_range: true`,
  `cold_count`), so a resume cannot turn the range on; a marker without both (1.0.0-rc.3 or
  earlier, or a run that did not qualify) resumes with the full scan, as does
  `MigrationOptions.fullReconcile`. What stays assumed is the engine's global checkpoint promise
  itself (processed on every copy before it is reported; never reused after a failover), which a
  single-node test cannot exercise.
- **Partial answers.** The engines answer a search that some shards could not run with 200, the
  other shards' hits and a total with `relation: eq` (verified on all four engines, plain and with
  a point in time, by a script query that throws on one shard; `_count` behaves the same;
  `allow_partial_search_results=false` turns it into a 400 `search_phase_execution_exception` on
  all four). `SearchResult` now carries `shardsFailed`, and every search and count the migrator
  relies on fails on a partial answer: on the old index the item fails and resumes; on the snapshot
  it counts as a lost snapshot. The response check was chosen over the request parameter because
  the SPI's `search` carries no request parameters and the check works the same everywhere;
  ordinary searches are unchanged.
- **Cost.** Under the same checkpoint the range path does a subset of the full scan's work, so
  there is no size threshold; but the checkpoint is the minimum over shards whose sequence numbers
  are not aligned in time, so on skewed shards the range can cover most of the index.

Rejected:

- **Per-shard sequence number accounting** (per shard, `max_seq_no - checkpoint` equal to the
  number of live documents above the checkpoint proves that no operation in between was a delete).
  It is sound, since every other consumer of a sequence number only makes the numbers differ, but
  it would run the sweep far too often. Observed on ES 8.19.23 and 9.5.5 and OpenSearch 2.19.7 and
  3.9.0: a no-op `_update`, a script `noop`, a create or `if_seq_no` conflict (409), a mapping
  failure (alone or in a bulk), a refresh and a flush consume no sequence number; a full-document
  write of an unchanged document, an `_update` with `detect_noop: false`, and a delete of a missing
  id (`result: not_found`) each consume one. Any document written twice after the checkpoint, the
  norm on a busy index, would force the sweep. It also needs per-shard counts
  (`preference=_shards:<n>`) and per-shard checkpoints in the marker. Counting ghosts directly needs
  neither and only fires on an actual ghost.
- **A per-shard hot pass** (`preference=_shards:<n>`, sorted on `_seq_no`, against per-shard
  checkpoints): exact rather than a superset on multi-shard indices, but more requests and a wider
  SPI (search request parameters). It is the fix for skewed and empty shards, left for later.
- **Counting deletes** with `indexing.delete_total` from `_stats`: a per-copy counter that resets
  when a shard copy restarts or relocates, so a difference of zero proves nothing.

## Addendum (2026-11-10): resuming the reconcile without replaying its creates

A soak test found that a resumed reconcile brought back a document deleted through the alias (on
1.0.0-rc.3 as well): the first run's reconcile created a document the snapshot lacked (written to
the old index after the catch-up), the run died after the reconcile but before the marker moved
on, the application deleted the document through the alias, and the resume replayed the whole
reconcile, whose `create` found no document and wrote it again. "Conditional writes make it
idempotent" holds for the conditional index and delete (409 once the document changed), not for
`create`, which a tombstone in the new index does not stop.

Decision: the reconcile records its progress in the marker (`pending.progress`) and a resume never
repeats a recorded write.

- Every pass that writes (hot, cold, a `CREATE_MISSING` degraded reconcile) reads the frozen old
  index sorted by `_seq_no`, then by shard (`_shard_doc`, or `_id` where that is missing). The
  frozen index takes no writes, and the order rests on sequence numbers, not on Lucene document
  ids that a merge can change, so a new point in time sees the same order. After every page that
  wrote, the cursor is recorded: the last sequence number and the ids at it already processed
  (sequence numbers are per shard, so at most one document per shard shares it). A resumed pass
  queries `_seq_no >= cursor` without those ids. Completed passes, including the sweep, go into
  `done`; the count of hot documents the snapshot lacks is kept too, so the range strategy's ghost
  count is the same after a resume.
- Residual: the run can die after a page's writes and before its cursor write. That page (at most
  1,000 documents) is replayed, and a document it created that was deleted through the alias in
  between is created again. No exact discriminator exists between "never created" and "created,
  then deleted": the delete's tombstone (and its version) lives only for `index.gc_deletes` (60 s by
  default), a realtime GET of a deleted document returns nothing, and an external-version write
  that a tombstone would stop would also overwrite a newer write through the alias once the
  tombstone is gone. Writing the cursor before the page (write-ahead) would turn the residual into
  dropping documents written to the old index, an acknowledged write lost; re-creating a document
  is the error the design already accepts for a document created after the catch-up and deleted
  through the alias before the reconcile reached it, so the library keeps that bias and makes the
  window one page.
- Conditional writes and the sweep's conditional deletes are replay-safe: a document the sweep
  deleted and the application wrote again before the resume has another sequence number, so the
  replayed delete gets 409 and the document stays.
- The same machinery serves the range and full-scan strategies, the degraded `CREATE_MISSING` path
  and the `REVERSE_DELTA` rollback. A marker of 1.0.0-rc.3 or earlier has no progress and replays
  the whole reconcile, as rc.3 did.

The soak also found that a partial `_update` through the alias can fail with 409 at the instant of
the swap (the engine read the document in the old index and the write resolved against the new
one) and during a `BACKFILL` (the update by query rewrote the document between the update's read
and its write). The write is not applied and a retry succeeds, so the guarantee is qualified to
full-document writes and deletes, and `SearchEngine.update` now retries in the engine
(`retry_on_conflict`, default 3; a new overload sets it): the merge carries no condition of the
caller's, so applying it to the newest version is what was asked for.
