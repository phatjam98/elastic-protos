# Soak harness

The integration suite proves each migration path with a handful of documents. The soak harness
runs them under sustained writes, kills a migrating JVM at and in the middle of every phase, and
races several JVMs through the same bootstrap. It lives in the unpublished testkit module and runs
against a cluster that is already up.

## Running it

```bash
docker run -d --name soak-engine -p 9200:9200 -e discovery.type=single-node \
  -e xpack.security.enabled=false -e cluster.routing.allocation.disk.threshold_enabled=false \
  docker.elastic.co/elasticsearch/elasticsearch:9.5.5
./gradlew :elastic-protos-testkit:soak --args="elasticsearch http://localhost:9200 all --expect-version 9.5.5"
docker rm -f soak-engine
```

For OpenSearch use `-e DISABLE_SECURITY_PLUGIN=true -e DISABLE_INSTALL_DEMO_CONFIG=true` and
`opensearch` as the first argument. Turn the disk watermarks off as above: a flood-stage
read-only block would show up as blocked writes. When other engines may be listening on the same
machine, use your own port and `--expect-version`, which refuses to run against a cluster that
reports another version.

`--args="<elasticsearch|opensearch> <url> <live|kill|bootstrap|all> [options]"`:

| Option | Default | Meaning |
|---|---|---|
| `--docs N` | live 200000, kill 50000, bootstrap 10000 | documents seeded before the migration |
| `--shards N` | 3 | `number_of_shards` of every soak index (a setting outside the content hash) |
| `--writers N` | 6 | writer threads |
| `--rate R` | 100 | operations per second per writer |
| `--modes a,b` | `zero-downtime,blocking,backfill,scripted` | live: migrations to run |
| `--phases A,B` | see [kill](#scenarios) | kill: kill points |
| `--jvms K` | 5 | bootstrap: concurrent JVMs |
| `--lock-ttl S` | 15 | kill/bootstrap: `lockTtl` in seconds (the library default is 120) |
| `--swap-grace S` | library default (5) | `swapGrace` in seconds |
| `--post-seconds S` | 5 | writes after the migration returned |
| `--out DIR` | `build/soak` (in the testkit module) | where reports and child logs go |
| `--label L` | `<product>-<version>` | report file prefix |
| `--seed N` | 42 | random seed |
| `--expect-version V` | none | refuse to run unless the cluster reports version `V` |
| `--keep` | off | keep the `soak-*` indices afterwards |

Every index the harness creates starts with `soak-` (a naming prefix per run); it deletes them
before and after each run. The exit code is 0 when no genuine violation was found, 1 when one
was, 2 on a usage error or a version mismatch, and 3 when the harness itself failed. A scenario
that crashes still writes its report, with the runs that finished and `verdict: HARNESS_ERROR`,
and the other scenarios still run.

The **Soak** GitHub Actions workflow (`.github/workflows/soak.yml`) runs the same command on the
four verified images. It is dispatched by hand (scenario and document count as inputs) and uploads
the reports as `soak-<image label>` artifacts.

## Scenarios

**live.** Bootstraps the example `Document` under a prefix, seeds N documents, and starts the
writers and a reader. Writer `w` owns the seeded ids `s-n` with `n % W == w` and the ids it
creates, and sends one request at a time, so the order of the writes of an id is defined. Each
writer creates (a full write of an absent id, new or previously deleted), overwrites, partially
updates (`_update` with `doc`) and deletes, at a steady rate, with a bias towards ids it wrote
recently so that documents get written on both sides of the alias swap. Then it migrates:

- `zero-downtime`: `title` becomes a `keyword` (REINDEX, `ReindexMode.ZERO_DOWNTIME`);
- `blocking`: the same with `ReindexMode.BLOCKING`;
- `backfill`: `body` gains a `keyword` multi-field (BACKFILL);
- `scripted`: the zero-downtime reindex through a content-preserving migration script
  (`ctx._source.remove('soak_no_such_field')`), so the script probe and the staging-index
  reconcile (`<new index>-reconcile`, every hot document written) run under load.

Writes keep going for `--post-seconds` after the migration returned, then the alias is read in
full and compared with the operation log. The run then checks the end state: the alias points at
one index, which is not write-blocked, is not the old index (reindex) or is the old index
(backfill), carries no `pending` marker and has the target mapping (`title` a `keyword`, or
`body.keyword` present); planning again is `NO_OP`; the lock index is empty.

**kill.** For every kill point in `--phases`, a child JVM runs the migration while the parent
writes, and the parent kills it with `SIGKILL`. A fresh migrator in the parent then plans and
applies. Kill points:

- at a phase boundary (`PREPARED`, `COPIED`, `CAUGHT_UP`, `SNAPSHOT`, `SWAPPED`, `FROZEN`,
  `RECONCILED`, `SWEPT`, `FINISHED`, `BACKFILL_MARKED`, `BACKFILLED`): the child's listener parks
  the migrating thread once it reports the phase (the lock heartbeat keeps running);
- in the middle of a phase: a fixed delay after the phase before it was reported, `MID_COPY`
  (400 ms after `PREPARED`, while the first `_reindex` task runs; the task carries on on the
  server after the kill), `MID_CATCHUP` (150 ms after `COPIED`), `MID_FREEZE` (`swapGrace` + 30 ms
  after `SWAPPED`, between the write block and the `FROZEN` marker), `MID_RECONCILE` (300 ms after
  `FROZEN`), `MID_SWEEP` (200 ms after `RECONCILED`), `MID_BACKFILL` (500 ms after
  `BACKFILL_MARKED`). The report's `midPhaseKill.insidePhase` says whether the kill landed before
  the next phase was reported;
- `SCRIPTED_<point>`: the same with the migration script (default `SCRIPTED_SWAPPED`,
  `SCRIPTED_MID_RECONCILE`).

The run checks that the resume completes (`APPLIED`, or `SKIPPED` when the kill came after the
marker was removed), the end state as for live, that the dead holder's lock was taken over no
later than its `expires_at` plus a third of `lockTtl` plus 3 s (the waiter polls every 1 to 2 s)
and released afterwards, and that the alias matches the log. When the resume needs no lock
(`NO_OP`), the dead holder's lock document is left to expire; the report says so
(`staleLockLeftToExpire`) and checks that it expires no later than `lockTtl` + 1 s after the kill.
A child that exits without reaching its kill point fails the run at once.

**bootstrap.** K JVMs plan and apply `Document`, `Firm`, `InboxItem` and `TypedMaps` at the same
wall-clock instant: on an empty cluster (`create`), after a mapping change (`reindex`, the
documents alias seeded; `title` also changes inside `InboxItem`, so two aliases reindex), each
with `lockWait` 10 minutes and zero, and on an interrupted reindex (`resume`: a child killed after
the swap left the marker and its lock). Per alias exactly one JVM applies the change, the others
end `SKIPPED` (planned again under the lock as `NO_OP`) or, with `lockWait` zero, `LOCKED`; no
`FAILED` outcome, one index per alias (two after a reindex), the alias on one index that is not
write-blocked and has no marker, the lock index empty, and the seeded documents unchanged. Every
JVM must exit 0, be ready before the start instant, and start within 500 ms of the others.

## The operation log

A write enters the log only once acknowledged, with the concrete index the engine reported (the
delete goes through `_bulk` so that its item names the index) and the time it was sent. A write
the engine rejected (404, 409, a block) changes nothing. A write with an unknown outcome (timeout,
I/O, 5xx) makes its id uncertain: that id is left out of the comparison, but the failed attempt
itself is always recorded and is a violation, so uncertain ids never hide anything. Blocked writes
and 429s are retried after 100 ms, as the README advises; every failed attempt is recorded with
the phase it failed in.

The expected document of an id is the replay of its log. Writes whose result contradicts the log
(a create that found the document, an overwrite that created it, a delete that found nothing) are
recorded as observations with the id's history at the time. The reader takes realtime `GET`s
through the alias and records every stale one with the id's history at the time. The final scan
reads the alias under a point in time (sorted by `_shard_doc`, or `_id` where the engine has no
`_shard_doc`) and fails when the number read differs from an exact count.

All times are the host's wall clock, shared by the parent and the child JVMs; a clock step during
a run (NTP) would shift the phase comparisons.

## Reading the report

`<out>/<label>-<scenario>.json` holds one entry per run under `runs`, plus `violations` and
`verdict` (`PASS`, `VIOLATIONS` or `HARNESS_ERROR`). The console prints the same in short. Per
run:

- `phases`: every phase reported (by the parent, the child, or the resumed migrator), in ms from
  the start of the migration; `timeline`: the bounds the rules below use, in ms from the swap.
- `writes`: `attempts`, `acknowledged`, and counters by operation (`attempted.*`, `acked.*`,
  `rejected.*`, `retried.*`, `unknownOutcome.*`, `observed.*`).
- `failures`: failed attempts by `phase/operation/kind`, and `byClass`, the documented class of
  each or `VIOLATION`, with samples of the violations.
- `reads`: realtime reads, the stale ones `byClass`, the documented stale window
  (`firstStaleAfterSwapMs`..`lastStaleAfterSwapMs`) next to `swapToReconcileBoundMs` and
  `swapToSweptMs`, and up to ten samples per class with the id's history.
- `observations`: by `kind/class`, with samples.
- `comparison`: `checked`, `matched`, `uncertainSkipped`, `violations` (`MISSING`, `EXTRA`,
  `DIFFERING`), `documentedAnomalies`, and up to ten `samples` of each with the expected and actual
  document and the id's history (operation, old or new index, time from the swap and from the
  catch-up).
- `outcome` / `resumeOutcome`: the migration outcome, including the reconcile statistics;
  `midPhaseKill`: where a mid-phase kill landed.
- `checks`: failed end-state checks.

## What counts as a violation

Times: *catch-up* is the last `COPIED` before the swap (writes sent after it may have missed the
catch-up), *copy start* the first `PREPARED`, *swap* the effective `SWAPPED`. The *reconcile
bound* is the first `RECONCILED`: when a run died while reconciling, that is the resumed run's,
since the new index lacks late writes until a reconcile completes (README "Running from several
instances"). The *sweep* is the first `SWEPT`. The README documents each anomaly only before the
reconcile reached the document, so the same symptom produced later (for example by a resumed
reconcile) is a violation. A resurrection is also a violation when a write of the new index found
the document while the log held it deleted: the reconcile had already created it once, so a
document present at the end was created twice.

Documented behaviour, counted apart:

| Class | Mode | What |
|---|---|---|
| `BLOCKED_AFTER_SWAP` | zero downtime | a write sent before the swap and blocked after it (a straggler that resolved the alias to the old index) whose retry then succeeded |
| `UPDATE_404_BEFORE_RECONCILE` | zero downtime | a partial update failing after the snapshot phase (the swap request follows it, and only the new index can answer 404 for a document the old index holds), sent before the reconcile bound, of a document the old index created after the catch-up |
| `STALE_PARTIAL_UPDATE` | zero downtime | the first write of an id after the swap is a partial update sent before the reconcile bound, and the document equals that update applied to an earlier state, every write in between having gone to the old index after the catch-up |
| `STALE_PARTIAL_UPDATE_ON_GHOST` | zero downtime | the same, where the stale copy is the one the first copy read (its base acknowledged before the copy start): the document was deleted from the old index during the copy and absent when the catch-up read it |
| `RESURRECTED_AFTER_DELETE` | zero downtime | the old index created the document after the catch-up; every write of the new index was sent before the reconcile bound, the first found nothing there, none found it while the log held it deleted, and the last deleted it; the alias holds the old index's version |
| `RECONCILE_WINDOW` (reads, observations) | zero downtime | stale reads, and writes that found the new index stale, sent between the swap and the reconcile bound |
| `GHOST_BEFORE_SWEEP` (reads, observations) | zero downtime | a document deleted from the old index after a copy pass read it (a version acknowledged before the last `CAUGHT_UP`), read or found in the new index before the sweep |
| `BLOCKING_WRITE_BLOCK` | blocking | a write sent before the swap and blocked by the write block |

The ghost classes are not in the README or ADR 0007 yet: the catch-up copies no deletes and the
sweep removes them after the swap, so until the sweep reads return documents deleted from the old
index after a copy pass read them (the README only says the new index can lack writes until the
reconcile, not that it shows deleted documents until the sweep), and a partial update can land on such a copy and bring its deleted fields back (the
reconcile keeps the update). They are kept as separately named classes until documented.

Everything else is a genuine violation: any other failed write; a stale read or an observation
outside the classes above (after the reconcile bound only a document left in a documented anomaly
explains one; in a blocking reindex or a backfill none is documented at all); a missing, extra or
differing document the classes above do not explain; and a failed end-state check. "Sent after
the catch-up" is a strict reading: a write sent just before `COPIED` was reported may still have
missed the catch-up, and is then reported as a violation to look at, not hidden.

## What a single-node run cannot show

The harness runs against whatever cluster it is given; the workflow and the examples above use
one node. That leaves out replica lag and replica refreshes, primary failover or relocation during
a migration, segment replication (OpenSearch), a red lock index, and a lease lost without a crash
(a network partition between holder and cluster, the "lost after two thirds of `lockTtl`" path).
Every JVM shares one clock, so clock skew between instances is not exercised either. Nor are
upserts, optimistic-concurrency writes from the application, bulk application writes, routing,
rollback, a degraded reconcile after the point in time expired, copies near `reindexTimeout`, or
production document sizes and write rates. A clean run is evidence for these paths at this scale,
not a bound on how often an anomaly occurs under real load.
