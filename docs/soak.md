# Soak harness

The integration suite proves each migration path with a handful of documents. The soak harness
runs them under sustained writes, kills a migrating JVM at every phase, and races several JVMs
through the same bootstrap. It lives in the unpublished testkit module and runs against a cluster
that is already up.

## Running it

```bash
docker run -d --name soak-engine -p 9200:9200 -e discovery.type=single-node \
  -e xpack.security.enabled=false -e cluster.routing.allocation.disk.threshold_enabled=false \
  docker.elastic.co/elasticsearch/elasticsearch:9.5.5
./gradlew :elastic-protos-testkit:soak --args="elasticsearch http://localhost:9200 all"
docker rm -f soak-engine
```

For OpenSearch use `-e DISABLE_SECURITY_PLUGIN=true -e DISABLE_INSTALL_DEMO_CONFIG=true` and
`opensearch` as the first argument. Turn the disk watermarks off as above: a flood-stage
read-only block would show up as blocked writes.

`--args="<elasticsearch|opensearch> <url> <live|kill|bootstrap|all> [options]"`:

| Option | Default | Meaning |
|---|---|---|
| `--docs N` | live 200000, kill 50000, bootstrap 10000 | documents seeded before the migration |
| `--writers N` | 4 | writer threads |
| `--rate R` | 50 | operations per second per writer |
| `--modes a,b` | `zero-downtime,blocking,backfill` | live: migrations to run |
| `--phases A,B` | every zero-downtime phase, `BACKFILL_MARKED`, `BACKFILLED` | kill: phases to kill at |
| `--jvms K` | 5 | bootstrap: concurrent JVMs |
| `--lock-ttl S` | 15 | kill/bootstrap: `lockTtl` in seconds (the library default is 120) |
| `--swap-grace S` | library default (5) | `swapGrace` in seconds |
| `--post-seconds S` | 5 | writes after the migration returned |
| `--out DIR` | `build/soak` (in the testkit module) | where reports and child logs go |
| `--label L` | `<product>-<version>` | report file prefix |
| `--seed N` | 42 | random seed |
| `--keep` | off | keep the `soak-*` indices afterwards |

Every index the harness creates starts with `soak-` (a naming prefix per run); it deletes them
before and after each run. The exit code is 0 when no genuine violation was found, 1 when one
was, 2 on a usage error and 3 when the harness itself failed.

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
- `backfill`: `body` gains a `keyword` multi-field (BACKFILL).

Writes keep going for `--post-seconds` after the migration returned, then the alias is read in
full and compared with the operation log.

**kill.** For every phase in `--phases`, a child JVM runs the migration while the parent writes.
When the child's listener reports the phase it parks the migrating thread (the lock heartbeat
keeps running) and the parent kills it with `SIGKILL`. A fresh migrator in the parent then plans
and applies. The run checks that the resume completes (`APPLIED`, or `SKIPPED` when the kill came
after the marker was removed), that the alias points at one index without a `pending` marker,
that planning again is `NO_OP`, that the dead holder's lock was taken over no later than its
`expires_at` plus a third of `lockTtl` plus 3 s (the waiter polls every 1 to 2 s) and released
afterwards, and that the alias matches the log. When the resume needs no lock (`NO_OP`), the dead
holder's lock document is left to expire; the report says so (`staleLockLeftToExpire`) and checks
that it expires within `lockTtl` of the kill.

**bootstrap.** K JVMs plan and apply `Document`, `Firm`, `InboxItem` and `TypedMaps` at the same
wall-clock instant: on an empty cluster (`create`), after a mapping change (`reindex`, the
documents alias seeded; `title` also changes inside `InboxItem`, so two aliases reindex), each
with `lockWait` 10 minutes and zero, and on an interrupted reindex (`resume`: a child killed after
the swap left the marker and its lock). Per alias exactly one JVM applies the change, the others
end `SKIPPED` (planned again under the lock as `NO_OP`) or, with `lockWait` zero, `LOCKED`; no
`FAILED` outcome, one index per alias (two after a reindex), the alias on one index without a
marker, the lock index empty, and the seeded documents unchanged.

## The operation log

A write enters the log only once acknowledged, with the concrete index the engine reported (the
delete goes through `_bulk` so that its item names the index) and the time it was sent. A write
the engine rejected (404, 409, a block) changes nothing; a write with an unknown outcome (timeout,
I/O, 5xx) makes its id uncertain and that id is left out of the comparison. Blocked writes and
429s are retried after 100 ms, as the README advises; every failed attempt is recorded with the
phase it failed in.

The expected document of an id is the replay of its log. Every difference from the alias is
either explained by a documented anomaly or a genuine violation.

## Reading the report

`<out>/<label>-<scenario>.json` holds one entry per run under `runs`, plus `violations` and
`verdict` (`PASS` or `VIOLATIONS`). The console prints the same in short. Per run:

- `phases`: every phase reported (by the parent, the child, or the resumed migrator), in ms from
  the start of the migration.
- `writes`: `attempts`, `acknowledged`, and counters by operation (`attempted.*`, `acked.*`,
  `rejected.*`, `retried.*`, `unknownOutcome.*`). `observed.*` counts writes whose result
  contradicted the log's view at the time (a delete that found nothing, an overwrite that created
  the document), by phase: the visible side of a stale new index.
- `failures`: failed attempts by `phase/operation/kind`, and `byClass`, the documented class of
  each or `VIOLATION`.
- `reads`: realtime `GET`s through the alias, only counted when no write of the id was in flight.
  `stale` reads differ from the log; `staleOfPersistentIds` are reads of ids that still differ at
  the end (documented anomalies); `firstStaleAfterSwapMs`..`lastStaleAfterSwapMs` is the observed
  stale-read window of the others, to compare with `swapToReconciledMs`.
- `comparison`: `checked`, `matched`, `uncertainSkipped`, `violations` (`MISSING`, `EXTRA`,
  `DIFFERING`), `documentedAnomalies`, and up to ten `samples` of each with the expected and actual
  document and the id's history (operation, old or new index, time from the swap and from the
  catch-up).
- `outcome` / `resumeOutcome`: the migration outcome, including the reconcile statistics.
- `checks`: failed structural checks (status, marker, lock, alias, indices).

## What counts as a violation

Documented behaviour, counted apart:

| Class | Mode | What |
|---|---|---|
| `BLOCKED_AFTER_SWAP` | zero downtime | a write blocked after the swap (a straggler that resolved the alias to the old index) |
| `UPDATE_404_BEFORE_RECONCILE` | zero downtime | a partial update after the swap of a document the old index created after the catch-up began; the new index lacks it until the reconcile |
| `STALE_PARTIAL_UPDATE` | zero downtime | the first write of an id after the swap is a partial update and the document equals that update applied to an earlier state, every write in between having gone to the old index after the catch-up began |
| `STALE_PARTIAL_UPDATE_ON_GHOST` | zero downtime | the same, where the stale copy is the one the first copy took: the document was deleted from the old index during the copy and was absent when the catch-up read it (the catch-up copies no deletes; the sweep removes them after the swap) |
| `RESURRECTED_AFTER_DELETE` | zero downtime | the old index last created the document after the catch-up began, it was deleted through the alias after the swap, and the reconcile created it again |
| `BLOCKING_WRITE_BLOCK` | blocking | a write blocked during the copy |

Everything else is a genuine violation: any other failed write, a stale read that ended before the
swap could have happened, a missing, extra or differing document the classes above do not explain,
and a failed check. "Sent after the catch-up began" means sent after the `COPIED` phase was
reported, which is a strict reading: a write sent just before it may still have missed the
catch-up, and is then reported as a violation to look at, not hidden.
