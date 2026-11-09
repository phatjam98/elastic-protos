# 0008. A per-alias lock in a lock index coordinates concurrent bootstraps

- Status: Accepted
- Date: 2026-11-09

## Context

Applications bootstrap their indices at startup, and a deployment usually starts several replicas
at once. Plan and apply were not coordinated: two instances could both create an index, or one
could reindex while another cleared its write block or deleted its target. The README asked users
to run the bootstrap from one place, which every Kubernetes deployment had to arrange by hand.

The engines offer optimistic concurrency on single documents (`op_type: create`, `if_seq_no` /
`if_primary_term`) on both Elasticsearch and OpenSearch, but no lock service. The zero-downtime
reindex (ADR 0007) already records its progress in a `pending` marker, so an interrupted migration
can be resumed by any process that plans again.

## Decision

- `apply` and `rollback` take a lock per alias before they change anything: a document with the
  alias as id in a lock index named by `IndexNamingStrategy.lockIndex()` (`elastic-protos-locks`,
  prefixed with the naming prefix). The index is a plain index (hidden indices are unverified on
  OpenSearch 2.19), created on first use with one shard and a strict mapping.
- The lock is taken with `op_type: create`; one creator wins. The holder (`hostname/pid/uuid8`)
  renews `expires_at` every third of `lockTtl` with a write conditional on its previous sequence
  number, and releases with a conditional delete. An expired lock is deleted on the sequence number
  it was read with, so exactly one of several takers removes it; all then race to create it.
- Under the lock the item is planned again and the fresh plan is applied. That turns finished work
  into `NO_OP` and lets a waiter resume a crashed holder's work from the `pending` marker.
- Waiting is bounded by `lockWait`; past it the item is `Status.LOCKED`.
- The lease is checked before every irreversible step; a lost lease stops the item without cleanup,
  like a crash, and the next run resumes.
- `MigrationOptions.lock(false)` turns it all off for single-writer deployments.

## Alternatives rejected

- **External coordination only** (Jobs, init containers, leader-only bootstrap): what we had. It
  works but every deployment has to get it right, and a mistake is destructive.
- **A single cluster-level lock in a hidden index**: serializes unrelated aliases, and hidden
  indices are not verified on every engine we support; a per-alias document in a plain index is
  portable and lets different aliases migrate in parallel.
- **Leader election** (a lease shared by all instances, or a coordination service): more moving
  parts and a dependency outside the search cluster, for a problem that per-alias optimistic
  concurrency on the cluster itself already solves.

## Consequences

- Several instances can bootstrap at once; the losers wait, re-plan and skip.
- Expiry relies on clocks: instances must agree to well within `lockTtl`.
- An instance can wait up to `lockWait` at startup; probe budgets must allow for it, or the
  migration runs from a dedicated job.
- A user alias named like the lock index collides with it.
- Every changing item costs a few extra requests (create, renewals, delete, and a re-plan).
