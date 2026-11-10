# 0006. Index settings stay out of the content hash

- Status: Accepted
- Date: 2026-11-08

## Context

Every concrete index is named `<base>-v<hash>`, where the hash is the first 8 hex digits of a
SHA-256 over the canonical `mappings` (`dynamic` plus `properties`) without `_meta`. The migrator
compares that hash, and a structural diff of the mappings, with what the alias points at today to
choose `NO_OP`, `PUT_MAPPING` or `REINDEX`.

An `IndexDefinition` also carries settings, assembled from `shards`, `replicas`/`has_replicas`,
`settings_json` (typically `analysis` definitions) and `MappingOverrides.setting(...)`. These are a
mix of concerns:

- Shard counts cannot change on an existing index; replica counts are operational and often differ
  per environment (`ReplicaPolicy`, the Micronaut `defaults.replicas` property).
- `refresh_interval` and similar keys are tuning that infrastructure owners change without a schema
  change.
- Analysis definitions (analyzers, tokenizers, filters, normalizers) are part of how text is
  indexed, but are referred to from the mappings by name.

If settings fed the hash, any of these changes would rename the target index and trigger a full
reindex at the next bootstrap, including changes made only to suit one environment.

## Decision

- The content hash covers the mappings only. Settings never contribute to it, and neither do
  `_meta` or the naming prefix.
- On an existing index, settings are compared only for the keys the proto declares. A difference
  that is the only change classifies as `NO_OP` with the warning `settings differ; not applied`
  (and, for shard and replica counts, a per-key warning). The library never updates the settings of
  an existing index and never reindexes because of settings alone. *(Superseded by the addendum
  below: dynamic settings are now updated in place, and static settings can opt into a reindex.)*
- Settings are applied whenever the library creates an index: on `CREATE`, and on the new index of
  a `REINDEX` that a mapping change caused. A settings change therefore rides along with the next
  mapping-driven reindex.
- A change that has to reach an existing index on its own, typically an analyzer edit, needs a
  deliberate step. The supported path is to make the change visible in the mappings: define the
  analyzer under a new name in `settings_json` and point the field's `analyzer` (or the message's
  `default_analyzer`) at it. The mapping parameter changes, so the hash changes and the migrator
  plans a `REINDEX` into a new index created with the new settings. Editing an analyzer definition
  in place under the same name is not picked up. The alternative is a manual reindex outside the
  library.
- No proto option is added to force a new hash (such as a `schema_version` field on the message);
  the mapping-visible rename covers the need without widening the options API.

## Consequences

- Infrastructure-only changes (replicas, refresh interval, environment-specific overrides) never
  cause a surprise reindex or a new index name.
- The same proto produces the same index name in every environment, whatever its settings.
- Analyzer edits need an explicit step, and a user who edits only `settings_json` sees a warning in
  the plan rather than a migration. The README's migration section documents the rename path.
- Indices created before a settings change keep their old settings until the next reindex; the
  `settings differ; not applied` warning in `plan(...)` output is the signal that they diverge.
  *(Superseded by the addendum below for dynamic settings, which are applied in place.)*

## Addendum (2026-11-09): settings applied through a settings diff

Settings remain outside the content hash. Instead of only warning, the migrator now compares the
settings the proto declares with the live index on every plan and applies them through a settings
diff: dynamic settings in place (`UPDATE_SETTINGS`, or before a `PUT_MAPPING` / `BACKFILL`), static
settings by a warning (default) or, with `StaticSettingsPolicy.REINDEX`, by a reindex into the next
generation of the same index name. Each created index records a digest of its declared static
settings in `_meta.elastic_protos.static_settings`, compared instead of the engine-reported values
so that a different spelling cannot loop a reindex. Only declared keys are compared, so environment-specific replica
policies still never change an index name or trigger a reindex; a removed setting is never reset.
*(Superseded by the second addendum for dynamic settings.)*
The rename path above remains the way to roll out an analyzer change without a static-settings
policy.

## Addendum (2026-11-10): removed dynamic settings are reset

Comparing only declared keys meant that a dynamic setting declared, applied and later removed
from the proto stayed on the index for good. The engines report only explicitly set settings
(without `include_defaults`), so a live value cannot tell a setting the proto once declared from
one an operator set by hand. The index therefore records what the proto declared:

- `_meta.elastic_protos.dynamic_settings` lists the declared dynamic keys (flattened, without
  `index.`, managed keys left out; omitted when empty). It is written whenever the migrator writes
  the index: at `CREATE`, on the new index of a `REINDEX`, with the mappings of a `PUT_MAPPING` or
  `BACKFILL`, and after an `UPDATE_SETTINGS` (which reads `_meta` back and rewrites it whole, since
  the engines replace it). A `NO_OP` writes nothing.
- A recorded key the proto no longer declares, and that is dynamic under the current options, is
  reset with a null value in the same `PUT _settings` as the other dynamic changes
  (`SettingsDiff.resets`), which makes the plan an `UPDATE_SETTINGS` when nothing else changes.
  After that the record no longer lists it, so a later value set by hand is left alone.
- Create-time policies are never recorded: the replica policy and `defaultShards` stay out, as
  before. A declared and then removed `number_of_replicas` goes back to the count the replica
  policy gives a new index rather than to the engine default (1, which turns a single-node cluster
  yellow), and is left alone while `auto_expand_replicas` manages it. A removed
  `auto_expand_replicas` is reset (to `false`), and a declared `number_of_replicas` is then
  compared in the same plan.
- A recorded key that is no longer dynamic (because `extraDynamicSettings` changed) cannot be
  reset in place; the plan warns. Instances with different `extraDynamicSettings` record different
  lists; only real writes rewrite the record, so they do not flap, but they should agree.
- Indices without a record (1.0.0-rc.3 or earlier) get it on their next settings or mapping
  update, and nothing is reset on their behalf: their history is unknown.
- Static settings are not recorded. A static setting removed from the proto changes the digest in
  `static_settings` while no declared static key differs; that was silently ignored and is now a
  warning, under either `StaticSettingsPolicy`, without a reindex: the digest cannot name the key,
  and a digest change also follows a change of `extraDynamicSettings`, which must not reindex.
