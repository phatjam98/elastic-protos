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
  the index: at `CREATE`, on the new index of a zero-downtime `REINDEX` (a blocking reindex that
  reuses a leftover keeps the leftover's record, which the next plan corrects), with the mappings
  of a `PUT_MAPPING` or `BACKFILL`, and after an `UPDATE_SETTINGS` (which reads `_meta` back and
  rewrites it whole, since the engines replace it). A `NO_OP` writes nothing.
- A recorded key the proto no longer declares leaves the record the first time a plan finds it,
  so the library acts on a removal exactly once (review: a key kept in the record because its
  reset was suppressed would otherwise reset a value set by hand much later). It is reset with a
  null value in the same `PUT _settings` as the other dynamic changes (`SettingsDiff.resets`) when
  it is dynamic under the current options, not managed, and carried by the index; otherwise it is
  released (`SettingsDiff.released`) and only the record changes. Either makes the plan an
  `UPDATE_SETTINGS` when nothing else changes; the next plan is a `NO_OP`.
- A key the index does not carry is never sent: on all four engines a null for an unknown key is a
  400 (`unknown setting`), which would fail every startup after an upgrade dropped a setting.
  Convergence after a run that stopped between the settings update and the record write follows:
  the key is then absent, so it is released.
- Create-time policies are never recorded: the replica policy and `defaultShards` stay out. A
  declared and then removed `number_of_replicas` goes back to the count the replica policy gives a
  new index rather than to the engine default (1, which turns a single-node cluster yellow), and is
  released while `auto_expand_replicas` manages it. A removed `auto_expand_replicas` is reset (to
  `false`) and, unless `number_of_replicas` is declared, the replica count goes back to the policy
  in the same request (all four engines accept both keys in one request, in either order).
- A leftover index that a zero-downtime reindex reuses is brought in line before the copy: its
  dynamic settings are compared with the declaration and reset against its own record, since the
  record written over its `_meta` next would otherwise forget them.
- A recorded key that is no longer dynamic (because `extraDynamicSettings` changed) cannot be
  reset in place; it is released with a warning. Instances should agree on `extraDynamicSettings`.
- Indices without a record (1.0.0-rc.3 or earlier) get it on their next settings or mapping
  update, and nothing is reset on their behalf: their history is unknown.
- Known consequences, documented rather than changed: once any write recorded a key, the proto
  owns it, so a hand-set value the proto later declared is reset on removal if a settings or
  mapping update happened in between; a reset goes to the engine's built-in default, not to an
  index template's value (on Elasticsearch a reset `routing.allocation.include._tier_preference`
  loses the `data_content` preference set at creation); and a rolling deploy with old and new
  protos toggles the setting on each restart until the old instances are gone.

Static settings get the same precision. Each created index records the declared static values in
`_meta.elastic_protos.static_values` next to the digest (omitted when empty; the digest of no
settings stands for it). Planning compares a declared static key with its recorded value, and only
a key added since with the engine's value. That keeps the spelling guard when the declaration
changed (review: with only a digest, any digest change switched to engine values, so a remaining
key the engine spells differently could reindex under `StaticSettingsPolicy.REINDEX`), and names a
removed key: a recorded static key no longer declared at all is the warning `static settings no
longer declared stay on <index> until it is reindexed: [key]`, under either policy, never a reset
or a reindex. It repeats on every plan while it holds, like any other lasting static difference
under `WARN`; it no longer fires for a static setting added with the live value or for a key moved
into `extraDynamicSettings`. Indices with only the digest (1.0.0-rc.3) keep the digest rules and
get no removal warning.
