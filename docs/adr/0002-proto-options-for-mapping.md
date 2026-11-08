# 0002. Proto options, not field-name heuristics, drive mappings

- Status: Accepted
- Date: 2026-11-01

## Context

The pre-1.0 mapper guessed: a string containing "centroid" became `geo_shape`, a string named `id`
became `text`, timestamps were detected by an `_at` suffix, and every message became `nested`. The
index suffix hashed only top-level field names, so type changes never produced a new index. Guesses
like these are invisible to readers of the proto, break silently on renames and cannot be overridden
cleanly.

## Decision

- Mapping behaviour is declared with custom options in `phatjam98/elasticprotos/v1/elastic_options.proto`:
  `(phatjam98.elasticprotos.v1.elastic)` on messages (`MessageOpts`) and
  `(phatjam98.elasticprotos.v1.elastic_field)` on fields (`FieldOpts`).
- Without options, fixed type-based defaults apply: `string` => `keyword`, singular message =>
  `object`, repeated message => `nested`, `Timestamp` => `date`, well-known and shipped geo types by
  their full name.
- Teams that cannot annotate their protos use `MappingOverrides`, built from the same option
  messages. Precedence, lowest to highest: `MappingDefaults`, proto options, `MappingOverrides`, a
  field's `raw_mapping_json`.
- Names never change a field's type. Requested types are validated against the proto type.
- The concrete index name carries a hash of the full canonical mapping.

## Consequences

- The proto is the readable, reviewable source of truth; the mapping is deterministic and covered by
  golden files.
- Free-text search is opt-in per field (`type: TEXT`), which keeps the default mapping cheap and
  exact-match friendly.
- The options proto is public API: option numbers (50001, 50002) and field numbers are frozen, and
  new options are additive.
- Consumers must make `elastic_options.proto` importable; the core jar ships it for protoc.
