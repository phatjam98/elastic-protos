# 0010. Typed maps through dynamic templates

- Status: Accepted
- Date: 2026-11-09

## Context

A proto `map<K, V>` holds data keys, not schema, so the mapper maps it as `flattened` by default:
one mapping entry, every leaf indexed as a keyword. That keeps the mapping bounded, but a key can
only be compared as a string: no numeric or date ranges, no analyzed full text, no geo queries.

`type: OBJECT` on a map used to emit `{"type": "object", "dynamic": true}` and nothing else. The
engine then guessed a type from the first value of each key (`"5"` as text, a long as `long`, an
RFC 3339 string as `date` only if date detection matched), documents with a differently shaped value
could be rejected later, and the query compiler, which only knew declared fields, treated every key
as unknown. The proto already states the value type, so the guess is avoidable.

Both engines support root-level `dynamic_templates`: the first template whose `path_match` matches
a new field decides its mapping. `*` in `path_match` matches any characters, dots included;
`match_mapping_type: "object"` restricts a template to object values; a list of patterns in
`path_match` needs Elasticsearch 8.9 and is not available in OpenSearch.

## Decision

- `type: OBJECT` (or `NESTED`) on a map field makes it a typed map. The container keeps
  `{"type": "object" | "nested", "dynamic": true}`, and the root mapping gains `dynamic_templates`
  generated from the value type. `flattened` stays the default for maps.
- The value is mapped by the ordinary property logic, with `*` standing for the key, into scratch
  collectors so nothing lands among the declared fields. A new option `value_type` requests the
  value type and is validated like `type`; the field's analyzer, keyword, format and indexing
  options apply to the values.
- A scalar, enum, wrapper, `Timestamp`, `LatLon` or `GeoShape` value is one template
  `ep:<path>.*`. A message value becomes one template per leaf (`ep:<path>.*.<leaf>`) and one
  template per object or nested container, including the value root `<path>.*`, restricted with
  `match_mapping_type: "object"` and mapped as `{"type": ..., "dynamic": true}`.
- Per-leaf templates were chosen over one object template per map carrying the value's
  `properties` tree. A `properties` tree inside a template mapping is not part of either engine's
  documented template contract, so its behaviour on all four supported engine lines would have to be
  relied on without a guarantee. Per-leaf templates use only the documented single-field form and
  are verified by the shared integration suite on Elasticsearch 8.19/9.x and OpenSearch 2.19/3.x.
  The cost is a longer template list (one entry per leaf and container of the value message).
- Ordering: because `*` crosses dots and the first match wins, templates are sorted by the number of
  `path_match` segments, deepest first, then by name. Names are `ep:` plus the `path_match`, unique
  by construction, so the list is deterministic and the content hash stable.
- Values whose message holds a map, `Struct`/`Value`/`ListValue` or `Any` at any depth stay
  untyped: only the root container template is emitted, with a warning. Typed maps inside typed map
  values are left for later.
- The `IndexDefinition` records the value fields as `dynamicFields` keyed by pattern;
  `resolve(path)` instantiates a pattern for a concrete key. The query compiler resolves fields
  through it, adds `ignore_unmapped: true` to `nested` queries on nested paths inside values and
  `unmapped_type` to sorts on keys (keys appear in the mapping only when a document uses them), and
  rejects sorts under such nested paths. The codec converts every entry like a field of the value
  type and rejects empty keys and keys containing `.`.
- A changed root `dynamic_templates` list is a `REINDEX`, unless every changed template belongs to
  a map the same diff adds; then putting the mapping, which replaces the whole template list on both
  engines, is enough.

## Consequences

- Keys of typed maps get real types: ranges on counters, analyzed text on notes, dates, geo points
  and nested matching inside message values.
- Every distinct key adds fields at index time. They count toward
  `index.mapping.total_fields.limit`, but the mapper cannot count them up front; it warns with the
  number of fields per key instead. Typed maps suit bounded key sets; flattened remains the right
  choice for open-ended ones.
- The content hash changes for every map already mapped with `type: OBJECT`, so such indices are
  reindexed on the next bootstrap. Flattened maps, and all other mappings, keep their hash.
- Map keys can no longer contain dots when the map is typed, since the engines expand dotted keys
  into sub-objects.
