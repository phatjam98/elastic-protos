# 0011. Maps, Struct and Any inside the values of typed maps

- Status: Accepted
- Date: 2026-11-10
- Supersedes: the "untyped values" bullet of [ADR 0010](0010-typed-maps-through-dynamic-templates.md)

## Context

ADR 0010 maps a typed map (`type: OBJECT` or `NESTED` on a `map`) to an `object` or `nested`
container with `dynamic: true` plus one root-level dynamic template per leaf and container of the
value, `*` standing for the key. It left a value untyped (only the root object template, with a
warning) when the value message held a `map`, a `Struct`/`Value`/`ListValue` or an `Any` at any
depth, or when the value itself was one of those. A single inner map therefore cost the whole value
its types: the engine guessed every leaf from the first document, so ranges, dates and geo queries
on the other fields were lost too. Typed maps inside typed map values were left for later.

Changing this changes the default mapping output, and with it the content hash. That is acceptable
only before 1.0.0, so it is done now.

## Decision

- The value message keeps its typed leaves, and every inner field follows the rule it follows
  anywhere else:
  - an inner `map` is `flattened` (sent as `flat_object` to OpenSearch; the translation in
    `JsonRestEngine` walks `dynamic_templates` too, and the read-back renames it again);
  - an inner `map` with `type: OBJECT` or `NESTED` is a typed map in its own right: its container is
    a template restricted to objects (`outer.*.inner`, `match_mapping_type: object`, mapping
    `{"type": "object" | "nested", "dynamic": true}`), and its values get templates with one more
    key wildcard (`outer.*.inner.*`, `outer.*.inner.*.leaf`). This recurses to any depth, bounded by
    `maxDepth` and the recursion guard like any message;
  - `Struct` is `flattened` (`MappingDefaults.structKind`);
  - `Value` and `ListValue` are `{"type": "object", "enabled": false}`, here and at the top level
    (see "Value and ListValue" below);
  - `Any`, `FieldMask` and `Empty`, which are left out of the mapping and the document elsewhere, are
    `{"type": "object", "enabled": false}` inside typed map values. The template has no
    `match_mapping_type`, so it also takes the string a `FieldMask` prints as; the engine keeps the
    value in `_source` without parsing it. Left out (pruned) instead, it would be lost on a round
    trip; left dynamic, its content would be mapped through the map's container. An `Any` cannot
    be printed without a type registry, so when no `CodecConfig.anyTypes` is configured the codec
    prunes an `Any` inside typed map values (a map of `Any` loses its entries), exactly as it prunes
    a top-level `Any`; with a registry it is written. This keeps the default codec from failing
    whole documents, at the price of the same data loss a top-level `Any` already has.
  - A value that is itself `Struct`, `Value` or `Any` follows the same rules (`attributes.*` =>
    `flattened`, `values.*` and `anys.*` => disabled object).
  - A disabled field takes no field options (`boost`, `copy_to`, `raw_mapping_json`, ...), and
    `FieldInfo.enabled()` is false for it, so the query compiler refuses every condition on it
    (an `exists` on a disabled object would silently never match).
- `IndexDefinition.resolve(path)` matches a concrete path segment by segment against the patterns:
  `*` matches exactly one key (non-empty, without `*`), other segments must be equal, and every `*`
  of the proto path takes its key. `typedMapValues` returns the innermost map a path runs through
  (for error messages), and `nestedAncestors` instantiates every nested pattern that prefixes the
  path (`outer.a`, `outer.a.inner`, `outer.a.inner.b` when all three are nested). The compilers
  already work through these three methods; a flattened field inside the values is found through
  `resolve`, so its keys (`outer.a.labels.env`) take the flattened-key operations, and is marked
  dynamic. Sorts and aggregations below a dynamic nested path stay refused, as in ADR 0010.
- The codec needed no new rule: it already converts values by pattern proto path
  (`outer.*.inner.*`), so int64 numbers, dates, geo points and the key check (blank keys and keys
  holding `.` or `*` throw `CodecException`) apply at every level. Its errors carry the concrete
  path (`outer.a.inner`) alongside.
- `DiffClassifier` keeps the ADR 0010 rule, keyed on the outermost map (`ep:P.*...`): an added
  template is applied in place when its map is added or the map's values were already typed, a
  changed one only when the map is added, and anything else (including an inner map switching
  between flattened and typed, or a rc.3 untyped map gaining templates) is a `REINDEX`.

## Template ambiguity

`path_match` treats `*` as "any characters, dots included", and the engine applies the first
template that matches (after `match_mapping_type`). With several wildcards a concrete path could in
principle match a template generated for a different field. The ordering of ADR 0010 (segment count
descending, then name) resolves every case, given two assumptions, both enforced:

- keys are non-empty and hold no `.` (the codec checks every level; it also refuses `*`);
- field names are literal: every non-key segment of a template is a field name holding no `*`,
  the only wildcard `path_match` knows on either engine (`Regex.simpleMatch`; `?` and other glob
  characters are plain characters there). Proto field names cannot hold `*`, and the mapper
  refuses an `elastic_field.name` or programmatic override holding `*` (or `.` or whitespace)
  everywhere, not only inside map values: renaming a typed map `counters` to `c*` would otherwise
  emit `ep:c*.*`, a two-segment template that matches `children.k` and, sorted by name, comes
  before `ep:children.*`.

1. Every template is generated from the schema tree: its segments are the literal names of the
   fields on the way down, with `*` exactly where a map's key sits and nowhere else. A concrete
   path the engine meets below a typed map is the same walk with each `*` replaced by one key, so
   its own template has exactly as many segments as the path.
2. A template with `n` segments contains `n - 1` literal dots, and `*` can only add characters, so
   it matches only paths of at least `n` segments; longer templates never match a shorter path.
3. Two different templates with the same segment count cannot both match one path: take the first
   segment where they differ. The segments before it are equal, so they name the same schema node,
   which is either a map (whose only child segment is `*`) or a message (whose child segments are
   field names). The differing segments are therefore two different field names, and a path segment
   cannot equal both. A key that equals a field name (outer key `metrics`, inner key `score`) does
   not change this: it occupies a `*` position, and at that position no template has a literal.
4. So among the templates that can match a path, its own template has the most segments and comes
   first; shorter templates (such as the value root `outer.*`, which matches every object below it)
   come later and are never reached.

The argument holds only while the engine never meets a path below a typed map that has no template
of its own length. That is true for leaves, containers, `flattened` and disabled fields (the engine
does not map anything inside the last two). It is false for a dynamic object inside the values: a
`Struct` mapped as `OBJECT` there lets documents bring arbitrary keys, and `outer.k.extra.score`
would be matched by `ep:outer.*.score` and typed as the value's `score` (or every object below it
made `nested` by a `NESTED` value root). Such a field is therefore not allowed:

- `Struct` with an explicit `type: OBJECT` or `NESTED` inside typed map values (including
  `value_type` on a map of `Struct`) throws `MappingException` asking for `FLATTENED`;
- `MappingDefaults.structKind(OBJECT)` there falls back to `flattened`, with a warning;
- an explicit type other than `IGNORE` on a `Value`/`ListValue`/`Any`/`FieldMask`/`Empty` there
  throws.

`raw_mapping_json` can still make a field inside the values dynamic; that is outside what the
mapper can check, and the README says so.

## Value and ListValue

The first version of this record mapped `Value` and `ListValue` like `Struct`, as `flattened`. A
review found, and the engines confirmed, that this rejects documents: a `Value` prints as any JSON
value and a `ListValue` as an array, and `flattened` (`flat_object` on OpenSearch) accepts only an
object or an array of objects. Elasticsearch 8.19.23 and 9.5.5 fail a bare string, number or
boolean and an array of scalars with "expecting token of type [START_OBJECT]"; OpenSearch 2.19.7
and 3.9.0 with "unexpected token [VALUE_STRING] in flat_object field value". The same failure
existed for top-level fields since the first release candidates, so the default changes everywhere:

- `Value` and `ListValue` map to `{"type": "object", "enabled": false}`. All four engines accept a
  scalar, an array of scalars, an array of objects, an object, `null`, `[]` and `{}` there, at the
  top level and through a dynamic template, and return it unchanged from `_source`. The fields are
  not searchable; the query compiler refuses conditions on them.
- An explicit `type` on a top-level `Value`/`ListValue` (`FLATTENED`, or `OBJECT` for a dynamic
  object) is kept for callers whose values are always objects, with a warning that other values
  are rejected. Inside typed map values an explicit type is refused, as for `Any`.
- `MappingDefaults.structKind` governs `Struct` only: a `Struct` always prints as an object, so
  `flattened` and `object` both fit it, while no field type other than a disabled object accepts
  every shape a `Value` can take.
- The content hash of indices with such fields changes, so they reindex once; this is listed as a
  breaking change before 1.0.0.

`InnerTypedMapsSpec` checks the argument by brute force: it instantiates every pattern of a mapping
with every field name as a key and runs first-match-wins over the generated templates, expecting each
path to land on its own template. The shared integration suite indexes documents whose keys equal
field names on Elasticsearch 8.19/9.x and OpenSearch 2.19/3.x and reads the resulting field types
back.

## Consequences

- Inner maps, `Struct` and `Any` no longer cost a typed map value its types; doubly keyed paths
  (`outer.a.inner.b`) take every operation of their type, and nested values at any level are wrapped
  with `ignore_unmapped: true`.
- Breaking for 1.0.0-rc.3 users: indices with such maps, or with `Value`/`ListValue` fields
  anywhere, get a new content hash and are reindexed once. Flattened maps and typed maps without these inner types keep their mapping and hash, which
  the existing goldens and a pinned rc.3 hash in `InnerTypedMapsSpec` confirm.
- Every distinct inner key adds fields at index time as well; the mapper warns per map pattern
  (`each distinct key of outer.*.inner adds ...`). Field-limit checks still cover only the declared
  properties, and nested fields inside nested values multiply per outer and inner key.
