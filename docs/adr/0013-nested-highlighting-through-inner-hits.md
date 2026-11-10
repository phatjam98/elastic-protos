# 0013. Highlighting nested text fields through inner hits

- Status: Accepted
- Date: 2026-11-10

## Context

[ADR 0009](0009-aggregation-and-highlight-dsl.md) left text fields inside nested objects out of
highlighting. Documents with nested parties, offices or line items are where full-text matches
often are, so callers need those fragments too.

A search-level `highlight` never returns fragments of nested fields: the engines highlight a
nested field only within the inner hits of a `nested` query, using that query's own clauses as
the highlight query. Verified on Elasticsearch 8.19.23 and 9.5.5 and OpenSearch 2.19.7 and 3.9.0:

- an `inner_hits` with a `highlight` on `nested { path: parties }` returns, per matching party,
  `_nested { field, offset }` and the fragments, under the inner hits' name on the hit;
- inner hits names must be unique within a request (the default name is the path);
- an `inner_hits` on a `nested` query inside another `nested` query is returned only when the
  outer query has an `inner_hits` too, inside the outer inner hits, with a chained `_nested`
  (`{field: boxes, offset: 0, _nested: {field: notes, offset: 1}}`, child fields relative);
- inner hits return `_source` unless told not to, and at most 3 objects per hit by default.

## Decision

- **Inner hits on the query's own clauses.** The compiler attaches an `inner_hits` with the
  highlight settings to every positive `nested` clause (in `filter`, `must` or `should`, not
  `must_not`) whose path is the innermost nested path of a highlighted field. Several clauses on
  one path (OR branches, separate groups) each get one, named `<path>__highlight`,
  `<path>__highlight2`, ...; a `nested` clause that only encloses such a clause gets an
  `inner_hits` without highlight so the inner one is returned. Inner hits ask for no `_source`;
  `Highlight.nested_objects` (new field 9) sets their size. Fields outside nested objects stay in
  the search-level `highlight`, which is left out when only nested fields are highlighted.
- **No clause on the path: refuse.** The alternative, adding a scoring-neutral clause such as
  `should: nested { path, match_all, inner_hits }`, cannot produce fragments: the inner hits
  would highlight with `match_all`, which marks nothing, and nothing in the query targets the
  nested fields anyway (with `require_field_match`, the default, only queried fields are
  highlighted). It would also return inner hits for every matching document and change scoring
  unless boosted to zero. So an explicitly requested nested field whose path no positive clause
  targets is a `QueryCompileException`, a warning in lenient mode.
- **Defaults stay outside nested objects; `include_nested` opts in.** With `Highlight.fields`
  empty, the default set remains the full-text text fields outside nested objects, so existing
  criteria compile byte-for-byte as before and do not start fetching inner hits.
  `Highlight.include_nested` (new field 8) adds the nested full-text text fields for the paths the
  query has a clause on; paths without one are skipped silently, since nothing was asked for by
  name.
- **Typed results without breaking `Hit`.** `Hit` gains a tenth component, `nestedHighlights`, a
  list of `NestedHighlight(path, offsets, highlight)`: one entry per nested object, with the full
  dotted path (`boxes.notes`), the array position at each level and the fragments keyed by full
  field path. The adapters read every level of inner hits, merge the fragments of one object
  returned by several inner hits (deduplicated) and order the entries by path and offsets.
  `Hit.highlight` keeps its meaning (fields outside nested objects); `Hit.fragments(field)` reads
  either. The nine- and five-argument constructors remain; `SearchPage` is unchanged, its hits
  carry the new component.

## Consequences

- The wire format grows by `Highlight.include_nested` and `Highlight.nested_objects`; criteria
  without them compile as before, and the existing goldens are unchanged.
- A highlighted nested field now needs a condition on its path; the error says so.
- Inner hits add per-hit work on the engine; `nested_objects` bounds it, and the engines refuse
  more than `index.max_inner_result_window` (100 by default).
- The in-memory engine still ignores `highlight` and does not run `nested` queries.
- Record patterns over `Hit` with nine components no longer compile; constructor calls do.
