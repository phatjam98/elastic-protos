# 0013. Highlighting nested text fields through inner hits

- Status: Accepted
- Date: 2026-11-10

## Context

[ADR 0009](0009-aggregation-and-highlight-dsl.md) left text fields inside nested objects out of
highlighting. Documents with nested parties, offices or line items are where full-text matches
often are, so callers need those fragments too.

A search-level `highlight` never returns fragments of nested fields: the engines highlight a
nested field only within the inner hits of a `nested` query. Verified on Elasticsearch 8.19.23 and
9.5.5 and OpenSearch 2.19.7 and 3.9.0:

- an `inner_hits` with a `highlight` on `nested { path: parties }` returns, per matching party,
  `_nested { field, offset }` and the fragments, under the inner hits' name, with `hits.total` the
  number of matching parties; at most `size` (3 by default) are returned;
- by default an inner hit highlights with its own nested query only, so two OR branches on one path
  mark up different terms of the same object; a `highlight_query` on the field overrides that;
- inner hits names must be unique within a request (the default name is the path);
- an `inner_hits` on a `nested` query inside another `nested` query is returned only when the
  outer query has an `inner_hits` too, inside the outer inner hits, with a chained `_nested` whose
  child field is relative and may contain dots (`{field: sections, offset: 0, _nested: {field:
  meta.annotations, offset: 1}}` for a nested array under a plain object);
- inner hits return `_source` unless told not to; `size` above `index.max_inner_result_window`
  (100 by default) is refused.

## Decision

- **Inner hits on the query's own clauses.** The compiler attaches an `inner_hits` with the
  highlight settings to every positive `nested` clause (in `filter`, `must` or `should`, not
  `must_not`) whose path is the innermost nested path of a highlighted field. Several clauses on
  one path each get one, named `<path>__highlight`, `<path>__highlight2`, .... Every highlighted
  field gets a `highlight_query` that ORs the queries of all positive clauses on its path, so an
  object marks up the same terms whichever clause returned it. A `nested` clause that only encloses
  such a clause gets an `inner_hits` without highlight so the inner one is returned. Inner hits ask
  for no `_source`; `Highlight.nested_objects` (field 9) sets their size at every level and is at
  most `QueryCompilerConfig.maxInnerResultWindow` (default 100). Fields outside nested objects
  stay in the search-level `highlight`, which is left out when only nested fields are
  highlighted. A search without hits compiles no highlighting.
- **No clause that can mark the field up: refuse.** A requested nested field is highlighted only
  when a positive clause on its path holds a full-text query (match, phrase, multi-match) on that
  field, or on any field of the path when `require_field_match` is false. Otherwise the field is a
  `QueryCompileException`, a warning in lenient mode: a keyword `term` or an `EXISTS` on the path
  would pay for inner hits and never return fragments. The engines could highlight such a field
  with text from elsewhere (a `match_all` nested clause with `boost: 0` and a `highlight_query`
  returns fragments without changing scores), but there is no query text on that path to
  highlight with, and choosing one is a separate feature. Refusing now keeps it open: turning a
  refusal into a result later is additive, the reverse is not.
- **Defaults stay outside nested objects; `include_nested` opts in.** With `Highlight.fields`
  empty, the default set remains the full-text text fields outside nested objects, so existing
  criteria compile byte-for-byte as before and do not start fetching inner hits.
  `Highlight.include_nested` (field 8) adds the nested full-text text fields that a clause can mark
  up; others are skipped silently, since nothing was asked for by name. Patterns of typed maps
  inside nested values (`folders.*.pages.text`) are refused: their nested path differs per key.
- **Typed results in one extensible holder.** `Hit` gains a tenth component,
  `NestedHighlights nestedHighlights`, a final class (not a record) so that more inner hits data
  can be added later without another component. It holds `objects()`, one
  `NestedHighlight(levels, highlight)` per nested object with fragments, in document order, and
  `totals()`, one `NestedTotal(parent, path, matched, returned, truncated)` per nested path and
  parent object for every level with inner hits, including carrier levels, so a caller can tell
  where the per-clause limit cut objects off. `levels` are `NestedLevel(field, offset)` exactly as
  the engines report them, keeping the level boundaries a dotted path loses; `path()` and
  `offsets()` are derived. The adapters read every level of inner hits; an object returned by
  several clauses is one entry whose fragments are taken from the first inner hit that has them
  (they agree, thanks to the shared highlight query), and an inner hit without a field or numeric
  offset in `_nested` is skipped rather than placed at offset 0. `Hit.highlight` keeps its meaning
  (fields outside nested objects); `Hit.fragments(field)` reads either. The nine- and
  five-argument constructors remain; `SearchPage` is unchanged.

## Consequences

- The wire format grows by `Highlight.include_nested` and `Highlight.nested_objects`; criteria
  without them compile as before, and the goldens that existed before are unchanged.
- A highlighted nested field needs a full-text condition on it inside its path; the error says so.
- Inner hits add per-hit work on the engine; `nested_objects` bounds it, and `NestedTotal` reports
  truncation instead of hiding it. `matched` is the largest count of one clause, a lower bound
  when several clauses match different objects.
- The in-memory engine still ignores `highlight` and does not run `nested` queries.
- Record patterns over `Hit` with nine components no longer compile; constructor calls do.
