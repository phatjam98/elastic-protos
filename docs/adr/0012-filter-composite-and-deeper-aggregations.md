# 0012. Filter, filters and composite aggregations, and deeper sub-aggregations

- Status: Accepted
- Date: 2026-11-10

## Context

[ADR 0009](0009-aggregation-and-highlight-dsl.md) left filter, composite and pipeline
aggregations out and allowed one level of sub-aggregations. The first application built on the
library needs conditional counts ("filings that name a plaintiff"), every bucket of a
high-cardinality combination (paging) and three-level breakdowns, and the proto shape freezes at
1.0.0, so the additions must be wire-compatible: new fields and `oneof` members only.

A filter condition should behave exactly like a query condition (typing, `.keyword` sub-fields,
nested wrapping, lenient skipping), which means reusing `SearchCondition` and `ConditionCompiler`.
`SearchCondition` was declared in `search_criteria.proto`, which imports `aggregations.proto`, so
`aggregations.proto` could not import it back: protobuf forbids import cycles.

Both engines (verified on Elasticsearch 8.19.23 and 9.5.5 and OpenSearch 2.19.7 and 3.9.0):

- evaluate a `filter` aggregation under a `nested` aggregation against the nested objects, so a
  plain `term` on `parties.role` tests one party, while a `nested` query on the path already
  entered returns wrong counts, and a `nested` query on a deeper path works as expected;
- sort the buckets of a keyed `filters` aggregation by key and put the other bucket last;
- accept `composite` only at the top level or directly under `nested`, return JSON booleans and
  `null` (for `missing_bucket`) in its keys, return an `after_key` with every non-empty page (also a
  short last one), and return neither buckets nor an `after_key` once there is nothing more.

## Decision

- **The condition messages move to `conditions.proto`.** `SearchCondition`, `FieldCondition`,
  `FullTextOptions`, `FieldBoost` and the enums `FullTextOperator`, `FullTextType` and
  `SearchOperationType` keep their package, full names, field numbers and generated Java classes;
  `search_criteria.proto` imports the new file with `import public`, so protos that import
  `search_criteria.proto` still see them, and the binary and JSON encodings are unchanged. Only
  their file descriptor changes: code that looked them up through
  `SearchCriteriaProtos.getDescriptor()` finds them in `ConditionProtos.getDescriptor()` now.
- **`Aggregation.kind` gains `filter = 9`, `filters = 10` and `composite = 11`.** `FilterAggregation
  { repeated SearchCondition condition }`; `FiltersAggregation { repeated KeyedFilter filters;
  string other_bucket_key }` with `KeyedFilter { key, repeated SearchCondition condition }`. A
  condition is an OR of ANDs, with the shape and compilation of `SearchCriteria.search_condition`,
  so a filter can say "type = X OR status = Y"; a single `SearchCondition` could not, and a fluent
  `or()` would have had nowhere to go. At least one is required. The builders pass every branch
  of a `Criteria` (`Criteria.conditions()`), so no fluent chain drops one; `CompositeAggregation { repeated
  CompositeSource sources; int32 size; map<string, PrimitiveValue> after }` with `CompositeSource
  { name, field, oneof kind { CompositeTermsSource terms, CompositeDateHistogramSource
  date_histogram, CompositeHistogramSource histogram }, SortingOrder order, bool missing_bucket }`.
  A `PrimitiveValue` without a kind in `after` is null. The composite source messages are their
  own types rather than the bucket aggregations' messages, whose `min_doc_count`, `extended_bounds`
  and `offset` the engines do not accept on sources.
- **Filters run in a nested context, chosen by `Aggregation.field`.** Without a field a filter runs
  in its parent's context (the root documents at the top level); with a nested path as field it is
  wrapped like any aggregation on that path and filters its nested objects. The condition is
  compiled by `ConditionCompiler` with the context already entered: fields in the context are
  tested directly, deeper nested fields get `nested` queries for the remaining levels, and fields
  outside the context are refused, since the engines cannot test them there. `FULL_TEXT` without
  fields uses the full-text fields inside the context, and `EXISTS` on a path the context has
  entered is `match_all`. Sub-aggregations run in the filter's context.
- **Lenient filters are skipped whole.** When lenient mode drops a predicate of a filter's
  condition (an unknown field), the whole filter aggregation is skipped with a warning, and a
  `filters` aggregation is skipped if any of its conditions lost one: a widened bucket would be a
  plausible but wrong count, and a missing keyed bucket would change the other bucket. This
  matches unknown-field aggregations and composites with an unknown source.
- **Root fields from a nested context: deferred.** Reusing `Aggregation.field` for the nested path
  leaves no spelling for "step out to the root (or an ancestor) first". That is additive later: a
  new field on `FilterAggregation` and `FiltersAggregation` (for example `string reverse_path`,
  empty meaning none, with a flag or sentinel for the root) would wrap the filter in a
  `reverse_nested` named `<name>__reverse`, which `AggregationResults` already unwraps. Nothing in
  the current fields or their meanings has to change for it, so it is left out of 1.0.
- **Composite is top level only.** Its sources must share their nested paths; it is then wrapped
  in `<name>__nested` aggregations as usual. Mixing nested paths would need a different context
  per source, which the engines cannot express, and composite under a bucket aggregation is refused
  by the engines, so both are refused at compile time with an explanation. An unknown source field
  skips the whole composite in lenient mode: dropping one source would change the keys and
  invalidate after keys. Its `size` is capped by `maxTermsSize`. Source names follow the
  character rules of aggregation names (no `__`, `[`, `]`, `>`, `.`, no trailing `_`); they can
  be relaxed later, not tightened.
- **Reading.** `AggregationResults.bucket(name)` reads single-bucket results (filter);
  `buckets(name)` reads filters and composite buckets too, composite keys being a `Map` of source
  name to value (`Bucket.compositeKey()`); `afterKey(name)` returns the composite's `after_key`.
  A composite is recognised by its `after_key` or its buckets' map keys; a page without buckets
  looks exactly like an empty histogram, so `afterKey` refuses it and callers check `buckets`
  first.
  Kind mismatches stay `IllegalStateException` and unknown names `NoSuchElementException`. The
  adapters parse responses into key-sorted maps, so keyed filters buckets and composite keys come
  sorted by key, the other bucket included; callers should look buckets up by key.
- **Paging.** `CompositePager` compiles its request once when opened, so invalid criteria and a
  composite skipped by a lenient compiler fail there rather than in `hasNext()`, and only that
  request is compiled (siblings the pager never sends are not checked). It sends only the named
  composite (no hits, no highlight, `track_total_hits: false`), feeds each `after_key` into the
  next request and stops at the first page without buckets or `after_key`; an engine that returns
  the after key it was sent makes it throw instead of looping. It
  does not stop at a short page, since the engines do not promise that, which costs one request.
  No point in time is opened: a composite page is a single search, and consistency across pages
  is not promised. `ElasticProtosService.composite(criteria, type, name)` returns one.
- **Depth.** `QueryCompilerConfig.maxAggregationDepth` (default 3, top level included;
  `elastic-protos.query.max-aggregation-depth`) replaces the single level. Nothing else assumed
  one level: wrapper names are unique per level because sibling names are, `BY_METRIC` paths are
  built per level from the emitted wrapper names, and `AggregationResults` unwraps by name at any
  depth. `BY_METRIC` still names a direct metric sub-aggregation.
- **Bucket guard.** `QueryCompilerConfig.maxAggregationBuckets` (`max-aggregation-buckets`),
  default 0 (off), refuses aggregation paths whose requested bucket counts multiply beyond it:
  terms and composite `size` (10 when unset), range buckets and filters (plus the other bucket);
  histograms count one, since their bucket count depends on the data. It is off by default because
  it is a worst-case estimate that would refuse criteria the engines accept (such as two nested
  terms of 1000 over sparse data); the engines' `search.max_buckets` still applies. It runs on
  the compiled aggregations, after validation and lenient skipping, so only what is sent counts.
- Pipeline aggregations stay out of scope.

## Consequences

- Criteria without the new kinds compile byte-for-byte as before (the existing goldens are
  unchanged). A criteria that used three levels was refused before and now compiles.
- The in-memory engine runs `filter` and `filters` (with `match_all`, `bool`, `term`, `terms`,
  `exists` and `nested` conditions) and any depth, and refuses `composite` with `BAD_REQUEST`.
- Composite keys lose their source order in the parsed response; the order of the buckets
  themselves is the engine's.
- Messages of a few existing errors changed slightly ("sets no kind" lists the new kinds; the
  depth error names `maxAggregationDepth`).
