# 0009. Aggregation and highlight DSL in the proto query model

- Status: Accepted; the aggregation kinds and the sub-aggregation depth are amended by [0011](0011-filter-composite-and-deeper-aggregations.md)
- Date: 2026-11-09

## Context

The query DSL compiled conditions, sorting and paging; aggregations and highlighting had to be
added to a copy of the compiled body by hand, and their results read from raw maps. That lost
what the DSL gives everywhere else: validation against the mapped field types, `.keyword`
sub-fields for text, automatic `nested` wrapping, and one wire format for API clients. Aggregations
on nested fields are where hand-written bodies most often go wrong: the field must be reached with
`nested` aggregations and left again with `reverse_nested`, and the response nests the results
inside those wrappers.

The DSL must stay engine-agnostic (plain maps on both Elasticsearch and OpenSearch), small enough
to validate completely, and readable without knowing how the compiler laid the body out.

## Decision

`SearchCriteria` gains `repeated Aggregation aggregation = 4` and `Highlight highlight = 5`
(`aggregations.proto`). An `Aggregation` is one of `terms`, `date_histogram`, `histogram`, `range`
or a single-value `metric` (`MIN`, `MAX`, `AVG`, `SUM`, `VALUE_COUNT`, `CARDINALITY`), on one
field, with one level of sub-aggregations. Pipeline, filter, composite and multi-value metric
aggregations are left out of v1.

- **Wrappers are found by name, not by layout metadata.** The compiler wraps an aggregation on a
  nested field in `<name>__nested` (`__nested2`, ... per extra level) and `<name>__reverse`
  aggregations, and user names may not contain `__`. `AggregationResults` unwraps them by that
  naming convention, so reading needs nothing but the response: no layout map has to travel from
  the compiler to the reader, results can be read from a stored or forwarded response, and `meta`
  stays free. Names that equal keys of bucket objects (`key`, `doc_count`, `buckets`, `value`, ...)
  are reserved, so the sub-results of a bucket are the bucket minus those keys.
- **`RangeBucket`, not `RangeValue`.** Engine range buckets are always `[from, to)`; `RangeValue`
  carries per-bound inclusivity that the engines could not honour. `RangeBucket` has an optional key
  and two optional bounds, each a `oneof` of a `double` (`from`/`to`) and a
  `google.protobuf.Timestamp` (`from_time`/`to_time`, date fields only). A `double` is exact on
  long fields only up to 2^53; that limit is documented rather than worked around, since such
  values are rare in range buckets and `NUM_RANGE` has the same one. Dates accept a `Timestamp`
  because it is the type the documents carry and avoids unit mistakes; doubles of epoch
  milliseconds stay accepted, as in `NUM_RANGE`. The compiler sends epoch milliseconds either way,
  with `format: epoch_millis`: an unambiguous, time-zone-free number both engines parse identically,
  whereas a formatted date string would depend on the field's `format`.
- **Room to grow.** Metrics are a `MetricAggregation { type }` message, not a bare enum, so options
  such as `missing`, `precision_threshold` or `format` can be added without a wire break;
  `TermsAggregation.order` is repeated (compound orders) and `Highlight.pre_tags`/`post_tags` are
  repeated, matching the engines' arrays. `min_doc_count` fields are `int32`, as in the engines.
- **Name rules.** Besides `__` and the bucket keys, names may not contain `[`, `]` or `>` (the
  engines reject them) or `.` (it would break `BY_METRIC` order paths), nor end with `_`, so that
  `AggregationResults` can strip a wrapper suffix (`__nested\d*` or `__reverse`) unambiguously.
- **Aggregations only via a negative page size.** `Pagination.size < 0` already compiled to
  `size: 0`; `Criteria.aggregationsOnly()` and `ElasticProtosService.aggregate` use it rather than a
  new field.
- Highlighting covers text fields outside nested objects (nested fields would need `inner_hits`);
  by default it highlights the index's full-text fields that qualify.
- `QueryCompilerConfig.maxTermsSize` (default 1000) caps terms sizes requested by API clients.
- `SearchPager` sends the aggregations with the first page only.

## Consequences

- Aggregations are validated like conditions: field kinds, `keyword_subfield`, flattened keys
  (terms, value count and cardinality only), interval and time-zone syntax, empty ranges, names,
  depth and terms ordering by a metric sub-aggregation, reached through the wrappers
  (`firms__reverse>firms`).
- The `__` restriction and the reserved names are a constraint on user names; both are checked at
  compile time with a clear error.
- Nested paths inside typed map values are refused: the engines have no `ignore_unmapped` for
  `nested` aggregations, and such paths exist only once a key is used.
- The wire format grows by two fields; criteria without them compile exactly as before.
- `SearchPage` gains an `aggregations` component; the previous six-component constructor stays and
  supplies empty results. `ElasticProtosService.aggregate` returns an `AggregationPage` (results,
  total, warnings), so the hit count and compiler warnings are not lost.
- Buckets in a nested context count nested objects; counting documents takes a `reverse_nested`
  metric. Nested terms can exceed the engines' `search.max_buckets` even within `maxTermsSize`.
