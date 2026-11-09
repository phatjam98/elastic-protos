# 0009. Aggregation and highlight DSL in the proto query model

- Status: Accepted
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
  and optional `from`/`to`, and dates are epoch milliseconds, as in `NUM_RANGE`.
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
  supplies empty results.
