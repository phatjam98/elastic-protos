# 0001. Engine-agnostic JSON core behind a SearchEngine SPI

- Status: Accepted
- Date: 2026-11-01

## Context

v1.0 must support Elasticsearch 8.x/9.x and OpenSearch 2.x/3.x. The two typed Java clients
(`elasticsearch-java`, `opensearch-java`) share an ancestry but have diverged packages, builders and
release cadences, and the typed builders change between majors. The pre-1.0 code was tied to one
client and mixed mapping, query building and client calls.

What elastic-protos produces (mappings, settings, query bodies, documents) is JSON in both engines,
and the REST APIs it needs are nearly identical. The known differences are small: the point-in-time
endpoints (`/{index}/_pit` vs `/{index}/_search/point_in_time`), the flattened field type
(`flattened` vs `flat_object`) and the availability of the `_shard_doc` tiebreaker.

## Decision

- `elastic-protos-core` emits and consumes plain `Map<String, Object>` JSON and depends on no engine
  client or framework.
- Engines are reached through one `SearchEngine` SPI. `JsonRestEngine` in core implements it over a
  minimal `RawHttp` transport and handles the engine differences, choosing by the product the
  cluster reports (`EngineInfo`).
- Each adapter module (`elastic-protos-elasticsearch`, `elastic-protos-opensearch`) only supplies the
  transport, built on the engine's official **low-level** REST client, plus a connection builder.
- The shared `EngineIntegrationSpec` must pass unchanged on every supported engine version; engine
  differences are fixed in core, never in tests.

## Consequences

- Adding or updating an engine is a small adapter, and the core is testable without Docker
  (`InMemorySearchEngine`, fake transports).
- Mappings always cross the SPI in Elasticsearch spelling, so content hashes and migration diffs are
  identical on both engines.
- Users who need APIs outside the SPI use their own client next to elastic-protos; there is no typed
  request model to fall back on.
- The adapters expose their REST client as an `api` (compile-scope) dependency, because
  `ElasticsearchEngine.of(RestClient)` / `OpenSearchEngine.of(RestClient)` put it in their public
  API.
