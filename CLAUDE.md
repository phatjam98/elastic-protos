# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

elastic-protos is an open-source Java 17 library that turns Protocol Buffer message definitions into
Elasticsearch/OpenSearch index mappings, migrates indices as the protos evolve, round-trips documents
between protobuf and JSON, and compiles a proto-defined query DSL into search requests. The core is
engine-agnostic; thin adapters talk to each engine through its official low-level REST client.

The roadmap to v1.0.0 lives in the owner's plan file (see memory). Milestones: M0 compile-green
skeleton, M1 core demoable, M2 both adapters + CI, M3 docs + 1.0.0-rc, M4 release.

## Modules

| Module | Purpose |
|---|---|
| `elastic-protos-core` | mapping, options, codec, engine SPI, migration, query (incl. `AggregationCompiler`, `HighlightCompiler`, `AggregationResults`, `Aggs`, `CompositePager`); owns all `.proto` files under `src/main/proto/phatjam98/elasticprotos/v1/` (`elastic_options`, `search_criteria`, `conditions`, `aggregations`, `flat_struct`, `sorting_criteria`, `pagination`, `geo`) |
| `elastic-protos-elasticsearch` | `SearchEngine` adapter on `elasticsearch-rest-client` (ES 8.x and 9.x) |
| `elastic-protos-opensearch` | `SearchEngine` adapter on `opensearch-rest-client` (OpenSearch 2.x and 3.x) |
| `elastic-protos-micronaut` | optional Micronaut 4 integration: config (`ElasticProtosConfiguration`), bean factories, startup bootstrap, `ElasticProtosService` (`search` -> `SearchPage`, `aggregate` -> `AggregationPage`) |
| `elastic-protos-testkit` | internal: example protos, golden fixtures, `InMemorySearchEngine`, shared engine integration spec (not published) |
| `elastic-protos-bom` | Maven BOM for the published modules |
| `examples/micronaut-sample` | runnable Micronaut example app; a standalone build, **not in settings.gradle**, consuming the published artifacts from `mavenLocal()` (`./gradlew publishToMavenLocal` first; `-PelasticProtosVersion=` to override). CI job `sample` builds it |
| `buildSrc` | convention plugins: `java-conventions`, `proto-conventions`, `publish-conventions`, `library-conventions` |

Java packages live under `com.phatjam98.elasticprotos.{options,mapping,codec,engine,migration,query,util}`.
Generated protobuf classes are in `com.phatjam98.elasticprotos.proto` and are excluded from
Checkstyle, JaCoCo and Javadoc.

## Build

```bash
./gradlew build                      # compile, checkstyle, unit tests, coverage gates, javadoc
./gradlew test                       # unit tests only (no Docker)
./gradlew :elastic-protos-elasticsearch:integrationTest -PengineImage=<docker image>   # Testcontainers (Docker)
./gradlew :elastic-protos-opensearch:integrationTest -PengineImage=<docker image>      # Testcontainers (Docker)
./gradlew :elastic-protos-testkit:demo --args="elasticsearch http://localhost:9200"   # or: opensearch <url>
./gradlew test -PtestJavaVersion=21  # run tests on JDK 21 (compilation always targets 17)
./gradlew showVersion
./gradlew stageAll                   # publish every module into build/staging-deploy for JReleaser
./gradlew jreleaserFullRelease --dryrun
```

- Versions are declared once in `gradle/libs.versions.toml`. buildSrc imports the same catalog.
- Java toolchain is 17 (`.sdkmanrc`). Tests run on 17 unless `-PtestJavaVersion` says otherwise.
- Protobuf stays on the 3.25 line on purpose: its generated code loads on 3.25+ and 4.x runtimes,
  and Micronaut 4 consumers are pinned to 3.25 by the Micronaut platform BOM.
- Checkstyle is enforced (`config/checkstyle/google_checks.xml`, Google Java Style, zero warnings).
  Checkstyle 12.x is the newest line that runs on Java 17.
- Coverage gates (JaCoCo lines, unit tests only): core 90%, adapters 70%, Micronaut 60%.
- Tests are Spock (Groovy) under `src/test/groovy`; Docker-backed tests go in `src/integrationTest/groovy`
  and never run from `./gradlew test`.
- `build` and `check` are Docker-free: they compile and checkstyle the `integrationTest` sources but do not
  run them. Run the `integrationTest` task of an adapter explicitly. Without `-PengineImage` the adapters
  default to the newest verified image (ES `9.5.5`, OpenSearch `3.9.0`; verified tags are listed in each
  adapter's build.gradle). The suites are skipped, not failed, when Docker is unavailable.
- Both adapter suites extend the shared `EngineIntegrationSpec` (testkit `src/main/groovy`), which must pass
  unchanged on ES 8.19/9.x and OpenSearch 2.19/3.x. Engine differences are handled in core, not in tests:
  `flattened` is sent as `flat_object` to OpenSearch and read back as `flattened` (JsonRestEngine), and the
  `_shard_doc` PIT tiebreaker is only used where `EngineInfo.supportsShardDoc()` (ES 7.12+, OpenSearch 3.3+).
- `demo` (testkit) bootstraps Firm/Document on a running cluster, indexes `SampleData`, runs FULL_TEXT
  "merger agreement" and prints the plan, the compiled query, ranked hits and the parsed best hit.
- JReleaser is applied once at the root; every module stages into the shared `build/staging-deploy`.
- Snapshots: `./gradlew stageAll jreleaserDeploy` (nexus2 deployer to the Central snapshots repo, no GitHub
  release). Releases: `./gradlew jreleaserFullRelease -Pversion=X.Y.Z` (Central Portal + GitHub release + tag).
  `-Pversion` overrides `gradle.properties` and must go on every Gradle call of a release run.
- Consumers get the core `.proto` files onto their protoc include path by a plain `implementation` dependency on
  `elastic-protos-core` (protobuf-gradle-plugin extracts them). Do not tell them to use the `protobuf`
  configuration: that recompiles the core protos and the well-known types into the consumer.

## CI (.github/workflows)

- `ci.yml` (push/PR to main, reusable via `workflow_call`): `static` (checkstyle main/test/integrationTest +
  javadoc), `unit` (`build` on JDK 17 and with `-PtestJavaVersion=21`), `unit-protobuf4` (unit tests of core,
  both adapters and testkit with `-PprotobufRuntimeVersion=<4.x>`, which forces protobuf-java/-util to that
  version on their `testRuntimeClasspath` only; compilation stays on 3.25; Micronaut is excluded to stay on the
  Micronaut-managed protobuf line), `integration`
  (ES 8.19.23, ES 9.5.5, OpenSearch 2.19.7, OpenSearch 3.9.0, with `-PrequireDocker=true` so a missing Docker
  fails instead of skipping), `coverage` (jacoco report + gates), `stage` (`stageAll verifyPublishedMetadata`,
  uploads the staged repo). Keep the integration matrix in step with the verified tags in the adapter build files.
- `stageAll` deletes `build/staging-deploy` first. `./gradlew stageAll verifyPublishedMetadata` is a separate
  command, not part of `check`/`build`; the CI `stage` job, `snapshot.yml` and `release.yml` run it. It fails
  when a staged `.module` has a `platform`/`enforced-platform` dependency in `apiElements`/`runtimeElements`, a
  POM imports a BOM other than our own, a POM dependency has no version, or the number of `.module`/`.pom` files
  differs from the published module count. Published modules never use `implementation`/`api platform(...)`:
  exported dependencies get explicit catalog versions (`jackson`), and the Micronaut runtime is `compileOnly`
  (the application provides it), so the Micronaut module exports only slf4j and elastic-protos-core.
- `-PprotobufRuntimeVersion` also passes `elasticprotos.protobufRuntime` to `test`; core's `ProtobufRuntimeSpec`
  then asserts the loaded `com.google.protobuf.RuntimeVersion` has that major version (skipped otherwise).
- Every published jar carries `Automatic-Module-Name: com.phatjam98.elasticprotos.<module>` (library-conventions).
  `util`, `engine.rest` and `query.NestedPathResolver` are internal (not public API, may change in minors).
- `snapshot.yml` (push to main): reuses ci.yml, then `stageAll` + `jreleaserDeploy` when the version is a SNAPSHOT.
- `release.yml` (tag `v*`, or manual dispatch with `version` and `dryrun`, default dry run): tag must match
  `vX.Y.Z` or `vX.Y.Z-rc.N` and be on `origin/main`; runs `build` and `jreleaserFullRelease` with `-Pversion`.
  Dispatch also takes `release_only`: recovery when Central succeeded but the GitHub release failed; it checks
  out the tagged commit and runs `jreleaserRelease` (never redeploys; needs the existing tag, not with dryrun).
- `codeql.yml` (Java, manual build) and `dependabot.yml` (`build(deps)` / `ci(deps)` prefixes, which the
  changelog excludes along with `chore`, `docs`, `test` and merge commits).
- The GitHub release uses the automatic `GITHUB_TOKEN` (no PAT secret). Required secrets: `JRELEASER_MAVENCENTRAL_USERNAME`, `JRELEASER_MAVENCENTRAL_PASSWORD`,
  `JRELEASER_NEXUS2_USERNAME`, `JRELEASER_NEXUS2_PASSWORD` (Central Portal token for both), `JRELEASER_GPG_PUBLIC_KEY`,
  `JRELEASER_GPG_SECRET_KEY`, `JRELEASER_GPG_PASSPHRASE`.

## Conventions

- Commits follow Conventional Commits (the changelog is generated from them).
- The core module must not depend on any engine client or framework. Mappings, queries and documents
  cross the engine SPI as plain `Map<String, Object>` / JSON.
- Mapping behaviour is configured by proto options in `elastic_options.proto` first, programmatic
  overrides second. Do not reintroduce field-name heuristics.
- Default mapping rules: `string` => `keyword` (opt in to `TEXT`), singular message => `object`,
  repeated message => `nested`, `Timestamp` => `date`, `map<K,V>` => `flattened` (`type: OBJECT` opts in
  to a typed map: dynamic object plus `dynamic_templates` from the value type, ADR 0010). Documents carry proto3 default values (`CodecConfig.includeDefaults` is true).
  Index settings, including analysis, are outside the content hash (ADR 0006).
- Migration actions: `CREATE`, `NO_OP`, `PUT_MAPPING`, `UPDATE_SETTINGS`, `BACKFILL`, `REINDEX`, `BLOCKED`.
  Only proto-declared settings are compared; dynamic ones (`DiffClassifier.DYNAMIC_SETTINGS` /
  `DYNAMIC_SETTING_PREFIXES` plus `MigrationOptions.extraDynamicSettings`) are applied in place
  (`UPDATE_SETTINGS`, or before `PUT_MAPPING`/`BACKFILL`); static ones warn or reindex into the next
  generation per `StaticSettingsPolicy`. New indices record a static-settings digest in
  `_meta.elastic_protos.static_settings`.
- `BACKFILL` puts the mapping and runs `_update_by_query` for `MappingDiff.unindexedAdditions` (added
  multi-fields, typed properties under a dynamic object).
- `REINDEX` defaults to `ReindexMode.ZERO_DOWNTIME` (ADR 0007): prepare (seq_no checkpoint), copy, catch up
  (external versioning), probe (scripts), snapshot (PIT), swap, freeze (`_block/write` after `swapGrace`),
  reconcile (hot pass then cold, conditional writes), sweep, finish. The phase lives in
  `_meta.elastic_protos.pending` on the new index (backfills use it too); `plan` reads the pending
  marker into `IndexPlan.resume` and `apply` continues from there. `BLOCKING` is the old write-blocking copy. Rollback: `RollbackMode`
  (`REVERSE_DELTA` default, `REVERSE_DELTA_FORCE`, `ALIAS_ONLY`).
- `apply` and `rollback` take a per-alias lock (ADR 0008, `AliasLock`) in `IndexNamingStrategy.lockIndex()`
  (`elastic-protos-locks`, prefixed by `prefixed(...)`), re-plan under it and check the lease before
  irreversible steps. `MigrationOptions.lock` / `lockWait` (10m) / `lockTtl` (2m); `Status.LOCKED`,
  `LockException`. The migrator writes the whole `_meta` on every update, since the engines replace it.
- Query DSL: `SearchCriteria.aggregation` / `highlight` (ADR 0009) compile through `AggregationCompiler`
  and `HighlightCompiler`; nested/reverse_nested wrappers named `<name>__nested` / `<name>__reverse`.
  Sub-aggregations nest down to `QueryCompilerConfig.maxAggregationDepth` (default 3, top level
  included); `maxAggregationBuckets` (default 0, off) guards the product of requested bucket counts.
  `filter` / `filters` / `composite` (ADR 0012): conditions are `repeated SearchCondition` (OR of
  ANDs, `ConditionCompiler.compileAny`, as the query; lenient skip of a predicate skips the whole
  filter/filters), compiled by
  `ConditionCompiler` with the aggregation's nested context entered (`NestedPathResolver` context:
  context paths not re-wrapped, fields outside refused); a filter with a nested path as `field`
  filters those nested objects. Composite is top level only, sources in one nested context, paged
  by `CompositePager` (compiles once at open; stops at a page without buckets or `after_key`;
  throws on a repeated `after_key`; `track_total_hits: false`). The condition messages live
  in `conditions.proto` (publicly imported by `search_criteria.proto`, which `aggregations.proto`
  cannot import: cycle). The adapters parse responses into key-sorted maps, so keyed buckets and
  composite keys come sorted by key. `RangeValue.min`/`max` are `optional` (open bounds).
- `InMemorySearchEngine` (testkit) runs only terms, filter, filters (conditions of `match_all`,
  `bool`, `term`, `terms`, `exists`, `nested`), the six metrics, `nested` and `reverse_nested`
  aggregations at any depth, refuses `composite` (BAD_REQUEST), ignores `highlight`, and models
  seq_no, tombstones and PIT snapshots like the engines. `EngineIntegrationSpec` has 55 feature
  methods.
