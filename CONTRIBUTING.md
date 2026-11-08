# Contributing to elastic-protos

Thanks for helping. This guide covers the development setup, the checks a change must pass and how
pull requests are reviewed. By participating you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).

## Development setup

- **JDK 17.** The toolchain is pinned in [`.sdkmanrc`](.sdkmanrc) (`sdk env install` with SDKMAN!).
  Gradle's toolchain support can also provision it. Compilation always targets 17.
- **Docker**, only for the integration tests. They use Testcontainers and are skipped (not failed)
  when Docker is unavailable. On Linux, engines need `sudo sysctl -w vm.max_map_count=262144`.
- Use the Gradle wrapper (`./gradlew`); dependency versions live in `gradle/libs.versions.toml`.

## Project layout

| Module | Contents |
|---|---|
| `elastic-protos-core` | Mapping, options, codec, engine SPI, migration, query; all `.proto` files under `src/main/proto/phatjam98/elasticprotos/v1/` |
| `elastic-protos-elasticsearch` | Adapter on `elasticsearch-rest-client` |
| `elastic-protos-opensearch` | Adapter on `opensearch-rest-client` |
| `elastic-protos-micronaut` | Micronaut 4 integration |
| `elastic-protos-testkit` | Internal: example protos, sample data, `InMemorySearchEngine`, the shared `EngineIntegrationSpec`, the `Demo` |
| `elastic-protos-bom` | Maven BOM |
| `buildSrc` | Convention plugins |

The core must not depend on any engine client or framework: mappings, queries and documents cross
the engine SPI as plain `Map<String, Object>`. Engine differences are handled in core
(`JsonRestEngine`), never in tests. Mapping behaviour is configured by proto options first and
programmatic overrides second; do not add field-name heuristics.

## Build and test

```bash
./gradlew build                      # compile, Checkstyle, unit tests, coverage gates, Javadoc (no Docker)
./gradlew test                       # unit tests only
./gradlew test -PtestJavaVersion=21  # run the tests on JDK 21
./gradlew :elastic-protos-core:test --tests '*QueryCompilerSpec'

# Docker-backed integration tests, per adapter; without -PengineImage the newest verified image is used
./gradlew :elastic-protos-elasticsearch:integrationTest -PengineImage=docker.elastic.co/elasticsearch/elasticsearch:8.19.23
./gradlew :elastic-protos-opensearch:integrationTest -PengineImage=opensearchproject/opensearch:2.19.7

# End-to-end demo against a running cluster
./gradlew :elastic-protos-testkit:demo --args="elasticsearch http://localhost:9200"
```

CI runs the integration suites on Elasticsearch 8.19.23 and 9.5.5 and OpenSearch 2.19.7 and 3.9.0.
A change to engine behaviour should pass locally on at least one image of each engine.

## Quality gates

- **Checkstyle, zero warnings.** Google Java Style (`config/checkstyle/google_checks.xml`) with
  `maxWarnings = 0`; generated protobuf code is excluded. Run
  `./gradlew checkstyleMain checkstyleTest checkstyleIntegrationTest`.
- **Coverage.** JaCoCo line coverage gates: core 90%, adapters 70%, Micronaut 60%. Integration tests
  do not count.
- **Javadoc** must build cleanly for public API (`./gradlew javadoc`).

## Tests

- Tests are [Spock](https://spockframework.org/) specifications in Groovy, named `*Spec`, under
  `src/test/groovy`. Use `given:` / `when:` / `then:` / `expect:` / `where:` blocks with descriptive
  feature names (`def "a failed reindex unblocks the old index"()`).
- Unit tests need no Docker. Use the testkit's `InMemorySearchEngine` or a fake transport instead
  of a real cluster.
- Docker-backed tests go in `src/integrationTest/groovy` and never run from `./gradlew test`.
  Engine-facing behaviour belongs in the shared `EngineIntegrationSpec`
  (`elastic-protos-testkit/src/main/groovy`), which both adapter suites extend and which must pass
  unchanged on every engine in the matrix.
- Use the example protos (`elastic-protos-testkit/src/main/proto/phatjam98/elasticprotos/example/`:
  `Firm`, `Document`, `InboxItem`) or add focused test protos under
  `elastic-protos-core/src/test/proto`.

### Golden files

Mapping and query output is checked against golden JSON under
`elastic-protos-core/src/test/resources/golden/` (`mapping/` and `query/`). After an intended change,
regenerate them and review the diff before committing:

```bash
./gradlew :elastic-protos-core:test -PupdateGolden=true
git diff elastic-protos-core/src/test/resources/golden
```

A changed mapping golden means a changed content hash, so existing indices would be migrated. Call
that out in the pull request.

## Commit messages

Commits follow [Conventional Commits](https://www.conventionalcommits.org/); the release changelog
is generated from them.

```text
<type>(<optional scope>): <summary>

<optional body>

<optional footer, e.g. BREAKING CHANGE: ...>
```

| Type | Use for | In the changelog |
|---|---|---|
| `feat` | A new feature | Yes |
| `fix` | A bug fix | Yes |
| `perf` | A performance improvement | Yes |
| `refactor` | Code change without behaviour change | Yes |
| `docs` | Documentation only | Hidden |
| `test` | Tests only | Hidden |
| `build` | Build, dependencies (Dependabot uses `build(deps)`) | Hidden |
| `ci` | CI workflows (Dependabot uses `ci(deps)`) | Hidden |
| `chore` | Anything else that does not touch shipped code | Hidden |

`style` and `revert` are accepted and also appear in the changelog. The hidden types are excluded
by `jreleaser.yml` (`excludeLabels` and `hide.categories`), as are bot contributors.

Mark breaking changes with `!` after the type (`feat!: ...`) or a `BREAKING CHANGE:` footer. Scopes
are optional; module names work well (`fix(opensearch): ...`). Merge commits are left out of the
changelog.

## Pull requests

1. Open an issue first for anything larger than a small fix, so the approach can be agreed.
2. Branch from `main`, keep the change focused, and add or update tests and documentation
   (README, Javadoc, ADRs for design decisions under `docs/adr`).
3. Run `./gradlew build` and, for engine-facing changes, the relevant `integrationTest` tasks.
4. Open the pull request against `main` and fill in the template. CI (static checks, unit tests on
   JDK 17 and 21, integration tests on all four engines, coverage, staging) must be green.
5. A maintainer reviews it (see `.github/CODEOWNERS`). Give the pull request a Conventional Commit
   title too: if it is squash-merged, the title becomes the commit message.

## Reporting issues

Use the issue templates for bugs and feature requests. Report security problems privately as
described in [docs/SECURITY.md](docs/SECURITY.md), not in public issues.

## License

By contributing you agree that your contributions are licensed under the
[Apache License, Version 2.0](LICENSE).
