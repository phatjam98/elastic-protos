## Summary

<!-- What does this change and why? Link the issue: "Closes #123". -->

## Type

<!-- The PR title should be a Conventional Commit, e.g. "feat(query): add ..." or "fix(opensearch): ...". -->

- [ ] `feat` / `fix` / `perf` / `refactor` (appears in the changelog)
- [ ] `docs` / `test` / `build` / `ci` / `chore`
- [ ] Breaking change (`!` in the title or a `BREAKING CHANGE:` footer), described below

## Checklist

- [ ] `./gradlew build` passes (Checkstyle with zero warnings, unit tests, coverage gates, Javadoc)
- [ ] Engine-facing changes: `integrationTest` passes on at least one Elasticsearch and one OpenSearch image
- [ ] Tests added or updated (Spock specs; shared engine behaviour in `EngineIntegrationSpec`)
- [ ] Golden files regenerated with `-PupdateGolden=true` and the diff reviewed, if mapping or query output changed
- [ ] A changed mapping golden (new content hash, so existing indices migrate) is called out below
- [ ] README / Javadoc / ADR updated where behaviour or public API changed
- [ ] No engine client or framework dependency added to `elastic-protos-core`

## Notes for reviewers

<!-- Migration impact, compatibility, anything worth a closer look. -->
