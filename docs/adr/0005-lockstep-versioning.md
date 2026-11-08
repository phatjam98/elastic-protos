# 0005. Lockstep versioning of all modules

- Status: Accepted
- Date: 2026-11-01

## Context

elastic-protos ships several artifacts (core, two adapters, the Micronaut module) that are built
from the same commit and depend on each other's internals through the `SearchEngine` SPI. Pre-1.0
versions were derived from git tags and commit hashes per build, which made published versions hard
to predict and impossible to overwrite after a mistake. Independent module versions would force
users to work out which core goes with which adapter.

## Decision

- Every published module has the same version, declared once in `gradle.properties` and overridden
  from the release tag (`vX.Y.Z` => `-Pversion=X.Y.Z`) by the release workflow.
- `elastic-protos-bom` constrains all modules to that version; the documentation tells users to
  import it.
- All modules are released together by one JReleaser run from the shared `build/staging-deploy`,
  even when only one module changed.
- Versions follow semantic versioning across the whole set. Pre-releases use `-rc.N`
  (`1.0.0-rc.1`); `main` carries the next `-SNAPSHOT`, which is published to the Central snapshots
  repository.
- The first public version is `1.0.0`, reset from the internal `2.1.0-SNAPSHOT` line.

## Consequences

- Users pick one version; mismatched core/adapter combinations cannot be resolved through the BOM.
- A change in one module bumps the version of all of them, so some releases contain unchanged
  artifacts.
- A breaking change in any published module means a major version for all.
- Maven Central releases are immutable, so release candidates are real, permanent versions; there
  are no throwaway test versions.
