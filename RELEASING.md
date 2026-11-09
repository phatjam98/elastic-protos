# Releasing elastic-protos

All published modules (`elastic-protos-core`, `-elasticsearch`, `-opensearch`, `-micronaut`, `-bom`)
are versioned and released together ([ADR 0005](docs/adr/0005-lockstep-versioning.md)). Gradle
stages every module into the shared `build/staging-deploy` directory (`./gradlew stageAll`), and
[JReleaser](https://jreleaser.org/) signs and deploys from there, configured once in
[`jreleaser.yml`](jreleaser.yml):

| Version | Command | Goes to |
|---|---|---|
| `X.Y.Z-SNAPSHOT` | `./gradlew jreleaserDeploy` | Central snapshots repository (`nexus2.snapshot-deploy`); no GitHub release |
| `X.Y.Z`, `X.Y.Z-rc.N` | `./gradlew jreleaserFullRelease` | Maven Central via the Portal API (`mavenCentral.release-deploy`) and a GitHub release with a changelog |

> **Maven Central releases are permanent.** A version can never be changed, replaced or deleted once
> published. Always dry-run first, and never publish throwaway test versions.

## Prerequisites

### Accounts

1. **Central Portal**: an account on [central.sonatype.com](https://central.sonatype.com/) with the
   namespace `com.phatjam98` verified (it is shared with proto-faker). Generate a **user token**
   (Account > Generate User Token); legacy OSSRH credentials do not work. For snapshot publishing,
   enable SNAPSHOTs for the namespace (Namespaces > `com.phatjam98`).
2. **GitHub token** for creating releases, needed for local runs only: a classic token with `repo`
   scope (for example the output of `gh auth token`). GitHub Actions uses its automatic token.
3. **GPG key** `0BF66CC2B921A8AA` (owner phatjam98@gmail.com, expires **2027-10-31**, see
   [Key renewal](#gpg-key-renewal)). Its public key must be on the public keyservers, because Central
   verifies signatures against them:

   ```bash
   gpg --list-secret-keys --keyid-format LONG 0BF66CC2B921A8AA
   gpg --keyserver keyserver.ubuntu.com --send-keys 0BF66CC2B921A8AA
   gpg --keyserver keys.openpgp.org --send-keys 0BF66CC2B921A8AA   # then confirm the email it sends
   ```

### GitHub secrets

Repository Settings > Secrets and variables > Actions. The snapshot and release workflows read:

| Secret | Value |
|---|---|
| `JRELEASER_MAVENCENTRAL_USERNAME` | Central Portal user token username |
| `JRELEASER_MAVENCENTRAL_PASSWORD` | Central Portal user token password |
| `JRELEASER_NEXUS2_USERNAME` | Central Portal user token username (snapshots) |
| `JRELEASER_NEXUS2_PASSWORD` | Central Portal user token password (snapshots) |
| `JRELEASER_GPG_PUBLIC_KEY` | `gpg --armor --export 0BF66CC2B921A8AA` |
| `JRELEASER_GPG_SECRET_KEY` | `gpg --armor --export-secret-keys 0BF66CC2B921A8AA` |
| `JRELEASER_GPG_PASSPHRASE` | The key's passphrase (leave empty if it has none) |

No GitHub token secret is needed in GitHub Actions: the workflows pass the automatic `GITHUB_TOKEN`
to JReleaser (job permission `contents: write`, plus `issues: write` for the release workflow). For a
local run, set `JRELEASER_GITHUB_TOKEN` to a personal access token with `repo` scope.

The keys are stored ASCII-armored. The workflows also import the secret key into the runner's GPG
agent (`crazy-max/ghaction-import-gpg`).

### Local environment

Copy [`.env.example`](.env.example) to `.env` (git-ignored), fill it in and load it before any local
JReleaser run:

```bash
cp .env.example .env
$EDITOR .env
set -a; source .env; set +a
```

## Local dry run

A dry run stages, signs and validates (including pomchecker and the key checks) but uploads nothing
and creates no tag or release. Pass the version you intend to release:

```bash
rm -rf build/staging-deploy          # JReleaser deploys everything it finds here
./gradlew build
./gradlew stageAll -Pversion=1.0.0-rc.1
./gradlew jreleaserFullRelease --dryrun -Pversion=1.0.0-rc.1
```

`jreleaserFullRelease` depends on `stageAll`, so the separate `stageAll` step is only there to
inspect the output first:

```bash
find build/staging-deploy -type f | sort
# build/staging-deploy/com/phatjam98/elastic-protos-core/1.0.0-rc.1/
#   elastic-protos-core-1.0.0-rc.1.jar, -sources.jar, -javadoc.jar, .pom, .module
# ... the same for -elasticsearch, -opensearch, -micronaut, and .pom/.module for -bom
```

Then read `build/jreleaser/trace.log` and the generated `build/jreleaser/release/CHANGELOG.md`.

For a snapshot, `./gradlew stageAll && ./gradlew jreleaserDeploy --dryrun` uses the version in
`gradle.properties`.

The same dry run is available in CI: Actions > **Release** > Run workflow, from `main`, with a version
(without the `v`) and **dry run** checked (the default). It uses the repository secrets, so it also
proves they are valid.

## Snapshot flow

Automatic. Every push to `main` runs [`snapshot.yml`](.github/workflows/snapshot.yml): it reads the
version, runs the full CI workflow, and if the version ends in `-SNAPSHOT` stages all modules and runs
`jreleaserDeploy` to the Central snapshots repository
(`https://central.sonatype.com/repository/maven-snapshots/`). Non-SNAPSHOT versions are skipped.

Consumers can try a snapshot with:

```groovy
repositories {
    maven { url 'https://central.sonatype.com/repository/maven-snapshots/' }
}
```

## Release flow

Releases are cut from `main` only; the workflow refuses a tag whose commit is not on `main`. Tags must
match `v<major>.<minor>.<patch>` or `v<major>.<minor>.<patch>-rc.<n>`.

1. **Check `main` is green** (CI, including the four integration engines) and review the commits
   since the last release; the changelog is generated from their Conventional Commit messages.
2. **Dry-run** the release, locally or with the workflow dispatch above.
3. **Set the release version** in `gradle.properties` and commit it on `main`:

   ```bash
   sed -i '' 's/^version=.*/version=1.0.0/' gradle.properties   # GNU sed: sed -i
   git commit -am "chore(release): 1.0.0"
   git push origin main
   ```

   The snapshot workflow skips this commit because the version is not a SNAPSHOT.
4. **Tag and push**:

   ```bash
   git tag -a v1.0.0 -m "elastic-protos 1.0.0"
   git push origin v1.0.0
   ```

5. [`release.yml`](.github/workflows/release.yml) runs: it validates the tag, checks the commit is
   on `main`, imports the GPG key, runs `./gradlew build -Pversion=1.0.0` and then
   `./gradlew jreleaserFullRelease -Pversion=1.0.0`, which stages, signs, uploads to the Central
   Portal, waits for validation and publication, and creates the GitHub release with the changelog.
   The version always comes from the tag. JReleaser logs are attached to the workflow run.
6. **Verify** (Central can take 10 to 30 minutes to show a new version, search longer):
   - [ ] [Central Portal deployments](https://central.sonatype.com/publishing/deployments): the
         deployment is `PUBLISHED`
   - [ ] [central.sonatype.com/artifact/com.phatjam98/elastic-protos-core](https://central.sonatype.com/artifact/com.phatjam98/elastic-protos-core)
         and the other four artifacts show the version
   - [ ] [repo1.maven.org/maven2/com/phatjam98/](https://repo1.maven.org/maven2/com/phatjam98/) has
         the jars, `.asc` signatures and checksums
   - [ ] [javadoc.io/doc/com.phatjam98/elastic-protos-core](https://javadoc.io/doc/com.phatjam98/elastic-protos-core)
         renders the new version (javadoc.io fetches it on first request)
   - [ ] the [GitHub release](https://github.com/phatjam98/elastic-protos/releases) exists with the
         expected changelog
   - [ ] a scratch project resolves `platform('com.phatjam98:elastic-protos-bom:1.0.0')` and compiles
         a proto that imports `elastic_options.proto`
7. **Bump to the next SNAPSHOT** and push:

   ```bash
   sed -i '' 's/^version=.*/version=1.0.1-SNAPSHOT/' gradle.properties
   git commit -am "chore(release): prepare 1.0.1-SNAPSHOT"
   git push origin main
   ```

## Release candidates

A release candidate is a real, permanent Central release, marked as a pre-release on GitHub (the
`prerelease.pattern` in `jreleaser.yml` matches `-rc`). `1.0.0-rc.1` precedes `1.0.0` so the first
consumer can trial the released artifacts.

1. Set `version=1.0.0-rc.1`, commit `chore(release): 1.0.0-rc.1`, push to `main`.
2. Tag `v1.0.0-rc.1` and push the tag; verify as above.
3. Set `version=1.0.0-SNAPSHOT` again and push, so snapshots resume.
4. Fixes go to `main`; cut `-rc.2` and so on the same way, then the final `1.0.0`.

Never re-tag or delete a published version; publish the next number instead.

## Troubleshooting

**pomchecker fails.** `jreleaser.yml` runs pomchecker with `failOnError: true` on every staged POM.
Central requires `name`, `description`, `url`, `licenses`, `developers` and `scm`; they come from
`buildSrc/src/main/groovy/elastic-protos.publish-conventions.gradle` and each module's `description`.
Check the generated POMs under `build/staging-deploy` or `*/build/publications/mavenJava/pom-default.xml`.

**Signing fails.** Make sure the three `JRELEASER_GPG_*` values are the armored exports of the same
key and the passphrase matches. Test locally:

```bash
echo test | gpg --clearsign --local-user 0BF66CC2B921A8AA
gpg --armor --export 0BF66CC2B921A8AA | head -1   # -----BEGIN PGP PUBLIC KEY BLOCK-----
```

**`Key ... was NOT found as published`.** JReleaser checks that the signing key is on a public
keyserver; Central rejects deployments whose signatures it cannot verify. Send the key to
`keyserver.ubuntu.com` and `keys.openpgp.org` (confirm the email for the latter), wait a few
minutes, and check the warning names `0BF66CC2B921A8AA` and not another key from your keyring.

**Central validation fails or the deployment stays `VALIDATING`.** Open the deployment on the
[deployments page](https://central.sonatype.com/publishing/deployments) for the exact errors. Common
causes: missing `-sources.jar` / `-javadoc.jar` or signatures (re-stage from a clean
`build/staging-deploy`), a namespace that is not verified, a token for another account, or a version
that already exists (versions are immutable). Drop a failed deployment in the Portal before retrying.
JReleaser polls for up to two hours (`maxRetries: 120`, `retryDelay: 60`).

**Snapshot deploy returns 401 or 403.** Check the `JRELEASER_NEXUS2_*` token and that SNAPSHOTs are
enabled for the namespace in the Central Portal.

**Stale or unexpected files uploaded.** JReleaser deploys everything under `build/staging-deploy`;
delete it before staging locally.

**`Auto configure detected '<branch>' as default branch`.** Harmless in local dry runs off `main`;
real releases run on `main`.

**Inspecting configuration.** `./gradlew jreleaserConfig` prints the resolved configuration with
secrets masked; add `--info` to any JReleaser task for more output.

## Recovering a half-finished release

A Maven Central release is irreversible, so never rerun `jreleaserFullRelease` for a version that is
already on Central. If the Central deploy succeeded but the GitHub release failed (for example a bad
GitHub token), run Actions > **Release** > Run workflow by hand (`workflow_dispatch`) from `main` with
`version` set, **dry run** unchecked and **release only** checked. That runs `jreleaserRelease` from the
tagged commit: it signs and creates the GitHub release with the changelog and never touches Maven
Central. The local equivalent, with the GPG and GitHub env vars set, is:

```bash
JRELEASER_SKIP_TAG=true ./gradlew jreleaserRelease -Pversion=X.Y.Z
```

If the tag itself is missing, dispatch without release only: that creates the tag as part of the full
release, so use it only when the version is not yet on Central.

## GPG key renewal

Key `0BF66CC2B921A8AA` expires on **2027-10-31**. Signing fails after that date, so extend it well
before (aim for September 2027):

```bash
gpg --quick-set-expire 0BF66CC2B921A8AA 2y          # primary key
gpg --quick-set-expire 0BF66CC2B921A8AA 2y '*'      # all subkeys
gpg --keyserver keyserver.ubuntu.com --send-keys 0BF66CC2B921A8AA
gpg --keyserver keys.openpgp.org --send-keys 0BF66CC2B921A8AA
```

Then update the `JRELEASER_GPG_PUBLIC_KEY` and `JRELEASER_GPG_SECRET_KEY` secrets (and your `.env`)
with fresh armored exports, update the date in this file, and run a release dry run to confirm. The
same key signs proto-faker, so update that repository's secrets too.

## References

- [JReleaser guide](https://jreleaser.org/guide/latest/)
- [Central Portal publishing](https://central.sonatype.org/publish/publish-portal-guide/)
- [Central requirements](https://central.sonatype.org/publish/requirements/)
