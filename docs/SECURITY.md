# Security Policy

## Supported versions

| Version | Supported |
|---|---|
| 1.x | Yes: security fixes are released as 1.x patch versions |
| < 1.0 (pre-release and `-SNAPSHOT` builds) | No |

Only the latest 1.x minor line receives fixes; upgrade to it to pick them up.

## Reporting a vulnerability

Please do **not** open a public issue, discussion or pull request for a security problem.

Report it privately through GitHub Security Advisories:
[github.com/phatjam98/elastic-protos/security/advisories/new](https://github.com/phatjam98/elastic-protos/security/advisories/new)
("Report a vulnerability" on the repository's Security tab).

Include, as far as you can:

- the affected version(s) and module(s) (`elastic-protos-core`, an adapter, the Micronaut module);
- the engine and version involved, if relevant;
- a description of the issue and its impact;
- steps or a minimal proto and code sample that reproduce it.

## What to expect

This is a volunteer-maintained project, so these are goals rather than guarantees:

- an acknowledgement within 5 business days;
- an initial assessment (confirmed, needs more information, or not a vulnerability) within 14 days;
- for confirmed issues, a fix or mitigation in a patch release as soon as practical, coordinated with
  you, followed by a published advisory that credits you unless you prefer otherwise.

## Scope

In scope: the published modules (`elastic-protos-core`, `elastic-protos-elasticsearch`,
`elastic-protos-opensearch`, `elastic-protos-micronaut`, `elastic-protos-bom`). Vulnerabilities in
Elasticsearch, OpenSearch, their clients or other dependencies should be reported to those projects;
let us know too if elastic-protos needs to update a dependency.

Note that `insecureTrustAllCertificates` / `elastic-protos.ssl.insecure-trust-all-certificates`
disables TLS verification by design and is documented as development-only; using it in production is
not a vulnerability in this library.
