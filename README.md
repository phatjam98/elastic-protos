# elastic-protos
Elastic Protos - Elasticsearch Java Library

A modern Java library providing high-level interfaces for Elasticsearch interactions, including client factory, service layer, request builders, geo utilities, and comprehensive models for search operations.

# Install
Add this to your `gradle.build` dependencies
```groovy
implementation("com.phatjam98:elastic-protos:0.1.1")
```
### Repositories
Required for geotools dependencies
```groovy
maven { url "https://repo.osgeo.org/repository/release/" }
```
Required for elastic-protos dependencies (configure as needed)
```groovy
// TODO: Configure your own Maven repository
mavenLocal() // For local development
```

# How to use this
## API Docs

API docs can be found [here](https://special-goggles-208e801f.pages.github.io/)

## Elasticsearch Service
### Configuration

In `application.yaml` (or other configuration source) you must include the httphosts

```yaml
elasticsearch:
  httpHosts: "http://elasticsearch:9200"
```

Other options available (but not all):

```yaml
elasticsearch:
  httpHosts: "http://localhost:9200"
  username: some_name
  password: the_password
  replicas: 1
  shards: 1
  insecure-trust-all-certificates: false  # Not intended for production!
```

#### Insecurely Disabling TLS Validation
We provide a configuration flag `elasticsearc.insecure-trust-all-certificates`.
to disable validating TLS hostnames; traffic to/from Elasticsearch is still encrypted
over the wire, but this makes us vulnerable to Man-In-The-Middle (MITM) attacks.
This is intended for testing and debugging only (e.g. for cases where it is difficult
to configure the applicatoin with signing certificate authority (CA) that signed the
Elasticsearch service's certificate. *DO NOT USE THIS FLAG IN PRODUCTION!*

In any class, `@Inject` the service and you are in.
```java
  @Inject
  private ElasticsearchService service;
```

## GeobufUtils

Placeholder

## RequestBuilder

Placeholder

# How to publish
This uses a semver gradle plugin that updates version based on git tags.
## Version Format
`0.1.0` << This is a Release version.

`0.1.1-548774e` << This is a clean branch.

`0.1.1-dirty-548774e` << This is a dirty branch with uncommitted changes

## Create Release
Check for the current version:
```shell
./gradlew showVersion
```
Given the above example of `0.1.1-548774e` if we are creating a new release after merging into main then `0.1.1` the next release:
```shell
git tag -a 0.1.1 -m "Release description"
git push --tags
```

## Publishing
For local development and testing, publish to local Maven repository:
```shell
./gradlew publishToMavenLocal
```

For release publishing, configure your preferred artifact repository in the build configuration.

It is important to note, overwriting of an artifact is prohibited. This is only a problem if you try and publish 
multiple times on the same dirty commit. The remedy is to commit your changes and publish.  
