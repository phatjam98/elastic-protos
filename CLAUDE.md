# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Elastic Protos - A modern Java library providing high-level interfaces for Elasticsearch interactions.

## Build Commands

```bash
# Build the entire project
./gradlew build

# Run all tests
./gradlew test

# Run tests for a specific module
./gradlew :elasticsearch-utils:test
./gradlew :elasticsearch-micronaut:test

# Run a single test class
./gradlew test --tests "com.phatjam98.elasticsearch.utils.DocIdUtilsSpec"

# Clean build artifacts
./gradlew clean

# Check code style
./gradlew checkstyle

# Generate code coverage report
./gradlew jacocoTestReport

# Verify code coverage meets thresholds
./gradlew jacocoTestCoverageVerification

# Show current version
./gradlew showVersion

# Publish to local Maven for testing
./gradlew publishToMavenLocal

# Publish to artifact registry (requires configuration)
./gradlew publish
```

## Architecture

### Module Structure

This is a multi-module Gradle project with dependencies between modules:

1. **elasticsearch-utils** (Java 11)
   - Core utilities for building Elasticsearch queries
   - Proto-to-JSON conversion utilities
   - Query models (equality, range, geo queries)
   - Mappings comparison utilities
   - Response handling utilities
   - 60% minimum code coverage requirement
   - No dependencies on other modules

2. **elasticsearch-micronaut** (Java 17)
   - Micronaut-based service layer
   - ElasticsearchService with index/document operations
   - BulkElasticsearchService for bulk operations
   - Client factory for creating ES connections
   - Migration and reindexing support
   - Contains Protocol Buffer definitions in `src/main/proto/`
   - 30% minimum code coverage requirement
   - Depends on: `elasticsearch-utils` module

3. **buildSrc** (Build Configuration)
   - Custom Gradle conventions for consistent configuration
   - `elastic-protos.java-conventions.gradle` - Base Java/Proto/Testing setup
   - `elastic-protos.library-conventions.gradle` - Library publishing and versioning
   - Shared by all modules to reduce duplication

### Key Design Patterns

1. **Protocol Buffer Integration**: All query criteria, pagination, and sorting use Protocol Buffers from local proto definitions
2. **Type Safety**: Generic types used throughout for compile-time safety
3. **Dependency Injection**: Micronaut framework for DI in the service layer
4. **Builder Pattern**: RequestBuilder for constructing Elasticsearch queries

### Testing Framework

- **Spock Framework**: All tests written in Groovy using Spock
- **Testcontainers**: Integration tests use Elasticsearch containers
- **Test Fixtures**: JSON test data in `src/test/resources/fixtures/`

## Key Classes and Locations

### elasticsearch-utils module
- Query Building: `elasticsearch-utils/src/main/java/com/phatjam98/elasticsearch/utils/RequestBuilder.java`
- Proto-JSON Conversion: `elasticsearch-utils/src/main/java/com/phatjam98/elasticsearch/utils/ProtoJsonUtils.java`
- Geo Utilities: `elasticsearch-utils/src/main/java/com/phatjam98/elasticsearch/utils/GeobufUtils.java`
- Query Models: `elasticsearch-utils/src/main/java/com/phatjam98/elasticsearch/utils/models/`

### elasticsearch-micronaut module
- Service Layer: `elasticsearch-micronaut/src/main/java/com/phatjam98/elasticsearch/micronaut/service/ElasticsearchService.java`
- Bulk Operations: `elasticsearch-micronaut/src/main/java/com/phatjam98/elasticsearch/micronaut/service/BulkElasticsearchService.java`
- Client Factory: `elasticsearch-micronaut/src/main/java/com/phatjam98/elasticsearch/micronaut/factory/ClientFactory.java`
- Proto Definitions: `elasticsearch-micronaut/src/main/proto/` (pagination, search_criteria, sorting_criteria, etc.)

## Configuration

Elasticsearch connection configured via `application.yaml`:
```yaml
elasticsearch:
  httpHosts: "http://elasticsearch:9200"  # Required
  username: some_name                      # Optional
  password: the_password                   # Optional
  replicas: 1                             # Optional
  shards: 1                               # Optional
  insecure-trust-all-certificates: false  # Optional - NEVER use in production
```

## Code Quality

- **Google Java Format**: Code style enforced via Checkstyle
- **Checkstyle Config**: Root-level `config/google-java-format.xml`
- **Coverage Tool**: JaCoCo with different thresholds per module
- **Test Framework**: Spock (Groovy) for BDD-style tests in `src/test/groovy/`

## Publishing and Versioning

- Uses semantic versioning (currently set in `gradle.properties`)
- Version format: `0.1.0` (release) or `0.1.1-548774e` (snapshot)
- Publishes to Google Cloud Artifact Registry (requires configuration)
- For local development: `./gradlew publishToMavenLocal`
- No overwriting of existing artifacts allowed
- Note: Git-based semver plugin is disabled (see `buildSrc/...library-conventions.gradle`)

## Dependencies

Key external dependencies:
- Elasticsearch Java Client 7.17.12
- Micronaut 4.0.2
- Google Protobuf 3.23.4
- Jackson 2.15.2
- Spock 2.3 (testing)

Maven repositories required:
- Maven Central
- OSGeo (for geotools): `https://repo.osgeo.org/repository/release/`

## Protocol Buffers

- Proto definitions are in `elasticsearch-micronaut/src/main/proto/`
- Key proto files: `pagination.proto`, `search_criteria.proto`, `sorting_criteria.proto`
- Compiled automatically by Gradle protobuf plugin during build
- Generated Java classes used throughout both modules for type-safe queries