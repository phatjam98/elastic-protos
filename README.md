# elastic-protos

[![CI](https://github.com/phatjam98/elastic-protos/actions/workflows/ci.yml/badge.svg)](https://github.com/phatjam98/elastic-protos/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/com.phatjam98/elastic-protos-core.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/com.phatjam98/elastic-protos-core)
[![javadoc](https://javadoc.io/badge2/com.phatjam98/elastic-protos-core/javadoc.svg)](https://javadoc.io/doc/com.phatjam98/elastic-protos-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

**elastic-protos** makes your Protocol Buffer messages the schema of your search indices. It turns
`.proto` definitions into Elasticsearch and OpenSearch index mappings, creates and migrates the
indices behind stable aliases as the protos evolve, converts messages to documents and back, and
compiles a proto-defined query DSL into search requests. The core is engine-agnostic; thin adapters
talk to each engine through its official low-level REST client.

## Contents

- [Why](#why)
- [Features](#features)
- [Requirements](#requirements)
- [Versioning and compatibility](#versioning-and-compatibility)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Mapping](#mapping)
- [Index naming and metadata](#index-naming-and-metadata)
- [Migrations](#migrations)
- [Document codec](#document-codec)
- [Query DSL](#query-dsl)
- [Engines](#engines)
- [Micronaut](#micronaut)
- [Limitations](#limitations)
- [Building from source](#building-from-source)
- [Contributing and license](#contributing-and-license)

## Why

If your domain model already lives in protobuf (gRPC APIs, Kafka events, stored messages), the
search index is one more copy of that schema, and keeping the two in step by hand is where bugs
come from.

| Option | Good at | Trade-off when your model is protobuf |
|---|---|---|
| Spring Data Elasticsearch | Annotated Java entities, repositories, the Spring ecosystem | Mapping lives in Java annotations on a second model class; Spring only; OpenSearch needs a separate project |
| Hibernate Search | Keeping an index in sync with JPA entities, both engines | Built around an ORM and a relational source of truth |
| Raw `elasticsearch-java` / `opensearch-java` | Full control of every API | You hand-write mappings, migrations, JSON conversion and query building, per engine |
| **elastic-protos** | The `.proto` is the schema: mappings, migrations, documents and queries derive from it | Covers indexing and search, not every engine API; reindex migrations block writes briefly (see [Limitations](#limitations)) |

The query DSL is itself a proto (`SearchCriteria`), so a search request can cross a gRPC boundary
unchanged and be compiled on the server against the index definition.

## Features

- Index mappings derived from proto descriptors, configured with proto options
  (`elastic`, `elastic_field`) and optional programmatic overrides; no field-name guessing.
- Content-hash-versioned indices (`documents-v1a2b3c4d`) behind a stable alias, with a recorded
  `_meta` block.
- Migrations that plan before they act: `CREATE`, `NO_OP`, `PUT_MAPPING`, `REINDEX` (with optional
  Painless scripts) or `BLOCKED`, a printable dry-run report, and rollback.
- `ProtoDocumentCodec`: message to document and back, honouring renamed and ignored fields, geo
  types, 64-bit integers and timestamps.
- `QueryCompiler`: `SearchCriteria` to a search body, validated against the mapped field types, with
  full-text search, negation, automatic `nested` wrapping, sorting, from/size and point-in-time paging.
- One `SearchEngine` SPI over plain JSON maps, with adapters for Elasticsearch 8.x/9.x and OpenSearch
  2.x/3.x, and a thread-safe `BulkBuffer`.
- Optional Micronaut 4 module: configuration properties, bean factories, startup bootstrap and a
  repository-style `ElasticProtosService`.

## Requirements

- Java 17 or newer (the library is compiled for 17 and tested on 17 and 21).
- protobuf-java 3.25 or newer at runtime; the library's generated code is built with protoc 3.25,
  and CI runs the unit tests on the protobuf-java 4.x runtime as well.
- Elasticsearch 8.x or 9.x, or OpenSearch 2.x or 3.x (see [Engines](#engines)).
- Micronaut 4 for the optional Micronaut module.

## Versioning and compatibility

- elastic-protos follows [semantic versioning](https://semver.org). All modules are released
  together under one version (lockstep); use the BOM to keep them aligned.
- The public API is every public type outside the packages marked internal. The internal packages,
  `com.phatjam98.elasticprotos.util` and `com.phatjam98.elasticprotos.engine.rest`, and the class
  `query.NestedPathResolver` may change in any minor release.
- A change to the default mapping rules changes the content hash of the affected indices, so every
  consumer gets a REINDEX on upgrade. Such changes are made in major versions only.
- Minimum runtime: Java 17 and protobuf-java 3.25; the protobuf-java 4.x runtime is verified in CI.
- Each published jar declares an `Automatic-Module-Name` (`com.phatjam98.elasticprotos.core`,
  `.elasticsearch`, `.opensearch`, `.micronaut`) for the module path.
- The modules declare no platform dependency of their own. The core declares `jackson-databind`
  with an explicit version; Jackson's own metadata then aligns the Jackson modules on your
  classpath to at least that version. The Micronaut module publishes no Micronaut dependency
  (see [Micronaut](#micronaut)).

## Installation

All modules share one version (lockstep). Import the BOM, then add the core and one adapter.

| Artifact | Purpose |
|---|---|
| `com.phatjam98:elastic-protos-bom` | Aligns the versions below |
| `com.phatjam98:elastic-protos-core` | Mapping, options, codec, migration, query, engine SPI |
| `com.phatjam98:elastic-protos-elasticsearch` | Adapter for Elasticsearch 8.x and 9.x |
| `com.phatjam98:elastic-protos-opensearch` | Adapter for OpenSearch 2.x and 3.x |
| `com.phatjam98:elastic-protos-micronaut` | Optional Micronaut 4 integration |

### Gradle

```groovy
plugins {
    id 'java'
    id 'com.google.protobuf' version '0.10.0'
}

dependencies {
    implementation platform('com.phatjam98:elastic-protos-bom:1.0.0')
    implementation 'com.phatjam98:elastic-protos-core'
    implementation 'com.phatjam98:elastic-protos-elasticsearch'   // or elastic-protos-opensearch
}

protobuf {
    protoc { artifact = 'com.google.protobuf:protoc:3.25.9' }
}
```

The core jar ships its `.proto` files. The protobuf Gradle plugin extracts `.proto` files from jars
on the compile classpath onto protoc's include path, so your own protos can simply
`import "phatjam98/elasticprotos/v1/elastic_options.proto";`. Do **not** add the core as a
`protobuf` dependency: that compiles its protos again and produces duplicate classes.

Use a protoc version no newer than the protobuf-java runtime on your classpath (the core brings in
3.25.x; a 4.x protoc needs a 4.x runtime).

### Maven

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>com.phatjam98</groupId>
      <artifactId>elastic-protos-bom</artifactId>
      <version>1.0.0</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>com.phatjam98</groupId>
    <artifactId>elastic-protos-core</artifactId>
  </dependency>
  <dependency>
    <groupId>com.phatjam98</groupId>
    <artifactId>elastic-protos-elasticsearch</artifactId>
  </dependency>
</dependencies>
```

To compile protos that import `elastic_options.proto`, use a protobuf Maven plugin that adds
`.proto` files from dependency jars to the import path (for example
`io.github.ascopes:protobuf-maven-plugin` or `org.xolstice.maven.plugins:protobuf-maven-plugin`).
The Maven setup is not exercised by this project's build; reports and fixes are welcome.

## Quick start

A complete, runnable version of these steps against a live cluster is the
[`Demo`](elastic-protos-testkit/src/main/java/com/phatjam98/elasticprotos/testkit/Demo.java):
`./gradlew :elastic-protos-testkit:demo --args="elasticsearch http://localhost:9200"`.

### 1. Annotate a proto

```proto
syntax = "proto3";

package acme.search.v1;

import "google/protobuf/timestamp.proto";
import "phatjam98/elasticprotos/v1/elastic_options.proto";

option java_package = "com.acme.search";
option java_multiple_files = true;

message Document {
  option (phatjam98.elasticprotos.v1.elastic) = {
    alias: "documents"
    default_analyzer: "english"
  };

  string id = 1 [(phatjam98.elasticprotos.v1.elastic_field).id = TRUE];
  string firm_id = 2;                     // keyword
  string title = 3 [(phatjam98.elasticprotos.v1.elastic_field) = { type: TEXT boost: 3.0 }];
  string body = 4 [(phatjam98.elasticprotos.v1.elastic_field).type = TEXT];
  repeated string tags = 5;               // keyword array
  repeated Party parties = 6;             // nested
  google.protobuf.Timestamp filed_at = 7; // date
  string internal_notes = 8 [(phatjam98.elasticprotos.v1.elastic_field).type = IGNORE];
}

message Party {
  string name = 1 [(phatjam98.elasticprotos.v1.elastic_field).type = TEXT];
  string role = 2;
}
```

### 2. Bootstrap, index, search

```java
try (SearchEngine engine = ElasticsearchEngine.builder()
    .hosts("http://localhost:9200")
    .build()) {

  // 1. Create (or migrate) the index behind the "documents" alias.
  ProtoMapper mapper = ProtoMapper.withDefaults();
  IndexMigrator migrator = IndexMigrator.builder().engine(engine).mapper(mapper).build();
  MigrationPlan plan = migrator.plan(Document.class);
  System.out.print(plan.toReport());
  MigrationResult migration = migrator.apply(plan);
  if (!migration.ok()) {
    throw new IllegalStateException("bootstrap failed: " + migration.problems());
  }

  // 2. Index a message.
  ProtoDocumentCodec codec = ProtoDocumentCodec.of(mapper);
  IndexDefinition documents = mapper.map(Document.class);
  Document doc = Document.newBuilder()
      .setId("doc-1")
      .setFirmId("firm-1")
      .setTitle("Merger agreement between Acme and Globex")
      .setBody("This agreement sets out the terms of the merger.")
      .addTags("m-and-a")
      .build();
  engine.index(documents.alias(), codec.idOf(doc).orElseThrow(), codec.toDocument(doc),
      IndexOptions.refreshed());

  // 3. Full-text search across every TEXT field, and read the hits back as messages.
  SearchCriteria criteria = Criteria.builder().fullText("merger agreement").build();
  CompiledSearch compiled = QueryCompiler.withDefaults().compile(criteria, documents);
  SearchResult result = engine.search(List.of(documents.alias()), compiled.body());
  for (Hit hit : result.hits()) {
    Document found = codec.fromHit(hit, Document.class);
    System.out.println(hit.score() + "  " + found.getTitle());
  }
}
```

The first run prints

```text
Migration plan: 1 index, changes pending
  CREATE      documents: (none) -> documents-vcccb33c0 (added 0, removed 0, changed 0)
```

and every later run reports `NO_OP ... up to date (hash cccb33c0)`.

Imports: `com.phatjam98.elasticprotos.{codec.ProtoDocumentCodec, elasticsearch.ElasticsearchEngine,
engine.*, mapping.*, migration.*, proto.SearchCriteria, query.*}`.

## Mapping

`ProtoMapper` turns a message descriptor into an `IndexDefinition`: the alias, the versioned index
name, settings, mappings, and the field metadata the codec, compiler and migrator use. It works from
descriptors, so `DynamicMessage` schemas work too, and it is deterministic.

### Precedence

Lowest to highest:

1. Library defaults (`MappingDefaults.standard()`)
2. Proto options (`elastic`, `elastic_field`)
3. Programmatic `MappingOverrides` (by message and field, then by path; the path wins)
4. A field's `raw_mapping_json`, deep-merged last

Index settings follow the same idea: `shards`/`replicas` options, then `settings_json`, then
`MappingOverrides.setting(...)`.

### Default rules

| Proto type | Mapping |
|---|---|
| `string` | `keyword` with `ignore_above: 256` (opt in to `TEXT`) |
| `enum` | `keyword` (the value name) |
| `bool` | `boolean` |
| `int32`, `sint32`, `sfixed32` | `integer` |
| `uint32`, `fixed32`, `int64`, `sint64`, `sfixed64` | `long` |
| `uint64`, `fixed64` | `keyword`, with a warning (unsigned 64-bit values do not fit a `long`) |
| `float` / `double` | `float` / `double` |
| `bytes` | `binary` |
| repeated scalar | same type as the scalar (engines index arrays natively) |
| singular message | `object` |
| repeated message | `nested` |
| `map<K, V>` | `flattened` (OpenSearch: `flat_object`); keys are queried by dotted path (`labels.env`). `type: OBJECT` (or `NESTED`) maps a typed map: an `object` with `dynamic: true` plus `dynamic_templates` that give every key the value type |
| `google.protobuf.Timestamp` | `date`, format `strict_date_optional_time` |
| `google.protobuf.Duration` | `keyword` |
| wrapper types (`StringValue`, `Int64Value`, ...) | the wrapped scalar's type |
| `google.protobuf.Struct`, `Value`, `ListValue` | `flattened` |
| `google.protobuf.Any`, `FieldMask`, `Empty` | left out, with a warning |
| `phatjam98.elasticprotos.v1.LatLon` | `geo_point` |
| `phatjam98.elasticprotos.v1.GeoShape` | `geo_shape` |
| recursive message, or nesting deeper than 5 | `object` with `enabled: false`, with a warning |

Maps default to `flattened` because their keys are data, not schema: an `object` with
`dynamic: true` adds a mapping entry (and a type guessed from the first value) for every new key,
which grows the mapping without bound and can reject a later document whose value has another type.
A flattened field indexes every leaf as a keyword under one mapping entry; its keys take `EQUALS`,
`ANY_IN`, `EXISTS` and `TEXT_PREFIX` (see [Query DSL](#query-dsl)).

Use `type: OBJECT` (or `NESTED`) when the values need their own type, for example numeric ranges
or full text on map values. The map becomes a typed map: an `object` with `dynamic: true` and, on
the root mapping, one `dynamic_templates` entry per value field, named `ep:<path>`, so every key
the engine sees takes the type the value would get as an ordinary field. A scalar, enum, wrapper,
`Timestamp`, `LatLon` or `GeoShape` value is one template (`counters.*` => `long`); a message
value is one template per leaf (`children.*.count` => `long`) and per object or nested field inside
it. `value_type` requests the value type, and the field's `analyzer`, `search_analyzer`,
`keyword_subfield`, `ignore_above`, `normalizer`, `format`, `index`, `doc_values`, `coerce` and
`null_value` apply to the values:

```protobuf
map<string, int64> counters = 2 [(phatjam98.elasticprotos.v1.elastic_field).type = OBJECT];
map<string, string> notes = 3 [(phatjam98.elasticprotos.v1.elastic_field) = {
  type: OBJECT value_type: TEXT analyzer: "english" full_text: TRUE
}];
map<string, Child> children = 4 [(phatjam98.elasticprotos.v1.elastic_field).type = OBJECT];
```

The codec checks the keys of typed maps only (keys of flattened maps pass unchanged): they must not
be blank and must contain no `.`, or it throws `CodecException`. Every distinct key adds fields to
the index at index time (the key's object, plus every field of a message value), which count toward
`index.mapping.total_fields.limit`; nested fields inside message values also count toward
`index.mapping.nested_fields.limit` (default 50), so indexing fails at about the 51st distinct key
of a map whose values hold a nested field. The mapper warns with both numbers per key. `Struct`,
`Value`, `ListValue` and `Any` values, and values whose message holds a map, `Struct` or `Any` at
any depth, stay untyped (`dynamic: true` only), with a warning. A typed map is a different mapping from a flattened one, so switching a map to or from
`OBJECT` reindexes.

The root mapping is `dynamic: strict`. `MappingDefaults` can change the string rule
(`withStringsAsText`), the keyword sub-field, `ignore_above`, the date format, the depth limit, the
root dynamic mode and how singular messages, repeated messages and `Struct` are mapped.

Requested types are validated (`value_type` against the map's value): `OBJECT`, `NESTED` and `FLATTENED` need a message or map field;
`GEO_POINT` needs a string or a message with `lat`/`lon` (or `latitude`, `lng`, `longitude`)
numbers; `GEO_SHAPE` needs a string or `GeoShape`; `DATE` on a message needs `Timestamp`. A
mapped-name override (`elastic_field.name`) must be non-empty and contain no `.` or whitespace.
Invalid options throw `MappingException`, naming the field path.

**Field limits.** The engines refuse mappings with more than 50 `nested` fields
(`index.mapping.nested_fields.limit`) or 1000 fields in total (`index.mapping.total_fields.limit`).
The mapper checks both up front and throws a `MappingException` naming the message, the count and
the limit. Fields are counted the way the engines count them: every mapped field, including
`object` and `nested` containers and multi-fields such as `title.keyword`. If you raise the limits
on the index (through `settings_json`), raise them in `MappingDefaults` too
(`withMaxNestedFields`, `withMaxTotalFields`); `0` turns a check off.

### Message options

`option (phatjam98.elasticprotos.v1.elastic) = { ... };`

| Option | Meaning | Default |
|---|---|---|
| `index` | Base name of the concrete index | the alias |
| `alias` | Alias the application reads and writes through | `index`, else snake_case of the message name |
| `shards` | `number_of_shards` | `0`: the migrator default (1) |
| `replicas` | `number_of_replicas`, read only when `has_replicas` is true | the migrator's replica policy |
| `has_replicas` | Marks `replicas` as set (so `0` can be expressed) | `false` |
| `dynamic` | Root dynamic mode: `DYNAMIC_STRICT`, `DYNAMIC_FALSE`, `DYNAMIC_TRUE` | `DYNAMIC_DEFAULT` = strict |
| `default_analyzer` | Analyzer for `TEXT` fields that set none | none |
| `settings_json` | Raw index settings JSON merged in (for example analysis definitions) | none |
| `id_field` | Index path of the field whose value is the document `_id` (alternative to `id: TRUE`) | none |
| `skip` | `TRUE`: never create an index for this message (`Bool3`) | unset (not skipped) |

### Field options

`string title = 3 [(phatjam98.elasticprotos.v1.elastic_field) = { type: TEXT boost: 2.0 }];`

| Option | Meaning | Default |
|---|---|---|
| `type` | Requested type: `KEYWORD`, `TEXT`, `SEARCH_AS_YOU_TYPE`, `INTEGER`, `LONG`, `DOUBLE`, `FLOAT`, `BOOLEAN`, `DATE`, `DATE_NANOS`, `GEO_POINT`, `GEO_SHAPE`, `OBJECT`, `NESTED`, `FLATTENED`, `BINARY`, `IP`, or `IGNORE` (left out of the mapping and of documents). On a `map` field, `OBJECT` (or `NESTED`) maps a typed map, whose keys take the value type through dynamic templates, instead of the default `flattened` | `AUTO`: the default rules |
| `value_type` | Type of the values of a `map` field mapped as `OBJECT` or `NESTED` (any `type` value but `IGNORE`); ignored, with a warning, on other fields | `AUTO`: the default rules for the value type |
| `analyzer` | Analyzer of a `TEXT` / `SEARCH_AS_YOU_TYPE` field | the message's `default_analyzer` |
| `search_analyzer` | Search-time analyzer | none |
| `keyword_subfield` | Give a `TEXT` field a `.keyword` sub-field (`TRUE` / `FALSE`) | `TRUE` |
| `ignore_above` | `ignore_above` of a keyword field or `.keyword` sub-field | `0`: 256 |
| `index` | `FALSE` sets `index: false` (stored, not searchable) | unset |
| `doc_values` | `FALSE` sets `doc_values: false` | unset |
| `copy_to` | Target fields for `copy_to` | none |
| `format` | Date format of a `DATE` / `DATE_NANOS` field | `strict_date_optional_time` |
| `normalizer` | Normalizer of a keyword field | none |
| `id` | `TRUE`: this field's value is the document `_id` (`Bool3`) | unset |
| `boost` | Boost when the field is searched by a `FULL_TEXT` query without a field list | `0`: 1.0 |
| `full_text` | `TRUE` includes a non-text (keyword) field in the default `FULL_TEXT` field set; `FALSE` leaves a `TEXT` field out of it (`Bool3`) | unset: `TEXT` fields only |
| `name` | Field name in the index (the codec renames on write and read) | the proto field name |
| `raw_mapping_json` | JSON deep-merged last into this field's mapping | none |
| `coerce` | `coerce` of a numeric field (`TRUE` / `FALSE`) | unset |
| `null_value` | `null_value` of the field | none |

Tri-state options use `Bool3` (`UNSET`, `TRUE`, `FALSE`), so "not set" is distinguishable from
`false` and a programmatic override can turn off an option the proto turned on. This includes the
on/off options `skip`, `id` and `full_text`: write `id: TRUE`, not `id: true`. A `TEXT` field without a `.keyword` sub-field cannot be used for exact matches or sorting;
the compiler says so.

### Programmatic overrides

For protos you cannot (or prefer not to) annotate. Overrides use the same option messages and only
replace what they set.

```java
MappingOverrides overrides = MappingOverrides.builder()
    // wherever acme.search.v1.Party appears
    .forField("acme.search.v1.Party", "role", f -> f.setNormalizer("lowercase"))
    // one path from the root message
    .forPath("firm_id", f -> f.setType(EsType.KEYWORD).setIgnoreAbove(64))
    .ignorePath("internal_notes")
    .forMessage("acme.search.v1.Document", m -> m.setShards(3))
    // on/off options take a boolean; FALSE wins over TRUE in the proto
    .fullText(Document.getDescriptor(), "title", false)
    .setting("analysis", Map.of("normalizer", Map.of("lowercase",
        Map.of("type", "custom", "filter", List.of("lowercase")))))
    .build();

ProtoMapper mapper = ProtoMapper.builder()
    .defaults(MappingDefaults.standard().withIgnoreAbove(512))
    .naming(IndexNamingStrategy.prefixed("dev"))   // dev-documents -> dev-documents-v<hash>
    .overrides(overrides)
    .build();
IndexDefinition documents = mapper.map(Document.class);
```

Inside `forField` / `forMessage` callbacks, `skip`, `id` and `full_text` are `Bool3`:
`f.setId(MappingOverrides.bool3(false))`. `ProtoMapper.isSkipped(descriptor)` reads only the proto;
`mapper.skips(descriptor)`, which the migrator uses, applies the overrides too.

## Index naming and metadata

- Applications always use the **alias** (`documents`).
- The concrete index is `<index or alias>-v<hash>` (`documents-vcccb33c0`), where the hash is the
  first 8 hex digits of a SHA-256 over the canonical `dynamic` + `properties` mapping. Any mapping
  change gives a new hash; settings and the naming prefix do not affect it (see
  [Settings changes](#settings-changes)).
- `IndexNamingStrategy.prefixed("dev")` prefixes both alias and index (`dev-documents`,
  `dev-documents-vcccb33c0`), for example per environment. The prefix is lower-cased, and one that
  cannot start an index name (empty; leading `-`, `_` or `+`; whitespace or any of
  `\ / * ? " < > | , # :`) is rejected with `IllegalArgumentException`. Implement
  `IndexNamingStrategy` for anything else.
- `PUT_MAPPING` extends an index in place, so the index keeps the name of the mapping it was created
  with while its recorded hash moves on. If a later reindex targets that same name (typically when a
  deploy is reverted under `RemovedFieldPolicy.REINDEX`), the new index gets a generation suffix:
  `documents-vcccb33c0-g2`, then `-g3`, the lowest that does not exist yet (or that is a leftover of
  an interrupted run recording the target hash). `IndexNamingStrategy.generationName` decides the
  suffix; generation 1 is the plain name.
- Every mapping carries its provenance, which the migrator reads back:

```json
"_meta": {
  "elastic_protos": {
    "hash": "cccb33c0",
    "proto": "acme.search.v1.Document",
    "schema_version": 1
  }
}
```

Indices without this block (created by other tools) are treated as hash `legacy`.

## Migrations

`IndexMigrator` compares what each message maps to with what its alias points at today.

```java
IndexMigrator migrator = IndexMigrator.builder()
    .engine(engine)
    .mapper(mapper)
    .options(MigrationOptions.builder()            // starts from MigrationOptions.standard()
        .failure(FailurePolicy.CONTINUE)
        .removedFields(RemovedFieldPolicy.KEEP)
        .replicas(ReplicaPolicy.fixed(1))
        .reindexTimeout(Duration.ofMinutes(30))
        .build())
    .scriptProvider(MigrationScriptProvider.classpath("migrations"))
    .build();

MigrationPlan plan = migrator.plan(Document.class);
System.out.print(plan.toReport());   // or plan.toJson() for CI artifacts
if (!plan.blocked().isEmpty()) {
  throw new IllegalStateException("blocked: " + plan.blocked());
}
MigrationResult result = migrator.apply(plan);
```

`migrator.bootstrap(Document.class, Firm.class)` is `apply(plan(...))` in one call.

### Actions

| Action | When | What `apply` does |
|---|---|---|
| `CREATE` | The alias does not exist | Creates the index with settings, mappings and the alias in one request |
| `NO_OP` | Hash matches, or the mappings already match | Nothing |
| `PUT_MAPPING` | Only fields were added that existing documents can be indexed into (new top-level fields, new properties under an object that already has typed properties, new typed maps with their `dynamic_templates`, new fields in the values of typed maps), or only `dynamic` changed | Extends the mapping of the current index in place |
| `REINDEX` | A field changed type or any mapping parameter other than `dynamic` (analyzer, `search_analyzer`, format, `ignore_above`, normalizer, `copy_to`, `index`, `doc_values`, `null_value`, `coerce`, `enabled`, ...); a template of a typed map that already exists changed or went away (except a removed map under `RemovedFieldPolicy.KEEP`), or an untyped map became typed; a multi-field was added to an existing field (for example `name.keyword`); a typed property was added under an object whose current mapping is dynamic; or fields were removed under `RemovedFieldPolicy.REINDEX` | New index, copy, verify, swap the alias (below) |
| `BLOCKED` | A concrete index is named like the alias; the alias points at several indices; or a breaking change with `allowReindex` off | Nothing; the outcome's status is `BLOCKED`, the plan's `reason` says why and `MigrationResult.ok()` is false |

An added multi-field, or typed properties over what used to be a dynamic object, need a reindex
because the engine indexes a document into a field only when the document is written: after a
`PUT_MAPPING`, sorts and exact matches on `name.keyword` would silently miss every older document.
The plan lists these fields in `diff().unindexedAdditions()`.

Removed fields are kept in the mapping with a warning by default (`RemovedFieldPolicy.KEEP`).

### Settings changes

Settings (`shards`, `replicas`, `settings_json` such as analysis definitions, and
`MappingOverrides.setting(...)`) are not part of the content hash
([ADR 0006](docs/adr/0006-settings-outside-the-content-hash.md)). A change to settings alone never
produces a new index:

- The plan is `NO_OP` with the warning `settings differ; not applied` (shard and replica
  differences get one warning per key). The library never updates the settings of an existing index.
- Settings are applied whenever an index is created: on `CREATE`, and on the new index of a
  mapping-driven `REINDEX`, so pending settings changes ride along with the next reindex.
- To roll out an analyzer change on its own, make it visible in the mappings: define the analyzer
  under a **new name** in `settings_json` and point the field's `analyzer` (or the message's
  `default_analyzer`) at it. The `analyzer` parameter is in the mappings, so the hash changes and the
  plan is a `REINDEX` into a new index that carries the new analysis settings. Editing an analyzer
  definition in place under the same name is not picked up; neither is any other settings-only edit.
  The alternative is a manual reindex outside the library.

```proto
message Document {
  option (phatjam98.elasticprotos.v1.elastic) = {
    // was "folding"; the new name changes the mapping, so the migrator reindexes
    settings_json: "{\"analysis\":{\"analyzer\":{\"folding_v2\":{\"tokenizer\":\"standard\",\"filter\":[\"lowercase\",\"asciifolding\",\"stop\"]}}}}"
  };
  string body = 4 [(phatjam98.elasticprotos.v1.elastic_field) = { type: TEXT analyzer: "folding_v2" }];
}
```

### Options

| `MigrationOptions` | Default |
|---|---|
| `failure` | `FAIL_FAST` (stop at the first failure); `CONTINUE` attempts every index |
| `allowReindex` | `true`; when false, breaking changes are `BLOCKED` |
| `deleteOldAfter` | `false`: the old index is retained, write-blocked |
| `removedFields` | `KEEP` |
| `defaultShards` | `1` for new indices whose settings set none |
| `replicas` | `ReplicaPolicy.dataNodesMinusOne(1)`: data nodes minus one, between 0 and 1 |
| `reindexTimeout` | 10 minutes |
| `dryRun` | `false` |
| `clearStaleWriteBlock` | `true`: lift a write block found on the alias target when it needs no reindex (see below) |

### Dry run

`plan(...)` never changes anything; print `toReport()` or `toJson()`. To run the whole bootstrap
path without changes:

```java
IndexMigrator dryRun = IndexMigrator.builder()
    .engine(engine)
    .options(MigrationOptions.standard().withDryRun(true))
    .build();
MigrationResult wouldDo = dryRun.bootstrap(Document.class);   // every outcome is DRY_RUN
```

### Reindex and the write-block window

A `REINDEX`:

1. creates the new index without the alias. It gets a generation suffix (see
   [Index naming](#index-naming-and-metadata)) when the alias already points at an index with the
   target name, or when an index with that name exists and is write-blocked: that is an old index
   retained by an earlier reindex, kept for rollback, and it is never reused or deleted. If an
   unblocked index with the target name already exists it is
   a leftover from an earlier run that did not finish: it is reused when its `_meta` records the
   target hash and the alias does not point at it, and otherwise deleted and recreated (logged as a
   warning), because its mappings are unknown. If the alias already points at it, the item fails
   without touching anything (another migration ran in the meantime; plan again);
2. sets a write block on the old index and refreshes it;
3. runs `_reindex` old to new, with the migration script if one is found;
4. checks the copy: the engine must report no failures and no timeout, and without a migration
   script both indices must hold the same number of documents. A script may drop or add documents
   on purpose, so with a script a differing count is logged and added to `Outcome.warnings()`
   instead of failing. Both counts are recorded in `Outcome.sourceCount()` and `destCount()`;
5. swaps the alias atomically;
6. deletes the old index if `deleteOldAfter`, otherwise retains it write-blocked
   (`Outcome.retainedIndices()`).

**Writes through the alias fail from step 2 until step 5.** Plan for that window (retry writes,
pause producers or run migrations in a maintenance slot); zero-downtime dual writes are out of
scope.

If any step fails, the outcome is `FAILED` with a `MigrationException`, the old index is unblocked
and the alias is read again: the new index is deleted only if this run created it and the alias
does not point at it. If the alias swap reports an error but the alias in fact points at the new
index alone (for example a lost response), the migration counts as applied.

The `_reindex` runs as a server-side task that the engine polls. When the `reindexTimeout`
expires, or polling fails, the task is cancelled (`POST /_tasks/{id}/_cancel`) and awaited for up
to 30 seconds before the error is reported, so the cleanup above does not race a copy still in
progress. Transient polling errors (5xx, 429, I/O) are retried three times with a short backoff
first.

**Write-block recovery.** A process killed between steps 2 and 5 leaves the old index, still
behind the alias, write-blocked. The next plan reports a warning for any alias target with
`index.blocks.write=true`. With `clearStaleWriteBlock` (the default), `apply` lifts the block when
that index needs no reindex (`NO_OP` or `PUT_MAPPING`); a `REINDEX` moves the alias to a new,
unblocked index anyway. The option assumes that a write block on a live alias target was left by
an interrupted reindex; turn it off if you set write blocks on live indices yourself.

### Migration scripts

The default provider, `ClasspathScriptProvider`, looks for a Painless script on the classpath, in
this order, for alias `a` migrating from hash `f` to hash `t`:

1. `migrations/<a>/<f>-<t>.painless`
2. `migrations/<a>/<t>.painless`
3. `migrations/<a>/default.painless`

`f` is `legacy` for indices without elastic-protos metadata, and `a` is the alias after the naming
strategy (for example `dev-documents`). Both hashes appear in the plan report and in `toJson()`
(`currentHash`, `targetHash`). Scripts receive the params `alias`, `from_hash` and `to_hash`:

```painless
// src/main/resources/migrations/documents/cccb33c0-5e1f09aa.painless
ctx._source.remove('legacy_field');
```

Supply your own `MigrationScriptProvider` (a functional interface) to load scripts from elsewhere,
or `MigrationScriptProvider.none()` for plain copies.

### Rollback

A retained old index can be put back behind the alias:

```java
for (MigrationResult.Outcome outcome : result.outcomes()) {
  IndexPlan item = outcome.plan();
  if (item.action() == Action.REINDEX && outcome.status() == MigrationResult.Status.APPLIED
      && !outcome.retainedIndices().isEmpty()) {
    migrator.rollback(item);   // alias back to item.currentIndex(), write block lifted
  }
}
```

`rollback(item, previousIndex)` targets any index. The new index is left in place, and documents
written to it after the swap are not copied back.

### Run the bootstrap from one place

Plan and apply are not coordinated across processes. **Running the migrator for the same aliases
from several instances at once is unsupported and can be destructive**: one instance can lift the
write block, or delete the index, that another is reindexing into, and documents written in that
window can be lost. Run the bootstrap from a single instance, a deploy job or an init container,
not from every replica at once. The Micronaut module's `elastic-protos.bootstrap.enabled: false`
turns it off on the other instances.

A reindex can take minutes. Whatever runs it must not be killed mid-copy: on Kubernetes, give the
pod that migrates a startup probe budget (`failureThreshold` x `periodSeconds`) longer than the
`reindexTimeout`, or run the migration as a Job. An interrupted reindex is recovered on the next
run (see [write-block recovery](#reindex-and-the-write-block-window)), but writes fail until then.

## Document codec

`ProtoDocumentCodec` follows protobuf's canonical JSON mapping with proto field names, plus the
decisions recorded in the `IndexDefinition`.

```java
ProtoDocumentCodec codec = ProtoDocumentCodec.of(mapper, CodecConfig.builder()
    .includeDefaults(true)                           // default: proto3 defaults are written
    .int64AsNumber(true)                             // default: int64 as JSON numbers
    .unknownFields(UnknownFieldPolicy.FAIL)          // default is IGNORE
    .build());

Map<String, Object> json = codec.toDocument(Document.getDefaultInstance());
Document back = codec.fromDocument(source, Document.class);
```

| Topic | Behaviour |
|---|---|
| Field names | Proto field names; `elastic_field.name` renames on write and back on read |
| Ignored fields | `IGNORE` fields (and `Any`/`FieldMask`/`Empty`) are dropped from documents |
| Defaults | Proto3 default values are written (`includeDefaults`, default true): `false`, `0`, `""`, the first enum value, `[]` and `{}`. Fields with presence stay absent when unset: sub-messages (never written as `{}`), `Timestamp` (never the epoch), wrappers, `Struct`, proto3 `optional` fields and unset oneof members. `""` is written only to keyword and text fields; date, ip, geo, numeric and binary fields leave it out. See below |
| 64-bit integers | Fields mapped as `long` are written as JSON numbers (`int64AsNumber`, default true) instead of protobuf's strings; values beyond 2^53 lose precision in JavaScript consumers. `uint64`/`fixed64` map to `keyword` and stay strings. A value that does not fit the mapped `long` (an unsigned field forced to `LONG`) throws `CodecException` naming the field |
| `Timestamp` | RFC 3339 strings; epoch-millisecond numbers are accepted when reading back |
| Enums, bytes | Value names; base64 |
| Wrappers, `Struct` | Unwrapped; plain JSON objects |
| `LatLon` | `{"lat": .., "lon": ..}`, always with both coordinates (a 0 coordinate, which canonical JSON omits, is written as `0.0`); reading accepts objects, `[lon, lat]`, `"lat,lon"` and `POINT (lon lat)` |
| `GeoShape` | Its WKT string or its parsed GeoJSON object |
| Unknown keys on read | `UnknownFieldPolicy.IGNORE` (default) drops them, since fields removed from a proto linger in old documents; `FAIL` throws `CodecException` |
| Ids | `codec.idOf(message)` reads the field marked `id` (or named by `id_field`) |

`fromHit(hit, Type.class)` reads a search hit; `fromDocument(source, descriptor)` returns a
`DynamicMessage`.

Why defaults are written: proto3 cannot tell an unset `bool`, number, string or enum from one set
to its default, and protobuf's JSON mapping leaves such values out. An index that never sees them
cannot answer `EQUALS false`, `EQUALS 0`, a `NUM_RANGE` that starts at 0, or `EQUALS
FIRM_STATUS_UNSPECIFIED`, and sorts put those documents with the missing ones. Writing them makes
default values match and sort like any other. The cost is that `EXISTS` on such a field matches
every document; declare the field `optional` (or use a wrapper type) where "was it set" matters,
since fields with presence are still left out when unset. `withIncludeDefaults(false)` (Micronaut:
`elastic-protos.codec.include-defaults: false`) restores the sparse documents. Reading back is the
same either way, so the setting can change without reindexing, but documents written before and
after differ until they are rewritten.

## Query DSL

`SearchCriteria` (in `search_criteria.proto`) is an OR of `SearchCondition`s, each an AND of
`FieldCondition`s, plus sorting and pagination. Build it with the generated builders or with the
fluent `Criteria` helper, and compile it against an `IndexDefinition`:

```java
SearchCriteria criteria = Criteria.builder()
    .condition()
        .where("firm_id").equalTo("firm-1")
        .and("filed_at").between(Instant.parse("2024-01-01T00:00:00Z"), Instant.now())
        .and("tags").not().anyOf("archived", "draft")
        .and("parties.role").equalTo("COUNSEL")       // wrapped in a nested query
    .or()
        .where("title").fullText("merger", o -> o.setOperator(FullTextOperator.OPERATOR_AND))
    .end()
    .sortByScore(SortingOrder.DESC)
    .sortBy("filed_at", SortingOrder.DESC)
    .page(20, 0)
    .build();

CompiledSearch compiled = QueryCompiler.withDefaults().compile(criteria, documents);
SearchResult result = engine.search(List.of(documents.alias()), compiled.body());
```

Each condition becomes a `bool` query: non-scoring predicates go to `filter`, scoring ones to
`must`, negated ones to `must_not`; several conditions are combined with `should` and
`minimum_should_match: 1`. Operations are validated against the mapped field type and unknown fields
throw `QueryCompileException` (or are skipped with a warning when lenient). `Values` builds
`FlatValue` operands for the generated API.

### Operations

| `SearchOperationType` | `Criteria` step | Field types | Compiles to |
|---|---|---|---|
| `EQUALS` | `equalTo(v)`, `isNull()` | exact fields: keyword, numeric, boolean, date, text with `.keyword`, flattened and its keys | `term` (on `.keyword` for text); null means "missing" |
| `EXISTS` | `exists()` | any | `exists`; on a nested field, "has an element" |
| `TEXT_MATCHES` | `matches(text)` | text (scoring), keyword | `match` |
| `TEXT_CONTAINS` | `contains(text)` | text; keyword needs `allowLeadingWildcards` | `match_phrase` on text, `*text*` wildcard on keyword |
| `TEXT_PREFIX` | `startsWith(p)` | keyword or text with `.keyword` | `prefix` |
| `TEXT_SUFFIX` | `endsWith(s)` | keyword or text with `.keyword`; needs `allowLeadingWildcards` | `*s` wildcard |
| `TEXT_REGEX` | `regex(pattern)` | keyword or text with `.keyword`; needs `allowRegex` | `regexp` |
| `NUM_RANGE` | `between(min, max)`, `between(Instant, Instant)`, `range(...)` | numeric, date | `range` with both bounds |
| `ANY_IN` | `anyOf(a, b, ...)` | exact fields | `terms` |
| `GEO_DISTANCE` | `withinDistance(lat, lon, "10km")` | `geo_point` | `geo_distance` |
| `GEO_BOUNDING_BOX` | `inBoundingBox(top, left, bottom, right)`, `inBoundingBox(wkt)` | `geo_point` | `geo_bounding_box` |
| `GEO_INTERSECTS` / `GEO_CONTAINS` / `GEO_CONTAINED` | `intersects(shape)`, `containsShape(shape)`, `withinShape(shape)` | `geo_shape` | `geo_shape` with relation `intersects` / `contains` / `within` |
| `FULL_TEXT` | `Criteria.fullText(q)`, or `where(f).fullText(q[, options])` | text or keyword | `multi_match` |

Notes:

- **Negation**: `not()` (`FieldCondition.negate`) moves the predicate to `must_not`.
- **Full text**: with no field, `FULL_TEXT` searches every field in the index's full-text set (all
  `TEXT` fields plus `full_text: TRUE` fields) with their `boost`. `FullTextOptions` sets explicit
  fields with boosts, `operator`, `fuzziness`, `type` (`BEST_FIELDS`, `PHRASE`, ...) and
  `minimum_should_match_pct`. `Criteria.fullText(q)` is added to every branch.
- **Nested**: predicates on fields under `nested` paths are wrapped in `nested` queries, once per
  level. Predicates of one condition on the same nested path are grouped so they must match the same
  element (`QueryCompilerConfig.groupNested`, default on). Full-text fields are grouped per nested
  path.
- **Ranges**: proto3 cannot express an unset `double`, so `NUM_RANGE` always emits both bounds; use
  very small or large values for open ranges. Date ranges are epoch milliseconds.
- **Flattened keys**: a path below a `flattened` field (a `map` or `Struct` by default), such as
  `labels.env`, needs no mapping entry. It takes `EQUALS`, `ANY_IN`, `EXISTS` and `TEXT_PREFIX`,
  compared as keywords (`labels.count` equal to `3` is the string `"3"`); other operations on a key
  throw `QueryCompileException`. Map the field with `type: OBJECT` for typed ranges or full text.
- **Typed map keys**: a path below a typed map (`type: OBJECT` or `NESTED` on a `map`), such as
  `counters.views` or `children.alice.count`, takes every operation of the value type: `NUM_RANGE`
  on a `long`, `TEXT_MATCHES` on `text`, `GEO_DISTANCE` on a `geo_point`, and so on. A nested
  field inside the values (`children.alice.items`) is wrapped in a `nested` query with
  `ignore_unmapped: true`, because it is mapped only once a document uses the key; a sort on a key
  adds `unmapped_type` for the same reason, and sorts under such a nested field are rejected.
  `FULL_TEXT` searches a key when it is listed (`notes.summary`), all keys through the pattern
  (`notes.*`), and, opt-in with `full_text: TRUE` on the map, the pattern as part of the default
  field set (text leaves `children.*.name` for message values). The engine expands a pattern such as
  `notes.*` to every mapped field below it, which includes the `notes.<key>.keyword` sub-fields, so
  an exact keyword match on a whole note also scores. Geo queries on a key add `ignore_unmapped:
  true`, so a key no document has used matches nothing instead of failing. A key segment holding
  `*` is not a key. An unknown field below a typed map names the value type in the error.
- **Leading wildcards** scan every term and are off by default.
- **Client-supplied criteria**: criteria often come straight from API callers, so the features that
  run code or can scan a whole field are opt-in. `TEXT_REGEX` needs `allowRegex`, and a pattern
  whose alternatives do not all start with a literal character (`.*foo`, `[a-z]+`, `(a|b)c`, `~a`)
  also needs `allowLeadingWildcards`. Script sorts need `allowScriptSort`. Without the flag the
  compiler throws `QueryCompileException` naming it.

### Sorting

| `SortingCriteria` | `Criteria` | Notes |
|---|---|---|
| `field_sort` | `sortBy(field, order)` | Text fields sort by `.keyword`; fields under `nested` paths get the `nested` sort clause |
| `score_sort` | `sortByScore(order)` | `_score` |
| `geo_distance_sort` | `sortByDistance(field, lat, lon, unit, order)` | `geo_point` fields |
| `script_sort` | generated builder | Needs `allowScriptSort`. `source` is inline Painless, `id` a stored script; `STRING` or `NUMBER` |

### Pagination

| Case | Behaviour |
|---|---|
| `size` 0 / negative | `defaultSize` (10) / 0 |
| `size` above `maxPageSize` (10 000) | `QueryCompileException`, with or without a point in time; `defaultSize` above it fails when the config is built and a `SearchPager` page size above it fails before the point in time is opened |
| `from` + `size` within `maxResultWindow` (10 000) | `from`/`size` paging |
| `from` + `size` beyond the window | `QueryCompileException` that points at point-in-time paging |
| `cursor` set (a point-in-time id) | `pit` + `search_after`, with a `_shard_doc` tiebreaker appended |

`SearchPager` does deep, consistent paging for you. It opens a point in time, feeds each page's last
sort values back as `search_after`, follows rotated PIT ids, and closes the PIT on `close()`. It adds
the `_shard_doc` tiebreaker only where `EngineInfo.supportsShardDoc()` (Elasticsearch 7.12+,
OpenSearch 3.3+); elsewhere, end the sort with a unique field (as here) so no hit is skipped.

```java
SearchCriteria all = Criteria.where("firm_id").equalTo("firm-1")
    .end()
    .sortBy("id", SortingOrder.ASC)
    .build();
try (SearchPager pager = SearchPager.open(engine, QueryCompiler.withDefaults(),
    List.of(documents.alias()), all, documents, 500)) {
  while (pager.hasNext()) {
    for (Hit hit : pager.next().hits()) {
      Document doc = codec.fromHit(hit, Document.class);
    }
  }
}
```

To page by hand, `PaginationSupport.next(pagination, result)` returns the pagination of the next
request. If you set `cursor` yourself on OpenSearch before 3.3, turn the tiebreaker off:

```java
QueryCompiler compiler = QueryCompiler.of(QueryCompilerConfig.builder()   // starts from standard()
    .lenient(true)                        // skip unknown fields with a warning
    .allowLeadingWildcards(true)          // enable TEXT_SUFFIX / keyword TEXT_CONTAINS
    .maxResultWindow(50_000)              // match index.max_result_window
    .maxPageSize(1_000)                   // largest page a caller may ask for
    .allowRegex(true)                     // enable TEXT_REGEX (literal prefix required)
    .allowScriptSort(true)                // enable script sorts: callers run Painless
    .trackTotalHits(TrackTotalHits.upTo(10_000))   // also exact(), none(), engineDefault()
    .shardDocTiebreaker(false)            // OpenSearch < 3.3 with your own cursor
    .build());
```

`compiled.body()` is an unmodifiable `Map`; copy it to add aggregations (`aggs`) or anything else the DSL does not cover;
raw aggregation results come back in `SearchResult.aggregations()`. `SearchResult.total()` is exact
when `totalExact()` is true and a lower bound otherwise. With `track_total_hits: false`
(`withTrackTotalHits(TrackTotalHits.none())`) the engine counts nothing: `total()` is then
`SearchResult.NO_TOTAL` (`-1`), `totalExact()` is false and `hasTotal()` is false.

## Engines

### Support matrix

Every pull request runs the shared `EngineIntegrationSpec` against these images:

| Engine | Image | Adapter |
|---|---|---|
| Elasticsearch 8.19 | `docker.elastic.co/elasticsearch/elasticsearch:8.19.23` | `elastic-protos-elasticsearch` |
| Elasticsearch 9.5 | `docker.elastic.co/elasticsearch/elasticsearch:9.5.5` | `elastic-protos-elasticsearch` |
| OpenSearch 2.19 | `opensearchproject/opensearch:2.19.7` | `elastic-protos-opensearch` |
| OpenSearch 3.9 | `opensearchproject/opensearch:3.9.0` | `elastic-protos-opensearch` |

Other 8.x/9.x and 2.x/3.x releases are expected to work but are not tested.

### Connecting

```java
SearchEngine es = ElasticsearchEngine.builder()
    .hosts("https://es-1.example.com:9200", "https://es-2.example.com:9200")
    .apiKey(System.getenv("ES_API_KEY"))          // base64 or "id:api_key"
    .sslContext(myCa)                             // optional: trust a private CA
    .build();

SearchEngine os = OpenSearchEngine.builder()
    .hosts("https://opensearch.example.com:9200")
    .basicAuth("admin", System.getenv("OS_PASSWORD"))
    .build();
```

Both builders offer `hosts`, `basicAuth`, `apiKey` (sent as `Authorization: ApiKey ...`),
`sslContext` and `insecureTrustAllCertificates` (local development only); `build()` refuses the two
together, since trusting every certificate would ignore the context. Building does not connect.
To reuse a client you already configured, wrap it; each adapter brings its engine's low-level REST
client (`org.elasticsearch.client:elasticsearch-rest-client` or
`org.opensearch.client:opensearch-rest-client`) transitively, so there is nothing else to declare:

```java
RestClient client = RestClient.builder(HttpHost.create("http://localhost:9200")).build();
SearchEngine engine = ElasticsearchEngine.of(client);   // closing the engine closes the client
```

`SearchEngine` is the whole SPI: index lifecycle, mappings, aliases, reindex, write blocks, document
writes, realtime `get` by id, `bulk`, `search`, `count`, point in time, `refresh` and `health`, all
as JSON-shaped maps. Transport failures are `EngineException`s of kind `IO` that name the request
and the hosts, for example `GET /documents/_alias on http://localhost:9200 failed: No route to host`.
Engine errors are classified by `EngineException.getKind()`. A `cluster_block_exception` with a 4xx
status (a write, read or read-only block, answered with 403, or the flood-stage disk watermark's
`read_only_allow_delete` block, answered with 429) and an `index_closed_exception` are `BLOCKED`, not
`UNAUTHORIZED` or `TRANSIENT`: retrying does not help until the block is lifted, and the reindex poll
loop does not retry them. A 5xx `cluster_block_exception` (state not recovered, no master) is
`TRANSIENT` and is retried.

```java
try (BulkBuffer buffer = BulkBuffer.builder()
    .engine(engine)
    .maxActions(1000)
    .flushInterval(Duration.ofSeconds(1))
    .listener(result -> result.failures().forEach(System.err::println))
    .errorListener((batch, error) -> retryLater.addAll(batch))
    .build()) {
  for (Document doc : docs) {
    buffer.add(BulkOp.index(documents.alias(), codec.idOf(doc).orElseThrow(),
        codec.toDocument(doc)));
  }
}
```

Failed items arrive in each `BulkResult`. When a whole bulk request fails (the engine call throws),
the error listener receives an immutable copy of the batch and the exception; the buffer drops the
batch from its queue and never retries it, so retry or park the operations there. The default error
listener logs them. A failure of the final flush in `close()` goes to the error listener and is
then thrown from `close()` as an `EngineException`.

### OpenSearch differences

Handled in core, so mappings, hashes and tests are identical on both engines:

- `flattened` is sent to OpenSearch as `flat_object` (OpenSearch 2.7+) and read back as `flattened`.
- The `_shard_doc` point-in-time tiebreaker exists only on OpenSearch 3.3+ (absent in 2.19 and 3.2);
  `SearchPager` skips it there.
- Opening and closing a point in time use each engine's own API.
- `apiKey` sends an `ApiKey` authorization header; whether OpenSearch accepts it depends on its
  security configuration. Basic auth is the usual choice.

## Micronaut

```groovy
dependencies {
    implementation platform('com.phatjam98:elastic-protos-bom:1.0.0')
    implementation 'com.phatjam98:elastic-protos-micronaut'
    implementation 'com.phatjam98:elastic-protos-elasticsearch'   // or elastic-protos-opensearch
}
```

```yaml
elastic-protos:
  engine: elasticsearch
  hosts:
    - https://es.example.com:9200
  api-key: ${ES_API_KEY}
  bootstrap:
    classes:
      - com.acme.search.Document
  naming:
    prefix: ${ENVIRONMENT:}
```

A default in a `${VAR:default}` placeholder that contains a colon, such as a URL, must be wrapped
in backticks, because Micronaut splits the placeholder at colons:
``hosts: ${ELASTIC_PROTOS_HOSTS:`http://localhost:9200`}``.

The module requires a Micronaut 4 application, which supplies the Micronaut runtime and its
versions (through the Micronaut Gradle plugin or
`platform('io.micronaut.platform:micronaut-platform:4.x')`); the module itself publishes no
Micronaut dependency. It is tested against Micronaut platform 4.10.x.

A complete, runnable application with REST endpoints is in
[`examples/micronaut-sample`](examples/micronaut-sample).

### Beans

| Bean | Source |
|---|---|
| `SearchEngine` | `EngineFactory`: the Elasticsearch or OpenSearch adapter per `elastic-protos.engine`, only if that adapter is on the classpath; `@Secondary`; closed on shutdown |
| `MappingDefaults`, `ProtoMapper`, `ProtoDocumentCodec`, `QueryCompilerConfig`, `QueryCompiler`, `MigrationOptions`, `IndexMigrator` | `ElasticProtosFactory`, from the properties below |
| `ElasticProtosService` | Repository-style access (below) |
| `ElasticProtosBootstrap` | Runs the bootstrap on `StartupEvent` |

All of them require a `SearchEngine` bean. Optional beans you may define:

| Your bean | Effect |
|---|---|
| `IndexNamingStrategy` | Used instead of `elastic-protos.naming.prefix` |
| `MappingOverrides` | Passed to the `ProtoMapper` |
| `MigrationScriptProvider` | Replaces the classpath script provider |
| `javax.net.ssl.SSLContext` named `elastic-protos` | TLS for the engine, for example a private CA |

To replace any factory bean entirely, annotate yours with `@Replaces`.

### Replacing the engine

Because the library's engine beans are `@Secondary`, any `SearchEngine` bean of your own wins, with
no `@Replaces` needed:

```java
@Factory
class MyEngineFactory {

  @Singleton
  @Bean(preDestroy = "close")
  SearchEngine searchEngine() {
    // Any SearchEngine bean you define wins over the library's @Secondary engine beans.
    return OpenSearchEngine.builder()
        .hosts("https://opensearch.internal:9200")
        .basicAuth("app", System.getenv("OS_PASSWORD"))
        .build();
  }
}
```

### Startup bootstrap

`ElasticProtosBootstrap` loads the classes in `elastic-protos.bootstrap.classes`, logs the plan
report at INFO and applies it. With `fail-on-error` (default) a failed or blocked index throws
`ElasticProtosBootstrap.BootstrapException` and aborts startup, and so does an exception from
planning or applying (for example an unreachable cluster or rejected credentials). With
`fail-on-error: false` every such error is logged at WARN and skipped, including deterministic ones
such as a `MappingException` from invalid proto options: the application starts without the index
and writes to it fail later. Keep the default `true` in deployments. An empty class list skips the
bootstrap; `enabled: false` removes the listener.

The bootstrap is enabled by default and runs in every instance that starts. **Concurrent bootstrap
from several instances is unsupported and can be destructive** (see
[Run the bootstrap from one place](#run-the-bootstrap-from-one-place)). With more than one replica,
set `elastic-protos.bootstrap.enabled: false` on all but one instance, or on all of them and run
the migration from a dedicated job. Because the bootstrap runs during startup, a reindex delays
readiness: on Kubernetes, make the startup probe budget longer than
`elastic-protos.bootstrap.reindex-timeout` so the pod is not killed mid-copy.

### ElasticProtosService

```java
@Singleton
public class DocumentRepository {

  private final ElasticProtosService elastic;

  public DocumentRepository(ElasticProtosService elastic) {
    this.elastic = elastic;
  }

  public void save(Document doc) {
    elastic.index(doc, IndexOptions.defaults());
  }

  public Optional<Document> find(String id) {
    return elastic.get(id, Document.class);
  }

  public SearchPage<Document> search(String text) {
    return elastic.search(Criteria.builder().fullText(text).pageSize(20).build(),
        Document.class);
  }

  public void saveAll(List<Document> docs) {
    try (BulkBuffer buffer = elastic.bulkBuffer(
        result -> result.failures().forEach(item -> log.warn("{}: {}", item.id(), item.error())),
        (batch, error) -> log.error("bulk request of {} operations failed", batch.size(), error))) {
      docs.forEach(doc -> buffer.add(elastic.indexOp(doc)));
    }
  }
}
```

`get` is realtime: it reads through the document API (`GET /{alias}/_doc/{id}`), not a search, so
it sees a write or a delete as soon as the call that made it returns, without a refresh. The alias
must point at one index. `delete(id, type, options)` takes `IndexOptions` for its refresh policy,
for example `IndexOptions.refreshed()` to drop the document from searches before it returns;
`delete(id, type)` does not refresh.

`bulkBuffer()` logs failed items and failed requests; `bulkBuffer(listener)` sends every batch's
`BulkResult` to a `Consumer<BulkResult>`, and `bulkBuffer(listener, errorListener)` also sends each
failed bulk request's batch and exception to a `BiConsumer<List<BulkOp>, Throwable>` (the
`Consumer<Throwable>` overload is deprecated). Failed batches are not retried, and `close()` throws
when the final batch fails. Listeners run on the thread that sends the batch: the caller of `add` or
`close`, or the flush timer.

It also offers `bulkIndex`, `definition`, `pager` (a `SearchPager`), `plan`, `bootstrap`,
`codec()` and `engine()`. `SearchPage` carries the messages, the raw hits, the total and the
`next` pagination.

### Properties

The configuration is bound and checked when the application context starts, before any
elastic-protos bean is used: an unparseable value such as `track-total-hits: maybe`, or settings
that do not fit together such as a `max-page-size` below `default-size`, abort
`ApplicationContext.start()` with the reason.

| Property | Default | Meaning |
|---|---|---|
| `elastic-protos.engine` | `elasticsearch` | `elasticsearch` or `opensearch` |
| `elastic-protos.hosts` | `http://localhost:9200` | List of URLs |
| `elastic-protos.username` / `password` | none | Basic authentication |
| `elastic-protos.api-key` | none | API key, base64 or `id:key` |
| `elastic-protos.ssl.insecure-trust-all-certificates` | `false` | Disable certificate and hostname checks; never in production |
| `elastic-protos.bootstrap.enabled` | `true` | Register the startup bootstrap |
| `elastic-protos.bootstrap.classes` | empty (skips the bootstrap) | Fully qualified generated message classes |
| `elastic-protos.bootstrap.failure-policy` | `FAIL_FAST` | Or `CONTINUE` |
| `elastic-protos.bootstrap.allow-reindex` | `true` | Apply breaking changes by reindexing |
| `elastic-protos.bootstrap.delete-old` | `false` | Delete the old index after a successful reindex |
| `elastic-protos.bootstrap.removed-fields` | `KEEP` | Or `REINDEX` |
| `elastic-protos.bootstrap.dry-run` | `false` | Plan and log only |
| `elastic-protos.bootstrap.clear-stale-write-block` | `true` | Lift a write block on an alias target that needs no reindex, assumed left by an interrupted reindex |
| `elastic-protos.bootstrap.fail-on-error` | `true` | Abort startup when an index fails or is blocked |
| `elastic-protos.bootstrap.reindex-timeout` | `10m` | How long a reindex may take |
| `elastic-protos.defaults.shards` | `1` | Shards of new indices (message options win) |
| `elastic-protos.defaults.replicas` | `-1` | Replicas of new indices; negative means data nodes minus one, at most 1 |
| `elastic-protos.naming.prefix` | empty | Prefix for every alias and index, joined with `-` |
| `elastic-protos.mapping.strings-as-text` | `false` | Map plain strings to `text` |
| `elastic-protos.mapping.keyword-subfield` | `true` | Give text fields a `.keyword` sub-field |
| `elastic-protos.mapping.ignore-above` | `256` | `ignore_above` of keyword fields |
| `elastic-protos.mapping.max-depth` | `5` | How deep nested messages are expanded |
| `elastic-protos.codec.include-defaults` | `true` | Write proto3 default values (`false`, `0`, `""`, first enum value, empty lists and maps) so `EQUALS`, ranges and sorts see them; `false` omits them and makes `EXISTS` mean "was set" (see [Document codec](#document-codec)) |
| `elastic-protos.codec.int64-as-number` | `true` | Write 64-bit integers as JSON numbers |
| `elastic-protos.codec.unknown-fields` | `IGNORE` | Or `FAIL` when reading back |
| `elastic-protos.query.allow-leading-wildcards` | `false` | Allow wildcard queries starting with `*` |
| `elastic-protos.query.lenient` | `false` | Skip unmapped fields with a warning |
| `elastic-protos.query.max-result-window` | `10000` | Largest `from + size` without a point in time |
| `elastic-protos.query.default-size` | `10` | Page size when the criteria set none |
| `elastic-protos.query.default-keep-alive` | `1m` | Point-in-time keep-alive |
| `elastic-protos.query.track-total-hits` | `true` | `true` counts hits exactly, `false` counts none, a number counts up to that bound, `default` leaves the key out so the engine's default applies |
| `elastic-protos.query.shard-doc-tiebreaker` | `AUTO` | `_shard_doc` tiebreaker on point-in-time searches: `AUTO`, `ON` or `OFF` |
| `elastic-protos.query.allow-script-sort` | `false` | Accept script sorts; callers then run Painless on the cluster |
| `elastic-protos.query.allow-regex` | `false` | Accept `TEXT_REGEX` (a pattern without a literal prefix also needs `allow-leading-wildcards`) |
| `elastic-protos.query.max-page-size` | `10000` | Largest page size, with or without a point in time |
| `elastic-protos.bulk.actions` | `500` | `bulkBuffer(...)` sends a batch at this many operations |
| `elastic-protos.bulk.flush-interval` | `1s` | Also send at this interval; `0s` disables |
| `elastic-protos.bulk.max-bytes` | `5MB` | Send a batch at about this size |

With `shard-doc-tiebreaker: AUTO`, `ElasticProtosService` asks the engine once, on the first search
with a `cursor`, whether it supports `_shard_doc` and leaves the tiebreaker out if not (OpenSearch
before 3.3); `pager(...)` always adapts.

## Limitations

- A reindex blocks writes for its duration; there is no dual-write, zero-downtime mode.
- Settings of existing indices (shards, replicas, analysis) are never changed by a migration;
  settings-only changes need a mapping-visible step (see [Settings changes](#settings-changes)).
- Keys of typed maps must not contain dots; dynamic keys count toward
  `index.mapping.total_fields.limit` (and nested fields in their values toward
  `index.mapping.nested_fields.limit`) at index time; inner maps inside typed map values stay untyped.
- Plans are not coordinated across processes; concurrent bootstrap is unsupported and can be
  destructive. Run the bootstrap from one place.
- `NUM_RANGE` has no open-ended bounds (proto3 `double` has no presence).
- The DSL covers queries, sorting and paging; aggregations, highlighting and the like go into the
  body map by hand.

## Building from source

```bash
./gradlew build                      # compile, Checkstyle, unit tests, coverage gates, Javadoc
./gradlew :elastic-protos-elasticsearch:integrationTest -PengineImage=docker.elastic.co/elasticsearch/elasticsearch:8.19.23
./gradlew :elastic-protos-opensearch:integrationTest -PengineImage=opensearchproject/opensearch:2.19.7
./gradlew :elastic-protos-testkit:demo --args="opensearch http://localhost:9200"
./gradlew publishToMavenLocal
```

JDK 17 is the toolchain (`.sdkmanrc`); integration tests need Docker and are skipped without it
unless `-PrequireDocker=true` is passed. See
[CONTRIBUTING.md](CONTRIBUTING.md) for the full workflow and [RELEASING.md](RELEASING.md) for
releases.

### Example application

[`examples/micronaut-sample`](examples/micronaut-sample) is a runnable Micronaut application
(REST endpoints for index, get, delete, search and bulk, startup bootstrap, Docker Compose files
for both engines). It is a standalone build that consumes the published artifacts:

```bash
./gradlew publishToMavenLocal
cd examples/micronaut-sample
docker compose --profile elasticsearch up -d --wait
./gradlew run -Dseed.enabled=true
curl -s 'http://localhost:8080/documents/search?q=merger%20agreement'
```

## Contributing and license

Contributions are welcome: read [CONTRIBUTING.md](CONTRIBUTING.md) and the
[Code of Conduct](CODE_OF_CONDUCT.md). Report vulnerabilities privately as described in
[docs/SECURITY.md](docs/SECURITY.md). Design decisions are recorded in [docs/adr](docs/adr/README.md),
and changes in [CHANGELOG.md](CHANGELOG.md) and the
[GitHub releases](https://github.com/phatjam98/elastic-protos/releases).

Licensed under the [Apache License, Version 2.0](LICENSE).
