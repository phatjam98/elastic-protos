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
| **elastic-protos** | The `.proto` is the schema: mappings, migrations, documents and queries derive from it | Covers indexing and search, not every engine API; a zero-downtime reindex serves slightly stale reads until it has reconciled (see [Limitations](#limitations)) |

The query DSL is itself a proto (`SearchCriteria`), so a search request can cross a gRPC boundary
unchanged and be compiled on the server against the index definition.

## Features

- Index mappings derived from proto descriptors, configured with proto options
  (`elastic`, `elastic_field`) and optional programmatic overrides; no field-name guessing. Maps
  are `flattened` by default, or typed maps whose keys take the value type.
- Content-hash-versioned indices (`documents-v1a2b3c4d`) behind a stable alias, with a recorded
  `_meta` block.
- Migrations that plan before they act: `CREATE`, `NO_OP`, `PUT_MAPPING`, `UPDATE_SETTINGS`
  (dynamic settings in place), `BACKFILL` (in-place update by query), `REINDEX` (with optional
  Painless scripts) or `BLOCKED`, a printable dry-run report, and rollback. By default a reindex
  never blocks writes to the live index: it copies, catches up, swaps the alias and reconciles
  with conditional writes, and resumes after a crash. A per-alias lock lets several instances
  bootstrap at once.
- `ProtoDocumentCodec`: message to document and back, honouring renamed and ignored fields, geo
  types, 64-bit integers and timestamps.
- `QueryCompiler`: `SearchCriteria` to a search body, validated against the mapped field types, with
  full-text search, negation, automatic `nested` wrapping, sorting, from/size and point-in-time paging,
  aggregations (terms, histograms, ranges, metrics) read back through typed `AggregationResults`, and
  highlighting.
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

`1.0.0-rc.3` is the current pre-release. The Maven coordinates and the proto option names will not
change before `1.0.0`, which follows once the release candidate has been exercised. See
[Maven Central](https://central.sonatype.com/artifact/com.phatjam98/elastic-protos-core) for the
latest version.

### Gradle

```groovy
plugins {
    id 'java'
    id 'com.google.protobuf' version '0.10.0'
}

dependencies {
    implementation platform('com.phatjam98:elastic-protos-bom:1.0.0-rc.3')
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
      <version>1.0.0-rc.3</version>
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
| `map<K, V>` | `flattened` (OpenSearch: `flat_object`); keys are queried by dotted path (`labels.env`). `type: OBJECT` (or `NESTED`) maps a typed map: an `object` (or `nested`) field with `dynamic: true` plus `dynamic_templates` that give every key the value type |
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
or full text on map values. The map becomes a typed map: an `object` (or `nested`) field with
`dynamic: true` and, on
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
  first 8 hex digits of a SHA-256 over the canonical `dynamic` + `properties` mapping (plus
  `dynamic_templates` when the index has typed maps). Any mapping
  change gives a new hash; settings and the naming prefix do not affect it (see
  [Settings changes](#settings-changes)).
- `IndexNamingStrategy.prefixed("dev")` prefixes both alias and index (`dev-documents`,
  `dev-documents-vcccb33c0`), for example per environment. The prefix is lower-cased, and one that
  cannot start an index name (empty; leading `-`, `_` or `+`; whitespace or any of
  `\ / * ? " < > | , # :`) is rejected with `IllegalArgumentException`. Implement
  `IndexNamingStrategy` for anything else.
- The migration lock index is `elastic-protos-locks`, or `<prefix>-elastic-protos-locks` with a
  prefix (`IndexNamingStrategy.lockIndex()`; see
  [Running from several instances](#running-from-several-instances)).
- `PUT_MAPPING` and `BACKFILL` extend an index in place, so the index keeps the name of the mapping it was created
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

Indices without this block (created by other tools) are treated as hash `legacy`. While a
backfill or a zero-downtime reindex is in progress, `elastic_protos` also carries a `pending`
object that lets the next run resume it (see [Zero-downtime reindex](#zero-downtime-reindex));
the migrator writes the whole `_meta` object on every update, since the engines replace it.

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
| `PUT_MAPPING` | Only fields were added that existing documents can be indexed into (new top-level fields, new properties under an object that already has typed properties, new typed maps with their `dynamic_templates`, new fields in the values of typed maps), or only `dynamic` changed | Extends the mapping of the current index in place (after updating dynamic settings that differ) |
| `UPDATE_SETTINGS` | The mappings match, and dynamic settings the proto declares differ ([Settings changes](#settings-changes)) | `PUT /<index>/_settings` with the differing dynamic settings |
| `BACKFILL` | Fields were added that existing documents were never indexed into: a multi-field added to an existing field (for example `name.keyword`), or a typed property added under an object whose current mapping is dynamic; nothing else needs a reindex | Extends the mapping in place, then re-indexes every document of the current index with `_update_by_query` (below) |
| `REINDEX` | A field changed type or any mapping parameter other than `dynamic` (analyzer, `search_analyzer`, format, `ignore_above`, normalizer, `copy_to`, `index`, `doc_values`, `null_value`, `coerce`, `enabled`, ...); a template of a typed map that already exists changed or went away (except a removed map under `RemovedFieldPolicy.KEEP`), or an untyped map became typed; fields were removed under `RemovedFieldPolicy.REINDEX`; a `BACKFILL` with `backfill` off or a migration script for the change; or static settings differ under `StaticSettingsPolicy.REINDEX` | New index, copy, swap the alias, reconcile ([Zero-downtime reindex](#zero-downtime-reindex)) |
| `BLOCKED` | A concrete index is named like the alias; the alias points at several indices; or a breaking change with `allowReindex` off | Nothing; the outcome's status is `BLOCKED`, the plan's `reason` says why and `MigrationResult.ok()` is false |

An added multi-field, or typed properties over what used to be a dynamic object, need more than a
`PUT_MAPPING` because the engine indexes a document into a field only when the document is
written: sorts and exact matches on `name.keyword` would silently miss every older document. The
plan lists these fields in `diff().unindexedAdditions()`.

A `BACKFILL` keeps the index and its name:

1. it puts the target mappings with `_meta.elastic_protos.pending = {"action": "backfill",
   "to_hash": "<target hash>"}` (both engines replace `_meta` as a whole on a mapping update);
2. runs `_update_by_query` over every document (`conflicts=proceed`, refresh, `reindexTimeout`),
   which re-indexes each one under the new mapping. A document written concurrently counts as a
   version conflict: its own write already indexed it under the new mapping. The result must be
   complete: no failures, no timeout, and every document counted;
3. writes `_meta` again without `pending`.

A run that dies or fails between 1 and 3 leaves the marker, and the next plan is `BACKFILL` again
even though the hash is current. A backfill is not gated by `allowReindex`. With `backfill` off,
or when a [migration script](#migration-scripts) exists for the change (a script means the
documents change), the item is a `REINDEX` instead.

Turning a container's `dynamic` from `false` to `true` stays a `PUT_MAPPING`, with a warning:
values that existing documents carry under it unmapped stay unindexed until those documents are
written again.

Removed fields are kept in the mapping with a warning by default (`RemovedFieldPolicy.KEEP`).

### Settings changes

Settings (`shards`, `replicas`, `settings_json` such as analysis definitions, and
`MappingOverrides.setting(...)`, with or without the `index.` prefix, nested or dotted) are not part
of the content hash ([ADR 0006](docs/adr/0006-settings-outside-the-content-hash.md)); they are
compared with the live index on every plan, key by key, after flattening both sides to dotted keys
(`analysis.analyzer.folding.tokenizer` is one key). Values compare as strings.

- **Only keys the proto declares are compared.** The `replicas` option of `MigrationOptions` (and
  `defaultShards`) applies only when an index is created, so a replica policy such as
  `dataNodesMinusOne` never shows up as a change. `number_of_replicas` is not compared while the
  proto sets `auto_expand_replicas` to anything but `false`, nor while the index has one and the
  proto declares none (with a warning). Removing a setting from the proto never resets it on the
  index.
- **Dynamic settings are applied in place.** A plan whose mappings match but whose dynamic settings
  differ is `UPDATE_SETTINGS` (`PUT /<index>/_settings`, under the alias lock); a `PUT_MAPPING` or
  `BACKFILL` updates the dynamic settings first (so a raised `mapping.total_fields.limit` is in
  place before the fields arrive); a `REINDEX` creates the new index with all settings. Dynamic
  keys: `number_of_replicas`, `auto_expand_replicas`, `refresh_interval`, `search.idle.after`, the
  `max_*` limits (`max_result_window`, `max_inner_result_window`, `max_rescore_window`,
  `max_docvalue_fields_search`, `max_script_fields`, `max_ngram_diff`, `max_shingle_diff`,
  `max_terms_count`, `max_regex_length`, `max_refresh_listeners`, `max_slices_per_scroll`),
  `analyze.max_token_count`, `highlight.max_analyzed_offset`, `query.default_field`,
  `default_pipeline`, `final_pipeline`, `gc_deletes`, `hidden`, `priority`,
  `write.wait_for_active_shards`, `unassigned.node_left.delayed_timeout`, the `routing.allocation`
  and `routing.rebalance` settings, the `mapping.*.limit` settings, `translog.durability`,
  `translog.flush_threshold_size`, and keys under `routing.allocation.include|exclude|require.`,
  `search.slowlog.`, `indexing.slowlog.` and `merge.`, and `soft_deletes.retention_lease.period`
  (`DiffClassifier.DYNAMIC_SETTINGS` and `DYNAMIC_SETTING_PREFIXES`).
  `MigrationOptions.extraDynamicSettings` adds keys (a leading `index.` is stripped; an entry
  ending in `.` is a prefix). Candidates are engine-specific dynamic settings the built-in list
  leaves out: on Elasticsearch `lifecycle.` and `mapping.total_fields.ignore_dynamic_beyond_limit`,
  on OpenSearch `plugins.index_state_management.` and `search.default_pipeline`.
- **Static settings** (everything else: `number_of_shards`, `analysis.*`, `codec`, `sort.*`,
  `similarity.*`, unknown keys) can only be set when an index is created. With
  `staticSettings(StaticSettingsPolicy.WARN)` (the default) a difference is the warning `static
  settings differ; not applied: <key before -> after, ...>`, and the dynamic part is still applied.
  With `StaticSettingsPolicy.REINDEX` the item is a `REINDEX` into the next generation of the index
  name (the hash does not change, so the new index is `<name>-g2`, `-g3`, ...), with the reason
  `static settings changed: ...`, or `BLOCKED` when `allowReindex` is off. The warning is left out
  when the item reindexes anyway, since the new index gets every setting.
- Every index the migrator creates records a digest of the declared static settings in
  `_meta.elastic_protos.static_settings` (the first 8 hex digits of the SHA-256 of their canonical
  flattened key/value map). When it is present, planning compares the declared static settings
  with the digest rather than with the values the engine reports, so an engine that spells a value
  differently cannot make `REINDEX` loop; indices without it are compared by value. Under
  `StaticSettingsPolicy.REINDEX` a new index whose engine-reported static values still differ from
  the declaration fails the item ("engine reports `<v>` for `<key>`; declare it that way") and is
  deleted.
- Keys the engine or the migrator manages (`blocks.*`, `uuid`, `creation_date`, `version.*`,
  `provided_name`) are never applied; a warning names them when their value differs.
- For dynamic keys, equivalent values in another spelling (`5000ms` against `5s`) compare as
  different, so they are applied again on every run: harmless, but noisy. Declare them the way the
  engine reports them. Static keys are covered by the digest above.
- The plan's `diff().settings()` lists the dynamic and static changes and the ignored keys, and
  `toJson()` includes them.

An analyzer change can also be rolled out as a mapping change, which needs no static-settings
policy: define the analyzer under a **new name** in `settings_json` and point the field's
`analyzer` (or the message's `default_analyzer`) at it. The `analyzer` parameter is in the
mappings, so the hash changes and the plan is a `REINDEX` into a new index that carries the new
analysis settings.

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
| `clearStaleWriteBlock` | `true`: lift a write block found on the alias target when it is kept or copied without blocking (see below) |
| `reindexMode` | `ZERO_DOWNTIME`; `BLOCKING` write-blocks the old index for the whole copy |
| `swapGrace` | 5 seconds: how long a zero-downtime reindex waits after the alias swap before it write-blocks the old index |
| `backfill` | `true`: apply unindexed additions with `BACKFILL`; `false` reindexes them |
| `lock` | `true`: take a per-alias lock and plan again under it (see [Running from several instances](#running-from-several-instances)) |
| `lockWait` | 10 minutes: how long to wait for another holder before the item is `LOCKED`; zero fails at once |
| `lockTtl` | 2 minutes (at least 1 second): a lock not renewed for this long is taken over |
| `staticSettings` | `WARN`: static settings that differ are a warning; `REINDEX` reindexes into the next generation |
| `extraDynamicSettings` | none: setting keys (without `index.`) to apply in place in addition to the built-in list |
| `degradedReconcile` | `REPORT`: when the snapshot is lost after the swap, write nothing and end `INCOMPLETE`; `CREATE_MISSING` creates the missing documents (and so brings back documents deleted since the swap) |
| `fullReconcile` | `false`: a zero-downtime reindex reads only the documents written after the global checkpoint where that is sound ([sequence number range](#zero-downtime-reindex)); `true` always compares every document and sweeps |

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

### Zero-downtime reindex

A zero-downtime `REINDEX` (the default `ReindexMode`) never blocks writes to the index the alias
points at. It runs in phases; the new index
records the phase in `_meta.elastic_protos.pending`, which is how an interrupted run is picked up
([ADR 0007](docs/adr/0007-zero-downtime-reindex.md)):

| Phase | What happens | `pending` on the new index |
|---|---|---|
| Prepare | Creates the new index without the alias (naming below), or reuses a leftover; records the old index's sequence number checkpoint: its global checkpoint (the lowest `seq_no.global_checkpoint` over its primaries, from `_stats`), or on an engine that reports none the lowest `seq_no.max_seq_no` over its shard copies | `{"action": "reindex", "from": "<old>", "phase": "copying", "checkpoint": <n>}` |
| Copy | `_reindex` old to new with `version_type: external` and `conflicts: proceed`, with the migration script if one is found | `copying` |
| Catch up | The same `_reindex` again: under external versioning it copies only documents written during the copy | `copying` |
| Probe | With a migration script: runs it over the first 1,000 documents of the old index into the staging index and fails the item, before anything moves, if it changed an id | `copying` |
| Snapshot | Refreshes the new index and opens a point in time over it (keep-alive `max(1h, reindexTimeout)`, at most 24 h) | `phase: swapping`, `pit: <id>`, and `seq_no_range: true`, `cold_count: <n>` when the range applies (below) |
| Swap | Moves the alias atomically | `swapping` |
| Freeze | Waits `swapGrace`, then write-blocks the old index with `PUT /<old>/_block/write`, which waits for writes in flight, and refreshes it | `reconciling` |
| Reconcile | Hot pass first: the documents of the frozen old index written after the checkpoint (`range` on `_seq_no`), which may have changed after the copy whatever their version says (a version restarts at 1 once a delete's tombstone is gone after `index.gc_deletes`). Then the rest, compared by version. Pages of 1,000 are scanned without sources and looked up in the snapshot; a document the snapshot lacks is `create`d in the new index, one that differs (hot: by version or source; with a script: always) is written with `if_seq_no`/`if_primary_term` from the snapshot, and only those documents' sources are fetched. Either write fails with 409 when a write through the alias after the swap got there first, and that write is kept. With the **sequence number range** (below), the hot pass is all there is | `reconciling` |
| Sweep | Pages through the snapshot; a document the old index no longer has (deleted after the copy) is deleted from the new index on its snapshot sequence number, so a document rewritten after the swap is kept. With the sequence number range it runs only when the document counts show that there are such documents | `reconciling` |
| Finish | Closes the point in time, deletes the staging index, removes `pending`; then deletes the old index if `deleteOldAfter`, otherwise retains it write-blocked (`Outcome.retainedIndices()`) | none |

Every `_reindex` and `_update_by_query` result must be complete (no failures, no timeout, every
document counted as created, updated, a no-op, deleted or a version conflict), and a reconcile or
sweep write that fails with anything other than 409 or 404 fails the item. `Outcome.sourceCount()`
is the frozen old index and `destCount()` the new index after the reconcile; writes after the swap
make them differ legitimately, so a difference is a warning. `Outcome.reconcile()` reports
`ReconcileStats(copied, superseded, ghostsDeleted, staleSkipped, strategy, swept)`.

**Sequence number range** (`ReconcileStrategy.SEQ_NO_RANGE`). The cold pass and the sweep each
read every document. When the premise below holds and is verified, the reconcile reads only the
documents of the old index above the checkpoint, and sweeps only when counting shows ghosts. The
checkpoint is the lowest over the shards, so the range is "every document written after the
checkpoint of the least busy shard": on evenly loaded shards that is about the documents changed
during the copy; on skewed shards (custom routing, one shard with heavy update churn) it can be most
of the index, since per-shard sequence numbers are not aligned in time. It is never more work than
the full scan with the same checkpoint.

Decided before the swap (otherwise the marker carries no `seq_no_range` and the reconcile is a full
scan):

- the engine reports a global checkpoint (`SearchEngine.globalCheckpoint`; both adapters do) that is
  not negative (a shard without operations reports -1, which would make the range the whole index)
  and not above the engine's own lowest `max_seq_no`;
- this run created the new index (not a leftover it reused), no migration script applies, it is not
  a rollback, and neither index uses OpenSearch segment replication (`index.replication.type:
  SEGMENT`; OpenSearch writes the cluster default into every new index);
- the copy is verifiably clean: the first pass read at least as many documents as the old index held
  at or below the checkpoint (counted right after the refresh that precedes it) and created exactly
  every document it read (no failure, version conflict, no-op, update or delete; a conflict on a
  fresh index means duplicate ids under custom routing, a no-op a dropping ingest pipeline); the
  catch-up deleted nothing; and the snapshot holds exactly as many documents as the two passes
  created.

The decision, with that count (`cold_count`), is written into the marker at `phase: swapping`, so a
resume can never turn the range on by itself. At the freeze, the old index's documents at or below
the checkpoint are counted again; more than at the copy (impossible while the engine keeps its
promise) means a full scan, with a warning. `MigrationOptions.fullReconcile` forces the full scan.

Why the premise is enough: every operation at or below a shard's global checkpoint was processed by
every copy of the shard before the checkpoint was read, and the copy's refresh comes after it, so
every document at or below the checkpoint was copied as it is and is in the snapshot unchanged; no
later operation gets a sequence number at or below it, even after a primary fails over.

Deletes are invisible to a range query, so the sweep is decided by counting:
`ghosts = count(snapshot) - count(old index) + hot documents missing from the snapshot`, which is
exact when the premise holds. Zero skips the sweep; a positive count runs it; a count the engine
reports only as a lower bound runs it with a warning. A negative count runs the cold pass and the
sweep with a warning, but it is only a last-resort inconsistency detector: a broken premise that
loses as many documents as there are real ghosts adds up to zero, which is why the premise is
checked before the swap instead. Repeated updates of a document, a document created and deleted
after the copy, or a delete followed by a new write do not trigger the sweep.

Every search and count the migrator relies on (the copy checks, the hot and cold passes, the
lookups, the sweep, the ghost count, the document counts of the outcome) fails when a shard did not
answer: the engines answer such a search with 200, the other shards' hits and an exact-looking
total (`SearchResult.shardsFailed()`). A partial answer from the old index fails the item (it
resumes); one from the snapshot counts as a lost snapshot. Other searches are unchanged.

Otherwise, including on a resume from a marker written by 1.0.0-rc.3 or earlier (no `seq_no_range`
or `cold_count`), the reconcile is a `ReconcileStrategy.FULL_SCAN`; a lost snapshot is `DEGRADED`.
`ReconcileStats.swept()` tells whether the sweep ran.

What writers and readers see:

- **Writes through the alias never fail**, except a write that resolved the alias to the old index
  before the swap and reaches it after the grace period: it gets a `BLOCKED` error
  (`cluster_block_exception`) and can be retried against the alias. It never lands silently in an
  index that nobody reads.
- **Reads** see the old index until the swap, then the new index. Between the catch-up and the end
  of the reconcile, the new index can lack writes made to the old index after the catch-up pass.
- **Last writer wins per document**: a write through the alias after the swap is never overwritten
  by the reconcile, and a document the new index held at the swap and that was deleted through the
  alias after it is not brought back. A document created in the old index after the catch-up and
  deleted through the alias after the swap, before the reconcile reached it, is created again: the
  new index never held it, so there is no delete to compare with. The hot pass reaches such
  documents first, which keeps this window to seconds.
- **Partial updates** (`_update`) after the swap apply to the document as the new index holds it.
  If that copy is stale, field changes made to the old index between the catch-up and the swap are
  lost for that document; full-document writes are not affected. An `_update` of a document
  created in the old index after the catch-up gets 404 from the new index until the reconcile has
  created it; an `_update` with `upsert` (or `doc_as_upsert`) creates a partial document, and the
  reconcile's `create` then gets 409 and keeps it, losing the fields the old index had.
- **Optimistic concurrency**: a write with `if_seq_no`/`if_primary_term` (or a version) read
  before the swap, or before the reconcile rewrote the document, gets 409, since the document now
  lives in another index or was rewritten; re-read and retry.

**Migration scripts** run on every copy, and on reconciled documents through a staging index
`<new index>-reconcile` (target mappings, no alias): the changed ids are copied with `_reindex` and
an `ids` query, then written into the new index with the conditional writes above. A script must
not change `_id`. The probe before the swap fails the item ("the migration script changes _id;
use BLOCKING or fix the script") when it changes the id of any of the first 1,000 documents; a
script that changes the ids of only some documents can get past the probe, and is then caught by
the reconcile, after the swap (`FAILED`, resumable once the script is fixed).

**Recovery.** Planning finds an interrupted run from the alias and the marker alone:

| Interrupted | Next `plan` | Next `apply` |
|---|---|---|
| Before the swap (marker on the new index, alias on the old one) | `REINDEX`, `resume.swapped() == false` | Reuses the new index and starts again from the copy; external versioning makes it idempotent |
| After the swap (marker on the index the alias points at) | `REINDEX` with `resume` (`from`, `pitId`, `phase`, `swapped`), even when the hash is current | Freezes the old index if needed, reconciles and sweeps. Conditional writes make this idempotent; `_reindex` never runs again, since external versioning compares version numbers and the new index takes live writes |
| After the swap, point in time expired | as above | Writes nothing by default (`degradedReconcile = REPORT`): counts the documents missing from the new index, those that differ (`staleSkipped`) and those only in the new index, reports them as a warning and ends `INCOMPLETE` (not ok), with the marker left at `phase: reconciling`. The operator then applies again with `DegradedReconcile.CREATE_MISSING`, which creates the missing documents and finishes, **bringing back every document deleted through the alias since the swap**, or removes the marker by hand |
| While finishing (`phase: finishing`) | as above | Removes the marker |

`IndexMigrator.Builder.listener(MigrationListener)` is told about every phase, for metrics and
logs. Exceptions from the listener are logged and ignored, except `MigrationListener.Abort`, which
stops the run where it stands, without cleanup; that is how the tests simulate crashes. A
resumed run closes the point in time an interrupted run recorded before it opens a new one.

A failure before the swap deletes the new index if this run created it and the alias does not
point at it; the old index was never blocked. A failure after the swap leaves the alias on the new
index and the marker in place: the outcome is `FAILED` and the next `plan` and `apply` resume.

The new index gets a generation suffix (see [Index naming](#index-naming-and-metadata)) when the
alias already points at an index with the target name, or when an index with that name exists and
is write-blocked: that is an old index retained by an earlier reindex, kept for rollback, and it
is never reused or deleted. An unblocked index with the target name is a leftover from an earlier
run that did not finish: it is reused when its `_meta` records the target hash and the alias does
not point at it, and otherwise deleted and recreated (logged as a warning). A blocking reindex
also recreates a leftover that carries a zero-downtime marker. If the alias already
points at it, the item fails without touching anything (another migration ran in the meantime;
plan again).

The `_reindex` and `_update_by_query` run as server-side tasks that the engine polls. When the
`reindexTimeout` expires, or polling fails, the task is cancelled (`POST /_tasks/{id}/_cancel`)
and awaited for up to 30 seconds before the error is reported. Transient polling errors (5xx, 429,
I/O) are retried three times with a short backoff first.

#### Blocking reindex

`reindexMode(ReindexMode.BLOCKING)` keeps the simpler procedure: write-block and refresh the old
index, one `_reindex` (with the script), check the copy (no failures, no timeout, and without a
script both indices must hold the same number of documents; with a script a differing count is a
warning), swap the alias, retain or delete the old index. **Writes through the alias fail from the
write block until the swap.** If a step fails, the old index is unblocked and the new index deleted
under the same rules as above.

**Write-block recovery.** A process killed during a blocking reindex leaves the old index, still
behind the alias, write-blocked. The next plan reports a warning for any alias target with
`index.blocks.write=true`. With `clearStaleWriteBlock` (the default), `apply` lifts the block when
that index is kept (`NO_OP`, `PUT_MAPPING`, `BACKFILL`) or copied by a zero-downtime reindex; a
blocking `REINDEX` moves the alias to a new, unblocked index anyway. The option assumes that a
write block on a live alias target was left by an interrupted reindex; turn it off if you set
write blocks on live indices yourself.

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
    MigrationResult.Outcome back = migrator.rollback(item);   // to item.currentIndex()
  }
}
```

`rollback(item, previousIndex, mode)` returns an `Outcome` (`index` is the restored index,
`retainedIndices` the new one) and never throws for engine errors; `rollback(item)` and
`rollback(item, previousIndex)` use `REVERSE_DELTA` and throw `MigrationException` when it does not
apply. The alias must point at the plan's target index only.

| `RollbackMode` | What it does |
|---|---|
| `REVERSE_DELTA` (default) | Unblocks the previous index and runs the zero-downtime procedure from the new index into it, without a script: copy, catch up, swap the alias, freeze the new index, reconcile, sweep. Versions are not comparable in this direction, so the reconcile compares sources: documents that differ, including those a migration script changed, are copied back in their new form. Writes made after the reindex are carried back. Any failure before the swap aborts the rollback: the previous index is write-blocked again, the alias stays on the new index, and the outcome is `FAILED`, advising `ALIAS_ONLY`; the previous index may already hold some of the copied documents. A rollback that dies before its swap leaves a note on the new index, which the next `plan` reports as a warning. Refused (`FAILED`) when a migration script exists for the reindex, since its transformed documents would be copied back. The new index is retained write-blocked |
| `REVERSE_DELTA_FORCE` | `REVERSE_DELTA` even when a migration script exists |
| `ALIAS_ONLY` | Unblocks the previous index, swaps the alias, waits `swapGrace` and write-blocks the new index. Writes made to the new index since the reindex are not carried back |

### Running from several instances

Several instances may start, and bootstrap, at once. `apply` takes a lock per alias before it
changes anything, so one instance does the work and the others wait and then find it done:

- The lock is a document named after the alias in the lock index, `elastic-protos-locks` (with a
  naming prefix, `<prefix>-elastic-protos-locks`; see `IndexNamingStrategy.lockIndex()`). The index
  is created on first use with one shard, replicas from `replicas`, and a strict mapping (`alias`,
  `holder` = `hostname/pid/uuid8`, `action`, `hash`, `acquired_at`, `expires_at`). An alias of
  yours named like the lock index collides with it.
- Taking the lock creates the document with `op_type: create`, so exactly one instance wins. The
  holder renews it every third of `lockTtl` (default 2 minutes) with a write conditional on its
  previous sequence number, and deletes it when done. A document whose `expires_at` has passed
  belongs to a dead holder and is deleted on the sequence number it was read with, so of several
  instances taking it over exactly one succeeds.
- **Under the lock the item is planned again** and the fresh plan applied. An instance that waited
  sees the alias already migrated (`NO_OP`, `SKIPPED`), or picks up what a crashed holder left
  (the `pending` marker of a [zero-downtime reindex](#zero-downtime-reindex) or a backfill).
- An instance waits up to `lockWait` (default 10 minutes; `Duration.ZERO` fails at once); after
  that the item is `LOCKED` (not ok) with a `LockException` naming the holder, and the run goes on
  with the other aliases even under `FAIL_FAST`. `BLOCKED` items take no lock, nor do `NO_OP`
  items unless they lift a stale write block (`clearStaleWriteBlock`). A plan
  whose target hash differs from the plan made under the lock (the mapper changed in between) is
  `FAILED` with "plan is stale: plan again". `Outcome.plan()` is the fresh plan when it was
  applied, and the original item otherwise.
- An expired lock is taken over only a third of `lockTtl` after its `expires_at`, as an allowance
  for clock skew.
- Before every step that cannot be taken back (alias swaps, write blocks, mapping and `_meta`
  updates, deleting the old index) the lease is checked. A lease found lost (taken over, or the
  document deleted, typically after the holder could not reach the cluster for longer than
  `lockTtl`) stops the item without cleanup (`FAILED` with a `LockException`), and the next run
  resumes from the marker. A lease is also treated as lost once two thirds of `lockTtl` have
  passed since its last successful write was sent, whatever the renewals report, so a holder cut
  off from the cluster stops before anyone can take over. Failed cleanups, the staging index and
  long copies check the lease too. `rollback` takes the same lock.
- The check is check-then-act, without fencing tokens: a lease can be lost between a check and the
  engine call that follows it (request latency plus up to a third of `lockTtl`).
- The lock coordinates migrators only. It does not protect against application writers going
  through the alias (the zero-downtime reindex handles those), external tools changing the
  indices, or migrators running with `lock(false)`.
- The lock index takes its replica count from `replicas` (possibly 0); while it is red, every item
  fails. Deleting it while instances run can let a lock write auto-create it with dynamic
  mappings; the migrator then logs a warning, and the index should be deleted again when nothing
  migrates.
- Clocks of the instances must agree to well within `lockTtl`: expiry compares the holder's
  `expires_at` with the reader's clock.
- Make `lockWait` fit your startup budget: an instance waiting for another one's reindex is not
  ready until it gets the lock or `lockWait` passes. With a long reindex, run the migration from
  one place (a Job, or `elastic-protos.bootstrap.enabled: false` on all but one instance) and let
  the others start without it.
- A single-writer deployment can turn the lock off (`lock(false)`, or
  `elastic-protos.bootstrap.lock.enabled: false`); several instances must then never migrate the
  same aliases at once, which is unsupported and can be destructive.

A reindex can take minutes. Whatever runs it must not be killed mid-copy: on Kubernetes, give the
pod that migrates a startup probe budget (`failureThreshold` x `periodSeconds`) longer than the
`reindexTimeout`, or run the migration as a Job. An interrupted reindex is resumed on the next run
(see [Zero-downtime reindex](#zero-downtime-reindex)); until then the alias may serve the old index,
or a new index that still lacks writes made after the catch-up pass.

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
`FieldCondition`s, plus sorting, pagination, [aggregations](#aggregations) and
[highlighting](#highlighting). Build it with the generated builders or with the
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
| `NUM_RANGE` | `between(min, max)`, `between(Instant, Instant)`, `range(...)`; open-ended: `atLeast`, `greaterThan`, `atMost`, `lessThan`, `onOrAfter`, `after`, `onOrBefore`, `before` | numeric, date | `range` with the bounds that are set |
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
- **Ranges**: `RangeValue.min` and `max` are proto3 `optional`, so a range may have only a lower or
  only an upper bound (`atLeast(n)`, `lessThan(n)`, `after(instant)`, ...; the same helpers exist on
  `Values`). Only the bounds that are set are emitted, and only the natural-side infinity (`-Infinity` min, `+Infinity` max) counts as unset.
  `min_inclusive` / `max_inclusive` default to `false`, so a hand-built range is exclusive unless you
  set them. A range with no bound, a NaN bound, or an empty range (`min > max`, equal bounds that are not
  both inclusive, `+Infinity` min, `-Infinity` max) throws `QueryCompileException`, also when
  lenient. Instant helpers use millisecond precision (sub-millisecond parts are truncated). Because
  `0.0` used to be omitted from the wire, criteria serialized by rc.1/rc.2 (binary or JSON) with a
  bound of exactly `0` now arrive with that bound unset; rebuild them or persist with
  `includingDefaultValueFields`. Date ranges are epoch milliseconds.
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

`compiled.body()` is an unmodifiable `Map`; copy it into a new map to add anything the DSL does not
cover. Aggregation results come back raw in `SearchResult.aggregations()`; read them with
[`AggregationResults`](#reading-the-results). `SearchResult.total()` is exact
when `totalExact()` is true and a lower bound otherwise. With `track_total_hits: false`
(`withTrackTotalHits(TrackTotalHits.none())`) the engine counts nothing: `total()` is then
`SearchResult.NO_TOTAL` (`-1`), `totalExact()` is false and `hasTotal()` is false.

### Aggregations

`SearchCriteria.aggregation` (messages in `aggregations.proto`) compiles into the body's `aggs`,
computed over every document the query matches. Build them with `Aggs` and add them with
`Criteria.aggregate(...)`; `aggregationsOnly()` sets the page size to `-1`, which compiles to
`size: 0` (no hits, but the total and the aggregations):

```java
SearchCriteria criteria = Criteria.where("type").equalTo("FILING").end()
    .aggregate(
        Aggs.sub(Aggs.terms("by_firm", "firm_id"),               // a bucket per firm ...
            Aggs.sum("total_size", "size_bytes"),                // ... with metrics inside
            Aggs.max("latest", "filed_at")),
        Aggs.dateHistogram("per_month", "filed_at", CalendarInterval.MONTH),
        Aggs.range("sizes", "size_bytes",
            Aggs.bucket("small", null, 50_000d), Aggs.bucket("large", 50_000d, null)),
        Aggs.cardinality("firms", "firm_id"))
    .aggregationsOnly()
    .build();
```

| Kind | `Aggs` | Allowed fields | Compiles to |
|---|---|---|---|
| `terms` | `terms(name, field)` | keyword, numeric, date, boolean, ip, text with `.keyword`, keys of flattened fields | `terms` on the exact-match path; `size` (at most `maxTermsSize`, default 1000), `min_doc_count`, `missing` (typed for the field), `order` |
| `date_histogram` | `dateHistogram(name, field, CalendarInterval)` or `(name, field, "90m")` | date | `calendar_interval` (`1m` `1h` `1d` `1w` `1M` `1q` `1y`) or `fixed_interval` (a whole number and `ms`, `s`, `m`, `h` or `d`); `time_zone` (checked with `ZoneId`), `format`, `min_doc_count`, `extended_bounds` (epoch millis) |
| `histogram` | `histogram(name, field, interval)` | numeric | `interval` (positive), `offset`, `min_doc_count` |
| `range` | `range(name, field, bucket(key, from, to)...)` | numeric, date | `range`; on dates `date_range` with `format: epoch_millis`. Buckets are `[from, to)`; a null bound is open. Bounds are `double`s (exact on long fields only up to 2^53) or, on date fields, `Instant`s (`from_time`/`to_time`); date bounds are sent as epoch millis |
| `metric` | `min`, `max` | numeric, date | `min` / `max` |
| | `avg`, `sum` | numeric | `avg` / `sum` |
| | `count`, `cardinality` | any exact-match field, including keys of flattened fields | `value_count` / `cardinality` on the exact-match path |

Metrics are `MetricAggregation { type }` messages (`Aggs.min(...)` and the like build them).
`cardinality` is approximate: the engines count with HyperLogLog++, exactly only below the precision
threshold (3000 distinct values by default); the in-memory test engine counts exactly.

Terms buckets are ordered by document count, descending, by default. Each `TermsOrder` orders
`BY_COUNT` (`_count`), `BY_KEY` (`_key`) or `BY_METRIC`, naming a metric sub-aggregation of the
same terms aggregation; the direction defaults to descending for counts and metrics and ascending
for keys. Several orders compile to a compound order: the first decides, the next break ties.

These rules are checked with a `QueryCompileException` (unknown fields are skipped with a warning
when lenient):

- **Names** are required and unique among siblings. They must not contain `__` (reserved for the
  wrappers), `[`, `]` or `>` (the engines reject them) or `.` (it breaks the `BY_METRIC` order
  path), must not end with `_` (so the wrapper suffix can be stripped unambiguously), and must not
  be a key of the engines' bucket objects (`key`, `key_as_string`, `doc_count`, `buckets`, `value`,
  `value_as_string`, `from`, `to`, `from_as_string`, `to_as_string`, `sum_other_doc_count`,
  `doc_count_error_upper_bound`, `meta`).
- **One level of sub-aggregations**: bucket aggregations take sub-aggregations, which take none of
  their own; metrics take none.
- **Text** fields aggregate through their `.keyword` sub-field; text without one is refused with a
  pointer to `keyword_subfield`. Whole `flattened` fields, objects, nested paths, geo and binary
  fields are refused.

**Nested fields.** Every aggregation runs in the nested context of its parent bucket (the root
document at the top level; sub-aggregations of a bucket aggregation run in the context of its
field), and the compiler wraps it to reach its field:

| Field vs. context | Wrapping |
|---|---|
| same nested path | none |
| deeper | one `nested` per extra level, named `<name>__nested`, `<name>__nested2`, ... |
| shallower (or top level) | `reverse_nested` named `<name>__reverse`, with `path` set to the field's innermost nested path (left out for the root) |
| a different branch | `reverse_nested` up to the deepest common nested path, then `nested` down |

For example, parties per role, ordered by the number of distinct firms of their documents:

```java
Aggs.sub(Aggs.terms("roles", "parties.role").setTerms(TermsAggregation.newBuilder()
        .setOrder(TermsOrder.newBuilder().setBy(TermsOrderBy.BY_METRIC).setMetric("firms"))),
    Aggs.cardinality("firms", "firm_id"))
```

compiles to

```json
"aggs": {
  "roles__nested": {
    "nested": { "path": "parties" },
    "aggs": {
      "roles": {
        "terms": { "field": "parties.role", "order": { "firms__reverse>firms": "desc" } },
        "aggs": {
          "firms__reverse": {
            "reverse_nested": {},
            "aggs": { "firms": { "cardinality": { "field": "firm_id" } } }
          }
        }
      }
    }
  }
}
```

Buckets in a nested context count nested objects, not documents: on the testkit sample, the
`DEFENDANT` bucket of `roles` has a `doc_count` of 4 parties. To count the documents those objects
belong to, add a metric that steps out with `reverse_nested`, for example `Aggs.count("docs", "id")`
or a `cardinality` of a top-level field as above. Nested terms multiply buckets: with a parent of
`size` n, each of up to n buckets can hold its own sub-buckets, and the engines fail a search whose
response would exceed `search.max_buckets` (65,536 by default) even when every `size` is within
`maxTermsSize`.

Nested paths inside the values of a typed map are refused: the engines have no `ignore_unmapped`
for `nested` aggregations, and such paths are mapped only once a key is used.

#### Reading the results

`AggregationResults` looks results up by the names in the criteria and unwraps the
`__nested`/`__reverse` wrappers:

```java
SearchResult result = engine.search(List.of(documents.alias()),
    QueryCompiler.withDefaults().compile(criteria, documents).body());
AggregationResults aggs = AggregationResults.of(result);

for (AggregationResults.Bucket bucket : aggs.buckets("by_firm")) {
  String firm = (String) bucket.key();
  long count = bucket.docCount();
  double total = bucket.sub().metric("total_size").value();
  String latest = bucket.sub().metric("latest").valueAsString();   // "2024-06-30T08:00:00.000Z"
}
long otherFirms = aggs.sumOtherDocCount("by_firm");
long firms = aggs.metric("firms").asLong();
```

`Bucket` carries `key` (a `String` for keyword terms and range buckets, a `Long` for integer and
date terms and for date histograms (epoch millis), a `Long` `1` or `0` for boolean terms (with
`keyAsString` `"true"` / `"false"`), a `Double` for histograms and floating-point terms),
`keyAsString` (dates in the form `2024-06-30T08:00:00.000Z`), `docCount`, `from`/`to` and their `*AsString` forms for range buckets, and
`sub`, the results inside the bucket. `MetricValue` has `value()` (null when no document had the
field), `valueAsString()`, `isPresent()` and `asLong()`. Keyed (object-shaped) buckets are read too.
An unknown name throws `NoSuchElementException` listing the names present; `buckets` on a metric or
`metric` on a bucket aggregation throws `IllegalStateException`. `names()`, `has(name)` and `raw()`
inspect the response.

`SearchPager` sends the aggregations with the first page only (they cover every match) and
highlighting with every page. Pipeline, filter, composite and other aggregations not listed here
still go into a copy of the body by hand.

### Highlighting

`SearchCriteria.highlight` compiles into the body's `highlight`; the fragments come back per hit in
`Hit.highlight()`:

```java
SearchCriteria criteria = Criteria.builder()
    .fullText("merger agreement")
    .highlight()                                  // every default full-text text field
    .build();

SearchCriteria custom = Criteria.builder()
    .fullText("merger")
    .highlight(h -> h.addFields(HighlightField.newBuilder().setField("title").setNumberOfFragments(0))
        .addPreTags("<mark>").addPostTags("</mark>").setFragmentSize(150).setHtmlEncode(true))
    .build();

CompiledSearch compiled = QueryCompiler.withDefaults().compile(criteria, documents);
for (Hit hit : engine.search(List.of(documents.alias()), compiled.body()).hits()) {
  List<String> titles = hit.highlight().getOrDefault("title", List.of());  // "<em>Merger</em> agreement ..."
}
```

| `Highlight` field | Compiles to |
|---|---|
| `fields` | `fields`; empty means the index's full-text fields that are text and outside nested objects. Explicit fields must be `TEXT` or `SEARCH_AS_YOU_TYPE` (typed map keys and patterns such as `notes.*` included) and outside nested objects; others fail, or are skipped with a warning when lenient |
| `HighlightField.fragment_size`, `number_of_fragments` | per-field settings |
| `fragment_size`, `number_of_fragments` | the same for every field; `number_of_fragments: 0` returns the whole value |
| `pre_tags`, `post_tags` | `pre_tags` / `post_tags`; empty means `["<em>"]` / `["</em>"]` |
| `require_field_match` | `require_field_match`, when set |
| `html_encode` | `encoder: html`: the field text is escaped (`&` becomes `&amp;`) before the tags are inserted |

When no field can be highlighted, the compiler adds a warning and no `highlight` clause.

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

`SearchEngine` is the whole SPI: index lifecycle, mappings, settings updates (`putSettings`),
aliases, reindex, `updateByQuery`, write blocks (`setWriteBlock(index, true)` is `PUT
/<index>/_block/write`, which waits for writes in flight), the sequence number checkpoints
(`globalCheckpoint` and `seqNoCheckpoint`, from `_stats?level=shards`; the defaults return empty
and -1, "unknown", which keeps the zero-downtime reconcile on a full scan), document writes,
realtime `get` by id, `bulk`, `search`, `count`, point in time, `refresh` and `health`, all as
JSON-shaped maps.

Writes support optimistic concurrency: `IndexResult` and `Hit` carry the document's `seqNo` and
`primaryTerm`, and `IndexOptions.ifMatch(seqNo, primaryTerm)` (or `BulkOp.indexIfMatch` /
`deleteIfMatch`) only writes while the document still has them, failing with `CONFLICT` otherwise.
`get` always returns `version`, `seqNo` and `primaryTerm` on the hit; `search` returns them when the
body asks for `"version": true` / `"seq_no_primary_term": true`, and per-field `highlight`
fragments when it has a `highlight`. `ReindexRequest` takes a source `query`, `VersionType.EXTERNAL`
(keep source versions and skip documents whose destination version is equal or higher) and
`proceedOnConflicts`. External versions are compared as numbers, not by time: a document created in
the destination by an ordinary write starts at version 1, so do not run an external pass once the
destination takes live writes. `ReindexResult` counts `versionConflicts`, `noops` and `deleted`;
`accounted()` checks that they add up to `total`, and `complete()` also requires `ok()`. `updateByQuery` rewrites matching documents in place, polled as a task like
reindex.

Transport failures are `EngineException`s of kind `IO` that name the request
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
  Terms and `cardinality` aggregations on a key of such a field return the same results as on
  Elasticsearch: verified on OpenSearch 2.19.7 and 3.9.0 and Elasticsearch 8.19.23 and 9.5.5.
- The `_shard_doc` point-in-time tiebreaker exists only on OpenSearch 3.3+ (absent in 2.19 and 3.2);
  `SearchPager` skips it there.
- Opening and closing a point in time use each engine's own API.
- `apiKey` sends an `ApiKey` authorization header; whether OpenSearch accepts it depends on its
  security configuration. Basic auth is the usual choice.

## Micronaut

```groovy
dependencies {
    implementation platform('com.phatjam98:elastic-protos-bom:1.0.0-rc.3')
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

The bootstrap is enabled by default and runs in every instance that starts. Instances that start
together coordinate through the per-alias lock (see
[Running from several instances](#running-from-several-instances)): one migrates, the others wait
up to `elastic-protos.bootstrap.lock.wait` and then find the work done; an index still locked
after that aborts startup under `fail-on-error`. Because the bootstrap runs during startup, a
reindex delays readiness: on Kubernetes, make the startup probe budget longer than
`elastic-protos.bootstrap.reindex-timeout` (and the lock wait) so the pod is not killed mid-copy, or
run the migration from a dedicated job with `enabled: false` on the application instances.

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
`codec()` and `engine()`. `SearchPage` carries the messages, the raw hits (with their highlights),
the total, the `next` pagination, the compiler warnings and the `aggregations` of the criteria
(`AggregationResults`, empty when there are none). `aggregate(criteria, type)` runs only the
aggregations, with no hits or highlights, and returns an `AggregationPage` with the `results`, the
`total` of matching documents (`-1` when totals are not tracked) and the compiler `warnings`. Both
methods log compiler warnings at `WARN`.

```java
AggregationPage perType = elastic.aggregate(
    Criteria.builder().aggregate(Aggs.terms("by_type", "type")).build(), Document.class);
long filings = perType.results().buckets("by_type").get(0).docCount();
```

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
| `elastic-protos.bootstrap.clear-stale-write-block` | `true` | Lift a write block on an alias target that is kept or copied without blocking, assumed left by an interrupted reindex |
| `elastic-protos.bootstrap.fail-on-error` | `true` | Abort startup when an index fails or is blocked |
| `elastic-protos.bootstrap.reindex-timeout` | `10m` | How long a reindex or backfill may take |
| `elastic-protos.bootstrap.reindex-mode` | `ZERO_DOWNTIME` | Or `BLOCKING` (write-block the old index during the copy) |
| `elastic-protos.bootstrap.swap-grace` | `5s` | Wait after the alias swap before the old index is write-blocked |
| `elastic-protos.bootstrap.backfill` | `true` | Apply additions existing documents were never indexed into with an update by query; `false` reindexes |
| `elastic-protos.bootstrap.static-settings` | `WARN` | Or `REINDEX`: reindex into the next generation when static settings differ |
| `elastic-protos.bootstrap.extra-dynamic-settings` | empty | Setting keys (without `index.`) to apply in place in addition to the built-in list |
| `elastic-protos.bootstrap.lock.enabled` | `true` | Take a per-alias lock in the lock index, so several instances can bootstrap at once |
| `elastic-protos.bootstrap.lock.wait` | `10m` | How long to wait for a lock another instance holds; `0s` fails at once |
| `elastic-protos.bootstrap.lock.ttl` | `2m` | How long a lock lives without a heartbeat |
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
| `elastic-protos.query.max-terms-size` | `1000` | Largest `size` of a terms aggregation |
| `elastic-protos.bulk.actions` | `500` | `bulkBuffer(...)` sends a batch at this many operations |
| `elastic-protos.bulk.flush-interval` | `1s` | Also send at this interval; `0s` disables |
| `elastic-protos.bulk.max-bytes` | `5MB` | Send a batch at about this size |

With `shard-doc-tiebreaker: AUTO`, `ElasticProtosService` asks the engine once, on the first search
with a `cursor`, whether it supports `_shard_doc` and leaves the tiebreaker out if not (OpenSearch
before 3.3); `pager(...)` always adapts.

## Limitations

- During a zero-downtime reindex, reads through the alias between the swap and the end of the
  reconcile can miss writes made to the old index after the catch-up pass; a partial `_update`
  in that window applies to the possibly stale copy and can drop those field changes (see
  [Zero-downtime reindex](#zero-downtime-reindex)). The window lasts as long as reading the
  documents above the lowest shard checkpoint (the changed documents on evenly loaded shards, up to
  most of the index on skewed ones), plus a sweep of the whole new index when documents were
  deleted from the old index during the copy; it is a full scan of both indices with a migration
  script, a reused leftover index, an engine without a global checkpoint, an empty shard, segment
  replication on either index, a copy that did not verify as clean, a rollback or `fullReconcile`. `BLOCKING` avoids both by
  failing writes for the whole copy instead.
- The one write a zero-downtime reindex can fail: a write that resolved the alias to the old index
  before the swap and arrives after `swapGrace` gets a `cluster_block_exception` (`BLOCKED`) and
  can be retried against the alias ([ADR 0007](docs/adr/0007-zero-downtime-reindex.md)).
- A document created after the catch-up pass and deleted through the alias after the swap,
  before the reconcile reached it, is created again by the reconcile: the new index never held it,
  so there is nothing to compare the delete with. The hot pass keeps this window short.
- Static settings of an existing index change only through a reindex
  (`StaticSettingsPolicy.REINDEX`; the default only warns), and a setting removed from the proto is
  never reset (see [Settings changes](#settings-changes)).
- Inner maps inside the values of a typed map (and `Struct`, `Value`, `ListValue` or `Any` values)
  stay untyped. Keys of typed maps must not be blank or contain dots, and every distinct key adds
  fields that count toward `index.mapping.total_fields.limit` (and nested fields in the values
  toward `index.mapping.nested_fields.limit`) at index time (see [Mapping](#default-rules)).
- The aggregation DSL covers terms, date histograms, histograms, ranges and single-value metrics
  with one level of sub-aggregations; pipeline, filter, composite and other aggregations go into a
  copy of the body by hand. Aggregations and sorts on nested paths inside typed map values are
  refused. Highlighting covers text fields outside nested objects (nested fields would need `inner_hits`).
  `InMemorySearchEngine` (testkit) runs only terms, the six metrics, `nested` and `reverse_nested`
  aggregations, and ignores `highlight`.
- The migration lock compares the holder's `expires_at` with the reader's clock, so the clocks of
  the instances must agree to well within `lockTtl`; it uses no fencing tokens (see
  [Running from several instances](#running-from-several-instances)).

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
