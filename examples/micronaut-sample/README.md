# elastic-protos Micronaut sample

A small Micronaut 4 application that stores protobuf `Document` messages in Elasticsearch or
OpenSearch with [elastic-protos](../../README.md): the indices are created from the protos on
startup, documents are written and read as protobuf JSON, and searches go through the query DSL.

It is a standalone Gradle build (not part of the repository's root build) that consumes the
published artifacts, exactly as your application would.

| File | What it shows |
|---|---|
| [`build.gradle`](build.gradle) | BOM, core, Micronaut module and both adapters; protoc setup |
| [`document.proto`](src/main/proto/acme/search/v1/document.proto), [`firm.proto`](src/main/proto/acme/search/v1/firm.proto) | `elastic` and `elastic_field` options: alias, analyzer, id, `TEXT` with boost, `IGNORE`, nested parties, a timestamp |
| [`application.yml`](src/main/resources/application.yml) | `elastic-protos.*` properties, each commented |
| [`DocumentController.java`](src/main/java/com/acme/search/DocumentController.java) | `ElasticProtosService`: index, get, delete, search, bulk buffer |
| [`DocumentJson.java`](src/main/java/com/acme/search/DocumentJson.java) | HTTP JSON to and from messages with `JsonFormat` |
| [`SampleSeeder.java`](src/main/java/com/acme/search/SampleSeeder.java) | Three sample documents, indexed when `seed.enabled=true` |
| [`DocumentControllerIT.groovy`](src/test/groovy/com/acme/search/DocumentControllerIT.groovy) | End-to-end Testcontainers spec; skipped without Docker |

## Requirements

- JDK 17 (the Gradle toolchain downloads one if needed)
- Docker, for the engine and the integration spec

## 1. Publish elastic-protos locally

Until 1.0.0 is on Maven Central, install the current snapshot into `~/.m2` from the repository
root:

```bash
./gradlew publishToMavenLocal
```

The sample resolves `elasticProtosVersion` (default `1.0.0-SNAPSHOT`, in
[`gradle.properties`](gradle.properties)) from `mavenLocal()` first, then Maven Central. To build
against a published release instead:

```bash
./gradlew build -PelasticProtosVersion=1.0.0
```

## 2. Start an engine

Both services listen on port 9200, so start one at a time:

```bash
docker compose --profile elasticsearch up -d --wait   # Elasticsearch 9.5.5
docker compose --profile opensearch up -d --wait      # or OpenSearch 3.9.0
```

Stop it with `docker compose --profile elasticsearch down -v` (or `--profile opensearch`);
`-v` also drops the data volume.

## 3. Run the application

From this directory:

```bash
./gradlew run -Dseed.enabled=true      # bootstrap the indices and index three sample documents
./gradlew run                          # later runs: the documents are already there
```

The startup log shows the migration plan of the bootstrap. On a fresh cluster:

```text
INFO  c.p.e.m.ElasticProtosBootstrap - elastic-protos bootstrap plan:
Migration plan: 2 indices, changes pending
  CREATE      documents: (none) -> documents-v2d2e398d (added 0, removed 0, changed 0)
  CREATE      firms: (none) -> firms-ve57d22c5 (added 0, removed 0, changed 0)
INFO  c.p.e.migration.IndexMigrator - documents: creating documents-v2d2e398d with the alias
INFO  c.p.e.migration.IndexMigrator - firms: creating firms-ve57d22c5 with the alias
INFO  c.p.e.m.ElasticProtosBootstrap - elastic-protos bootstrap finished: 2 index(es) ok
INFO  com.acme.search.SampleSeeder - Seeded 3 sample documents into alias 'documents'
INFO  io.micronaut.runtime.Micronaut - Startup completed in 896ms. Server Running: http://localhost:8080
```

and on every later start, until a proto changes:

```text
Migration plan: 2 indices, no changes
  NO_OP       documents: documents-v2d2e398d (in place) (added 0, removed 0, changed 0)
  NO_OP       firms: firms-ve57d22c5 (in place) (added 0, removed 0, changed 0)
```

The index name is the alias plus a hash of the mapping. Change a mapped field in
`document.proto` and restart to see an `UPDATE_MAPPING` or `REINDEX` plan instead.

### OpenSearch

Start the `opensearch` compose profile and select the adapter with an environment variable:

```bash
ELASTIC_PROTOS_ENGINE=opensearch ./gradlew run -Dseed.enabled=true
```

Nothing else changes: both adapters are on the classpath and `elastic-protos.engine` picks one.
`ELASTIC_PROTOS_HOSTS` points the application at another cluster (comma-separated URLs).

## 4. Call the API

Documents are protobuf JSON: field names in camelCase (snake_case is accepted on input), enums
by name, timestamps as RFC 3339 strings and 64-bit integers as strings. `internalNotes` is an
`IGNORE` field: it is accepted but never sent to the engine, so it does not come back.

### Search

`GET /documents/search?q=<text>&size=<n>&from=<offset>` runs a full-text query across every
`TEXT` field (`title` with boost 3, `body`, `parties.name`). `size` defaults to
`elastic-protos.query.default-size` (20).

```bash
curl -s 'http://localhost:8080/documents/search?q=merger%20agreement'
```

```json
{
  "total": 2,
  "totalExact": true,
  "hits": [
    {
      "id": "doc-1",
      "score": 4.094785,
      "document": {
        "id": "doc-1",
        "firmId": "firm-1",
        "title": "Merger agreement between Acme and Globex",
        "body": "This agreement sets out the terms of the merger of Acme Corp into Globex.",
        "type": "FILING",
        "tags": ["m-and-a"],
        "parties": [
          { "name": "Acme Corp", "role": "PLAINTIFF" },
          { "name": "Globex Inc", "role": "DEFENDANT" }
        ],
        "filedAt": "2026-03-02T15:04:05Z",
        "sizeBytes": "48213"
      }
    },
    { "id": "doc-3", "score": 1.4558235, "document": { "id": "doc-3", "title": "Notice of hearing on the proposed merger", "...": "..." } }
  ]
}
```

Scores differ between engines. A `warnings` array appears when the query compiler reports
non-fatal problems.

### Index one document

`POST /documents` answers `201 Created` with the id; a document without an `id` gets a generated
one. The write waits for a refresh, so it is searchable as soon as the call returns.

```bash
curl -s -i -X POST http://localhost:8080/documents -H 'Content-Type: application/json' -d '{
  "id": "doc-4", "firmId": "firm-3", "title": "Settlement agreement",
  "body": "The parties settle all claims.", "type": "FILING", "filedAt": "2026-05-01T10:00:00Z"
}'
```

```text
HTTP/1.1 201 Created
{"id":"doc-4","result":"created"}
```

JSON that does not fit the message is a `400`:

```text
HTTP/1.1 400 Bad Request
{"error":"Cannot find field: bogus in message acme.search.v1.Document"}
```

### Read one document

```bash
curl -s http://localhost:8080/documents/doc-4
```

```json
{"id":"doc-4","firmId":"firm-3","title":"Settlement agreement","body":"The parties settle all claims.","type":"FILING","filedAt":"2026-05-01T10:00:00Z"}
```

An unknown id is a `404`.

### Bulk index

`POST /documents/_bulk` takes a JSON array, writes it through `ElasticProtosService.bulkBuffer()`
(batched per `elastic-protos.bulk.*`) and answers `202 Accepted`. A listener passed to
`bulkBuffer(listener, errorListener)` counts the bulk requests and indexed items and collects the
reason of every failed item or failed request in `errors`, which Micronaut leaves out of the JSON
when it is empty. The error listener receives each failed request's batch of operations with the
exception; the controller records the batch size and the reason.

```bash
curl -s -X POST http://localhost:8080/documents/_bulk -H 'Content-Type: application/json' -d '[
  {"id": "lease-1", "title": "Lease agreement, unit 1"},
  {"id": "lease-2", "title": "Lease agreement, unit 2"}
]'
```

```json
{"submitted":2,"batches":1,"indexed":2}
```

Page through the results with `size` and `from`:

```bash
curl -s 'http://localhost:8080/documents/search?q=lease&size=1&from=1'
```

```json
{"total":2,"totalExact":true,"hits":[{"id":"lease-2","score":3.0348785,"document":{"id":"lease-2","title":"Lease agreement, unit 2"}}]}
```

### Delete

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X DELETE http://localhost:8080/documents/doc-4   # 204
curl -s -o /dev/null -w '%{http_code}\n' -X DELETE http://localhost:8080/documents/doc-4   # 404
```

## Tests

```bash
./gradlew build
```

- `DocumentJsonSpec` round-trips documents through `DocumentJson`; no Docker needed.
- `DocumentControllerIT` starts Elasticsearch 9.5.5 with Testcontainers, boots the application
  against it with `seed.enabled=true` and exercises every endpoint. It is skipped when Docker is
  not available.

## Configuration notes

- In `application.yml`, a placeholder default that contains a colon must be wrapped in
  backticks: ``${ELASTIC_PROTOS_HOSTS:`http://localhost:9200`}``. Without them Micronaut splits
  the default at its first colon and the client fails with `No route to host`.
- `elastic-protos.defaults.replicas: 0` keeps the indices green on a single-node cluster. Leave it
  out in production (the default is data nodes minus one, at most 1).
- Every property is listed in the
  [Micronaut section of the main README](../../README.md#properties).
