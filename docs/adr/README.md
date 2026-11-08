# Architecture decision records

Short records of the decisions that shape elastic-protos, in the
[MADR](https://adr.github.io/madr/) style: context, decision, consequences.

| ADR | Title | Status | Date |
|---|---|---|---|
| [0001](0001-engine-agnostic-json-core.md) | Engine-agnostic JSON core behind a `SearchEngine` SPI | Accepted | 2026-11-01 |
| [0002](0002-proto-options-for-mapping.md) | Proto options, not field-name heuristics, drive mappings | Accepted | 2026-11-01 |
| [0003](0003-drop-geobuf.md) | Drop geobuf; support simple geo only | Accepted | 2026-11-01 |
| [0004](0004-java-17-baseline.md) | Java 17 baseline and the protobuf 3.25 line | Accepted | 2026-11-01 |
| [0005](0005-lockstep-versioning.md) | Lockstep versioning of all modules | Accepted | 2026-11-01 |

## Adding a record

Copy the structure of an existing record into `NNNN-short-title.md` with the next number, add it to
the table above, and include it in the pull request that implements the decision. Records are not
rewritten later: a reversed decision gets a new record, and the old one's status becomes
"Superseded by NNNN".
