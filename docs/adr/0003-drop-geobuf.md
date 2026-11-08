# 0003. Drop geobuf; support simple geo only

- Status: Accepted
- Date: 2026-11-01

## Context

The pre-1.0 library depended on a private geobuf library (`GeoBufProtos`, `GeobufUtils`) and on
GeoTools from the OSGeo repository. The private dependency was not resolvable outside its original
monorepo, so the build was broken for anyone else, and geobuf conversion was wired through the JSON
codec. An open-source library cannot depend on unpublished artifacts or require a non-Central
repository.

## Decision

- Remove geobuf and GeoTools entirely.
- Keep geo support that needs no geometry library:
  - `geo_point` for `phatjam98.elasticprotos.v1.LatLon`, for messages with `lat`/`lon` number fields
    and for strings (`"lat,lon"` or WKT `POINT`), by default or with `type: GEO_POINT`;
  - `geo_shape` for `phatjam98.elasticprotos.v1.GeoShape` (WKT or GeoJSON) and for strings, by default
    or with `type: GEO_SHAPE`;
  - geo queries (`GEO_DISTANCE`, `GEO_BOUNDING_BOX`, `GEO_INTERSECTS`, `GEO_CONTAINS`,
    `GEO_CONTAINED`) and geo-distance sorting.
- `FlatValue` field 9 (`geo_value`) is reserved so the old wire format cannot be misread.

## Consequences

- Every dependency is on Maven Central and the build works from a clean checkout.
- Geometry is passed through to the engine as WKT or GeoJSON; the library does not validate,
  simplify or reproject geometries.
- Users of geobuf convert to WKT or GeoJSON before indexing.
