# New Development Plan

## Goals
- Eliminate avoidable external API calls by mirroring critical reference data locally and hydrating caches at startup.
- Improve validator throughput by avoiding repeated network latency and expensive resource initialisation.

## Immediate Focus Areas
1. **Caching & Data Localisation**
   - Treat the service as “offline-first”: every dependency that can be mirrored locally should be downloaded, versioned, and cached in-process before any external query is attempted.
   - Build service-level caches (consider Caffeine/Ehcache for in-process usage or Redis for shared caches) for residual lookups that still require remote calls:
     - GBIF taxonomy, WoRMS, IRMNG, and Global Names parser responses (keyed by canonical name and authority).
     - GeoLocate locality queries (normalize keys and introduce TTL or LRU eviction).
   - Persist authoritative vocabularies and supporting data under version control or in an artefact bucket, refreshing them on a scheduled job:
     - Natural Earth / GADM country and primary-division boundaries, EEZ polygons, and centroid datasets (replacing Getty country checks).
     - ISO 3166 alpha-2/alpha-3/numeric code tables, IANA TZ, and other small static reference lists.
     - GBIF backbone snapshots trimmed to higher taxonomy (kingdom → genus) plus auxiliary lists of all genera and common canonical forms.
     - GBIF vocabularies (basisOfRecord, establishmentMeans, etc.) stored as JSON; record the source URL, download timestamp, and hash for auditing.
   - Expose a lightweight data-update pipeline (CLI or GitHub workflow) that redownloads these datasets, verifies checksums, and rebuilds any derived indexes (e.g. R-tree caches for polygons).

2. **Geospatial Optimisation**
   - Avoid reopening shapefiles for each query. Load Natural Earth boundaries and other spatial layers once (e.g. via GeoTools feature stores cached in memory or an embedded spatial index) and reuse them throughout the process.
   - Add a point-to-country cache that snaps coordinates to a small grid (e.g. 0.1°) to amortise spatial lookups.

3. **Supporting Infrastructure**
   - Define warm-up routines to hydrate caches, load local datasets, and publish readiness once in-memory indexes are ready.
   - Add structured logging and metrics (latency per external dependency, cache hit rates) to identify future hotspots.
   - Provide integration tests that mock external APIs to ensure determinism.

## Suggested Next Steps
1. Audit current validators to catalogue every external dependency and identify which ones can be replaced with local snapshots immediately.
2. Implement the data-ingest workflow that downloads/validates boundary files, GBIF taxonomy slices, and vocabularies; generate serialised caches (e.g. GeoPackage/SQLite or compressed binary indexes).
3. Layer in service caches and local data stores, ensuring fallbacks exist for stale or missing datasets.
4. Add monitoring/tests to verify that runtime traffic to external services trends toward zero and that cache refresh jobs keep data current.
