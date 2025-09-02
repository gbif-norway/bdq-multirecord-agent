## Inline BDQ: Resident JVM Service (No HTTP path)

**STATUS: IMPLEMENTED** ✅

Goal: Remove the external bdq-api and run BDQ libraries locally in a resident JVM inside the same container. Python sends batched JSON RPC to a single long-lived Java process over a local Unix domain socket. No HTTP fallback, no compatibility layer.

**Implementation Complete**: The BDQ functionality has been successfully moved inline with a resident JVM server, TG2 test mapping parser, and Unix socket communication. See README.md for usage details.

### Repos / Libraries

- Geo reference (fork): [geo_ref_qc](https://github.com/rukayaj/geo_ref_qc)
- Event date: [event_date_qc](https://github.com/FilteredPush/event_date_qc.git)
- Scientific name: [sci_name_qc](https://github.com/FilteredPush/sci_name_qc.git)
- Record occurrence/metadata: [rec_occur_qc](https://github.com/FilteredPush/rec_occur_qc.git)

These provide the implementations referenced by TG2 tests and are suitable for direct embedding.

### Source of truth for test mapping

- Use `TG2_tests.csv` checked into this repo to drive mapping from `testId` → (library, class, method, actedUpon, consulted, parameters).
- Columns used:
  - Label (testId)
  - InformationElement:ActedUpon (actedUpon)
  - InformationElement:Consulted (consulted)
  - Parameters (parameters)
  - Example Implementations (Mechanisms) and Link to Specification Source Code (identify library, class, and validate method)

Parsing rules:
- Determine library by inspecting the "Link to Specification Source Code" URL path (e.g., contains `geo_ref_qc`, `event_date_qc`, `sci_name_qc`, `rec_occur_qc`).
- Extract class from the URL path (e.g., `DwCGeoRefDQ.java`).
- Derive method name from the `Label` using a convention: prefix lowercased (`validation|amendment|measure|issue`) + camelCase of the remainder. If ambiguous or not found via reflection, fall back to scanning the target class for methods with matching prefix and signature and confirm against the spec link.
- Normalize actedUpon/consulted term names by stripping `dwc:` and lowercasing for matching to CSV columns; preserve original `dwc:` names when calling the Java methods that expect those keys.

The parsed mapping is built once on startup by Python and provided to the Java service (see warmup below). A small static override table can address any outliers discovered during testing.

### Architecture

- Single container with 2 cooperating processes:
  - Python FastAPI app (orchestrator)
  - Java BDQ server (resident JVM)
- IPC: Unix domain socket at `/tmp/bdq_jvm.sock` using line-delimited JSON (JSON Lines). Each request and response is one line of UTF-8 JSON. For portability, fallback to localhost TCP on non-Unix if ever needed (not required for Docker/Cloud Run).
- Concurrency: Java server uses a bounded thread pool sized to available CPUs (e.g., `max(2, cores)`), executes tuples in batches per test with internal parallelism where libraries are thread-safe.
- Caching: Per-test, per-unique-tuple LRU cache across requests in the JVM to avoid recomputation when the same tuple repeats across datasets.

### Request/Response schema

Request (multiple tests in one round-trip):
```json
{
  "requestId": "uuid-...",
  "tests": [
    {
      "testId": "AMENDMENT_COORDINATES_FROM_VERBATIM",
      "actedUpon": ["dwc:decimalLatitude","dwc:decimalLongitude","dwc:geodeticDatum"],
      "consulted": ["dwc:verbatimCoordinates","dwc:verbatimLatitude","dwc:verbatimLongitude","dwc:verbatimCoordinateSystem","dwc:verbatimSRS"],
      "parameters": {"bdq:spatialBufferInMeters": "3000"},
      "tuples": [["51.5074","-0.1278","WGS84"], ["...","...","..."]]
    },
    { "testId": "VALIDATION_COUNTRY_FOUND", "actedUpon": ["dwc:country"], "tuples": [["Eswatini"], ["Tasmania"]] }
  ]
}
```

Response:
```json
{
  "requestId": "uuid-...",
  "results": {
    "AMENDMENT_COORDINATES_FROM_VERBATIM": {
      "tupleResults": [
        {"tupleIndex": 0, "status": "FILLED_IN", "result": {"dwc:decimalLatitude":"...","dwc:decimalLongitude":"...","dwc:geodeticDatum":"..."}, "comment": "..."},
        {"tupleIndex": 1, "status": "NOT_AMENDED"}
      ]
    },
    "VALIDATION_COUNTRY_FOUND": {
      "tupleResults": [
        {"tupleIndex": 0, "status": "RUN_HAS_RESULT", "result": "COMPLIANT", "comment": "..."},
        {"tupleIndex": 1, "status": "RUN_HAS_RESULT", "result": "NOT_COMPLIANT", "comment": "..."}
      ]
    }
  },
  "errors": [
    {"testId": "...", "error": "Unknown testId"}
  ]
}
```

Notes:
- Results align by `tupleIndex`. Python keeps the mapping from unique tuple → rows.
- The Java server adds `test_type` where applicable (Validation/Amendment/Issue/Measure) based on the prefix of `testId`.

### Java resident server

- Module: `bdq-jvm-server` (Maven, shaded JAR)
  - Dependencies: `geo_ref_qc`, `event_date_qc`, `sci_name_qc`, `rec_occur_qc`, Jackson, slf4j-simple
  - Startup: `java -Xms256m -Xmx1024m -jar /opt/bdq/bdq-jvm-server.jar --socket=/tmp/bdq_jvm.sock`
  - Behavior:
    - Create Unix domain socket server, accept one or more concurrent client connections
    - Each connection processes newline-delimited JSON requests; responses are returned in order
    - Internal thread pool executes tests; per-test worker batches tuples for that test
    - Warmup: on startup, accept a "warmup" message from Python containing the parsed mapping and pre-load reflective handles (classes/methods) and any library heavy resources
    - Caching: per-test LRU (size configurable) for tuple→result
    - Health: responds to `{"health":true}` ping with `{"ok":true}`

### Python integration (only local JVM)

- Remove HTTP path entirely. `BDQService` becomes a thin client over the local socket:
  - On FastAPI startup: ensure server process is running; start if not; send warmup with TG2 mapping
  - For each dataset: group by test, produce unique tuples; send one multi-test request; wait for response; map back to rows
  - Timeouts/backoff at message level; automatic reconnect/restart of JVM on failure
- Progress: maintain periodic logging and Discord updates per test during batching; the server can stream interim progress envelopes (optional), but Python can also estimate progress by counting tuples processed when partial responses are enabled later.

### Building the test mapping from TG2_tests.csv

- Implement a small Python parser for `TG2_tests.csv` that returns a dict keyed by `Label` (testId):
  - `library`: inferred from link URL (e.g., contains `geo_ref_qc`, `event_date_qc`, `sci_name_qc`, `rec_occur_qc`)
  - `javaClass`: inferred from link path (e.g., `org.filteredpush.qc.georeference.DwCGeoRefDQ`)
  - `javaMethod`: derived from Label by convention; verify via reflection during warmup; if not found, try scanning class for methods starting with the correct prefix and best token match
  - `actedUpon`, `consulted`, `parameters`: parsed as lists (split on commas, trim, keep original `dwc:` prefixes)
  - `testType`: inferred from the Label prefix

Validation during warmup:
- For each test: reflectively resolve class+method; if not resolvable, mark test as unsupported and report once to logs/Discord. Continue with other tests.

### Dockerfile (multi-stage)

1) Build Java server:
   - `FROM maven:3.9-eclipse-temurin-17 AS bdqbuild`
   - Copy `bdq-jvm-server` sources and `pom.xml`; run `mvn -q -DskipTests package`
   - Output: `/workspace/bdq-jvm-server/target/bdq-jvm-server.jar`

2) Final runtime (Python + JRE):
   - Base: current Python image
   - Install JRE 17 (or copy from `eclipse-temurin:17-jre` stage)
   - Copy the shaded JAR to `/opt/bdq/bdq-jvm-server.jar`
   - Entrypoint remains the Python app; it will spawn the JVM server on startup

### Maven + Docker build instructions

Recommended: multi-module build using a git submodule for your forked `geo_ref_qc`.

Repo layout (new):
```
bdq-multirecord-agent/
  java/
    bdq-jvm-server/           # our resident server
    geo_ref_qc/               # submodule of https://github.com/rukayaj/geo_ref_qc (optional)
    pom.xml                   # parent pom (aggregator)
```

Parent `pom.xml` sketch:
```xml
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.bdq</groupId>
  <artifactId>bdq-parent</artifactId>
  <version>1.0.0</version>
  <packaging>pom</packaging>
  <modules>
    <module>geo_ref_qc</module>        <!-- optional: include only if using fork -->
    <module>bdq-jvm-server</module>
  </modules>
  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>org.filteredpush</groupId>
        <artifactId>geo_ref_qc</artifactId>
        <version>2.1.1</version>
      </dependency>
      <dependency>
        <groupId>org.filteredpush</groupId>
        <artifactId>event_date_qc</artifactId>
        <version>3.0.5</version>
      </dependency>
      <dependency>
        <groupId>org.filteredpush</groupId>
        <artifactId>sci_name_qc</artifactId>
        <version>1.1.2</version>
      </dependency>
      <dependency>
        <groupId>org.filteredpush</groupId>
        <artifactId>rec_occur_qc</artifactId>
        <version>1.0.1</version>
      </dependency>
    </dependencies>
  </dependencyManagement>
</project>
```

`bdq-jvm-server/pom.xml` sketch:
```xml
<project>
  <parent>
    <groupId>org.bdq</groupId>
    <artifactId>bdq-parent</artifactId>
    <version>1.0.0</version>
  </parent>
  <modelVersion>4.0.0</modelVersion>
  <artifactId>bdq-jvm-server</artifactId>
  <dependencies>
    <!-- Use forked geo_ref_qc if present as module; otherwise from Central -->
    <dependency>
      <groupId>org.filteredpush</groupId>
      <artifactId>geo_ref_qc</artifactId>
    </dependency>
    <dependency>
      <groupId>org.filteredpush</groupId>
      <artifactId>event_date_qc</artifactId>
    </dependency>
    <dependency>
      <groupId>org.filteredpush</groupId>
      <artifactId>sci_name_qc</artifactId>
    </dependency>
    <dependency>
      <groupId>org.filteredpush</groupId>
      <artifactId>rec_occur_qc</artifactId>
    </dependency>
    <dependency>
      <groupId>com.fasterxml.jackson.core</groupId>
      <artifactId>jackson-databind</artifactId>
      <version>2.17.1</version>
    </dependency>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-simple</artifactId>
      <version>2.0.13</version>
    </dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.0</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <createDependencyReducedPom>false</createDependencyReducedPom>
              <transformers>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <mainClass>org.bdq.server.Main</mainClass>
                </transformer>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

Git submodule (optional, if using your fork):
```
git submodule add https://github.com/rukayaj/geo_ref_qc java/geo_ref_qc
```

Dockerfile (excerpt):
```
FROM maven:3.9-eclipse-temurin-17 AS bdqbuild
WORKDIR /workspace
COPY java/pom.xml java/pom.xml
COPY java/geo_ref_qc/ java/geo_ref_qc/   # if submodule present
COPY java/bdq-jvm-server/ java/bdq-jvm-server/
RUN mvn -q -f java/pom.xml -DskipTests package

FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=bdqbuild /workspace/java/bdq-jvm-server/target/bdq-jvm-server.jar /opt/bdq/bdq-jvm-server.jar
COPY . /app
ENV BDQ_SOCKET_PATH=/tmp/bdq_jvm.sock
ENV BDQ_JAVA_OPTS="-Xms256m -Xmx1024m"
# Python entrypoint stays the same; it spawns: java $BDQ_JAVA_OPTS -jar /opt/bdq/bdq-jvm-server.jar --socket=$BDQ_SOCKET_PATH
```

### Cloud Build

Existing `cloudbuild.yaml` builds/pushes/deploys a single Docker image. No changes needed if we keep the Java build inside the Docker multi-stage as above. The `docker build` will compile the Java modules and produce the shaded JAR automatically in the first stage, then copy it into the final image.

If build times become high, consider adding a build cache step (optional):
- Use `--cache-from` with a registry cache image; or split Java build artifacts into a separate builder image you update only when Java sources change.

### Robustness

- Supervisor: Python monitors the Unix socket; if the JVM process dies or becomes unresponsive, kill and restart it, resend warmup, and retry the current request (idempotent as we dedupe by unique tuples).
- Bounded concurrency: cap worker threads; queue overflow returns a backpressure error to the Python client which will segment the request.
- Timeouts: per-request timeout (e.g., 5–10 minutes for very large batches); subprocess-level kill on timeout with safe restart.
- Memory: set `-Xmx` conservatively; stream/process tuples in chunks inside the Java server if needed for very large batches.

### Observability

- JVM logs forwarded to Python logs; notable events mirrored to Discord:
  - Startup/ready, warmup success/failures, unknown testIds, persistent failures
- Python continues to emit per-test progress and final counts.

Error reporting (Discord):
- On any Java-side exception (per-tuple or per-test), emit a structured JSON snippet in the Discord message body so it can be pasted to a fixer model directly. Include:
```json
{
  "kind": "bdq_error",
  "requestId": "uuid-...",
  "testId": "AMENDMENT_COORDINATES_FROM_VERBATIM",
  "javaClass": "org.filteredpush.qc.georeference.DwCGeoRefDQ",
  "javaMethod": "amendmentCoordinatesFromVerbatim",
  "tupleSample": {"dwc:decimalLatitude":"-23.712","dwc:decimalLongitude":"139.92","dwc:geodeticDatum":"EPSG:4326"},
  "parameters": {"bdq:spatialBufferInMeters": "3000"},
  "exception": {"type": "java.lang.IllegalArgumentException", "message": "...", "stacktrace": "..."},
  "server": {"version": "bdq-jvm-server 1.0.0", "libs": {"geo_ref_qc": "2.1.1", "event_date_qc": "3.0.5", "sci_name_qc": "1.1.2", "rec_occur_qc": "1.0.1"}},
  "attempt": 1
}
```
- Rate-limiting: at most one alert per unique (testId, exception.type, exception.message) per minute to avoid floods; include a suppressed count if throttled.
- Grouping: per-test aggregation emits a single summary alert if all tuples for a test fail.

Progress/heartbeat (Discord):
- For long-running tests, emit periodic structured updates, keeping current behavior:
```json
{
  "kind": "bdq_progress",
  "testId": "AMENDMENT_COORDINATES_FROM_VERBATIM",
  "processed": 800,
  "total": 1681,
  "success": 780,
  "fail": 20,
  "elapsedSec": 62
}
```
- Cadence: every N=200 tuples or ~60s, whichever first; heartbeat "still processing" if no progress for >120s.
- Final per-test completion message with counts and duration.

Partial failure semantics (continue on error):
- If a single tuple throws, record a per-tuple error internally and continue; tuple result omitted from outputs.
- If a whole test method fails (e.g., reflection, required resources), return a per-test error in the response; Python:
  - logs and alerts to Discord (single alert)
  - marks the test as skipped with reason "TECHNICAL_ISSUE"
  - includes it in the email summary (existing `skipped_tests`), keeping current UX.
- Other tests in the same request continue unaffected.

Repro payloads in alerts:
- Include a compact repro section in error alerts showing `testId`, `actedUpon` names, a minimal `params` example, and the resolved `javaClass#method`. This allows copy-paste to a local runner for quick fixes.

### Migration and removal

- Remove all code that calls external bdq-api; delete endpoints/flags associated with HTTP mode.
- Keep a single code path (local JVM) for simplicity and maintainability.

### Benchmarks (live)

- Measure wall-clock, CPU/mem for representative datasets (1k–20k unique tuples across multiple tests). Expect significant improvements due to no per-tuple HTTP.

### Tasks

1) Implement `bdq-jvm-server` (resident) with Unix socket JSON-RPC and shaded deps
2) Parse `TG2_tests.csv` at Python startup to produce test mapping; send warmup to JVM
3) Update `BDQService` to use only the local JVM path; remove HTTP code
4) Update Dockerfile to build/copy server JAR and include JRE 17
5) Add health checks, restart logic, caching, and progress logging/alerts


