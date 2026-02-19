# Multi-stage build for Python + Java services
FROM maven:3.9-eclipse-temurin-17 AS java-builder

WORKDIR /bdq-api

# Install git (needed for submodule initialization if needed)
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Copy BDQ API source files
COPY bdq-api/pom.xml .
COPY bdq-api/src ./src
COPY bdq-api/TG2_tests.csv .
COPY bdq-api/.mvn.settings.xml ./settings.xml

# Copy bdq-api directory to check for lib submodules, then use them or clone
COPY bdq-api ./bdq-api-temp

# Use FilteredPush libs from submodules if they exist and are populated, otherwise clone them
# Cloud Build should initialize submodules, but if they're empty/missing, clone from GitHub
RUN if [ -d "bdq-api-temp/lib" ] && [ "$(ls -A bdq-api-temp/lib 2>/dev/null)" ] && [ -d "bdq-api-temp/lib/geo_ref_qc/.git" ]; then \
      echo "Using submodules from repo..." && \
      cp -r bdq-api-temp/lib ./lib; \
    else \
      echo "Submodules not found or empty, cloning FilteredPush libs from GitHub..." && \
      mkdir -p lib && \
      git clone --depth 1 https://github.com/FilteredPush/sci_name_qc.git lib/sci_name_qc && \
      git clone --depth 1 https://github.com/FilteredPush/geo_ref_qc.git lib/geo_ref_qc && \
      git clone --depth 1 https://github.com/FilteredPush/event_date_qc.git lib/event_date_qc && \
      git clone --depth 1 https://github.com/FilteredPush/rec_occur_qc.git lib/rec_occur_qc; \
    fi && \
    rm -rf bdq-api-temp

# Install rdfbeans 2.2 as 2.3-SNAPSHOT (required by FilteredPush libs via ffdq-api)
# rdfbeans:2.3-SNAPSHOT is not in Maven repos; we download 2.2 from Maven Central and install as 2.3-SNAPSHOT
RUN mvn dependency:get -Dartifact=org.cyberborean:rdfbeans:2.2 -q && \
    JAR="/root/.m2/repository/org/cyberborean/rdfbeans/2.2/rdfbeans-2.2.jar" && \
    mvn install:install-file \
      -DgroupId=org.cyberborean \
      -DartifactId=rdfbeans \
      -Dversion=2.3-SNAPSHOT \
      -Dpackaging=jar \
      -Dfile="$JAR" \
      -DgeneratePom=true \
      -DcreateChecksum=true \
      -s /bdq-api/settings.xml

# Build and install FilteredPush libs into local Maven repo (dependency order: sci_name_qc first, then geo_ref_qc)
# Use Maven settings.xml to access Sonatype OSS snapshots
RUN cd lib/sci_name_qc && mvn install -DskipTests -s ../../settings.xml && cd ../.. \
 && cd lib/geo_ref_qc && mvn install -DskipTests -s ../../settings.xml && cd ../.. \
 && cd lib/event_date_qc && mvn install -DskipTests -s ../../settings.xml && cd ../.. \
 && cd lib/rec_occur_qc && mvn install -DskipTests -s ../../settings.xml && cd ../..

# Build BDQ API (resolves FilteredPush SNAPSHOTs from local repo)
RUN mvn clean package -DskipTests -s settings.xml

# Python application stage (bookworm has openjdk-17; trixie only has openjdk-21)
FROM python:3.11-bookworm-slim

WORKDIR /app

# Install system dependencies (including Java for running BDQ API)
RUN apt-get update && apt-get install -y \
    curl \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Copy Python application code
COPY app/ app/
COPY requirements.txt .

# Install Python dependencies
ENV PIP_DEFAULT_TIMEOUT=120
RUN pip install --no-cache-dir -r requirements.txt

# Copy built BDQ API JAR from builder stage
COPY --from=java-builder /bdq-api/target/bdq-api-1.0.0.jar /app/bdq-api.jar

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PORT=8080
ENV BDQ_API_PORT=8081
ENV PYTHONPATH=/app
ENV JAVA_OPTS="-Xmx2g -Xms512m"

# Expose only port 8080 (Python FastAPI) - Java BDQ API runs internally on 8081
EXPOSE 8080

# Create startup script to run both services
RUN echo '#!/bin/bash\n\
set -e\n\
# Start BDQ API in background on port 8081\n\
echo "Starting BDQ API..."\n\
java $JAVA_OPTS -jar /app/bdq-api.jar --server.port=8081 &\n\
BDQ_PID=$!\n\
\n\
# Wait for BDQ API to be ready (up to 60 seconds)\n\
echo "Waiting for BDQ API to start..."\n\
for i in {1..60}; do\n\
  if curl -f http://localhost:8081/actuator/health > /dev/null 2>&1; then\n\
    echo "BDQ API is ready"\n\
    break\n\
  fi\n\
  if [ $i -eq 60 ]; then\n\
    echo "BDQ API failed to start after 60 seconds"\n\
    kill $BDQ_PID 2>/dev/null || true\n\
    exit 1\n\
  fi\n\
  sleep 1\n\
done\n\
\n\
# Start Python FastAPI on port 8080\n\
echo "Starting Python FastAPI..."\n\
exec python -m uvicorn app.main:app --host 0.0.0.0 --port 8080\n\
' > /app/start.sh && chmod +x /app/start.sh

# Run startup script
CMD ["/app/start.sh"]
