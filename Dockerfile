# Multi-stage build for Python + Java services
FROM maven:3.9-eclipse-temurin-17 AS java-builder

WORKDIR /bdq-api

# Copy BDQ API and FilteredPush libs (submodules built below)
COPY bdq-api/pom.xml .
COPY bdq-api/src ./src
COPY bdq-api/TG2_tests.csv .
COPY bdq-api/lib ./lib

# Build and install FilteredPush libs into local Maven repo (dependency order: sci_name_qc first, then geo_ref_qc)
RUN cd lib/sci_name_qc && mvn install -DskipTests && cd ../.. \
 && cd lib/geo_ref_qc && mvn install -DskipTests && cd ../.. \
 && cd lib/event_date_qc && mvn install -DskipTests && cd ../.. \
 && cd lib/rec_occur_qc && mvn install -DskipTests && cd ../..

# Build BDQ API (resolves FilteredPush SNAPSHOTs from local repo)
RUN mvn clean package -DskipTests

# Python application stage
FROM python:3.11-slim

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
