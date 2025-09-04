# Multi-stage build for BDQ Py4J Gateway
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /workspace

# Copy Java project files
COPY java/ java/

# Copy Maven settings for SNAPSHOT dependencies
COPY .mvn.settings.xml /root/.m2/settings.xml

# Build BDQ libraries first - run Maven from the java directory
WORKDIR /workspace/java

# Temporarily remove the problematic bdqtestrunner module from parent POM
RUN sed -i '/<module>bdqtestrunner<\/module>/d' pom.xml

# Disable git-commit-id-maven-plugin in vendored modules (no .git in container context)
RUN set -eux; \
  for pom in geo_ref_qc/pom.xml event_date_qc/pom.xml sci_name_qc/pom.xml rec_occur_qc/pom.xml; do \
    perl -i -pe 'BEGIN{undef $/;} s|<plugin>\s*<groupId>io\.github\.git-commit-id</groupId>.*?</plugin>||smg' "$pom"; \
  done

# Build BDQ libraries first
RUN mvn -B -ntp clean install -DskipTests -pl geo_ref_qc,sci_name_qc,event_date_qc,rec_occur_qc

# Build the Py4J Gateway with all dependencies
WORKDIR /workspace/java/bdq-py4j-gateway
RUN mvn -B -ntp clean package -DskipTests

# Runtime image
FROM python:3.11-slim

WORKDIR /app

# Install JRE for running the Py4J Gateway
RUN apt-get update && apt-get install -y \
    openjdk-21-jre \
    && rm -rf /var/lib/apt/lists/*

# Copy the Py4J Gateway JAR from build stage
COPY --from=build /workspace/java/bdq-py4j-gateway/target/bdq-py4j-gateway-1.0.0.jar /opt/bdq/bdq-py4j-gateway.jar

# Copy Python application code
COPY app/ app/
COPY requirements.txt .
COPY bdq-spec/tg2/core/TG2_tests.csv .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PORT=8080
ENV PYTHONPATH=/app
ENV BDQ_PY4J_GATEWAY_JAR=/opt/bdq/bdq-py4j-gateway.jar
ENV BDQ_JAVA_OPTS="-Xms256m -Xmx1024m"

# Expose port
EXPOSE 8080

# Run the Python application
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
