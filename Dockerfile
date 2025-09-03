# Multi-stage build for BDQ CLI
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /workspace

# Copy Java project files
COPY java/ java/

# Copy Maven settings for SNAPSHOT dependencies
COPY bdq-api-files-for-debugging/.mvn.settings.xml /root/.m2/settings.xml

# Use vendored FilteredPush libraries in the repo (avoid network flakiness)

# Build Java project - run Maven from the java directory
WORKDIR /workspace/java

# Temporarily remove the problematic bdqtestrunner module from parent POM
RUN sed -i '/<module>bdqtestrunner<\/module>/d' pom.xml

# Ensure git is available and initialize minimal repos for vendored modules
# The upstream modules use git-commit-id-plugin which expects a .git directory
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/* \
 && set -eux; \
   for mod in geo_ref_qc event_date_qc sci_name_qc rec_occur_qc; do \
     echo "Initializing git repo in java/$mod"; \
     cd "/workspace/java/$mod"; \
     git init; \
     git config user.email "build@example.com"; \
     git config user.name "Build"; \
     git add -A; \
     git commit -m "vendored snapshot for build" || true; \
   done

# Now build the main project with locally installed libraries
RUN mvn -B -ntp clean package -DskipTests

# Runtime image
FROM python:3.11-slim

WORKDIR /app

# Install JRE for running the Java CLI
RUN apt-get update && apt-get install -y \
    openjdk-21-jre \
    && rm -rf /var/lib/apt/lists/*

# Copy the CLI JAR from build stage
COPY --from=build /workspace/java/bdq-cli/target/bdq-cli-1.0.0.jar /opt/bdq/bdq-cli.jar

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
ENV BDQ_CLI_JAR=/opt/bdq/bdq-cli.jar
ENV BDQ_JAVA_OPTS="-Xms256m -Xmx1024m"

# Expose port
EXPOSE 8080

# Run the Python application
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
