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



# Now build the main project with locally installed libraries
RUN mvn -B -ntp -Dgit.commit.id.skip=true clean package -DskipTests

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
