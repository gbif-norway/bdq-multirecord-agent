# Multi-stage build: First stage builds Java BDQ server
FROM maven:3.9-eclipse-temurin-17 AS bdqbuild
WORKDIR /workspace

# Copy Java project files - copy the entire java directory structure
COPY java/ java/

# Build Java project - run Maven from the java directory
WORKDIR /workspace/java
RUN mvn -q -DskipTests package

# Second stage: Runtime image with Python and JRE
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies including JRE
RUN apt-get update && apt-get install -y \
    gcc \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Copy Java BDQ server from build stage
COPY --from=bdqbuild /workspace/java/bdq-jvm-server/target/bdq-jvm-server.jar /opt/bdq/bdq-jvm-server.jar

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and TG2 test definitions
COPY app/ .
COPY TG2_tests.csv .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PORT=8080
ENV PYTHONPATH=/app
ENV BDQ_SOCKET_PATH=/tmp/bdq_jvm.sock
ENV BDQ_JAVA_OPTS="-Xms256m -Xmx1024m"

# Expose port
EXPOSE 8080

# Run the application
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
