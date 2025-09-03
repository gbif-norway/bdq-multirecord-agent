# Use Maven image with JDK to build and run
# Allow switching JDK easily at build time
ARG TEMURIN_VER=17
FROM maven:3.9.6-eclipse-temurin-${TEMURIN_VER}

WORKDIR /app

# Maven tuning (reduce noise, set heap)
ENV MAVEN_OPTS="-Xmx1g -XX:+UseG1GC" \
    MAVEN_CONFIG="/root/.m2"

# Copy Maven configuration
COPY pom.xml .
COPY lib lib

# Provide Maven settings to mirror GBIF repo over HTTPS
COPY .mvn.settings.xml /root/.m2/settings.xml

# Copy source code  
COPY src src

# Always fetch latest FilteredPush libraries (builds newest code)
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/* \
 && set -eux; \
   for mod in sci_name_qc geo_ref_qc event_date_qc rec_occur_qc; do \
     echo "Cloning latest $mod"; \
     rm -rf "lib/$mod"; \
     git clone --depth 1 "https://github.com/FilteredPush/$mod.git" "lib/$mod"; \
   done

# Build and install FilteredPush libraries first (networked resolves)
RUN mvn -B -ntp -U -DskipTests -Dmaven.javadoc.skip=true -Dgpg.skip=true \
    -Dgit-commit-id.skip=true -Dgit.commit.id.skip=true -Dgit-commit-id.failOnNoGitDirectory=false \
    install -f lib/sci_name_qc/pom.xml
RUN mvn -B -ntp -U -DskipTests -Dmaven.javadoc.skip=true -Dgpg.skip=true \
    -Dgit-commit-id.skip=true -Dgit.commit.id.skip=true -Dgit-commit-id.failOnNoGitDirectory=false \
    install -f lib/geo_ref_qc/pom.xml
RUN mvn -B -ntp -U -DskipTests -Dmaven.javadoc.skip=true -Dgpg.skip=true \
    -Dgit-commit-id.skip=true -Dgit.commit.id.skip=true -Dgit-commit-id.failOnNoGitDirectory=false \
    install -f lib/event_date_qc/pom.xml
RUN mvn -B -ntp -U -DskipTests -Dmaven.javadoc.skip=true -Dgpg.skip=true \
    -Dgit-commit-id.skip=true -Dgit.commit.id.skip=true -Dgit-commit-id.failOnNoGitDirectory=false \
    install -f lib/rec_occur_qc/pom.xml

# Build the application
RUN mvn -B -ntp clean package -DskipTests

# Install curl for docker-compose healthcheck
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

EXPOSE 8080

# Run the application directly
CMD ["java", "-jar", "target/bdq-api-1.0.0.jar"]
