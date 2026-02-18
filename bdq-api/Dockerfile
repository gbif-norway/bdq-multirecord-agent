FROM eclipse-temurin:17-jre

WORKDIR /app

COPY target/bdq-api-1.0.0.jar app.jar

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
