# Build stage
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml ./
RUN mvn -B -ntp dependency:go-offline
COPY src ./src
RUN mvn -B -ntp package -DskipTests

# Runtime stage (fast-jar layout)
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/quarkus-app/ /app/
ENV NATS_URL=nats://nats:4222
EXPOSE 8080
CMD ["java", "-jar", "quarkus-run.jar"]
