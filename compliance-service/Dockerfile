FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY . .
RUN mvn -B -pl compliance-service -am -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /workspace/compliance-service/target/compliance-service-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 8093 9093
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
