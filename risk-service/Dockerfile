FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY . .
RUN mvn -B -pl risk-service -am -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /workspace/risk-service/target/risk-service-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 8090 9091
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
