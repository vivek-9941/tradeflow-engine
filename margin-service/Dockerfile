FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY . .
RUN mvn -B -pl margin-service -am -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /workspace/margin-service/target/margin-service-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 8092 9094
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
