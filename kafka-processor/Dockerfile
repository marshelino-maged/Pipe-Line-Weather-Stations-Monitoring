# Use Java 17 base image
FROM eclipse-temurin:17-jdk-alpine

WORKDIR /app

COPY /target/kafka-processor-1.0-SNAPSHOT.jar app.jar

CMD ["java", "-jar", "app.jar"]
