# syntax=docker/dockerfile:1

FROM openjdk:16-alpine3.13
WORKDIR /usr/app
COPY producer/target/producer-1.0-SNAPSHOT.jar /usr/app/
ENTRYPOINT ["java", "-jar", "producer-1.0-SNAPSHOT.jar"]
