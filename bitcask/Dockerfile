FROM openjdk:21-slim

WORKDIR /app

COPY ./target/bitcask-central.jar ./bitcask-central.jar
COPY ./scripts/bitcask_client.sh ./bitcask_client.sh
COPY ./config ./config
COPY ./data ./data

RUN chmod +x bitcask_client.sh

ENTRYPOINT ["java", "-jar", "bitcask-central.jar"]