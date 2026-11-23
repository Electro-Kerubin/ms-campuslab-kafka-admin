# Imagen del microservicio administrador de Kafka
# Build:  docker build -t ms-campuslab-kafka-admin:latest .
# Run:    docker run -p 8087:8087 -e KAFKA_BOOTSTRAP_SERVERS=kafka1:29092,kafka2:29092,kafka3:29092 ms-campuslab-kafka-admin:latest

# 1) Compilamos con Maven y JDK 21
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
# Descarga dependencias primero para aprovechar la caché de capas
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests -B

# 2) Imagen final liviana: solo el JRE y el jar
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar

ENV SERVER_PORT=8087

EXPOSE 8087
ENTRYPOINT ["java", "-jar", "app.jar"]
