FROM eclipse-temurin:21-jdk-jammy AS build

WORKDIR /workspace
COPY . .
RUN ./gradlew shadowJar --no-daemon

FROM kestra/kestra:1.3.39

COPY --from=build /workspace/build/libs/*.jar /app/plugins/
