FROM eclipse-temurin:17-jre
WORKDIR /app
COPY dist/jms-publisher-0.0.1.jar /app/app.jar
EXPOSE 9800
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
