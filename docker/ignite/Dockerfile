FROM eclipse-temurin:17-jre
WORKDIR /app
COPY dist/datafirewall-ignite-0.0.1.jar /app/app.jar
COPY config/logback-spring.xml /app/config/logback-spring.xml
RUN mkdir -p /app/logs
EXPOSE 9900
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
