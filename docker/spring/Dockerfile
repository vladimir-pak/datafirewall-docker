FROM eclipse-temurin:17-jre
WORKDIR /app
COPY dist/datafirewall-spring-0.0.1.jar /app/app.jar
COPY config/logback-spring.xml /app/config/logback-spring.xml
RUN mkdir -p /app/logs
EXPOSE 9200
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
