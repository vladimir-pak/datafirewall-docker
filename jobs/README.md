Сюда добавить JAR Flink Job:
datafirewall-flink.jar.

Запускать job в flink-jm1 после сборки (build) и запуска кластера:
  docker compose exec flink-jm1 flink run -d -c com.gpb.datafirewall.Main \
    /opt/flink/usrlib/datafirewall-flink.jar --config.file /opt/flink/usrlib/job.properties
