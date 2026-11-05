FROM flink:1.20.3-scala_2.12-java17

# Install local directories used by JAAS, temp files and local recovery.
# The JobManagers have no mounted local-state volume, so these paths must
# already exist when the container starts. Flink processes run as `flink`.
RUN set -eux; \
    install -d -o flink -g flink -m 0775 \
        /opt/flink/local-state \
        /opt/flink/local-state/tmp \
        /opt/flink/local-state/backup; \
    mkdir -p /opt/flink/plugins/s3-fs-hadoop; \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/
