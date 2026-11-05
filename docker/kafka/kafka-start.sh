#!/usr/bin/env bash
set -euo pipefail

: "${KAFKA_CLUSTER_ID:?KAFKA_CLUSTER_ID is required}"
: "${KAFKA_USERNAME:?KAFKA_USERNAME is required}"
: "${KAFKA_PASSWORD:?KAFKA_PASSWORD is required}"

cat >/tmp/datafirewall-server.properties <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@kafka:9093
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,INTERNAL:SASL_PLAINTEXT,EXTERNAL:SASL_PLAINTEXT
listeners=CONTROLLER://0.0.0.0:9093,INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094
advertised.listeners=INTERNAL://kafka:9092,EXTERNAL://${KAFKA_EXTERNAL_HOST:-localhost}:9094
inter.broker.listener.name=INTERNAL
sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
listener.name.internal.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${KAFKA_USERNAME}" password="${KAFKA_PASSWORD}";
listener.name.external.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${KAFKA_USERNAME}" password="${KAFKA_PASSWORD}";
log.dirs=/var/lib/kafka/data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=0
num.partitions=1
EOF

if [[ ! -f /var/lib/kafka/data/meta.properties ]]; then
  echo 'Formatting new Kafka KRaft metadata log with SCRAM credentials'
  /opt/kafka/bin/kafka-storage.sh format \
    -t "$KAFKA_CLUSTER_ID" \
    -c /tmp/datafirewall-server.properties \
    --add-scram "SCRAM-SHA-512=[name=\"${KAFKA_USERNAME}\",password=\"${KAFKA_PASSWORD}\"]"
else
  echo 'Kafka KRaft storage exists: reusing persisted broker credentials'
fi

exec /opt/kafka/bin/kafka-server-start.sh /tmp/datafirewall-server.properties
