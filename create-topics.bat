@echo off

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server kafka:9092 --topic orders --partitions 3 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server kafka:9092 --topic orders-retry-1 --partitions 3 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server kafka:9092 --topic orders-retry-2 --partitions 3 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server kafka:9092 --topic orders-dlq --partitions 1 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server kafka:9092 --topic order-status-updates --partitions 1 --replication-factor 1

echo.
echo ============================
echo Kafka Topics Created
echo ============================

docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

pause