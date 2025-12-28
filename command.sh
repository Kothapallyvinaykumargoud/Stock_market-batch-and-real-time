docker-compose exec kafka kafka-console-consumer
-bootstrap-server localhost:29092 \
-topic stock-market-batch
-from-beginning

docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic stock-market-realtime --from-beginning