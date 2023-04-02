from confluent_kafka.admin import AdminClient, NewTopic

admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
