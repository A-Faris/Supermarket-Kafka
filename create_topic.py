"""Create topic"""

from confluent_kafka.admin import AdminClient, NewTopic

admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
name = NewTopic("Bob", 1)
job = NewTopic("sigma_labs", 1)
shop = NewTopic("supermarket", 1)
admin_client.create_topics([name, job, shop])
