"""A small example of a Kafka consumer."""

from os import environ

from dotenv import load_dotenv
from confluent_kafka import Consumer


def consume_messages(consumer: Consumer) -> None:
    while True:
        print("Before")
        msg = consumer.poll(1)
        print("After")
        if msg:
            print(msg.value().decode())


if __name__ == "__main__":

    load_dotenv()

    consumer = Consumer({
        "bootstrap.servers": environ["BOOTSTRAP_SERVERS"],
        "group.id": environ["ID"],
        "auto.offset.reset": environ["AUTO_OFFSET"],
        'security.protocol': environ["SECURITY_PROTOCOL"],
        'sasl.mechanisms': environ["SASL_MECHANISM"],
        'sasl.username': environ["USERNAME"],
        'sasl.password': environ["PASSWORD"]
    })

    consumer.subscribe([environ["TOPIC"]])
    consume_messages(consumer)
