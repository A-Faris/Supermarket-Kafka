"""A small example of a Kafka producer."""

from os import environ

from dotenv import load_dotenv
from confluent_kafka import Producer


if __name__ == "__main__":

    load_dotenv()

    kafka_producer = Producer({
        "bootstrap.servers": environ["BOOTSTRAP_SERVERS"],
        'security.protocol': environ["SECURITY_PROTOCOL"],
        'sasl.mechanisms': environ["SASL_MECHANISM"],
        'sasl.username': environ["USERNAME"],
        'sasl.password': environ["PASSWORD"]
    })

    name = input("What is your name?: ")
    while True:
        msg = input("What is your message?: ")

        kafka_producer.produce(topic=environ["TOPIC"],
                               value=f"{name} said: {msg}"
                               )

        # kafka_producer.flush()  # Waits for all messages to be sent.
