"""Consumer"""
#!/usr/bin/env python

import json
from argparse import ArgumentParser, FileType
from configparser import ConfigParser

from confluent_kafka import Consumer, OFFSET_BEGINNING, KafkaException

if __name__ == "__main__":
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument("config_file", type=FileType("r"))
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser["default"])
    config.update(config_parser["consumer"])

    # Create Consumer instance
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        """Reset offset"""
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    TOPIC = "supermarket"
    consumer.subscribe([TOPIC], on_assign=reset_offset)

    # Task: Poll for new messages from Kafka and print them.
    #
    # You can use `consumer.poll(1.0)` to get a single message at
    # a time from Kafka.
    #
    def temp_error(temp, type, machine_id):
        """Says if mechine is too hot or cold"""
        if type == "freezer" and temp > -1:
            return f"ERROR: FREEZER WITH ID {machine_id} IS TOO HOT"
        if type == "fridge":
            if temp > 3:
                return f"""ERROR: FREEZER WITH ID {machine_id} IS TOO HOT"""
            elif temp < -1:
                return f"""ERROR: FREEZER WITH ID {machine_id} IS TOO COLD"""

    try:
        while True:
            # Write code here to get messages from Kafka
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                raise KafkaException(msg.error())
            else:
                value = json.loads(msg.value().decode('utf-8'))
                machine_id = msg.key().decode('utf-8')
                print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), key=machine_id, value=str(value)))

                temp = int(value["temp"][:-2])
                type = value["type"]
                error = temp_error(temp, type, machine_id)
                if error:
                    print(error)

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
