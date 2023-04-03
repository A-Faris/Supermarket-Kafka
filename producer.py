#!/usr/bin/env python

import sys
import json
from random import choice, randrange
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import sched, time


if __name__ == "__main__":
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument("config_file", type=FileType("r"))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser["default"])

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print("ERROR: Message failed delivery: {}".format(err))
        else:
            print(
                "Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), key=msg.key().decode("utf-8"), value=msg.value().decode("utf-8")
                )
            )

    topic = "supermarket"
    machine = ["freezer", "fridge"]
    starttime = time.time()

    def produce_event():
        id = randrange(1, 14)
        machine_type = choice(machine)
        machine_id = machine_type + "_" + str(id)
        temperature = json.dumps({ "id:" str(id), "temp": str(randrange(-10, 3)) + '°c'})
        producer.produce(topic, temperature, machine_id, callback=delivery_callback)

    while True:
        produce_event()
        producer.poll(10000)
        time.sleep(1.0 - ((time.time() - starttime) % 1.0))
