from kafka import KafkaProducer
import time
from faker import Faker
import json
fake = Faker()
Faker.seed(0)


def kafka_python_producer_sync(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8'))
    print("Sending " + msg)
    producer.flush(timeout=60)


def success(metadata):
    print(metadata.topic)


def error(exception):
    print(exception)


def kafka_python_producer_async(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8')).add_callback(success).add_errback(error)
    producer.flush()


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='34.67.197.41:9092')
    # use your VM's external IP Here!
    for number in range(50000):
        msg = {
            "type": "trade",
            "symbol_id": "BITSTAMP_SPOT_BTC_USD",
            "sequence": fake.credit_card_number(),
            "time_exchange": fake.iso8601(),
            "time_coinapi": fake.iso8601(),
            "uuid": "770C7A3B-7258-4441-8182-83740F3E2457",
            "price": number * 20,
            "size": number / 4,
            "taker_side": "BUY"
        }
        kafka_python_producer_async(producer, json.dumps(msg), "trade")
        time.sleep(1)

