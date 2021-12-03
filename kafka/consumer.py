from kafka import KafkaConsumer

# Read topic results
def read_from_topic(kafka_consumer, topic):
    kafka_consumer.subscribe(topics=[topic])
    for msg in kafka_consumer:
        print(msg.value.decode("utf-8"))

# Helper script to check if everything works correctly
if __name__ == '__main__':
    consumer = KafkaConsumer(bootstrap_servers='34.122.235.139:9092',  # use your VM's external IP Here!
                             auto_offset_reset='latest',
                             consumer_timeout_ms=90000)
    read_from_topic(consumer, 'trades_aggregated')
