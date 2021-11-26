from kafka.admin import KafkaAdminClient, NewTopic


def delete_topics(admin, topic_list):
    admin.delete_topics(topics=topic_list)


def create_topics(admin, topic_list):
    admin.create_topics(new_topics=topic_list, validate_only=False)


if __name__ == '__main__':
    admin_client = KafkaAdminClient(bootstrap_servers="34.69.175.54:9092",
                                    client_id='Crypto_producer')  # use your VM's external IP Here!
    topic_list = [NewTopic(name="trade_echo", num_partitions=1, replication_factor=1),
                  NewTopic(name="trade", num_partitions=1, replication_factor=1, topic_configs={'retention.ms': '900000'})]
    delete_topics(admin_client, ['trade', 'trade_echo'])
    create_topics(admin_client, topic_list)
