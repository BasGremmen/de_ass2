from kafka.admin import KafkaAdminClient, NewTopic

# Remove topics
def delete_topics(admin, topic_list):
    admin.delete_topics(topics=topic_list)

# Add topics
def create_topics(admin, topic_list):
    admin.create_topics(new_topics=topic_list, validate_only=False)

# Create the topics beforehand, with a retention time of 15 minutes, keeping the data any longer is unnecessary
if __name__ == '__main__':
    admin_client = KafkaAdminClient(bootstrap_servers="34.67.197.41:9092",
                                    client_id='Crypto_producer')  # use your VM's external IP Here!
    topic_list = [NewTopic(name="trade", num_partitions=1, replication_factor=1, topic_configs={'retention.ms': '900000'}),
                  NewTopic(name="trades_aggregated", num_partitions=1, replication_factor=1, topic_configs={'retention.ms': '900000'})]
    delete_topics(admin_client, ['trade', 'trades_aggregated'])
    create_topics(admin_client, topic_list)
