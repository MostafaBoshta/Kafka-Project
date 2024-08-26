from confluent_kafka.admin import AdminClient, NewTopic
conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
}

admin_client = AdminClient(conf)

# Define the topic configuration
me = 'MostafaHamdy-1'
num_partitions = 3
replication_factor = 1

error_topic = 'MostafaHamdyerrorTopic'
success_topic = 'MostafaHamdySuccessTopic'


# Create a NewTopic object with the desired configuration
new_topic = NewTopic(me, num_partitions=num_partitions, replication_factor=replication_factor)
error_topic = NewTopic(error_topic, num_partitions=num_partitions, replication_factor=replication_factor)
success_topic = NewTopic(success_topic, num_partitions=num_partitions, replication_factor=replication_factor)
#Add the new topic to the Kafka admin client
admin_client.create_topics([new_topic])[me].result()

admin_client.create_topics([error_topic])[error_topic].result()
admin_client.create_topics([success_topic])[success_topic].result()


for topic in admin_client.list_topics().topics:
    print(topic)