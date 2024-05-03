from confluent_kafka.admin import AdminClient, NewTopic

def delete_topic(bootstrap_servers, topic_name):
    # Create an AdminClient instance
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    # Check if the topic exists
    topic_metadata = admin_client.list_topics(timeout=5)
    if topic_name not in topic_metadata.topics:
        print(f"Topic '{topic_name}' does not exist.")
        return

    # Construct the topic deletion request
    futures = admin_client.delete_topics([topic_name])

    # Wait for the topic deletion to complete
    for topic, future in futures.items():
        try:
            future.result()  # Wait for the operation to complete
            print(f"Topic '{topic}' deleted successfully.")
        except Exception as e:
            print(f"Failed to delete topic '{topic}': {e}")

if __name__ == "__main__":
    bootstrap_servers = 'localhost:9092'  # Kafka broker address
    topic_name = 'test2'  # Name of the topic to delete

    delete_topic(bootstrap_servers, topic_name)
