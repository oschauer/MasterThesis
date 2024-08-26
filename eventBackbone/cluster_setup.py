"""Simple helper file to setup the kafka cluster
"""

from confluent_kafka.admin import AdminClient, NewTopic

import time
import logging

logging.basicConfig(level=logging.DEBUG)

config = {"bootstrap.servers": "localhost:29092"}
topics = ["orders", "completed_orders", "schedules", "machines_status"]

admin_client = AdminClient(config)


def setup_cluster():
    """Can be used to setup the Kafka broker with the needed topics."""

    new_topics = [
        NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics
    ]
    fs = admin_client.create_topics(new_topics)

    for topic, f in fs.items():
        try:
            f.result()
            logging.info("Topic {} created".format(topic))
        except Exception as e:
            logging.error("Failed to create topic {}: {}".format(topic, e))


def reset_cluster():
    """Deletes and recreates the topics in the Kafka broker, in turn deleting all events.
    Should not be used outside of debugging and testing.
    """
    fs = admin_client.delete_topics(topics, operation_timeout=30)
    for topic, f in fs.items():
        try:
            f.result()
            logging.info("Topic {} deleted".format(topic))
        except Exception as e:
            logging.error("Failed to delete topic {}: {}".format(topic, e))

    time.sleep(10)
    setup_cluster()


if __name__ == "__main__":
    setup_cluster()
