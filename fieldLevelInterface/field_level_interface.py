"""Class containing all required functions for the "fieldlevelinterface" container
"""

from confluent_kafka import Producer, Consumer, TopicPartition, IsolationLevel
from confluent_kafka.admin import AdminClient, OffsetSpec
import json
from threading import Thread
import time
import logging

logging.basicConfig(level=logging.DEBUG)

config = {"bootstrap.servers": "kafka:9092", "group.id": "fli-kafka"}

latest_schedule: list = []
offset = 0
schedule_changed = False


def on_assign(consumer, partitions):
    """Custom callback method to adjust Kafka consumer offset on subscription.
    By subtracting 1, the setup thread will always find at least one schedule
    event, if the topic is not empty

     Args:
         consumer (_type_): Automatically passed by subscribe methods if used as callback
         partitions (_type_): Automatically passed by subscribe methods if used as callback
    """
    for p in partitions:
        if offset > 0:
            p.offset = offset - 1
            logging.info(
                "[On_Assign Callback] - Reverted topic partition position %d by one to fetch latest schedule",
                offset,
            )
    consumer.assign(partitions)


def setup(schedule_consumer):
    """Thread to setup the consumer, as well as to poll the latest available schedule.
    Polls for 15 seconds after the last consumed event.

    Args:
        schedule_consumer (confluent_kafka.Consumer): Kafka Consumer object to be subscribed to schedule topic
    """
    global schedule_changed
    global latest_schedule
    schedule_consumer.subscribe(["schedules"], on_assign=on_assign)
    change_iter = 0
    logging.info("[Setup] - Polling for present events in topics")
    while True:

        if change_iter > 30:
            logging.info(
                "Polled for 30 times in 15 seconds with no change. Latest fetched schedule contains %d orders",
                len(latest_schedule),
            )
            break

        schedule_msg = schedule_consumer.poll(0.5)
        if schedule_msg is None:
            change_iter += 1
        elif schedule_msg.error():
            logging.error("ERROR: %s".format(schedule_msg.error()))
        else:
            change_iter = 0
            message = json.loads(schedule_msg.value())
            latest_schedule = message
            schedule_changed = True


def schedule_updater_thread(schedule_consumer):
    """Thread continuously polling the provided consumer for new schedules.
    If a new schedule is polled, the global variable "schedule_changed" is set
    to True. Polled schedule is storing in the global variable "latest_schedule"

    Args:
        schedule_consumer (confluent_kafka.Consumer): Kafka Consumer object used to subscribe to schedule topic
    """
    global schedule_changed
    global latest_schedule
    logging.info("[Schedule Updater] - Thread started")
    loop_counter = 0

    while True:
        schedule_msg = schedule_consumer.poll(1.0)
        if schedule_msg is None and loop_counter < 2:
            logging.info(
                "[Schedule Updater] - Polled schedule with no result. Waiting for 5 seconds"
            )
            time.sleep(5)
            loop_counter += 1
        elif schedule_msg is None and loop_counter == 2:
            logging.info(
                "[Schedule Updater] - Polled schedule with no result. Waiting for 5 seconds. Supressing identical messages until change"
            )
            time.sleep(5)
            loop_counter += 1
        elif schedule_msg is None and loop_counter > 2:
            time.sleep(5)
        elif schedule_msg.error():
            logging.error("ERROR: %s".format(schedule_msg.error()))
        else:
            loop_counter = 0
            message = json.loads(schedule_msg.value())
            latest_schedule = message
            logging.info(
                "[Schedule Updater] - Updated latest schedule including %d item(s)",
                len(latest_schedule),
            )
            schedule_changed = True


def initiate_production_thread(producer):
    """Thread that initialites production if a new schedule is polled. Reacts to global variable
    "schedule_changed" and sets it to False after processing the highest priority order in the polled
    schedule from global variable "lastest_schedule". If schedule is empty, will wait for new schedule.
    After order is completed, will publish the order's property "order_id" to the topic "completed_orders"

    Args:
        producer (conflient_kafka.Producer): Kafka Producer object used to publish to cluster
    """
    global latest_schedule
    global schedule_changed
    local_schedule = latest_schedule

    logging.info(
        "[Production Intitiator] - Thread started, waiting for 15 seconds to give topics time to setup"
    )
    time.sleep(15)

    while True:
        if local_schedule:
            order = local_schedule[0]
            logging.info(
                "[Production Initiator] - Intitiated production of order with id %d",
                order.get("order_id"),
            )
            order_items: dict = order.get("order_items")
            for item_id, item in order_items.items():
                logging.info(
                    "[Production Initiator] - Starting production of item %s of order %d",
                    item_id,
                    order.get("order_id"),
                )
                oven_status = {
                    "status": 1,
                    "product": item.get("product"),
                    "temp": item.get("temp"),
                    "duration": item.get("duration"),
                }
                producer.produce(
                    "machines_status", json.dumps(oven_status, indent=2), key="oven"
                )
                producer.flush()
                oven(item.get("product"), item.get("temp"), item.get("duration"))
                oven_status = {
                    "status": 0,
                    "product": "-",
                    "temp": "-",
                    "duration": "-",
                }
                producer.produce(
                    "machines_status", json.dumps(oven_status, indent=2), "oven"
                )
                producer.flush()
                logging.info(
                    "[Production Initiator] - Completed item %s of order %d",
                    item_id,
                    order.get("order_id"),
                )
            logging.info(
                "[Production Initiator] - Order #%d completed",
                order.get("order_id"),
            )
            producer.produce(
                "completed_orders", json.dumps(order, indent=2).encode("utf-8")
            )
            producer.flush()
            logging.info(
                "[Production Initiator] - Published completed order event to topic 'completed_orders'"
            )
            schedule_changed = False
            logging.info("[Production Initiator] - Waiting for schedule update...")
            while schedule_changed == False:
                time.sleep(1)
            local_schedule = latest_schedule
            logging.info("[Production Initiator] - Schedule updated")
            time.sleep(1)

        else:
            logging.info(
                "[Production Initiator] - Schedule is empty, waiting for schedule update..."
            )
            schedule_changed = False
            while schedule_changed == False:
                time.sleep(1)
            local_schedule = latest_schedule
            logging.info("[Production Initiator] - Schedule updated")
            time.sleep(1)


def oven(product: str, temp: int, duration: int):
    """Very simple function simulating the operation of an oven with the provided arguments.
    Simply blocks excecution via time.sleep()

    Args:
        product (str): Type of product processed (either blue, red or white)
        temp (int): Temperature of the oven
        duration (int): Duration of the baking process
    """
    logging.info(
        "[Oven]- Simulating oven baking product %s with temperature %d °C for %d seconds",
        product,
        temp,
        duration,
    )
    time.sleep(duration)
    logging.info("[Oven]- Oven finished baking")


if __name__ == "__main__":
    # Initiation of kafka producers and consumers with given config
    producer = Producer(config)
    schedule_consumer = Consumer(config)

    # Queries offesets for partitions in target topic
    topic = schedule_consumer.list_topics(topic="schedules")
    partitions = [
        TopicPartition("schedules", partition)
        for partition in list(topic.topics["schedules"].partitions.keys())
    ]
    for p in partitions:
        low_offset, high_offset = schedule_consumer.get_watermark_offsets(p)
        offset = high_offset
    logging.info("[Initialization] - High Offset is %d", high_offset)

    schedule_thread = Thread(
        target=schedule_updater_thread, daemon=True, args=(schedule_consumer,)
    )
    production_thread = Thread(
        target=initiate_production_thread, daemon=True, args=(producer,)
    )
    setup_thread = Thread(target=setup, args=(schedule_consumer,))

    logging.info("[Initialization] - Starting setup thread")
    setup_thread.start()
    setup_thread.join()
    logging.info("[Initialization] - Setup thread done")
    logging.info("Starting worker threads")
    schedule_thread.start()
    production_thread.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.warning("Exited via KeyboardInterrupt")
    finally:
        schedule_consumer.close()
