"""Class containing all required functions for the "productionplanner" container
"""

from confluent_kafka import Producer, Consumer
import json
import threading
import time
import logging

logging.basicConfig(level=logging.DEBUG)

config = {"bootstrap.servers": "kafka:9092", "group.id": "pp-kafka"}
completed_orders: list[int] = []
orders: list[dict] = []
change_flag = False


def on_assign(consumer, partitions):
    """Custom callback method to adjust Kafka consumer offset on subscription.
    Setting offset to 0 means, if container is started, it will always consume
    all events that are in a topic

     Args:
         consumer (_type_): Automatically passed by subscribe methods if used as callback
         partitions (_type_): Automatically passed by subscribe methods if used as callback
    """
    for p in partitions:
        if p.offset != 0:
            p.offset = 0
            logging.info(
                "[On_Assign Callback] - Reverted topic partition position to 0 to fetch all orders"
            )
    consumer.assign(partitions)


def setup(order_consumer, completed_order_consumer):
    """Thread to setup the consumers, as well as to poll all orders/completed_orders.
    Polls for 15 seconds after the last consumed event.

    Args:
        order_consumer (confluent_kafka.Consumer): Kafka Consumer object to be subscribed to orders topic
        completed_order_consumer (confluent_kafka.Consumer): Kafka Consumer object to be subscribed to completed_orders topic
    """
    global completed_orders
    global orders
    compl_change_iter = 0
    order_change_iter = 0
    has_changed = False
    order_consumer.subscribe(["orders"], on_assign=on_assign)
    completed_order_consumer.subscribe(["completed_orders"], on_assign=on_assign)
    logging.info("[Setup] - Polling for present events in topics")

    while True:

        if compl_change_iter > 30 and order_change_iter > 30:
            logging.info(
                "Polled for 30 times in 15 seconds with no changes. Pulled %d orders and %d completed orders",
                len(orders),
                len(completed_orders),
            )
            break

        completed_orders_msg = completed_order_consumer.poll(0.5)
        orders_msg = order_consumer.poll(0.5)

        if completed_orders_msg is None:
            compl_change_iter += 1
        elif completed_orders_msg.error():
            logging.error("ERROR: %s".format(completed_orders_msg.error()))
        else:
            message = json.loads(completed_orders_msg.value())
            completed_orders.append(message.get("order_id"))
            compl_change_iter = 0
            has_changed = True

        if orders_msg is None:
            order_change_iter += 1
        elif orders_msg.error():
            logging.error("ERROR: %s".format(orders_msg.error()))
        else:
            message = json.loads(orders_msg.value())
            orders.append(message)
            order_change_iter = 0
            has_changed = True


def order_update_thread(order_consumer, completed_order_consumer):
    """Thread continuously polling the provided consumers for new events.
    If a new event is polled, the global variable, its contained order/completed order
    is appended to the global variable "orders" or "completed_orders" respectively.
    Sets gobal "change_flag" if new event if polled.

    Args:
        order_consumer (confluent_kafka.Consumer): Kafka Consumer object to be subscribed to orders topic
        completed_order_consumer (confluent_kafka.Consumer): Kafka Consumer object to be subscribed to completed_orders topic
    """
    global change_flag
    global completed_orders
    global orders

    logging.info(
        "[Order Updater] - Thread started, waiting for 10 seconds to give topics time to setup"
    )
    time.sleep(10)
    loop1 = 0
    loop2 = 0

    while True:
        completed_orders_msg = completed_order_consumer.poll(0.5)
        orders_msg = order_consumer.poll(0.5)

        if completed_orders_msg is None and loop1 < 2:
            logging.info("[Order Updater] - Polled completed orders with no result")
            loop1 += 1
        elif completed_orders_msg is None and loop1 == 2:
            logging.info(
                "[Order Updater] - Polled completed orders with no result. Supressing similar messages until change"
            )
            loop1 += 1
        elif completed_orders_msg is None and loop1 > 2:
            loop1 += 1
        elif completed_orders_msg.error():
            logging.error("ERROR: %s".format(completed_orders_msg.error()))
        else:
            loop1 = 0
            message = json.loads(completed_orders_msg.value())
            completed_orders.append(message.get("order_id"))
            logging.info(
                "[Order Updater] - Updated list of completed orders with order: %d",
                message.get("order_id"),
            )
            change_flag = True

        if orders_msg is None and loop2 < 2:
            logging.info("[Order Updater] - Polled orders with no result")
            loop2 += 1
        elif orders_msg is None and loop2 == 2:
            logging.info(
                "[Order Updater] - Polled orders with no result. Supressing similar messages until change"
            )
            loop2 += 1
        elif orders_msg is None and loop2 > 2:
            loop2 += 1
        elif orders_msg.error():
            logging.error("ERROR: %s".format(orders_msg.error()))
        else:
            loop2 = 0
            message = json.loads(orders_msg.value())
            orders.append(message)
            logging.info(
                "[Order Updater] - Updated list of orders with order: %d",
                message.get("order_id"),
            )
            change_flag = True


def production_planner_thread(producer):
    """Thread that initialites planning if new orders/completed_orders are polled. Reacts to global variable
    "change_flag" and sets it to False after calling production_planner().

    Args:
        producer (confluent_kafka.Producer): Kafka Producer object used to publish to cluster
    """
    logging.info("[Production Planner] - Calling production planner on startup")
    production_planner(producer)
    global change_flag
    while True:
        if change_flag:
            logging.info(
                "[Production Planner] - Change in orders detected, calling production planner in 5 seconds"
            )
            time.sleep(5)
            production_planner(producer)
            change_flag = False
        else:
            logging.info(
                "[Production Planner] - Orders have not changed, waiting for updates"
            )
            while change_flag == False:
                time.sleep(1)
            logging.info("[Production Planner] - Orders updated.")


def production_planner(producer):
    """Used by production_planner_thread(). Generates a schedule according to available orders and
    completed_orders. Publishes empty schedule, if all orders are completed. Generated schedule sorts
    orders according to their priority and time attributes.

    Args:
        producer (confluent_kafka.Producer): Kafka Producer object used to publish to cluster
    """
    global orders
    global completed_orders
    local_orders = []
    sorted_orders = []

    logging.info("[Production Planner] - Production planner started")
    for order in orders:
        logging.info(
            "[Production Planner] - Currently inspecting order %d",
            order.get("order_id"),
        )
        if order.get("order_id") not in completed_orders:
            logging.info(
                "[Production Planner] - Order %d not completed, adding to open orders",
                order.get("order_id"),
            )
            local_orders.append(order)
        else:
            logging.info(
                "[Production Planner] - Order %d already completed, discarding",
                order.get("order_id"),
            )

    if len(local_orders) != 0:
        sorted_orders = sorted(
            local_orders,
            key=lambda order: (order.get("order_prio"), order.get("order_time")),
            reverse=True,
        )
        logging.info(
            "[Production Planner] - Order list sorted according to priority and order time"
        )
    else:
        logging.info("[Production Planner] - Order list was empty")

    producer.produce("schedules", json.dumps(sorted_orders, indent=2).encode("utf-8"))
    producer.flush()
    logging.info(
        "Event containing latest schedule published with %d orders to topic 'schedules'",
        len(sorted_orders),
    )


if __name__ == "__main__":

    order_consumer = Consumer(config)
    completed_order_consumer = Consumer(config)
    producer = Producer(config)

    orders_thread = threading.Thread(
        target=order_update_thread,
        daemon=True,
        args=(order_consumer, completed_order_consumer),
    )
    production_thread = threading.Thread(
        target=production_planner_thread, daemon=True, args=(producer,)
    )
    setup_thread = threading.Thread(
        target=setup, args=(order_consumer, completed_order_consumer)
    )
    logging.info(
        "[Initialization] - Starting setup thread. Waiting for it's termination"
    )
    setup_thread.start()
    setup_thread.join()
    logging.info("[Initialization] - Setup thread done. Starting worker threads")
    orders_thread.start()
    production_thread.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.warning("Exited via KeyboardInterrupt")
    finally:
        order_consumer.close()
        completed_order_consumer.close()
