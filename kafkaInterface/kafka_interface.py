from flask import Flask, request, jsonify
import json
from confluent_kafka import Producer, Consumer
import logging
import threading

logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)
config = {"bootstrap.servers": "kafka:9092", "group.id": "kafka-interface"}

oven_status = {
    "status": 0,
    "product": "-",
    "temp": "-",
    "duration": "-",
}


@app.route("/orders", methods=["POST"])
def accept_order():
    order = json.loads(request.data)
    producer = Producer(config)
    logging.info("[Interface] - Order received via POST request")
    try:
        encoded_order = json.dumps(order, indent=2).encode("utf-8")
        producer.produce("orders", encoded_order)
        logging.info("[Interface] - Order event published")
        producer.flush()
        return jsonify(success=True)
    finally:
        producer.flush()


@app.route("/oven_status", methods=["GET"])
def oven_status_request():
    logging.info("Sending following GET repsonse: %s", get_oven_status())
    return json.dumps(get_oven_status()).encode("utf-8")


def get_oven_status():
    global oven_status
    return oven_status


def machine_update(subscriber):
    global oven_status
    subscriber.subscribe(["machines_status"])
    loop1 = 0
    while True:
        msg = subscriber.poll(1.0)
        if msg is None and loop1 < 2:
            logging.info(
                "[Machine Update Thread] - Polled nothing from machines_status topic"
            )
            loop1 += 1
        elif msg is None and loop1 == 2:
            logging.info(
                "[Machine Update Thread] - Polled nothing from machines_status topic. Supressing similar messages until change."
            )
            loop1 += 1
        elif msg is None and loop1 > 2:
            loop1 += 1
        elif msg.error():
            logging.error("ERROR: %s".format(msg.error()))
        else:
            loop1 = 0
            message = json.loads(msg.value())
            logging.info(
                "[Machine Update Thread] - Polled machine event %s from machines_status topic",
                str(message),
            )
            if msg.key().decode("ascii") == "oven":
                oven_status = message
                logging.info(
                    "[Machine Update Thread] - Polled event concernes oven, oven status updated to %s",
                    oven_status,
                )

            else:
                logging.info(
                    "[Machine Update Thread] - Polled event key %s does not concern oven",
                    msg.key(),
                )


if __name__ == "__main__":
    subscriber = Consumer(config)
    machine_update_thread = threading.Thread(
        target=machine_update, args=(subscriber,), daemon=True
    )
    machine_update_thread.start()
    app.run(host="172.18.0.5", debug=True)
