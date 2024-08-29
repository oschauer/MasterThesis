"""Offers an UI to submit orders to the ERP. Includes methods for generating and sending orders.

"""

import random
import time
import requests
import logging
from nicegui import ui

logging.basicConfig(level=logging.DEBUG)

# Available product varieties
products = ["white", "red", "blue"]
last_submitted_order = ""
last_response_code = ""
oven_status = "no response from Field Interface yet..."


custom_items = []
custom_items_str = ""


def generate_random_order():
    """Generates a randomized order.

    Returns:
        dict: Order ready to be submitted to the cluster
    """
    item_id = 0
    order_items = {}
    for _ in range(random.randint(1, 4)):
        order_items.update(generate_random_order_item(item_id))
        item_id += 1
    return {
        "order_id": random.randint(1, 1000000),
        "order_time": time.time(),
        "order_prio": random.randint(1, 5),
        "order_items": order_items,
    }


def generate_custom_order(prio, items: list[dict]):
    """Generates a custom order with given parameters.

    Args:
        prio (int): The priority of the order to be generated
        items (list[dict]): List of order items

    Returns:
        dict: Order ready to be submitted to the cluster
    """
    item_id = 0
    order_items = {}
    for item in items:
        order_items.update(generate_custom_order_item(item_id, item))

        item_id += 1
    return {
        "order_id": random.randint(1, 1000000),
        "order_time": time.time(),
        "order_prio": prio,
        "order_items": order_items,
    }


def generate_custom_order_item(item_id, item):
    """Generates a valid order_item entry

    Args:
        item_id (int): Key for the dict entry
        item (dict): Value for the dict entry

    Returns:
        dict: A valid entry for the "order_items" dict of an order
    """
    return {item_id: item}


def generate_random_order_item(item_id):
    """Generates an order item for a randomly generated order.

    Args:
        item_id (int): ID of the order item

    Returns:
        dict: Entry for the order_items of a randomly generated order
    """
    product = random.choice(products)
    temp = random.randint(1, 5) * 100
    duration = random.randint(5, 15)

    return {item_id: {"product": product, "temp": temp, "duration": duration}}


def ui_random_button_trigger():
    """Onclick method for the "random order" button"""
    url = "http://172.18.0.5:5000/orders"
    order = generate_random_order()
    global last_submitted_order
    global last_response_code
    last_submitted_order = str(order)
    r = requests.post(url, json=order, timeout=5)
    logging.info("Status code for latest request is: %d", r.status_code)
    last_response_code = str(r.status_code)


def ui_custom_button_trigger(prio):
    """Onclick method for the "submit order" button

    Args:
        prio (int): Priority of the assembled order
    """
    global custom_items
    url = "http://172.18.0.5:5000/orders"
    order = generate_custom_order(prio, custom_items)
    global last_submitted_order
    global last_response_code
    last_submitted_order = str(order)
    r = requests.post(url, json=order, timeout=5)

    last_response_code = str(r.status_code)
    clear_custom_items()


def ui_refresh_trigger():
    """Onclick method for the "refresh" button for the oven status"""
    global oven_status
    url = "http://172.18.0.5:5000/oven_status"
    r = requests.get(url)
    logging.info("Status code for latest GET request is: %d", r.status_code)
    msg = r.json()
    logging.info("Got new oven status %s, code: %d", str(msg), r.status_code)
    oven_status = str(msg)


def user_interface():
    """Constructs the UI of the ERP"""
    global last_response_code
    global last_submitted_order
    global custom_items
    global custom_items_str
    global oven_status

    with ui.dialog() as dialog, ui.card():
        with ui.column():
            with ui.row():
                ui.label("Product Type: ")
                product = ui.toggle(["white", "red", "blue"], value="white")
            with ui.row():
                ui.label("Oven temperature: ")
                temp = ui.slider(min=100, max=500, value=100)
            ui.label().bind_text_from(temp, "value")
            with ui.row():
                ui.label("Baking duration: ")
                dur = ui.slider(min=1, max=30, value=1)
            with ui.row():
                ui.label().bind_text_from(dur, "value")
                ui.label("seconds")
            with ui.row():
                ui.button("Abort", on_click=dialog.close)
                ui.button(
                    "Submit",
                    on_click=lambda: (
                        custom_items.append(
                            {
                                "product": product.value,
                                "temp": temp.value,
                                "duration": dur.value,
                            }
                        ),
                        set_custom_items_str(custom_items),
                        dialog.close(),
                    ),
                )

    with ui.row():
        with ui.column():
            ui.label("Create new order")
            ui.separator()
            with ui.row():
                ui.label("Priority (5 is highest):")
                prio = ui.toggle([1, 2, 3, 4, 5], value=1)
            with ui.row():
                ui.label("Order items")
                with ui.scroll_area().classes("w-64 h-64 border"):
                    ui.label().bind_text_from(globals(), "custom_items_str")
                with ui.column():
                    ui.button("Add item", on_click=dialog.open)
                    ui.button("Discard items", on_click=clear_custom_items)
            ui.button(
                "Submit order", on_click=lambda: ui_custom_button_trigger(prio.value)
            )
            ui.button(
                "Generate and submit random order", on_click=ui_random_button_trigger
            )

        with ui.column():
            ui.label("Last submitted order")
            ui.separator()
            with ui.scroll_area().classes("w-64 h-64 border"):
                last_order_label = ui.label(last_submitted_order).bind_text_from(
                    globals(), "last_submitted_order"
                )
            with ui.row():
                ui.label("Status code: ")
                ui.label(last_response_code).bind_text_from(
                    globals(), "last_response_code"
                )

        with ui.column():
            ui.label("Oven Status")
            ui.separator()
            with ui.scroll_area().classes("w-64 h-64 border"):
                ui.label(last_submitted_order).bind_text_from(globals(), "oven_status")
            ui.button("Refresh", on_click=ui_refresh_trigger)

    ui.run(port=8081)
    ui.separator()


def set_custom_items_str(s):
    """Setter methods for the custom items string variable

    Args:
        s (str): Value the string to be set to
    """
    global custom_items_str
    custom_items_str = str(s)
    if not custom_items_str:
        custom_items_str = "-"


def clear_custom_items():
    """Clears the custom items variable (list)"""
    global custom_items
    custom_items.clear()
    set_custom_items_str("")


if __name__ in {"__main__", "__mp_main__"}:
    user_interface()
