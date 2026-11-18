"""Connection and message helpers for the scripts in this directory."""

import time

import pika


def connect():
    return pika.Connection(pika.ConnectionParameters(host="localhost"))


def persistent(**kwargs):
    return pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent,
        **kwargs,
    )


def decode(value):
    if isinstance(value, bytes):
        return value.decode()
    if isinstance(value, list):
        return [decode(item) for item in value]
    if isinstance(value, dict):
        return {decode(key): decode(item) for key, item in value.items()}
    return value


def headers_of(properties):
    if properties is None or not properties.headers:
        return {}
    return decode(properties.headers)


def reset(conn, queues, exchanges):
    """Delete topology from a previous run. Missing objects are ignored."""
    for name in queues:
        channel = conn.channel()
        try:
            channel.queue_delete(name)
        except Exception:
            continue
        channel.close()
    for name in exchanges:
        channel = conn.channel()
        try:
            channel.exchange_delete(name)
        except Exception:
            continue
        channel.close()


def get_message(channel, queue, timeout=5, auto_ack=False):
    deadline = time.monotonic() + timeout
    while True:
        method, properties, body = channel.basic_get(queue, auto_ack=auto_ack)
        if method is not None:
            return method, properties, body
        if time.monotonic() >= deadline:
            return None, None, None
        time.sleep(0.05)


def drain(channel, queue):
    messages = []
    while True:
        method, properties, body = channel.basic_get(queue, auto_ack=True)
        if method is None:
            return messages
        messages.append((properties, body))
