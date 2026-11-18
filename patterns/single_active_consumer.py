#!/usr/bin/env python
"""One active consumer, with a hot standby.

x-single-active-consumer keeps ordering without making the worker a single
point of failure. The broker delivers only to the active consumer. When that
consumer is cancelled, the standby is promoted and new messages move there.
Unacked messages from the old consumer are requeued to the new one.

    python patterns/single_active_consumer.py
"""

import queue
import time

import broker

QUEUE = "patterns.single-active-consumer.orders"


def main():
    with broker.connect() as first_conn, broker.connect() as second_conn:
        broker.reset(first_conn, [QUEUE], [])
        try:
            for conn in (first_conn, second_conn):
                channel = conn.channel()
                channel.queue_declare(
                    QUEUE,
                    durable=True,
                    arguments={
                        "x-queue-type": "quorum",
                        "x-single-active-consumer": True,
                    },
                )
                channel.close()

            received = queue.Queue()
            consumers = {}

            def subscribe(name, conn):
                channel = conn.channel()
                channel.basic_qos(prefetch_count=1)

                def on_message(ch, method, _properties, body):
                    received.put((name, body))
                    ch.basic_ack(method.delivery_tag)

                tag = channel.basic_consume(QUEUE, on_message)
                consumers[name] = (channel, tag)

            subscribe("worker-a", first_conn)
            subscribe("worker-b", second_conn)
            time.sleep(0.2)

            publisher = first_conn.channel()
            publisher.basic_publish("", QUEUE, b"m1", properties=broker.persistent())
            active, body = received.get(timeout=5)
            standby = "worker-b" if active == "worker-a" else "worker-a"
            print(f"{active} is active and got {body.decode()}")

            publisher.basic_publish("", QUEUE, b"m2", properties=broker.persistent())
            name, body = received.get(timeout=5)
            print(f"{name} got {body.decode()}")
            if name != active or body != b"m2":
                raise SystemExit("the standby received a message while the active consumer was up")

            channel, tag = consumers[active]
            channel.basic_cancel(tag)
            publisher.basic_publish("", QUEUE, b"m3", properties=broker.persistent())
            name, body = received.get(timeout=5)
            print(f"after cancel, {name} got {body.decode()}")
            if name != standby or body != b"m3":
                raise SystemExit(f"expected {standby} to be promoted, got {name} {body}")
        finally:
            broker.reset(first_conn, [QUEUE], [])


if __name__ == "__main__":
    main()
