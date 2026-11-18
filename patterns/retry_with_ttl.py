#!/usr/bin/env python
"""Retry transient failures without blocking the worker.

nack(requeue=false) dead-letters the message into a queue whose only job is
to wait. When its TTL expires, that queue dead-letters the message back to
the work queue. x-death counts the trips. After a fixed number, park the
message instead of retrying it forever.

    python patterns/retry_with_ttl.py
"""

import threading

import broker

WORK_EXCHANGE = "patterns.retry.work"
RETRY_EXCHANGE = "patterns.retry.delay"
WORK_QUEUE = "patterns.retry.orders"
RETRY_QUEUE = "patterns.retry.orders.400ms"
PARKING = "patterns.retry.parking"
TTL_MS = 400


def rejected_count(properties):
    for death in broker.headers_of(properties).get("x-death", []):
        if death.get("queue") == WORK_QUEUE and death.get("reason") == "rejected":
            return int(death.get("count", 0))
    return 0


def main():
    with broker.connect() as conn:
        broker.reset(
            conn,
            [WORK_QUEUE, RETRY_QUEUE, PARKING],
            [WORK_EXCHANGE, RETRY_EXCHANGE],
        )
        try:
            channel = conn.channel()
            channel.exchange_declare(WORK_EXCHANGE, exchange_type="direct", durable=True)
            channel.exchange_declare(RETRY_EXCHANGE, exchange_type="direct", durable=True)
            channel.queue_declare(PARKING, durable=True, arguments={"x-queue-type": "quorum"})
            channel.queue_declare(
                WORK_QUEUE,
                durable=True,
                arguments={
                    "x-queue-type": "quorum",
                    "x-delivery-limit": -1,
                    "x-dead-letter-exchange": RETRY_EXCHANGE,
                    "x-dead-letter-routing-key": "order",
                },
            )
            channel.queue_bind(WORK_QUEUE, WORK_EXCHANGE, routing_key="order")
            channel.queue_declare(
                RETRY_QUEUE,
                durable=True,
                arguments={
                    "x-queue-type": "quorum",
                    "x-message-ttl": TTL_MS,
                    "x-dead-letter-exchange": WORK_EXCHANGE,
                    "x-dead-letter-routing-key": "order",
                },
            )
            channel.queue_bind(RETRY_QUEUE, RETRY_EXCHANGE, routing_key="order")

            done = threading.Event()
            trace = []

            def on_message(ch, method, properties, body):
                attempts = rejected_count(properties)
                trace.append(attempts)
                print(f"delivery x-death rejected count={attempts} body={body.decode()}")
                if attempts >= 2:
                    ch.basic_publish(
                        "", PARKING, body, properties=broker.persistent()
                    )
                    ch.basic_ack(method.delivery_tag)
                    done.set()
                    return
                ch.basic_nack(method.delivery_tag, requeue=False)

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(WORK_QUEUE, on_message)
            channel.basic_publish(
                WORK_EXCHANGE,
                "order",
                b"order-17",
                properties=broker.persistent(),
            )
            if not done.wait(8):
                raise SystemExit(f"retry loop did not finish, trace={trace}")
            if trace != [0, 1, 2]:
                raise SystemExit(f"expected two delayed retries, trace={trace}")

            parked_method, _props, parked_body = broker.get_message(
                channel, PARKING, timeout=3, auto_ack=True
            )
            if parked_body != b"order-17":
                raise SystemExit(
                    f"parking lot has method={parked_method} body={parked_body}"
                )
            if broker.drain(channel, WORK_QUEUE):
                raise SystemExit("the work queue still has the message")
            print("parked order-17 after two delayed retries")
        finally:
            broker.reset(
                conn,
                [WORK_QUEUE, RETRY_QUEUE, PARKING],
                [WORK_EXCHANGE, RETRY_EXCHANGE],
            )


if __name__ == "__main__":
    main()
