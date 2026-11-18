#!/usr/bin/env python
"""A poison message must not be redelivered forever.

On a quorum queue, basic.reject(requeue=true) counts as a failed delivery.
basic.nack(requeue=true) does not, on current RabbitMQ: it means "not this
consumer right now". When x-delivery-count passes x-delivery-limit, the
broker dead-letters the message instead of handing it out again.

    python patterns/poison_delivery_limit.py
"""

import time

import broker

QUEUE = "patterns.poison.orders"
DLX = "patterns.poison.dead-letter"
PARKING = "patterns.poison.parking"
LIMIT = 2


def main():
    with broker.connect() as conn:
        broker.reset(conn, [QUEUE, PARKING], [DLX])
        try:
            channel = conn.channel()
            channel.exchange_declare(DLX, exchange_type="direct", durable=True)
            channel.queue_declare(
                PARKING, durable=True, arguments={"x-queue-type": "quorum"}
            )
            channel.queue_bind(PARKING, DLX, routing_key="poison")
            channel.queue_declare(
                QUEUE,
                durable=True,
                arguments={
                    "x-queue-type": "quorum",
                    "x-delivery-limit": LIMIT,
                    "x-dead-letter-exchange": DLX,
                    "x-dead-letter-routing-key": "poison",
                },
            )
            channel.basic_publish(
                "", QUEUE, b"bad-payload", properties=broker.persistent()
            )

            counts = []
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                method, properties, body = channel.basic_get(QUEUE, auto_ack=False)
                if method is None:
                    parked = broker.drain(channel, PARKING)
                    if parked:
                        print(f"dead-lettered after delivery counts {counts}")
                        if counts != [0, 1, 2]:
                            raise SystemExit(
                                f"expected deliveries 0,1,2 before the limit, got {counts}"
                            )
                        if parked[0][1] != b"bad-payload":
                            raise SystemExit("parking lot has the wrong body")
                        return
                    time.sleep(0.05)
                    continue
                count = broker.headers_of(properties).get("x-delivery-count", 0)
                counts.append(count)
                print(f"reject requeue=true x-delivery-count={count} body={body.decode()}")
                channel.basic_reject(method.delivery_tag, requeue=True)
                time.sleep(0.05)
            raise SystemExit(f"poison message was not dead-lettered, counts={counts}")
        finally:
            broker.reset(conn, [QUEUE, PARKING], [DLX])


if __name__ == "__main__":
    main()
