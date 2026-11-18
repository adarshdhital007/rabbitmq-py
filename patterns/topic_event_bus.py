#!/usr/bin/env python
"""Route a stream of events with a topic exchange.

A routing key is a hierarchy, not a label. * matches one word and # matches
zero or more. Bind queues to the facts they own. A message can match more
than one queue, which is how audit and billing both see the same payment.

    python patterns/topic_event_bus.py
"""

import broker
from confirms import Confirmer

EXCHANGE = "patterns.events"
BINDINGS = {
    "patterns.events.billing": ["order.*.paid"],
    "patterns.events.fraud": ["order.eu.*"],
    "patterns.events.audit": ["order.#"],
}
PUBLISH = [
    ("order.us.paid", b"us-paid"),
    ("order.eu.paid", b"eu-paid"),
    ("order.eu.cancelled", b"eu-cancelled"),
    ("user.us.created", b"user-created"),
]


def main():
    queues = list(BINDINGS)
    with broker.connect() as conn:
        broker.reset(conn, queues, [EXCHANGE])
        try:
            channel = conn.channel()
            channel.exchange_declare(EXCHANGE, exchange_type="topic", durable=True)
            for queue, keys in BINDINGS.items():
                channel.queue_declare(queue, durable=True)
                for key in keys:
                    channel.queue_bind(queue, EXCHANGE, routing_key=key)
            confirms = Confirmer(channel)

            for key, body in PUBLISH:
                tag = confirms.publish(channel, EXCHANGE, key, body)
                print(f"published {key} {body.decode()} confirm {confirms.wait(tag)}")

            received = {queue: [] for queue in queues}
            for queue in queues:
                for _properties, body in broker.drain(channel, queue):
                    received[queue].append(body)
                print(f"{queue} {received[queue]}")

            expected = {
                "patterns.events.billing": [b"us-paid", b"eu-paid"],
                "patterns.events.fraud": [b"eu-paid", b"eu-cancelled"],
                "patterns.events.audit": [b"us-paid", b"eu-paid", b"eu-cancelled"],
            }
            if received != expected:
                raise SystemExit(f"unexpected routing: {received}")
            print("user.us.created matched no queue; its confirm ack does not mean it was stored")
        finally:
            broker.reset(conn, queues, [EXCHANGE])


if __name__ == "__main__":
    main()
