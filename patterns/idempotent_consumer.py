#!/usr/bin/env python
"""At-least-once delivery means the consumer will see duplicates.

Publisher confirms can be lost on the way back. The publisher then sends the
same message-id again. The broker stores both copies. The consumer has to
make the side effect happen once and still ack the duplicate, or the broker
will send it again.

    python patterns/idempotent_consumer.py
"""

import threading

import broker
from confirms import Confirmer

QUEUE = "patterns.idempotent.payments"


def main():
    with broker.connect() as conn:
        broker.reset(conn, [QUEUE], [])
        try:
            channel = conn.channel()
            channel.queue_declare(
                QUEUE, durable=True, arguments={"x-queue-type": "quorum"}
            )
            confirms = Confirmer(channel)
            properties = broker.persistent(message_id="pay-1001")
            for _ in range(2):
                tag = confirms.publish(
                    channel, "", QUEUE, b"charge-1001", properties=properties
                )
                print(f"publish message-id=pay-1001 confirm {confirms.wait(tag)}")

            applied = []
            duplicates = []
            finished = threading.Event()

            def on_message(ch, method, props, body):
                message_id = props.message_id
                if message_id in applied:
                    duplicates.append(message_id)
                    print(f"duplicate {message_id}, ack without charging again")
                else:
                    applied.append(message_id)
                    print(f"apply {message_id} body={body.decode()}")
                ch.basic_ack(method.delivery_tag)
                if len(applied) + len(duplicates) == 2:
                    finished.set()

            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(QUEUE, on_message)
            if not finished.wait(5):
                raise SystemExit("both copies were not consumed")
            if applied != ["pay-1001"] or duplicates != ["pay-1001"]:
                raise SystemExit(f"applied={applied} duplicates={duplicates}")
            print("side effect ran once")
        finally:
            broker.reset(conn, [QUEUE], [])


if __name__ == "__main__":
    main()
