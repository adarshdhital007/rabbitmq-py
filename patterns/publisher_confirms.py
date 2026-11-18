#!/usr/bin/env python
"""Publisher confirms are the production publish path.

An ack means a quorum queue has replicated the message. It does not mean a
consumer processed it. An unroutable publish is still acked: the broker is
confirming that routing finished, including the decision to drop the message.

    python patterns/publisher_confirms.py
"""

import queue

import pika

import broker
from confirms import Confirmer

QUEUE = "patterns.confirms"
EXCHANGE = "patterns.confirms.nowhere"


def main():
    with broker.connect() as conn:
        broker.reset(conn, [QUEUE], [EXCHANGE])
        try:
            channel = conn.channel()
            channel.queue_declare(
                QUEUE,
                durable=True,
                arguments={"x-queue-type": "quorum"},
            )
            channel.exchange_declare(EXCHANGE, exchange_type="direct", durable=True)
            confirms = Confirmer(channel)

            print("quorum queue accepts three persistent messages")
            for text in (b"one", b"two", b"three"):
                tag = confirms.publish(
                    channel, "", QUEUE, text, properties=broker.persistent()
                )
                print(f"  tag {tag} {confirms.wait(tag)} {text.decode()}")

            returned = queue.Queue()

            def on_return(_channel, method, _properties, body):
                returned.put((method.reply_code, method.reply_text, body))

            channel.add_on_return_callback(on_return)
            tag = confirms.publish(
                channel,
                EXCHANGE,
                "missing",
                b"dropped",
                properties=broker.persistent(),
                mandatory=True,
            )
            status = confirms.wait(tag)
            reply_code, reply_text, body = returned.get(timeout=5)
            print("unroutable mandatory publish")
            print(f"  confirm {status}")
            print(f"  basic.return {reply_code} {reply_text} body={body.decode()}")
            if status != "ack":
                raise SystemExit("a completed unroutable publish is still confirmed")
            if reply_text != "NO_ROUTE":
                raise SystemExit(f"expected NO_ROUTE, got {reply_text}")
        finally:
            broker.reset(conn, [QUEUE], [EXCHANGE])


if __name__ == "__main__":
    main()
