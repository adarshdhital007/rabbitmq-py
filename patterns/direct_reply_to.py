#!/usr/bin/env python
"""Request and reply without declaring a response queue.

The client consumes amq.rabbitmq.reply-to on the same channel it publishes
on, in auto-ack mode. The broker rewrites reply-to before the server sees
it. Replies are not durable: if this client disconnects, the reply is gone,
so the client owns the timeout.

    python patterns/direct_reply_to.py
"""

import threading
import uuid

import pika

import broker

REQUESTS = "patterns.rpc.requests"
REPLY_TO = "amq.rabbitmq.reply-to"


def main():
    with broker.connect() as server_conn, broker.connect() as client_conn:
        broker.reset(server_conn, [REQUESTS], [])
        try:
            server = server_conn.channel()
            client = client_conn.channel()
            server.queue_declare(
                REQUESTS, durable=True, arguments={"x-queue-type": "quorum"}
            )

            waiters = {}

            def on_reply(_channel, _method, properties, body):
                event, box = waiters[properties.correlation_id]
                box.append(body)
                event.set()

            def on_request(channel, method, properties, body):
                if body != b"drop":
                    channel.basic_publish(
                        "",
                        properties.reply_to,
                        f"pong:{body.decode()}".encode(),
                        properties=pika.BasicProperties(
                            correlation_id=properties.correlation_id
                        ),
                    )
                channel.basic_ack(method.delivery_tag)

            client.basic_consume(REPLY_TO, on_reply, auto_ack=True)
            server.basic_qos(prefetch_count=1)
            server.basic_consume(REQUESTS, on_request)

            def request(body, timeout):
                correlation_id = str(uuid.uuid4())
                event = threading.Event()
                waiters[correlation_id] = (event, [])
                client.basic_publish(
                    "",
                    REQUESTS,
                    body,
                    properties=pika.BasicProperties(
                        reply_to=REPLY_TO,
                        correlation_id=correlation_id,
                    ),
                )
                if not event.wait(timeout):
                    return None
                return waiters[correlation_id][1][0]

            reply = request(b"ping", timeout=3)
            print(f"ping -> {None if reply is None else reply.decode()}")
            if reply != b"pong:ping":
                raise SystemExit(f"expected pong:ping, got {reply}")

            missing = request(b"drop", timeout=1)
            print(f"drop -> {missing}")
            if missing is not None:
                raise SystemExit("a request the server acked without a reply must time out")
        finally:
            broker.reset(server_conn, [REQUESTS], [])


if __name__ == "__main__":
    main()
