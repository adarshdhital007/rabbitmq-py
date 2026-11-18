#!/usr/bin/env python
"""Unroutable messages need an explicit destination.

mandatory=True tells this publisher only. An alternate exchange keeps the
message for every publisher, including ones that forget the flag. Bind the
alternate exchange to a catch-all queue and alert on it.

    python patterns/alternate_exchange.py
"""

import broker
from confirms import Confirmer

EXCHANGE = "patterns.orders"
CATCHALL_EXCHANGE = "patterns.orders.unroutable"
QUEUE = "patterns.orders.created"
CATCHALL = "patterns.orders.catchall"


def main():
    with broker.connect() as conn:
        broker.reset(conn, [QUEUE, CATCHALL], [EXCHANGE, CATCHALL_EXCHANGE])
        try:
            channel = conn.channel()
            channel.exchange_declare(CATCHALL_EXCHANGE, exchange_type="fanout", durable=True)
            channel.queue_declare(CATCHALL, durable=True)
            channel.queue_bind(CATCHALL, CATCHALL_EXCHANGE)
            channel.exchange_declare(
                EXCHANGE,
                exchange_type="direct",
                durable=True,
                arguments={"alternate-exchange": CATCHALL_EXCHANGE},
            )
            channel.queue_declare(QUEUE, durable=True)
            channel.queue_bind(QUEUE, EXCHANGE, routing_key="created")
            confirms = Confirmer(channel)

            tag = confirms.publish(channel, EXCHANGE, "created", b"order-1")
            print(f"routed publish confirm {confirms.wait(tag)}")
            method, _properties, body = broker.get_message(channel, QUEUE, auto_ack=True)
            if body != b"order-1":
                raise SystemExit(f"expected the created queue to receive order-1, got {body}")
            print(f"  {method.exchange} key={method.routing_key} body={body.decode()}")

            tag = confirms.publish(channel, EXCHANGE, "creted", b"order-2")
            print(f"misspelled key confirm {confirms.wait(tag)}")
            method, _properties, body = broker.get_message(channel, CATCHALL, auto_ack=True)
            if body != b"order-2":
                raise SystemExit("the alternate exchange did not keep the unroutable message")
            print(
                "  alternate exchange kept it: "
                f"exchange={method.exchange} key={method.routing_key} body={body.decode()}"
            )
            if broker.get_message(channel, QUEUE, timeout=0.3)[0] is not None:
                raise SystemExit("the misspelled key was also routed to the created queue")
        finally:
            broker.reset(conn, [QUEUE, CATCHALL], [EXCHANGE, CATCHALL_EXCHANGE])


if __name__ == "__main__":
    main()
