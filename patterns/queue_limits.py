#!/usr/bin/env python
"""A full queue must push back on publishers.

On a classic queue, x-overflow=reject-publish nacks the first message that
does not fit. A quorum queue is different: the leader tells publishing
channels to stop only after the limit is reached, so the queue overshoots
by at least one message. Later publishes on that channel are nacked.

drop-head never nacks. It confirms the publish and deletes the oldest
message. Use it only when losing old data is acceptable.

    python patterns/queue_limits.py
"""

import broker
from confirms import Confirmer

CLASSIC = "patterns.limits.classic"
REJECT = "patterns.limits.quorum-reject"
DROP = "patterns.limits.quorum-drop"


def publish_all(channel, confirms, queue, count):
    outcomes = []
    for number in range(count):
        body = str(number).encode()
        tag = confirms.publish(channel, "", queue, body, properties=broker.persistent())
        outcomes.append((body, confirms.wait(tag)))
    return outcomes


def bodies(channel, queue):
    return [body for _properties, body in broker.drain(channel, queue)]


def show(title, outcomes):
    print(title)
    for body, status in outcomes:
        print(f"  {body.decode()} {status}")


def main():
    with broker.connect() as conn:
        broker.reset(conn, [CLASSIC, REJECT, DROP], [])
        try:
            channel = conn.channel()
            channel.queue_declare(
                CLASSIC,
                durable=True,
                arguments={"x-max-length": 2, "x-overflow": "reject-publish"},
            )
            channel.queue_declare(
                REJECT,
                durable=True,
                arguments={
                    "x-queue-type": "quorum",
                    "x-max-length": 2,
                    "x-overflow": "reject-publish",
                },
            )
            channel.queue_declare(
                DROP,
                durable=True,
                arguments={
                    "x-queue-type": "quorum",
                    "x-max-length": 2,
                    "x-overflow": "drop-head",
                },
            )
            confirms = Confirmer(channel)

            classic = publish_all(channel, confirms, CLASSIC, 3)
            quorum_reject = publish_all(channel, confirms, REJECT, 4)
            quorum_drop = publish_all(channel, confirms, DROP, 4)
            show("classic reject-publish, max-length 2", classic)
            show("quorum reject-publish, max-length 2", quorum_reject)
            show("quorum drop-head, max-length 2", quorum_drop)

            classic_kept = bodies(channel, CLASSIC)
            reject_kept = bodies(channel, REJECT)
            drop_kept = bodies(channel, DROP)
            print(f"classic kept {[body.decode() for body in classic_kept]}")
            print(f"quorum reject-publish kept {[body.decode() for body in reject_kept]}")
            print(f"quorum drop-head kept {[body.decode() for body in drop_kept]}")

            if classic != [(b"0", "ack"), (b"1", "ack"), (b"2", "nack")]:
                raise SystemExit(f"classic reject-publish was not exact: {classic}")
            if quorum_reject != [
                (b"0", "ack"),
                (b"1", "ack"),
                (b"2", "ack"),
                (b"3", "nack"),
            ]:
                raise SystemExit(f"quorum reject-publish did not overshoot: {quorum_reject}")
            if quorum_drop != [(b"0", "ack"), (b"1", "ack"), (b"2", "ack"), (b"3", "ack")]:
                raise SystemExit(f"drop-head nacked a publisher: {quorum_drop}")
            if classic_kept != [b"0", b"1"] or reject_kept != [b"0", b"1", b"2"]:
                raise SystemExit("reject-publish queues did not keep the accepted prefix")
            if drop_kept != [b"2", b"3"]:
                raise SystemExit("drop-head did not keep the newest messages")
        finally:
            broker.reset(conn, [CLASSIC, REJECT, DROP], [])


if __name__ == "__main__":
    main()
