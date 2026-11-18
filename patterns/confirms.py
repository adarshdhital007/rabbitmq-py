"""Publisher-confirm bookkeeping for the current Pika connection."""

import queue
import threading
import time

import pika


class Confirmer:
    def __init__(self, channel):
        self._status = {}
        self._cond = threading.Condition()
        channel.confirm_delivery(self._on_confirm)

    def _on_confirm(self, method_frame):
        method = getattr(method_frame, "method", method_frame)
        kind = "ack" if isinstance(method, pika.spec.Basic.Ack) else "nack"
        with self._cond:
            if method.multiple:
                for tag in range(1, method.delivery_tag + 1):
                    self._status.setdefault(tag, kind)
            else:
                self._status[method.delivery_tag] = kind
            self._cond.notify_all()

    def publish(self, channel, exchange, routing_key, body, properties=None, mandatory=False):
        tags = queue.Queue()
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            properties=properties,
            mandatory=mandatory,
            on_publish=tags.put,
        )
        return tags.get(timeout=5)

    def wait(self, tag, timeout=5):
        deadline = time.monotonic() + timeout
        with self._cond:
            while tag not in self._status:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"no publisher confirm for delivery tag {tag}")
                self._cond.wait(remaining)
            return self._status[tag]
