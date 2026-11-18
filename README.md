# rpy

localhost:5672, guest/guest. RabbitMQ 4, because of quorum queues.

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

UI: http://localhost:15672

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

From the repo root. Ctrl+C to stop a consumer.

## work queues

Worker first. Each `.` is a second of sleep.

```bash
python work_queues/worker.py
python work_queues/new_task.py
python work_queues/new_task.py "heavy job ....."
```

## publish / subscribe

Nothing is queued. If the receiver isn't up, the log is gone.

```bash
python publish_subscribe/receive_logs.py
python publish_subscribe/emit_log.py "a log line"
```

## routing

Receiver takes the severities to bind. Second terminal with just `error` if I only want those.

```bash
python routing/receive_logs_direct.py info warning
python routing/emit_log_direct.py info "hello"
python routing/emit_log_direct.py error "disk full"
```

## patterns

One at a time. Each one deletes its own queues on the way out.

```bash
python patterns/publisher_confirms.py
python patterns/alternate_exchange.py
python patterns/retry_with_ttl.py
python patterns/poison_delivery_limit.py
python patterns/queue_limits.py
python patterns/direct_reply_to.py
python patterns/single_active_consumer.py
python patterns/idempotent_consumer.py
python patterns/topic_event_bus.py
```

publisher_confirms — unroutable mandatory publish is still an ack, plus `basic.return` `NO_ROUTE`.

alternate_exchange — same miss, caught with an alternate exchange. Key is `creted` on purpose.

retry_with_ttl — nack, requeue false, 400ms TTL, back to the work queue, park after two. `x-delivery-limit` is -1 so the quorum default doesn't steal it first.

poison_delivery_limit — reject, requeue true. That one counts. Limit is 2, then dead-letter. nack requeue true would not count.

queue_limits — max-length 2. Classic reject-publish nacks the 3rd. Quorum reject-publish lets one extra in, then nacks. drop-head acks everything and drops the old ones.

direct_reply_to — `amq.rabbitmq.reply-to`. ping comes back. drop is acked with no reply, so the client times out.

single_active_consumer — cancel the active one and the other gets the next message.

idempotent_consumer — same message-id twice. Apply once, ack the copy anyway.

topic_event_bus — billing `order.*.paid`, fraud `order.eu.*`, audit `order.#`. `order.eu.paid` hits all three. `user.us.created` hits none, confirm is still an ack.
