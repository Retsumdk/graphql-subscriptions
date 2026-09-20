# graphql-subscriptions

> GraphQL-style pub/sub subscription engine with per-listener field filtering and topic delivery.

### What it is

GraphQL-style pub/sub subscription engine with per-listener field filtering.

Real, working Python for the Retsumdk ecosystem with an executable test suite.

## Getting started

```bash
pip install -r requirements.txt
pytest -q
```

## Why it exists

A subscription is only useful if it can say "no". Most one-file pub/sub samples fan
every event out to every listener and leave the filtering to the caller -- which means
every listener carries every event in memory and re-implements the same predicate.
This engine puts the predicate *inside* the subscription: `subscribe()` takes a
`filter_fn`, and an event that fails the filter is never appended to that listener's
queue. Filtering at the boundary keeps listener buffers small and makes the delivery
count returned by `publish()` an honest measure of what was actually stored.

## Features

- **Topic-scoped fan-out** -- `publish(topic, event)` delivers to exactly the listeners
  registered for that topic, not to the whole engine.
- **Per-listener field filtering** -- a `filter_fn` decides, event by event, whether a
  listener receives it. Non-matching events are dropped before they are stored.
- **Honest delivery counts** -- `publish()` returns the number of listeners that
  *accepted* the event, so a filter that rejects everything returns `0`.
- **Stable listener ids** -- `subscribe()` returns a monotonically increasing
  `sub-N` id, so unsubscribe never has to rely on the callable's identity.
- **Idempotent unsubscribe** -- removing a listener twice returns `True` then `False`
  instead of raising.
- **Buffered reads** -- `events_for()` returns a copy of a listener's queue, so callers
  cannot mutate engine state by holding the list.
- **Zero dependencies** -- standard library only (`itertools`, `typing`).

## Architecture

```
publish(topic, event)
   │
   ▼
SubscriptionEngine
   ├── _listeners     listener id -> {topic, filter, events}
   ├── _topic_index   topic -> set(listener ids)      ← fan-out lookup
   └── _counter       itertools.count(1)              ← stable sub-N ids
        │
        ├── for each listener on the topic
        │      filter is None           → accept
        │      filter(event) is True    → accept
        │      filter(event) is False   → drop, not stored
        └── return the number accepted
```

The two structures are kept in sync by `subscribe()` / `unsubscribe()`: `_topic_index`
answers "who wants this topic" in O(1), and `_listeners` holds the per-listener queue
and predicate. A listener removed from one is always removed from the other.

## Usage

Subscribe twice to the same topic -- once with a filter, once without -- and publish
one event that fails the filter and one that passes:

```python
from graphql_subscriptions import SubscriptionEngine

eng = SubscriptionEngine()

live = eng.subscribe("orders", filter_fn=lambda e: e.get("status") == "paid")
audit = eng.subscribe("orders")

print(eng.publish("orders", {"id": 1, "status": "pending"}))   # 1
print(eng.publish("orders", {"id": 2, "status": "paid"}))      # 2

print(eng.events_for(live))    # [{'id': 2, 'status': 'paid'}]
print(eng.events_for(audit))   # [{'id': 1, 'status': 'pending'},
                               #  {'id': 2, 'status': 'paid'}]
```

The first publish is delivered to `audit` only (count `1`); the second is accepted by
both (count `2`). The filtered listener never sees the pending order.

Topics with no subscribers are harmless:

```python
print(eng.publish("ops", {"id": 3}))   # 0
```

Unsubscribing is idempotent and takes effect immediately:

```python
lid = eng.subscribe("ops")
print(eng.unsubscribe(lid))   # True  -> removed
print(eng.unsubscribe(lid))   # False -> already gone
```

Every snippet above was executed against the current commit and the comments are the
exact program output.

## API reference

| Member | Signature | Returns | Notes |
| --- | --- | --- | --- |
| `SubscriptionEngine.subscribe` | `subscribe(topic: str, filter_fn: Callable[[dict], bool] \| None = None) -> str` | new listener id (`sub-1`, `sub-2`, ...) | An unset filter accepts every event. |
| `SubscriptionEngine.publish` | `publish(topic: str, event: dict) -> int` | number of listeners that accepted | Returns `0` for a topic with no subscribers. |
| `SubscriptionEngine.unsubscribe` | `unsubscribe(lid: str) -> bool` | `True` if removed, `False` if unknown | Safe to call twice. |
| `SubscriptionEngine.events_for` | `events_for(lid: str) -> list[dict]` | copy of that listener's queue | Raises `KeyError` for an unknown id. |

## Delivery semantics and caveats

- **Filters run at publish time, not read time.** A rejected event is never stored, so it
  cannot be recovered later by a broadened filter -- re-subscribe and republish instead.
- **Events are shared by reference, not copied.** Two listeners on the same topic receive
  the *same* dict object; mutating it in one place affects the other's history.
- **Synchronous dispatch.** `publish()` calls every filter inline and returns once the
  fan-out is complete. A slow filter slows the publisher.
- **No wildcards and no persistence.** Topics are matched by exact string, and the queue
  lives in memory only -- a restart drops the history.
- **`events_for()` raises on unknown ids.** It deliberately does not return `[]`, so a
  typo in a listener id surfaces immediately rather than looking like silence.

## Real-world use case

An agent gateway can expose one topic per domain -- `orders`, `payments`, `alerts` --
and let each consumer subscribe to only the slice it needs: a settlement worker
subscribes to `payments` filtered on `status == "settled"`, while an audit sink
subscribes to the same topic with no filter and keeps the complete record. The
settlement worker's queue stays proportional to the work it must do, and no consumer
needs to know which other consumers exist.

## Testing

```bash
pip install -r requirements.txt
pytest -q
# 3 passed
```

| Test | What it pins down |
| --- | --- |
| `test_fans_out_to_subscribers` | One publish reaches every listener on the topic and returns the accepted count. |
| `test_filter_drops_non_matching` | A listener's `filter_fn` removes non-matching events from its queue. |
| `test_unsubscribe_stops_delivery` | An unsubscribed listener receives nothing. |

## License

[MIT](LICENSE) © Retsumdk
