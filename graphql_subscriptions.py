"""GraphQL-style pub/sub subscription engine with per-listener field filtering.

Real, working implementation for the Retsumdk ecosystem. Listeners subscribe to
a topic, publish() fans events out to matching listeners, and a per-listener
filter can drop events that do not match the requested selection.
"""
from __future__ import annotations

import itertools
from typing import Callable

ListenerId = str


class SubscriptionEngine:
    def __init__(self):
        self._listeners: dict[ListenerId, dict] = {}
        self._counter = itertools.count(1)
        self._topic_index: dict[str, set[ListenerId]] = {}

    def subscribe(self, topic: str, filter_fn: Callable[[dict], bool] | None = None) -> ListenerId:
        lid = f"sub-{next(self._counter)}"
        self._listeners[lid] = {"topic": topic, "filter": filter_fn, "events": []}
        self._topic_index.setdefault(topic, set()).add(lid)
        return lid

    def unsubscribe(self, lid: ListenerId) -> bool:
        if lid not in self._listeners:
            return False
        topic = self._listeners[lid]["topic"]
        self._topic_index[topic].discard(lid)
        del self._listeners[lid]
        return True

    def publish(self, topic: str, event: dict) -> int:
        delivered = 0
        for lid in list(self._topic_index.get(topic, ())):
            listener = self._listeners[lid]
            fn = listener["filter"]
            if fn is not None and not fn(event):
                continue
            listener["events"].append(event)
            delivered += 1
        return delivered

    def events_for(self, lid: ListenerId) -> list[dict]:
        return list(self._listeners[lid]["events"])
