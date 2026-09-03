from graphql_subscriptions import SubscriptionEngine


def test_fans_out_to_subscribers():
    eng = SubscriptionEngine()
    a = eng.subscribe("orders")
    b = eng.subscribe("orders")
    n = eng.publish("orders", {"id": 1})
    assert n == 2
    assert eng.events_for(a) == [{"id": 1}]
    assert eng.events_for(b) == [{"id": 1}]


def test_filter_drops_non_matching():
    eng = SubscriptionEngine()
    lid = eng.subscribe("orders", filter_fn=lambda e: e.get("status") == "paid")
    eng.publish("orders", {"id": 1, "status": "pending"})
    eng.publish("orders", {"id": 2, "status": "paid"})
    assert eng.events_for(lid) == [{"id": 2, "status": "paid"}]


def test_unsubscribe_stops_delivery():
    eng = SubscriptionEngine()
    lid = eng.subscribe("orders")
    eng.unsubscribe(lid)
    assert eng.publish("orders", {"id": 9}) == 0
