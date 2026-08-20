from dura import DurableEngine, run_workers


def charge_card(customer_id, amount_cents):
    print(f"charged {customer_id} {amount_cents}c")
    return {"charged": True}


def charge_order(engine, task):
    charge_card(task.params["customer_id"], task.params["amount_cents"])
    return {"order_id": task.params["order_id"]}


engine = DurableEngine("engine.db")
engine.spawn_task(
    name="charge_order",
    params={"order_id": "ord_42", "customer_id": "cust_9", "amount_cents": 4200},
    idempotency_key="charge-2026-01",
)

run_workers(engine, handlers={"charge_order": charge_order}, worker_count=2)
