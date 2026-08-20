from dura import DurableEngine

engine = DurableEngine("engine.db")
ref = engine.spawn_task(
    name="charge_order",
    params={"order_id": "ord_42", "customer_id": "cust_9", "amount_cents": 4200},
    idempotency_key="charge-2026-01",
)
task = engine.get_task(ref.task_id)
print(task.state)
print(task.result)
