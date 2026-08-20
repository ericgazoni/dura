import time
from dataclasses import dataclass

from dura import DurableEngine, run_workers


@dataclass
class Order:
    id: str
    customer_id: str
    amount_cents: int


def fetch_pending_orders():
    # Stand-in for wherever your orders actually come from.
    for n in range(1, 21):
        yield Order(id=f"ord_{n}", customer_id=f"cust_{n}", amount_cents=1000 + n * 100)


def charge_card(*, customer_id, amount_cents, idempotency_key):
    # Stand-in for a real payment gateway call.
    time.sleep(1)
    print(f"charged {customer_id} {amount_cents}c ({idempotency_key})")


def send_receipt(customer_id, order_id):
    # Stand-in for a real email/notification call.
    time.sleep(1)
    print(f"receipt sent to {customer_id} for {order_id}")


def charge_order(engine, task):
    engine.checkpoint(
        task_id=task.task_id,
        step_name="charge",
        fn=lambda: charge_card(
            customer_id=task.params["customer_id"],
            amount_cents=task.params["amount_cents"],
            idempotency_key=f"charge:{task.task_id}",
        ),
    )
    engine.checkpoint(
        task_id=task.task_id,
        step_name="receipt",
        fn=lambda: send_receipt(task.params["customer_id"], task.params["order_id"]),
    )
    return {"charged": task.params["order_id"]}


engine = DurableEngine("quickstart.db")

for order in fetch_pending_orders():
    engine.spawn_task(
        name="charge_order",
        params={
            "order_id": order.id,
            "customer_id": order.customer_id,
            "amount_cents": order.amount_cents,
        },
        idempotency_key=f"charge_order:{order.id}",
    )

run_workers(engine, handlers={"charge_order": charge_order}, worker_count=2)
