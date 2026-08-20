import threading
import time

from dura import DurableEngine, run_worker


def charge_card(customer_id, amount_cents):
    print(f"charged {customer_id} {amount_cents}c (this only happens once)")
    return {"charged": True}


def send_receipt(customer_id, order_id):
    print(f"receipt sent to {customer_id} for {order_id}")
    return True


def charge_order(engine, task):
    print(f"Attempt {task.attempt}")
    result = engine.checkpoint(
        task_id=task.task_id,
        step_name="charge",
        fn=lambda: charge_card(task.params["customer_id"], task.params["amount_cents"]),
    )
    time.sleep(8)  # pretend sending the receipt takes a while
    engine.checkpoint(
        task_id=task.task_id,
        step_name="receipt",
        fn=lambda: send_receipt(task.params["customer_id"], task.params["order_id"]),
    )
    print("Done")
    return result


engine = DurableEngine("engine.db")
engine.spawn_task(
    name="charge_order",
    params={"order_id": "ord_42", "customer_id": "cust_9", "amount_cents": 4200},
    idempotency_key="charge-2026-02",
)

run_worker(
    engine,
    handlers={"charge_order": charge_order},
    worker_id="tutorial-worker",
    stop_event=threading.Event(),
    claim_timeout_secs=5,
)
