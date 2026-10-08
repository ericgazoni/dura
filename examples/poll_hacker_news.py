"""Poll the Hacker News API, fan out per-story processing, and reschedule.

A single, more realistic example that ties together most of dura's pieces:

- ``poll_top_stories`` is a recurring task: it reschedules its own next run
  before returning (see "how to schedule recurring tasks").
- It fans out one ``fetch_story`` task per new story id (see "how to chain
  and fan out tasks"), each with its own retries, since a flaky network
  call shouldn't take the whole poll down with it.
- ``fetch_story`` checkpoints its two steps, fetching then processing, so a
  crash or retry doesn't redo either one (see "how to checkpoint steps").
- Durable state does two jobs here: a "seen" set dedupes stories across
  polls, and a per-batch countdown drives fan-in (see "how to use durable
  state").
- ``summarize_batch`` waits on an event that fires once every story in its
  batch is done, however long that takes, without polling (see "how to
  wait for events").

Run it with ``python poll_hacker_news.py``. It needs network access to
``hacker-news.firebaseio.com``, a free, unauthenticated API. Press Ctrl+C
to stop; run it again and it picks up exactly where it left off, including
mid-poll, without reprocessing a story it already handled.
"""

# --8<-- [start:setup]
import json
import urllib.request
from datetime import timedelta
from urllib.parse import urlparse

from dura import DurableEngine, RetryStrategy, run_workers

API = "https://hacker-news.firebaseio.com/v0"
STORIES_PER_POLL = 5
POLL_INTERVAL = timedelta(seconds=30)  # a real poller would use minutes, not seconds


def fetch_json(url):
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.load(response)


# --8<-- [end:setup]


# --8<-- [start:poll]
def poll_top_stories(engine, task):
    run_number = task.params.get("run_number", 0) + 1
    batch_id = task.task_id

    all_ids = fetch_json(f"{API}/topstories.json")
    unseen = [
        story_id
        for story_id in all_ids
        if engine.get_state(namespace="hn:seen", key=str(story_id), default=None)
        is None
    ]
    batch = unseen[:STORIES_PER_POLL]

    if batch:
        # Set the countdown before spawning any child, so a fetch_story that
        # finishes unusually fast never decrements a counter that isn't
        # written yet.
        engine.set_state(
            namespace=f"batch:{batch_id}", key="remaining", value=len(batch)
        )
        for story_id in batch:
            engine.spawn_task(
                name="fetch_story",
                params={"story_id": story_id, "batch_id": batch_id},
                idempotency_key=f"fetch_story:{story_id}",
                max_attempts=3,
                retry=RetryStrategy(kind="fixed", base_seconds=5),
            )
        engine.spawn_task(
            name="summarize_batch",
            params={"batch_id": batch_id},
            idempotency_key=f"summarize_batch:{batch_id}",
        )
        print(f"poll #{run_number}: fanned out {len(batch)} new stories")
    else:
        print(f"poll #{run_number}: nothing new")

    # Reschedule the next poll before returning. A fresh idempotency_key,
    # derived from a strictly-incrementing run_number, keeps this chain from
    # forking or stalling.
    engine.spawn_task(
        name="poll_top_stories",
        params={"run_number": run_number},
        available_after=POLL_INTERVAL,
        idempotency_key=f"poll_top_stories:{run_number}",
    )
    return {"fanned_out": len(batch)}


# --8<-- [end:poll]


# --8<-- [start:fetch]
def fetch_story(engine, task):
    story_id = task.params["story_id"]
    batch_id = task.params["batch_id"]

    item = engine.checkpoint(
        task_id=task.task_id,
        step_name="fetch",
        fn=lambda: fetch_json(f"{API}/item/{story_id}.json"),
    )

    digest = engine.checkpoint(
        task_id=task.task_id,
        step_name="process",
        fn=lambda: {
            "title": item.get("title", "(no title)"),
            "score": item.get("score", 0),
            "domain": urlparse(item.get("url", "")).netloc or "news.ycombinator.com",
        },
    )

    engine.set_state(
        namespace=f"batch:{batch_id}:digests", key=str(story_id), value=digest
    )
    engine.set_state(namespace="hn:seen", key=str(story_id), value=True)

    remaining = engine.update_state(
        namespace=f"batch:{batch_id}", key="remaining", fn=lambda n: n - 1
    )
    if remaining == 0:
        engine.emit_event(event_name=f"batch_done:{batch_id}")

    return digest


# --8<-- [end:fetch]


# --8<-- [start:summarize]
def summarize_batch(engine, task):
    batch_id = task.params["batch_id"]

    engine.wait_for_event(
        run_id=task.run_id,
        task_id=task.task_id,
        step_name="wait",
        event_name=f"batch_done:{batch_id}",
        timeout_secs=600,
    )

    digests_namespace = f"batch:{batch_id}:digests"
    digests = engine.list_state(digests_namespace)
    print(f"batch {batch_id[:8]} done, {len(digests)} stories:")
    for digest in digests.values():
        print(f"  [{digest['score']:>4}] {digest['title']} ({digest['domain']})")

    # The countdown and digests were only needed to get here; durable state
    # outlives tasks by design (see "how to use durable state"), so without
    # this they'd accumulate forever, one namespace per batch.
    for story_id in digests:
        engine.delete_state(namespace=digests_namespace, key=story_id)
    engine.delete_state(namespace=f"batch:{batch_id}", key="remaining")

    return {"summarized": len(digests)}


# --8<-- [end:summarize]


# --8<-- [start:wiring]
engine = DurableEngine("poll_hacker_news.db")
engine.spawn_task(
    name="poll_top_stories",
    params={"run_number": 0},
    idempotency_key="poll_top_stories:0",
)

run_workers(
    engine,
    handlers={
        "poll_top_stories": poll_top_stories,
        "fetch_story": fetch_story,
        "summarize_batch": summarize_batch,
    },
    worker_count=4,
)
# --8<-- [end:wiring]
