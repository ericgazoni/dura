---
icon: lucide/newspaper
---

# Polling an API, fanning out, and rescheduling

The how-to guides each cover one piece of `dura` in isolation. This page
walks through a single, more realistic script that combines several of
them: it polls a public API on a schedule, fans out per-item work with
its own retries, and waits for that work to finish before summarizing it.

The scenario: keep an eye on [Hacker News](https://news.ycombinator.com)'s
front page, fetch and process any story we haven't seen yet, and print a
digest once a batch is done. The whole script is at
[`examples/poll_hacker_news.py`](https://github.com/ericgazoni/dura/blob/main/examples/poll_hacker_news.py)
and runs as-is, no API key required.

## The setup

```python
--8<-- "examples/poll_hacker_news.py:setup"
```

`fetch_json` is the only thing in this script that isn't `dura`: a small
wrapper around the standard library's `urllib.request`, since the point
of this example is what happens around a network call, not how to make
one.

## Polling and fanning out

```python
--8<-- "examples/poll_hacker_news.py:poll"
```

`poll_top_stories` does three things, and the order matters:

1. It figures out which of the current top stories it hasn't already
   processed, using a `hn:seen` durable state entry as a dedup set. See
   [how to use durable state](../how-to/use-durable-state.md).
2. It spawns one `fetch_story` task per new story, each with its own
   `idempotency_key` and retry strategy, since a flaky network call
   should only cost that one story a retry, not the whole poll. See [how
   to chain and fan out tasks](../how-to/compose-tasks.md) and [how to
   configure retries](../how-to/configure-retries.md). The countdown used
   for fan-in is written before any child is spawned, so a `fetch_story`
   that finishes unusually fast never decrements a counter that isn't
   there yet.
3. Before returning, it spawns its own next run, `POLL_INTERVAL` later,
   with a fresh `idempotency_key` derived from a strictly-incrementing
   `run_number`. That's what makes it recurring. See [how to schedule
   recurring tasks](../how-to/schedule-recurring-tasks.md).

## Fetching and processing each story

```python
--8<-- "examples/poll_hacker_news.py:fetch"
```

Fetching and processing are two separate checkpoints, not one, so a crash
between them doesn't repeat the network call just to redo a bit of local
computation. See [how to checkpoint steps](../how-to/checkpoint-steps.md).

Once a story is processed, `fetch_story` writes its digest to durable
state (so `summarize_batch` can read it back later, from a different
task) and marks the story `seen` (so no later poll fans it out again).
Then it decrements the batch's countdown, and if this was the last story
in the batch, emits an event. Nothing is polling for that countdown to
hit zero: whichever `fetch_story` happens to be the last one just says so.

## Waiting for the batch to finish

```python
--8<-- "examples/poll_hacker_news.py:summarize"
```

`summarize_batch` is spawned once per poll, right alongside the
`fetch_story` tasks, and immediately parks itself on that same event,
however many stories are in the batch or however long they take. See
[how to wait for events](../how-to/wait-for-events.md). Once the event
fires, it reads back every digest written to durable state and prints
them.

## Wiring it together

```python
--8<-- "examples/poll_hacker_news.py:wiring"
```

One `run_workers` pool with a handful of threads runs all three task
types: the poll, the fetches, and the summary. `dura` doesn't need to
know they're related to each other, that relationship lives entirely in
the `batch_id` each one carries in its `params`. See [how to run a worker
pool](../how-to/run-worker-pool.md).

## Run it

```bash
python poll_hacker_news.py
```

```
poll #1: fanned out 5 new stories
batch f6fd05ac done, 5 stories:
  [ 127] A faster way to calculate the day of the week (www.benjoffe.com)
  [ 819] OpenRouter is joining Stripe (openrouter.ai)
  [ 609] Go 1.27 (go.dev)
  [ 179] Turns are Better than Radians (2022) (www.computerenhance.com)
  [ 100] Windows brings out the Rorschach test in everyone (devblogs.microsoft.com)
```

Leave it running and it polls again every `POLL_INTERVAL`, picking up a
fresh batch of stories each time, since the ones already seen are
excluded up front. Kill it, `Ctrl+C` or `kill -9`, at any point and run
it again: whatever had already completed, a fetch, a checkpoint, a
countdown, an emitted event, stays exactly as it was, and only the work
that hadn't finished yet gets picked back up.
