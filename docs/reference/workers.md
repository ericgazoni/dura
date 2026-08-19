---
icon: lucide/server-cog
---

# `dura.workers`

Defined in `dura/workers.py`. `process_task`, `run_worker`, and
`run_workers` are re-exported from the top-level `dura` package.

::: dura.workers.run_workers

::: dura.workers.run_worker

::: dura.workers.process_task

## Constants

`SHUTDOWN_GRACE_SECONDS` (`float`, default `10.0`): how long `run_workers`
waits for in-flight handlers to finish on shutdown before abandoning them.
