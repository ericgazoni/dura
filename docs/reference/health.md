---
icon: lucide/activity
---

# `dura.health`

Defined in `dura/health.py`. `Heartbeat` and
`start_metrics_and_health_server` are re-exported from the top-level `dura`
package.

::: dura.health.Heartbeat

::: dura.health.start_metrics_and_health_server

## HTTP endpoints

The server started by `start_metrics_and_health_server` serves:

| Path | Method | Response |
|---|---|---|
| `/healthz`, `/livez` | `GET` | `200 alive silence=<n>s` if `seconds_since_beat() < max_silence_seconds`, else `503 stuck silence=<n>s` |
| `/metrics`, `/` | `GET` | Prometheus text-format metrics from `prometheus_client`'s default `REGISTRY` |
| any other path | `GET` | `404 not found` |
