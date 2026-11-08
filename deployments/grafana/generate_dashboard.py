"""Generates deployments/grafana/dashboards/taskforge.json.

Run from the repository root: python deployments/grafana/generate_dashboard.py
Every panel title is a question an operator asks during an incident.
"""
import json
import os

DS = {"type": "prometheus", "uid": "prometheus"}
panels = []
state = {"y": 0, "id": 1}


def row(title):
    panels.append({"type": "row", "title": title, "collapsed": False, "id": state["id"],
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": state["y"]}, "panels": []})
    state["id"] += 1
    state["y"] += 1


def panel(kind, title, targets, x, w, h=7, unit=None, thresholds=None, description=None):
    item = {
        "type": kind, "title": title, "id": state["id"], "datasource": DS,
        "gridPos": {"h": h, "w": w, "x": x, "y": state["y"]},
        "targets": [{"datasource": DS, "expr": expr, "legendFormat": legend, "refId": chr(65 + i)}
                    for i, (expr, legend) in enumerate(targets)],
        "fieldConfig": {"defaults": {}, "overrides": []},
        "options": {},
    }
    if description:
        item["description"] = description
    if unit:
        item["fieldConfig"]["defaults"]["unit"] = unit
    if thresholds:
        item["fieldConfig"]["defaults"]["thresholds"] = {"mode": "absolute", "steps": thresholds}
    if kind == "timeseries" and thresholds:
        item["fieldConfig"]["defaults"]["custom"] = {"thresholdsStyle": {"mode": "line"}}
    panels.append(item)
    state["id"] += 1


def next_line(h=7):
    state["y"] += h


GREEN_RED = lambda limit: [{"color": "green", "value": None}, {"color": "red", "value": limit}]
RED_GREEN = lambda limit: [{"color": "red", "value": None}, {"color": "green", "value": limit}]

row("Are we meeting our SLOs?")
panel("stat", "Start latency: share of jobs started within 5 s (target 99 %)",
      [('1 - (sum(increase(taskforge_job_start_delay_seconds_count[$__range])) - sum(increase(taskforge_job_start_delay_seconds_bucket{le="5"}[$__range]))) / sum(increase(taskforge_job_start_delay_seconds_count[$__range]))', "SLI")],
      0, 6, unit="percentunit", thresholds=RED_GREEN(0.99))
panel("stat", "Reliability: share of jobs not lost to platform failures (target 99.5 %)",
      [('1 - (sum(increase(taskforge_job_outcomes_total{outcome="dead_lettered",class="platform"}[$__range])) or vector(0)) / sum(increase(taskforge_job_outcomes_total{outcome=~"succeeded|dead_lettered"}[$__range]))', "SLI")],
      6, 6, unit="percentunit", thresholds=RED_GREEN(0.995))
panel("timeseries", "How fast are we burning the error budget? (1 = exactly on budget)",
      [("taskforge:start_latency_bad:ratio_rate1h / 0.01", "start latency 1h"),
       ("taskforge:reliability_bad:ratio_rate1h / 0.005", "reliability 1h")],
      12, 6, thresholds=GREEN_RED(14.4))
panel("table", "Which alerts are firing?",
      [('ALERTS{alertstate="firing"}', "{{alertname}}")], 18, 6)
next_line()

row("Is work piling up?")
panel("timeseries", "How many jobs are in each state?",
      [("max by (state) (taskforge_jobs)", "{{state}}")], 0, 8)
panel("timeseries", "How long has the oldest ready job waited? (alert at 60 s)",
      [("max(taskforge_oldest_ready_job_age_seconds)", "oldest ready job")], 8, 8, unit="s", thresholds=GREEN_RED(60))
panel("timeseries", "Are dead letters, outbox lag, or workflows accumulating?",
      [("max(taskforge_dead_letters_unredriven)", "unredriven dead letters"),
       ("max(taskforge_outbox_oldest_unpublished_age_seconds)", "outbox lag (s)"),
       ("max by (status) (taskforge_workflows)", "workflows {{status}}")], 16, 8)
next_line()

row("Are jobs starting and finishing on time?")
panel("timeseries", "Are jobs starting on time? (start delay from readiness, 5 s line)",
      [('histogram_quantile(0.5, sum by (le) (rate(taskforge_job_start_delay_seconds_bucket[5m])))', "p50"),
       ('histogram_quantile(0.95, sum by (le) (rate(taskforge_job_start_delay_seconds_bucket[5m])))', "p95"),
       ('histogram_quantile(0.99, sum by (le) (rate(taskforge_job_start_delay_seconds_bucket[5m])))', "p99")],
      0, 12, unit="s", thresholds=GREEN_RED(5))
panel("timeseries", "How long do attempts take, by kind? (p95)",
      [('histogram_quantile(0.95, sum by (le, kind) (rate(taskforge_job_attempt_duration_seconds_bucket[5m])))', "{{kind}}")],
      12, 12, unit="s")
next_line()

row("What is failing?")
panel("timeseries", "Which outcomes are we seeing, and whose fault are failures?",
      [("sum by (outcome, class) (rate(taskforge_job_outcomes_total[5m]))", "{{outcome}} ({{class}})")], 0, 8, unit="ops")
panel("timeseries", "Are workers crashing? (lease recoveries)",
      [("sum by (result) (rate(taskforge_lease_recoveries_total[5m]))", "{{result}}")], 8, 8, unit="ops")
panel("timeseries", "Is telemetry itself healthy? (trace export failures, scrape targets)",
      [("sum(rate(taskforge_otel_export_failures_total[5m]))", "export failures/s"),
       ('sum by (job) (up{job=~"taskforge-.*"})', "up {{job}}")], 16, 8)
next_line()

row("Is the API healthy?")
panel("timeseries", "How many requests per route?",
      [("sum by (route) (rate(taskforge_http_requests_total[5m]))", "{{route}}")], 0, 8, unit="reqps")
panel("timeseries", "What share of requests fail with 5xx?",
      [('sum(rate(taskforge_http_requests_total{code=~"5.."}[5m])) / sum(rate(taskforge_http_requests_total[5m]))', "5xx ratio")],
      8, 8, unit="percentunit")
panel("timeseries", "How slow is each route? (p95)",
      [('histogram_quantile(0.95, sum by (le, route) (rate(taskforge_http_request_duration_seconds_bucket[5m])))', "{{route}}")],
      16, 8, unit="s")
next_line()

dashboard = {
    "uid": "taskforge-overview",
    "title": "TaskForge — what is broken?",
    "tags": ["taskforge", "slo"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "10s",
    "time": {"from": "now-1h", "to": "now"},
    "editable": False,
    "panels": panels,
}
out = os.path.join("deployments", "grafana", "dashboards", "taskforge.json")
os.makedirs(os.path.dirname(out), exist_ok=True)
with open(out, "w", encoding="utf-8", newline="\n") as handle:
    json.dump(dashboard, handle, indent=2, ensure_ascii=False)
    handle.write("\n")
print(out)
