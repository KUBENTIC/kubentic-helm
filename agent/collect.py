"""
Kubentic collection agent — cloud-agnostic (AWS / GKE / AKS / on-prem).

Runs as a one-shot pod inside the user's cluster via a CronJob managed by
the kubentic-operator. Uses the in-cluster ServiceAccount token to:
  1. Collect current + previous pod logs from the K8s API
  2. Collect full restart history from VictoriaLogs (if configured)
  3. Detect ghost pods (deleted during window) via VictoriaLogs streams
  4. Collect metrics from VictoriaMetrics
  5. Enrich metrics with node info
  6. Zip everything and ship to the Kubentic backend

Log file naming:
  {log_type}_{ns}_{pod}_{container}.log            — stable pod (0 restarts)
  {log_type}_{ns}_{pod}_{container}_log{N}.log     — current run (N restarts)
  {log_type}_{ns}_{pod}_{container}_log{N-1}.log   — previous run (K8s API)
  {log_type}_{ns}_{pod}_{container}_log0..{N-2}.log — older runs (VictoriaLogs)
  {log_type}_{ns}_{pod}_{container}_terminated.log  — ghost/deleted pod

Environment variables (injected by the operator):
  KUBENTIC_ACCESS_TOKEN   — required, bearer token for the backend
  KUBENTIC_BACKEND_URL    — default https://pa.kubentic.ai:8443
  TLS_SKIP_VERIFY         — "true" to skip TLS verification (dev only)
  UPLOAD_MAX_RETRIES      — integer, default 3
  COLLECT_LOGS            — "true"/"false", default true
  LOG_SINCE_HOURS         — hours to look back, default 3
  LOG_CONCURRENCY         — parallel K8s API calls, default 10
  MAX_BYTES_PER_POD       — max log bytes per container, 0 = unlimited
  LOG_MAX_TAIL_LINES      — max lines per log file, default 30000
  EXCLUDE_NAMESPACES      — comma-separated namespaces to skip
  COLLECT_METRICS         — "true"/"false", default true
  VICTORIA_METRICS_URL    — VictoriaMetrics base URL
  VICTORIA_LOGS_URL       — VictoriaLogs base URL (optional)
  VL_HISTORY_SECONDS      — how far back to fetch VL history, default 604800 (7d)
  VL_MAX_LINES            — max log lines per container from VL, default 100000
  METRICS_RANGE_SECONDS   — metrics query window, default 10800 (3h)
  METRICS_STEP            — metrics resolution step, default "60s"
"""

import asyncio
import json
import logging
import os
import random
import ssl
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import aiofiles
import aiohttp
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# ─── Configuration ────────────────────────────────────────────────────────────

KUBENTIC_TOKEN    = os.environ["KUBENTIC_ACCESS_TOKEN"]
_BACKEND_BASE     = os.environ.get("KUBENTIC_BACKEND_URL", "https://pa.kubentic.ai:8443").rstrip("/")
ENDPOINT          = f"{_BACKEND_BASE}/pa/call"
TLS_SKIP_VERIFY   = os.environ.get("TLS_SKIP_VERIFY", "false").lower() == "true"
UPLOAD_MAX_RETRIES = int(os.environ.get("UPLOAD_MAX_RETRIES", "3"))

COLLECT_LOGS      = os.environ.get("COLLECT_LOGS", "true").lower() == "true"
LOG_SINCE_HOURS   = int(os.environ.get("LOG_SINCE_HOURS", "3"))
LOG_SINCE_SECONDS = LOG_SINCE_HOURS * 3600
LOG_CONCURRENCY   = int(os.environ.get("LOG_CONCURRENCY", "10"))
MAX_BYTES_PER_POD = int(os.environ.get("MAX_BYTES_PER_POD", "0"))
LOG_MAX_TAIL_LINES = int(os.environ.get("LOG_MAX_TAIL_LINES", "30000"))
EXCLUDE_NAMESPACES = set(
    ns.strip()
    for ns in os.environ.get("EXCLUDE_NAMESPACES", "").split(",")
    if ns.strip()
)

COLLECT_METRICS      = os.environ.get("COLLECT_METRICS", "true").lower() == "true"
VICTORIA_METRICS_URL = os.environ.get("VICTORIA_METRICS_URL", "").rstrip("/")
VICTORIA_LOGS_URL    = os.environ.get("VICTORIA_LOGS_URL", "").rstrip("/")
VL_HISTORY_SECONDS   = int(os.environ.get("VL_HISTORY_SECONDS", str(7 * 24 * 3600)))
VL_MAX_LINES         = int(os.environ.get("VL_MAX_LINES", "100000"))
METRICS_RANGE_SECONDS = int(os.environ.get("METRICS_RANGE_SECONDS", "10800"))
METRICS_STEP         = os.environ.get("METRICS_STEP", "60s")

MAX_LOG_WORKERS        = LOG_CONCURRENCY
MAX_METRIC_CONCURRENCY = 10
VL_CONCURRENCY         = 5

BASEDIR     = Path("/tmp/kubentic-collection")
LOGS_DIR    = BASEDIR / "kubectl_logs_3hr"
METRICS_DIR = BASEDIR / "metrics_3hr"

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("kubentic-agent")

# ─── Namespace & metric classification ───────────────────────────────────────

K8S_SYSTEM_NAMESPACES = frozenset({
    "kube-system", "kube-public", "kube-node-lease", "kube-flannel", "default",
    "monitoring", "log-collector", "prometheus", "grafana", "alertmanager",
    "loki", "tempo", "mimir", "thanos", "cortex", "jaeger", "zipkin",
    "opentelemetry", "elastic-system", "elasticsearch", "kibana",
    "fluentd", "fluent-bit", "logstash", "datadog", "newrelic",
    "dynatrace", "splunk", "sumologic",
    "victoria-metrics", "vm", "victoriametrics", "vmagent", "vmalert",
    "vmauth", "vminsert", "vmselect", "vmstorage",
    "kubentic-foresight",
    "istio-system", "linkerd", "consul", "kuma-system", "gloo-system",
    "ambassador", "traefik", "nginx-ingress", "ingress-nginx", "kong",
    "contour-internal", "projectcontour",
    "cert-manager", "certificate-manager", "vault", "vault-system",
    "rook-ceph", "ceph", "longhorn-system", "openebs", "portworx",
    "storageos", "minio", "minio-operator",
    "calico-system", "tigera-operator", "cilium", "weave", "flannel",
    "kube-router", "metallb-system", "multus", "whereabouts", "network-system",
    "falco", "gatekeeper-system", "opa", "kyverno", "policy-system",
    "neuvector", "aqua", "sysdig", "twistlock", "anchore", "trivy-system",
    "argocd", "argo", "argo-events", "argo-workflows", "argo-rollouts",
    "flux-system", "fluxcd", "tekton-pipelines", "tekton",
    "jenkins", "gitlab-runner", "spinnaker",
    "rancher-system", "cattle-system", "fleet-system", "karpenter",
    "cluster-autoscaler", "kubefed-system", "open-cluster-management",
    "velero", "kasten-io", "stash", "trilio-system",
    "operator-lifecycle-manager", "olm", "kubedb",
    "redis-operator", "postgres-operator", "mysql-operator",
    "mongodb-operator", "cassandra-operator", "kafka-operator",
    "elastic-operator", "prometheus-operator",
    "knative-serving", "knative-eventing", "openfaas", "openfaas-fn",
    "fission", "kubeless",
    "kubeflow", "mlflow", "seldon-system", "kserve",
    "kubecost", "opencost",
    "keda", "crossplane-system", "external-secrets", "sealed-secrets",
    "reloader", "kubernetes-dashboard", "kube-state-metrics",
    "metrics-server", "node-problem-detector",
})

K8S_SYSTEM_METRIC_PREFIXES = frozenset({
    "kube_", "kubelet_", "apiserver_", "etcd_", "scheduler_",
    "controller_manager_", "kube_proxy_", "coredns_", "kubedns_",
    "node_", "container_runtime_", "containerd_", "crio_", "docker_", "runc_",
    "cadvisor_", "container_", "machine_",
    "calico_", "cilium_", "weave_", "flannel_", "istio_", "envoy_",
    "linkerd_", "nginx_ingress_", "traefik_", "ambassador_", "kong_", "haproxy_",
    "ceph_", "rook_", "longhorn_", "openebs_", "portworx_", "storageos_", "minio_",
    "prometheus_", "alertmanager_", "grafana_", "loki_", "promtail_",
    "tempo_", "mimir_", "thanos_", "cortex_", "jaeger_", "zipkin_",
    "otel_", "opentelemetry_", "elasticsearch_", "fluentd_", "fluent_bit_", "logstash_",
    "vm_", "victoria_metrics_", "vmagent_", "vmalert_", "vmauth_",
    "vminsert_", "vmselect_", "vmstorage_", "victoriametrics_",
    "cert_manager_", "certmanager_", "vault_",
    "argocd_", "argo_", "flux_", "tekton_",
    "istio_requests_", "istio_request_", "pilot_", "galley_", "mixer_", "citadel_",
    "cluster_autoscaler_", "karpenter_",
    "velero_", "restic_", "falco_", "gatekeeper_", "opa_", "kyverno_",
    "neuvector_", "aqua_", "trivy_",
    "knative_", "openfaas_", "fission_",
    "postgres_operator_", "mysql_operator_", "mongodb_operator_",
    "redis_operator_", "kafka_",
    "kubecost_", "opencost_",
    "keda_", "crossplane_", "external_dns_", "external_secrets_",
    "sealed_secrets_", "reloader_", "metrics_server_", "kube_state_metrics_",
    "node_exporter_", "blackbox_exporter_",
    "process_", "go_", "http_", "grpc_", "workqueue_", "rest_client_",
})

METRIC_QUERIES = [
    ("container_cpu_usage",         'rate(container_cpu_usage_seconds_total{container!=""}[5m])'),
    ("container_memory_usage",      'container_memory_usage_bytes{container!=""}'),
    ("container_memory_working_set",'container_memory_working_set_bytes{container!=""}'),
    ("container_fs_reads",          'rate(container_fs_reads_bytes_total{container!=""}[5m])'),
    ("container_fs_writes",         'rate(container_fs_writes_bytes_total{container!=""}[5m])'),
    ("container_network_receive",   'rate(container_network_receive_bytes_total{container!=""}[5m])'),
    ("container_network_transmit",  'rate(container_network_transmit_bytes_total{container!=""}[5m])'),
    ("container_restarts",          "kube_pod_container_status_restarts_total"),
    ("kube_deployment_available",   "kube_deployment_status_available_replicas"),
    ("kube_deployment_replicas",    "kube_deployment_spec_replicas"),
    ("kubelet_running_containers",  "kubelet_running_containers"),
    ("kubelet_running_pods",        "kubelet_running_pods"),
    ("kube_node_status_condition",  "kube_node_status_condition"),
    ("kube_pod_ready",              "kube_pod_status_ready"),
    ("kube_pod_status_phase",       "kube_pod_status_phase"),
    ("node_cpu_all_modes",          "rate(node_cpu_seconds_total[5m])"),
    ("node_cpu_idle",               'rate(node_cpu_seconds_total{mode="idle"}[5m])'),
    ("node_cpu_usage",              "instance:node_cpu_utilisation:rate5m"),
    ("node_cpu_usage_percent",      '100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)'),
    ("node_disk_read_bytes",        "rate(node_disk_read_bytes_total[5m])"),
    ("node_disk_write_bytes",       "rate(node_disk_written_bytes_total[5m])"),
    ("node_filesystem_usage",       "node_filesystem_size_bytes - node_filesystem_avail_bytes"),
    ("node_load1",                  "node_load1"),
    ("node_load5",                  "node_load5"),
    ("node_load15",                 "node_load15"),
    ("node_memory_total",           "node_memory_MemTotal_bytes"),
    ("node_memory_available",       "node_memory_MemAvailable_bytes"),
    ("node_memory_used",            "node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes"),
    ("node_network_receive",        "rate(node_network_receive_bytes_total[5m])"),
    ("node_network_transmit",       "rate(node_network_transmit_bytes_total[5m])"),
]


def classify_namespace(ns: str) -> str:
    return "k8s" if ns in K8S_SYSTEM_NAMESPACES else "application"


def classify_metric(metric_name: str, namespace: str = "") -> str:
    for prefix in K8S_SYSTEM_METRIC_PREFIXES:
        if metric_name.startswith(prefix):
            return "k8s"
    return classify_namespace(namespace) if namespace else "unknown"


# ─── Container status helpers ─────────────────────────────────────────────────

def _container_status_info(pod, container_name: str) -> dict:
    info = {"restart_count": 0, "has_previous": False,
            "curr_started": None, "prev_started": None, "prev_finished": None}
    if not pod.status or not pod.status.container_statuses:
        return info
    for cs in pod.status.container_statuses:
        if cs.name != container_name:
            continue
        info["restart_count"] = cs.restart_count or 0
        if cs.state:
            if cs.state.running and cs.state.running.started_at:
                info["curr_started"] = cs.state.running.started_at
            elif cs.state.terminated and cs.state.terminated.started_at:
                info["curr_started"] = cs.state.terminated.started_at
        if cs.last_state and cs.last_state.terminated:
            info["has_previous"] = True
            lt = cs.last_state.terminated
            info["prev_started"] = lt.started_at
            info["prev_finished"] = lt.finished_at
        break
    return info


# ─── 1a) K8s log fetching ─────────────────────────────────────────────────────

def fetch_single_log(core_v1, ns: str, pod_name: str, container: str,
                     previous: bool = False, suffix: Optional[str] = None) -> Optional[str]:
    log_type = classify_namespace(ns)
    fname = (f"{log_type}_{ns}_{pod_name}_{container}_{suffix}.log"
             if suffix else f"{log_type}_{ns}_{pod_name}_{container}.log")
    fpath = LOGS_DIR / fname
    try:
        kwargs = dict(
            name=pod_name, namespace=ns, container=container,
            timestamps=True, previous=previous, tail_lines=LOG_MAX_TAIL_LINES,
        )
        if not previous:
            kwargs["since_seconds"] = LOG_SINCE_SECONDS
        if MAX_BYTES_PER_POD > 0 and not previous:
            kwargs["limit_bytes"] = MAX_BYTES_PER_POD
        raw = core_v1.read_namespaced_pod_log(**kwargs)
        fpath.write_text(raw or "", encoding="utf-8")
        if raw and raw.count("\n") >= LOG_MAX_TAIL_LINES - 1:
            log.warning("[CAP] %s/%s/%s hit %d-line cap", ns, pod_name, container, LOG_MAX_TAIL_LINES)
    except ApiException as e:
        if previous and e.status in (400, 404):
            return None
        fpath.write_text(f"ERROR: {e.status} {e.reason}\n", encoding="utf-8")
    except Exception as e:
        fpath.write_text(f"ERROR: {e}\n", encoding="utf-8")
    return str(fpath)


# ─── 1b) VictoriaLogs historical log fetching ────────────────────────────────

def _parse_vl_time(ts_str: str) -> int:
    ts_str = ts_str.rstrip("Z").replace("T", " ")
    base, frac = (ts_str.split(".", 1) if "." in ts_str else (ts_str, ""))
    frac = (frac + "000000000")[:9]
    dt = datetime.strptime(base, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp()) * 1_000_000_000 + int(frac)


def _ns_to_iso(ts_ns: int) -> str:
    dt = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


async def _vl_query_range(session, ns: str, pod: str, container: str,
                          start_ns: int, end_ns: int, batch_size: int = 5000) -> List[Tuple[int, str]]:
    all_lines: List[Tuple[int, str]] = []
    current_end = end_ns
    query = f'{{namespace="{ns}",pod="{pod}",container="{container}"}}'
    while len(all_lines) < VL_MAX_LINES:
        try:
            async with session.get(
                f"{VICTORIA_LOGS_URL}/select/logsql/query",
                params={"query": query, "start": str(start_ns), "end": str(current_end), "limit": str(batch_size)},
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                if resp.status != 200:
                    log.warning("[VL] %d for %s/%s/%s", resp.status, ns, pod, container)
                    break
                raw = await resp.text()
        except Exception as e:
            log.warning("[VL] Error %s/%s/%s: %s", ns, pod, container, e)
            break
        batch: List[Tuple[int, str]] = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                batch.append((_parse_vl_time(obj.get("_time", "")), obj.get("_msg", "")))
            except Exception:
                continue
        if not batch:
            break
        all_lines.extend(batch)
        if len(batch) < batch_size:
            break
        oldest = min(ts for ts, _ in batch)
        if oldest <= start_ns:
            break
        current_end = oldest - 1
    all_lines.sort(key=lambda x: x[0])
    seen: set = set()
    deduped = []
    for item in all_lines:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def _split_by_gaps(lines: List[Tuple[int, str]], num_groups: int) -> List[List[Tuple[int, str]]]:
    if num_groups <= 1 or len(lines) < 2:
        return [lines]
    gaps = [(lines[i][0] - lines[i - 1][0], i) for i in range(1, len(lines))]
    top_gaps = sorted(gaps, key=lambda x: x[0], reverse=True)[: num_groups - 1]
    split_pts = sorted(idx for _, idx in top_gaps)
    groups: List[List] = []
    prev = 0
    for sp in split_pts:
        groups.append(lines[prev:sp])
        prev = sp
    groups.append(lines[prev:])
    while len(groups) < num_groups:
        groups.insert(0, [])
    return groups


async def collect_vl_historical(sem: asyncio.Semaphore, ns: str, pod: str,
                                container: str, restart_count: int, prev_started) -> int:
    if not VICTORIA_LOGS_URL:
        return 0
    num_historical = restart_count - 1
    log_type = classify_namespace(ns)
    now_ns = int(time.time() * 1e9)
    try:
        end_ns = int(prev_started.timestamp() * 1e9) - 1 if prev_started else now_ns
    except Exception:
        end_ns = now_ns
    start_ns = int((time.time() - VL_HISTORY_SECONDS) * 1e9)
    log.info("[VL] %s/%s/%s: querying %d historical run(s)", ns, pod, container, num_historical)
    async with sem:
        async with aiohttp.ClientSession() as session:
            lines = await _vl_query_range(session, ns, pod, container, start_ns, end_ns)
    if not lines:
        return 0
    groups = _split_by_gaps(lines, num_historical)
    saved = 0
    for i, group in enumerate(groups):
        fpath = LOGS_DIR / f"{log_type}_{ns}_{pod}_{container}_log{i}.log"
        text = "\n".join(f"{_ns_to_iso(ts)} {line}" for ts, line in group)
        fpath.write_text(text + ("\n" if text else ""), encoding="utf-8")
        log.info("[VL] Saved %s (%d lines)", fpath.name, len(group))
        saved += 1
    return saved


# ─── 1c) Log collection orchestrator ─────────────────────────────────────────

def collect_logs(core_v1) -> Tuple[List[tuple], set]:
    log.info("=" * 60)
    log.info("Collecting Logs (last %dh + full restart history)", LOG_SINCE_HOURS)
    log.info("=" * 60)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    pods = core_v1.list_pod_for_all_namespaces(watch=False)
    k8s_work: List[tuple] = []
    vl_work: List[tuple] = []
    live_pod_set: set = set()

    for pod in pods.items:
        ns = pod.metadata.namespace
        if ns in EXCLUDE_NAMESPACES:
            continue
        pod_name = pod.metadata.name
        phase = (pod.status.phase if pod.status else "Unknown") or "Unknown"
        all_containers = list(pod.spec.containers or [])
        if pod.spec.init_containers and phase not in ("Pending",):
            all_containers += list(pod.spec.init_containers)

        for c in all_containers:
            info = _container_status_info(pod, c.name)
            N = info["restart_count"]
            now_utc = datetime.now(timezone.utc)
            window_start = now_utc.timestamp() - LOG_SINCE_SECONDS
            recently_restarted = False
            if N > 0:
                ref = info["prev_finished"] or info["curr_started"]
                try:
                    recently_restarted = ref.timestamp() >= window_start if ref else True
                except Exception:
                    recently_restarted = True

            if N == 0 or not recently_restarted:
                k8s_work.append((ns, pod_name, c.name, False, None))
                live_pod_set.add((ns, pod_name, c.name))
            else:
                k8s_work.append((ns, pod_name, c.name, False, f"log{N}"))
                live_pod_set.add((ns, pod_name, c.name))
                if info["has_previous"]:
                    k8s_work.append((ns, pod_name, c.name, True, f"log{N - 1}"))
                if N >= 2:
                    vl_work.append((ns, pod_name, c.name, N, info["prev_started"]))

    log.info("K8s fetch targets: %d  VictoriaLogs targets: %d", len(k8s_work), len(vl_work))

    collected = failed = 0
    with ThreadPoolExecutor(max_workers=MAX_LOG_WORKERS) as executor:
        futures = {
            executor.submit(fetch_single_log, client.CoreV1Api(), ns, pod, cont, prev, suffix):
            f"{ns}/{pod}/{cont}"
            for ns, pod, cont, prev, suffix in k8s_work
        }
        for future in as_completed(futures):
            try:
                if future.result() is not None:
                    collected += 1
            except Exception as e:
                failed += 1
                log.warning("FAILED %s: %s", futures[future], e)

    log.info("K8s logs done: %d collected, %d failed", collected, failed)
    return vl_work, live_pod_set


async def _run_vl_work(vl_work: List[tuple]) -> int:
    sem = asyncio.Semaphore(VL_CONCURRENCY)
    tasks = [collect_vl_historical(sem, ns, pod, cont, N, prev) for ns, pod, cont, N, prev in vl_work]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return sum(r for r in results if isinstance(r, int))


# ─── 1d) Ghost pod detection ──────────────────────────────────────────────────

async def collect_ghost_pods(live_pod_set: set) -> int:
    log.info("=" * 60)
    log.info("Detecting Ghost Pods (deleted during collection window)")
    log.info("=" * 60)
    if not VICTORIA_LOGS_URL:
        log.info("VICTORIA_LOGS_URL not set — skipping ghost pod detection")
        return 0

    now_ns = int(time.time() * 1e9)
    start_ns = int((time.time() - LOG_SINCE_SECONDS) * 1e9)

    streams = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{VICTORIA_LOGS_URL}/select/logsql/streams",
                params={"query": "*", "start": str(start_ns), "end": str(now_ns)},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 200:
                    for line in (await resp.text()).splitlines():
                        try:
                            obj = json.loads(line.strip())
                            if obj.get("namespace") and obj.get("pod") and obj.get("container"):
                                streams.append(obj)
                        except Exception:
                            continue
    except Exception as e:
        log.warning("[GHOST] Error querying VictoriaLogs streams: %s", e)
        return 0

    ghost_pods = [
        (s["namespace"], s["pod"], s["container"]) for s in streams
        if (s["namespace"], s["pod"], s["container"]) not in live_pod_set
    ]
    log.info("Live pods: %d  Ghost pods: %d", len(live_pod_set), len(ghost_pods))
    if not ghost_pods:
        return 0

    sem = asyncio.Semaphore(VL_CONCURRENCY)
    saved = 0

    async def _fetch_ghost(ns, pod, container):
        log_type = classify_namespace(ns)
        fpath = LOGS_DIR / f"{log_type}_{ns}_{pod}_{container}_terminated.log"
        async with sem:
            async with aiohttp.ClientSession() as session:
                lines = await _vl_query_range(session, ns, pod, container, start_ns, now_ns)
        if not lines:
            return None
        text = "\n".join(f"{_ns_to_iso(ts)} {line}" for ts, line in lines)
        fpath.write_text(text + "\n", encoding="utf-8")
        log.info("[GHOST] %s/%s/%s → %s (%d lines)", ns, pod, container, fpath.name, len(lines))
        return str(fpath)

    results = await asyncio.gather(
        *[_fetch_ghost(ns, pod, cont) for ns, pod, cont in ghost_pods],
        return_exceptions=True,
    )
    saved = sum(1 for r in results if isinstance(r, str))
    log.info("Ghost pod logs saved: %d", saved)
    return saved


# ─── 2) Metrics collection ────────────────────────────────────────────────────

async def _fetch_metric(session, sem: asyncio.Semaphore, name: str, params: dict):
    fpath = METRICS_DIR / f"{name}.json"
    async with sem:
        try:
            async with session.get(
                f"{VICTORIA_METRICS_URL}/api/v1/query_range",
                params=params,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = json.loads(await resp.text())
            for entry in data.get("data", {}).get("result", []):
                metric = entry.get("metric", {})
                ns = metric.get("namespace", "")
                metric["log_type"] = classify_namespace(ns) if ns else classify_metric(name)
            data["metric_name"] = name
            async with aiofiles.open(fpath, "w") as f:
                await f.write(json.dumps(data, indent=2))
        except Exception as e:
            log.warning("FAILED metric %s: %s", name, e)
            async with aiofiles.open(fpath, "w") as f:
                await f.write(json.dumps({"error": str(e), "metric_name": name}, indent=2))


async def collect_metrics() -> int:
    log.info("=" * 60)
    log.info("Collecting Metrics (last %ds)", METRICS_RANGE_SECONDS)
    log.info("=" * 60)
    if not VICTORIA_METRICS_URL:
        log.warning("VICTORIA_METRICS_URL not set — skipping metrics")
        return 0

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{VICTORIA_METRICS_URL}/api/v1/query",
                params={"query": "up"},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                await resp.text()
    except Exception:
        log.error("Cannot reach VictoriaMetrics at %s", VICTORIA_METRICS_URL)
        return 0

    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    now = int(time.time())
    start = now - METRICS_RANGE_SECONDS
    sem = asyncio.Semaphore(MAX_METRIC_CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[
            _fetch_metric(session, sem, name, {"query": query, "start": str(start), "end": str(now), "step": METRICS_STEP})
            for name, query in METRIC_QUERIES
        ])

    count = len(list(METRICS_DIR.glob("*.json")))
    log.info("Metrics collected: %d files", count)
    return count


# ─── 3) Metric enrichment ─────────────────────────────────────────────────────

def enrich_metrics(core_v1):
    log.info("=" * 60)
    log.info("Enriching Metrics with Node Info")
    log.info("=" * 60)
    pods = core_v1.list_pod_for_all_namespaces(watch=False)
    pod_to_node: dict = {}
    ip_to_node: dict = {}
    node_ip_to_name: dict = {}
    for pod in pods.items:
        ns = pod.metadata.namespace
        node = pod.spec.node_name or ""
        node_ip = (pod.status.host_ip or "") if pod.status else ""
        pod_ip = (pod.status.pod_ip or "") if pod.status else ""
        pod_to_node[f"{ns}/{pod.metadata.name}"] = {"node": node, "node_ip": node_ip}
        if pod_ip:
            ip_to_node[pod_ip] = {"node": node, "node_ip": node_ip}
        if node and node_ip:
            node_ip_to_name[node_ip] = node

    enriched = 0
    for fpath in sorted(METRICS_DIR.glob("*.json")):
        try:
            data = json.loads(fpath.read_text())
        except Exception:
            continue
        results = data.get("data", {}).get("result", [])
        if not results:
            continue
        modified = False
        metric_name = data.get("metric_name", "")
        for entry in results:
            metric = entry.get("metric", {})
            if not metric.get("node"):
                key = f"{metric.get('namespace','')}/{metric.get('pod','')}"
                inst_ip = metric.get("instance", "").split(":")[0]
                if key in pod_to_node and metric.get("pod"):
                    metric.update(pod_to_node[key])
                    modified = True
                elif inst_ip in ip_to_node:
                    metric.update(ip_to_node[inst_ip])
                    modified = True
                elif inst_ip in node_ip_to_name:
                    metric["node"] = node_ip_to_name[inst_ip]
                    metric["node_ip"] = inst_ip
                    modified = True
            if "log_type" not in metric:
                ns = metric.get("namespace", "")
                metric["log_type"] = classify_namespace(ns) if ns else classify_metric(metric_name)
                modified = True
        if modified:
            fpath.write_text(json.dumps(data, indent=2))
            enriched += 1
    log.info("Enriched %d metric files", enriched)


# ─── 4) Zip & upload ──────────────────────────────────────────────────────────

def _write_metadata(stats: dict) -> Path:
    meta = {
        "collected_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_hours": LOG_SINCE_HOURS,
        "vl_history_days": VL_HISTORY_SECONDS // 86400,
        "log_max_tail_lines": LOG_MAX_TAIL_LINES,
        "endpoint": ENDPOINT,
        **stats,
    }
    meta_path = BASEDIR / "collection_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2))
    return meta_path


async def zip_and_upload(meta_path: Optional[Path] = None):
    log.info("=" * 60)
    log.info("Zipping & Uploading")
    log.info("=" * 60)
    zip_path = BASEDIR / "k8s_logs_metrics.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for folder in [LOGS_DIR, METRICS_DIR]:
            if folder.exists():
                for f in folder.iterdir():
                    if f.is_file():
                        zf.write(f, arcname=f"{folder.name}/{f.name}")
        if meta_path and meta_path.exists():
            zf.write(meta_path, arcname=meta_path.name)

    size_mb = zip_path.stat().st_size / 1024 / 1024
    log.info("Created zip: %.1f MB", size_mb)

    ssl_ctx = ssl.create_default_context()
    if TLS_SKIP_VERIFY:
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    for attempt in range(1, UPLOAD_MAX_RETRIES + 1):
        try:
            connector = aiohttp.TCPConnector(ssl=ssl_ctx)
            async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=600)) as session:
                with open(zip_path, "rb") as fh:
                    form = aiohttp.FormData()
                    form.add_field("file", fh, filename="k8s_logs_metrics.zip", content_type="application/zip")
                    async with session.post(
                        ENDPOINT, data=form,
                        headers={"Authorization": f"Bearer {KUBENTIC_TOKEN}"},
                    ) as resp:
                        body = await resp.text()
                        if resp.status < 400:
                            log.info("Upload succeeded (attempt %d) — HTTP %d", attempt, resp.status)
                            return
                        log.error("Upload failed HTTP %d: %s (attempt %d/%d)",
                                  resp.status, body[:200], attempt, UPLOAD_MAX_RETRIES)
        except Exception as e:
            log.error("Upload error (attempt %d/%d): %s", attempt, UPLOAD_MAX_RETRIES, e)

        if attempt < UPLOAD_MAX_RETRIES:
            wait = 2 ** attempt + random.uniform(0, 1)
            log.info("Retrying in %.1fs...", wait)
            await asyncio.sleep(wait)

    raise RuntimeError(f"Upload failed after {UPLOAD_MAX_RETRIES} attempts")


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    log.info("=" * 60)
    log.info("Kubentic Agent — cloud-agnostic collector")
    log.info("Backend:  %s", ENDPOINT)
    log.info("VM URL:   %s", VICTORIA_METRICS_URL or "(not set)")
    log.info("VL URL:   %s", VICTORIA_LOGS_URL or "(not set — historical runs skipped)")
    log.info("=" * 60)

    try:
        config.load_incluster_config()
    except config.ConfigException:
        log.error("Not running inside a Kubernetes cluster")
        sys.exit(1)

    core_v1 = client.CoreV1Api()
    BASEDIR.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    vl_work, live_pod_set = collect_logs(core_v1) if COLLECT_LOGS else ([], set())

    if vl_work:
        vl_count = await _run_vl_work(vl_work)
        log.info("VictoriaLogs historical: %d file(s) saved", vl_count)

    ghost_count = await collect_ghost_pods(live_pod_set) if COLLECT_LOGS else 0

    metrics_count = await collect_metrics() if COLLECT_METRICS else 0
    if COLLECT_METRICS and metrics_count > 0:
        enrich_metrics(core_v1)

    elapsed = time.time() - t0
    all_logs = list(LOGS_DIR.glob("*.log")) if LOGS_DIR.exists() else []
    meta_path = _write_metadata({
        "total_elapsed_seconds": round(elapsed, 1),
        "total_log_files": len(all_logs),
        "application_logs": sum(1 for f in all_logs if f.name.startswith("application_")),
        "k8s_system_logs": sum(1 for f in all_logs if f.name.startswith("k8s_")),
        "numbered_logs": sum(1 for f in all_logs if "_log" in f.stem),
        "ghost_terminated_logs": ghost_count,
        "metrics_files": metrics_count,
    })

    await zip_and_upload(meta_path)

    log.info("=" * 60)
    log.info("DONE in %.1fs — %d logs, %d metrics", elapsed, len(all_logs), metrics_count)
    log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
