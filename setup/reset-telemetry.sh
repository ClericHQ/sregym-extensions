#!/usr/bin/env bash
# Empty Loki (and Prometheus, when installed) between incidents, so an investigation can't read an earlier run's
# telemetry, or the healthy period's, which would give it what only Cleric's discovered knowledge should. Both keep
# their data on an emptyDir; a restart drops it.
set -euo pipefail
kubectl -n observe rollout restart statefulset/loki >/dev/null
if kubectl -n observe get deployment/prometheus-server >/dev/null 2>&1; then
  kubectl -n observe rollout restart deployment/prometheus-server >/dev/null
  kubectl -n observe rollout status deployment/prometheus-server --timeout=300s >/dev/null
fi
kubectl -n observe rollout status statefulset/loki --timeout=300s >/dev/null
echo "Telemetry emptied."
