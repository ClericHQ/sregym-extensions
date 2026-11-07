#!/usr/bin/env bash
# Install what the scenarios need beside them on the current kubectl context: Loki and Promtail (logs), Grafana over
# Loki (how Cleric reads logs), Prometheus (cluster and container metrics), and a read-only identity for
# Cleric. Prints the values Cleric's integrations need and writes them to setup/out/connection.env.
set -euo pipefail
cd "$(dirname "$0")"

for tool in kubectl helm curl python3; do
  command -v "$tool" >/dev/null || { echo "$tool is required" >&2; exit 1; }
done
echo "Installing into context $(kubectl config current-context)"

helm repo add grafana https://grafana.github.io/helm-charts >/dev/null
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null
helm repo update grafana prometheus-community >/dev/null

# Each release's output goes to setup/out/install.log; a failure prints its tail.
mkdir -p out
: > out/install.log
release() {
  echo "installing $1..."
  if ! helm upgrade --install "$@" -n observe --wait --timeout 10m >> out/install.log 2>&1; then
    echo "helm failed on $1; the end of setup/out/install.log:" >&2
    tail -20 out/install.log >&2
    exit 1
  fi
}

kubectl create namespace observe --dry-run=client -o yaml | kubectl apply -f - >/dev/null
release loki grafana/loki --version 7.5.0 -f values/loki.yaml
release promtail grafana/promtail --version 6.17.1 -f values/promtail.yaml

GRAFANA_PASSWORD=$(python3 -c 'import secrets; print(secrets.token_urlsafe(18))')
release grafana grafana/grafana --version 10.5.15 -f values/grafana.yaml --set adminPassword="$GRAFANA_PASSWORD"
release prometheus prometheus-community/prometheus --version 29.44.0 -f values/prometheus.yaml

# Cleric's identity: read-only telemetry access (objects, logs, ConfigMaps), no Secrets, exec, port-forward or
# service proxy; a long-lived token, since a new token is a changed integration and restarts Cleric's discovery.
kubectl apply -f reader-rbac.yaml >/dev/null
for _ in $(seq 30); do
  READER_TOKEN=$(kubectl -n cleric get secret cleric-reader-token -o jsonpath='{.data.token}' 2>/dev/null | base64 -d || true)
  [ -n "$READER_TOKEN" ] && break
  sleep 1
done

# A Viewer token for Cleric's Grafana integration.
kubectl -n observe port-forward svc/grafana 13000:80 >/dev/null 2>&1 &
FORWARD=$!
trap 'kill $FORWARD 2>/dev/null || true' EXIT
for _ in $(seq 30); do curl -sf localhost:13000/api/health >/dev/null && break; sleep 1; done
SA_ID=$(curl -sf -u "admin:$GRAFANA_PASSWORD" -X POST localhost:13000/api/serviceaccounts \
  -H 'Content-Type: application/json' -d '{"name":"cleric","role":"Viewer"}' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' \
  || curl -sf -u "admin:$GRAFANA_PASSWORD" "localhost:13000/api/serviceaccounts/search?query=cleric" \
     | python3 -c 'import json,sys; print(json.load(sys.stdin)["serviceAccounts"][0]["id"])')
GRAFANA_TOKEN=$(curl -sf -u "admin:$GRAFANA_PASSWORD" -X POST "localhost:13000/api/serviceaccounts/$SA_ID/tokens" \
  -H 'Content-Type: application/json' -d "{\"name\":\"cleric-$(date +%s)\"}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["key"])')

KUBE_HOST=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
KUBE_CA_B64=$(kubectl config view --minify --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')

umask 077
cat > out/connection.env <<EOF
# What Cleric's integrations need. Secrets: keep this file private. The *_SERVICE entries are in-cluster services to
# expose to Cleric, not URLs it can reach as they are.
KUBE_HOST=$KUBE_HOST
KUBE_CA_B64=$KUBE_CA_B64
READER_TOKEN=$READER_TOKEN
GRAFANA_SERVICE=observe/grafana:80
GRAFANA_TOKEN=$GRAFANA_TOKEN
GRAFANA_ADMIN_PASSWORD=$GRAFANA_PASSWORD
PROMETHEUS_SERVICE=observe/prometheus-server:80
EOF
echo "Done. Connection values are in setup/out/connection.env."
echo "Cleric must reach the API server ($KUBE_HOST), Grafana (observe/grafana:80) and Prometheus"
echo "(observe/prometheus-server:80): expose them as you would for any cluster you connect to Cleric."
