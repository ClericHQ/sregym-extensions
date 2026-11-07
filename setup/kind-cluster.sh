#!/usr/bin/env bash
# Optional: a local kind cluster to run the scenarios on, for trying the kit on a laptop (Docker Desktop or similar
# with ~6 CPUs and 8 GB for its VM). Prints SCENARIO_HOST_ADDRESS, the address pods reach this machine at.
set -euo pipefail
NAME=${1:-cleric-scenarios}
command -v kind >/dev/null || { echo "kind is required (https://kind.sigs.k8s.io)" >&2; exit 1; }
if ! kind get clusters | grep -qx "$NAME"; then
  kind create cluster --name "$NAME" --wait 120s --config - <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
EOF
fi
kubectl config use-context "kind-$NAME" >/dev/null
# Docker Desktop resolves host.docker.internal to the host from inside a container; elsewhere the node network's
# gateway is the host.
NODE="$NAME-worker"
ADDRESS=$(docker exec "$NODE" getent hosts host.docker.internal 2>/dev/null | awk '{print $1}' | head -1)
if [ -z "$ADDRESS" ]; then
  ADDRESS=$(docker network inspect kind -f '{{range .IPAM.Config}}{{.Gateway}} {{end}}' | tr ' ' '\n' | grep -E '^[0-9.]+$' | head -1)
fi
echo "Cluster $NAME is up (context kind-$NAME)."
echo "export SCENARIO_HOST_ADDRESS=$ADDRESS"
