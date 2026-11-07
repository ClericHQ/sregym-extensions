# Cleric knowledge scenarios

This repo contains five incidents that intend to demonstrate the value of environmental knowledge when investigating.

They are modeled after the SREGym harness, and re-use its grader. The environments themselves are different, though are
deployed similarly.

| world | scenario | what decides it |
| --- | --- | --- |
| support-desk | `support_handoff_failed` | a chatbot handoff's offer reached the agent's desk before the desk was listening |
| workspace-suite | `payments_core_5xx` | one admin's bulk reads through an internal console set off a 5xx alert |
| storefront | `orders_drop_edge` | the CDN's firewall blocks the Android app; only the CDN's log stream shows it |
| storefront | `checkout_fx_quota` | a feature flag drains a shared quota; only the flag provider's audit log says who turned it on |
| delivery | `couriers_offline` | a partner's rota feed went stale; knowing how it normally behaves is critical |

These steps are written for an agent with access to your tenant's Cleric MCP server and to a shell on the machine
that runs this kit. A person can follow them too: every MCP call has a counterpart in Cleric's web app.

We intend for you to compare Cleric's responses to other agents, such as a plain Claude Code, Codex, or other production-facing
agent.

## Before you start

**A tenant for this.** Use a Cleric tenant with no other integrations. Discovery reads every integration, and
investigations can use every one, so other systems would enter what Cleric learns and draw it away from the worlds,
and the worlds' services would enter your real service catalog.

**A Kubernetes cluster** you can install into with an admin kubectl context, and a way for Cleric to reach its API
server, a Grafana and a Prometheus in it. About 4 CPUs and 6 GB free in the cluster is enough; nothing else
should run in namespaces the worlds use (`support`, `realtime`, `workspace`, `mail`, `identity`, `backoffice`, `shop`,
`fulfil`, `platform`, `marketing`, `eats`, `fleet`, `partners`, `observe`, `cleric`). `setup/kind-cluster.sh` makes a
local one (it needs Docker and kind, and ~6 CPUs and 8 GB for Docker's VM). Connectivity is left open to the evaluator:
you may use tunnels to expose local deployments or deploy to the cloud for easy integration.

**This machine** needs `python3` (3.11 or later), `kubectl`, `helm` and `curl`, and must stay up while a world runs:
`scenario.py` plays everything outside the cluster (the users, the partners, the CDN) from here. The cluster's pods must
be able to reach this machine: set `SCENARIO_HOST_ADDRESS` to an address of it they can route to (for a kind cluster
on this machine, `setup/kind-cluster.sh` prints it). `scenario.py` holds the answers: never run it where Cleric can read
it, such as in the cluster.

**A remote cluster with a local driver.** The partners' and the FX provider's APIs, which the worlds' services treat as
external, are served by `scenario.py` (ports 18471 and 18472), not in the cluster. If the cluster runs
elsewhere (a cloud cluster, a remote server) while `scenario.py` runs here, its pods must still be able to route back
to this machine on those ports: give this machine an address the cluster can reach (a VPN, a reverse tunnel such as
`ssh -R`, or a public address with the ports open to the cluster's egress) and set `SCENARIO_HOST_ADDRESS` to it.
Alternatively, colocate the driver with the worlds, but make sure it's not somewhere the agent has access to (such as
in the main cluster, without modifying the RBAC).

**For grading**, an Anthropic API key (`ANTHROPIC_API_KEY`). The grader runs Claude Sonnet 4.5 unless `GRADER_MODEL`
names another model.

## 1. Set up the cluster (once)

```bash
setup/kind-cluster.sh                     # only if you want a local kind cluster; prints SCENARIO_HOST_ADDRESS
setup/install.sh                          # Loki, Promtail, Grafana, Prometheus, and Cleric's read-only identity
export SCENARIO_HOST_ADDRESS=<from above, or this machine's address the pods can reach>
export SCENARIO_OTLP_ENDPOINT=            # no tracing backend is installed
```

`install.sh` writes the values for Cleric's integrations to `setup/out/connection.env` (helm's output goes to
`setup/out/install.log`). Expose the API server, `observe/grafana:80` and `observe/prometheus-server:80` to Cleric as you
would for any cluster you connect; the integrations below take the URLs Cleric reaches them at, not the in-cluster
names.

## 2. For each world: let Cleric learn it

Do the worlds one at a time, in any order: `support-desk`, `workspace-suite`, `storefront`, `delivery`.

1. **Start the world's everyday traffic** and let it warm up for four minutes, so every service has logged its usual
   lines:

   ```bash
   python3 scenario.py steady storefront && sleep 240
   ```

2. **Start discovery.** For the first world, **connect the integrations** below: saving them is what starts Cleric's
   discovery. Create all three within a minute or two, since each save restarts discovery to cover what has been saved
   so far. Names and descriptions are what Cleric sees: keep them neutral.

   For **every world after the first**, keep the integrations and call **`recalibrate`** (no arguments) instead. It
   deletes what Cleric learned of the previous world and every memory, then runs discovery again over the integrations
   connected, as if they had just been connected. Its reply suggests adding new integrations: don't, the three you have
   stay connected and are what discovery reads. It can't be undone; on a tenant used only for this kit that is the
   point, but say so to the user before the first call.

   With the values from `setup/out/connection.env`, `create_integration_connection`:

   ```json
   {"configuration": {"name": "prod-k8s", "toolkit_type": "kubernetes", "description": "Production cluster",
     "config": {"host": "<API server URL Cleric reaches>", "certificate_b64": "<KUBE_CA_B64, or empty>"},
     "credentials": {"api_token": "<READER_TOKEN>"}}}
   ```
   ```json
   {"configuration": {"name": "prod-grafana", "toolkit_type": "grafana", "description": "Grafana with the cluster's logs",
     "config": {"host": "<Grafana URL Cleric reaches>"}, "credentials": {"api_token": "<GRAFANA_TOKEN>"}}}
   ```
   ```json
   {"configuration": {"name": "prod-prometheus", "toolkit_type": "prometheus", "description": "Cluster metrics",
     "config": {"host": "<Prometheus URL Cleric reaches>", "type": "prometheus"}, "credentials": {}}}
   ```

   Check the field names first with `list_integration_schemas` (`toolkit_type`: kubernetes, grafana, prometheus). The
   reader token sends credentials through you to your model provider; enter them under Integrations in the web app
   instead if that matters. Cleric tests each connection before saving it and refuses a Kubernetes identity that can
   write or read Secrets; `setup/reader-rbac.yaml` is read-only.

3. **Wait for discovery**, with the traffic still running: 15-40 minutes for a world. Each
   `create_integration_connection` reply links to System Health, which shows discovery's progress; after a
   `recalibrate`, open System Health in the web app. When it has finished, check with `get_service` that Cleric knows
   the world's services and how they call each other:

   | world | `get_service` on | should list |
   | --- | --- | --- |
   | support-desk | `routing`, `push-api` | routing calls push-api and ticketing; push-api calls push-hub |
   | workspace-suite | `admin-api` | admin-api calls payments-core and accounts |
   | storefront | `search` | search calls fx-gateway, flag-relay, catalog-api and settings |
   | delivery | `rota-sync`, `dispatch` | rota-sync calls courier-api; dispatch calls courier-api, deliveries and eta |

   If a world's services aren't there yet, wait another 15 minutes and ask again. Cleric starts investigations only
   once System Health shows calibration complete: until then `create_issue` is refused.

4. **Stop the traffic and empty the telemetry**, so the incidents start fresh:

   ```bash
   python3 scenario.py stop && setup/reset-telemetry.sh
   ```

## 3. Run the world's incidents

Run each scenario of the world 6 times. For each run:

```bash
python3 scenario.py start checkout_fx_quota      # waits until the incident's alert condition holds (up to ~10 min)
```

It prints the alert. Start Cleric's investigation at once, with exactly that text:

- `create_issue` with `{"description": "<the alert>"}`, then
- `get_issue` with `{"id": "<the slug it returned>", "wait_seconds": 60}`, repeated until its status is no longer
  `investigating`.

Save Cleric's answer and the run's answer key. The answer is the issue's latest answer exactly as `get_issue` returns
it, every section of it (Summary, Root cause, Evidence, Next step), without the issue's header, its remediation or its
citations.

```bash
mkdir -p results/checkout_fx_quota/1
# write the answer to results/checkout_fx_quota/1/answer.md
cp "$(ls -d state/checkout_fx_quota-* | tail -1)/fault.json" results/checkout_fx_quota/1/
python3 scenario.py stop && setup/reset-telemetry.sh
```

Then the next run. When a world's scenarios are done, go on to the next world (step 2, with `recalibrate`).

## 4. Grade

```bash
python3 grade/grade.py results/
```

It grades every answer against its run's fault with SREGym's diagnosis checklist, prints each scenario's pass count
and mean score, and writes `results/grades.json` with each answer's score and the judge's reasoning. An answer it
couldn't grade (a network error, say) is listed and left out of the counts; run it again.

## Files

| path | what it is |
| --- | --- |
| `scenario.py` | starts and stops worlds and incidents; holds the answers |
| `driver/` | the worlds: their services (`driver/knowledge_services/`, deployed as ConfigMaps, readable by Cleric) and the traffic, partners and incidents (`driver/knowledge_faults.py`) |
| `setup/` | the cluster's telemetry stack, Cleric's read-only identity (`reader-rbac.yaml`), and the telemetry reset |
| `grade/` | the grader, SREGym's checklist and its licence |
