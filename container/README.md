# cloudera-smoke-runner (container)

OCI-compliant container that orchestrates Cloudera CDP smoke/health tests. It
launches an ephemeral EC2 inside the cluster's VPC, runs the probe via SSM,
collects results, emits **Nagios + Prometheus + JSON + HTML** outputs, and
tears everything down.

## Build

```bash
# via the helper script (picks podman or docker automatically)
./run.sh --build

# or manually
podman build -t cloudera-smoke-runner:latest .      # or: docker build ...
```

Optional build args:
```bash
podman build \
  --build-arg IMAGE_VERSION=1.0.0 \
  --build-arg IMAGE_REVISION=$(git rev-parse --short HEAD 2>/dev/null || echo local) \
  --build-arg IMAGE_CREATED=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
  -t cloudera-smoke-runner:1.0.0 .
```

## Run

```bash
./run.sh                             # prompts for anything not in env
./run.sh --output-dir /tmp/results   # override output dir
ENV_NAME=prod AWS_PROFILE=my-sso CLUSTER_NAME=BaseCluster1 \
  AWS_REGION=ap-southeast-1 VPC_ID=vpc-x SUBNET_ID=subnet-y \
  CM_HOST=10.0.0.10 CM_PORT=7183 CM_USER=admin CM_PASS=... \
  ./run.sh                           # fully non-interactive
```

Before running, ensure your AWS SSO session is valid on the host:
```bash
aws sso login --profile my-sso
```
`run.sh` mounts `~/.aws` read-only so the container can reuse the cached token.

## Outputs (written to `./output/` by default)

| File | Purpose |
|---|---|
| `smoke-<env>-<ts>.nagios` | Single-line Nagios plugin output (same as stdout) |
| `smoke-<env>-<ts>.prom`   | Prometheus text exposition format |
| `smoke-<env>-<ts>.json`   | Full structured results |
| `smoke-<env>-<ts>.html`   | Standalone HTML report (no external deps) |

Stdout is always the Nagios line; exit code follows Nagios convention:

| Code | Meaning |
|---:|---|
| 0 | OK — all services healthy |
| 1 | WARNING — at least one CONCERNING / utilization > 85% |
| 2 | CRITICAL — any FAIL, service down, utilization > 95% |
| 3 | UNKNOWN — orchestrator couldn't complete (auth, network, API) |

## Integration examples

**Nagios/Icinga** (`command_line`):
```
command_line  /opt/cloudera-smoke/run.sh --output-dir /var/tmp/smoke
```

**Prometheus** — scrape the `.prom` file with node_exporter's textfile collector:
```
# /etc/prometheus/node_exporter/textfile/
ln -sf /var/tmp/smoke/smoke-prod-*.prom /etc/prometheus/node_exporter/textfile/cloudera-smoke.prom
```

**Cron** (hourly):
```cron
0 * * * * /opt/cloudera-smoke/run.sh >> /var/log/cloudera-smoke.log 2>&1
```

## Files in this directory

```
Dockerfile          # OCI image (multi-stage, non-root, dumb-init, labels)
requirements.txt    # boto3, requests, urllib3
entrypoint.sh       # reads /run/cm_pass if CM_PASS unset, execs smoke.py
smoke.py            # orchestrator (SSO, VPC, EC2 launch, SSM probe, teardown)
remote_probe.py     # runs on the ephemeral EC2; emits JSON result block
report.py           # nagios / prometheus / html builders
run.sh              # host-side runner; autodetects podman/docker
README.md           # this file
```
