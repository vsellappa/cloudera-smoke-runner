#!/usr/bin/env python3
"""
Runs on the ephemeral EC2 inside the cluster VPC. Reads config JSON from stdin.
Emits a JSON result block between ---PROBE-JSON-START--- / ---PROBE-JSON-END--- markers.
"""
from __future__ import annotations
import json
import os
import socket
import sys
import time
import warnings
from urllib.parse import quote

warnings.filterwarnings("ignore")
try:
    import requests
except ImportError:
    os.system("pip3 install --quiet requests >/dev/null 2>&1")
    import requests

cfg = json.loads(sys.stdin.read())
CM_HOST  = cfg["CM_HOST"];  CM_PORT  = int(cfg.get("CM_PORT", 7183))
CM_USER  = cfg["CM_USER"];  CM_PASS  = cfg["CM_PASS"]
CLUSTER  = cfg["CLUSTER_NAME"]
SVC_FILTER = cfg.get("SERVICES_TO_TEST", "all")

BASE = f"https://{CM_HOST}:{CM_PORT}"
AUTH = (CM_USER, CM_PASS)
S = requests.Session(); S.auth = AUTH; S.verify = False

def log(m): print(f"# {m}", file=sys.stderr, flush=True)

def api(path, method="GET", body=None, timeout=30):
    r = S.request(method, f"{BASE}{path}",
                  json=body, timeout=timeout,
                  headers={"Content-Type":"application/json"} if body else None)
    return r

def get(path, **kw):
    r = api(path, **kw)
    r.raise_for_status()
    return r.json()

# ---------------------------------------------------------------------------
# Phase A: CM self + cluster discovery
# ---------------------------------------------------------------------------
cm_version = get("/api/version").__str__() if False else None
r = api("/api/version"); api_version = r.text.strip().strip('"') if r.ok else None
cm_v = get("/api/v51/cm/version") if api_version else {}
clusters_all = get("/api/v51/clusters").get("items", [])

# Identify the requested base cluster; discover other clusters (likely ECS)
target = next((c for c in clusters_all if c["name"] == CLUSTER), None)
if not target:
    print("---PROBE-JSON-START---")
    print(json.dumps({"error": f"cluster '{CLUSTER}' not found in CM",
                      "available": [c["name"] for c in clusters_all]}))
    print("---PROBE-JSON-END---")
    sys.exit(2)

data_services_cluster = None
data_services_version = None
# ECS clusters often don't appear in /clusters — probe by name pattern
for candidate in ("ECSCluster1", "default-ecs"):
    try:
        c = get(f"/api/v51/clusters/{candidate}")
        data_services_cluster = c["name"]
        data_services_version = c.get("fullVersion")
        break
    except requests.HTTPError:
        continue
# Try controlPlanes endpoint for the canonical PvC version
try:
    cp = get("/api/v51/controlPlanes")
    for item in cp.get("items", []):
        if item.get("version"):
            data_services_version = data_services_version or item["version"]
        if not data_services_cluster:
            for c in clusters_all:
                if c.get("clusterType") != "BASE_CLUSTER":
                    data_services_cluster = c["name"]; break
except Exception: pass

# ---------------------------------------------------------------------------
# Phase B: Base cluster service health
# ---------------------------------------------------------------------------
services = get(f"/api/v51/clusters/{CLUSTER}/services").get("items", [])
if SVC_FILTER != "all":
    wanted = {s.strip().lower() for s in SVC_FILTER.split(",")}
    services = [s for s in services if s["name"].lower() in wanted or s["type"].lower() in wanted]

svc_results = []
for s in services:
    state = s.get("serviceState", "?")
    health = s.get("healthSummary", "?")
    verdict = "PASS"
    if state not in ("STARTED","NA"): verdict = "FAIL"
    elif health == "BAD":             verdict = "FAIL"
    elif health == "CONCERNING":      verdict = "WARN"
    role_summary = {}
    try:
        rr = get(f"/api/v51/clusters/{CLUSTER}/services/{s['name']}/roles")
        for role in rr.get("items", []):
            t = role["type"]
            slot = role_summary.setdefault(t, {"count":0,"started":0,"good":0})
            slot["count"] += 1
            if role.get("roleState")=="STARTED": slot["started"] += 1
            if role.get("healthSummary")=="GOOD": slot["good"] += 1
    except Exception as e:
        role_summary["error"] = str(e)
    svc_results.append({"name":s["name"],"type":s["type"],"state":state,
                        "health":health,"verdict":verdict,"roles":role_summary})

# ---------------------------------------------------------------------------
# Phase C: Data Services (ECS) health
# ---------------------------------------------------------------------------
ds_results = []
if data_services_cluster:
    try:
        ds_services = get(f"/api/v51/clusters/{data_services_cluster}/services").get("items", [])
        for s in ds_services:
            state = s.get("serviceState","?"); health = s.get("healthSummary","?")
            verdict = "PASS" if health=="GOOD" and state=="STARTED" else ("WARN" if health=="CONCERNING" else "FAIL")
            # Pull failing checks + role roll-up
            failed = []; ds_roles = {}
            try:
                detail = get(f"/api/v51/clusters/{data_services_cluster}/services/{s['name']}")
                for hc in detail.get("healthChecks", []):
                    if hc.get("summary") not in ("GOOD","DISABLED"):
                        failed.append({"name":hc["name"],"summary":hc["summary"]})
            except Exception: pass
            try:
                rr = get(f"/api/v51/clusters/{data_services_cluster}/services/{s['name']}/roles")
                for role in rr.get("items", []):
                    t = role["type"]
                    slot = ds_roles.setdefault(t, {"count":0,"started":0,"good":0})
                    slot["count"] += 1
                    if role.get("roleState")=="STARTED": slot["started"] += 1
                    if role.get("healthSummary")=="GOOD": slot["good"] += 1
            except Exception: pass
            # URLs for DS service
            ds_urls = []
            cm_ext = f"https://{CM_HOST}:{CM_PORT}"
            if s["type"] == "ECS":
                # ECS server typically exposes Rancher UI on 8443; CM has the link
                ds_urls.append({"label":"ECS (CM)", "url": f"{cm_ext}/cmf/services?clusterName={data_services_cluster}"})
            elif s["type"] == "DOCKER":
                ds_urls.append({"label":"Docker (CM)", "url": f"{cm_ext}/cmf/services?clusterName={data_services_cluster}"})
            else:
                ds_urls.append({"label":f"{s['name']} (CM)", "url": f"{cm_ext}/cmf/services?clusterName={data_services_cluster}"})
            ds_results.append({"name":s["name"],"type":s["type"],"state":state,
                               "health":health,"verdict":verdict,
                               "failed_checks":failed,"roles":ds_roles,"urls":ds_urls})
    except Exception as e:
        ds_results = [{"error": f"ds probe: {e}"}]

# ---------------------------------------------------------------------------
# Phase D: Host utilization (CPU/RAM via CM timeseries)
# ---------------------------------------------------------------------------
host_rows = {}
for m in ("cpu_percent","physical_memory_used","physical_memory_total"):
    try:
        r = get(f"/api/v51/timeseries?query={quote('select '+m+' where category=HOST')}")
        for item in r.get("items", []):
            for ts in item.get("timeSeries", []):
                host = ts["metadata"].get("entityName","?")
                data = ts.get("data", [])
                if data:
                    host_rows.setdefault(host, {})[m] = data[-1]["value"]
    except Exception as e:
        log(f"timeseries {m}: {e}")

hosts_out = []
for h, m in sorted(host_rows.items()):
    cpu = m.get("cpu_percent")
    mu, mt = m.get("physical_memory_used"), m.get("physical_memory_total")
    mem_pct = (mu/mt*100) if (mu and mt) else None
    worst = max([x for x in (cpu, mem_pct) if x is not None], default=0)
    verdict = "PASS" if worst < 85 else ("WARN" if worst < 95 else "FAIL")
    hosts_out.append({"host":h, "cpu_pct":cpu, "mem_pct":mem_pct, "verdict":verdict})

# ---------------------------------------------------------------------------
# Disk utilization (per host/mount via FILESYSTEM-category timeseries)
# ---------------------------------------------------------------------------
disk_rows = []
GB = 1024**3
try:
    queries = [
        "select total_bytes_on_filesystem, bytes_used_on_filesystem where category=FILESYSTEM",
        "select capacity, capacity_used where category=FILESYSTEM",
    ]
    raw = {}
    for q in queries:
        try:
            r = get(f"/api/v51/timeseries?query={quote(q)}")
        except Exception:
            continue
        for item in r.get("items", []):
            for ts in item.get("timeSeries", []):
                meta = ts["metadata"]
                attrs = meta.get("attributes", {}) or {}
                host  = attrs.get("hostname") or attrs.get("hostId") or meta.get("entityName","?")
                mount = attrs.get("mountpoint") or attrs.get("mountPoint") or attrs.get("filesystemType") or "?"
                metric = meta["metricName"]
                data = ts.get("data", [])
                if data:
                    raw.setdefault((host, mount), {})[metric] = data[-1]["value"]
        if raw: break  # first query that yielded data wins

    # Resolve hostId -> hostname when needed
    by_id = {h["hostId"]: h["hostname"] for h in all_hosts.values()}

    # Per-mount rows
    per_host_total = {}
    per_host_used  = {}
    for (host, mount), m in sorted(raw.items()):
        if host in by_id: host = by_id[host]
        total = m.get("total_bytes_on_filesystem") or m.get("capacity")
        used  = m.get("bytes_used_on_filesystem")  or m.get("capacity_used")
        if not total or used is None: continue
        pct = used/total*100
        verdict = "PASS" if pct < 85 else ("WARN" if pct < 95 else "FAIL")
        disk_rows.append({"host":host, "mount":mount,
                          "used_gb": round(used/GB, 1),
                          "total_gb": round(total/GB, 1),
                          "pct": round(pct,1), "verdict":verdict})
        per_host_total[host] = per_host_total.get(host,0) + total
        per_host_used[host]  = per_host_used.get(host,0)  + used

    # Per-host roll-up appended at end (mount="* total *")
    for host in sorted(per_host_total):
        t, u = per_host_total[host], per_host_used[host]
        if not t: continue
        pct = u/t*100
        verdict = "PASS" if pct < 85 else ("WARN" if pct < 95 else "FAIL")
        disk_rows.append({"host":host, "mount":"(total all mounts)",
                          "used_gb":round(u/GB,1), "total_gb":round(t/GB,1),
                          "pct":round(pct,1), "verdict":verdict})

    # Fallback: if FILESYSTEM gave nothing, use HOST-category aggregate metrics
    if not disk_rows:
        for q in (
            "select total_disk_space_across_disks, disk_space_used_across_disks where category=HOST",
            "select bytes_total_across_disks, bytes_used_across_disks where category=HOST",
        ):
            try:
                r = get(f"/api/v51/timeseries?query={quote(q)}")
            except Exception:
                continue
            agg = {}
            for item in r.get("items", []):
                for ts in item.get("timeSeries", []):
                    host = ts["metadata"].get("entityName","?")
                    metric = ts["metadata"]["metricName"]
                    data = ts.get("data", [])
                    if data:
                        agg.setdefault(host,{})[metric] = data[-1]["value"]
            for h, m in sorted(agg.items()):
                t = m.get("total_disk_space_across_disks") or m.get("bytes_total_across_disks")
                u = m.get("disk_space_used_across_disks")  or m.get("bytes_used_across_disks")
                if not t or u is None: continue
                pct = u/t*100
                verdict = "PASS" if pct < 85 else ("WARN" if pct < 95 else "FAIL")
                disk_rows.append({"host":h, "mount":"(host aggregate)",
                                  "used_gb":round(u/GB,1), "total_gb":round(t/GB,1),
                                  "pct":round(pct,1), "verdict":verdict})
                disk_rows.append({"host":h, "mount":"(total all mounts)",
                                  "used_gb":round(u/GB,1), "total_gb":round(t/GB,1),
                                  "pct":round(pct,1), "verdict":verdict})
            if disk_rows: break
except Exception as e:
    log(f"disk probe: {e}")

# ---------------------------------------------------------------------------
# Phase E: Discover endpoints + kerberos status
# ---------------------------------------------------------------------------
kerberos_enabled = False
auto_tls_enabled = False
try:
    cmcfg = get("/api/v51/cm/config?view=full")
    for i in cmcfg.get("items", []):
        n = i.get("name",""); v = i.get("value")
        if n in ("SECURITY_REALM","KDC_HOST") and v:
            kerberos_enabled = True
        if n in ("AUTO_TLS_TYPE","AGENT_TLS","NEED_AGENT_VALIDATION") and v not in (None,"NONE","false","disabled"):
            auto_tls_enabled = True
except Exception: pass

all_hosts = {h["hostId"]:h for h in get("/api/v51/hosts").get("items", [])}
ip_by_hostname = {h["hostname"]: h["ipAddress"] for h in all_hosts.values()}

def role_hosts(svc, rt):
    try:
        roles = get(f"/api/v51/clusters/{CLUSTER}/services/{svc}/roles").get("items", [])
        return [all_hosts[r["hostRef"]["hostId"]]["hostname"]
                for r in roles if r["type"]==rt and r["hostRef"]["hostId"] in all_hosts]
    except Exception:
        return []

endpoints = {
    "NN":   role_hosts("hdfs","NAMENODE"),
    "SHS":  role_hosts("spark3_on_yarn","SPARK3_YARN_HISTORY_SERVER") or role_hosts("spark_on_yarn","SPARK_YARN_HISTORY_SERVER"),
    "JHS":  role_hosts("yarn","JOBHISTORY"),
    "RM":   role_hosts("yarn","RESOURCEMANAGER"),
    "HS2":  role_hosts("hive_on_tez","HIVESERVER2") or role_hosts("hive","HIVESERVER2"),
    "HMS":  role_hosts("hive","HIVEMETASTORE"),
    "IMPALAD": role_hosts("impala","IMPALAD"),
    "KAFKA": role_hosts("kafka","KAFKA_BROKER"),
    "HBM":  role_hosts("hbase","MASTER"),
    "HBRS": role_hosts("hbase","REGIONSERVER"),
    "ZK":   role_hosts("zookeeper","SERVER"),
    "SOLR": role_hosts("solr","SOLR_SERVER"),
    "KNOX": role_hosts("knox","KNOX_GATEWAY"),
    "HUE":  role_hosts("hue","HUE_SERVER"),
    "RANGER": role_hosts("ranger","RANGER_ADMIN"),
    "ATLAS":  role_hosts("atlas","ATLAS_SERVER"),
    "OZONE_OM": role_hosts("ozone","OZONE_MANAGER"),
    "OZONE_S3G": role_hosts("ozone","S3_GATEWAY"),
    "NIFI": role_hosts("nifi","NIFI_NODE"),
    "NIFIREG": role_hosts("nifiregistry","NIFI_REGISTRY_SERVER"),
}

# Per-service web UI URLs. These are the canonical CDP ports for a wire-
# encrypted (TLS) deployment. Where a service has no native web UI, link to
# its CM management page.
def _first(hs): return hs[0] if hs else None
def _u(host, port, scheme="https", path=""): return f"{scheme}://{host}:{port}{path}" if host else None
service_urls = {}
def addurl(svc, label, url):
    if url: service_urls.setdefault(svc, []).append({"label":label,"url":url})

cm_base_ext = f"https://{CM_HOST}:{CM_PORT}"
addurl("hdfs",       "NameNode UI",     _u(_first(endpoints["NN"]),   9871))
addurl("yarn",       "ResourceManager", _u(_first(endpoints["RM"]),   8090))
addurl("yarn",       "JobHistory",      _u(_first(endpoints["JHS"]),  19890))
addurl("hbase",      "Master UI",       _u(_first(endpoints["HBM"]),  16010))
addurl("hive",       "HMS (CM)",        f"{cm_base_ext}/cmf/services?clusterName={CLUSTER}")
addurl("hive_on_tez","HiveServer2 UI",  _u(_first(endpoints["HS2"]),  10002))
addurl("impala",     "Impala Daemon UI",_u(_first(endpoints["IMPALAD"]), 25000))
addurl("hue",        "Hue",             _u(_first(endpoints["HUE"]),  8889))
addurl("knox",       "Gateway",         _u(_first(endpoints["KNOX"]), 8443, path="/gateway/admin/"))
addurl("ranger",     "Ranger Admin",    _u(_first(endpoints["RANGER"]), 6182))
addurl("atlas",      "Atlas",           _u(_first(endpoints["ATLAS"]), 31443))
addurl("solr",       "Solr",            _u(_first(endpoints["SOLR"]), 8985, path="/solr/"))
addurl("ozone",      "Ozone Manager",   _u(_first(endpoints["OZONE_OM"]), 9875))
addurl("ozone",      "S3 Gateway",      _u(_first(endpoints["OZONE_S3G"]), 9879))
addurl("nifi",       "NiFi",            _u(_first(endpoints["NIFI"]), 8443, path="/nifi/"))
addurl("nifiregistry","NiFi Registry",  _u(_first(endpoints["NIFIREG"]), 18433, path="/nifi-registry/"))
addurl("spark3_on_yarn","Spark History",_u(_first(endpoints["SHS"]),  18489))
addurl("kafka",      "Kafka (CM)",      f"{cm_base_ext}/cmf/services?clusterName={CLUSTER}")
addurl("zookeeper",  "ZooKeeper (CM)",  f"{cm_base_ext}/cmf/services?clusterName={CLUSTER}")
addurl("phoenix",    "Phoenix (CM)",    f"{cm_base_ext}/cmf/services?clusterName={CLUSTER}")
addurl("dataviz",    "Data Viz",        _u(_first(endpoints["NN"]),   38443))  # best-effort
addurl("tez",        "Tez (CM)",        f"{cm_base_ext}/cmf/services?clusterName={CLUSTER}")
addurl("core_settings","Core Settings (CM)", f"{cm_base_ext}/cmf/services?clusterName={CLUSTER}")
addurl("iceberg_replication","Iceberg Replication (CM)", f"{cm_base_ext}/cmf/services?clusterName={CLUSTER}")

# Attach url list onto each service result
for s in svc_results:
    s["urls"] = service_urls.get(s["name"], [])

# ---------------------------------------------------------------------------
# Phase F: CLI-ish tests — CM commands + TCP probes + ZK ruok
# ---------------------------------------------------------------------------
cli_tests = []
def rec(service, test, verdict, detail, elapsed=None):
    cli_tests.append({"service":service,"test":test,"verdict":verdict,
                      "detail":detail,"elapsed_s": round(elapsed,2) if elapsed else None})

def run_cm_cmd(svc, cmd, cluster=CLUSTER):
    t0=time.time()
    try:
        r = api(f"/api/v51/clusters/{cluster}/services/{svc}/commands/{cmd}", method="POST")
        if r.status_code >= 400:
            return "FAIL", f"HTTP {r.status_code} {r.text[:200]}", time.time()-t0
        cid = r.json().get("id")
        if not cid: return "FAIL", "no cmd id", time.time()-t0
        while time.time()-t0 < 180:
            c = get(f"/api/v51/commands/{cid}")
            if not c.get("active"):
                ok = c.get("success", False)
                return ("PASS" if ok else "FAIL"), f"cmd_id={cid} {c.get('resultMessage','')}"[:300], time.time()-t0
            time.sleep(3)
        return "FAIL", f"cmd {cid} timeout", time.time()-t0
    except Exception as e:
        return "FAIL", f"{e}", time.time()-t0

def tcp_probe(host, port, timeout=5):
    t0=time.time()
    try:
        ip = ip_by_hostname.get(host, host)
        s = socket.create_connection((ip,port),timeout=timeout); s.close()
        return "PASS", f"{host}({ip}):{port} reachable", time.time()-t0
    except Exception as e:
        return "FAIL", f"{host}:{port} err={e}", time.time()-t0

def zk_ruok(host, port=2181):
    t0=time.time()
    try:
        ip = ip_by_hostname.get(host, host)
        s = socket.create_connection((ip,port),timeout=5)
        s.sendall(b"ruok"); data=s.recv(16); s.close()
        resp = data.decode(errors="ignore").strip()
        if resp == "imok": return "PASS", f"{host}:{port} -> imok", time.time()-t0
        return "WARN", f"{host}:{port} ruok returned '{resp}'", time.time()-t0
    except Exception as e:
        return "FAIL", f"{host}:{port} err={e}", time.time()-t0

# Safe idempotent CM commands (create dirs)
for svc, cmd in (("hdfs","hdfsCreateTmpDir"),
                  ("hive","hiveCreateHiveWarehouse"),
                  ("hive","hiveCreateHiveUserDir")):
    v,d,t = run_cm_cmd(svc, cmd); rec(svc.upper(), f"{cmd} (CM-invoked)", v, d, t)

# Port probes (TLS-first where relevant)
probes = []
for h in endpoints["NN"]:     probes += [(h, 9871, "HDFS", "NameNode HTTPS"), (h, 8020, "HDFS", "NameNode RPC")]
for h in endpoints["RM"]:     probes += [(h, 8090, "YARN", "RM HTTPS"),       (h, 8032, "YARN", "RM Scheduler")]
for h in endpoints["HS2"]:    probes += [(h, 10000,"Hive", "HS2 Thrift")]
for h in endpoints["IMPALAD"]:probes += [(h, 21050,"Impala","impalad HS2")]
for h in endpoints["KAFKA"]:  probes += [(h, 9093, "Kafka", "Broker SSL"), (h, 9092, "Kafka", "Broker PLAINTEXT")]
for h in endpoints["HBM"]:    probes += [(h, 16000,"HBase","Master RPC"), (h, 16010,"HBase","Master Web UI")]
for h in endpoints["HBRS"]:   probes += [(h, 16020,"HBase","RegionServer")]
for h in endpoints["SOLR"]:   probes += [(h, 8985, "Solr","Solr TLS")]
for h in endpoints["KNOX"]:   probes += [(h, 8443, "Knox","Gateway")]
for h in endpoints["HUE"]:    probes += [(h, 8889, "Hue","Hue")]
for h in endpoints["RANGER"]: probes += [(h, 6182, "Ranger","Admin HTTPS")]
for h in endpoints["ATLAS"]:  probes += [(h, 31443,"Atlas","Atlas HTTPS")]
for h in endpoints["OZONE_OM"]: probes += [(h, 9875,"Ozone","OM HTTPS")]
for h in endpoints["NIFI"]:   probes += [(h, 8443, "NiFi","NiFi HTTPS")]

for host, port, svc, label in probes:
    v,d,t = tcp_probe(host, port)
    rec(svc, f"{label} ({host.split('.')[0]}:{port})", v, d, t)

for h in endpoints["ZK"]:
    v,d,t = zk_ruok(h, 2181)
    rec("ZooKeeper", f"ruok @ {h.split('.')[0]}:2181", v, d, t)

# ---------------------------------------------------------------------------
# Phase G: Reachability checks (runner ↔ CM, DNS forward/reverse)
# ---------------------------------------------------------------------------
reachability = []
def reach(check, verdict, detail=""):
    reachability.append({"check":check, "verdict":verdict, "detail":detail})

# CM HTTPS
v,d,_ = tcp_probe(CM_HOST, CM_PORT)
reach(f"Test runner → CM {CM_PORT} (HTTPS)", v, d)
# CM HTTP (default 7180) — usually redirects to HTTPS in TLS-on clusters
v,d,_ = tcp_probe(CM_HOST, 7180)
reach("Test runner → CM 7180 (HTTP redirect)", v, d)
# Reverse DNS for CM IP
import socket as _sk
try:
    rev = _sk.gethostbyaddr(CM_HOST)[0]
    reach("VPC DNS reverse lookup of CM IP", "PASS", f"{CM_HOST} → {rev}")
except Exception as e:
    reach("VPC DNS reverse lookup of CM IP", "FAIL", f"{e}")
# Forward DNS for any cluster hostname (if VPC has no PHZ for it, this fails
# but we mitigate by /etc/hosts on the runner)
sample_host = next((h for h in ip_by_hostname.keys() if "." in h), None)
if sample_host:
    try:
        ip = _sk.gethostbyname(sample_host)
        reach(f"Forward DNS for cluster hostname ({sample_host})", "PASS", f"{sample_host} → {ip}")
    except Exception as e:
        reach(f"Forward DNS for cluster hostname ({sample_host})", "SKIP",
              "no Private Hosted Zone for this domain in the VPC; injected /etc/hosts on runner only")

# ---------------------------------------------------------------------------
# Emit
# ---------------------------------------------------------------------------
out = {
    "cm": {"api_version": api_version, "cm_version": cm_v.get("version"),
           "cdp_version": target.get("fullVersion"),
           "cluster_name": target["name"],
           "data_services_cluster": data_services_cluster,
           "data_services_version": data_services_version,
           "kerberos_enabled": kerberos_enabled,
           "auto_tls_enabled": auto_tls_enabled},
    "base_cluster_services": svc_results,
    "data_services": ds_results,
    "hosts": hosts_out,
    "disks": disk_rows,
    "cli_tests": cli_tests,
    "reachability": reachability,
    "endpoints": endpoints,
}
print("---PROBE-JSON-START---")
print(json.dumps(out, default=str))
print("---PROBE-JSON-END---")
