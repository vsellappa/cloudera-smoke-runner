"""Output builders: Nagios, Prometheus text exposition, HTML."""
from __future__ import annotations
import datetime as dt
import html as _h
import json
from typing import Tuple

def _esc_label(v: str) -> str:
    return str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

def _verdict_counts(r: dict) -> dict:
    c = {"PASS":0,"WARN":0,"FAIL":0,"SKIP":0}
    for s in r.get("base_cluster_services", []): c[s["verdict"]] = c.get(s["verdict"],0)+1
    for s in r.get("data_services", []):
        v = s.get("verdict")
        if v: c[v] = c.get(v,0)+1
    for h in r.get("hosts", []):        c[h["verdict"]] = c.get(h["verdict"],0)+1
    for d in r.get("disks", []):
        # only count totals (one per host) so we don't double-count mounts
        if d.get("mount") == "(total all mounts)":
            c[d["verdict"]] = c.get(d["verdict"],0)+1
    for t in r.get("cli_tests", []):    c[t["verdict"]] = c.get(t["verdict"],0)+1
    return c

# ---------------------------------------------------------------------------
# Nagios plugin output: single line + perfdata + exit code 0/1/2/3
# ---------------------------------------------------------------------------
def nagios(r: dict) -> Tuple[int, str]:
    c = _verdict_counts(r)
    run = r.get("run", {})
    cluster = run.get("cluster","?")
    # any FAIL → CRIT; any WARN → WARN; else OK
    if c["FAIL"] > 0:
        code, status = 2, "CRITICAL"
    elif c["WARN"] > 0:
        code, status = 1, "WARNING"
    else:
        code, status = 0, "OK"
    max_cpu = max((h.get("cpu_pct") or 0) for h in r.get("hosts", [])) if r.get("hosts") else 0
    max_mem = max((h.get("mem_pct") or 0) for h in r.get("hosts", [])) if r.get("hosts") else 0
    svc_total = len(r.get("base_cluster_services", []))
    svc_ok    = sum(1 for s in r.get("base_cluster_services", []) if s["verdict"]=="PASS")
    ds_total  = len(r.get("data_services", []))
    ds_ok     = sum(1 for s in r.get("data_services", []) if s.get("verdict")=="PASS")
    cli_total = len(r.get("cli_tests", []))
    cli_ok    = sum(1 for t in r.get("cli_tests", []) if t["verdict"]=="PASS")

    # human-readable failures (up to 3 for brevity)
    failing = []
    for s in r.get("base_cluster_services", []):
        if s["verdict"] in ("FAIL","WARN"):
            failing.append(f"{s['name']}={s['verdict']}")
    for s in r.get("data_services", []):
        if s.get("verdict") in ("FAIL","WARN"):
            failing.append(f"ds/{s['name']}={s['verdict']}")
    for h in r.get("hosts", []):
        if h["verdict"] in ("FAIL","WARN"):
            failing.append(f"host/{h['host'].split('.')[0]}={h['verdict']}")
    issues = ("; issues=[" + ",".join(failing[:6]) + "]") if failing else ""
    summary = f"{status} - Cloudera {cluster}: {svc_ok}/{svc_total} base, {ds_ok}/{ds_total} data-svc, cli {cli_ok}/{cli_total}{issues}"
    # perfdata: "label=value[UOM];warn;crit;min;max"
    perf = " ".join([
        f"services_ok={svc_ok};;;0;{svc_total}",
        f"services_warn={c['WARN']};;;0;",
        f"services_fail={c['FAIL']};;;0;",
        f"cli_ok={cli_ok};;;0;{cli_total}",
        f"cli_fail={sum(1 for t in r.get('cli_tests',[]) if t['verdict']=='FAIL')};;;0;",
        f"host_cpu_max={max_cpu:.1f}%;85;95;0;100",
        f"host_mem_max={max_mem:.1f}%;85;95;0;100",
    ])
    return code, f"{summary} | {perf}"

# ---------------------------------------------------------------------------
# Prometheus text exposition format
# ---------------------------------------------------------------------------
def prometheus(r: dict) -> str:
    run = r.get("run", {})
    cluster = run.get("cluster","?")
    env     = run.get("env","?")
    lines = []
    lines.append("# HELP smoke_service_healthy 1 if CM reports service health GOOD and state STARTED/NA")
    lines.append("# TYPE smoke_service_healthy gauge")
    for s in r.get("base_cluster_services", []):
        val = 1 if s["verdict"]=="PASS" else 0
        lines.append(f'smoke_service_healthy{{cluster="{_esc_label(cluster)}",env="{_esc_label(env)}",service="{_esc_label(s["name"])}",type="{_esc_label(s["type"])}"}} {val}')
    lines.append("# HELP smoke_data_service_healthy 1 if Data Services (PvC) service is GOOD")
    lines.append("# TYPE smoke_data_service_healthy gauge")
    for s in r.get("data_services", []):
        if "name" not in s: continue
        val = 1 if s.get("verdict")=="PASS" else 0
        lines.append(f'smoke_data_service_healthy{{cluster="{_esc_label(cluster)}",env="{_esc_label(env)}",service="{_esc_label(s["name"])}"}} {val}')
    lines.append("# HELP smoke_host_cpu_percent Host CPU %")
    lines.append("# TYPE smoke_host_cpu_percent gauge")
    lines.append("# HELP smoke_host_memory_percent Host memory %")
    lines.append("# TYPE smoke_host_memory_percent gauge")
    for h in r.get("hosts", []):
        hn = h["host"]
        if h.get("cpu_pct") is not None:
            lines.append(f'smoke_host_cpu_percent{{cluster="{_esc_label(cluster)}",env="{_esc_label(env)}",host="{_esc_label(hn)}"}} {h["cpu_pct"]}')
        if h.get("mem_pct") is not None:
            lines.append(f'smoke_host_memory_percent{{cluster="{_esc_label(cluster)}",env="{_esc_label(env)}",host="{_esc_label(hn)}"}} {h["mem_pct"]}')
    lines.append("# HELP smoke_cli_test_result 1 PASS, 0 FAIL/WARN")
    lines.append("# TYPE smoke_cli_test_result gauge")
    for t in r.get("cli_tests", []):
        val = 1 if t["verdict"]=="PASS" else 0
        lines.append(f'smoke_cli_test_result{{cluster="{_esc_label(cluster)}",env="{_esc_label(env)}",service="{_esc_label(t["service"])}",test="{_esc_label(t["test"])}"}} {val}')
    c = _verdict_counts(r)
    lines.append("# HELP smoke_tests_total Count of tests by verdict")
    lines.append("# TYPE smoke_tests_total gauge")
    for v, n in c.items():
        lines.append(f'smoke_tests_total{{cluster="{_esc_label(cluster)}",env="{_esc_label(env)}",verdict="{v}"}} {n}')
    lines.append("")
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# HTML — standalone, no external deps
# ---------------------------------------------------------------------------
def html_report(r: dict) -> str:
    run = r.get("run", {})
    c = _verdict_counts(r)
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    def badge(v):
        col = {"PASS":"#16a34a","WARN":"#ca8a04","FAIL":"#dc2626","SKIP":"#6b7280","INFO":"#2563eb"}.get(v,"#475569")
        return f'<span style="background:{col};color:#fff;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600">{_h.escape(v)}</span>'
    def svc_row(s):
        roles_s = "<br>".join(f"{k}: {v['started']}/{v['count']} STARTED, {v['good']}/{v['count']} GOOD"
                              for k,v in s.get("roles",{}).items() if isinstance(v,dict))
        urls_s = "<br>".join(
            f'<a href="{_h.escape(u["url"])}" target="_blank" rel="noopener" style="color:#2563eb;text-decoration:none">{_h.escape(u["label"])}</a>'
            for u in s.get("urls", []) if u.get("url")
        ) or '<span style="color:#94a3b8;font-size:11px">—</span>'
        return f"<tr><td>{_h.escape(s['name'])}</td><td>{_h.escape(s.get('type',''))}</td><td>{_h.escape(s['state'])}</td><td>{_h.escape(s['health'])}</td><td>{badge(s['verdict'])}</td><td>{urls_s}</td><td>{roles_s}</td></tr>"
    def disk_row(d):
        used = d.get("used_gb"); tot = d.get("total_gb"); pct = d.get("pct")
        col = "#16a34a" if (pct is not None and pct<85) else ("#ca8a04" if (pct is not None and pct<95) else "#dc2626")
        bar = ""
        if pct is not None:
            bar = (f'<div style="background:#e5e7eb;border-radius:3px;width:120px;height:12px;position:relative">'
                   f'<div style="background:{col};width:{min(pct,100):.1f}%;height:100%;border-radius:3px"></div>'
                   f'<span style="position:absolute;top:-3px;left:130px;font-size:11px">{pct:.1f}%</span></div>')
        used_total = f"{used:.1f} / {tot:.1f} GB" if (used is not None and tot is not None) else "—"
        is_total_row = (d.get("mount") == "(total all mounts)")
        weight = "font-weight:600;background:#f8fafc" if is_total_row else ""
        return f'<tr style="{weight}"><td>{_h.escape(d["host"])}</td><td><code style="font-size:11px">{_h.escape(d["mount"])}</code></td><td>{used_total}</td><td>{bar}</td><td>{badge(d["verdict"])}</td></tr>'
    def ds_row(s):
        if "name" not in s:
            return f"<tr><td colspan=7>{_h.escape(s.get('error',''))}</td></tr>"
        fcs = "<br>".join(f"{f['name']} ({f['summary']})" for f in s.get("failed_checks", []))
        roles_s = "<br>".join(f"{k}: {v['started']}/{v['count']} STARTED, {v['good']}/{v['count']} GOOD"
                              for k,v in s.get("roles",{}).items() if isinstance(v,dict))
        urls_s = "<br>".join(
            f'<a href="{_h.escape(u["url"])}" target="_blank" rel="noopener" style="color:#2563eb;text-decoration:none">{_h.escape(u["label"])}</a>'
            for u in s.get("urls", []) if u.get("url")
        ) or '<span style="color:#94a3b8;font-size:11px">—</span>'
        return f"<tr><td>{_h.escape(s['name'])}</td><td>{_h.escape(s.get('type',''))}</td><td>{_h.escape(s['state'])}</td><td>{_h.escape(s['health'])}</td><td>{badge(s['verdict'])}</td><td>{urls_s}</td><td>{roles_s}{('<br><br><b>Failed checks:</b><br>'+fcs) if fcs else ''}</td></tr>"
    def host_row(h):
        def bar(v):
            if v is None: return "n/a"
            col = "#16a34a" if v<85 else ("#ca8a04" if v<95 else "#dc2626")
            return f'<div style="background:#e5e7eb;border-radius:3px;width:120px;height:12px;position:relative"><div style="background:{col};width:{min(v,100):.1f}%;height:100%;border-radius:3px"></div><span style="position:absolute;top:-3px;left:130px;font-size:11px">{v:.1f}%</span></div>'
        return f"<tr><td>{_h.escape(h['host'])}</td><td>{bar(h.get('cpu_pct'))}</td><td>{bar(h.get('mem_pct'))}</td><td>{badge(h['verdict'])}</td></tr>"
    def cli_row(t):
        return f"<tr><td>{_h.escape(t['service'])}</td><td>{_h.escape(t['test'])}</td><td>{badge(t['verdict'])}</td><td><code style='font-size:11px'>{_h.escape(t['detail'])}</code></td></tr>"

    cm = r.get("cm", {})
    ds_cluster = cm.get("data_services_cluster")
    ds_version = cm.get("data_services_version")
    ds_subtitle = (f' · Data Services: <b>{_h.escape(ds_cluster)}</b>'
                   + (f' (PvC {_h.escape(ds_version)})' if ds_version else '')) if ds_cluster else ''
    n_svc = len(r.get("base_cluster_services", []))
    # Headline summary text — derived
    headline_bits = []
    fail_n = c.get("FAIL", 0); warn_n = c.get("WARN", 0)
    if fail_n == 0 and warn_n == 0:
        headline_bits.append(f"<b>{_h.escape(cm.get('cluster_name',''))}</b> is fully healthy — all {n_svc} services STARTED + GOOD.")
    elif fail_n == 0:
        headline_bits.append(f"<b>{_h.escape(cm.get('cluster_name',''))}</b> is operational with {warn_n} WARN(s).")
    else:
        headline_bits.append(f"<b>{_h.escape(cm.get('cluster_name',''))}</b> reports <b>{fail_n} FAIL(s)</b> and {warn_n} WARN(s) — investigate.")
    if r.get("hosts"):
        max_cpu = max((h.get("cpu_pct") or 0) for h in r["hosts"])
        max_mem = max((h.get("mem_pct") or 0) for h in r["hosts"])
        headline_bits.append(f"Host CPU max {max_cpu:.1f}%, RAM max {max_mem:.1f}%.")
    # Disk roll-ups (per-host totals only, to avoid double-counting mounts)
    disk_totals = [d for d in r.get("disks",[]) if d.get("mount") == "(total all mounts)"]
    disk_peak_pct  = max((d.get("pct") or 0) for d in disk_totals) if disk_totals else None
    cluster_disk_used  = sum((d.get("used_gb")  or 0) for d in disk_totals) if disk_totals else 0
    cluster_disk_total = sum((d.get("total_gb") or 0) for d in disk_totals) if disk_totals else 0
    cluster_disk_pct   = (cluster_disk_used/cluster_disk_total*100) if cluster_disk_total else None
    if disk_totals:
        headline_bits.append(
            f"Cluster disk: {cluster_disk_used:,.0f} / {cluster_disk_total:,.0f} GB used "
            f"({cluster_disk_pct:.1f}%); peak host {disk_peak_pct:.1f}%."
        )
    if cm.get("kerberos_enabled"):
        headline_bits.append("Kerberos: ENABLED.")
    headline = " ".join(headline_bits)
    # KPI helpers for disk
    def _kpi_class(pct):
        if pct is None: return "info"
        return "pass" if pct < 85 else ("warn" if pct < 95 else "fail")
    disk_kpi_cls   = _kpi_class(cluster_disk_pct)
    disk_peak_cls  = _kpi_class(disk_peak_pct)

    cli_total = len(r.get("cli_tests", []))
    cli_pass  = sum(1 for t in r.get("cli_tests",[]) if t["verdict"]=="PASS")
    cli_fail  = sum(1 for t in r.get("cli_tests",[]) if t["verdict"]=="FAIL")
    skip_n = c.get("SKIP", 0)

    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>Cloudera Smoke Report — {_h.escape(run.get('cluster',''))} — {_h.escape(run.get('ts',''))}</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;margin:0;background:#f8fafc;color:#1e293b}}
.container{{max-width:1200px;margin:0 auto;padding:24px}}
.header-bar{{background:#0f172a;color:#f1f5f9;padding:20px 24px}}
.header-bar .subtitle{{color:#94a3b8;font-size:14px}}
h1{{margin:0 0 4px;font-size:26px}}
h2{{margin:28px 0 12px;font-size:20px;border-bottom:2px solid #e2e8f0;padding-bottom:6px}}
table{{width:100%;border-collapse:collapse;background:#fff;box-shadow:0 1px 2px rgba(0,0,0,0.05);border-radius:6px;overflow:hidden;font-size:13px}}
th{{background:#f1f5f9;text-align:left;padding:10px;font-weight:600;font-size:12px;color:#475569;text-transform:uppercase;letter-spacing:0.04em}}
td{{padding:9px 10px;border-top:1px solid #e2e8f0;vertical-align:top}}
.kpis{{display:flex;gap:12px;margin:16px 0;flex-wrap:wrap}}
.kpi{{background:#fff;padding:14px 18px;border-radius:8px;box-shadow:0 1px 2px rgba(0,0,0,0.05);min-width:120px}}
.kpi .label{{font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.05em}}
.kpi .val{{font-size:28px;font-weight:700;margin-top:2px}}
.kpi.pass .val{{color:#16a34a}}.kpi.warn .val{{color:#ca8a04}}.kpi.fail .val{{color:#dc2626}}
.kpi.skip .val{{color:#6b7280}}.kpi.info .val{{color:#2563eb}}
.info{{background:#dbeafe;border-left:4px solid #2563eb;padding:10px 14px;margin:12px 0;border-radius:4px;font-size:13px}}
.note{{background:#fef3c7;border-left:4px solid #f59e0b;padding:10px 14px;margin:12px 0;border-radius:4px;font-size:13px}}
.meta{{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;font-size:13px;margin:8px 0}}
.meta .item{{background:#fff;padding:8px 10px;border-radius:4px;border:1px solid #e2e8f0}}
.meta .k{{color:#64748b;font-size:11px;text-transform:uppercase}}
.meta .v{{font-weight:600}}
code{{background:#f1f5f9;padding:1px 5px;border-radius:3px;font-family:"SF Mono",Menlo,Consolas,monospace}}
footer{{margin-top:40px;padding:20px;text-align:center;color:#94a3b8;font-size:12px}}
</style></head><body>
<div class="header-bar"><div class="container" style="padding:0">
  <h1>Cloudera Smoke &amp; Health Test Report</h1>
  <div class="subtitle">Environment: <b>{_h.escape(run.get('env',''))}</b> · Cluster: <b>{_h.escape(run.get('cluster',''))}</b> ({_h.escape(cm.get('cdp_version',''))}){ds_subtitle} · Report generated: {now}</div>
</div></div>
<div class="container">

<div class="meta">
  <div class="item"><div class="k">CM Host</div><div class="v">{_h.escape(run.get('cm_host',''))}:{_h.escape(str(run.get('cm_port','')))}</div></div>
  <div class="item"><div class="k">CM Version</div><div class="v">{_h.escape(cm.get('cm_version',''))} (API {_h.escape(cm.get('api_version',''))})</div></div>
  <div class="item"><div class="k">CDP Version</div><div class="v">{_h.escape(cm.get('cdp_version',''))} — {n_svc} services</div></div>
  <div class="item"><div class="k">Data Services</div><div class="v">{(_h.escape(ds_cluster)+' · PvC '+_h.escape(ds_version)) if ds_cluster and ds_version else (_h.escape(ds_cluster) if ds_cluster else '—')}</div></div>
  <div class="item"><div class="k">AWS</div><div class="v">{_h.escape(run.get('aws_account',''))} · {_h.escape(run.get('aws_region',''))}</div></div>
  <div class="item"><div class="k">Security</div><div class="v">Kerberos: {'ENABLED' if cm.get('kerberos_enabled') else 'DISABLED'} · AutoTLS: {'ENABLED' if cm.get('auto_tls_enabled') else 'DISABLED'}</div></div>
</div>

<h2>Overall Verdict</h2>
<div class="kpis">
  <div class="kpi pass"><div class="label">PASS</div><div class="val">{c.get('PASS',0)}</div></div>
  <div class="kpi warn"><div class="label">WARN</div><div class="val">{c.get('WARN',0)}</div></div>
  <div class="kpi fail"><div class="label">FAIL</div><div class="val">{c.get('FAIL',0)}</div></div>
  <div class="kpi skip"><div class="label">Skipped</div><div class="val">{skip_n}</div></div>
  <div class="kpi info"><div class="label">CLI tests run</div><div class="val">{cli_total}</div></div>
  <div class="kpi {disk_kpi_cls}"><div class="label">Cluster Disk</div><div class="val" style="font-size:18px">{f'{cluster_disk_used:,.0f}/{cluster_disk_total:,.0f} GB' if cluster_disk_total else 'n/a'}</div><div style="font-size:12px;color:#64748b">{f'{cluster_disk_pct:.1f}% used' if cluster_disk_pct is not None else ''}</div></div>
  <div class="kpi {disk_peak_cls}"><div class="label">Disk Peak Host</div><div class="val">{f'{disk_peak_pct:.1f}%' if disk_peak_pct is not None else 'n/a'}</div></div>
</div>
<div class="info"><b>Summary:</b> {headline}</div>
<h2>1. {_h.escape(run.get('cluster',''))} — Service Health (via CM API)</h2>
<table><thead><tr><th>Service</th><th>Type</th><th>State</th><th>Health</th><th>Verdict</th><th>URLs</th><th>Roles</th></tr></thead>
<tbody>{''.join(svc_row(s) for s in r.get('base_cluster_services',[]))}</tbody></table>
<h2>2. Data Services{(' — '+_h.escape(ds_cluster)+' (PvC '+_h.escape(ds_version)+')') if ds_cluster and ds_version else (' — '+_h.escape(ds_cluster) if ds_cluster else '')}</h2>
<table><thead><tr><th>Service</th><th>Type</th><th>State</th><th>Health</th><th>Verdict</th><th>URLs</th><th>Roles / Failed Checks</th></tr></thead>
<tbody>{''.join(ds_row(s) for s in r.get('data_services',[])) or '<tr><td colspan=7>No Data Services cluster discovered.</td></tr>'}</tbody></table>
{('<div class="note"><b>Attention:</b> Data Services has issues — '+', '.join(s['name']+' ('+s['verdict']+')' for s in r.get('data_services',[]) if s.get('verdict') in ('WARN','FAIL'))+'. Drill in via CM UI &rarr; '+_h.escape(ds_cluster or '')+' &rarr; the affected service &rarr; Health Tests.</div>') if any(s.get('verdict') in ('WARN','FAIL') for s in r.get('data_services',[])) else ''}
<h2>3. Host Utilization (CPU / RAM — CM timeseries, last value)</h2>
<table><thead><tr><th>Host</th><th>CPU %</th><th>Memory %</th><th>Verdict</th></tr></thead>
<tbody>{''.join(host_row(h) for h in r.get('hosts',[]))}</tbody></table>
<h2>4. Disk Utilization (per host / mount — CM FILESYSTEM timeseries)</h2>
<table><thead><tr><th>Host</th><th>Mount</th><th>Used / Total</th><th>% Used</th><th>Verdict</th></tr></thead>
<tbody>{''.join(disk_row(d) for d in r.get('disks',[])) or '<tr><td colspan=5>No FILESYSTEM timeseries returned.</td></tr>'}</tbody></table>
<h2>5. CLI / Endpoint Tests</h2>
<table><thead><tr><th>Service</th><th>Test</th><th>Verdict</th><th>Detail</th></tr></thead>
<tbody>{''.join(cli_row(t) for t in r.get('cli_tests',[]))}</tbody></table>
<h2>6. Reachability</h2>
<table><thead><tr><th>Check</th><th>Result</th><th>Detail</th></tr></thead>
<tbody>{''.join(f'<tr><td>{_h.escape(x["check"])}</td><td>{badge(x["verdict"])}</td><td><span style="color:#64748b;font-size:12px">{_h.escape(x.get("detail",""))}</span></td></tr>' for x in r.get('reachability',[])) or '<tr><td colspan=3>No reachability checks recorded.</td></tr>'}</tbody></table>
<footer>Report run timestamp: {_h.escape(run.get('ts',''))} · AWS account {_h.escape(run.get('aws_account',''))} · Region {_h.escape(run.get('aws_region',''))}</footer>
</div></body></html>"""
