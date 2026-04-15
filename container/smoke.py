#!/usr/bin/env python3
"""
Cloudera CDP smoke/health orchestrator.

Reads parameters from environment (see run.sh). Launches an ephemeral EC2 inside
the cluster's VPC, runs remote_probe.py on it via SSM, collects results, emits
Nagios + Prometheus + JSON + HTML outputs, then tears down the runner.

Exit code: Nagios convention — 0=OK, 1=WARN, 2=CRIT, 3=UNKNOWN.
"""
from __future__ import annotations
import base64
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, TokenRetrievalError

import report

# ---------------------------------------------------------------------------
# Config (from env)
# ---------------------------------------------------------------------------
ENV_NAME         = os.environ.get("ENV_NAME", "unknown")
AWS_REGION       = os.environ["AWS_REGION"]
VPC_ID           = os.environ["VPC_ID"]
SUBNET_ID        = os.environ["SUBNET_ID"]
CM_HOST          = os.environ["CM_HOST"]
CM_PORT          = int(os.environ.get("CM_PORT", "7183"))
CM_USER          = os.environ.get("CM_USER", "admin")
def _load_cm_pass() -> str:
    v = os.environ.get("CM_PASS")
    if v: return v
    p = Path("/run/cm_pass")
    if p.is_file() and os.access(p, os.R_OK):
        return p.read_text().rstrip("\n")
    print("UNKNOWN - CM_PASS not provided (set env var or mount a readable /run/cm_pass)", file=sys.stderr)
    sys.exit(3)
CM_PASS          = _load_cm_pass()
CLUSTER_NAME     = os.environ["CLUSTER_NAME"]
SERVICES_TO_TEST = os.environ.get("SERVICES_TO_TEST", "all")
OUTPUT_DIR       = Path(os.environ.get("OUTPUT_DIR", "/output"))
OUTPUT_FORMATS   = set(f.strip() for f in os.environ.get("OUTPUT_FORMATS", "nagios,prom,json,html").split(",") if f.strip())

TS = dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
NAME_PREFIX = f"smoke-runner-{ENV_NAME}-{TS}"
TAGS = [
    {"Key": "ManagedBy", "Value": "smoke-runner"},
    {"Key": "Env",       "Value": ENV_NAME},
    {"Key": "Ephemeral", "Value": "true"},
    {"Key": "RunTs",     "Value": TS},
]

NAGIOS_OK, NAGIOS_WARN, NAGIOS_CRIT, NAGIOS_UNKNOWN = 0, 1, 2, 3

def log(msg: str):
    print(f"[{dt.datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------
try:
    session = boto3.Session(region_name=AWS_REGION)
    ec2  = session.client("ec2")
    iam  = session.client("iam")
    ssm  = session.client("ssm")
    sts  = session.client("sts")
    IDENT = sts.get_caller_identity()
except (NoCredentialsError, TokenRetrievalError) as e:
    print(f"UNKNOWN - AWS credentials unavailable: {e}", file=sys.stderr)
    print("Run 'aws sso login --profile <profile>' on the host, then re-run.", file=sys.stderr)
    sys.exit(NAGIOS_UNKNOWN)

log(f"Account {IDENT['Account']} — {IDENT['Arn'].split('/')[-1]}")

# ---------------------------------------------------------------------------
# Teardown registry — always runs, even on failure
# ---------------------------------------------------------------------------
_created = {"instance": None, "sg": None, "role": None, "profile": None, "eni_restored": False}

def teardown():
    log("=== Teardown ===")
    iid = _created.get("instance")
    if iid:
        try:
            ec2.terminate_instances(InstanceIds=[iid])
            ec2.get_waiter("instance_terminated").wait(InstanceIds=[iid])
            log(f"terminated {iid}")
        except ClientError as e:
            log(f"terminate err: {e}")
    prof = _created.get("profile")
    if prof:
        try: iam.remove_role_from_instance_profile(InstanceProfileName=prof, RoleName=prof)
        except ClientError: pass
        try:
            iam.delete_instance_profile(InstanceProfileName=prof)
            log(f"deleted instance profile {prof}")
        except ClientError as e:
            log(f"profile del err: {e}")
    role = _created.get("role")
    if role:
        try: iam.detach_role_policy(RoleName=role, PolicyArn="arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore")
        except ClientError: pass
        try:
            iam.delete_role(RoleName=role)
            log(f"deleted role {role}")
        except ClientError as e:
            log(f"role del err: {e}")
    sg = _created.get("sg")
    if sg:
        for attempt in range(5):
            try:
                ec2.delete_security_group(GroupId=sg)
                log(f"deleted sg {sg}")
                break
            except ClientError as e:
                log(f"sg del retry {attempt+1}: {e}")
                time.sleep(15)

# ---------------------------------------------------------------------------
# Phase 1: pre-flight
# ---------------------------------------------------------------------------
def preflight() -> dict:
    log("=== Phase 1: pre-flight ===")
    vpc  = ec2.describe_vpcs(VpcIds=[VPC_ID])["Vpcs"][0]
    subn = ec2.describe_subnets(SubnetIds=[SUBNET_ID])["Subnets"][0]
    if subn["VpcId"] != VPC_ID:
        raise SystemExit(f"CRITICAL - subnet {SUBNET_ID} not in VPC {VPC_ID}")

    # Find latest Amazon Linux 2023 AMI
    imgs = ec2.describe_images(
        Owners=["amazon"],
        Filters=[
            {"Name": "name",  "Values": ["al2023-ami-2023.*-kernel-*-x86_64"]},
            {"Name": "state", "Values": ["available"]},
        ],
    )["Images"]
    ami = sorted(imgs, key=lambda i: i["CreationDate"])[-1]
    log(f"VPC {VPC_ID} CIDR={vpc['CidrBlock']}  Subnet={SUBNET_ID} AZ={subn['AvailabilityZone']}")
    log(f"AMI (AL2023) {ami['ImageId']}")

    # Detect Cloudera SG(s): any SG referenced by instances in this VPC other than
    # default SGs. We just pick SGs that have CM_HOST's private IP as an instance.
    cm_sgs = []
    instances = ec2.describe_instances(
        Filters=[{"Name":"vpc-id","Values":[VPC_ID]},
                 {"Name":"private-ip-address","Values":[CM_HOST]}])
    for res in instances["Reservations"]:
        for inst in res["Instances"]:
            cm_sgs = [g["GroupId"] for g in inst["SecurityGroups"]]
    log(f"Cloudera SGs to attach to runner: {cm_sgs}")
    return {"vpc": vpc, "subnet": subn, "ami_id": ami["ImageId"], "cloudera_sgs": cm_sgs}

# ---------------------------------------------------------------------------
# Phase 2: launch runner
# ---------------------------------------------------------------------------
def launch_runner(pf: dict) -> str:
    log("=== Phase 2: launch runner ===")
    trust = json.dumps({"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]})
    iam.create_role(RoleName=NAME_PREFIX, AssumeRolePolicyDocument=trust,
                    Tags=[{"Key":t["Key"],"Value":t["Value"]} for t in TAGS])
    _created["role"] = NAME_PREFIX
    iam.attach_role_policy(RoleName=NAME_PREFIX, PolicyArn="arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore")
    iam.create_instance_profile(InstanceProfileName=NAME_PREFIX)
    _created["profile"] = NAME_PREFIX
    iam.add_role_to_instance_profile(InstanceProfileName=NAME_PREFIX, RoleName=NAME_PREFIX)

    sg = ec2.create_security_group(
        GroupName=f"{NAME_PREFIX}-sg",
        Description="Ephemeral smoke runner SG",
        VpcId=VPC_ID,
        TagSpecifications=[{"ResourceType":"security-group","Tags":TAGS}],
    )["GroupId"]
    _created["sg"] = sg
    log(f"sg={sg} role/profile={NAME_PREFIX}")

    user_data = r"""#!/bin/bash
set -e
exec > /var/log/user-data.log 2>&1
dnf install -y --skip-broken python3 python3-pip nmap-ncat bind-utils jq
pip3 install --quiet requests
touch /tmp/userdata.done
"""
    # IAM profile propagation is eventually consistent — retry run_instances
    # with exponential backoff on the "invalid IAM Instance Profile name" error.
    sg_list = [sg] + pf["cloudera_sgs"]
    run_args = dict(
        ImageId=pf["ami_id"],
        InstanceType="t3.micro",
        MinCount=1, MaxCount=1,
        SubnetId=SUBNET_ID,
        SecurityGroupIds=sg_list,
        IamInstanceProfile={"Name": NAME_PREFIX},
        UserData=user_data,
        MetadataOptions={"HttpTokens":"required","HttpEndpoint":"enabled"},
        TagSpecifications=[{"ResourceType":"instance",
                            "Tags":TAGS+[{"Key":"Name","Value":NAME_PREFIX}]}],
    )
    inst = None
    delay = 5
    deadline_p = time.time() + 120  # up to 2 min for propagation
    while True:
        try:
            inst = ec2.run_instances(**run_args)["Instances"][0]
            break
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            msg  = e.response.get("Error", {}).get("Message", "")
            transient = (code in ("InvalidParameterValue","InvalidInstanceProfile.NotFound")
                         and "IAM Instance Profile" in msg)
            if transient and time.time() < deadline_p:
                log(f"waiting for IAM instance profile propagation ({delay}s): {msg}")
                time.sleep(delay)
                delay = min(delay * 2, 20)
                continue
            raise
    iid = inst["InstanceId"]
    _created["instance"] = iid
    log(f"launched {iid}, waiting for SSM Online")

    # Poll SSM up to 8 minutes
    deadline = time.time() + 480
    while time.time() < deadline:
        r = ssm.describe_instance_information(Filters=[{"Key":"InstanceIds","Values":[iid]}])
        if r["InstanceInformationList"] and r["InstanceInformationList"][0]["PingStatus"] == "Online":
            log(f"{iid} SSM Online")
            return iid
        time.sleep(15)
    raise SystemExit(f"CRITICAL - {iid} did not become SSM Online in time")

# ---------------------------------------------------------------------------
# Phase 3+4: ship probe, run, collect
# ---------------------------------------------------------------------------
def run_probe(iid: str) -> dict:
    log("=== Phase 3+4: remote probe ===")
    probe = Path("/app/remote_probe.py").read_bytes()
    probe_b64 = base64.b64encode(probe).decode()

    # Build environment for the probe
    probe_env = {
        "CM_HOST": CM_HOST, "CM_PORT": str(CM_PORT),
        "CM_USER": CM_USER, "CM_PASS": CM_PASS,
        "CLUSTER_NAME": CLUSTER_NAME,
        "SERVICES_TO_TEST": SERVICES_TO_TEST,
    }
    env_b64 = base64.b64encode(json.dumps(probe_env).encode()).decode()

    cmd = (
        "set -e; "
        f"echo {probe_b64} | base64 -d > /tmp/probe.py; "
        f"echo {env_b64}   | base64 -d > /tmp/probe_env.json; "
        # Wait for userdata so requests/python are installed
        "for i in $(seq 1 40); do [ -f /tmp/userdata.done ] && break; sleep 5; done; "
        "python3 /tmp/probe.py < /tmp/probe_env.json"
    )

    resp = ssm.send_command(
        InstanceIds=[iid],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [cmd]},
        TimeoutSeconds=1800,
        Comment=f"smoke-probe {TS}",
    )
    cid = resp["Command"]["CommandId"]
    log(f"command {cid} sent")
    deadline = time.time() + 1800
    while time.time() < deadline:
        time.sleep(10)
        try:
            inv = ssm.get_command_invocation(CommandId=cid, InstanceId=iid)
        except ClientError:
            continue
        if inv["Status"] not in ("Pending","InProgress","Delayed"):
            break
    out = inv.get("StandardOutputContent","")
    err = inv.get("StandardErrorContent","")
    if inv["Status"] != "Success":
        log(f"probe status={inv['Status']}")
        if err: log(f"stderr tail: {err[-500:]}")
    # Result JSON is the last valid JSON object in stdout (framed by markers)
    start = out.find("---PROBE-JSON-START---")
    end   = out.find("---PROBE-JSON-END---")
    if start < 0 or end < 0:
        raise SystemExit(f"CRITICAL - probe did not emit result JSON. stdout tail:\n{out[-1000:]}")
    result = json.loads(out[start+len("---PROBE-JSON-START---"):end].strip())
    result["ssm_command_id"] = cid
    result["ssm_status"] = inv["Status"]
    return result

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    try:
        pf = preflight()
        iid = launch_runner(pf)
        result = run_probe(iid)
    except SystemExit:
        teardown()
        raise
    except Exception as e:
        log(f"ERROR: {e}")
        teardown()
        print(f"UNKNOWN - orchestrator exception: {e}")
        return NAGIOS_UNKNOWN

    # Augment result with run metadata
    result["run"] = {
        "ts": TS, "env": ENV_NAME, "cluster": CLUSTER_NAME,
        "aws_region": AWS_REGION, "aws_account": IDENT["Account"],
        "cm_host": CM_HOST, "cm_port": CM_PORT,
    }

    teardown()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    base = OUTPUT_DIR / f"smoke-{ENV_NAME}-{TS}"
    nagios_code, nagios_line = report.nagios(result)

    if "json"   in OUTPUT_FORMATS:
        (base.with_suffix(".json")).write_text(json.dumps(result, indent=2))
        log(f"wrote {base}.json")
    if "prom"   in OUTPUT_FORMATS:
        (base.with_suffix(".prom")).write_text(report.prometheus(result))
        log(f"wrote {base}.prom")
    if "html"   in OUTPUT_FORMATS:
        (base.with_suffix(".html")).write_text(report.html_report(result))
        log(f"wrote {base}.html")
    if "nagios" in OUTPUT_FORMATS:
        (base.with_suffix(".nagios")).write_text(nagios_line + "\n")
        log(f"wrote {base}.nagios")

    # Nagios stdout + exit code
    print(nagios_line)
    return nagios_code

if __name__ == "__main__":
    sys.exit(main())
