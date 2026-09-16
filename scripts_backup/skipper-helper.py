#!/usr/bin/env python3
"""dfly-inspect: read-only Dragonfly Cloud investigation workflows."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys

DEFAULT_ENV = "prod"
DEFAULT_CONFIG_PARAMETER = "replica_delete_expired"
DEFAULT_GCX_CONTEXT = "dragonfly-prod"
UTC_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def parse_utc_timestamp(value: str) -> str:
    try:
        return (
            datetime.strptime(value, UTC_TIMESTAMP_FORMAT)
            .replace(tzinfo=timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%SZ")
        )
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"invalid UTC timestamp {value!r}; expected {UTC_TIMESTAMP_FORMAT!r}"
        ) from error


def get_logs(
    node_id: str,
    start: str,
    end: str,
    environment: str,
    context: str,
    limit: int,
    datastore_id: str | None,
    shard_id: str | None,
    output: Path | None,
) -> int:
    if not node_id.startswith("node_"):
        raise ValueError("node_id must begin with 'node_'")
    if datastore_id and not datastore_id.startswith("dst_"):
        raise ValueError("datastore_id must begin with 'dst_'")
    if shard_id and not shard_id.startswith("shard_"):
        raise ValueError("shard_id must begin with 'shard_'")
    if limit <= 0:
        raise ValueError("limit must be positive")

    start_time = datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ")
    end_time = datetime.strptime(end, "%Y-%m-%dT%H:%M:%SZ")
    if end_time <= start_time:
        raise ValueError("end time must be after start time")

    node_shard_id, node_datastore_id = get_node_details(environment, node_id)
    datastore_id = datastore_id or node_datastore_id
    shard_id = shard_id or node_shard_id
    if not datastore_id or not shard_id:
        raise RuntimeError(f"could not identify the datastore and shard for {node_id}")

    # Grafana's node dashboard uses parsed fields, not Loki stream labels, for these filters.
    query = (
        '{service_name=~"systemd_journal|dfcloud-logs"}'
        " | systemd_unit=`dragonfly.service`"
        f" | datastore_id=`{datastore_id}`"
        f" | shard_id=`{shard_id}`"
        f" | node_id=`{node_id}` |= ``"
    )
    command = [
        "gcx",
        "--context",
        context,
        "logs",
        "query",
        "--from",
        start,
        "--to",
        end,
        "--limit",
        str(limit),
        "-o",
        "raw",
        query,
    ]
    if output is None:
        result = subprocess.run(command, text=True, check=False)
    else:
        result = subprocess.run(command, text=True, capture_output=True, check=False)
        output.write_text(result.stdout)
        if result.stderr:
            print(result.stderr, end="", file=sys.stderr)
    return result.returncode


def run_dfadmin(environment: str, *arguments: str, allow_failure: bool = False) -> str:
    command = ["dfadmin-wrapper", "--env", environment, *arguments]
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode != 0 and not allow_failure:
        error = result.stderr.strip() or result.stdout.strip() or "no output"
        raise RuntimeError(f"{' '.join(command)} failed: {error}")
    return result.stdout.strip()


def extract_field(node_details: str, field: str) -> str:
    match = re.search(rf"^\s*{re.escape(field)}:\s*(.+?)\s*$", node_details, re.MULTILINE)
    return match.group(1) if match else ""


def get_node_details(environment: str, node_id: str) -> tuple[str, str]:
    details = run_dfadmin(environment, "node", "show", node_id)
    return extract_field(details, "shard_id"), extract_field(details, "datastore_id")


def get_node_address(environment: str, node_id: str) -> tuple[str, str, str, str]:
    details = run_dfadmin(environment, "node", "show", node_id)
    return (
        extract_field(details, "shard_id"),
        extract_field(details, "role"),
        extract_field(details, "public_ip"),
        extract_field(details, "private_ip"),
    )


def get_shard_node_ids(environment: str, shard_id: str) -> list[str]:
    output = run_dfadmin(environment, "shard", "show", shard_id, "--nodes", "--output", "json")
    try:
        shard = json.loads(output)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"dfadmin did not return valid JSON: {error.msg}") from error

    node_ids = []
    for record in find_node_records(shard):
        node_id = first_value(record, {"node_id"})
        if node_id and node_id not in node_ids:
            node_ids.append(node_id)
    if not node_ids:
        raise RuntimeError(f"dfadmin returned no node records for shard {shard_id}")
    return node_ids


def get_datastore_node_ids(environment: str, datastore_id: str) -> list[str]:
    output = run_dfadmin(
        environment, "datastore", "show", datastore_id, "--shards", "--nodes", "--output", "json"
    )
    try:
        datastore = json.loads(output)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"dfadmin did not return valid JSON: {error.msg}") from error

    node_ids = []
    for record in find_node_records(datastore):
        node_id = first_value(record, {"node_id"})
        if node_id and node_id not in node_ids:
            node_ids.append(node_id)
    if not node_ids:
        raise RuntimeError(f"dfadmin returned no node records for datastore {datastore_id}")
    return node_ids


def show_addresses(environment: str, targets: list[str]) -> int:
    node_ids = []
    for target in targets:
        if target.startswith("node_"):
            resolved_node_ids = [target]
        elif target.startswith("shard_"):
            resolved_node_ids = get_shard_node_ids(environment, target)
        elif target.startswith("dst_"):
            resolved_node_ids = get_datastore_node_ids(environment, target)
        else:
            raise ValueError(
                f"{target!r} must be a node ID (node_...), shard ID (shard_...), or datastore ID (dst_...)"
            )
        for node_id in resolved_node_ids:
            if node_id not in node_ids:
                node_ids.append(node_id)

    for node_id in node_ids:
        shard_id, role, public_ip, private_ip = get_node_address(environment, node_id)
        print(f"Node:       {node_id}")
        print(f"Shard:      {shard_id or '<unavailable>'}")
        print(f"Role:       {role or '<unknown>'}")
        print(f"Public IP:  {public_ip or '<unavailable>'}")
        print(f"Private IP: {private_ip or '<unavailable>'}")
        if node_id != node_ids[-1]:
            print()
    return 0


def get_role(environment: str, node_id: str) -> tuple[str, str]:
    replication = run_dfadmin(environment, "node", "resp", node_id, "--", "INFO", "replication")
    normalized_replication = (
        replication.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\r", "\n")
    )
    match = re.search(
        r"^\s*role\s*[:=]\s*[\"']?(master|slave|primary|replica)[\"']?",
        normalized_replication,
        re.IGNORECASE | re.MULTILINE,
    )
    if not match:
        return "", replication
    role = match.group(1).lower()
    return {"primary": "master", "replica": "slave"}.get(role, role), replication


def get_key_count(environment: str, node_id: str) -> int:
    keyspace = run_dfadmin(environment, "node", "resp", node_id, "--", "INFO", "keyspace")
    match = re.search(r"keys=(\d+)", keyspace)
    if not match:
        raise RuntimeError(f"could not parse a key count from INFO keyspace for {node_id}")
    return int(match.group(1))


def get_config(environment: str, node_id: str, parameter: str) -> str:
    return run_dfadmin(
        environment, "node", "resp", node_id, "--", "CONFIG", "GET", parameter, allow_failure=True
    )


def config_response_is_empty(response: str) -> bool:
    return response.strip().lower() in {"", "{}", "[]", "null"}


def first_value(value: object, names: set[str]) -> str:
    if isinstance(value, dict):
        for name, child in value.items():
            if name.lower() in names and child not in (None, ""):
                return str(child)
        for child in value.values():
            found = first_value(child, names)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = first_value(child, names)
            if found:
                return found
    return ""


def find_node_records(value: object) -> list[dict]:
    records = []
    if isinstance(value, dict):
        if value.get("node_id"):
            records.append(value)
        for child in value.values():
            records.extend(find_node_records(child))
    elif isinstance(value, list):
        for child in value:
            records.extend(find_node_records(child))
    return records


def discover_datastore(environment: str, target: str) -> int:
    if target.startswith("node_"):
        _, datastore_id = get_node_details(environment, target)
        if not datastore_id:
            raise RuntimeError(f"could not identify the datastore for {target}")
    elif target.startswith("dst_"):
        datastore_id = target
    else:
        raise ValueError("target must be a node ID (node_...) or datastore ID (dst_...)")

    output = run_dfadmin(
        environment, "datastore", "show", datastore_id, "--shards", "--nodes", "--output", "json"
    )
    try:
        datastore = json.loads(output)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"dfadmin did not return valid JSON: {error.msg}") from error

    records = find_node_records(datastore)
    if not records:
        raise RuntimeError("dfadmin returned no node records for this datastore")

    unique_records = {}
    for record in records:
        node_id = first_value(record, {"node_id"})
        unique_records[node_id] = record

    shards = {}
    for node_id, record in unique_records.items():
        shard_id = first_value(record, {"shard_id"}) or "<unavailable>"
        role = first_value(record, {"role"}).lower()
        shard = shards.setdefault(shard_id, {"primary": "", "replicas": [], "other": []})
        if role in {"master", "primary"}:
            shard["primary"] = node_id
        elif role in {"slave", "replica"}:
            shard["replicas"].append(node_id)
        else:
            shard["other"].append(node_id)

    rows = []
    for shard_id, members in sorted(shards.items()):
        replicas = sorted(members["replicas"])
        replicas.extend(sorted(members["other"]))
        rows.append(
            (shard_id, members["primary"] or "<unavailable>", ", ".join(replicas) or "<none>")
        )

    headers = ("Shard", "Primary", "Replicas")
    widths = [
        max(len(header), *(len(row[index]) for row in rows)) for index, header in enumerate(headers)
    ]

    print("Skipper Cluster Discovery Report")
    print(f"Environment: {environment}")
    print(f"Datastore: {datastore_id}")
    print(f"{headers[0]:<{widths[0]}}  {headers[1]:<{widths[1]}}  {headers[2]}")
    for shard_id, primary, replicas in rows:
        print(f"{shard_id:<{widths[0]}}  {primary:<{widths[1]}}  {replicas}")
    return 0


def check_pair(environment: str, first_node_id: str, second_node_id: str, parameter: str) -> int:
    first_shard, first_datastore = get_node_details(environment, first_node_id)
    second_shard, second_datastore = get_node_details(environment, second_node_id)
    first_role, first_replication = get_role(environment, first_node_id)
    second_role, second_replication = get_role(environment, second_node_id)

    print("Skipper Primary and Replica Pair Report")
    print(f"Environment: {environment}")
    print(
        f"First node:  {first_node_id} (role: {first_role or 'unknown'}, "
        f"shard: {first_shard or 'unknown'}, datastore: {first_datastore or 'unknown'})"
    )
    print(
        f"Second node: {second_node_id} (role: {second_role or 'unknown'}, "
        f"shard: {second_shard or 'unknown'}, datastore: {second_datastore or 'unknown'})"
    )

    if not first_shard or first_shard != second_shard:
        print(
            "Pair validation: FAIL - nodes are not in the same shard. No key-count comparison made."
        )
        return 2
    if not first_datastore or first_datastore != second_datastore:
        print(
            "Pair validation: FAIL - nodes are not in the same datastore. No key-count comparison made."
        )
        return 2

    roles = {first_role: first_node_id, second_role: second_node_id}
    if set(roles) != {"master", "slave"}:
        print(
            "Pair validation: FAIL - expected one primary and one replica; "
            f"got {first_role or 'unknown'} and {second_role or 'unknown'}."
        )
        if not first_role:
            print(f"First node replication response: {first_replication or '<empty response>'}")
        if not second_role:
            print(f"Second node replication response: {second_replication or '<empty response>'}")
        return 2

    master_node_id = roles["master"]
    replica_node_id = roles["slave"]
    master_keys = get_key_count(environment, master_node_id)
    replica_keys = get_key_count(environment, replica_node_id)
    signed_gap = master_keys - replica_keys

    print(
        f"Pair validation: PASS - same shard {first_shard}; "
        f"primary={master_node_id} replica={replica_node_id}"
    )
    print(f"Primary keys: {master_keys}")
    print(f"Replica keys: {replica_keys}")
    print(f"Primary - replica gap: {signed_gap} (absolute: {abs(signed_gap)})")
    if signed_gap < 0:
        print(f"Direction: replica has {abs(signed_gap)} more keys than primary.")
    elif signed_gap > 0:
        print(f"Direction: primary has {abs(signed_gap)} more keys than replica.")
    else:
        print("Direction: key counts match.")

    master_config = get_config(environment, master_node_id, parameter)
    replica_config = get_config(environment, replica_node_id, parameter)
    print(f"\nCONFIG GET {parameter}")
    print(
        f"Primary response: {master_config if not config_response_is_empty(master_config) else '<not exposed>'}"
    )
    print(
        f"Replica response: {replica_config if not config_response_is_empty(replica_config) else '<not exposed>'}"
    )
    if config_response_is_empty(master_config) and config_response_is_empty(replica_config):
        print(
            "Interpretation: parameter is not exposed by CONFIG GET; this does not prove it is unset."
        )
    if parameter == DEFAULT_CONFIG_PARAMETER:
        print("Note: replica_delete_expired is a runtime flag; /flagz is the authoritative value.")
    return 0


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="dfly-inspect: read-only Dragonfly topology checks and Grafana log retrieval.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""command reference:
  discover <dst_...|node_...>
      Show every shard's primary and replicas for a datastore.
      %(prog)s discover dst_ckf66xo5q

  addresses <dst_...|shard_...|node_...> [...]
      Show roles and public/private IPs for all resolved nodes.
      %(prog)s addresses dst_ckf66xo5q

  replication-gap <node_...> <node_...>
      Compare a primary/replica pair's key counts and runtime configuration.
      %(prog)s replication-gap node_primary node_replica

  logs <node_...> <start UTC> <end UTC>
      Retrieve up to 5000 Dragonfly log lines. Optional filters: --limit,
      --datastore-id, --shard-id, and --output.
      %(prog)s logs node_px5aifkwl \\
          '2026-09-10 06:00:00' '2026-09-10 07:00:00' \\
          --output /tmp/node_px5aifkwl_0600-0700Z.log

All commands use production by default. For arguments and optional flags:
  %(prog)s <command> --help""",
    )
    commands = parser.add_subparsers(dest="command", required=True, title="commands")

    gap_parser = commands.add_parser(
        "replication-gap",
        help="compare key counts for a primary and replica pair",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="example: %(prog)s replication-gap node_primary node_replica",
    )
    gap_parser.add_argument("first_node_id", help="Either member of the primary and replica pair")
    gap_parser.add_argument(
        "second_node_id", help="The other member of the primary and replica pair"
    )
    gap_parser.add_argument(
        "--env", default=DEFAULT_ENV, help=f"dfadmin environment (default: {DEFAULT_ENV})"
    )
    gap_parser.add_argument(
        "--config-parameter",
        default=DEFAULT_CONFIG_PARAMETER,
        help=f"Parameter to inspect with CONFIG GET (default: {DEFAULT_CONFIG_PARAMETER})",
    )

    discover_parser = commands.add_parser(
        "discover",
        help="show primary-to-replica topology for a datastore",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="example: %(prog)s discover dst_ckf66xo5q",
    )
    discover_parser.add_argument("target", help="A node ID (node_...) or datastore ID (dst_...)")
    discover_parser.add_argument(
        "--env", default=DEFAULT_ENV, help=f"dfadmin environment (default: {DEFAULT_ENV})"
    )

    addresses_parser = commands.add_parser(
        "addresses",
        help="show public and private IPs for nodes, shards, or an entire datastore",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="example: %(prog)s addresses dst_ckf66xo5q",
    )
    addresses_parser.add_argument(
        "targets",
        nargs="+",
        help="Node IDs (node_...), shard IDs (shard_...), or datastore IDs (dst_...)",
    )
    addresses_parser.add_argument(
        "--env", default=DEFAULT_ENV, help=f"dfadmin environment (default: {DEFAULT_ENV})"
    )

    logs_parser = commands.add_parser(
        "logs",
        help="retrieve Dragonfly service logs for one node in a UTC time window",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""examples:
            %(prog)s node_px5aifkwl \\
                '2026-09-10 06:00:00' '2026-09-10 07:00:00'

            %(prog)s node_px5aifkwl \\
                '2026-09-10 06:00:00' '2026-09-10 07:00:00' \\
            --datastore-id dst_ckf66xo5q --shard-id shard_xa6i8i29j \\
            --output /tmp/node_px5aifkwl_0600-0700Z.log""",
    )
    logs_parser.add_argument("node_id", help="Node ID (node_...)")
    logs_parser.add_argument(
        "start", type=parse_utc_timestamp, help="Start time in UTC: YYYY-MM-DD HH:MM:SS"
    )
    logs_parser.add_argument(
        "end", type=parse_utc_timestamp, help="End time in UTC: YYYY-MM-DD HH:MM:SS"
    )
    logs_parser.add_argument(
        "--context",
        default=DEFAULT_GCX_CONTEXT,
        help=f"gcx context (default: {DEFAULT_GCX_CONTEXT})",
    )
    logs_parser.add_argument(
        "--env", default=DEFAULT_ENV, help=f"dfadmin-wrapper environment (default: {DEFAULT_ENV})"
    )
    logs_parser.add_argument(
        "--limit", type=int, default=5000, help="Maximum log lines (default: 5000)"
    )
    logs_parser.add_argument(
        "--datastore-id", help="Optional datastore label (dst_...) to match Grafana's full selector"
    )
    logs_parser.add_argument(
        "--shard-id", help="Optional shard label (shard_...) to match Grafana's full selector"
    )
    logs_parser.add_argument("--output", type=Path, help="Write raw log output to this local file")
    return parser.parse_args()


def main() -> int:
    arguments = parse_arguments()
    executables = ["gcx", "dfadmin-wrapper"] if arguments.command == "logs" else ["dfadmin-wrapper"]
    missing = next((executable for executable in executables if not shutil.which(executable)), None)
    if missing:
        print(f"ERROR: {missing} is not on PATH", file=sys.stderr)
        return 1
    try:
        if arguments.command == "logs":
            return get_logs(
                arguments.node_id,
                arguments.start,
                arguments.end,
                arguments.env,
                arguments.context,
                arguments.limit,
                arguments.datastore_id,
                arguments.shard_id,
                arguments.output,
            )
        if arguments.command == "discover":
            return discover_datastore(arguments.env, arguments.target)
        if arguments.command == "addresses":
            return show_addresses(arguments.env, arguments.targets)
        return check_pair(
            arguments.env,
            arguments.first_node_id,
            arguments.second_node_id,
            arguments.config_parameter,
        )
    except (RuntimeError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
