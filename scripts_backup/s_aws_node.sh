#!/usr/bin/env bash

# Manages the status and start/stop lifecycle of the four fixed AWS benchmark
# instances in us-east-1: VM and bare-metal server/client nodes.

set -euo pipefail

readonly AWS_REGION="us-east-1"
readonly VM_SERVER_ID="i-05d6a86593a69c4d2"
readonly VM_CLIENT_ID="i-0bfe5e51710694df5"
readonly BM_SERVER_ID="i-01abd2187a0a6058d"
readonly BM_CLIENT_ID="i-09d66c89238ed71be"

usage() {
  cat <<'EOF'
Usage:
  s_aws_node
  s_aws_node list
  s_aws_node status <node> [node...]
  s_aws_node start <node> [node...]
  s_aws_node stop [--hard] <node> [node...]

Nodes:
  myaws-vm-server | vm-server  Existing r6g VM server
  myaws-vm-client | vm-client  Existing c7g VM client
  myaws-bm-server | bm-server  Bare-metal benchmark server
  myaws-bm-client | bm-client  Bare-metal benchmark client

Examples:
  s_aws_node
  s_aws_node list
  s_aws_node start bm-server
  s_aws_node stop all
  s_aws_node stop --hard bm-server bm-client
  s_aws_node stop myaws-bm-client
  s_aws_node status vm-server

The script operates only on the four nodes listed above in us-east-1.
Use `s_aws_node stop all` to stop all four nodes in one AWS request.
--hard (or --skip-os-shutdown) bypasses the guest OS shutdown. It can lose
unflushed results and require filesystem recovery at the next boot.
EOF
}

require_aws_auth() {
  if AWS_PAGER='' aws sts get-caller-identity --output json >/dev/null 2>&1; then
    return
  fi

  cat >&2 <<'EOF'
AWS authentication is unavailable or has expired.
Authenticate with your normal AWS SSO profile, for example:
  aws sso login

Then retry this command.
EOF
  exit 1
}

node_id() {
  case "$1" in
    myaws-vm-server|vm-server)
      printf '%s\n' "$VM_SERVER_ID"
      ;;
    myaws-vm-client|vm-client)
      printf '%s\n' "$VM_CLIENT_ID"
      ;;
    myaws-bm-server|bm-server)
      printf '%s\n' "$BM_SERVER_ID"
      ;;
    myaws-bm-client|bm-client)
      printf '%s\n' "$BM_CLIENT_ID"
      ;;
    *)
      printf 'Unknown node: %s\n\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
}

describe_nodes() {
  AWS_PAGER='' aws ec2 describe-instances \
    --region "$AWS_REGION" \
    --instance-ids "$@" \
    --query 'Reservations[].Instances[].{Name:Tags[?Key==`Name`]|[0].Value,Id:InstanceId,State:State.Name,Type:InstanceType,PrivateIp:PrivateIpAddress,ElasticIp:PublicIpAddress,Az:Placement.AvailabilityZone}' \
    --output table
}

main() {
  case "${1:-list}" in
    -h|--help|help)
      usage
      return
      ;;
    list)
      [[ $# -le 1 ]] || {
        usage >&2
        exit 2
      }
      require_aws_auth
      describe_nodes "$VM_SERVER_ID" "$VM_CLIENT_ID" "$BM_SERVER_ID" "$BM_CLIENT_ID"
      ;;
    status)
      [[ $# -ge 2 ]] || {
        usage >&2
        exit 2
      }
      require_aws_auth
      local instance_ids=()
      local node
      for node in "${@:2}"; do
        instance_ids+=("$(node_id "$node")")
      done
      describe_nodes "${instance_ids[@]}"
      ;;
    start|stop)
      [[ $# -ge 2 ]] || {
        usage >&2
        exit 2
      }
      local action="$1"
      local instance_ids=()
      local skip_os_shutdown=false
      local node
      shift
      for node in "$@"; do
        case "$node" in
          all)
            [[ $action == stop ]] || {
              printf 'all is only valid with stop\n' >&2
              exit 2
            }
            instance_ids+=("$VM_SERVER_ID" "$VM_CLIENT_ID" "$BM_SERVER_ID" "$BM_CLIENT_ID")
            ;;
          --hard|--skip-os-shutdown)
            [[ $action == stop ]] || {
              printf '%s is only valid with stop\n' "$node" >&2
              exit 2
            }
            skip_os_shutdown=true
            ;;
          --*)
            printf 'Unknown option: %s\n' "$node" >&2
            exit 2
            ;;
          *)
            instance_ids+=("$(node_id "$node")")
            ;;
        esac
      done
      [[ ${#instance_ids[@]} -gt 0 ]] || {
        usage >&2
        exit 2
      }

      require_aws_auth

      case "$action" in
        start)
          AWS_PAGER='' aws ec2 start-instances \
            --region "$AWS_REGION" \
            --instance-ids "${instance_ids[@]}" \
            --output table
          echo
          describe_nodes "${instance_ids[@]}"
          ;;
        stop)
          local stop_options=()
          if [[ $skip_os_shutdown == true ]]; then
            echo 'Hard stop: bypassing guest OS shutdown.'
            stop_options=(--skip-os-shutdown)
          fi
          AWS_PAGER='' aws ec2 stop-instances \
            --region "$AWS_REGION" \
            --instance-ids "${instance_ids[@]}" \
            "${stop_options[@]}" \
            --output table
          echo
          describe_nodes "${instance_ids[@]}"
          ;;
      esac
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
}

main "$@"
