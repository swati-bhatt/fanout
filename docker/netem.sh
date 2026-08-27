#!/usr/bin/env bash
# Apply a named network-impairment profile to the fanout server container's egress.
#
#   ./docker/netem.sh <profile>   # clean | wifi | mobile | lossy | satellite | flapping
#   ./docker/netem.sh clear
#   ./docker/netem.sh show
#
# WHAT IS SHAPED: the container's egress only -- i.e. server -> client, the delivery direction the
# benchmark measures. Client -> server requests are unshaped, so a poll's request leg is fast while
# its response leg is impaired. State that asymmetry when reporting: it understates the cost of
# request/response transports (poll, long poll) relative to push transports, making any polling
# disadvantage measured here a LOWER bound.
#
# Profiles use `netem` delay + jitter + correlation and loss. Correlation matters: real networks
# lose packets in bursts, and uncorrelated loss is unrealistically kind to protocols that recover
# per-packet. Reordering is left off deliberately -- TCP treats it as loss and it would confound
# the loss axis.
set -euo pipefail

CONTAINER="${CONTAINER:-fanout-netem}"
IFACE="${IFACE:-eth0}"
PROFILE="${1:-show}"

in_container() { docker exec --privileged "$CONTAINER" "$@"; }

clear_qdisc() {
  # `|| true`: deleting when no qdisc is attached exits non-zero, which is not an error here.
  in_container tc qdisc del dev "$IFACE" root 2>/dev/null || true
}

apply() {
  clear_qdisc
  # shellcheck disable=SC2086
  in_container tc qdisc add dev "$IFACE" root netem $1
  echo "[netem] $CONTAINER $IFACE <- $1"
}

case "$PROFILE" in
  clean)
    clear_qdisc
    echo "[netem] cleared — baseline (container adds its own ~0.1-0.5ms virtual-NIC overhead)"
    ;;
  clear)
    clear_qdisc
    echo "[netem] cleared"
    ;;
  wifi)
    # Good indoor Wi-Fi: low latency, mild jitter, occasional bursty loss.
    apply "delay 20ms 5ms distribution normal loss 0.5% 25%"
    ;;
  mobile)
    # Typical 4G/LTE: higher RTT, heavy jitter, real loss.
    apply "delay 60ms 20ms distribution normal loss 1% 25%"
    ;;
  lossy)
    # Congested/degraded link: loss is the dominant impairment, strongly correlated.
    apply "delay 30ms 10ms distribution normal loss 3% 50%"
    ;;
  satellite)
    # GEO satellite: very high RTT, low jitter, low loss. Punishes chatty request/response
    # protocols far more than persistent connections — the sharpest transport discriminator.
    apply "delay 300ms 20ms distribution normal loss 0.1%"
    ;;
  flapping)
    # Not a netem profile: repeatedly sever and restore connectivity to exercise reconnect,
    # resync-from-lastSeq, and missed-update accounting rather than steady-state latency.
    echo "[netem] flapping: 5 cycles of 3s down / 12s up on $CONTAINER"
    for i in $(seq 1 5); do
      in_container tc qdisc replace dev "$IFACE" root netem loss 100%
      echo "  cycle $i: DOWN 3s"
      sleep 3
      clear_qdisc
      echo "  cycle $i: UP 12s"
      sleep 12
    done
    echo "[netem] flapping complete, link clean"
    ;;
  show)
    in_container tc qdisc show dev "$IFACE"
    ;;
  *)
    echo "unknown profile: $PROFILE" >&2
    echo "usage: $0 {clean|clear|wifi|mobile|lossy|satellite|flapping|show}" >&2
    exit 1
    ;;
esac
