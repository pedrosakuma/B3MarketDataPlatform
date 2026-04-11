#!/bin/sh
set -e

PCAP_DIR="${PCAP_DIR:-/app/pcap}"
PCAP_PREFIX="${PCAP_PREFIX:-20250331_MBO_084_EQT}"
WS_PORT="${WS_PORT:-8080}"
REPLAY_SPEED="${REPLAY_SPEED:-5}"

# Build --pcap-prefix args (comma-separated for multi-channel)
PREFIX_ARGS=""
IFS=','
for prefix in $PCAP_PREFIX; do
  PREFIX_ARGS="$PREFIX_ARGS --pcap-prefix ${PCAP_DIR}/${prefix}"
done
unset IFS

exec /app/B3.Umdf.ConsoleApp \
  $PREFIX_ARGS \
  --ws-port "${WS_PORT}" \
  --speed "${REPLAY_SPEED}"
