#!/bin/sh
set -e

PCAP_DIR="${PCAP_DIR:-/app/pcap}"
PCAP_PREFIX="${PCAP_PREFIX:-20250331_MBO_084_EQT}"
WS_PORT="${WS_PORT:-8080}"

PREFIX="${PCAP_DIR}/${PCAP_PREFIX}"

exec /app/B3.Umdf.ConsoleApp \
  "${PREFIX}_Incremental_FeedA.pcap" \
  "${PREFIX}_Incremental_FeedB.pcap" \
  "${PREFIX}_InstrumentDefinition.pcap" \
  "${PREFIX}_SnapshotRecovery.pcap" \
  --ws-port "${WS_PORT}"
