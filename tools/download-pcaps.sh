#!/usr/bin/env bash
# Downloads B3 UMDF Binary PCAP examples from the B3 website.
# Usage: ./tools/download-pcaps.sh [output-dir]

set -euo pipefail

OUTDIR="${1:-pcap}"
mkdir -p "$OUTDIR"

BASE="https://mktdatabinario.z15.web.core.windows.net/PCAPS/BinaryUMDF/SiteB3"

declare -A FILES=(
  ["MBO_EQT_Incremental_FeedA"]="Equities - Incremental Feed A"
  ["MBO_EQT_Incremental_FeedB"]="Equities - Incremental Feed B"
  ["MBO_EQT_InstrumentDefinition"]="Equities - Instrument Definition"
  ["MBO_EQT_SnapshotRecovery"]="Equities - Snapshot Recovery"
  ["MBO_DRV_Incremental_FeedA"]="Derivatives - Incremental Feed A"
  ["MBO_DRV_Incremental_FeedB"]="Derivatives - Incremental Feed B"
  ["MBO_DRV_InstrumentDefinition"]="Derivatives - Instrument Definition"
  ["MBO_DRV_SnapshotRecovery"]="Derivatives - Snapshot Recovery"
)

for key in "${!FILES[@]}"; do
  url="$BASE/${key}.zip"
  desc="${FILES[$key]}"
  zip="$OUTDIR/${key}.zip"

  if [ -f "$OUTDIR/${key}.pcap" ] || [ -f "$OUTDIR/${key}.pcapng" ]; then
    echo "  [skip] $desc (already extracted)"
    continue
  fi

  echo "  [download] $desc"
  curl -sSL -o "$zip" "$url"
  echo "  [extract] $desc"
  unzip -qo "$zip" -d "$OUTDIR"
  rm -f "$zip"
done

echo ""
echo "PCAP files downloaded to: $OUTDIR/"
ls -lh "$OUTDIR/"
