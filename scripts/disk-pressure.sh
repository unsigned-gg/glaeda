#!/bin/bash
# Run one bounded disk-pressure pass for a fleet mini.  The LaunchAgent owns the
# cadence; keeping this as a tiny wrapper gives runner installs the same explicit
# entry point as the fleet bootstrap and makes a one-shot operator pass testable.
set -euo pipefail

if [[ "${1:-}" == "--help" ]]; then
  echo "usage: disk-pressure.sh [--once]"
  exit 0
fi
if [[ "${1:-}" != "--once" ]]; then
  echo "usage: disk-pressure.sh --once" >&2
  exit 2
fi

home="${HOME:?HOME is required}"
python="${GLAEDA_PYTHON:-/opt/homebrew/bin/python3}"
disk="${GLAEDA_DISK:-$home/.local/bin/glaeda-disk}"
export GLAEDA_DISK_CALLER="${GLAEDA_DISK_CALLER:-disk-pressure LaunchAgent}"
# Each pass ends before the next one starts (the agent runs every 15 minutes): glaeda-disk stops starting walks and
# deletes 60 s before HYGIENE_RUN_DEADLINE and the next pass goes on, so no pass waits behind a long one.
export HYGIENE_RUN_DEADLINE="${HYGIENE_RUN_DEADLINE:-$(( $(date +%s) + ${GLAEDA_DISK_PASS_S:-840} ))}"
# GLAEDA_DISK_PRESSURE_ONLY=1 (setup's --gh-only profile): only below the floor, no idle sweep.
idle=(--idle)
[[ "${GLAEDA_DISK_PRESSURE_ONLY:-}" == 1 ]] && idle=()
if [[ -n "${GLAEDA_ROLE:-}" ]]; then
  exec "$python" "$disk" --role "$GLAEDA_ROLE" --pressure ${idle[@]+"${idle[@]}"} --apply --top 0
fi
exec "$python" "$disk" --pressure ${idle[@]+"${idle[@]}"} --apply --top 0
