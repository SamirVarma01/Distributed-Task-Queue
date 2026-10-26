# Sourced by the benchmark scripts. On a laptop, results are only comparable on mains
# power with the machine awake: earlier runs here were skewed by battery drain and by
# the machine sleeping mid-run.
#  - Refuses to start on battery power unless ALLOW_BATTERY=1.
#  - Re-runs the calling script under caffeinate so the machine does not idle-sleep.
#    Closing the lid still sleeps it; keep it open.
if command -v pmset >/dev/null && pmset -g batt | grep -q "'Battery Power'" && [ -z "${ALLOW_BATTERY:-}" ]; then
  echo "On battery power. Plug in for comparable results, or set ALLOW_BATTERY=1." >&2
  exit 1
fi
if command -v caffeinate >/dev/null && [ -z "${DTQ_CAFFEINATED:-}" ]; then
  exec env DTQ_CAFFEINATED=1 caffeinate -ims "$0" "$@"
fi
