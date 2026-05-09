#!/bin/sh
set -eu

record_path="${CRIUS_FAKE_CNI_PLUGIN_RECORD_PATH:-}"
if [ -n "$record_path" ]; then
  cat > "$record_path"
else
  cat >/dev/null
fi

should_fail=false
fail_on="${CRIUS_FAKE_CNI_PLUGIN_FAIL_ON:-}"
if [ -n "$fail_on" ]; then
  old_ifs=$IFS
  IFS=','
  for phase in $fail_on; do
    if [ "$(printf '%s' "$phase" | tr '[:lower:]' '[:upper:]')" = "${CNI_COMMAND:-}" ]; then
      should_fail=true
      break
    fi
  done
  IFS=$old_ifs
fi

if [ "$should_fail" = "true" ]; then
  printf '%s\n' "${CRIUS_FAKE_CNI_PLUGIN_ERROR_MESSAGE:-fake CNI failure injection}" >&2
  exit "${CRIUS_FAKE_CNI_PLUGIN_ERROR_CODE:-42}"
fi

if [ "${CNI_COMMAND:-}" = "DEL" ]; then
  exit 0
fi

if [ -n "${CRIUS_FAKE_CNI_PLUGIN_RESULT_JSON:-}" ]; then
  printf '%s\n' "${CRIUS_FAKE_CNI_PLUGIN_RESULT_JSON}"
else
  printf '%s\n' '{"cniVersion":"1.0.0","ips":[{"address":"10.88.0.2/16"}]}'
fi
