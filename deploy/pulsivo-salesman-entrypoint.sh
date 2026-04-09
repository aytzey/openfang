#!/bin/sh
set -eu

DATA_HOME="${HOME:-/data}"
PULSIVO_SALESMAN_ROOT="${DATA_HOME}/.pulsivo-salesman"
CONFIG_PATH="${PULSIVO_SALESMAN_ROOT}/config.toml"
LISTEN_ADDR="${PULSIVO_SALESMAN_LISTEN:-127.0.0.1:4200}"

mkdir -p "${DATA_HOME}" "${PULSIVO_SALESMAN_ROOT}" "${PULSIVO_SALESMAN_ROOT}/data" "${PULSIVO_SALESMAN_ROOT}/agents"

if [ ! -f "${CONFIG_PATH}" ]; then
  echo "Bootstrapping Pulsivo Salesman config in ${PULSIVO_SALESMAN_ROOT}"
  pulsivo-salesman init --quick
fi

if grep -q '^api_listen[[:space:]]*=' "${CONFIG_PATH}"; then
  sed -i "s#^api_listen[[:space:]]*=.*#api_listen = \"${LISTEN_ADDR}\"#" "${CONFIG_PATH}"
else
  printf '\napi_listen = "%s"\n' "${LISTEN_ADDR}" >> "${CONFIG_PATH}"
fi

exec pulsivo-salesman "$@"
