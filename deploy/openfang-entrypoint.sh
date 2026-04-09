#!/bin/sh
set -eu

DATA_HOME="${HOME:-/data}"
OPENFANG_ROOT="${DATA_HOME}/.openfang"
CONFIG_PATH="${OPENFANG_ROOT}/config.toml"
LISTEN_ADDR="${OPENFANG_LISTEN:-127.0.0.1:4200}"

mkdir -p "${DATA_HOME}" "${OPENFANG_ROOT}" "${OPENFANG_ROOT}/data" "${OPENFANG_ROOT}/agents"

if [ ! -f "${CONFIG_PATH}" ]; then
  echo "Bootstrapping OpenFang config in ${OPENFANG_ROOT}"
  openfang init --quick
fi

if grep -q '^api_listen[[:space:]]*=' "${CONFIG_PATH}"; then
  sed -i "s#^api_listen[[:space:]]*=.*#api_listen = \"${LISTEN_ADDR}\"#" "${CONFIG_PATH}"
else
  printf '\napi_listen = "%s"\n' "${LISTEN_ADDR}" >> "${CONFIG_PATH}"
fi

exec openfang "$@"
