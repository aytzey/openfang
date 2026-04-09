#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export PULSIVO_SALESMAN_UID="${PULSIVO_SALESMAN_UID:-$(id -u)}"
export PULSIVO_SALESMAN_GID="${PULSIVO_SALESMAN_GID:-$(id -g)}"
export PULSIVO_SALESMAN_DATA_DIR="${PULSIVO_SALESMAN_DATA_DIR:-${ROOT_DIR}/.docker/pulsivo-salesman-home}"
export PULSIVO_SALESMAN_CODEX_DIR="${PULSIVO_SALESMAN_CODEX_DIR:-${HOME}/.codex}"

command="${1:-up}"
shift || true

cleanup() {
  docker compose down --volumes --remove-orphans --rmi local >/dev/null 2>&1 || true
  rm -rf "${PULSIVO_SALESMAN_DATA_DIR}"
  mkdir -p "${PULSIVO_SALESMAN_DATA_DIR}"
  docker builder prune -af >/dev/null 2>&1 || true
}

case "${command}" in
  up)
    cleanup
    exec docker compose up --build --force-recreate --renew-anon-volumes "$@"
    ;;
  build)
    cleanup
    exec docker compose build --pull --no-cache "$@"
    ;;
  clean)
    cleanup
    ;;
  down)
    exec docker compose down --volumes --remove-orphans --rmi local "$@"
    ;;
  logs)
    exec docker compose logs "$@"
    ;;
  *)
    echo "Usage: $0 {up|build|clean|down|logs} [docker compose args...]" >&2
    exit 1
    ;;
esac
