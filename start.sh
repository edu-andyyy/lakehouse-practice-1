#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results
docker info >/dev/null
docker compose config --quiet
: > results/pull.log
docker compose config --images | sort -u > results/images.local.txt
while IFS= read -r image; do
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    docker image pull "$image" 2>&1 | tee -a results/pull.log
  fi
done < results/images.local.txt
if ! docker compose up -d --pull never --wait --wait-timeout 600 2>&1 | tee results/deploy.log; then
  docker compose ps --all > results/docker-ps.txt
  docker compose logs --no-color --tail 100 > results/services.log 2>&1
  printf '\nОшибка запуска. Диагностика сохранена в results/services.log\n'
  exit 1
fi
docker compose ps --all > results/docker-ps.txt
docker info --format '{{json .}}' > results/docker-info.local.json
python3 scripts/capture_environment.py
printf '\nСтек запущен. Следующая команда: ./run.sh\n'
