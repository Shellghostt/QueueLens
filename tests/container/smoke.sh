#!/usr/bin/env bash
# Exercise the packaged server against isolated, disposable infrastructure.
set -euo pipefail

image=${1:-queuelens:ci}
work=$(mktemp -d)
suffix=$(basename "$work")
network="ql-image-$suffix"
postgres="ql-pg-$suffix"
redis="ql-redis-$suffix"
collector="ql-server-$suffix"
token=container-smoke-admin-token

cleanup() {
  status=$?
  if (( status != 0 )); then
    docker logs "$collector" 2>&1 || true
    docker logs "$postgres" 2>&1 || true
  fi
  docker rm -f -v "$collector" "$postgres" "$redis" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
  rm -rf -- "$work"
  exit "$status"
}
trap cleanup EXIT

[[ $(docker image inspect --format '{{.Config.User}}' "$image") == '65532' ]]
docker run --rm --network none "$image" version
docker network create "$network" >/dev/null
docker run -d --name "$postgres" --network "$network" --network-alias postgres \
  --tmpfs /var/lib/postgresql/data \
  -e POSTGRES_USER=queuelens -e POSTGRES_PASSWORD=queuelens -e POSTGRES_DB=queuelens \
  postgres:16-alpine >/dev/null
docker run -d --name "$redis" --network "$network" --network-alias redis \
  --tmpfs /data redis:7.2-alpine >/dev/null

ready=false
for (( i=0; i<60; i++ )); do
  if docker exec "$postgres" pg_isready -U queuelens >/dev/null 2>&1 &&
     [[ $(docker exec "$redis" redis-cli ping 2>/dev/null) == PONG ]]; then
    ready=true
    break
  fi
  sleep 1
done
[[ $ready == true ]]
# A minimal Redis fixture checks packaged adapter wiring. The separate
# integration suite verifies the key layout with the actual BullMQ library.
docker exec "$redis" redis-cli HSET bull:image-smoke:meta smoke 1 >/dev/null
docker exec "$redis" redis-cli RPUSH bull:image-smoke:wait job-1 >/dev/null
docker run -d --name "$collector" --network "$network" --read-only --cap-drop ALL \
  -p 127.0.0.1::8080 \
  -e QUEUELENS_POSTGRES_DSN=postgresql://queuelens:queuelens@postgres:5432/queuelens \
  -e QUEUELENS_REDIS_URL=redis://redis:6379/0 \
  -e QUEUELENS_ADMIN_TOKEN="$token" -e QUEUELENS_POLL_INTERVAL=100ms \
  "$image" >/dev/null
port=$(docker port "$collector" 8080/tcp)
url="http://$port"
curl --fail --silent --show-error --max-time 2 --retry 60 --retry-delay 1 \
  --retry-connrefused "$url/readyz" >"$work/ready.json"
curl --fail --silent --show-error "$url/healthz" >"$work/health.json"

[[ $(curl --silent --output /dev/null --write-out '%{http_code}' \
  -H 'Content-Type: application/json' --data '{"v":1,"worker_id":"image-worker"}' \
  "$url/v1/workers/heartbeat") == 401 ]]
[[ $(curl --silent --output /dev/null --write-out '%{http_code}' \
  -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data '{"v":99,"worker_id":"image-worker"}' "$url/v1/workers/heartbeat") == 400 ]]
[[ $(curl --silent --output "$work/heartbeat.json" --write-out '%{http_code}' \
  -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data '{"v":1,"worker_id":"image-worker","pool":"container-smoke"}' \
  "$url/v1/workers/heartbeat") == 202 ]]

timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
[[ $(curl --silent --output "$work/events.json" --write-out '%{http_code}' \
  -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data "[{\"v\":1,\"ts\":\"$timestamp\",\"queue\":\"image-smoke\",\"job_id\":\"image-job\",\"state\":\"completed\",\"worker_id\":\"image-worker\",\"duration_ms\":12}]" \
  "$url/v1/events") == 202 ]]
python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["accepted"]==1 and d["dropped"]==0 and not d["rejected"], d' <"$work/events.json"

persisted=false
for (( i=0; i<30; i++ )); do
  count=$(docker exec "$postgres" psql -U queuelens -Atc \
    "SELECT count(*) FROM queue_events WHERE job_id='image-job' AND state='completed' AND duration_ms=12")
  if [[ $count == 1 ]]; then persisted=true; break; fi
  sleep 1
done
[[ $persisted == true ]]
[[ $(docker exec "$postgres" psql -U queuelens -Atc \
  "SELECT count(*) FROM workers WHERE worker_id='image-worker' AND pool='container-smoke' AND last_seen>=first_seen") == 1 ]]
curl --fail --silent --show-error -H "Authorization: Bearer $token" "$url/v1/queues" >"$work/queues.json"
python3 -c 'import json,sys; d=json.load(sys.stdin); assert any(q["queue"]=="image-smoke" and q["waiting"]==1 and not q["stale"] for q in d["queues"]), d' <"$work/queues.json"
curl --fail --silent --show-error "$url/metrics" >"$work/metrics.txt"
grep -q '^queuelens_heartbeats_accepted_total 1' "$work/metrics.txt"
docker stop --time 10 "$collector" >/dev/null
[[ $(docker inspect --format '{{.State.ExitCode}}' "$collector") == 0 ]]
printf 'QueueLens container: nonroot/read-only startup, migrations, Redis polling, auth, events, heartbeats and shutdown passed.\n'
