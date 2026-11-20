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
  # The entrypoint's temporary initialization server listens only on a Unix
  # socket. Wait for the final TCP listener before starting the collector.
  if docker exec "$postgres" pg_isready -h 127.0.0.1 -U queuelens >/dev/null 2>&1 &&
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
  -e QUEUELENS_DASHBOARD_PASSWORD=container-smoke-viewer-password \
  -e QUEUELENS_DASHBOARD_ORIGIN=http://localhost:8080 \
  -e QUEUELENS_DASHBOARD_INSECURE_DEV=true \
  "$image" >/dev/null
port=$(docker port "$collector" 8080/tcp)
url="http://$port"
ready=false
for (( i=0; i<60; i++ )); do
  # A newly published Docker port may briefly reset a connection before the
  # application is listening. Bound retries for all startup failures.
  if curl --fail --silent --max-time 2 "$url/readyz" >"$work/ready.json"; then
    ready=true
    break
  fi
  sleep 1
done
[[ $ready == true ]]
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

# M3 must also work through the packaged runtime's projector and query wiring.
for path in jobs/failed jobs/stalled jobs/suspected-stalled workers; do
  [[ $(curl --silent --max-time 5 --output /dev/null --write-out '%{http_code}' \
    "$url/v1/$path") == 401 ]]
done
[[ $(curl --silent --max-time 5 --output "$work/lifecycle-events.json" --write-out '%{http_code}' \
  -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data "[{\"v\":1,\"id\":\"image-failure-event\",\"ts\":\"$timestamp\",\"queue\":\"image-smoke\",\"job_id\":\"image-failed\",\"state\":\"failed\"},{\"v\":1,\"id\":\"image-stall-event\",\"ts\":\"$timestamp\",\"queue\":\"image-smoke\",\"job_id\":\"image-stalled\",\"state\":\"stalled\"}]" \
  "$url/v1/events") == 202 ]]
python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["accepted"]==2 and d["dropped"]==0 and not d["rejected"], d' <"$work/lifecycle-events.json"
projected=false
for (( i=0; i<30; i++ )); do
  count=$(docker exec "$postgres" psql -U queuelens -Atc \
    "SELECT count(*) FROM job_runs WHERE queue='image-smoke' AND ((job_id='image-failed' AND state='failed') OR (job_id='image-stalled' AND state='stalled'))")
  if [[ $count == 2 ]]; then projected=true; break; fi
  sleep 1
done
[[ $projected == true ]]
[[ $(docker exec "$postgres" psql -U queuelens -Atc \
  "SELECT count(*) FROM queue_events WHERE job_id='image-job' AND event_id IS NULL AND identity_kind='missing'") == 1 ]]
for state in failed stalled; do
  curl --fail --silent --show-error --max-time 5 -H "Authorization: Bearer $token" \
    "$url/v1/jobs/$state?queue=image-smoke&limit=1" >"$work/$state.json"
  python3 -c 'import json,sys; d=json.load(sys.stdin); item=d["items"][0]; assert len(d["items"])==1 and item["state"]==sys.argv[1] and item["state_evidence"]=="observed" and item["retry_exhausted"] is None and not item["history"]["complete_history"] and d["projection_lag"]["known"], d' "$state" <"$work/$state.json"
done
curl --fail --silent --show-error --max-time 5 -H "Authorization: Bearer $token" \
  "$url/v1/jobs/suspected-stalled?queue=image-smoke" >"$work/suspected.json"
python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["items"]==[], d' <"$work/suspected.json"
curl --fail --silent --show-error --max-time 5 -H "Authorization: Bearer $token" \
  "$url/v1/workers?limit=1" >"$work/workers.json"
python3 -c 'import json,sys; d=json.load(sys.stdin); assert len(d["items"])==1 and d["items"][0]["worker_id"]=="image-worker" and d["items"][0]["status"]=="alive", d' <"$work/workers.json"

# M4 production assets, read-only session and retained timeline all use the
# packaged binary. Host/Origin emulate a proxy forwarding its configured host.
dashboard_headers=(-H 'Host: localhost:8080' -H 'Origin: http://localhost:8080')
curl --fail --silent --show-error "${dashboard_headers[@]}" "$url/" >"$work/dashboard.html"
grep -q '<div id="root">' "$work/dashboard.html"
[[ $(curl --silent --output /dev/null --write-out '%{http_code}' \
  "${dashboard_headers[@]}" "$url/dashboard/api/overview") == 401 ]]
curl --fail --silent --show-error "${dashboard_headers[@]}" -c "$work/cookies" \
  -H 'Content-Type: application/json' --data '{"password":"container-smoke-viewer-password"}' \
  "$url/dashboard/session" >"$work/session.json"
python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["authenticated"] and d["expires_at"], d' <"$work/session.json"
curl --fail --silent --show-error "${dashboard_headers[@]}" -b "$work/cookies" \
  "$url/dashboard/api/job?queue=image-smoke&job_id=image-failed" >"$work/timeline.json"
python3 -c 'import json,sys; d=json.load(sys.stdin); j=d["job"]; assert j["availability"]=="observed" and len(j["attempts"])==1 and j["summary"]["retry_exhausted"] is None, d' <"$work/timeline.json"
curl --fail --silent --show-error "${dashboard_headers[@]}" -b "$work/cookies" \
  "$url/dashboard/api/overview" >"$work/overview.json"
python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["metrics"]["available"] and d["queues"]["source"]=="last_broker_poll", d' <"$work/overview.json"
[[ $(curl --silent --output /dev/null --write-out '%{http_code}' \
  "${dashboard_headers[@]}" -b "$work/cookies" "$url/v1/dashboard/overview") == 401 ]]
[[ $(curl --silent --output /dev/null --write-out '%{http_code}' \
  "${dashboard_headers[@]}" -b "$work/cookies" -X DELETE "$url/dashboard/session") == 204 ]]
[[ $(curl --silent --output /dev/null --write-out '%{http_code}' \
  "${dashboard_headers[@]}" -b "$work/cookies" "$url/dashboard/api/overview") == 401 ]]
docker stop --time 10 "$collector" >/dev/null
[[ $(docker inspect --format '{{.State.ExitCode}}' "$collector") == 0 ]]
printf 'QueueLens container: nonroot/read-only startup, migrations, Redis polling, auth, identity, lifecycle queries, embedded dashboard/session/timeline, heartbeats and shutdown passed.\n'
