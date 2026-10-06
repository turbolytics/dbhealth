#!/usr/bin/env bash
# The release test: the built image against a real Postgres.
#
#   scripts/release-test.sh dbhealth:dev
#
# Starts postgres:18 with one table, runs the image's validate, then runs
# it for three 2-second intervals reporting to a StatsD listener here, and
# checks the gauges: the probe answered, the table was found with its row
# count, and nothing in the log carries the password. Everything it starts
# is removed on exit.
set -euo pipefail
IMAGE=${1:?image}
NET=dbhealth-rt-$$
PG=dbhealth-rt-pg-$$
RUN=dbhealth-rt-run-$$
SD=dbhealth-rt-statsd-$$
PASS=rt-secret-$$
WORK=$(mktemp -d)
cleanup() {
  docker rm -f "$PG" "$RUN" "$SD" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

docker network create "$NET" >/dev/null
docker run -d --name "$PG" --network "$NET" -e POSTGRES_USER=rt -e POSTGRES_PASSWORD="$PASS" -e POSTGRES_DB=rt postgres:18 >/dev/null
# Not pg_isready: the image's init starts a temporary server first, which
# answers pg_isready before the rt database exists. A query against rt is
# the only honest readiness.
for i in $(seq 1 60); do
  docker exec "$PG" psql -U rt -d rt -Atc "SELECT 1" >/dev/null 2>&1 && break
  sleep 1
done
docker exec "$PG" psql -U rt -d rt -Atc "SELECT 1" >/dev/null
docker exec "$PG" psql -U rt -d rt -q \
  -c "CREATE TABLE public.events (id serial PRIMARY KEY, created_at timestamptz NOT NULL DEFAULT now())" \
  -c "INSERT INTO public.events SELECT FROM generate_series(1, 250)" \
  -c "SELECT pg_stat_force_next_flush()" \
  -c "VACUUM ANALYZE public.events" >/dev/null

# StatsD: a listener on the same network that prints every datagram, so
# the test reads its logs. A container, not the host: on Docker Desktop
# the bridge gateway is not the host.
cat > "$WORK/statsd.py" <<'PY'
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("0.0.0.0", 8125))
while True:
    sys.stdout.write(s.recv(65535).decode())
    sys.stdout.flush()
PY
docker run -d --name "$SD" --network "$NET" -v "$WORK/statsd.py:/statsd.py:ro" python:3-alpine python -u /statsd.py >/dev/null

cat > "$WORK/dbhealth.yml" <<YML
databases:
  - kind: postgres
    dsn: "{{ DBHEALTH_DSN }}"
    name: release-test
probe:
  interval_seconds: 2
  timeout_seconds: 2
tables:
  discover:
    schemas: [public]
  rows: exact
  freshness_interval_seconds: 2
  rows_exact_interval_seconds: 2
report:
  statsd: ${SD}:8125
  metrics: prometheus
  listen: 0.0.0.0:8000
YML
DSN="postgres://rt:${PASS}@${PG}:5432/rt?sslmode=disable"

echo "validate:"
docker run --rm --network "$NET" -e DBHEALTH_DSN="$DSN" -v "$WORK/dbhealth.yml:/etc/dbhealth/dbhealth.yml:ro" "$IMAGE" validate -c /etc/dbhealth/dbhealth.yml | tee "$WORK/validate.txt"
grep -q "ok: 1 databases" "$WORK/validate.txt"

echo "run, 7s, then SIGTERM:"
docker run -d --name "$RUN" --network "$NET" -e DBHEALTH_DSN="$DSN" -v "$WORK/dbhealth.yml:/etc/dbhealth/dbhealth.yml:ro" "$IMAGE" run -c /etc/dbhealth/dbhealth.yml >/dev/null
sleep 5
# Scraped from the same network: /healthz and /metrics, as Prometheus would.
docker run --rm --network "$NET" curlimages/curl:8.11.1 -s "http://${RUN}:8000/healthz" > "$WORK/healthz.txt" || true
docker run --rm --network "$NET" curlimages/curl:8.11.1 -s "http://${RUN}:8000/metrics" > "$WORK/metrics.txt" || true
sleep 2
docker stop -t 10 "$RUN" >/dev/null
docker logs "$RUN" > "$WORK/run.log" 2>&1
cat "$WORK/run.log"
docker logs "$SD" > "$WORK/statsd.txt" 2>&1
echo "$(wc -l < "$WORK/statsd.txt") gauges received"

fail=0
check() { grep -q -- "$1" "$WORK/statsd.txt" && echo "ok   $1" || { echo "MISSING $1"; fail=1; }; }
check "dbhealth.probe.ok:1|g|#db:release-test"
check "dbhealth.connections.max:100|g|#db:release-test"
check "dbhealth.table.rows:250|g|#db:release-test,table:public.events"
check "dbhealth.table.rows_exact:1|g|#db:release-test,table:public.events"
check "dbhealth.table.newest_at:"
grep -q '"status":"healthy"' "$WORK/healthz.txt" && echo "ok   /healthz" || { echo "MISSING /healthz"; fail=1; }
grep -q 'dbhealth_probe_ok{db="release-test"} 1' "$WORK/metrics.txt" && echo "ok   /metrics probe_ok" || { echo "MISSING /metrics dbhealth_probe_ok"; fail=1; }
grep -q 'dbhealth_table_rows{db="release-test",table="public.events"} 250' "$WORK/metrics.txt" && echo "ok   /metrics table_rows" || { echo "MISSING /metrics dbhealth_table_rows"; fail=1; }
grep -q "dbhealth started" "$WORK/run.log" || { echo "MISSING started log line"; fail=1; }
grep -q "dbhealth stopped" "$WORK/run.log" || { echo "MISSING clean stop on SIGTERM"; fail=1; }
if grep -q "$PASS" "$WORK/run.log" "$WORK/statsd.txt" "$WORK/metrics.txt"; then echo "LEAK: the password is in the output"; fail=1; fi
exit $fail
