# Tenant Provisioning Control Plane

A control-plane service for a multi-tenant SaaS. It records tenants, starts
asynchronous provisioning **tasks** for them, publishes those tasks to
RabbitMQ, and consumes the progress workers report back. A worker simulator
does the "provisioning" part.

**Stack:** Python 3.12 · FastAPI · SQLAlchemy 2 + Alembic · PostgreSQL 16 · RabbitMQ 4 (pika) · Docker Compose · Make

---

## Contents

1. [Quick start](#1-quick-start)
2. [Architecture](#2-architecture)
3. [Project structure](#3-project-structure)
4. [Domain and state machines](#4-domain-and-state-machines)
5. [HTTP API](#5-http-api)
6. [Messaging](#6-messaging)
7. [Failure and chaos scenarios](#7-failure-and-chaos-scenarios)
8. [Testing](#8-testing)
9. [Quality gates](#9-quality-gates)
10. [Design decisions and trade-offs](#10-design-decisions-and-trade-offs)
11. [Configuration](#11-configuration)
12. [Known limitations and next steps](#12-known-limitations-and-next-steps)

---

## 1. Quick start

**Requirements:** Docker with Compose v2, and `make`. Python is not needed on the host; everything runs in containers.

```bash
git clone <this-repo> && cd tenant-control-plane
make up      # builds and boots everything; waits until all services are healthy
make test    # runs the full suite: unit + integration + end-to-end against the live stack
```

`make up` copies `.env.example` to `.env` the first time. `.env` holds local
throwaway credentials and is git-ignored. Then it starts PostgreSQL, RabbitMQ,
the API (which runs migrations first), the outbox relay, the task-update
consumer and the worker simulator.

| What | Where |
|---|---|
| API | http://localhost:8000/api/v1 |
| OpenAPI / Swagger UI | http://localhost:8000/docs |
| Health | http://localhost:8000/health |
| RabbitMQ management UI | http://localhost:15672 (credentials in `.env`) |

Watch a tenant go through its whole lifecycle with no manual steps:

```bash
curl -s -X POST localhost:8000/api/v1/tenants \
  -H 'content-type: application/json' -d '{"slug":"acme","name":"Acme Corp"}'
# -> 201, tenant "provisioning", deploy task "accepted"

# about 2 s later the worker has reported in_progress, then done:
curl -s localhost:8000/api/v1/tenants/<tenant_id>             # status "active", version 2
curl -s "localhost:8000/api/v1/tasks?tenant_id=<tenant_id>"   # deploy task "done"
```

Other targets (`make help`):

| Command | Does |
|---|---|
| `make up` | Build and boot the stack |
| `make test` | Full test suite (boots the stack if needed) |
| `make lint` | Formatter check, linter, type checker, security scanner |
| `make check` | `lint` + `test`, which is what CI runs |
| `make audit` | Scan locked dependencies for known CVEs (`pip-audit`) |
| `make format` | Auto-format and auto-fix your working tree |
| `make worker ARGS="..."` | Re-run the worker with different flags (see [§7](#7-failure-and-chaos-scenarios)) |
| `make logs` / `make ps` | Follow logs / service status |
| `make down` | Stop (data kept: tenants survive restarts) |
| `make clean` | Stop and delete all data volumes |

---

## 2. Architecture

```mermaid
flowchart LR
    client([HTTP client])

    subgraph cp["Control plane (one image, three processes)"]
        api["<b>api</b><br/>FastAPI"]
        relay["<b>relay</b><br/>outbox → broker"]
        consumer["<b>consumer</b><br/>task updates → DB"]
    end

    db[("PostgreSQL<br/>tenants · tasks<br/>outbox_messages · processed_messages")]

    subgraph mq["RabbitMQ"]
        tx{{"control_plane.tasks<br/>(topic)"}}
        tq[["worker.tasks<br/>(quorum)"]]
        ux{{"control_plane.task_updates<br/>(topic)"}}
        uq[["control_plane.task_updates<br/>(quorum)"]]
        dlq[["*.dead_letter"]]
    end

    worker["<b>worker</b><br/>simulator"]

    client -- REST --> api
    api -- "1 tx: tenant + task + outbox row" --> db
    relay -- "2 poll committed rows" --> db
    relay -- "3 publish (confirmed)" --> tx --> tq --> worker
    worker -- "4 in_progress / done / failed" --> ux --> uq --> consumer
    consumer -- "5 1 tx: inbox + task (+ tenant)" --> db
    uq -. "poison / delivery limit" .-> dlq
```

### Request flow

1. **API** handles e.g. `POST /tenants`. In **one database transaction** it
   writes the tenant, its new task, and an `outbox_messages` row containing
   the task event, then returns `201`/`202`. Nothing touches the broker
   during the request.
2. **Relay** polls `outbox_messages` for unpublished rows. Uncommitted rows are
   invisible to it, so an event is published **only after the commit**. A
   rollback leaves no row, so nothing is published.
3. Relay publishes each event with **publisher confirms** and marks the row
   published only after the broker confirmed it.
4. **Worker** consumes the task, publishes `in_progress`, simulates work,
   publishes `done` or `failed`, and only then acks the task.
5. **Consumer** applies each update in one transaction: it records the
   update's id in `processed_messages` (the idempotency inbox), moves the task
   forward, and on a terminal outcome moves the tenant.

### Processes

| Service | Command | Responsibility |
|---|---|---|
| `api` | `control-plane api` | REST API; runs `alembic upgrade head` on start |
| `relay` | `control-plane relay` | Transactional outbox → `control_plane.tasks` |
| `consumer` | `control-plane consumer` | `control_plane.task_updates` → tasks/tenants |
| `worker` | `worker-simulator` | Consumes tasks, publishes simulated progress |
| `postgres`, `rabbitmq` | | Infrastructure (named volumes, so state persists) |

### Layering

```
api (HTTP: schemas, routes, error mapping)     messaging (AMQP: relay, consumer, contracts)
                    \                                   /
                     ──────────►  services  ◄──────────        one transaction per use case
                                     │
                     domain (pure rules)   repositories (queries)   →   db (ORM models)
```

* `domain` holds pure business rules (statuses, state machines, guards, errors), with no frameworks and no I/O.
* `repositories` own the SQL. `services` own the business rules and the transaction boundary.
* `api` and `messaging` are thin adapters: they parse input, call a service, and map the result to HTTP or AMQP.

---

## 3. Project structure

```
.
├── src/
│   ├── control_plane/
│   │   ├── api/                  # HTTP adapter
│   │   │   ├── app.py            #   application factory
│   │   │   ├── dependencies.py   #   per-request DB session, services, pagination
│   │   │   ├── errors.py         #   domain error → HTTP status + error body
│   │   │   ├── schemas.py        #   request/response models
│   │   │   └── routes/           #   tenants.py · tasks.py · health.py
│   │   ├── core/                 # config (env), logging, clock/timestamp format
│   │   ├── domain/               # enums, state machines & guards (rules.py), errors
│   │   ├── db/                   # ORM models, session factory
│   │   ├── repositories/         # tenants · tasks · outbox · inbox · pagination
│   │   ├── services/             # tenants (commands) · tasks (queries) · task_updates
│   │   ├── messaging/            # contracts · topology · connection · publisher
│   │   │                         # outbox_relay · consumer
│   │   ├── bootstrap.py          # composition root: settings → api / relay / consumer
│   │   └── cli.py                # `control-plane api|relay|consumer`
│   └── worker_simulator/         # config (flags/env) · simulator · cli
├── migrations/                   # Alembic (schema is built only through migrations)
├── tests/
│   ├── unit/                     # pure logic, no I/O
│   ├── integration/              # real PostgreSQL: API, consumer, relay, races
│   └── e2e/                      # live stack: real broker, worker, HTTP races
├── scripts/publish-task-update.sh  # publish raw updates via RabbitMQ's HTTP API
├── docker/postgres/              # init script (creates the test database)
├── Dockerfile                    # multi-stage: runtime / dev
├── compose.yaml
├── Makefile                      # the command runner
├── pyproject.toml                # deps + ruff / mypy / bandit / pytest config
└── requirements*.lock            # pinned, reproducible dependency sets
```

---

## 4. Domain and state machines

### Tenant

| Field | Rules |
|---|---|
| `id` | UUID, server-generated |
| `slug` | `^[a-z][-a-z0-9]{1,26}[a-z0-9]$` (3–28 chars, k8s-namespace compatible). Unique **forever**: destroyed tenants keep their row, so their slug stays reserved. Immutable. |
| `name` | Non-empty after trimming, ≤ 255 chars |
| `status` | `provisioning` · `active` · `updating` · `destroying` · `destroyed` · `failed` |
| `version` | Starts at 1; +1 on **every** mutation, including worker-driven status changes |
| `created_at` / `updated_at` | ISO 8601, milliseconds, `Z`, e.g. `2026-01-31T12:00:00.123Z` |

### Task

| Field | Rules |
|---|---|
| `id` | UUID, server-generated. It is also the event id |
| `type` | `deploy` · `update` · `destroy` |
| `tenant_id` | The tenant it operates on |
| `status` | `accepted` → `in_progress` → `done` \| `failed` |
| `error` | Failure reason reported by the worker (nullable) |
| `created_at` / `updated_at` | Same format as tenants |

### Transitions

```mermaid
stateDiagram-v2
    direction LR
    [*] --> provisioning : POST (deploy task)
    provisioning --> active : deploy done
    provisioning --> failed : deploy failed
    active --> updating : PATCH (update task)
    updating --> active : update done
    updating --> failed : update failed
    active --> destroying : DELETE (destroy task)
    failed --> destroying : DELETE (destroy task)
    destroying --> destroyed : destroy done
    destroying --> failed : destroy failed
```

All rules live in [`domain/rules.py`](src/control_plane/domain/rules.py):

* **API guards.** `PATCH` only from `active`. `DELETE` only from `active` or `failed`. Anything else returns `tenant_update_not_allowed`.
* **Task transitions are forward-only.** Rank: `accepted(0) < in_progress(1) < done|failed(2)`. An update applies only if it raises the rank, so terminal statuses are final and stale updates are no-ops.
* **At most one open task per tenant.** This is not a separate lock; it follows from the guards. Every task-starting call moves the tenant into a transitional status, from which no further task can start until the open one is terminal.

---

## 5. HTTP API

Base path `/api/v1`. JSON in, JSON out. The interactive spec is at `/docs`.

| Method | Path | Success | Description |
|---|---|---|---|
| `POST` | `/tenants` | `201` + `Location` | Create a tenant and start its `deploy` task |
| `GET` | `/tenants` | `200` | List tenants (`?status=`, `?limit=`, `?offset=`) |
| `GET` | `/tenants/{id}` | `200` | Get a tenant |
| `PATCH` | `/tenants/{id}` | `202` | Partial update, start an `update` task. Body needs `version` |
| `DELETE` | `/tenants/{id}` | `202` | Start a `destroy` task |
| `GET` | `/tasks` | `200` | List tasks (`?tenant_id=`, `?status=`, `?type=`, `?limit=`, `?offset=`) |
| `GET` | `/tasks/{id}` | `200` | Get a task |
| `GET` | `/health` | `200` / `503` | Liveness + database check |

**Conventions**

* Mutations are asynchronous, so `PATCH`/`DELETE` return **`202 Accepted`**.
  Every mutating call returns `{"tenant": …, "task": …}`, so the caller gets
  the id of the task to follow.
* `DELETE` does not remove the row. The tenant moves to `destroying`, then `destroyed`, and stays readable.
* Pagination: `limit` (1–100, default 20) and `offset` (≥ 0). Lists return
  `{"items": [...], "total", "limit", "offset"}`, ordered by `created_at`
  ascending, so a tenant's tasks read as its history.
* Filters are plain query parameters on the collection. Tasks are filtered by
  `tenant_id` rather than nested under tenants, so there is exactly one task
  collection.
* Tasks are read-only: `POST/PATCH/DELETE /tasks…` returns `405`.

### Examples

```bash
API=localhost:8000/api/v1

# Create
curl -si -X POST $API/tenants -H 'content-type: application/json' \
     -d '{"slug":"acme","name":"Acme Corp"}'
```
```http
HTTP/1.1 201 Created
location: http://localhost:8000/api/v1/tenants/3f1c…

{"tenant":{"id":"3f1c…","slug":"acme","name":"Acme Corp","status":"provisioning","version":1,
           "created_at":"2026-11-14T18:32:07.012Z","updated_at":"2026-11-14T18:32:07.012Z"},
 "task":{"id":"9b2e…","type":"deploy","tenant_id":"3f1c…","status":"accepted","error":null,
         "created_at":"2026-11-14T18:32:07.012Z","updated_at":"2026-11-14T18:32:07.012Z"}}
```
```bash
# Read / list
curl -s $API/tenants/<tenant_id>
curl -s "$API/tenants?status=active&limit=10"
curl -s "$API/tasks?tenant_id=<tenant_id>"
curl -s $API/tasks/<task_id>

# Update: send the version you last read (optimistic locking)
curl -s -X PATCH $API/tenants/<tenant_id> -H 'content-type: application/json' \
     -d '{"version":2,"name":"Acme Inc"}'

# Delete
curl -s -X DELETE $API/tenants/<tenant_id>
```

### Errors

Every error has the same shape, so clients branch on `error.code`:

```json
{"error": {"code": "tenant_version_conflict",
           "message": "Tenant 3f1c… is at version 3, not 2.",
           "details": {"tenant_id": "3f1c…", "expected_version": 2, "current_version": 3}}}
```

| `code` | HTTP | When |
|---|---|---|
| `validation_error` | 422 | Bad body/query/path: invalid slug, empty name, missing `version`, unknown or immutable field, malformed JSON. `details` lists `field`/`message`/`type` |
| `tenant_not_found` | 404 | Unknown tenant id |
| `task_not_found` | 404 | Unknown task id |
| `tenant_already_exists` | 409 | Slug taken (including by a destroyed tenant) |
| `tenant_update_not_allowed` | 409 | `PATCH`/`DELETE` from a status the state machine forbids. `details.status`, `details.operation` |
| `tenant_version_conflict` | 409 | `PATCH` with a `version` that is not the current one |
| `resource_not_found` | 404 | Unknown route |
| `method_not_allowed` | 405 | e.g. writing to `/tasks` |
| `internal_error` | 500 | Unexpected error (logged server-side; no stack trace leaves the API) |

For `PATCH`, the version is checked first, then the state guard: a client
holding a stale representation is told to re-read before anything else.

---

## 6. Messaging

### Topology

Every process declares it idempotently on connect
([`messaging/topology.py`](src/control_plane/messaging/topology.py)).

| Exchange (type) | Routing keys | Queue | Consumer |
|---|---|---|---|
| `control_plane.tasks` (topic) | `task.deploy` · `task.update` · `task.destroy` | `worker.tasks` | worker |
| `control_plane.task_updates` (topic) | `task_update.in_progress` · `.done` · `.failed` | `control_plane.task_updates` | consumer |
| `control_plane.dead_letter` (direct) | `<queue name>` | `<queue>.dead_letter` | — (inspect manually) |

Both work queues are **durable quorum queues** with `x-delivery-limit: 5` and
dead-lettering. The control plane also declares `worker.tasks`, so tasks
published before any worker exists are queued, not dropped. All messages are
persistent JSON, published `mandatory` with publisher confirms.

### Outbound: task event (`control_plane.tasks`)

The task *is* the event. AMQP `message_id` = task `id`.

```json
{"id": "9b2e…", "type": "deploy", "tenant_id": "3f1c…", "status": "accepted",
 "created_at": "2026-11-14T18:32:07.012Z"}
```

### Inbound: task update (`control_plane.task_updates`)

```json
{"message_id": "c0a8…", "task_id": "9b2e…", "status": "done",
 "occurred_at": "2026-11-14T18:32:09.250Z", "error": null}
```

* `message_id` is the **idempotency key**. The worker generates it once per
  logical update (a fresh UUID4), and it stays the same across redeliveries
  and deliberate re-publishes.
* `task_id` is the reference to the task being updated.
* `status` is one of `in_progress`, `done`, `failed` (`accepted` is rejected).
* `error` is an optional failure reason, stored on the task.
* Unknown fields are ignored, for forward compatibility.

### Delivery guarantees

| Requirement | How |
|---|---|
| Publish **after** commit, never on rollback | Transactional outbox: the event row commits atomically with the tenant/task; the relay only sees committed rows |
| Exactly one event per state-changing call | One outbox row per task (`message_id` unique). Failed calls roll back their row |
| At-least-once out | Row marked published only after the broker's confirm. A crash in between republishes it; consumers are idempotent |
| **Idempotent** in | `processed_messages` inbox keyed by `message_id`, written in the *same* transaction as the state change (`INSERT … ON CONFLICT DO NOTHING`). A repeat changes nothing and is acked |
| **Order-tolerant** in | Forward-only task transitions (§4). Stale updates are acked and recorded as `stale` with no effect. A terminal update arriving *before* `in_progress` is applied; the late `in_progress` is then ignored |
| Concurrent duplicates | The task row is locked (`SELECT … FOR UPDATE`) before the inbox insert, so parallel consumers serialise per task |
| **Poison** messages | Malformed body or unknown `task_id` → `basic_reject(requeue=False)` → dead-letter queue immediately. The consumer keeps going |
| **Transient** failures | DB errors or bugs → back off (exponential, jittered, from `x-delivery-count`) then `nack(requeue=True)`. After 5 deliveries the broker dead-letters it |
| Broker outages | Every process reconnects with exponential backoff and jitter (0.5 s doubling to a 30 s cap) and redeclares the topology |
| Multiple relays | `SELECT … FOR UPDATE SKIP LOCKED` keeps replicas from publishing the same rows concurrently |

---

## 7. Failure and chaos scenarios

### A. Restart the worker with different behaviour

| Flag | Env var | Default | Effect |
|---|---|---|---|
| `--fail-rate` | `WORKER_FAIL_RATE` | `0` | Probability [0–1] a task ends `failed` |
| `--min-delay-ms` | `WORKER_MIN_DELAY_MS` | `500` | Minimum simulated work time |
| `--max-delay-ms` | `WORKER_MAX_DELAY_MS` | `2000` | Maximum simulated work time |
| `--duplicate-rate` | `WORKER_DUPLICATE_RATE` | `0` | Probability every update of a task is sent **twice** (same `message_id`) |
| `--out-of-order-rate` | `WORKER_OUT_OF_ORDER_RATE` | `0` | Probability the terminal update is sent **before** `in_progress` |
| `--seed` | `WORKER_SEED` | random | Reproducible outcomes and timings |

Flags take precedence over env vars. Either way works:

```bash
# Foreground, with flags (Ctrl-C to stop; `make up` restores the default worker)
make worker ARGS="--fail-rate=1"
make worker ARGS="--duplicate-rate=1 --out-of-order-rate=1 --min-delay-ms=3000 --max-delay-ms=5000"

# Background, with env vars
WORKER_FAIL_RATE=1 docker compose up -d worker
docker compose up -d worker          # back to the .env defaults
```

With `--fail-rate=1`, a new tenant ends `failed`, and its deploy task is
`failed` with `error: "simulated provisioning failure"`. `DELETE` is then
allowed (`failed → destroying`). Restart the worker normally and the destroy
completes (`destroyed`).

With duplicates and out-of-order delivery on, the tenant still converges to
`active` with `version` 2. `docker compose logs consumer` shows `duplicate`
and `stale` outcomes being acked.

### B. Publish messages straight to the broker

[`scripts/publish-task-update.sh`](scripts/publish-task-update.sh) uses
RabbitMQ's own management HTTP API, so it needs only `curl`:

```bash
TASK=<a task id from GET /api/v1/tasks>

# Duplicate: same message_id twice -> second one is a no-op
scripts/publish-task-update.sh $TASK in_progress 11111111-1111-4111-8111-111111111111
scripts/publish-task-update.sh $TASK in_progress 11111111-1111-4111-8111-111111111111

# Out of order / stale: after the task is done, these change nothing
scripts/publish-task-update.sh $TASK in_progress
scripts/publish-task-update.sh $TASK failed

# Poison: dead-lettered, consumer keeps running
scripts/publish-task-update.sh --raw 'this is not json'
scripts/publish-task-update.sh 00000000-0000-4000-8000-000000000000 done   # unknown task
```

Each call prints `{"routed":true}`. Inspect the dead-letter queue
`control_plane.task_updates.dead_letter` in the management UI
(http://localhost:15672 → Queues), or with:

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages
```

Equivalent raw `curl`, if you prefer no script:

```bash
source .env
curl -u "$RABBITMQ_USER:$RABBITMQ_PASSWORD" -H 'content-type: application/json' \
  -X POST localhost:15672/api/exchanges/%2F/control_plane.task_updates/publish \
  -d '{"properties":{"message_id":"m-1"},"routing_key":"task_update.done","payload_encoding":"string",
       "payload":"{\"message_id\":\"6a0b0c1e-1d4f-4c55-9a43-2f5b8c7d9e01\",\"task_id\":\"'"$TASK"'\",\"status\":\"done\",\"occurred_at\":\"2026-01-01T00:00:00.000Z\"}"}'
```

### C. Infrastructure failures

```bash
docker compose stop rabbitmq   # API keeps accepting writes; events queue up in the outbox
docker compose start rabbitmq  # relay/consumer/worker reconnect; backlog drains, tenants converge
docker compose restart postgres  # consumer retries updates with backoff; nothing is lost
make down && make up           # tenants and tasks survive (named volumes)
```

---

## 8. Testing

`make test` runs about 210 tests in a container (the `tests` Compose service,
`dev` image) and prints coverage.

| Suite | Needs | Covers |
|---|---|---|
| `tests/unit` | nothing | State machines (full transition matrix), guards, slug/name validation, timestamp format, wire contracts, backoff, worker update planning and flags, pika adapters (ack / reject / nack, topology, publish properties) with mocked channels |
| `tests/integration` | PostgreSQL | Every endpoint and error code. Optimistic locking. Every allowed and forbidden transition. Consumer idempotency, ordering, poison and transient handling. Outbox "after commit only", partial-failure and `SKIP LOCKED` behaviour. **Concurrency races** with real parallel transactions. The whole pipeline in-process (API → outbox → relay → simulator plan → consumer) under duplicate and out-of-order chaos |
| `tests/e2e` | Running stack | Real broker, relay, consumer and worker: full lifecycle, duplicates/stale/poison published to RabbitMQ, dead-lettering, consumer survival, and concurrent HTTP `PATCH` and create races |

**Concurrency tests** ([`test_concurrency.py`](tests/integration/test_concurrency.py),
[`test_live_stack.py`](tests/e2e/test_live_stack.py)) start 16–20 threads at
once (barrier). Each uses its own connection, and the tests assert exact
outcomes:

* N creates with one slug → exactly **1** created, N-1 `tenant_already_exists`, exactly one task and one outbox event.
* N `PATCH`es with the same version → exactly **1** accepted, N-1 `tenant_version_conflict`, version +1 once.
* N `DELETE`s → exactly **1** destroy task.
* Mixed `PATCH`/`DELETE` storm → still exactly one open task.
* N consumers receiving the same update at the same moment → applied once.

Integration tests build the schema **by running the Alembic migrations**, so
migrations are tested too. They use a separate `control_plane_test` database
and never touch live data. Without the stack, integration and e2e tests skip
themselves.

Running locally without Docker (optional):

```bash
python3.12 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.lock && pip install --no-deps -e .
TEST_DATABASE_URL=postgresql+psycopg://user:pass@localhost:5432/control_plane_test pytest
```

---

## 9. Quality gates

`make lint` (and `make check` = lint + test, also run by
[CI](.github/workflows/ci.yml)):

| Gate | Tool |
|---|---|
| Formatter (all code formatted) | `ruff format --check` |
| Linter (pycodestyle, pyflakes, isort, bugbear, pyupgrade, bandit rules, naming, pytest style…) | `ruff check` |
| Static typing, `strict` mode, over `src/`, `tests/`, `migrations/` | `mypy` (+ pydantic plugin) |
| Security: code | `bandit` |
| Security: dependencies | `pip-audit` against `requirements.lock` (`make audit`, needs network) |
| Tests + coverage | `pytest` + `pytest-cov` |

Dependencies are pinned in `requirements.lock` / `requirements-dev.lock`.
Regenerate them with
`uv pip compile pyproject.toml [--extra dev] --python-version 3.12 --python-platform linux -o <file>`.

**Secrets:** none in the repo. `.env` is git-ignored. `.env.example` holds
only placeholder local-container credentials, and Compose refuses to start if
they are missing.

---

## 10. Design decisions and trade-offs

**Transactional outbox, not "publish after commit" in the request.**
Publishing right after `COMMIT` in the handler still loses the event if the
process dies between commit and publish, and couples API latency and
availability to the broker. With the outbox, the event is durable the moment
the data is, and the API keeps working while RabbitMQ is down.
*Cost:* a polling relay process and about 0.5 s of extra latency.
LISTEN/NOTIFY or CDC (Debezium) would remove the polling if that ever matters.

**Idempotent inbox, not dedup in memory or in the broker.**
The `processed_messages` insert sits in the same transaction as the state
change, so "processed" and "applied" can never disagree, even across
consumer restarts and replicas.
*Cost:* one row per update. A TTL cleanup job would bound it.

**Order tolerance from a forward-only state machine, not sequence numbers.**
Each task has a tiny, totally ordered lifecycle, so "only move forward" makes
reordering, duplicates and stragglers all harmless with no per-task sequence
bookkeeping. I deliberately allow `accepted → done|failed`: if the terminal
update overtakes `in_progress`, rejecting it would lose the outcome forever.

**Optimistic locking: version precondition + short row lock + versioned UPDATE.**
Clients send the `version` they read (no lock is held between requests). On
the server, `PATCH`/`DELETE` load the row `FOR UPDATE` for the few
milliseconds of the transaction, so concurrent writers serialise and the
loser gets a *deterministic*, correct error (`tenant_version_conflict` or
`tenant_update_not_allowed`). The model also uses SQLAlchemy's
`version_id_col`, so every tenant UPDATE, the consumer's included, bumps
`version` and carries `WHERE version = <read>`. That is a second, independent
guard against lost updates.
*Alternative:* a single conditional `UPDATE … WHERE version = :v` (no lock).
It is equally correct, but a 0-row result then needs a re-read to tell
"conflict" from "not allowed" apart.

**Slug uniqueness from the database.** A unique constraint is the only thing
that is race-proof. Of N concurrent inserts, one commits and the rest get a
unique violation, mapped by constraint name to `tenant_already_exists`. Soft
deletion (`destroyed` rows stay) keeps the slug reserved, as required.

**Quorum queues with a delivery limit and DLX.** A broker-native poison-message
guard. A message that keeps failing for any reason (even a bug) is
dead-lettered after 5 deliveries instead of blocking the queue forever. No
custom retry-count headers or republish logic.

**Separate processes from one image.** API, relay and consumer scale and fail
independently, and stay simple: synchronous code, `pika`'s
`BlockingConnection`, no event-loop juggling. *Cost:* more containers. They
are small and share one image.

**Sync FastAPI + SQLAlchemy.** Handlers are short DB transactions; sync code
runs in FastAPI's threadpool and is easier to reason about (and to test for
races) than async. Async would only pay off at much higher concurrency.

**`PATCH` applies `name` immediately.** The new name is stored as the tenant
moves to `updating`, and the `update` task "applies" it. If the update fails,
the tenant is `failed` with the new name recorded, which is the desired
state to retry or destroy from.

**Worker message ids are never derived from `--seed`.** The seed drives
outcomes and timing only. Seeded ids would make a restarted worker, or two
workers sharing a seed, reuse idempotency keys, and their updates would be
silently dropped as duplicates. A regression test covers this.

---

## 11. Configuration

Control-plane processes read environment variables
([`core/config.py`](src/control_plane/core/config.py)). Compose builds the
URLs from `.env`.

| Variable | Default | Used by |
|---|---|---|
| `DATABASE_URL` | *required* | api, relay, consumer |
| `RABBITMQ_URL` | `amqp://localhost:5672/%2F` | relay, consumer, worker |
| `LOG_LEVEL` | `INFO` | all |
| `API_HOST` / `API_PORT` | `0.0.0.0` / `8000` | api |
| `OUTBOX_BATCH_SIZE` | `100` | relay |
| `OUTBOX_POLL_INTERVAL_SECONDS` | `0.5` | relay |
| `CONSUMER_PREFETCH_COUNT` | `10` | consumer |
| `WORKER_*` | see [§7](#7-failure-and-chaos-scenarios) | worker |

---

## 12. Known limitations and next steps

Left out deliberately to keep the scope on correctness:

* **No authentication or authorisation.** In production the API would sit behind an auth gateway or OIDC.
* **Offset pagination.** Simple and documented. Cursor pagination would be stable under concurrent inserts.
* **Inbox and outbox grow forever.** Add a retention job (for example, delete published or processed rows older than N days).
* **No metrics or tracing.** Next would be outbox lag, consumer outcomes per type (`applied`/`stale`/`duplicate`), DLQ depth, and OpenTelemetry trace propagation through AMQP headers.
* **Migrations run on API start.** Fine for one replica. With several, run them as a one-off job before rollout.
* **The worker processes one task at a time** (`prefetch=1`). Scale it with `docker compose up -d --scale worker=3`.
