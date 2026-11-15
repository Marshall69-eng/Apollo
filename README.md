# NetZero Wave: storm baler siting, monitoring and DRRMO alerts

Dashboard and API for the Calumpang River watershed (Batangas City, Lipa City, San Jose,
Cuenca, Ibaan, Taysan, Rosario, Padre Garcia): Leaflet GIS map, site-selection scoring, live buoy
monitoring, and automated Telegram / SMS / webhook alerts to Barangay and LGU DRRMO personnel.

## Two modes

| | `SIMULATION_MODE=true` (default) | `SIMULATION_MODE=false` |
|---|---|---|
| Sensor data | random-walk simulator | real buoys via `POST /api/ingest/{site}` |
| Notifications | **always dry-run** (`simulated`) | real channels |
| UI | amber **DEMO DATA** banner | no banner |
| Startup | generates a demo admin password and prints it once | **refuses to start** without `SECRET_KEY` and `ADMIN_PASSWORD` |
| `/docs` | available | disabled |

Demo data can never page a real person: the simulator only runs in demo mode, and demo mode forces dry-run.

## Quick start (demo)

```bash
pip install -r requirements.txt
cp .env.example .env
uvicorn main:app --reload        # watch the console for the demo admin password
```

Open <http://127.0.0.1:8000> and sign in (username `admin`).

## Going live checklist

1. `.env`: `SIMULATION_MODE=false`, a 32+ character `SECRET_KEY`, `ADMIN_PASSWORD` (first admin only).
2. Add real recipients in the **Alerts** tab. Seeded placeholder recipients are never created in live
   mode, and any that exist from an earlier demo run are deactivated on startup.
   - Telegram: add the bot to the DRRMO group, read the chat id from
     `https://api.telegram.org/bot<TOKEN>/getUpdates`, add it as a recipient. (`TELEGRAM_CHAT_ID` in `.env` is
     no longer used: every recipient is messaged at **its own** chat id.)
   - Tier 1 = first responders. Tier 2 = escalation contacts (e.g. the provincial DRRMO).
3. Create more accounts: `POST /api/users` (admin). Change the admin password: `POST /api/auth/password`.
4. Issue one device key per buoy: `POST /api/sites/{site_id}/device-key` (admin). The key is shown once.
5. Put it behind HTTPS (`docker compose up -d` with `DOMAIN=your.domain` does this with Caddy).
6. Confirm the alert thresholds with the LGU DRRMOs and PAGASA (see "Data caveats").
7. Run a drill from the Alerts tab and confirm the right people received the labelled DRILL message.

## Roles

| Role | Can |
|---|---|
| viewer | read sites, units, alert log (phone numbers / chat ids masked) |
| operator | + acknowledge alerts, send drills (3/min), manage recipients, deploy / retire units |
| admin | + users, device keys, delete recipients, audit log |

Login is rate-limited (5 failures / 5 min per IP + username). Every state change is written to the
audit log (`GET /api/audit`). Rate limits are per worker process.

## Buoy ingest contract (changed from the prototype)

```bash
curl -X POST https://your.domain/api/ingest/S4 \
  -H "X-Device-Key: nzw_..." -H "Content-Type: application/json" \
  -d '{"flow": 1.8, "debris": 62, "flood": 55, "solar": 71}'
```

Ranges: `flow` 0-15 m/s, the others 0-100. Out-of-range or missing values get `422` and are not stored.
A wrong key or unknown site gets the same `401`. The call stores the reading and runs the alert engine.

## How alerting behaves

| Behaviour | Detail |
|---|---|
| Confirmation | a level must appear in **2 of the last 3** readings, so one noisy spike pages nobody |
| Hysteresis | once Warning/Critical, readings must fall **3 index points / 0.15 m/s below** the threshold to release, so a value hovering on the line cannot flap |
| Reminders | while a level persists and nobody has acknowledged, a reminder goes out every 10 min |
| Escalation | an alert unacknowledged for 15 min is sent to tier-2 recipients, once per severity per episode |
| All clear | when the site recovers, tier 1 (and tier 2 if it was escalated) gets an ALL CLEAR |
| Offline | a deployed buoy silent for `STALE_AFTER_SECONDS` raises a SENSOR OFFLINE alert; absence of data is not safety |
| Acknowledge | records who acknowledged, and clears the whole episode's reminders |
| Drills | clearly labelled **DRILL, no action required**; excluded from reminders and escalation |
| Delivery | alerts are queued in an outbox and sent by one background leader with retry and backoff (30 s, 1, 2, 4 min, 5 attempts). A slow channel never stalls the sensor loop. Status chips: `pending`, `sent`, `simulated`, `failed` |

Thresholds stay **locked in code** (`THRESHOLDS` in `engine.py`, read-only API, `405` on writes).
Changing one is a reviewed code change.

## Configuration

See `.env.example` for every variable. The important ones: `SIMULATION_MODE`, `SECRET_KEY`,
`ADMIN_PASSWORD`, `DB_PATH`, `STALE_AFTER_SECONDS`, `READINGS_RETENTION_DAYS`, `CORS_ORIGINS`,
`TELEGRAM_BOT_TOKEN`, `VONAGE_*`, `GENERIC_WEBHOOK_URL`, `WEBHOOK_SECRET`.
The webhook URL comes from the environment only, never from the UI, so an operator can't point the
server at an internal address.

## API summary

| Method | Path | Role |
|---|---|---|
| GET | `/healthz`, `/api/meta` | public |
| POST | `/api/auth/login` | public |
| GET / POST | `/api/auth/me`, `/api/auth/password` | any signed-in user |
| GET | `/api/sites`, `/api/units`, `/api/units/{id}`, `/api/sites/{id}/history`, `/api/geo/rivers`, `/api/alert-config`, `/api/alerts` | viewer |
| POST / DELETE | `/api/deploy/{site}`, `/api/units/{id}` | operator |
| GET / POST | `/api/recipients`, `/api/recipients/{id}/toggle` | operator |
| POST | `/api/alerts/{id}/ack`, `/api/alerts/test/{site}`, `/api/alerts/evaluate` | operator |
| DELETE | `/api/recipients/{id}` | admin |
| GET / POST / DELETE | `/api/users`, `/api/users/{name}` | admin |
| POST / GET | `/api/sites/{id}/device-key`, `/api/audit` | admin |
| POST | `/api/ingest/{site}` | device key |

## Tests

```bash
pip install -r requirements-dev.txt
python -m pytest -q          # or: python -m unittest discover -s tests -t .
```

`test_engine.py` and `test_auth_channels.py` cover the alert logic, auth primitives and channel senders
(no network). `test_api.py` covers authentication, roles, ingest validation and masking over HTTP.

## Docker

```bash
cp .env.example .env   # fill in the live settings
docker compose up -d   # app (2 workers) + Caddy with automatic HTTPS for $DOMAIN
```

The database lives in the `nzw-data` volume: back it up. Put `calumpang_rivers.geojson` in `./geo`.

## Data caveats (unchanged, still true)

- Site coordinates are municipal / poblacion centres, not surveyed buoy positions, and the river line is
  a schematic until you supply real geometry in `calumpang_rivers.geojson`.
- Starting flow / debris / flood / solar values are illustrative simulator inputs, not measurements.
- Alert thresholds (flood proximity 72 / 85, flow 2.6 / 3.0 m/s) were inherited from the Pasig-Marikina
  prototype and are **not calibrated** for the Calumpang watershed. Confirm them with the LGU DRRMOs and PAGASA.
- The 0-100 "flood proximity index" is not a physical unit; replace it with water level / freeboard when
  real sensors exist.
- Official flood warnings in the Philippines come from PAGASA and the LDRRMOs. Agree the relationship in
  writing with the LGUs, and treat recipient phone numbers as personal data (Data Privacy Act of 2012).

## Upgrading the prototype database

Existing `storm_baler.db` files upgrade in place (new columns and tables are added, nothing is lost).
Old alerts keep their stored delivery list; new ones use the outbox.

## Not built yet

Real river geometry and data feeds, physical-unit thresholds, ML forecasting / camera detection, a public
resident status page and PWA, SSE push instead of 3 s polling, Postgres/Timescale, trend charts,
unit telemetry (battery, bin fill, faults).
