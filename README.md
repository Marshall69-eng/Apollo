[NetZero Wave — GIS map + automated DRRMO alerts.md](https://github.com/user-attachments/files/31858425/NetZero.Wave.GIS.map.%2B.automated.DRRMO.alerts.md)
# NetZero Wave — GIS map + automated DRRMO alerts

Storm baler siting and monitoring dashboard for the Pasig–Marikina river system.
This version adds two features on top of the original prototype:

1. **Interactive GIS map (Leaflet.js)** — candidate sites and active buoys pinned
   at their real coordinates on the river network, with colour-coded status markers.
2. **Automated Telegram / SMS alert webhooks** — emergency notifications dispatched
   to Barangay and LGU DRRMO personnel when a site's flood proximity or flow
   velocity crosses a critical safety threshold.

## Run

```bash
pip install -r requirements.txt
cp .env.example .env        # optional — see "Going live" below
uvicorn main:app --reload
```

Open <http://127.0.0.1:8000> — API docs at <http://127.0.0.1:8000/docs>.

## Feature 1 — Interactive GIS map

- Leaflet 1.9.4 from CDN, Esri dark canvas basemap (no API key needed).
- River centrelines served as GeoJSON from `GET /api/geo/rivers` and drawn as a
  glowing polyline (Pasig, Marikina and San Juan branches).
- One `circleMarker` per site, recoloured on every 3-second poll:
  green = nominal, amber = warning threshold, red = critical threshold.
- Sites with an active buoy get a pulsing blue ring.
- Popups show live flow, flood index, debris, score and coordinates, plus
  "Deploy here" and "Test alert" buttons.
- Clicking a row in the candidate table flies the map to that site; clicking a
  marker highlights the matching table row.

Site coordinates (decimal degrees):

| Site | Lat | Lon | Barangay / LGU |
|---|---|---|---|
| Marikina River — Tumana Bridge | 14.64650 | 121.08630 | Brgy. Tumana, Marikina City |
| Pasig River — Guadalupe | 14.56847 | 121.04600 | Brgy. Guadalupe Nuevo, Makati / Mandaluyong |
| San Juan River — Ortigas Ave | 14.60166 | 121.02030 | Brgy. Corazon de Jesus, San Juan City |
| Marikina River — Nangka | 14.67548 | 121.10685 | Brgy. Nangka, Marikina City |
| Pasig River — Lambingan | 14.58639 | 121.01972 | Brgy. 866 Sta. Ana, Manila |

## Feature 2 — Alert dispatch service

`alerts.py` is a standalone dispatcher; `main.py` calls it after every sensor tick.

Pipeline: sensor tick → `evaluate_alerts()` classifies each site against the
configured thresholds → per-site, per-severity cooldown check → message composed
→ fanned out to every active recipient → alert plus per-recipient delivery status
written to the `alerts` table → surfaced in the Alerts tab.

Thresholds: flood proximity warning 72 / critical 85, flow velocity warning 2.6 m/s /
critical 3.0 m/s, 10-minute re-alert cooldown.

**Thresholds are locked.** They are safety constants defined in the `THRESHOLDS`
dict in `main.py` and served read-only, so a dashboard operator cannot raise a
limit and suppress a warning while an event is in progress. There is no PUT,
PATCH or POST on `/api/alert-config` — those verbs return `405 Method Not
Allowed` — and the old writable `config` table is dropped on startup. Changing a
threshold is a reviewed code change and a redeployment, which also leaves a git
audit trail of who loosened what and when.

The Alerts tab shows the values as a read-only readout with a Locked badge.
Operators can still manage the recipient list, acknowledge alerts, and fire
drills — the operational actions — just not recalibrate the trigger.

### Channels

| Channel | Transport |
|---|---|
| `telegram` | `POST https://api.telegram.org/bot<TOKEN>/sendMessage` |
| `sms` | Twilio `POST /2010-04-01/Accounts/<SID>/Messages.json` |
| `webhook` | Your own URL, receives the full JSON alert payload |

**Dry-run mode:** with no credentials set, alerts are still evaluated, recorded
and displayed with delivery status `simulated`. The demo works end to end without
a paid Twilio account, and no message can accidentally reach a real barangay
officer during testing.

### Going live

Fill in `.env`:

- `TELEGRAM_BOT_TOKEN` — from @BotFather. Add the bot to the DRRMO group, read the
  group chat id from `https://api.telegram.org/bot<TOKEN>/getUpdates`, then paste
  that id as the recipient target in the Alerts tab.
- `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_FROM_NUMBER` — from the Twilio
  console. Trial accounts can only text verified numbers.
- `GENERIC_WEBHOOK_URL` — optional bridge into an existing LGU incident system.

Restart the server; the chips at the top of the Alerts tab flip from
"dry-run" to "live".

## New API endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/geo/rivers` | GeoJSON river centrelines for the map |
| GET | `/api/alert-config` | Read the locked thresholds (no write counterpart) |
| GET / POST | `/api/recipients` | List / add DRRMO recipients |
| POST | `/api/recipients/{id}/toggle` | Mute or re-enable a recipient |
| DELETE | `/api/recipients/{id}` | Remove a recipient |
| GET | `/api/alerts` | Alert log with per-recipient delivery status |
| POST | `/api/alerts/{id}/ack` | Acknowledge an alert |
| POST | `/api/alerts/test/{site_id}` | Force a drill dispatch |
| POST | `/api/alerts/evaluate` | Run the threshold check immediately |
| POST | `/api/ingest/{site_id}` | Real buoy ingestion — writes a reading, then evaluates thresholds |

`/api/sites` now also returns `lat`, `lon`, `barangay`, `status` and `deployed`.

## Wiring real hardware

Replace `simulate_tick()` with your MQTT subscriber or LoRaWAN webhook and have it
`POST /api/ingest/{site_id}` with `flow`, `debris`, `flood` and `solar`. That single
call stores the reading and runs the alert engine — nothing else changes.

## Database

SQLite (`storm_baler.db`), created and migrated automatically on startup. Existing
databases from the earlier prototype are upgraded in place: the GIS columns are
added via `ALTER TABLE` and backfilled, so no data is lost.
