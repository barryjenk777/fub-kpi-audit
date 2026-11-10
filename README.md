# Legacy Home Team — KPI Audit System

Automated weekly KPI checker for Follow Up Boss. Determines which agents qualify for the Priority Agents (earned lead routing) group based on four pass/fail metrics.

## Setup

### 1. Install dependencies
```bash
pip install requests python-dateutil tabulate
```

### 2. Set your FUB API key
```bash
export FUB_API_KEY="your_api_key_here"
```

Or create a `.env` file:
```
FUB_API_KEY=your_api_key_here
```

### 3. Configure your team
Edit `config.py` to:
- Set your KPI thresholds
- Add/remove agent names from the audit list
- Set the Priority Agents group ID (once created in FUB)

### 4. Run the audit
```bash
# Report only (Phase 1 — recommended first)
python kpi_audit.py

# Auto-update the Priority group in FUB (Phase 2)
python kpi_audit.py --update-group
```

## KPI Thresholds (configurable in config.py)

| KPI | Default | What It Measures |
|-----|---------|-----------------|
| Outbound Calls | 75/week | Dial volume |
| Conversations (2+ min) | 5/week | Quality engagement |
| Speed to Lead | < 5 min avg | Responsiveness |
| Out of Compliance | 0 leads | Pipeline discipline |

## How It Works

1. Pulls the agent roster from FUB `/users`
2. For each agent, queries `/calls` for outbound activity in the past 7 days
3. Counts calls ≥ 120 seconds as conversations
4. Pulls recently assigned leads and checks first-contact timestamps for speed-to-lead
5. Queries `/people` for `MAV_NUDGE_OUTSTANDING` tagged leads per agent
6. Outputs a pass/fail report
7. (Phase 2) Updates the Priority Agents group membership via `/groups/:id`

## Scheduling (optional)

Run every Monday at 6:00 AM via cron:
```bash
0 6 * * 1 cd /path/to/fub-kpi-audit && python kpi_audit.py --update-group >> audit.log 2>&1
```

## Running the scheduler as its own service (recommended)

Command Center is one Flask app plus 77 cron jobs. By default they run in
the same gunicorn process, which is why heavy jobs make pages slow. To split
them on Railway:

1. Create a second service from this repo. Start command: `python worker.py`.
   Give it the same environment variables as the web service.
2. On the **web** service set `RUN_SCHEDULER=0` (web-only, no cron jobs).
3. Share the Follow Up Boss budget (200 requests/min per key): set
   `FUB_RPM=60` on the web service and `FUB_RPM=120` on the worker.
4. Deploy both. `scheduler_locks` stops a job from double-firing if both
   processes overlap during the cutover.

Other knobs (all optional): `DB_POOL=0` disables the Postgres connection
pool, `DB_POOL_MAX` (default 12) caps it, `SCHEDULER_THREADS` (default 4)
caps concurrent cron jobs, `FUB_MAX_INFLIGHT` (default 4) caps concurrent
Follow Up Boss requests per process.
