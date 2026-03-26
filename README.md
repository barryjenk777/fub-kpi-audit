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
