# FUB API Appointment Data - Complete Analysis

**Analysis Date:** 2026-03-31  
**Data Window:** Last 30 days (52 total appointments)  
**Critical Finding:** 28 appointments are OVERDUE for outcomes (69.2% have no outcome recorded)

---

## 1. APPOINTMENT FIELD STRUCTURE

### Available Fields
```
id, created, updated, createdById, updatedById, title, description,
start, end, timezone, allDay, outcome, outcomeId, type, typeId,
invitees, location, originFub, isEditable, isDeletable, detailsVisible
```

### Key Fields for Accountability System
| Field | Type | Purpose | Notes |
|-------|------|---------|-------|
| **id** | int | Unique appointment identifier | Primary key |
| **createdById** | int | Who scheduled the appointment | ISA (46) vs Agent (49-52) vs Manager (50) |
| **created** | ISO datetime | When appointment was booked | Tracks scheduling activity |
| **start** | ISO datetime | Appointment date/time | Essential for past/future filtering |
| **outcome** | string | Result of appointment | See outcomes table below |
| **outcomeId** | int | Outcome code (1-5) | Required to SET outcome via API |
| **invitees** | array | Attendees with personId | Links to lead data |
| **title** | string | Appointment description | User-readable context |
| **description** | string | Additional notes | Optional context |

### Critical Limitation ❌
**Appointments do NOT have a direct `personId` field**  
Instead, the appointment-to-lead link is embedded in the `invitees` array:
```json
"invitees": [
  { "userId": 51, "personId": null, "name": "Julz Gat" },      // Agent
  { "userId": null, "personId": 97580, "name": "Mindy Strait" } // Lead
]
```

---

## 2. LINKING APPOINTMENTS TO PEOPLE/LEADS

### How to Find the Lead
1. Extract the `invitees` array from the appointment
2. Find the invitee with `personId` (not `userId`)
3. Use that `personId` to fetch the person record via `/people/{personId}`

### Example
```python
for appt in appointments:
    for invitee in appt.get('invitees', []):
        if invitee.get('personId'):  # This is the lead
            person_id = invitee['personId']
            person = client._request('GET', f'people/{person_id}')
            print(f"Lead: {person.get('name')}")
            print(f"Stage: {person.get('stage')}")
            print(f"Last Activity: {person.get('lastActivity')}")
```

### Coverage
- **51 of 52** appointments have invitees (98%)
- **40 unique leads** linked to appointments in the 30-day window
- Some leads may have multiple appointments

---

## 3. APPOINTMENT OUTCOMES

### Outcome Distribution (30 days)
```
⚠ NONE (pending):        36 appointments (69.2%)  ← THE ACCOUNTABILITY GAP
✓ Met with Client:        8 appointments (15.4%)
✓ Reschedule Needed:      4 appointments ( 7.7%)
✓ No show:                4 appointments ( 7.7%)
```

### Outcome ID Reference (for API updates)
```
outcomeId 1 → 'No show'
outcomeId 4 → 'Reschedule Needed'
outcomeId 5 → 'Met with Client'
outcomeId null → NONE/PENDING
```

### The Accountability Gap
- **28 appointments are PAST their scheduled date** but have no outcome recorded
- **8 appointments are FUTURE** (scheduled in April) and correctly pending
- **70% of all appointments have no outcome** - this is the core problem to solve

---

## 4. WHO CREATED APPOINTMENTS: ISA vs AGENT

### Creator Breakdown
```
Fhalen (ISA #46):        20 appointments (38.5%)  ← Primary scheduler
Tanya (Agent #49):       15 appointments (28.8%)
Unknown Agent (#48):      7 appointments (13.5%)
Julz (Agent #51):         4 appointments ( 7.7%)
Jon (Agent #47):          3 appointments ( 5.8%)
Barry (Admin #1):         2 appointments ( 3.8%)
Joseph (Manager #50):     1 appointment  ( 1.9%)
```

### Outcomes by Creator (CRITICAL)
```
Fhalen (ISA):
  ⚠ PENDING:  14/20 (70.0%) ← ISA accountability gap
  ✓ Met:      2/20 (10.0%)
  ✓ No show:  2/20 (10.0%)
  ✓ Reschedule: 2/20 (10.0%)

Tanya (Agent):
  ⚠ PENDING:  15/15 (100.0%) ← WORSE - ALL pending!
  ✓ Met:       0/15 (0%)
  ✓ Reschedule: 0/15 (0%)

Jon (Agent):
  ✓ Met:       2/3 (66.7%) ← BEST accountability
  ✓ Reschedule: 1/3 (33.3%)

Other Agents (47, 48, 51, 52):
  Mixed results - some close out appointments, some don't
```

**Key Insight:** Tanya Thompson has created 15 appointments with ZERO outcomes recorded. These are all from March (past) dates and need immediate attention.

---

## 5. TRACKING LEAD STAGE CHANGES

### Approach
After an appointment, you can check if the lead's stage was updated:

```python
# Get the appointment
appt = client.get_appointments(...)[0]

# Extract the lead's personId from invitees
for invitee in appt.get('invitees', []):
    if invitee.get('personId'):
        person_id = invitee['personId']
        
        # Fetch the person record
        person = client.get_person(person_id)
        
        # Check if stage changed AFTER appointment
        appt_time = datetime.fromisoformat(appt['start'])
        person_stage_updated = datetime.fromisoformat(person.get('lastActivity', ''))
        
        if person_stage_updated > appt_time:
            print(f"✓ Lead was followed up: stage={person.get('stage')}")
        else:
            print(f"⚠ No follow-up activity after appointment")
```

### Data Available
- `person.stage` - Current stage of the lead
- `person.lastActivity` - Timestamp of last activity
- Can infer follow-up by comparing `lastActivity` with `appointment.start`

---

## 6. API CAPABILITIES FOR APPOINTMENT ACCOUNTABILITY SYSTEM

### ✅ What We Can Do

#### Read Appointments
```python
client.get_appointments(since=datetime(...), until=datetime(...))
```
- Returns all appointments (no date filters work reliably, must filter client-side)
- Newest-first sorting available via `sort: -created`

#### Read Leads
```python
person = client.get_person(person_id)
```
- Returns person record with `stage`, `lastActivity`, `tags`, etc.

#### Create Tasks (for follow-up accountability)
```python
FUB_API endpoint: POST /v1/tasks
Payload example:
{
  "name": "Follow up on [lead name] meeting",
  "personId": 97580,
  "assignedUserId": 50,
  "dueDate": "2026-04-05"
}
Response: 201 Created
```
✓ Can create automatic follow-up tasks tied to lead records

#### Update Appointment Outcomes
```python
FUB_API endpoint: PUT /v1/appointments/{id}
Payload:
{
  "start": "2026-03-31T14:00:00Z",  # REQUIRED - must include
  "end": "2026-03-31T14:30:00Z",    # REQUIRED - must include
  "outcomeId": 5                     # 1=No show, 4=Reschedule, 5=Met
}
Response: 200 OK
```
✓ Can set appointment outcomes programmatically
✓ Must include start/end dates in update

#### Add Tags to Leads
```python
client.add_tag(person_id, "Follow_Up_Needed")
```
✓ Can tag leads for follow-up workflows

#### Get All Tasks
```python
client.get_tasks(user_id=None, status=None)
```
✓ Can monitor follow-up task completion

### ❌ Limitations

1. **No direct personId in appointment** - Must extract from invitees
2. **Appointment API doesn't support date filtering** - Must paginate all and filter client-side
3. **Updates require start/end dates** - Cannot do partial updates
4. **No batch update operations** - Must update appointments one-at-a-time (rate-limited)
5. **No "appointment assigned to" field** - Only `createdById` (who scheduled it)

---

## 7. RECOMMENDED SYSTEM ARCHITECTURE

### Core Accountability Workflow

```
1. COLLECT (hourly)
   ├─ Fetch all appointments from last 30 days
   ├─ Filter to PAST appointments with outcome == None
   └─ Identify responsible party (createdById)

2. TAG & NOTIFY (daily)
   ├─ For overdue outcomes:
   │  ├─ Tag the lead: "Appt_Outcome_Pending"
   │  └─ Create task for creator: "Close out appointment XYZ"
   └─ For complete appointments with no follow-up:
      ├─ Tag the lead: "Appt_Complete_No_Followup"
      └─ Create follow-up task for assigned agent

3. TRACK & REPORT (weekly)
   ├─ Creator accountability:
   │  ├─ Total appointments created
   │  ├─ Outcome completion rate (%)
   │  └─ Average days to close out
   └─ Lead follow-up:
      ├─ Appointments with no stage change after meeting
      ├─ Rescheduled appointments aging without follow-up
      └─ "No show" appointments not re-engaged

4. ENFORCE (automated)
   ├─ Past appointments with no outcome → Tag + Task
   ├─ Rescheduled appts 5+ days past new date → Tag + Task
   └─ "Met" appointments 3+ days with no person.lastActivity → Task
```

### Database Schema (for tracking)
```
appointment_accountability (
  appointment_id int,
  person_id int,
  creator_id int (createdById),
  appt_date datetime,
  outcome text,
  outcome_set_date datetime,
  lead_stage_at_appt text,
  lead_stage_after text,
  days_to_outcome int,
  last_activity_date datetime,
  accountability_gap boolean,  -- True if PAST appt with no outcome
  follow_up_task_created boolean,
  follow_up_task_id int,
  indexed: (creator_id, appt_date, accountability_gap)
)
```

---

## 8. KEY METRICS TO BUILD

### For Creators (ISA vs Agents)
1. **Appointment Volume** - Total appointments created (30d)
2. **Outcome Completion Rate** - % with outcome recorded (target: 100%)
3. **Days to Close** - Avg days from appt.start to outcome recorded
4. **Outcome Mix** - % Met / No show / Reschedule
5. **Follow-up Rate** - % of "Met" appointments with person.lastActivity > appt.start

### For Leads
1. **Appointment-to-Stage** - % of appointments resulting in stage change
2. **Rescheduled Aging** - Days since "Reschedule Needed" with no new appt
3. **Accountability Gap** - Days since past appt with no outcome
4. **Task Completion** - % of auto-created follow-up tasks completed

---

## 9. TESTING & VALIDATION

### What We've Verified ✅
- [x] Appointment GET works with date filtering (client-side)
- [x] Appointment POST works (can create new appointments)
- [x] Appointment PUT works (can update outcomes with outcomeId)
- [x] Task POST works (can create follow-up tasks)
- [x] Person GET works (can fetch lead records)
- [x] Person PUT works (can add/remove tags)
- [x] Invitees structure reliably contains personId

### Test Appointment Created
```
ID: 21407
Title: Test Appointment
Start: 2026-04-10T14:00:00Z
OutcomeId: 5 (Met with Client)
Status: ✓ Successfully updated
```

---

## 10. SAMPLE QUERY: ACCOUNTABILITY REPORT

```python
from datetime import datetime, timedelta, timezone
from collections import Counter

client = FUBClient()
today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

# Get past 30 days
appts = client.get_appointments(since=today - timedelta(days=30))

# Filter to PAST appointments
past_appts = [a for a in appts if a.get('start', '') < today.isoformat()]

# Accountability gaps
no_outcome = [a for a in past_appts if a.get('outcome') is None]

# By creator
by_creator = Counter(a.get('createdById') for a in no_outcome)

print(f"ACCOUNTABILITY REPORT - {today.date()}")
print(f"Past appointments with no outcome: {len(no_outcome)}/{len(past_appts)}")
print(f"\nBy creator:")
for creator_id, count in sorted(by_creator.items(), key=lambda x: -x[1]):
    total = sum(1 for a in appts if a.get('createdById') == creator_id)
    pct = 100 * count / total
    print(f"  User {creator_id}: {count}/{total} ({pct:.1f}%)")

# Oldest unresolved
oldest = min(no_outcome, key=lambda a: a.get('start', ''))
print(f"\nOldest unresolved: {oldest.get('start')[:10]} - {oldest.get('title')}")
```

---

## Summary

You have **28 overdue appointments** (69.2% of all appointments) with no outcome recorded. The system is ready to support:

1. ✓ Real-time accountability tracking
2. ✓ Automated task creation for outcomes
3. ✓ Lead follow-up verification
4. ✓ Creator performance reporting
5. ✓ Stage change correlation

The API fully supports reading, creating, and updating appointments, as well as creating follow-up tasks and tagging leads for accountability workflows.
