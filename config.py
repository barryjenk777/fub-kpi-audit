"""
Legacy Home Team — KPI Audit Configuration
Edit these values to match your team's thresholds and FUB setup.
"""

# =============================================================================
# KPI THRESHOLDS — An agent must pass ALL to earn Priority routing
# These are defaults; override per-run via CLI flags:
#   python kpi_audit.py --min-calls 30 --min-convos 3 --max-ooc 5
# =============================================================================

# Minimum outbound calls per week (start low, ramp up weekly)
MIN_OUTBOUND_CALLS = 30

# Minimum real conversations per week
MIN_CONVERSATIONS = 5

# Minimum call duration (seconds) to count as a conversation
# FUB defines a "Conversation" as a call >= 120 seconds (2 minutes)
CONVERSATION_THRESHOLD_SECONDS = 120

# Maximum average speed-to-lead in minutes (for newly assigned leads)
MAX_SPEED_TO_LEAD_MINUTES = 5

# Set to False to disable speed-to-lead as a pass/fail requirement
ENABLE_SPEED_TO_LEAD = False

# Maximum number of leads with MAV_NUDGE_OUTSTANDING tag
# Set high initially while team clears backlog, then tighten over time
MAX_OUT_OF_COMPLIANCE = 100


# =============================================================================
# AGENT ROSTER — Auto-detected from FUB
# The audit automatically finds all Active agents in FUB.
# Add names here to EXCLUDE from audits (team leaders, ISAs, admins).
# =============================================================================

EXCLUDED_USERS = [
    "Barry Jenkins",
    "Fhalen Tendencia",
    "Joseph Fuscaldo",
]


# =============================================================================
# ROLE-SPECIFIC USER IDS
# =============================================================================

# Sales Manager — tracked on the "Sales Manager" tab
MANAGER_USER_ID = 50       # Joseph Fuscaldo
MANAGER_NAME = "Joseph Fuscaldo"

# ISA — tracked on the "ISA Performance" tab
ISA_USER_ID = 46           # Fhalen Tendencia
ISA_NAME = "Fhalen Tendencia"

# Tag applied to leads where ISA set an appointment but agent hasn't followed up
DROPPED_BALL_TAG = "Fhalen_Pending"

# Tag applied to leads Fhalen connected with that went stale (no agent follow-up)
ISA_FOLLOWUP_TAG = "ISA_Followup"

# Days without activity before a handed-off lead is considered "stale"
STALE_LEAD_DAYS = 5

# Agents to NEVER remove from the priority group even if they miss KPIs
# (e.g., team leader who should always receive transfers as fallback)
PROTECTED_AGENTS = [
    # "Barry Jenkins",
]


# =============================================================================
# FUB GROUP CONFIGURATION
# =============================================================================

# The FUB Group ID for your "Priority Agents" routing group
PRIORITY_GROUP_ID = 12

# The FUB Shared Inbox ID for "Live Calls" phone routing
# The API cannot manage inbox membership (403), so the audit prints
# a report of who should be added/removed for manual action.
LIVE_CALLS_INBOX_ID = 4


# =============================================================================
# EXCLUDED LEAD SOURCES — Conversations with these sources don't count
# (e.g., Sphere leads are past clients, not purchased leads)
# =============================================================================

EXCLUDED_LEAD_SOURCES = [
    "Sphere",
]


# =============================================================================
# COMPLIANCE TAG
# =============================================================================

# The tag FUB/Maverick applies to leads that are out of compliance
COMPLIANCE_TAG = "MAV_NUDGE_OUTSTANDING"


# =============================================================================
# SPEED TO LEAD — How far back to look for newly assigned leads
# =============================================================================

# Number of days back to look for newly assigned leads when calculating
# speed-to-lead. 7 = only this week's new assignments.
SPEED_TO_LEAD_LOOKBACK_DAYS = 7


# =============================================================================
# REPORTING
# =============================================================================

# Set to True to print detailed per-agent breakdowns (useful for debugging)
VERBOSE = False

# Audit period in days (7 = weekly audit)
AUDIT_PERIOD_DAYS = 7


# =============================================================================
# EMAIL NOTIFICATIONS (SendGrid)
# =============================================================================

# Set your SendGrid API key as an environment variable:
#   export SENDGRID_API_KEY="SG.your_key_here"

# "From" address (must be verified in SendGrid)
EMAIL_FROM = "barry@yourfriendlyagent.net"

# Recipients for the audit report email
EMAIL_RECIPIENTS = [
    "clientreview@yourfriendlyagent.net",
    "barry@yourfriendlyagent.net",
    "thejoefu@gmail.com",
]

# Person responsible for updating the Live Calls inbox in FUB
# (API cannot manage shared inboxes, so this person is tasked manually)
LIVE_CALLS_ADMIN = "Fhalen Tendencia"
LIVE_CALLS_ADMIN_EMAIL = "fhalen@yourfriendlyagent.net"  # Update with Fhalen's real email

# Sales Manager email (Joe's Monday morning coaching email)
MANAGER_EMAIL = "thejoefu@gmail.com"
