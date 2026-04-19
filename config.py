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
MAX_OUT_OF_COMPLIANCE = 30


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

# FUB user IDs whose calls should NEVER be counted or fetched.
# userId=1 is a system/automated account that logs thousands of calls/week
# and serves no purpose in any call metric.
EXCLUDED_CALL_USER_IDS = {1}


# =============================================================================
# ROLE-SPECIFIC USER IDS
# =============================================================================

# Sales Manager — tracked on the "Sales Manager" tab
MANAGER_USER_ID = 50       # Joseph Fuscaldo
MANAGER_NAME = "Joseph Fuscaldo"

# ISA — tracked on the "ISA Performance" tab
ISA_USER_ID = 46           # Fhalen Tendencia
ISA_NAME = "Fhalen Tendencia"

# Fhalen also dials through MojoDialer under Barry's FUB account, so calls
# logged under these FUB user names are also credited to her on the ISA tab.
ISA_MOJO_USER_NAMES = ["Barry Jenkins"]

# Tag applied to leads where ISA set an appointment but agent hasn't followed up
DROPPED_BALL_TAG = "Fhalen_Pending"

# Tag applied to leads Fhalen connected with that went stale (no agent follow-up)
ISA_FOLLOWUP_TAG = "ISA_Followup"

# Days without activity before a handed-off lead is considered "stale"
STALE_LEAD_DAYS = 5

# How far back to sweep for ISA handoff leads (90d initial, 30d ongoing)
ISA_HANDOFF_LOOKBACK_DAYS = 90

# =============================================================================
# APPOINTMENT ACCOUNTABILITY
# =============================================================================

# Tag lifecycle: APT_SET → APT_OUTCOME_NEEDED → APT_STALE → removed on outcome
APT_SET_TAG = "APT_SET"
APT_OUTCOME_NEEDED_TAG = "APT_OUTCOME_NEEDED"
APT_STALE_TAG = "APT_STALE"

# Escalation thresholds (hours since appointment start time)
APT_TIER1_HOURS = 24    # Grace period — APT_SET applied
APT_TIER2_HOURS = 48    # Escalate — APT_OUTCOME_NEEDED + FUB task created
APT_TIER3_HOURS = 168   # 7 days — APT_STALE, red flag for Barry

# How far back to look for appointments
APT_LOOKBACK_DAYS = 30

# FUB outcome IDs (from API)
APT_OUTCOME_IDS = {1: "No show", 4: "Reschedule Needed", 5: "Met with Client"}

# Task template when escalating to Tier 2
APT_TASK_TEMPLATE = "Update appointment outcome: {lead_name}"


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
    "Courted.io",
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
LIVE_CALLS_ADMIN_EMAIL = "clientreview@yourfriendlyagent.net"

# CC list for per-agent appointment accountability emails
APT_EMAIL_CC = [
    "barry@yourfriendlyagent.net",
    "thejoefu@gmail.com",
    "clientreview@yourfriendlyagent.net",
]

# Sales Manager email (Joe's Monday morning coaching email)
MANAGER_EMAIL = "thejoefu@gmail.com"


# =============================================================================
# LEADSTREAM — Daily Lead Priority Scoring
# Scores leads every few hours and tags the top ones so agents can use a
# FUB smart list filtered by the LeadStream tag as their daily call list.
# =============================================================================

# Optional: set FUB_WEBHOOK_SECRET in .env to verify webhook requests from FUB
# (set the same value in FUB under Admin → Integrations → Webhooks → Secret)

# Optional: use a separate FUB API key for LeadStream to isolate API usage.
# Set FUB_LEADSTREAM_API_KEY in .env or environment. Falls back to FUB_API_KEY.
LEADSTREAM_API_KEY_ENV = "FUB_LEADSTREAM_API_KEY"

# Tags applied to priority leads (agents filter smart lists by these)
LEADSTREAM_TAG = "LeadStream"
LEADSTREAM_POND_TAG = "LeadStream_Pond"

# How many leads to tag per agent / for the pond
LEADSTREAM_LIMIT = 20
LEADSTREAM_POND_LIMIT = 80

# Ylopo signal tags and their base point values (highest tier first)
LEADSTREAM_SIGNAL_TAGS = {
    "AI_NEEDS_FOLLOW_UP":       100,  # rAIya text converted — call NOW
    "AI_VOICE_NEEDS_FOLLOW_UP":  95,  # AI voice converted
    "HANDRAISER":                80,  # Lead asking for help
    "YPRIORITY":                 50,  # Ylopo top-priority buyer signal
    # Behavioral intent tags — set by Ylopo based on IDX activity
    "Y_HOME_3_VIEW":             45,  # Viewed same home 3+ times — strong attachment
    "HVB":                       35,  # High-value buyer flag
    "Y_SHARED_LISTING":          30,  # Shared a listing (likely with partner/spouse)
    "RETURNED":                  25,  # Came back after going quiet
    "Y_REMARKETING_ENGAGED":     20,  # Re-engaged via remarketing ads
    "Y_SELLER_REPORT_VIEWED":    15,  # Viewed home value report — potential seller
    "Y_ADDRESS_FOUND":           10,  # Ylopo identified their current home address
}

# IDX site visit recency scoring (hours_threshold: points)
# A lead browsing the site RIGHT NOW outranks stale tagged leads
LEADSTREAM_VISIT_RECENCY = [
    (1, 50),    # Within 1 hour — actively browsing
    (6, 35),    # Within 6 hours — very recent
    (24, 25),   # Within 24 hours — visited today
    (72, 15),   # Within 3 days — recent activity
    (168, 5),   # Within 7 days — some interest
]

# Bonus points for other signals
LEADSTREAM_NEW_LEAD_24H_BONUS = 15     # Created in last 24 hours
LEADSTREAM_NEW_LEAD_72H_BONUS = 10     # Created in last 72 hours
LEADSTREAM_SELLER_BONUS = 10           # Seller tag present
LEADSTREAM_MULTI_SIGNAL_BONUS = 10     # Multiple Ylopo signals

# Tags that identify seller leads (for seller bonus)
SELLER_TAGS = ["Seller", "Home Valuation", "seller", "Listing"]

# Stale/aging/re-engage tier points
LEADSTREAM_STALE_HOT_POINTS = 40       # Had signal + no contact in 3+ days
LEADSTREAM_AGING_NEW_POINTS = 30       # New (< 7 days), never contacted
LEADSTREAM_REENGAGE_POINTS = 20        # Last contact 7-14 days ago
LEADSTREAM_COLD_LEAD_POINTS = 10       # Older lead, no contact in lookback window

# Suppression: after an attempt without conversation, suppress for this many hours
LEADSTREAM_SUPPRESS_HOURS = 48

# Lead sources to EXCLUDE from LeadStream scoring entirely
LEADSTREAM_EXCLUDED_SOURCES = [
    "Courted.io",
]

# =============================================================================
# YLOPO LEAD SOURCE ROUTING
# =============================================================================

# FUB source strings that identify Ylopo AI prospecting leads.
# These are homeowners rAIya (Ylopo AI) texted about their home value,
# had a real AI conversation with, and live-transferred into FUB.
# IMPORTANT: Only leads that also have AI_NEEDS_FOLLOW_UP or
# AI_VOICE_NEEDS_FOLLOW_UP tags count — older Ylopo Prospecting leads
# that predate the AI conversation feature are skipped entirely.
YLOPO_PROSPECTING_SOURCES = [
    "Ylopo Prospecting",
]

# FUB stages that mean the agent has already resolved/deferred this lead.
# Leads in these stages score 0 regardless of Ylopo signal tags.
# This prevents "AI_NEEDS_FOLLOW_UP" from re-surfacing a lead the agent has
# already worked and parked (e.g., "not selling till 2027", family situation).
LEADSTREAM_EXCLUDED_STAGES = [
    "C - Cold 6+ Months",
    "C - Cold 3-6 Months",
    "Cold",
    "Not Interested",
    "Closed - Lost",
    "Unqualified",
    "Do Not Contact",
    "Future - 1 Year+",
    "Future - 2+ Years",
]

# FUB pond IDs that LeadStream should score for the pond queue.
# Ponds NOT in this list are skipped entirely — they have different workflows
# (probate, off-market investor, recruiting) that don't belong in the agent
# daily call list alongside Ylopo inbound leads.
#
# Current pond map:
#   1 = Probate Pond      → separate workflow, skip
#   4 = Shark Tank 🦈     → main agent working pond ✓
#   5 = Seller Test       → skip
#   6 = Storage Pond      → parked/cold, skip
#   7 = Engaged Seller    → active seller prospecting ✓
#   8 = Recruiting Agents → skip
#   9 = MYPlus            → pre-foreclosure investor leads, skip
LEADSTREAM_ALLOWED_POND_IDS = {4, 7}

# New Lead Immediate Mailer — "I caught you at the computer" opener
# Pond 4 (Shark Tank) only — mix of new Ylopo buyer/seller leads and old leads.
SHARK_TANK_POND_ID = 4
BARRY_FUB_USER_ID  = 1   # Barry Jenkins — attributes email notes in FUB timeline
# Minutes after lead creation before the immediate email fires (feels human, not instant)
NEW_LEAD_EMAIL_DELAY_MINUTES = 12
# How far back to look for new leads per check run (scheduler runs every 5 min)
# Hard outer fence: never email a lead created more than this many minutes ago.
# 1440 = 24 hours — Barry's rule: "don't send for anything created longer than 1 day ago"
NEW_LEAD_LOOKBACK_MINUTES = 1440
# Max immediate emails sent per day — prevents runaway sends on bulk imports.
NEW_LEAD_DAILY_CAP = 15

# Stale hot threshold: days since last agent contact to qualify
LEADSTREAM_STALE_DAYS = 3

# Re-engage window: contact was 7-14 days ago
LEADSTREAM_REENGAGE_MIN_DAYS = 7
LEADSTREAM_REENGAGE_MAX_DAYS = 14
