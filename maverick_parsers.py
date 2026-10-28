"""
maverick_parsers.py — pure text parsers for Grok Bot's raw Maverick page
posts. No database, no Flask: importable by tests on any Python.

Grok posts bare page text (inner_text), unlike the retired Mac courier
which injected sentinel headers. Kind detection is by content signature,
and every parser returns (data, parsed_keys, warnings) so the ingest
endpoint can answer with parsed/skipped counts instead of silent zeros.
"""

import re

PIPELINE_STAGES = (
    "New / No Contact Made",
    "Attempted Contact / Unresponsive",
    "Appointment Outstanding",
    "Short Term Nurture",
    "Long Term Nurture",
    "Actively Listed / Showing Homes",
    "Closed / Under Contract",
    "Rejected / Trash",
)


def detect_kind(raw):
    """Classify a raw post by content signature."""
    t = (raw or "")
    if t.startswith("GROKBOT STATUS"):
        return "status"
    if "MAVERICK GRADING DASHBOARD" in t:
        return "grading_dashboard"
    if "MAVERICK RULES BOARD" in t:
        return "rules"
    # Pipeline page first: its prose mentions "Rules & Alerts", so the
    # rules check must come after and match the rules page's own header.
    if "Sales Pipeline Health" in t or "Lead Response Metrics" in t:
        return "pipeline_dashboard"
    if "AGENT RULES & ALERTS" in t.upper() or "Create New Rule" in t:
        return "rules"
    if "AI Grading Call History" in t and re.search(r"\b\d{1,2}\b", t):
        return "call_history"
    return "unknown"


def parse_pipeline_dashboard(raw):
    """The Business Dashboard's Sales Pipeline Health tab, as page text:
    stage label lines followed (possibly after blank lines) by a count
    line and a percent line. Returns (data, parsed_keys, warnings)."""
    data, keys, warnings = {}, [], []
    lines = [ln.strip() for ln in (raw or "").splitlines()]
    body = [ln for ln in lines if ln]

    m = re.search(r"(\d+(?:\.\d+)?)%\s*New,?\s*Non-Staged After 48 Hours",
                  raw or "")
    if m:
        data["pct_nonstaged_48h"] = float(m.group(1))
        keys.append("pct_nonstaged_48h")
    m = re.search(r"([\d,]+)\s*New Leads", raw or "")
    if m:
        data["new_leads"] = int(m.group(1).replace(",", ""))
        keys.append("new_leads")
    m = re.search(r"(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})", raw or "")
    if m:
        data["window"] = "%s - %s" % (m.group(1), m.group(2))
        keys.append("window")
    m = re.search(r"move[d]?\s+(\d+)\s+extra leads", raw or "")
    if m:
        data["rules_moved_leads"] = int(m.group(1))
        keys.append("rules_moved_leads")

    stages = []
    for label in PIPELINE_STAGES:
        try:
            i = body.index(label)
        except ValueError:
            continue
        count = pct = None
        for nxt in body[i + 1:i + 4]:
            if count is None and re.fullmatch(r"[\d,]+", nxt):
                count = int(nxt.replace(",", ""))
            elif count is not None and re.fullmatch(r"\d+(?:\.\d+)?%", nxt):
                pct = float(nxt[:-1])
                break
            elif re.fullmatch(r"[A-Za-z].*", nxt):
                break
        if count is not None:
            stages.append({"stage": label, "count": count, "pct": pct})
    if stages:
        data["stages"] = stages
        keys.append("stages(%d)" % len(stages))
    missing = [s for s in PIPELINE_STAGES
               if s in (raw or "") and s not in
               {x["stage"] for x in stages}]
    if missing:
        warnings.append("stage labels present but values not parsed: "
                        + ", ".join(missing[:3]))
    if not data:
        warnings.append("pipeline dashboard recognized but no metrics "
                        "parsed")
    return data, keys, warnings


def parse_rules_page(raw):
    """The Rules & Alerts page as page text. Group summaries look like a
    group name line near 'N% Past Due / N% At Risk / N% Complete';
    individual rule rows look like 'N (M%) Past Due'. An empty board
    (headers only) is VALID and returns ([], [], []) with ok semantics."""
    rows, keys, warnings = [], [], []
    t = raw or ""
    groups = []
    for m in re.finditer(
            r"([A-Z][A-Za-z &/]{2,40}Rules)\s*[\s\S]{0,120}?"
            r"(\d+)% Past Due[\s\S]{0,40}?(\d+)% At Risk"
            r"[\s\S]{0,40}?(\d+)% Complete", t):
        groups.append({"group": m.group(1).strip(),
                       "past_due_pct": int(m.group(2)),
                       "at_risk_pct": int(m.group(3)),
                       "complete_pct": int(m.group(4))})
    for m in re.finditer(r"(\d+) \((\d+)%\) Past Due", t):
        rows.append({"past_due": int(m.group(1)),
                     "past_due_pct": int(m.group(2))})
    if groups:
        keys.append("groups(%d)" % len(groups))
    if rows:
        keys.append("rule_rows(%d)" % len(rows))
    return {"groups": groups, "rule_rows": rows}, keys, warnings
