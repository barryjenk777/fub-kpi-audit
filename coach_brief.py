"""Danny's Monday coaching brief.

The job (Barry, Nov 11 2026): for the agents Danny actually met in the
last two weeks, hand him the film. Call grades, objections and how they
were handled, who is asking for the appointment and who is not, the best
call and the call to coach, and claims-vs-film lines that let him poke
holes in "I'm doing great." Agents he did not meet are NOT included.

Who Danny met comes from two ledgers: his Impact Tracker submissions
(entries with met == "yes") and the 1-on-1 commitments log. The film is
the Maverick nightly scrape (maverick_calls). Nothing here is estimated;
every claim carries the call it came from.
"""
from datetime import datetime, timezone, timedelta

import config
import db as _db

BASE = getattr(config, "BASE_URL", "https://www.legacycommandcenter.com")
FUB = "https://yourfriendlyagent.followupboss.com/2/people/view/%s"
MAVERICK_FILM = ("https://my.maverickre.com/ai/call-grading/"
                 "ai-grading-call-history")


def _met_agents(days=14):
    """agent -> {'meetings': [labels], 'commitments': [texts]}."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    out = {}
    for u in _db.get_manager_updates(limit=10) or []:
        try:
            sub = datetime.fromisoformat(u["submitted_at"])
            if sub.tzinfo is None:
                sub = sub.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError, KeyError):
            continue
        if sub < cutoff:
            continue
        label = "tracker %s" % (u.get("week_end") or
                                u["submitted_at"][:10])
        for e in u.get("entries") or []:
            if isinstance(e, dict) and (e.get("met") or "").lower() == "yes":
                name = (e.get("agent") or "").strip()
                if name:
                    rec = out.setdefault(name, {"meetings": [],
                                                "commitments": [],
                                                "notes": []})
                    rec["meetings"].append(label)
                    note = (e.get("note") or e.get("commit") or "").strip()
                    if note:
                        rec["notes"].append(note[:160])
    for c in _db.get_coaching_commitments(active_only=False) or []:
        try:
            md = c.get("met_date")
            md_dt = datetime.fromisoformat(str(md)).replace(
                tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
        if md_dt < cutoff:
            continue
        name = (c.get("agent_name") or "").strip()
        if not name:
            continue
        rec = out.setdefault(name, {"meetings": [], "commitments": [],
                                    "notes": []})
        rec["meetings"].append("1-on-1 %s" % str(md)[:10])
        if c.get("commitment"):
            rec["commitments"].append({
                "text": c["commitment"][:160],
                "date": str(md)[:10],
                "metric": c.get("metric"), "target": c.get("target")})
    return out


def _film_for(agent, days=14):
    calls = _db.get_maverick_calls(days=days, agent_name=agent,
                                   limit=120) or []
    graded = [c for c in calls if c.get("call_grade") is not None]
    if not graded:
        return {"graded": 0}
    graded.sort(key=lambda c: c.get("call_time") or "")
    grades = [float(c["call_grade"]) for c in graded]
    half = len(graded) // 2 or 1
    first = grades[:half]
    second = grades[half:] or first
    avg = round(sum(grades) / len(grades), 1)
    trend = round((sum(second) / len(second)) - (sum(first) / len(first)), 1)
    talk = [float(c["agent_talk_time_percent"]) for c in graded
            if c.get("agent_talk_time_percent") is not None]
    qr = [float(c["questions_asked_ratio"]) for c in graded
          if c.get("questions_asked_ratio") is not None]
    appts = [c for c in graded if c.get("appointment_set")]
    # Objection board: what they heard and what they did with it.
    objections = []
    for c in graded:
        ob = (c.get("objections") or "").strip()
        if ob and ob.lower() not in ("none", "null", "n/a", ""):
            objections.append({
                "lead": c.get("lead_name"), "lead_id": c.get("lead_fub_id"),
                "objection": ob[:160],
                "handling": (c.get("objection_handling") or "")[:220],
                "grade": float(c["call_grade"]),
                "when": (c.get("call_time") or "")[:10]})
    # Real conversations only for best/worst (hangups don't teach).
    convos = [c for c in graded if (c.get("duration_sec") or 0) >= 60]
    best = max(convos or graded, key=lambda c: float(c["call_grade"]))
    worst = min(convos or graded, key=lambda c: float(c["call_grade"]))

    def _card(c):
        return {"lead": c.get("lead_name"), "lead_id": c.get("lead_fub_id"),
                "grade": float(c["call_grade"]),
                "when": (c.get("call_time") or "")[:10],
                "minutes": round((c.get("duration_sec") or 0) / 60, 1),
                "outcome": c.get("outcome"),
                "appointment_set": bool(c.get("appointment_set")),
                "summary": (c.get("summary") or "")[:260],
                "coach_tip": (c.get("coaching_tips") or "")[:260]}
    return {
        "graded": len(graded), "avg_grade": avg, "trend": trend,
        "appts_set": len(appts),
        "appt_names": [c.get("lead_name") for c in appts][:5],
        "avg_talk_pct": round(sum(talk) / len(talk)) if talk else None,
        "avg_questions_ratio": round(sum(qr) / len(qr), 2) if qr else None,
        "objections": objections[:6],
        "objections_total": len(objections),
        "best": _card(best), "worst": _card(worst),
    }


def _holes(agent, film):
    """Claims-vs-film: the lines that let Danny poke holes politely.
    Every line is a fact from the scrape, never a judgment invented."""
    out = []
    g = film.get("graded", 0)
    if not g:
        return out
    if film["appts_set"] == 0 and g >= 4:
        out.append("%d graded conversations in two weeks, zero ended with "
                   "an appointment on the calendar. Ask them to walk you "
                   "through the close of their last call." % g)
    if (film.get("avg_talk_pct") or 0) >= 60:
        out.append("They are talking %d%% of the time. That is a "
                   "presentation, not a conversation. The lead's "
                   "motivation never gets room to surface."
                   % film["avg_talk_pct"])
    if film.get("trend", 0) <= -1.0:
        out.append("Grades are falling inside the window (%.1f). "
                   "Whatever changed, it shows on the film."
                   % film["trend"])
    if film.get("objections_total", 0) == 0 and g >= 5:
        out.append("Zero objections recorded across %d calls. Either every "
                   "lead is perfect, or the calls end before anyone gets "
                   "real. Short calls hide." % g)
    if (film.get("avg_grade") or 10) < 5:
        out.append("A %.1f average grade is a skills conversation, not an "
                   "effort conversation." % film["avg_grade"])
    return out[:3]


def build_brief(days=14):
    met = _met_agents(days=days)
    agents = []
    for name, rec in sorted(met.items()):
        film = _film_for(name, days=days)
        agents.append({"agent": name, **rec, "film": film,
                       "holes": _holes(name, film)})
    # Who to meet next: worst film among the met, plus a line for it.
    scored = [a for a in agents if a["film"].get("graded")]
    scored.sort(key=lambda a: (a["film"].get("avg_grade") or 10)
                + (a["film"].get("trend") or 0))
    suggest = [{"agent": a["agent"],
                "why": ("%.1f avg, trend %+0.1f, %d appts from %d calls"
                        % (a["film"]["avg_grade"], a["film"]["trend"],
                           a["film"]["appts_set"], a["film"]["graded"]))}
               for a in scored[:2]]
    return {"generated_at": datetime.now(timezone.utc).isoformat(),
            "window_days": days, "agents": agents, "suggest": suggest}


def _e(s):
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;"))


def build_html(data):
    css_card = ("background:#ffffff;border:1px solid #e5e7eb;"
                "border-radius:12px;padding:18px 20px;margin:14px 0")
    rows = []
    for a in data["agents"]:
        f = a["film"]
        name = _e(a["agent"])
        meet = ", ".join(sorted(set(a["meetings"])))
        lib = ("%s/calls?agent=%s" % (BASE, name.replace(" ", "%20")))
        if not f.get("graded"):
            rows.append(
                '<div style="%s"><h3 style="margin:0 0 4px;font-size:17px;'
                'color:#0f172a">%s</h3>'
                '<p style="margin:0;font-size:13px;color:#6b7280">Met: %s</p>'
                '<p style="margin:10px 0 0;font-size:14px;color:#b45309">'
                'No graded calls in the window. That IS the coaching '
                'conversation: the film cannot improve what it never sees.'
                '</p></div>' % (css_card, name, _e(meet)))
            continue
        trend = f.get("trend") or 0
        trend_txt = ("improving %+0.1f" % trend if trend > 0.3 else
                     "falling %+0.1f" % trend if trend < -0.3 else "flat")
        trend_color = ("#0d7a4e" if trend > 0.3 else
                       "#b91c1c" if trend < -0.3 else "#6b7280")
        stat = lambda v, l: ('<td style="padding:6px 10px;text-align:center">'
                             '<div style="font-size:20px;font-weight:800;'
                             'color:#0f172a">%s</div><div style="font-size:'
                             '11px;color:#6b7280;text-transform:uppercase;'
                             'letter-spacing:.05em">%s</div></td>' % (v, l))
        holes = "".join('<li style="margin:4px 0">%s</li>' % _e(h)
                        for h in a["holes"])
        objs = "".join(
            '<li style="margin:6px 0"><b>%s</b> (%s, grade %.1f): '
            '&ldquo;%s&rdquo;<br><span style="color:#6b7280">How it was '
            'handled: %s</span></li>'
            % (_e(o["lead"]), _e(o["when"]), o["grade"], _e(o["objection"]),
               _e(o["handling"] or "not addressed on the call"))
            for o in f["objections"][:4])
        commits = "".join('<li style="margin:3px 0">%s <span style="color:'
                          '#6b7280">(%s)</span></li>'
                          % (_e(c["text"]), _e(c["date"]))
                          for c in a.get("commitments", [])[:3])

        def call_block(c, label, color):
            fub_link = FUB % c["lead_id"] if c.get("lead_id") else lib
            return (
                '<div style="border-left:4px solid %s;padding:8px 12px;'
                'margin:8px 0;background:#f8fafc;border-radius:0 8px 8px 0">'
                '<div style="font-size:12px;font-weight:700;color:%s;'
                'text-transform:uppercase;letter-spacing:.05em">%s '
                '&middot; grade %.1f &middot; %s min &middot; %s</div>'
                '<div style="font-size:13.5px;color:#0f172a;margin-top:3px">'
                '<b><a href="%s" style="color:#0f172a">%s</a></b>%s. %s</div>'
                '%s'
                '<div style="font-size:12px;margin-top:5px">'
                '<a href="%s" style="color:#2563eb">Read the transcript in '
                'the Call Library</a> &middot; <a href="%s" style="color:'
                '#2563eb">Listen in Maverick</a></div></div>'
                % (color, color, label, c["grade"], c["minutes"],
                   _e(c["when"]), fub_link, _e(c["lead"]),
                   " (appointment set)" if c["appointment_set"] else "",
                   _e(c["summary"]),
                   ('<div style="font-size:12.5px;color:#92600a;margin-top:'
                    '4px"><b>The coaching point:</b> %s</div>'
                    % _e(c["coach_tip"])) if c.get("coach_tip") else "",
                   "%s&q=%s" % (lib, _e(c["lead"]).replace(" ", "%20")),
                   MAVERICK_FILM))
        rows.append(
            '<div style="%s">'
            '<h3 style="margin:0 0 2px;font-size:17px;color:#0f172a">%s'
            '<span style="font-weight:600;font-size:12px;color:%s;'
            'margin-left:8px">%s</span></h3>'
            '<p style="margin:0 0 8px;font-size:12.5px;color:#6b7280">'
            'Met: %s</p>'
            '<table style="width:100%%;border-collapse:collapse;margin:'
            '4px 0 10px"><tr>%s%s%s%s%s</tr></table>'
            '%s%s%s%s'
            '<div style="font-size:12px;margin-top:8px">'
            '<a href="%s" style="color:#2563eb">All of %s\'s film in the '
            'Call Library</a></div></div>'
            % (css_card, name, trend_color, trend_txt, _e(meet),
               stat("%.1f" % f["avg_grade"], "avg grade"),
               stat(f["graded"], "graded calls"),
               stat(f["appts_set"], "appts set"),
               stat("%s%%" % f["avg_talk_pct"]
                    if f["avg_talk_pct"] is not None else "?",
                    "their talk time"),
               stat(f["objections_total"], "objections heard"),
               ('<div style="background:#fef3c7;border-radius:8px;padding:'
                '10px 14px;margin:8px 0"><b style="font-size:13px;color:'
                '#92600a">Where to poke:</b><ul style="margin:4px 0 0 '
                '18px;padding:0;font-size:13px;color:#374151">%s</ul></div>'
                % holes) if holes else "",
               call_block(f["best"], "The call to praise", "#0d7a4e")
               + call_block(f["worst"], "The call to coach", "#b91c1c"),
               ('<div style="margin:8px 0"><b style="font-size:13px;color:'
                '#0f172a">Objection board:</b><ul style="margin:4px 0 0 '
                '18px;padding:0;font-size:12.5px;color:#374151">%s</ul>'
                '</div>' % objs) if objs else
               '<div style="font-size:12.5px;color:#6b7280;margin:8px 0">'
               'No objections recorded on any call this window.</div>',
               ('<div style="margin:8px 0"><b style="font-size:13px;color:'
                '#0f172a">Open commitments from your 1-on-1s:</b>'
                '<ul style="margin:4px 0 0 18px;padding:0;font-size:12.5px;'
                'color:#374151">%s</ul></div>' % commits) if commits else "",
               lib, name.split()[0]))
    suggest = "".join(
        '<li style="margin:4px 0"><b>%s</b> <span style="color:#cbd5e1">'
        '%s</span></li>' % (_e(s["agent"]), _e(s["why"]))
        for s in data["suggest"])
    n = len(data["agents"])
    return (
        '<html><head><meta charset="utf-8"></head>'
        '<body style="margin:0;background:#f5f7fa">'
        '<div style="max-width:680px;margin:0 auto;font-family:-apple-system,'
        "'Segoe UI'"',Arial,sans-serif;padding:16px">'
        '<div style="background:#0f172a;border-radius:12px;padding:24px;'
        'text-align:center">'
        '<p style="margin:0;color:#f59e0b;font-size:12px;font-weight:700;'
        'text-transform:uppercase;letter-spacing:2px">Monday coaching brief'
        '</p>'
        '<h1 style="margin:8px 0 4px;color:#ffffff;font-size:24px;'
        'font-weight:800">The film on the %d agent%s you met</h1>'
        '<p style="margin:0;color:#94a3b8;font-size:13px">Last %d days of '
        'graded calls from Maverick. Every number links to its receipt.</p>'
        '%s</div>%s'
        '<p style="font-size:11.5px;color:#9ca3af;margin:16px 4px">Built '
        'from Danny\'s Impact Tracker meetings and the 1-on-1 log; agents '
        'not met in the window are not shown. Grades, objections and '
        'transcripts come from the nightly Maverick scrape. Audio lives in '
        'Maverick; transcripts in the Call Library.</p>'
        '</div></body></html>'
        % (n, "" if n == 1 else "s", data["window_days"],
           ('<div style="text-align:left;background:#1e293b;border-radius:'
            '8px;padding:10px 16px;margin-top:14px"><p style="margin:0;'
            'color:#f59e0b;font-size:12px;font-weight:700">Worth your hour '
            'this week:</p><ul style="margin:6px 0 0 18px;padding:0;'
            'color:#e2e8f0;font-size:13px">%s</ul></div>' % suggest)
           if suggest else "",
           "".join(rows)))
