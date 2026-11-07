# Claude Code handoff — Ylopo note inbound is unresolved

Do not implement a qualification change from this prompt until Barry Jenkins
answers the question at the bottom. The Nov 6 2026 patch on
`cursor/fix-new-lead-email-routing-diag-ff78` fixed a different, reproduced
defect (new-lead email `NameError`) and added delivery diagnostics. It did
not change who is offered on the claim board.

## Repo and entry points

- Repo: Legacy Command Center (`barryjenk777/fub-kpi-audit`).
- Webhook: `app.py` `webhook_fub` → `_fub_process_webhook` → `_desk_intake_people`.
- Same function is the safety sweep: `scheduled_desk_sweep`.
- Decision helper (no I/O): `desk_route.py` `evaluate_desk_route`,
  `text_is_recent_inbound`, `call_is_recent_inbound`.
- Native gate I/O: `app.py` `_had_inbound_within` (FUB `textMessages` and
  `calls` only). It does not call `get_person_notes`.
- Ops mirror: `app.py` `_desk_mirror_card` → `SLACK_DESK_OPS_CHANNEL`.
- Claim offers: `dispatch.py` `make_offer`, only when
  `dispatch_ai_live == "1"` and the helper returns `claim_offer`.
- Public wins channel default: `SLACK_WINS_CHANNEL` or `#lead-desk`.
  Claim posts there happen on accept (`api_slack_actions`), not at intake.
- Owned re-engagement ladder: `escalation.py` `open_escalation("owned_reengage")`,
  called only when the owner ping runs and `_ask and _engaged`.
- New-lead email (separate from the desk): `pond_mailer.py`
  `run_new_lead_mailer`. Shark Tank, unclaimed (or Barry), not an owned
  Ylopo re-engage.

## What the code does today

`DESK_CONVERSION_TAGS` in `app.py` admits a lead. `y_ai_priority` is in that
list and is then excluded from the human-ask set:

```text
ask = tags other than y_ai_priority
engaged = ask is non-empty AND a native inbound text (isIncoming)
          or a connected inbound call (isIncoming and duration > 0)
          whose timestamp, first 19 chars read as UTC, is within 24h
claim offer = dispatch_ai_live stripped == "1" AND not owned AND engaged
```

Owned means `assignedTo` is not blank, not Fhalen Tendencia, not Barry
Jenkins, and not in `COACHING_TEXT_EXCLUDED_AGENTS`.

Outcomes:

- Priority only (`y_ai_priority`): lane `AI priority`, ops mirror, soft
  owner ping if owned. No claim offer. No guarantee ladder.
- Human-ask tag, no native inbound: lane `AI follow-up attempt`. Mirror
  and a softer owner ping. No ladder.
- Human-ask tag plus native inbound, owned: lane `AI conversion`, hot
  owner ping, `owned_reengage` ladder. Not offered to teammates.
- Same, unowned, desk live: `dispatch.make_offer`.
- A call in the last 60 minutes on an owned lead: `transfer_already_routed`,
  no ping, no ladder.
- Duplicate webhooks: `filter_new_desk_tags` plus weekly
  `claim_once("aidispatch_<id>_<iso week>")`. A repeat logs
  `reason=already_routed_this_iso_week` and does not offer again.

Ylopo notes are not read. `fub_client.get_person_notes` exists and is used
by Save-Bot and Lead Memory. There is no `From:` parser and no
`peopleNotesCreated` webhook branch. A note body, affirmative or declining,
does not set `inbound_recent`.

The native gate does not read message text. Any `isIncoming` text inside
24h counts. Opt-out tags (`DO_NOT_CALL`, `NO_EMAIL`, `AI_OPT_OUT`, …) block
the new-lead email path. They are not consulted by `_desk_intake_people`.

Timestamps: `created[:19]` parsed as UTC. Correct for a trailing `Z`.
A numeric offset would be sliced off. Do not change the 24h window while
answering the note question.

## Observed examples (do not re-route from this prompt)

Partial audit only. Current tags are not trigger-time history.

- Ryan 304398 and Donn 304403: `Y_AI_PRIORITY` / soft priority notes, owned,
  ops mirrors. Ryan's last note `From:` declines a connect. Under current
  code these are priority signals, not claim-board conversions. Note
  `From:` lines do not change that.
- Noah 304358 and Steven Dailey 110613: aged alerts versus the Nov 4 ops
  minute. Not a fresh transfer.
- Jon 187267: `ISA_TRANSFER_UNSUCCESSFUL`, not a successful text handoff.
  The Nov 4 guarantee opened on the pre-`a7ae94e` build. Current code
  does not open the ladder without native inbound.
- Steven Byrd 304402: voice-transfer tag plus a long call, and a separate
  new-lead email `NameError` (`to_override` was undefined in
  `run_new_lead_mailer`). That email defect is fixed on the branch above.
  It does not explain lead-desk silence.
- Anita 304405: cash site, not Ylopo. Assignment to Fhalen is
  `claim_once`. Group DM `missing_scope` falls back to singles. Do not
  change OAuth scopes in code. Barry's CC DM showed the fallback ran.
- Melvina 304406: voice follow-up tags, not a demonstrated text handoff.

## Diagnostics added

`routing_delivery` log lines and `automation_event_log` rows:
`person_id`, class, decision, reason, destination, attempted_at, slack_ts,
error. No message body. `decision=sent` on a Slack post requires Slack
`ok` and a `ts`. A group-DM fallback with only some singles acknowledged
logs `error=partial`. Owner pings set `desk_owner_ping.success` only when
`dm_user` returns a ts.

## Tests already in the repo

```text
python3 -m unittest tests.test_desk_route tests.test_new_lead_email tests.test_slack_delivery tests.test_maverick_parsers
```

Fixtures are in those files. They use fake tokens and example.test
addresses. Do not point them at live FUB, Slack, SendGrid, or Postgres.

## Question for Barry (answer this before any eligibility edit)

When an owned Ylopo lead has a fresh note line `From: <lead>` that asks
for a human, and there is no native FUB inbound text, should that note
count as the 24h inbound half of a conversion?

Constraints if the answer is yes:

- Do not treat `y_ai_priority` alone, "successfully engage" summaries,
  outbound-only `To:` lines, or AI-authored summaries as that ask.
- Do not treat a decline, opt-out, or "I'll reach out" as a transfer.
  Ryan 304398 is the negative fixture.
- Do not treat stale note timestamps as fresh. Noah and Steven Dailey
  are the aged fixtures.
- Keep owned leads off the claim board. Keep unsuccessful voice
  (Jon) and cash assignment (Anita) on their existing paths.
- Preserve DNC / opt-out, `claim_once`, the tag ledger, the flood
  breaker, and the answered-call dedupe.
- Add fixtures for one affirmative note, one decline, one stale note,
  one outbound-only note, one duplicate webhook, and one Slack `ok:false`
  that must not log `decision=sent`.
- Do not backfill, replay webhooks, or message real leads.

If the answer is no, leave `evaluate_desk_route` as it is. The public
lead-desk quiet period for soft AI priority is then the current rule,
not a dropped Slack post.
