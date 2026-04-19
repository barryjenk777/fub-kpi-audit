"""
Follow Up Boss API client.
Handles authentication, pagination, and rate limiting.
"""

import logging
import os
import time
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# FUB note formatter — structured email accountability record for agents
# ─────────────────────────────────────────────────────────────────────────────

def _build_email_note(subject, sequence_num, lead_type, avatar_used=None,
                      total_emails=9, cooldown_days=None):
    """Build a human-readable FUB note that tells an agent exactly what went out
    and what they should (or shouldn't) do next.

    Format is designed to be scanned in under 10 seconds:
      • Header: email number, type label
      • Subject line
      • One-liner on what the email does
      • Clear agent action with next-send timing
    """
    seq = sequence_num

    # ── Email type label ────────────────────────────────────────────────────
    is_video = bool(avatar_used)

    if lead_type == "zbuyer":
        ltype_label = "Z-Buyer / Cash Offer"
    elif lead_type in ("seller", "ylopo_seller", "ylopo_prospecting"):
        ltype_label = "Seller (Ylopo Prospecting)"
    else:
        ltype_label = "Buyer (IDX)"

    # Map sequence number to a human description of what the email does
    _seq_descriptions = {
        1: ("First Touch" + (" · HeyGen Video 🎥" if is_video else ""),
            "Personalized intro — behavioral observation + local market intel. "
            "Gives them a reason to reply without pressure.",
            "This is Email 1. No action needed — Barry's system will follow up automatically."),
        2: ("Listing Drop" if lead_type not in ("zbuyer", "seller", "ylopo_seller", "ylopo_prospecting")
            else "Follow-Up" + (" · HeyGen Video 🎥" if is_video else ""),
            ("Curated IDX listings matching their search. Short, personal, links embedded."
             if lead_type not in ("zbuyer", "seller", "ylopo_seller", "ylopo_prospecting")
             else "Adds one new piece of value the first video didn't cover. "
                  "Suit avatar — professional follow-through energy."),
            "If they reply → jump in and introduce yourself. "
            f"Next automated email in {cooldown_days or 10} days if no reply."),
        3: ("Breakup",
            "Short, honest question. No pressure. Gives them an easy way to reply "
            "yes or no — or to opt out gracefully.",
            "If they reply → jump in. This is the last sprint email. "
            "If no reply, drip phase starts automatically."),
        4: ("Drip · Re-engagement" + (" · HeyGen Video 🎥" if is_video else ""),
            "First drip content email after the breakup. "
            "Fresh angle — market insight or updated context. "
            + ("Suit avatar video re-ignites the relationship." if is_video
               else "Warm text, no links."),
            f"If they reply → jump in. Next automated email in {cooldown_days or 10} days if no reply."),
    }

    if seq in _seq_descriptions:
        type_label, what_it_does, agent_action = _seq_descriptions[seq]
    elif seq >= 5 and seq % 2 == 1:
        # Odd drip emails: listing drops
        type_label   = f"Drip · Listing Drop (Email {seq} of {total_emails})"
        what_it_does = "Curated IDX listings matching their search criteria. Short, personal, links embedded."
        agent_action = (f"If they reply → jump in. "
                        f"Next automated email in {cooldown_days or 10} days if no reply.")
    else:
        # Even drip emails: content
        type_label   = f"Drip · Content (Email {seq} of {total_emails})"
        what_it_does = "Longer warm content — market angle, story, or local intel. No links."
        agent_action = (f"If they reply → jump in. "
                        f"Next automated email in {cooldown_days or 10} days if no reply.")

    # Sequence completion (Email 9 or beyond)
    if seq >= total_emails:
        agent_action = (
            "⚠️  SEQUENCE COMPLETE — automated nurture is done for this lead. "
            "Barry will NOT send more automated emails. This lead needs human follow-up now."
        )

    note = (
        f"📧 AUTOMATED EMAIL — Email {seq} of {total_emails} · {type_label}\n"
        f"{'─' * 52}\n"
        f"Lead type : {ltype_label}\n"
        f"Subject   : \"{subject}\"\n"
        f"Video     : {'Yes — ' + avatar_used[:12] + '...' if is_video else 'No (text email)'}\n"
        f"\n"
        f"WHAT WENT OUT\n"
        f"{what_it_does}\n"
        f"\n"
        f"AGENT ACTION\n"
        f"{agent_action}\n"
        f"{'─' * 52}\n"
        f"Barry Jenkins AI Nurture · Legacy Home Team"
    )
    return note


class FUBClient:
    BASE_URL = "https://api.followupboss.com/v1"
    RATE_LIMIT_DELAY = 0.35  # ~170 requests/min to stay under 200/min limit

    def __init__(self, api_key=None):
        self.api_key = api_key or os.environ.get("FUB_API_KEY")
        if not self.api_key:
            raise ValueError(
                "FUB API key required. Set FUB_API_KEY environment variable "
                "or pass api_key to FUBClient."
            )
        self.session = requests.Session()
        self.session.auth = (self.api_key, "")
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        self._request_count = 0

    def _request(self, method, endpoint, params=None, json_data=None):
        """Make a rate-limited request to the FUB API with 429 retry backoff."""
        url = f"{self.BASE_URL}/{endpoint}"
        time.sleep(self.RATE_LIMIT_DELAY)
        self._request_count += 1

        for attempt in range(3):  # max 3 attempts
            response = self.session.request(
                method, url, params=params, json=json_data
            )
            if response.status_code == 429:
                wait = 3 * (attempt + 1)  # 3s, 6s, 9s — max 18s total
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response.json()

        # All retries exhausted — raise the last response
        response.raise_for_status()

    def _get_paginated(self, endpoint, params=None, max_pages=300):
        """Fetch all pages of a paginated endpoint.

        Uses offset-based pagination (preserves date filters) up to
        FUB's 2000-offset cap, then stops. For typical weekly audits
        this is sufficient.
        """
        if params is None:
            params = {}
        limit = params.setdefault("limit", 100)
        params.setdefault("offset", 0)

        all_items = []
        page = 0
        max_offset = 2000  # FUB rejects offsets beyond this

        while page < max_pages:
            data = self._request("GET", endpoint, params=params)

            # Extract items from response
            items = []
            if isinstance(data, dict):
                for key in [endpoint, "calls", "people", "textmessages",
                           "textMessages", "users", "groups", "tasks",
                           "appointments"]:
                    if key in data:
                        items = data[key]
                        break
                else:
                    if "_metadata" not in data:
                        return data
                    items = []

            all_items.extend(items)

            # Stop when we get fewer items than the limit (last page)
            if len(items) < limit:
                break

            next_offset = params["offset"] + limit
            if next_offset >= max_offset:
                break

            params["offset"] = next_offset
            page += 1

        return all_items

    # ---- Users ----

    def get_users(self):
        """Get all users (agents) in the account."""
        return self._get_paginated("users")

    def get_user_by_name(self, name):
        """Find a user by display name."""
        users = self.get_users()
        for user in users:
            full_name = f"{user.get('firstName', '')} {user.get('lastName', '')}".strip()
            if full_name.lower() == name.lower():
                return user
        return None

    def get_agents_with_email(self, excluded_names=None):
        """
        Return a list of active agent dicts with name, email, and FUB user ID.
        Filters out users in excluded_names (e.g. Barry, ISA, manager).
        """
        excluded = {n.lower() for n in (excluded_names or [])}
        users = self.get_users()
        agents = []
        for u in users:
            full_name = f"{u.get('firstName', '')} {u.get('lastName', '')}".strip()
            if full_name.lower() in excluded:
                continue
            # FUB returns isActive or active field
            if not u.get("isActive", u.get("active", True)):
                continue
            # Skip users with no email (system accounts)
            email = u.get("email", "")
            if not email:
                continue
            # FUB stores phone on the user object under several possible keys
            fub_phone = (u.get("mobilePhone") or u.get("phone") or
                         u.get("phoneNumber") or u.get("mobile") or "")
            agents.append({
                "name":        full_name,
                "fub_user_id": u.get("id"),
                "email":       email,
                "role":        u.get("roleType", u.get("role", "")),
                "fub_phone":   fub_phone or None,
            })
        return agents

    # ---- Deals ----

    def get_deals(self, since=None, stage=None, limit=100):
        """
        Fetch deals from FUB (populated by Dotloop via API Nation / native sync).

        FUB Deals endpoint: GET /v1/deals
        Each deal includes: id, name, price, stage, stageId, createdAt,
        updatedAt, closedAt, person (linked contact), assignedTo (agent name),
        assignedUserId.
        """
        params = {"limit": limit, "sort": "-updated"}
        if stage:
            params["stage"] = stage

        all_deals = []
        offset = 0
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ") if since else None

        while offset < 2000:
            params["offset"] = offset
            try:
                data = self._request("GET", "deals", params=params)
            except Exception as e:
                logger.warning("FUB deals fetch failed at offset %s: %s", offset, e)
                break

            # FUB wraps deals under "deals" key
            items = []
            if isinstance(data, dict):
                for key in ("deals", "opportunities"):
                    if key in data:
                        items = data[key]
                        break
            elif isinstance(data, list):
                items = data

            if not items:
                break

            past_range = False
            for deal in items:
                updated = deal.get("updated", deal.get("updatedAt", ""))
                if since_str and updated and updated < since_str:
                    past_range = True
                    break
                all_deals.append(deal)

            if past_range or len(items) < limit:
                break
            offset += limit

        return all_deals

    # ---- Calls ----

    def get_calls(self, user_id=None, person_id=None, since=None, until=None):
        """
        Get calls within a date range.

        When person_id is supplied, queries by personId directly — accurate
        regardless of how many calls the agent has (bypasses the 2000-offset
        pagination cap that would miss older calls for high-volume agents).

        Without person_id, paginates newest-first by userId and stops once
        past the `since` boundary (capped at offset 2000).
        """
        limit = 100
        offset = 0
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ") if since else None
        until_str = until.strftime("%Y-%m-%dT%H:%M:%SZ") if until else None

        all_calls = []
        seen_ids = set()

        # personId filter: FUB returns ALL calls for this person — no agent-volume
        # pagination issue. Date filtering is still done client-side.
        if person_id:
            params = {"personId": person_id, "limit": limit, "sort": "-created"}
            offset = 0
            while offset < 500:  # safety cap — no person will have 500+ calls
                params["offset"] = offset
                data = self._request("GET", "calls", params=params)
                items = data.get("calls", [])
                if not items:
                    break
                for call in items:
                    cid = call.get("id")
                    if cid in seen_ids:
                        continue
                    seen_ids.add(cid)
                    created = call.get("created", "")
                    if since_str and created < since_str:
                        continue
                    if until_str and created > until_str:
                        continue
                    all_calls.append(call)
                if len(items) < limit:
                    break
                offset += limit
            return all_calls

        # Bulk userId path.
        #
        # FUB's calls endpoint returns records newest-first with a hard offset
        # cap of 2000.  Post-window calls (system accounts, ISA, current week)
        # fill the cap before we reach the audit window — returning 0 calls.
        #
        # Fix: pass dateFrom/dateTo as server-side filters so FUB trims the
        # result set to the window before we paginate.  Client-side date checks
        # remain as a safety net in case the params are silently ignored.
        # FUB hard-caps the calls endpoint at offset=2000 — attempting offset=2000
        # returns 400 Bad Request. The while loop condition `while offset < 2000`
        # means the last valid iteration is offset=1900, giving us 2000 records max.
        # Per-agent userId filtering was tried but FUB ignores userId server-side,
        # so a single bulk fetch is the correct approach.
        max_offset = 2000
        while offset < max_offset:
            params = {"limit": limit, "offset": offset, "sort": "-created"}
            if user_id:
                params["userId"] = user_id
            # NOTE: FUB calls endpoint does NOT support dateFrom/dateTo — returns 400.
            # Date filtering is done client-side below (past_range early-exit).

            data = self._request("GET", "calls", params=params)
            items = data.get("calls", [])

            if not items:
                break

            past_range = False
            for call in items:
                cid = call.get("id")
                created = call.get("created", "")

                if cid in seen_ids:
                    continue
                seen_ids.add(cid)

                if since_str and created < since_str:
                    past_range = True
                    break

                if until_str and created > until_str:
                    continue

                all_calls.append(call)

            if past_range or len(items) < limit:
                break

            offset += limit

        return all_calls

    # ---- People ----

    def get_people(self, assigned_user_id=None, tag=None, updated_since=None,
                   created_since=None, pond_id=None, limit=100):
        """Get people (leads) with optional filters."""
        params = {"limit": limit}
        if assigned_user_id:
            params["assignedUserId"] = assigned_user_id
        if tag:
            params["tag"] = tag
        if updated_since:
            params["updatedSince"] = updated_since.strftime("%Y-%m-%dT%H:%M:%S")
        if created_since:
            # Always send UTC with Z suffix so FUB doesn't misinterpret as local time
            if created_since.tzinfo is None:
                created_since = created_since.replace(tzinfo=__import__("datetime").timezone.utc)
            params["createdSince"] = created_since.strftime("%Y-%m-%dT%H:%M:%SZ")
        if pond_id is not None:
            params["assignedPondId"] = pond_id
        return self._get_paginated("people", params)

    def get_people_recent(self, pond_id, limit=200):
        """Fetch the most recently-active leads from a pond, capped at `limit`.

        Sorted by lastActivity descending — surfaces highest-engagement leads
        first, which mirrors LeadStream score ordering without needing the
        full paginated pull of 2000+ records.
        """
        params = {
            "assignedPondId": pond_id,
            "limit": 100,           # page size
            "sort": "-lastActivity", # newest activity first
            "offset": 0,
        }
        all_leads = []
        seen_ids = set()

        while len(all_leads) < limit:
            data = self._request("GET", "people", params=params)
            items = data.get("people", []) if isinstance(data, dict) else []
            if not items:
                break
            for p in items:
                pid = p.get("id")
                if pid and pid not in seen_ids:
                    seen_ids.add(pid)
                    all_leads.append(p)
                    if len(all_leads) >= limit:
                        break
            if len(items) < 100:
                break  # last page
            params["offset"] += 100

        return all_leads

    # ---- Groups ----

    def get_groups(self):
        """Get all lead distribution groups."""
        return self._get_paginated("groups")

    def get_group(self, group_id):
        """Get a specific group by ID."""
        return self._request("GET", f"groups/{group_id}")

    def update_group(self, group_id, user_ids):
        """
        Update the members of a group.
        user_ids: list of FUB user IDs to include in the group.
        """
        return self._request("PUT", f"groups/{group_id}", json_data={
            "users": user_ids
        })

    # ---- Appointments ----

    def get_appointments(self, since=None, until=None):
        """Get appointments within a date range.

        FUB ignores date filter params on appointments, so we paginate
        newest-first and filter client-side by the 'start' field.
        """
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ") if since else None
        until_str = until.strftime("%Y-%m-%dT%H:%M:%SZ") if until else None

        limit = 100
        offset = 0
        max_offset = 2000
        all_appts = []
        seen_ids = set()

        while offset < max_offset:
            params = {"limit": limit, "offset": offset, "sort": "-created"}
            data = self._request("GET", "appointments", params=params)
            items = data.get("appointments", [])

            if not items:
                break

            past_range = False
            for appt in items:
                aid = appt.get("id")
                start = appt.get("start", appt.get("created", ""))

                if aid in seen_ids:
                    continue
                seen_ids.add(aid)

                if since_str and start < since_str:
                    past_range = True
                    break
                if until_str and start > until_str:
                    continue

                all_appts.append(appt)

            if past_range or len(items) < limit:
                break

            offset += limit

        return all_appts

    # ---- Text Messages ----

    def get_text_messages(self, user_id=None, since=None):
        """Get text messages, optionally filtered.

        Note: FUB's textMessages API rejects dateFrom without a userId.
        The date filter is only applied when a userId is also provided.
        """
        params = {}
        if user_id:
            params["userId"] = user_id
            if since:
                params["dateFrom"] = since.strftime("%Y-%m-%d")
        return self._get_paginated("textMessages", params)

    def count_texts_for_user(self, user_id, since=None, until=None, calls=None):
        """Count outbound texts for a user by scanning calls for their phone,
        then querying textMessages by fromNumber. Returns (outbound, inbound, unique_people)."""
        # Get user's phone number from provided calls or fetch
        phone = None
        if calls:
            for c in calls:
                if c.get("userId") == user_id and c.get("fromNumber"):
                    phone = c["fromNumber"]
                    break
        if not phone:
            recent_calls = self.get_calls(since=since, until=until)
            for c in recent_calls:
                if c.get("userId") == user_id and c.get("fromNumber"):
                    phone = c["fromNumber"]
                    break

        if not phone:
            return 0, 0, 0

        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ") if since else None
        until_str = until.strftime("%Y-%m-%dT%H:%M:%SZ") if until else None

        # Fetch outbound texts
        outbound = 0
        inbound = 0
        people = set()
        for direction_key, is_out in [("fromNumber", True), ("toNumber", False)]:
            offset = 0
            while offset < 2000:
                params = {"limit": 100, "offset": offset, direction_key: phone}
                data = self._request("GET", "textMessages", params=params)
                items = data.get("textMessages", data.get("textmessages", []))
                if not items:
                    break
                for msg in items:
                    created = msg.get("created", "")
                    if since_str and created < since_str:
                        break
                    if until_str and created >= until_str:
                        continue
                    if is_out:
                        outbound += 1
                    else:
                        inbound += 1
                    pid = msg.get("personId")
                    if pid:
                        people.add(pid)
                else:
                    if len(items) < 100:
                        break
                    offset += 100
                    continue
                break  # inner for-loop broke (date cutoff)

        return outbound, inbound, len(people)

    # ---- Tasks ----

    def get_tasks(self, user_id=None, status=None):
        """Get tasks, optionally filtered."""
        params = {}
        if user_id:
            params["assignedUserId"] = user_id
        if status:
            params["status"] = status
        return self._get_paginated("tasks", params)

    def create_task(self, person_id, assigned_user_id, name, due_date=None, description=None):
        """Create a task on a lead, assigned to a specific user."""
        payload = {
            "personId": person_id,
            "assignedUserId": assigned_user_id,
            "name": name,
        }
        if due_date:
            payload["dueDate"] = due_date
        if description:
            payload["description"] = description
        return self._request("POST", "tasks", json_data=payload)

    # ---- People (create / individual) ----

    def create_person(self, deduplicate=True, **fields):
        """Create a new person in FUB. Uses dedup by default to avoid duplicates.

        Example:
            client.create_person(
                firstName="John", lastName="Doe",
                emails=[{"type": "home", "value": "j@example.com"}],
                phones=[{"type": "mobile", "value": "5551234567"}],
                tags=["BatchLeads", "Pre-Foreclosure"],
                source="BatchLeads",
            )
        """
        fields["deduplicate"] = deduplicate
        return self._request("POST", "people", json_data=fields)

    def search_people_by_email(self, email):
        """Search for people by email address."""
        return self._get_paginated("people", {"email": email, "limit": 10})

    def search_people_by_phone(self, phone):
        """Search for people by phone number."""
        return self._get_paginated("people", {"phone": phone, "limit": 10})

    def get_person(self, person_id):
        """Get a single person by ID."""
        return self._request("GET", f"people/{person_id}")

    def add_tag(self, person_id, tag):
        """Add a tag to a person."""
        person = self.get_person(person_id)
        tags = person.get("tags", []) or []
        if tag not in tags:
            tags.append(tag)
            return self._request("PUT", f"people/{person_id}", json_data={"tags": tags})
        return person

    def remove_tag(self, person_id, tag):
        """Remove a tag from a person."""
        person = self.get_person(person_id)
        tags = person.get("tags", []) or []
        if tag in tags:
            tags.remove(tag)
            return self._request("PUT", f"people/{person_id}", json_data={"tags": tags})
        return person

    def add_tag_fast(self, person_id, tag, existing_tags, extra_fields=None):
        """Add a tag without fetching the person first (caller provides current tags).
        extra_fields: optional dict merged into the PUT payload (e.g. custom fields)."""
        if tag not in existing_tags:
            tags = list(existing_tags) + [tag]
            payload = {"tags": tags}
            if extra_fields:
                payload.update(extra_fields)
            return self._request("PUT", f"people/{person_id}", json_data=payload)
        elif extra_fields:
            # Tag already present but still need to update extra fields
            return self._request("PUT", f"people/{person_id}", json_data=extra_fields)
        return None

    def remove_tag_fast(self, person_id, tag, existing_tags, extra_fields=None):
        """Remove a tag without fetching the person first (caller provides current tags).
        extra_fields: optional dict merged into the PUT payload (e.g. custom fields)."""
        if tag in existing_tags:
            tags = [t for t in existing_tags if t != tag]
            payload = {"tags": tags}
            if extra_fields:
                payload.update(extra_fields)
            return self._request("PUT", f"people/{person_id}", json_data=payload)
        elif extra_fields:
            return self._request("PUT", f"people/{person_id}", json_data=extra_fields)
        return None

    def get_people_by_tag(self, tag):
        """Fetch leads and client-side filter by tag.

        NOTE: FUB's tag= filter on the people endpoint does NOT reliably filter
        by tag — it appears to return all leads. We fetch all leads and filter
        client-side to find those that actually have the tag.
        """
        all_people = self.get_all_people()
        return [p for p in all_people if tag in (p.get("tags") or [])]

    def get_all_people(self):
        """Fetch all leads in FUB (no filter). Used for client-side tag filtering."""
        return self._get_paginated("people", {"limit": 100})

    def log_email_sent(self, person_id, subject, message, user_id=None,
                       sequence_num=None, lead_type=None, avatar_used=None,
                       total_emails=9, cooldown_days=None):
        """Log an outbound automated email to a person's FUB activity timeline.

        FUB's /v1/emails endpoint returns 403 for third-party integrations.
        We use /v1/notes instead — appears in the contact timeline so agents
        can see exactly what went out, what it does, and what action to take.

        When sequence_num / lead_type are provided the note is a structured
        accountability record an agent can read in 10 seconds. Without them
        it falls back to the old subject + preview format.

        Non-fatal: logs a warning and returns None on failure.
        """
        if sequence_num is not None and lead_type is not None:
            note_body = _build_email_note(
                subject=subject,
                sequence_num=sequence_num,
                lead_type=lead_type,
                avatar_used=avatar_used,
                total_emails=total_emails,
                cooldown_days=cooldown_days,
            )
        else:
            # Legacy fallback — plain preview (used by new_lead_mailer etc.)
            preview = (message or "")[:600].strip()
            if len(message or "") > 600:
                preview += "\n[...truncated]"
            note_body = f"📧 EMAIL SENT\nSubject: \"{subject}\"\n\n{preview}"

        try:
            payload = {"personId": int(person_id), "body": note_body}
            if user_id:
                payload["userId"] = user_id
            try:
                result = self._request("POST", "notes", json_data=payload)
                logger.info("FUB note posted for person %s (seq %s)", person_id, sequence_num)
                print(f"    📝 FUB note posted (person {person_id}, seq {sequence_num})")
                return result
            except Exception as e_with_user:
                # userId may be rejected if it references a system account.
                # Retry without it — note will post as the API key owner.
                if user_id:
                    logger.warning(
                        "FUB note with userId=%s failed for person %s: %s — retrying without userId",
                        user_id, person_id, e_with_user,
                    )
                    try:
                        payload.pop("userId", None)
                        result = self._request("POST", "notes", json_data=payload)
                        logger.info("FUB note posted (no userId) for person %s (seq %s)", person_id, sequence_num)
                        print(f"    📝 FUB note posted without userId (person {person_id}, seq {sequence_num})")
                        return result
                    except Exception as e_no_user:
                        logger.warning(
                            "FUB note failed (both attempts) for person %s: %s",
                            person_id, e_no_user,
                        )
                        print(f"    ⚠️  FUB note FAILED for person {person_id}: {e_no_user}")
                        return None
                else:
                    logger.warning("FUB note failed for person %s: %s", person_id, e_with_user)
                    print(f"    ⚠️  FUB note FAILED for person {person_id}: {e_with_user}")
                    return None
        except Exception as e:
            logger.warning("FUB email note log error for person %s: %s", person_id, e)
            return None

    def get_events(self, since=None, event_type=None, limit=100, max_pages=5):
        """Get events (site visits, property views, etc.) with optional filters.

        Event types include: 'Viewed Page', 'Viewed Property', 'Property Saved',
        'Registration', 'Searched Properties', etc.

        max_pages defaults to 5 (500 events) to avoid rate limiting on
        high-volume event types.
        """
        params = {"limit": limit, "sort": "-created"}
        if event_type:
            params["type"] = event_type
        if since:
            params["since"] = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        return self._get_paginated("events", params, max_pages=max_pages)

    def get_events_for_person(self, person_id, days=30, limit=50):
        """Get all IDX events for a specific lead.

        Returns property views, saves, and search page visits for the lead.
        Uses personId filter directly — returns all events regardless of
        agent or volume constraints.

        Each 'Viewed Property' and 'Property Saved' event includes a full
        property object: street, city, state, code (zip), price, bedrooms,
        bathrooms, area (sqft), mlsNumber, url.
        """
        from datetime import datetime, timedelta, timezone
        since = datetime.now(timezone.utc) - timedelta(days=days)
        params = {
            "personId": person_id,
            "limit": limit,
            "sort": "-created",
            "since": since.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        data = self._request("GET", "events", params=params)
        return data.get("events", [])

    @property
    def request_count(self):
        return self._request_count
