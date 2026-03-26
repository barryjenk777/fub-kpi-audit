"""
Follow Up Boss API client.
Handles authentication, pagination, and rate limiting.
"""

import os
import time
import requests
from datetime import datetime, timedelta


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
        """Make a rate-limited request to the FUB API."""
        url = f"{self.BASE_URL}/{endpoint}"
        time.sleep(self.RATE_LIMIT_DELAY)
        self._request_count += 1

        response = self.session.request(
            method, url, params=params, json=json_data
        )
        response.raise_for_status()
        return response.json()

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

    # ---- Calls ----

    def get_calls(self, user_id=None, since=None, until=None):
        """
        Get calls within a date range.

        FUB's date filters and cursor pagination are unreliable, so we
        paginate newest-first using offsets and stop once we pass the
        `since` boundary.  Results are deduplicated and date-filtered
        client-side.
        """
        limit = 100
        offset = 0
        max_offset = 2000
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ") if since else None
        until_str = until.strftime("%Y-%m-%dT%H:%M:%SZ") if until else None

        all_calls = []
        seen_ids = set()

        while offset < max_offset:
            params = {"limit": limit, "offset": offset, "sort": "-created"}
            if user_id:
                params["userId"] = user_id

            data = self._request("GET", "calls", params=params)
            items = data.get("calls", [])

            if not items:
                break

            past_range = False
            for call in items:
                cid = call.get("id")
                created = call.get("created", "")

                # Skip duplicates
                if cid in seen_ids:
                    continue
                seen_ids.add(cid)

                # Stop if we've passed the since boundary
                if since_str and created < since_str:
                    past_range = True
                    break

                # Skip calls after until
                if until_str and created > until_str:
                    continue

                all_calls.append(call)

            if past_range or len(items) < limit:
                break

            offset += limit

        return all_calls

    # ---- People ----

    def get_people(self, assigned_user_id=None, tag=None, updated_since=None,
                   created_since=None, limit=100):
        """Get people (leads) with optional filters."""
        params = {"limit": limit}
        if assigned_user_id:
            params["assignedUserId"] = assigned_user_id
        if tag:
            params["tag"] = tag
        if updated_since:
            params["updatedSince"] = updated_since.strftime("%Y-%m-%dT%H:%M:%S")
        if created_since:
            params["createdSince"] = created_since.strftime("%Y-%m-%dT%H:%M:%S")
        return self._get_paginated("people", params)

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
        """Get text messages, optionally filtered."""
        params = {}
        if user_id:
            params["userId"] = user_id
        if since:
            params["dateFrom"] = since.strftime("%Y-%m-%d")
        return self._get_paginated("textMessages", params)

    # ---- Tasks ----

    def get_tasks(self, user_id=None, status=None):
        """Get tasks, optionally filtered."""
        params = {}
        if user_id:
            params["assignedUserId"] = user_id
        if status:
            params["status"] = status
        return self._get_paginated("tasks", params)

    @property
    def request_count(self):
        return self._request_count
