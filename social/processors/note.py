"""Note / idea handler. Plain text or markdown note dropped in inbox.

A short text note gets fanned out:
- LinkedIn long-form (Barry's pillar platform for B2B)
- IG carousel (3-5 quote graphic slides)
- Threads (sharper, shorter take)
- Facebook (broadcast version)
- Email newsletter snippet (for the Tuesday send)
"""

NOTE_MIMES = {"text/plain", "text/markdown", "application/vnd.google-apps.document"}


def classify(file_meta: dict) -> bool:
    mime = file_meta.get("mimeType", "")
    title = file_meta.get("title", "").lower()
    if mime in NOTE_MIMES:
        return True
    return title.endswith((".md", ".txt"))


def treatment(file_meta: dict) -> dict:
    return {
        "kind": "note",
        "outputs": ["linkedin_long", "ig_carousel", "threads", "facebook", "newsletter_snippet"],
    }
