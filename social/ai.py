"""Caption revision via Anthropic API.

Used by the review UI's "Revise" affordance. Barry types a direction
("make it shorter", "tie it to the book", "more contrarian"), this
module sends the photo + current caption + voice brief + revision
prompt to Claude and returns a new caption.

The voice brief is large and rarely changes, so it's cached (ephemeral
prompt cache) to keep per-revision cost minimal.

Requires ANTHROPIC_API_KEY (env var or in credentials.json under
'anthropic_api_key').
"""

from __future__ import annotations

import os
from functools import lru_cache

import anthropic

from . import config

MODEL = "claude-opus-4-7"
MAX_TOKENS = 2000


PLATFORM_RULES = {
    "instagram": (
        "Instagram caption. Mobile-native rhythm, short paragraphs, line breaks. "
        "Sharp hook in the first line. Three-step framework or list. "
        "Soft CTA at the end. Max 5 hashtags total. Output the full caption "
        "including hashtags at the end. Use the structure from the voice brief."
    ),
    "linkedin": (
        "LinkedIn long-form post for B2B agent and broker audience. "
        "Lead with a sharp business truth. Name the agent's pain. "
        "Three-step framework. Close with a softer CTA. "
        "0 to 2 hashtags max, placed at the very end. "
        "Heavier on data, lighter on emoji. This is the speaker-booking "
        "and brokerage-deal channel: write to be reposted by leaders."
    ),
    "threads": (
        "Threads post. Shortest of all platforms. 1 to 3 sentences. "
        "Sharp hook + reframe. No hashtags. No CTA. Standalone hot take. "
        "Optimize for algo: be opinionated, contrarian, bait engagement honestly."
    ),
    "facebook": (
        "Facebook post for friends, peers, and warm audience. "
        "Warmer and more personal than Instagram. "
        "Less framework, more story. CTA can be conversational ('what would you add?'). "
        "0 to 2 hashtags."
    ),
}


@lru_cache(maxsize=1)
def _voice_brief() -> str:
    return config.BRIEF_PATH.read_text()


def _client() -> anthropic.Anthropic:
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        try:
            creds = config.load()
            key = creds.get("anthropic_api_key")
        except FileNotFoundError:
            pass
    if not key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY not set. Either export it in your shell, "
            "or add 'anthropic_api_key' to ~/.config/blotato/credentials.json."
        )
    return anthropic.Anthropic(api_key=key)


def revise_caption(draft: dict, platform: str, user_prompt: str) -> str:
    """Generate a revised caption for one platform based on user direction.

    Sends: voice brief (cached) + platform rules + photo + current caption + user prompt.
    Returns the new caption text.
    """
    if platform not in PLATFORM_RULES:
        raise ValueError(f"Unknown platform: {platform}")

    current = draft["platforms"][platform].get("caption", "")
    image_url = draft["media"].get("hosted_url")

    system = [
        {
            "type": "text",
            "text": _voice_brief(),
            "cache_control": {"type": "ephemeral"},
        },
        {
            "type": "text",
            "text": (
                f"PLATFORM RULES for {platform}:\n{PLATFORM_RULES[platform]}\n\n"
                "HARD RULE (no exceptions): NEVER use em-dashes (—) or en-dashes (–) in the caption. "
                "Use periods, commas, parentheses, or line breaks instead. "
                "If you find yourself reaching for an em-dash, rewrite as two short sentences."
            ),
        },
    ]

    user_content = []
    if image_url:
        user_content.append({
            "type": "image",
            "source": {"type": "url", "url": image_url},
        })

    user_content.append({
        "type": "text",
        "text": (
            f"Current {platform} caption:\n\n---\n{current}\n---\n\n"
            f"Barry's revision direction:\n{user_prompt}\n\n"
            f"Rewrite the {platform} caption following that direction. "
            "Output ONLY the new caption text. No preamble, no explanation, no labels, "
            "no surrounding markdown fences."
        ),
    })

    resp = _client().messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system,
        messages=[{"role": "user", "content": user_content}],
    )
    text = resp.content[0].text.strip()
    return text
