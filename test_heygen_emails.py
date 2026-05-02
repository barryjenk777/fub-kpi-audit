"""
Test: Send HeyGen video emails to Barry only — plain text format.

Plain text eliminates the HTML template spam signals that were hurting
deliverability (logo image, newsletter footer, tracking-style URL patterns).
The video link routes through /v/<token> on our Railway domain so heygen.com
never appears in the email body.

Two test leads:
  Sarah — Ylopo Prospecting seller, 412 Harbour View Dr, Suffolk
  Marcus — Z-buyer (cash offer), 2218 Kempsville Rd, Virginia Beach
  Jordan — Buyer searching in Chesapeake, $380k-$450k, 3br

No real leads are emailed. All sends go to barry@yourfriendlyagent.net.
"""

import logging
import os
import sys

# Load .env so local runs pick up keys
from pathlib import Path
env_file = Path(__file__).parent / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("test_heygen")

TEST_EMAIL = "barry@yourfriendlyagent.net"

SIGN_OFF = (
    "Barry Jenkins\n"
    "Legacy Home Team | LPT Realty\n"
    "(757) 919-8874\n"
    "www.legacyhomesearch.com"
)


def send_test_email(subject, body_text, body_html=None, label=""):
    """Send test email — HTML has slim thumbnail block, plain text is fallback."""
    from pond_mailer import send_email
    print(f"\n{'='*60}")
    print(f"  Sending test email: {label}")
    print(f"  To: {TEST_EMAIL}")
    print(f"  Subject: {subject}")
    print(f"  Format: {'HTML (slim thumbnail) + plain text' if body_html else 'plain text only'}")
    print(f"{'='*60}")
    result = send_email(
        to_email=TEST_EMAIL,
        subject=f"[TEST] {subject}",
        body_text=body_text,
        body_html=body_html,
        dry_run=False,
    )
    if result:
        print(f"  ✓ Sent successfully")
    else:
        print(f"  ✗ Send failed — check SendGrid logs")
    return result


def test_seller_email():
    """Sarah — Ylopo Prospecting seller at 412 Harbour View Dr, Suffolk."""
    first_name = "Sarah"
    street     = "412 Harbour View Dr"
    city       = "Suffolk"

    from heygen_client import (
        is_available,
        generate_seller_video_script,
        generate_and_wait,
        get_background_url,
        make_video_plain_text,
        make_video_email_html,
        DEFAULT_AVATAR, DEFAULT_VOICE,
    )

    if not is_available():
        print("  ✗ HEYGEN_API_KEY not set — skipping video generation")
        return False

    print(f"\n[Seller] Generating video script for {first_name} at {street}, {city}...")
    script = generate_seller_video_script(first_name=first_name, street=street, city=city)
    print(f"  Script ({len(script)} chars):\n  ---\n  {script[:300]}{'...' if len(script) > 300 else ''}\n  ---")

    bg_url = get_background_url("seller", address=street, city=city)
    print(f"  Background: {bg_url}")
    print(f"\n  Submitting to HeyGen and waiting (~60-120s)...")
    video_result = generate_and_wait(script, background_url=bg_url,
                                     avatar_id=DEFAULT_AVATAR, voice_id=DEFAULT_VOICE, timeout_seconds=360)

    if not video_result or not video_result.get("video_url"):
        print("  ✗ HeyGen video failed — sending text-only fallback (no video link)")
        body_text = (
            f"{first_name} —\n\n"
            f"I was pulling recent sale numbers for a few of my clients in {city} "
            f"when your place on {street} came up.\n\n"
            f"Would a quick 10-minute call make sense? Just reply here.\n\n"
            + SIGN_OFF
        )
        return send_test_email(
            subject=f"{first_name} — quick note about {street}",
            body_text=body_text,
            label=f"Seller (text-only fallback) — {first_name}",
        )

    print(f"  ✓ Video ready: {video_result['video_url'][:80]}...")
    print(f"  Duration: {video_result.get('duration', '?')}s")

    _setup = (f"{first_name} — I was pulling recent sale numbers for a few of my clients "
              f"in {city} when your place on {street} came up. Put together a short video for you.")
    _cta   = "Would a quick 10-minute call make sense? Just reply here."

    body_text = (
        f"{_setup}\n\n"
        f"{make_video_plain_text(video_result['video_url'], first_name=first_name, video_id=video_result.get('video_id', ''))}\n"
        f"{_cta}\n\n" + SIGN_OFF
    )
    body_html = make_video_email_html(
        setup_text=_setup,
        video_url=video_result["video_url"],
        thumbnail_url=video_result["thumbnail_url"],
        cta_text=_cta,
        first_name=first_name,
        caption=f"I looked into {street} before I hit record. Here's what stood out.",
        duration=video_result.get("duration", 0),
        video_id=video_result.get("video_id", ""),
    )

    return send_test_email(
        subject=f"{first_name} — quick video for {street}",
        body_text=body_text,
        body_html=body_html,
        label=f"Seller (HeyGen video + thumbnail) — {first_name}",
    )


def test_zbuyer_email():
    """Marcus — Z-buyer (cash offer request), 2218 Kempsville Rd, Virginia Beach."""
    first_name = "Marcus"
    street     = "2218 Kempsville Rd"
    city       = "Virginia Beach"

    from heygen_client import (
        is_available,
        generate_zbuyer_video_script,
        generate_and_wait,
        get_background_url,
        make_video_plain_text,
        make_video_email_html,
        DEFAULT_AVATAR, DEFAULT_VOICE,
    )

    if not is_available():
        print("  ✗ HEYGEN_API_KEY not set — skipping video generation")
        return False

    print(f"\n[Z-Buyer] Generating video script for {first_name} at {street}, {city}...")
    script = generate_zbuyer_video_script(first_name=first_name, street=street, city=city)
    print(f"  Script ({len(script)} chars):\n  ---\n  {script[:300]}{'...' if len(script) > 300 else ''}\n  ---")

    bg_url = get_background_url("zbuyer", address=street, city=city)
    print(f"  Background: {bg_url}")
    print(f"\n  Submitting to HeyGen and waiting (~60-120s)...")
    video_result = generate_and_wait(script, background_url=bg_url,
                                     avatar_id=DEFAULT_AVATAR, voice_id=DEFAULT_VOICE, timeout_seconds=360)

    if not video_result or not video_result.get("video_url"):
        print("  ✗ HeyGen video failed — sending text-only fallback")
        body_text = (
            f"{first_name} —\n\n"
            f"Saw your cash offer request for {street} come through.\n\n"
            f"10 minutes on the phone and I'll run both numbers for you. "
            f"Just reply here.\n\n"
            + SIGN_OFF
        )
        return send_test_email(
            subject=f"{first_name} — your home on {street}",
            body_text=body_text,
            label=f"Z-Buyer (text-only fallback) — {first_name}",
        )

    print(f"  ✓ Video ready: {video_result['video_url'][:80]}...")
    print(f"  Duration: {video_result.get('duration', '?')}s")

    _setup = (f"{first_name} — saw your cash offer request for {street} come through. "
              f"Put together a short video for you.")
    _cta   = f"10 minutes on the phone and I'll run both numbers for {street}. Just reply here."

    body_text = (
        f"{_setup}\n\n"
        f"{make_video_plain_text(video_result['video_url'], first_name=first_name, video_id=video_result.get('video_id', ''))}\n"
        f"{_cta}\n\n" + SIGN_OFF
    )
    body_html = make_video_email_html(
        setup_text=_setup,
        video_url=video_result["video_url"],
        thumbnail_url=video_result["thumbnail_url"],
        cta_text=_cta,
        first_name=first_name,
        caption=f"cash offer vs. listing — I ran both numbers for {street}.",
        duration=video_result.get("duration", 0),
        video_id=video_result.get("video_id", ""),
    )

    return send_test_email(
        subject=f"{first_name} — your home on {street}",
        body_text=body_text,
        body_html=body_html,
        label=f"Z-Buyer (HeyGen video + thumbnail) — {first_name}",
    )


def test_buyer_email():
    """Jordan — Ylopo buyer searching for homes in Chesapeake, $380k-$450k, 3br."""
    first_name  = "Jordan"
    city        = "Chesapeake"
    price_min   = 380000
    price_max   = 450000
    beds        = [3]
    prop_type   = "house"
    mv_street   = "812 Copperfield Dr"   # most-viewed property (simulated)
    strategy    = "repeat_view"
    view_count  = 7

    from heygen_client import (
        is_available,
        generate_buyer_video_script,
        generate_and_wait,
        get_background_url,
        make_video_plain_text,
        make_video_email_html,
        DEFAULT_AVATAR, DEFAULT_VOICE,
    )

    if not is_available():
        print("  ✗ HEYGEN_API_KEY not set — skipping video generation")
        return False

    print(f"\n[Buyer] Generating video script for {first_name} searching in {city} "
          f"${price_min//1000}k–${price_max//1000}k, {beds[0]}br...")
    script = generate_buyer_video_script(
        first_name=first_name,
        city=city,
        price_min=price_min,
        price_max=price_max,
        beds=beds,
        property_type=prop_type,
        most_viewed_street=mv_street,
        strategy=strategy,
        view_count=view_count,
    )
    print(f"  Script ({len(script)} chars):\n  ---\n  {script[:400]}{'...' if len(script) > 400 else ''}\n  ---")

    bg_url = get_background_url("buyer", city=city)
    print(f"  Background: {bg_url}")
    print(f"\n  Submitting to HeyGen and waiting (~60-120s)...")
    video_result = generate_and_wait(script, background_url=bg_url,
                                     avatar_id=DEFAULT_AVATAR, voice_id=DEFAULT_VOICE,
                                     timeout_seconds=360)

    # Copy depends on whether they've been circling a specific home
    if mv_street and strategy in ("saved_property", "repeat_view"):
        _setup_text = (
            f"{first_name} —\n\n"
            f"Saw you've been looking in {city} and circling back to {mv_street}. "
            f"Put together a short video for you."
        )
        _cta_text = (
            f"10 minutes and I can walk you through what I'm actually seeing on that one "
            f"— and your search overall. Just reply here."
        )
        _subj = f"{first_name} — I looked into {mv_street}"
    else:
        _setup_text = (
            f"{first_name} —\n\n"
            f"Saw your search come through for homes in {city}. "
            f"Put together a short video for you."
        )
        _cta_text = (
            f"10 minutes and I can walk you through exactly what I'm seeing right now. "
            f"Just reply here."
        )
        _subj = f"{first_name} — your {city} search"

    if not video_result or not video_result.get("video_url"):
        print("  ✗ HeyGen video failed — sending text-only fallback")
        body_text = f"{_setup_text}\n\n{_cta_text}\n\n" + SIGN_OFF
        return send_test_email(
            subject=_subj,
            body_text=body_text,
            label=f"Buyer (text-only fallback) — {first_name}",
        )

    print(f"  ✓ Video ready: {video_result['video_url'][:80]}...")
    print(f"  Duration: {video_result.get('duration', '?')}s")

    body_text = (
        f"{_setup_text}\n\n"
        f"{make_video_plain_text(video_result['video_url'], first_name=first_name, video_id=video_result.get('video_id', ''))}\n"
        f"{_cta_text}\n\n"
        + SIGN_OFF
    )
    _caption = (
        f"what I actually think about {mv_street}."
        if mv_street and strategy in ("saved_property", "repeat_view")
        else f"what the {city} market actually looks like right now."
    )
    body_html = make_video_email_html(
        setup_text=_setup_text,
        video_url=video_result["video_url"],
        thumbnail_url=video_result["thumbnail_url"],
        cta_text=_cta_text,
        first_name=first_name,
        caption=_caption,
        duration=video_result.get("duration", 0),
        video_id=video_result.get("video_id", ""),
    )

    return send_test_email(
        subject=_subj,
        body_text=body_text,
        body_html=body_html,
        label=f"Buyer (HeyGen video + thumbnail) — {first_name}",
    )


if __name__ == "__main__":
    print(f"\n{'#'*60}")
    print(f"  HeyGen Test — sending to {TEST_EMAIL} only")
    print(f"  Format: plain text only (no HTML, no images, no footer)")
    print(f"  Video links: /v/<token> on Railway domain (heygen.com hidden)")
    print(f"  NO real leads will be contacted.")
    print(f"{'#'*60}")

    ok1 = test_seller_email()
    ok2 = test_zbuyer_email()
    ok3 = test_buyer_email()

    print(f"\n{'='*60}")
    print(f"  Results:")
    print(f"    Seller email:  {'✓ sent' if ok1 else '✗ failed'}")
    print(f"    Z-buyer email: {'✓ sent' if ok2 else '✗ failed'}")
    print(f"    Buyer email:   {'✓ sent' if ok3 else '✗ failed'}")
    print(f"{'='*60}\n")

    sys.exit(0 if (ok1 and ok2 and ok3) else 1)
