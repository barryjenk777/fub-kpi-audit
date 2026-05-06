"""Video handler. Phone Reels, Zoom recordings, keynote footage.

PARTIALLY IMPLEMENTED. Phase 1 = manual caption (drop a finished Reel + a sentence,
get platform variants). Phase 2 = clip extraction. Phase 3 = full keynote shred.

Currently supports: file metadata sniff and treatment routing.
Does NOT yet: extract clips (needs ffmpeg), transcribe (needs whisper).
"""

VIDEO_MIMES = {"video/mp4", "video/quicktime", "video/x-m4v", "video/webm"}


def classify(file_meta: dict) -> bool:
    return file_meta.get("mimeType", "").startswith("video/")


def treatment(file_meta: dict) -> dict:
    """Routes video into one of three tiers based on duration."""
    duration_sec = file_meta.get("videoMediaMetadata", {}).get("durationMillis", 0) / 1000
    if duration_sec == 0:
        tier = "tier_1_manual"
    elif duration_sec <= 90:
        tier = "tier_1_finished_reel"
    elif duration_sec <= 600:
        tier = "tier_2_clip_extraction"
    else:
        tier = "tier_3_keynote_shred"

    return {
        "kind": "video",
        "tier": tier,
        "duration_sec": duration_sec,
        "outputs": ["ig_reel", "tiktok", "youtube_short", "linkedin_video", "facebook_reel", "threads_video"],
        "implemented": tier == "tier_1_finished_reel",
        "blocking_deps": [] if tier == "tier_1_finished_reel" else ["ffmpeg", "whisper"],
    }
