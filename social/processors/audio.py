"""Audio handler. Voice memos, podcast clips, m4a/mp3.

NOT YET IMPLEMENTED. Phase 2.
Requires whisper (or whisper.cpp) for transcription.

Once built, an audio file produces:
- Transcript -> note-style fan-out (LI, IG carousel, Threads, FB, newsletter)
- Optional: extract 30-60 sec audiogram clip for IG/Threads
"""

AUDIO_MIMES = {"audio/mpeg", "audio/mp4", "audio/m4a", "audio/wav", "audio/x-m4a"}


def classify(file_meta: dict) -> bool:
    return file_meta.get("mimeType", "").startswith("audio/")


def treatment(file_meta: dict) -> dict:
    return {
        "kind": "audio",
        "outputs": ["linkedin_long", "ig_carousel", "threads", "facebook", "newsletter_snippet"],
        "implemented": False,
        "blocking_dep": "whisper (install via brew install whisper-cpp or pip install openai-whisper)",
    }
