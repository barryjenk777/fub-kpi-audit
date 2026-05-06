"""Photo handler. iPhone HEIC, JPG, PNG.

A still photo gets:
- IG feed post (single image)
- LinkedIn image post (longer caption, B2B framing)
- Facebook post (cross-post from IG with light edit)
- Threads short take (1-3 sentences)
- Optional: IG carousel if photo can support a 3-5 slide quote-graphic story
"""

PHOTO_MIMES = {"image/jpeg", "image/jpg", "image/png", "image/heif", "image/heic"}


def classify(file_meta: dict) -> bool:
    return file_meta.get("mimeType", "") in PHOTO_MIMES


def treatment(file_meta: dict) -> dict:
    return {
        "kind": "photo",
        "outputs": ["ig_feed", "linkedin_image", "facebook", "threads"],
        "needs_orientation_fix": file_meta.get("mimeType") in {"image/heif", "image/heic", "image/jpeg"},
        "convert_to_jpg": file_meta.get("mimeType") in {"image/heif", "image/heic"},
    }
