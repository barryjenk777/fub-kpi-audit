"""Per-input-type processors.

Each processor implements:
    classify(file_meta) -> bool  # does this processor handle this file?
    treatment(file_meta) -> dict # what platforms + formats does it produce?
"""
from . import photo, link, note, audio, video  # noqa: F401
