"""Social content engine for Barry Jenkins thought-leadership pipeline.

Architecture:
    GDrive Content Inbox (Barry drops media)
        -> social.engine (scan + classify + plan)
        -> GDrive Content Drafts (week_of_<date>/post_<id>.md)
        -> Flask review UI (/social/review)
        -> blotato_client.publish_multi (cross-platform publish)
        -> GDrive Content Published (archive)

See briefs/ig_voice_guide.md for voice and brief.
"""
