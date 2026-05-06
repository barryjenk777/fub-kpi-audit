"""Flask Blueprint for the social review UI.

Mounts at /social/. Register in app.py:

    from social.web import bp as social_bp
    app.register_blueprint(social_bp)

Routes:
    GET  /social/review                       list of pending drafts grouped by source
    GET  /social/draft/<draft_id>             single draft detail
    POST /social/draft/<draft_id>/caption     edit caption for a platform
    POST /social/draft/<draft_id>/approve     mark approved + publish via Blotato
    POST /social/draft/<draft_id>/skip        mark all platforms skipped
"""

from __future__ import annotations

import json
from pathlib import Path

from flask import Blueprint, jsonify, render_template, request

from . import ai, config, engine, storage

import sys
sys.path.insert(0, str(config.REPO_ROOT))
import blotato_client

bp = Blueprint("social", __name__, url_prefix="/social", template_folder="templates")


def _load_draft(draft_id: str) -> tuple[dict | None, None]:
    """Returns (draft_dict, None). Second arg kept for API stability."""
    return storage.get(draft_id), None


def _save_draft(draft: dict, _unused=None) -> None:
    storage.save(draft)


@bp.route("/review")
def review():
    drafts = list(engine.list_existing_drafts().values())
    drafts.sort(key=lambda d: d["created_at"], reverse=True)
    return render_template("social_review.html", drafts=drafts)


@bp.route("/draft/<draft_id>")
def draft_detail(draft_id):
    d, _ = _load_draft(draft_id)
    if not d:
        return jsonify({"error": "not found"}), 404
    return jsonify(d)


@bp.route("/draft/<draft_id>/caption", methods=["POST"])
def edit_caption(draft_id):
    d, path = _load_draft(draft_id)
    if not d:
        return jsonify({"error": "not found"}), 404
    body = request.get_json(force=True)
    platform = body.get("platform")
    caption = body.get("caption", "")
    if platform not in d["platforms"]:
        return jsonify({"error": f"unknown platform {platform}"}), 400
    d["platforms"][platform]["caption"] = caption
    _save_draft(d, path)
    return jsonify({"ok": True, "platform": platform, "caption": caption})


@bp.route("/draft/<draft_id>/approve", methods=["POST"])
def approve(draft_id):
    """Approve specified platforms and immediately publish via Blotato."""
    d, path = _load_draft(draft_id)
    if not d:
        return jsonify({"error": "not found"}), 404
    body = request.get_json(force=True)
    platforms = body.get("platforms", [])
    if not platforms:
        return jsonify({"error": "no platforms specified"}), 400

    hosted = d["media"].get("blotato_hosted_url")
    if not hosted:
        public_url = d["media"].get("hosted_url")
        if not public_url:
            return jsonify({"error": "media not hosted yet — run upload step"}), 400
        hosted = blotato_client.upload_media(public_url)
        d["media"]["blotato_hosted_url"] = hosted
        _save_draft(d, path)

    results = {}
    for plat in platforms:
        if plat not in d["platforms"]:
            results[plat] = {"error": "unknown platform"}
            continue
        cap = d["platforms"][plat]["caption"]
        if not cap.strip():
            results[plat] = {"error": "empty caption"}
            continue
        resp = blotato_client.publish(plat, cap, [hosted])
        if resp.get("error"):
            d["platforms"][plat]["status"] = "failed"
        else:
            d["platforms"][plat]["status"] = "published"
            d["platforms"][plat]["submission_id"] = resp.get("postSubmissionId") or resp.get("id")
        results[plat] = resp
    _save_draft(d, path)
    return jsonify({"ok": True, "results": results})


@bp.route("/draft/<draft_id>/skip", methods=["POST"])
def skip(draft_id):
    d, path = _load_draft(draft_id)
    if not d:
        return jsonify({"error": "not found"}), 404
    for plat in d["platforms"]:
        d["platforms"][plat]["status"] = "skipped"
    _save_draft(d, path)
    return jsonify({"ok": True})


@bp.route("/draft/<draft_id>/revise", methods=["POST"])
def revise(draft_id):
    """Send the draft + a user prompt to Claude, get a revised caption back, save it."""
    d, path = _load_draft(draft_id)
    if not d:
        return jsonify({"error": "not found"}), 404
    body = request.get_json(force=True)
    platform = body.get("platform")
    prompt = (body.get("prompt") or "").strip()
    if platform not in d["platforms"]:
        return jsonify({"error": f"unknown platform {platform}"}), 400
    if not prompt:
        return jsonify({"error": "empty revision prompt"}), 400
    try:
        new_caption = ai.revise_caption(d, platform, prompt)
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": f"revision failed: {type(e).__name__}: {e}"}), 500
    d["platforms"][platform]["caption"] = new_caption
    _save_draft(d, path)
    return jsonify({"ok": True, "platform": platform, "caption": new_caption})
