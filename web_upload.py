"""
Serves the BIN tool HTML + catalog API + sendout to Telegram.
Local: http://127.0.0.1:8787/ (or UPLOAD_SERVER_PORT). If PORT is set (e.g. Railway),
binds 0.0.0.0 for a public URL; GET /health for load balancers.
"""

from __future__ import annotations

import hmac
import logging
import os
import socket
import threading
import time
from pathlib import Path

import requests
from flask import Flask, Response, jsonify, request, send_from_directory
from requests.adapters import HTTPAdapter

from bin_leads_store import merge_groups_from_web, stock_tiers_api_payload
from catalog_store import format_sendout_text, load_catalog

logger = logging.getLogger(__name__)

# Reuse TCP connections to HandyAPI (many parallel /lookup requests from the browser).
_HANDY_HTTP = requests.Session()
_HANDY_HTTP.mount("https://", HTTPAdapter(pool_connections=64, pool_maxsize=64, max_retries=0))

# HandyAPI (https://data.handyapi.com) — /api/bin-lookup uses the secret key on the server only.
# Frontend (publishable): safe in browsers. Backend (secret): never expose to the client.
# Override with HANDYAPI_SECRET_KEY / HANDYAPI_KEY in .env in production.
_DEFAULT_HANDY_PUBLISHABLE = "PUB-0YJ4vbkT51j6WsUzdVmHhP2xq"  # Frontend API key (publishable)
_DEFAULT_HANDY_SECRET = "HAS-0YJ8gymGXS4rwKkctUbV"  # Backend API key (secret)


def _handyapi_key() -> str:
    for candidate in (
        os.environ.get("HANDYAPI_SECRET_KEY", "").strip(),
        os.environ.get("HANDYAPI_KEY", "").strip(),
        _DEFAULT_HANDY_SECRET,
        _DEFAULT_HANDY_PUBLISHABLE,
    ):
        if candidate:
            return candidate
    return _DEFAULT_HANDY_PUBLISHABLE


def _issuer_from_handy_payload(payload: object) -> str:
    """Extract bank/issuer string from HandyAPI JSON (varied shapes)."""
    if not isinstance(payload, dict):
        return ""
    key_order = (
        "Issuer",
        "issuer",
        "Bank",
        "bank",
        "CardIssuer",
        "cardIssuer",
        "ISSUER",
        "Company",
        "company",
        "IssuingBank",
        "issuingBank",
        "Issuing_organization",
    )
    for k in key_order:
        v = payload.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict):
            for ik in ("Name", "name", "Issuer", "issuer", "FullName", "fullName"):
                inner = v.get(ik)
                if isinstance(inner, str) and inner.strip():
                    return inner.strip()
    for alt in (
        "issuer_name",
        "IssuerName",
        "bank_name",
        "BankName",
        "issuing_bank",
        "IssuingBankName",
    ):
        v = payload.get(alt)
        if isinstance(v, str) and v.strip():
            return v.strip()
    for nest in ("Data", "Result", "data", "result", "BIN", "bin", "Payload"):
        sub = payload.get(nest)
        if isinstance(sub, dict):
            got = _issuer_from_handy_payload(sub)
            if got:
                return got
    return ""


def _issuer_from_binlist_payload(payload: dict) -> str:
    """Bank label from binlist.net API v3 JSON (https://binlist.net/)."""
    bank = payload.get("bank")
    if not isinstance(bank, dict):
        return ""
    name = str(bank.get("name") or "").strip()
    city = str(bank.get("city") or "").strip()
    if name and city:
        return f"{name}, {city}"
    return name


def _greip_api_key() -> str:
    return (os.environ.get("GREIP_API_KEY") or os.environ.get("GREIP_KEY") or "").strip()


def _issuer_from_greip_payload(payload: object) -> str:
    """Bank label from Greip BIN response (https://docs.greip.io/api-reference/endpoint/data-lookup/bin)."""
    if not isinstance(payload, dict):
        return ""
    if str(payload.get("status") or "").lower() != "success":
        return ""
    data = payload.get("data")
    if not isinstance(data, dict):
        return ""
    info = data.get("info")
    if not isinstance(info, dict):
        return ""
    bank = info.get("bank")
    if not isinstance(bank, dict):
        return ""
    name = str(bank.get("name") or "").strip()
    city = str(bank.get("city") or "").strip()
    if name and city:
        return f"{name}, {city}"
    return name


def _greip_bin_lookup_dict(d: str) -> dict:
    """Greip BIN/IIN lookup via https://greipapi.com/lookup/bin (Bearer key)."""
    key = _greip_api_key()
    if not key:
        return {
            "Status": "ERROR",
            "Message": "Set GREIP_API_KEY (Greip dashboard API key)",
            "LookupComplete": True,
        }
    try:
        r = requests.get(
            "https://greipapi.com/lookup/bin",
            params={"bin": d, "format": "JSON", "mode": "live"},
            headers={
                "Authorization": f"Bearer {key}",
                "Accept": "application/json",
            },
            timeout=10,
        )
        ct = (r.headers.get("content-type") or "").lower()
        if "application/json" not in ct:
            return {
                "Status": "ERROR",
                "Message": (r.text or f"HTTP {r.status_code}")[:240],
                "LookupComplete": bool(not r.ok),
            }
        try:
            body = r.json()
        except ValueError:
            return {
                "Status": "ERROR",
                "Message": "Invalid JSON from Greip",
                "LookupComplete": True,
            }
        if not isinstance(body, dict):
            return {
                "Status": "ERROR",
                "Message": "Invalid Greip response",
                "LookupComplete": True,
            }
        if str(body.get("status") or "").lower() == "error":
            desc = str(
                body.get("description") or body.get("type") or "Greip API error"
            ).strip()
            code = body.get("code")
            if code is not None:
                desc = f"[{code}] {desc}"
            return {
                "Status": "ERROR",
                "Message": desc[:280],
                "LookupComplete": True,
                "greip": body,
            }
        iss = _issuer_from_greip_payload(body)
        out = {
            "Status": "SUCCESS",
            "Issuer": iss if iss else "",
            "LookupComplete": True,
            "greip": body,
        }
        return out
    except requests.RequestException as e:
        logger.warning("Greip BIN %s: %s", d, e)
        return {
            "Status": "ERROR",
            "Message": str(e)[:200],
            "LookupComplete": False,
        }


def _bin_lookup_backend() -> str:
    """Default: both — HandyAPI first, then binlist.net if still no issuer (reduces 'Unknown Bank').
    Set BIN_LOOKUP_BACKEND=handy to skip binlist. Other: binlist | greip | greip_then_handy.
    """
    v = (os.environ.get("BIN_LOOKUP_BACKEND") or "").strip().lower()
    allowed = ("binlist", "handy", "both", "greip", "greip_then_handy")
    if v in allowed:
        return v
    return "both"


def _handy_bin_lookup_dict(d: str) -> dict:
    """Call HandyAPI with enough timeout for parallel load; short backoff between retries."""
    last_payload: dict | None = None
    for attempt in range(5):
        try:
            r = _HANDY_HTTP.get(
                f"https://data.handyapi.com/bin/{d}",
                headers={
                    "x-api-key": _handyapi_key(),
                    "Accept": "application/json",
                },
                timeout=22,
            )
            ct = (r.headers.get("content-type") or "").lower()
            if "application/json" in ct:
                try:
                    payload = r.json()
                except ValueError:
                    payload = {"Status": "ERROR", "Message": (r.text or "")[:240]}
            else:
                payload = {"Status": "ERROR", "Message": (r.text or "")[:240]}
            if not isinstance(payload, dict):
                payload = {"Status": "ERROR", "Message": "Invalid JSON from HandyAPI"}
            last_payload = payload
            iss = _issuer_from_handy_payload(payload)
            if r.ok and iss:
                out = dict(payload)
                if not str(out.get("Issuer") or "").strip():
                    out["Issuer"] = iss
                if str(out.get("Status") or "").upper() != "SUCCESS":
                    out["Status"] = "SUCCESS"
                out["LookupComplete"] = True
                return out
            if not r.ok:
                logger.warning(
                    "HandyAPI HTTP %s for BIN %s (try %s)",
                    r.status_code,
                    d,
                    attempt + 1,
                )
        except requests.RequestException as e:
            logger.warning("HandyAPI request BIN %s (try %s): %s", d, attempt + 1, e)
        if attempt < 4:
            time.sleep(0.08 + 0.07 * attempt)
    if isinstance(last_payload, dict):
        iss = _issuer_from_handy_payload(last_payload)
        if iss:
            out = dict(last_payload)
            out["Issuer"] = iss
            out["Status"] = "SUCCESS"
            out["LookupComplete"] = True
            return out
        if str(last_payload.get("Status") or "").upper() == "SUCCESS":
            out = dict(last_payload)
            if not str(out.get("Issuer") or "").strip():
                out["Issuer"] = ""
            out["LookupComplete"] = True
            return out
    return {
        "Status": "ERROR",
        "Message": "BIN lookup failed (HandyAPI)",
        "LookupComplete": True,
    }


def _response_success_with_issuer(d: dict) -> bool:
    if str(d.get("Status") or "").upper() != "SUCCESS":
        return False
    if str(d.get("Issuer") or "").strip():
        return True
    return bool(_issuer_from_handy_payload(d))


def _leadbot_api_secret_ok() -> bool:
    """When LEADBOT_API_SECRET is set, POST /api/sync-groups and /api/sendout require header X-Leadbot-Secret."""
    expected = os.environ.get("LEADBOT_API_SECRET", "").strip()
    if not expected:
        return True
    got = (request.headers.get("X-Leadbot-Secret") or "").strip()
    try:
        return hmac.compare_digest(
            got.encode("utf-8"), expected.encode("utf-8")
        )
    except Exception:
        return False


def create_app(html_file: Path) -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 32 * 1024 * 1024
    parent = html_file.resolve().parent
    fname = html_file.name

    @app.before_request
    def _cors_preflight() -> Response | None:
        if request.method == "OPTIONS" and request.path.startswith("/api/"):
            r = Response(status=204)
            r.headers["Access-Control-Allow-Origin"] = "*"
            r.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
            r.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Leadbot-Secret"
            return r
        return None

    @app.after_request
    def _cors_headers(response):
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Leadbot-Secret"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        return response

    @app.route("/")
    def index():
        if not html_file.is_file():
            return "<p>BIN tool HTML missing.</p>", 404
        return send_from_directory(str(parent), fname)

    @app.get("/api/catalog")
    def api_catalog():
        return jsonify(load_catalog())

    @app.get("/health")
    def health():
        return jsonify(ok=True), 200

    @app.get("/api/genderize")
    def api_genderize():
        """Proxy genderize.io (first-name gender) for the HTML tool — same-origin, no CORS issues."""
        raw = (request.args.get("name") or "").strip()
        if len(raw) < 2:
            return jsonify(count=0, name=raw, gender=None, probability=0.0), 400
        name = raw[:80]
        try:
            r = requests.get(
                "https://api.genderize.io",
                params={"name": name},
                timeout=15,
            )
            if not r.ok:
                return (
                    jsonify(
                        error="genderize.io HTTP %s" % r.status_code,
                        name=name,
                    ),
                    502,
                )
            payload = r.json()
            if not isinstance(payload, dict):
                return jsonify(error="Invalid JSON from genderize.io"), 502
            return jsonify(payload), 200
        except requests.RequestException as e:
            logger.warning("genderize %s: %s", name, e)
            return jsonify(error=str(e)[:160], name=name), 502

    @app.get("/api/agify")
    def api_agify():
        """Proxy agify.io (first-name predicted age) for the HTML tool — same-origin, no CORS."""
        raw = (request.args.get("name") or "").strip()
        if len(raw) < 2:
            return jsonify(count=0, name=raw, age=None), 400
        name = raw[:80]
        try:
            r = requests.get(
                "https://api.agify.io",
                params={"name": name},
                timeout=15,
            )
            if not r.ok:
                return (
                    jsonify(
                        error="agify.io HTTP %s" % r.status_code,
                        name=name,
                    ),
                    502,
                )
            payload = r.json()
            if not isinstance(payload, dict):
                return jsonify(error="Invalid JSON from agify.io"), 502
            return jsonify(payload), 200
        except requests.RequestException as e:
            logger.warning("agify %s: %s", name, e)
            return jsonify(error=str(e)[:160], name=name), 502

    @app.get("/api/bin-lookup/<bin6>")
    def api_bin_lookup(bin6: str):
        """BIN for HTML tool: Greip (https://greip.io) + Handy + binlist — BIN_LOOKUP_BACKEND / GREIP_API_KEY."""
        d = "".join(c for c in str(bin6) if c.isdigit())[:6]
        if len(d) != 6:
            return jsonify(Status="ERROR", Message="Need 6-digit BIN"), 400
        backend = _bin_lookup_backend()

        if backend in ("greip", "greip_then_handy"):
            greip_out = _greip_bin_lookup_dict(d)
            if _response_success_with_issuer(greip_out):
                return jsonify(greip_out), 200
            if backend == "greip":
                if str(greip_out.get("Status") or "").upper() == "SUCCESS":
                    return jsonify(greip_out), 200
                return jsonify(greip_out), 502

        if backend in ("handy", "both", "greip_then_handy"):
            handy_out = _handy_bin_lookup_dict(d)
            if _response_success_with_issuer(handy_out):
                return jsonify(handy_out), 200
            if backend == "handy":
                if str(handy_out.get("Status") or "").upper() == "SUCCESS":
                    return jsonify(handy_out), 200
                return jsonify(handy_out), 502

        if backend not in ("binlist", "both", "greip_then_handy"):
            return jsonify(Status="ERROR", Message="BIN lookup exhausted"), 502

        try:
            r = requests.get(
                f"https://lookup.binlist.net/{d}",
                headers={
                    "Accept-Version": "3",
                    "Accept": "application/json",
                },
                timeout=12,
            )
            if r.status_code == 404:
                return jsonify(
                    Status="ERROR",
                    Message="BIN not found",
                    LookupComplete=True,
                ), 404
            if r.status_code == 429:
                logger.warning("binlist.net rate limited for BIN %s", d)
                return (
                    jsonify(
                        Status="ERROR",
                        Message="binlist.net rate limit — see https://binlist.net/ (free tier is strict)",
                        LookupComplete=True,
                    ),
                    429,
                )
            ct = (r.headers.get("content-type") or "").lower()
            if "application/json" not in ct or not r.ok:
                return (
                    jsonify(
                        Status="ERROR",
                        Message=(r.text or f"HTTP {r.status_code}")[:240],
                    ),
                    502,
                )
            try:
                raw = r.json()
            except ValueError:
                return jsonify(Status="ERROR", Message="Invalid JSON from binlist.net"), 502
            if not isinstance(raw, dict):
                return jsonify(Status="ERROR", Message="Invalid binlist.net response"), 502
            iss = _issuer_from_binlist_payload(raw)
            out = {
                "Status": "SUCCESS",
                "Issuer": iss if iss else "",
                "LookupComplete": True,
                "binlist": raw,
            }
            return jsonify(out), 200
        except requests.RequestException as e:
            logger.warning("bin-lookup %s: %s", d, e)
            return jsonify(Status="ERROR", Message=str(e)[:200]), 502

    @app.get("/api/stock-tiers")
    def api_stock_tiers():
        try:
            return jsonify(stock_tiers_api_payload())
        except Exception as e:
            logger.exception("stock-tiers")
            return jsonify(error=str(e)[:200]), 500

    @app.post("/api/sync-groups")
    def api_sync_groups():
        if not _leadbot_api_secret_ok():
            return (
                jsonify(
                    ok=False,
                    error="Unauthorized — wrong or missing X-Leadbot-Secret (set LEADBOT_API_SECRET on server).",
                ),
                401,
            )
        body = request.get_json(silent=True) or {}
        groups = body.get("groups")
        if not isinstance(groups, dict):
            return (
                jsonify(
                    ok=False,
                    error='Expected JSON: { "groups": {...}, "tier": "first"|"second" }',
                ),
                400,
            )
        tier = body.get("tier", "first")
        try:
            stats = merge_groups_from_web(groups, tier=tier)
        except Exception as e:
            logger.exception("sync-groups")
            return jsonify(ok=False, error=str(e)[:240]), 500
        return jsonify(ok=True, **stats)

    @app.post("/api/sendout")
    def api_sendout():
        if not _leadbot_api_secret_ok():
            return (
                jsonify(
                    ok=False,
                    error="Unauthorized — wrong or missing X-Leadbot-Secret (set LEADBOT_API_SECRET on server).",
                ),
                401,
            )
        token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        chat = os.environ.get("UPLOAD_NOTIFY_CHAT_ID", "").strip()
        if not token or not chat:
            return (
                jsonify(
                    ok=False,
                    error="Set TELEGRAM_BOT_TOKEN and UPLOAD_NOTIFY_CHAT_ID in .env",
                ),
                400,
            )

        text = format_sendout_text()
        data = load_catalog()
        bins = data.get("bins", [])

        try:
            if len(text) <= 3800:
                r = requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={
                        "chat_id": chat,
                        "text": text,
                    },
                    timeout=60,
                )
            else:
                fname_doc = "bin_sendout.txt"
                r = requests.post(
                    f"https://api.telegram.org/bot{token}/sendDocument",
                    data={
                        "chat_id": chat,
                        "caption": (
                            "📤 Sendout — firsthand + secondhand "
                            f"({len(bins)} catalog BINs). "
                            "Brand sections are in the file; bot command /stock lists by network."
                        ),
                    },
                    files={"document": (fname_doc, text.encode("utf-8"))},
                    timeout=120,
                )
            if not r.ok:
                logger.error("Telegram send failed: %s %s", r.status_code, r.text[:400])
                return (
                    jsonify(ok=False, error="Telegram API error", detail=r.text[:200]),
                    502,
                )
        except requests.RequestException as e:
            logger.exception("sendout request")
            return jsonify(ok=False, error=str(e)[:200]), 502

        return jsonify(ok=True, bins=len(bins))

    return app


def _wait_for_listen(port: int, *, timeout: float = 45.0) -> bool:
    """Block until something accepts TCP on 127.0.0.1:port (Flask bound to 0.0.0.0)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1.5):
                return True
        except OSError:
            time.sleep(0.15)
    return False


def run_public_http_forever(html_file: Path) -> None:
    """
    Bind $PORT on 0.0.0.0 in the current thread (blocking).
    Use this on Railway so the container's main process holds the HTTP listener.
    """
    raw = os.environ.get("PORT", "").strip()
    if not raw:
        raise RuntimeError("run_public_http_forever requires PORT")
    p = int(raw)
    host = "0.0.0.0"
    if not html_file.is_file():
        logger.warning(
            "HTML tool not found (%s) — serving /health + APIs only",
            html_file,
        )
    app = create_app(html_file)
    from waitress import serve

    logger.info("Waitress binding %s:%s (main thread, Railway)", host, p)
    serve(app, host=host, port=p, threads=32, channel_timeout=90)


def start_upload_server_background(
    html_file: Path,
    *,
    port: int | None = None,
) -> threading.Thread | None:
    env_port = os.environ.get("PORT", "").strip()
    if not html_file.is_file():
        if not env_port:
            logger.warning(
                "HTML tool not found (%s) — web server not started (set PORT to serve /health only)",
                html_file,
            )
            return None
        logger.warning(
            "HTML tool not found (%s) — serving /health + APIs only (Railway / cloud)",
            html_file,
        )
    if port is not None:
        host, p = "127.0.0.1", int(port)
    elif env_port:
        host, p = "0.0.0.0", int(env_port.strip())
    else:
        host = os.environ.get("FLASK_HOST", "127.0.0.1")
        p = int(os.environ.get("UPLOAD_SERVER_PORT", "8787"))
    app = create_app(html_file)

    def run() -> None:
        try:
            if env_port:
                from waitress import serve

                serve(app, host=host, port=p, threads=32, channel_timeout=90)
            else:
                app.run(
                    host=host,
                    port=p,
                    use_reloader=False,
                    debug=False,
                    threaded=True,
                )
        except Exception:
            logger.exception("Web server thread failed")
            raise

    t = threading.Thread(target=run, daemon=True, name="web-tool")
    t.start()
    if env_port and not _wait_for_listen(p, timeout=45.0):
        logger.error(
            "HTTP server did not bind on port %s within timeout — health checks may fail",
            p,
        )
    if host == "0.0.0.0":
        logger.info("BIN tool + sendout: http://0.0.0.0:%s/ (use your public URL)", p)
    else:
        logger.info("BIN tool + sendout: http://%s:%s/", host, p)
    return t
