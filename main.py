import argparse
import asyncio
import http.server
import json
import os
import signal
import socketserver
import sys
import threading
import time
from typing import Any, Dict, List

import httpx
from telegram import Update
from telegram.error import Conflict

from child_bots import child_manager
from mother_admin import (
    SAMPLE_OTP_PREVIEW_ITEM,
    build_mother_application,
    configure_mother_commands,
    trigger_proactive_handover,
)
from shared_core import (
    ADMIN_USER_IDS,
    BOT_PROCESS_START_TIME,
    MOTHER_BOT_TOKEN,
    POLL_INTERVAL_SECONDS,
    SMS_FORMAT_META,
    STARTUP_TYPE,
    cache_sms_message,
    cleanup_old_otp_messages,
    extract_otp_code,
    format_otp_card,
    get_child_token,
    get_country_iso_display,
    get_effective_api_key,
    get_effective_base_url,
    get_forwarding_resumed_epoch,
    get_group_sms_format,
    get_inbox_sms_format,
    get_number_owner,
    get_otp_dashboard_stats,
    get_system_stats,
    get_target_group_chat_ids,
    gist_storage,
    init_db,
    is_group_forwarding_enabled,
    is_inbox_otp_enabled,
    is_message_duplicate,
    logger,
    mask_bot_token,
    parse_message_timestamp,
    preload_seen_otps_from_db,
    save_processed_otp_message,
    thirdwave_client,
)

STOP_EVENT = asyncio.Event()


# ==============================================================================
# OPTIONAL HEALTH ENDPOINT (PaaS / Cloud Keepalive)
# ==============================================================================
class _HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        payload = json.dumps({
            "status": "ok",
            "uptime_sec": int(time.time() - BOT_PROCESS_START_TIME),
            "otp_bot_running": child_manager.is_running("otp"),
            "number_bot_running": child_manager.is_running("number"),
        }).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:
        return


def start_health_server() -> None:
    port_str = os.getenv("PORT", "").strip()
    if not port_str.isdigit():
        return
    port = int(port_str)

    def _serve() -> None:
        try:
            with socketserver.TCPServer(("0.0.0.0", port), _HealthHandler) as httpd:
                httpd.serve_forever()
        except Exception as e:
            logger.warning(f"⚠️ Health server on port {port} stopped: {e}")

    t = threading.Thread(target=_serve, daemon=True, name="health-http")
    t.start()
    logger.info(f"🩺 Health server listening on port {port}")


# ==============================================================================
# SINGLE UNIFIED THIRDWAVE POLLER -> ROUTES TO BOTH CHILD BOTS
# ==============================================================================
async def _dispatch_single_item(item: Dict[str, Any], dest_groups: List[int]) -> None:
    mid = str(
        item.get("id")
        or item.get("msg_id")
        or item.get("message_id")
        or item.get("uuid")
        or f"gen_{int(time.time() * 1000)}"
    )
    if is_message_duplicate(mid, item):
        return

    raw_phone = str(item.get("number") or item.get("destinationNumber") or item.get("phone") or "").strip()
    raw_msg = str(
        item.get("sms")
        or item.get("messageBody")
        or item.get("message")
        or item.get("text")
        or item.get("body")
        or ""
    ).strip()
    country = get_country_iso_display(item)
    otp_code = extract_otp_code(raw_msg)

    item["_resolved_mid"] = mid
    item["id"] = mid
    if raw_msg:
        cache_sms_message(mid, raw_msg)

    raw_ts = str(
        item.get("received_at")
        or item.get("receivedAt")
        or item.get("messageTime")
        or item.get("createdAt")
        or item.get("created_at")
        or ""
    )
    msg_ts = parse_message_timestamp(raw_ts)
    resumed_at = get_forwarding_resumed_epoch()

    # Route 1: Public Group OTP Card via OTP Child Bot (masked phone + Group Format)
    group_deliver_ok = False
    if dest_groups and is_group_forwarding_enabled():
        if not resumed_at or not msg_ts or msg_ts >= (resumed_at - 10):
            ok_cnt, _ = await child_manager.deliver_group_otp(item, dest_groups)
            group_deliver_ok = ok_cnt > 0

    # Route 2: Private Number Inbox DM via Number Child Bot (unmasked phone + Inbox Format)
    inbox_deliver_ok = False
    owner_id = get_number_owner(raw_phone) if raw_phone else None
    if owner_id and is_inbox_otp_enabled():
        inbox_deliver_ok = await child_manager.deliver_inbox_otp(item, owner_id)

    status = "delivered" if (group_deliver_ok or inbox_deliver_ok) else "skipped"
    save_processed_otp_message(
        item=item,
        country=country,
        otp_code=otp_code,
        group_forwarded=group_deliver_ok,
        inbox_delivered=inbox_deliver_ok,
        user_id=owner_id or 0,
        status=status,
    )


async def run_startup_reconciliation() -> None:
    """Marks older backlog messages seen on fresh start; delivers recent handover window."""
    window_sec = 120 if STARTUP_TYPE == "handover" else 45
    cutoff = time.time() - window_sec
    msgs = await thirdwave_client.fetch_incoming_messages(limit=50)
    if not msgs:
        return

    dest_groups = get_target_group_chat_ids()
    for item in reversed(msgs):
        mid = str(item.get("id") or item.get("msg_id") or item.get("message_id") or item.get("uuid") or "")
        raw_ts = str(
            item.get("received_at")
            or item.get("receivedAt")
            or item.get("messageTime")
            or item.get("createdAt")
            or item.get("created_at")
            or ""
        )
        msg_ts = parse_message_timestamp(raw_ts)
        if msg_ts and msg_ts >= cutoff:
            await _dispatch_single_item(item, dest_groups)
        elif mid:
            is_message_duplicate(mid, item)

    logger.info(f"🔄 Startup reconciliation complete ({STARTUP_TYPE}, window={window_sec}s).")


async def unified_poller_loop() -> None:
    """Single shared poller serving both the OTP Bot and Number Bot."""
    logger.info(f"📡 Unified Thirdwave poller active (interval={POLL_INTERVAL_SECONDS}s)")
    while not STOP_EVENT.is_set():
        try:
            if is_group_forwarding_enabled() or is_inbox_otp_enabled():
                msgs = await thirdwave_client.fetch_incoming_messages(limit=50)
                if msgs:
                    dest_groups = get_target_group_chat_ids()
                    for item in reversed(msgs):
                        await _dispatch_single_item(item, dest_groups)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"⚠️ Unified poller loop warning: {e}")

        try:
            await asyncio.wait_for(STOP_EVENT.wait(), timeout=POLL_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass


async def maintenance_loop() -> None:
    """Periodic 28h OTP retention cleanup + Gist backup + GitHub proactive handover."""
    in_github = bool(os.getenv("GITHUB_ACTIONS"))
    handover_minutes = 318  # Safely before the 325m bash loop / 360m GitHub Actions hard cap
    last_cleanup = time.time()
    last_gist_sync = time.time()

    while not STOP_EVENT.is_set():
        now = time.time()
        if now - last_cleanup >= 1800:
            last_cleanup = now
            try:
                cleanup_old_otp_messages(max_age_hours=28)
            except Exception:
                pass

        if gist_storage.enabled and (
            (gist_storage._dirty and (now - last_gist_sync >= 60))
            or (now - last_gist_sync >= 1800)
        ):
            last_gist_sync = now
            try:
                await gist_storage.export_and_sync(immediate=False)
            except Exception:
                pass

        if in_github and (now - BOT_PROCESS_START_TIME) >= (handover_minutes * 60):
            logger.info("⏱️ Reached 318m session cap — running proactive zero-restart handover...")
            await trigger_proactive_handover(manual_restart=False)
            STOP_EVENT.set()
            break

        try:
            await asyncio.wait_for(STOP_EVENT.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            pass


# ==============================================================================
# DIAGNOSTICS (--test / --diag)
# ==============================================================================
def run_diagnostics(check_network: bool = True) -> bool:
    """Runs offline/online configuration, database, format, and token diagnostics."""
    print("========================================================")
    print("  MOTHER BOT - SYSTEM DIAGNOSTICS")
    print("========================================================")
    ok_all = True

    init_db()
    sys_stats = get_system_stats()
    otp_today, otp_28h = get_otp_dashboard_stats()
    print(f"[OK] SQLite DB ready (users={sys_stats['total_users']}, countries={sys_stats['total_countries']}, stock={sys_stats['total_available']}, otps_28h={otp_28h}, today={otp_today})")

    if MOTHER_BOT_TOKEN:
        print(f"[OK] MOTHER_BOT_TOKEN set ({mask_bot_token(MOTHER_BOT_TOKEN)})")
    else:
        print("[FAIL] MOTHER_BOT_TOKEN is missing! Set it in MOTHER_BOT/.env")
        ok_all = False

    if ADMIN_USER_IDS:
        print(f"[OK] ADMIN_USER_IDS: {ADMIN_USER_IDS}")
    else:
        print("[WARN] ADMIN_USER_IDS is empty! Set your Telegram user ID in .env")

    otp_tok = get_child_token("otp")
    num_tok = get_child_token("number")
    print(f"[INFO] OTP Child Bot token: {mask_bot_token(otp_tok) if otp_tok else 'Not linked yet (link via Mother Bot)'}")
    print(f"[INFO] Number Child Bot token: {mask_bot_token(num_tok) if num_tok else 'Not linked yet (link via Mother Bot)'}")
    print(f"[INFO] Group OTP: {'ON' if is_group_forwarding_enabled() else 'OFF'} | format={get_group_sms_format()}")
    print(f"[INFO] Inbox DM:  {'ON' if is_inbox_otp_enabled() else 'OFF'} | format={get_inbox_sms_format()}")

    # Verify all 8 card formats render cleanly for both Group (masked) & Inbox (unmasked)
    for fk in SMS_FORMAT_META:
        g_card, g_otp, _ = format_otp_card(SAMPLE_OTP_PREVIEW_ITEM, sms_format=fk, masked=True)
        i_card, i_otp, _ = format_otp_card(SAMPLE_OTP_PREVIEW_ITEM, sms_format=fk, masked=False)
        assert g_card and i_card and g_otp == "687-559" and i_otp == "687-559"
    print(f"[OK] Format engine verified across all {len(SMS_FORMAT_META)} Group & Inbox styles")

    if check_network:
        for role_lbl, tok in (("MOTHER_BOT", MOTHER_BOT_TOKEN), ("OTP_BOT", otp_tok), ("NUMBER_BOT", num_tok)):
            if not tok:
                continue
            try:
                r = httpx.get(f"https://api.telegram.org/bot{tok}/getMe", timeout=8.0)
                if r.status_code == 200 and r.json().get("ok"):
                    uname = r.json().get("result", {}).get("username", "")
                    print(f"[OK] Telegram API verified {role_lbl} (@{uname})")
                elif r.status_code == 401:
                    print(f"[FAIL] {role_lbl} token rejected by Telegram (HTTP 401 Unauthorized)")
                    ok_all = False
                else:
                    print(f"[WARN] Telegram API check for {role_lbl} returned HTTP {r.status_code}")
            except Exception as e:
                print(f"[WARN] Network unreachable for Telegram {role_lbl} check: {e}")

        api_key = get_effective_api_key()
        base_url = get_effective_base_url()
        if api_key:
            try:
                r = httpx.get(f"{base_url}/api/v1/traffic", params={"api_key": api_key, "limit": 3}, timeout=8.0)
                print(f"[{'OK' if r.is_success else 'WARN'}] Thirdwave API ({base_url}) HTTP {r.status_code}")
            except Exception as e:
                print(f"[WARN] Network unreachable for Thirdwave API ({base_url}): {e}")
        else:
            print("[WARN] THIRDWAVE_API_KEY is not configured yet")

    print("========================================================")
    print("RESULT:", "PASS" if ok_all else "ACTION NEEDED")
    return ok_all


# ==============================================================================
# MAIN LIFECYCLE (MOTHER BOT + CHILD BOTS IN ONE PROCESS)
# ==============================================================================
async def main() -> None:
    if not MOTHER_BOT_TOKEN:
        print(
            "\n❌ ERROR: MOTHER_BOT_TOKEN is not set!\n"
            "Set MOTHER_BOT_TOKEN in MOTHER_BOT/.env (or GitHub Actions Secrets) and re-run.\n",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        r = httpx.get(f"https://api.telegram.org/bot{MOTHER_BOT_TOKEN}/getMe", timeout=8.0)
        if r.status_code == 401:
            logger.critical("❌ MOTHER_BOT_TOKEN is INVALID or REVOKED (HTTP 401 Unauthorized). Update it in @BotFather.")
            sys.exit(1)
    except SystemExit:
        raise
    except Exception:
        pass

    init_db()
    if gist_storage.enabled:
        await gist_storage.ensure_gist()
        await gist_storage.restore_from_gist()
    thirdwave_client.update_credentials(get_effective_base_url(), get_effective_api_key())
    preload_seen_otps_from_db()
    start_health_server()

    mother_app = build_mother_application(MOTHER_BOT_TOKEN)
    await mother_app.initialize()
    me = await mother_app.bot.get_me()
    await configure_mother_commands(mother_app.bot)
    await mother_app.start()
    if mother_app.updater:
        for attempt in range(1, 6):
            try:
                await mother_app.updater.start_polling(
                    drop_pending_updates=True,
                    allowed_updates=Update.ALL_TYPES,
                )
                break
            except Conflict:
                if attempt == 5:
                    raise
                logger.warning(
                    f"⚠️ Mother bot Conflict on polling startup (attempt {attempt}/5) — retrying in 4s..."
                )
                await asyncio.sleep(4.0)
    logger.info(f"👑 Mother Bot online as @{me.username} (id={me.id})")

    # Auto-start any linked & enabled child bots (OTP Bot + Number Bot)
    await child_manager.start_all_saved()

    # Reconcile startup OTP window once child bots are up
    await run_startup_reconciliation()

    poller_task = asyncio.create_task(unified_poller_loop(), name="unified-poller")
    maint_task = asyncio.create_task(maintenance_loop(), name="maintenance-loop")

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, STOP_EVENT.set)
        except NotImplementedError:
            # Windows ProactorEventLoop fallback
            signal.signal(sig, lambda *_: STOP_EVENT.set())

    try:
        await STOP_EVENT.wait()
    finally:
        logger.info("🛑 Shutting down Mother Bot & linked child bots cleanly...")
        for t in (poller_task, maint_task):
            t.cancel()
        await asyncio.gather(poller_task, maint_task, return_exceptions=True)

        await child_manager.stop_all()
        await thirdwave_client.close()
        try:
            if mother_app.updater and mother_app.updater.running:
                await mother_app.updater.stop()
            if mother_app.running:
                await mother_app.stop()
            await mother_app.shutdown()
        except Exception:
            pass

        if gist_storage.enabled:
            try:
                await gist_storage.export_and_sync(immediate=True)
            except Exception:
                pass
        logger.info("✅ Clean shutdown complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mother Bot - Unified OTP & Number Controller")
    parser.add_argument("--test", "--diag", action="store_true", help="Run system diagnostics and exit")
    parser.add_argument("--offline", action="store_true", help="Skip live network checks during --test")
    args = parser.parse_args()

    if args.test:
        passed = run_diagnostics(check_network=not args.offline)
        sys.exit(0 if passed else 1)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except SystemExit:
        raise
    except Exception as exc:
        logger.exception(f"Unhandled runtime exception in Mother Bot: {exc}")
        sys.exit(2)

