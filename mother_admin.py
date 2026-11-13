import asyncio
import html
import os
import re
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx
from telegram import (
    Bot,
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    MenuButtonCommands,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.constants import ChatType, ParseMode
from telegram.error import Conflict, Forbidden, NetworkError, TelegramError, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from child_bots import child_manager, render_country_broadcast_panel
from shared_core import (
    ADMIN_USER_IDS,
    BOT_PROCESS_START_TIME,
    POLL_INTERVAL_SECONDS,
    SMS_FORMAT_META,
    STOCKS_DIR,
    ClosingConnection,
    add_admin,
    add_dynamic_linked_group,
    add_numbers_to_country,
    clear_child_token,
    clear_dynamic_linked_groups,
    delete_country_and_stock,
    delete_user,
    format_country_name,
    format_otp_card,
    get_admin_fixed_quantity,
    get_all_admin_details,
    get_all_countries_with_stock,
    get_all_user_ids,
    get_all_users_detailed,
    get_bot_display_name,
    get_broadcast_messages,
    get_country_by_id,
    get_current_commit,
    get_custom_link_buttons,
    get_effective_api_key,
    get_effective_base_url,
    get_group_sms_format,
    get_inbox_sms_format,
    get_main_db,
    get_or_create_country,
    get_otp_dashboard_stats,
    get_recent_broadcasts,
    get_system_stats,
    get_target_group_chat_ids,
    get_user_details,
    gist_storage,
    is_admin,
    is_group_forwarding_enabled,
    is_inbox_otp_enabled,
    is_user_banned,
    logger,
    mark_broadcast_deleted,
    normalize_button_url,
    build_otp_keyboard,
    record_broadcast_history,
    record_broadcast_message,
    remove_admin,
    remove_dynamic_linked_group,
    remove_numbers_from_country,
    sanitize_phone_number,
    save_custom_link_buttons,
    set_admin_fixed_quantity,
    set_bot_display_name,
    set_bot_setting,
    set_child_enabled,
    set_child_token,
    set_group_forwarding_enabled,
    set_group_sms_format,
    set_inbox_otp_enabled,
    set_inbox_sms_format,
    set_user_ban_status,
    set_user_secret_access,
    thirdwave_client,
    update_country_button_name,
    update_country_service_name,
    is_child_enabled,
)

# Ephemeral per-admin wizard state (keyed by admin user_id)
ADMIN_STATES: Dict[int, Dict[str, Any]] = {}

# Clean 6-button Mother Bot Admin Keyboard (only ever shown inside Mother Bot)
MOTHER_REPLY_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📊 Dashboard"), KeyboardButton("🤖 Linked Bots")],
        [KeyboardButton("📩 Format & Inbox"), KeyboardButton("📦 Stock & Users")],
        [KeyboardButton("👥 Groups & Links"), KeyboardButton("⚙️ System & API")],
    ],
    resize_keyboard=True,
    is_persistent=True,
)

# Sample payload used by live Format Preview cards
SAMPLE_OTP_PREVIEW_ITEM: Dict[str, Any] = {
    "id": "preview_001",
    "number": "9779847015546",
    "sender": "WhatsApp",
    "country": "NP",
    "message": "<#> Your WhatsApp code: 687-559\nDon't share this code with others\n4sgLq1p5sV6",
    "created_at": "2026-11-13 12:00:00",
}


# ==============================================================================
# PROACTIVE ZERO-RESTART GITHUB HANDOVER
# ==============================================================================
async def trigger_proactive_handover(manual_restart: bool = False) -> bool:
    """
    Flushes SQLite WALs, pushes Gist snapshot, and triggers the next
    GitHub Actions workflow_dispatch run before shutting down this runner.
    """
    repo = os.getenv("GITHUB_REPOSITORY", "").strip()
    if not repo:
        logger.info("ℹ️ Proactive handover skipped (no GITHUB_REPOSITORY env var).")
        return False

    tokens: List[str] = []
    for t in (
        os.getenv("PAT", "").strip(),
        os.getenv("GIST_TOKEN", "").strip(),
        os.getenv("GH_TOKEN", "").strip(),
        os.getenv("GITHUB_TOKEN", "").strip(),
    ):
        if t and t not in tokens and t.lower() != "true":
            tokens.append(t)
    if not tokens:
        logger.info("ℹ️ Proactive handover skipped (no GitHub auth token).")
        return False

    try:
        with get_main_db() as conn:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        if os.path.exists(STOCKS_DIR):
            for fname in os.listdir(STOCKS_DIR):
                if fname.endswith(".db"):
                    try:
                        with ClosingConnection(sqlite3.connect(os.path.join(STOCKS_DIR, fname), timeout=30.0)) as cc:
                            cc.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                    except Exception:
                        pass
    except Exception:
        pass

    if gist_storage.enabled:
        try:
            await gist_storage.export_and_sync(is_handover=True)
        except Exception:
            pass

    url = f"https://api.github.com/repos/{repo}/actions/workflows/run_bot.yml/dispatches"
    payload = {
        "ref": "main",
        "inputs": {
            "is_handover": "true",
            "is_manual_restart": "true" if manual_restart else "false",
        },
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            for tok in tokens:
                res = await client.post(
                    url,
                    headers={
                        "Authorization": f"Bearer {tok}",
                        "Accept": "application/vnd.github+json",
                        "X-GitHub-Api-Version": "2022-11-28",
                        "User-Agent": "ThirdwaveMotherBot",
                    },
                    json=payload,
                )
                if res.status_code in (200, 201, 204):
                    try:
                        with open(".handover_dispatched", "w", encoding="utf-8") as f:
                            f.write(str(time.time()))
                    except Exception:
                        pass
                    logger.info("🚀 Proactive handover dispatched successfully.")
                    return True
                logger.warning(f"⚠️ Handover token ({tok[:6]}...) returned HTTP {res.status_code}")
    except Exception as e:
        logger.warning(f"⚠️ Handover error: {e}")
    return False


# ==============================================================================
# SMALL UI & FORMATTING HELPERS
# ==============================================================================
async def _safe_edit(query: CallbackQuery, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    try:
        await query.edit_message_text(
            text=text[:4090],
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
    except TelegramError as e:
        if "message is not modified" not in str(e).lower():
            raise


def _format_uptime() -> str:
    elapsed = int(max(0, time.time() - BOT_PROCESS_START_TIME))
    h, rem = divmod(elapsed, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def _fmt_meta(fmt_key: str) -> Tuple[str, str, str]:
    """Returns (button_label, display_name, description) for a format key."""
    entry = SMS_FORMAT_META.get(fmt_key)
    if entry:
        return entry[0], entry[1], entry[2]
    return fmt_key, fmt_key, ""


def _child_badge(st: Dict[str, Any]) -> str:
    if not st["has_token"]:
        return "🔴 Not Linked"
    if not st["enabled"]:
        return "🟡 Paused"
    if st["running"]:
        uname = f"@{st['username']}" if st["username"] else st["display_name"]
        return f"🟢 Live ({html.escape(uname)})"
    if st["last_error"]:
        return "🔴 Error"
    return "🟠 Stopped"


# ==============================================================================
# UNIFIED MOTHER PANEL VIEW BUILDERS (Zero Duplication)
# ==============================================================================
def build_dashboard_view() -> Tuple[str, InlineKeyboardMarkup]:
    otp_st = child_manager.get_status("otp")
    num_st = child_manager.get_status("number")
    otp_today, otp_total_28h = get_otp_dashboard_stats()
    sys_stats = get_system_stats()

    _, grp_fmt, _ = _fmt_meta(get_group_sms_format())
    _, inb_fmt, _ = _fmt_meta(get_inbox_sms_format())
    grp_state = "🟢 ON" if is_group_forwarding_enabled() else "🔴 OFF"
    inb_state = "🟢 ON" if is_inbox_otp_enabled() else "🔴 OFF"
    mother_name = html.escape(get_bot_display_name("mother"))

    text = (
        f"👑 <b>{mother_name} — Unified Control</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 <b>Linked Child Bots:</b>\n"
        f"• <b>OTP Bot:</b> {_child_badge(otp_st)}\n"
        f"• <b>Number Bot:</b> {_child_badge(num_st)}\n\n"
        f"📩 <b>Delivery &amp; Active Formats:</b>\n"
        f"• <b>Group OTP:</b> {grp_state} · <code>{html.escape(grp_fmt)}</code>\n"
        f"• <b>Inbox DM:</b> {inb_state} · <code>{html.escape(inb_fmt)}</code>\n\n"
        f"📊 <b>Live Telemetry:</b>\n"
        f"• <b>OTPs Today / 28h Total:</b> <code>{otp_today}</code> / <code>{otp_total_28h}</code>\n"
        f"• <b>Linked Groups:</b> <code>{len(get_target_group_chat_ids())}</code>\n"
        f"• <b>Users / Countries:</b> <code>{sys_stats['total_users']}</code> / <code>{sys_stats['total_countries']}</code>\n"
        f"• <b>Available Stock:</b> <code>{sys_stats['total_available']}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱️ <b>Uptime:</b> <code>{_format_uptime()}</code> · <code>{html.escape(get_current_commit())}</code>"
    )
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔄 Refresh", callback_data="m_dash"),
            InlineKeyboardButton("🤖 Linked Bots", callback_data="m_bots"),
        ],
        [
            InlineKeyboardButton("📩 Format & Inbox", callback_data="m_fmt"),
            InlineKeyboardButton("📦 Stock & Users", callback_data="m_stock"),
        ],
        [
            InlineKeyboardButton("👥 Groups & Links", callback_data="m_groups"),
            InlineKeyboardButton("⚙️ System & API", callback_data="m_sys"),
        ],
    ])
    return text, kb


def build_linked_bots_view() -> Tuple[str, InlineKeyboardMarkup]:
    otp_st = child_manager.get_status("otp")
    num_st = child_manager.get_status("number")
    mother_name = html.escape(get_bot_display_name("mother"))

    def _slot_block(title: str, st: Dict[str, Any]) -> str:
        err = f"\n   ⚠️ <i>{html.escape(st['last_error'][:90])}</i>" if st["last_error"] else ""
        uname = f"@{st['username']}" if st["username"] else "—"
        return (
            f"{title}\n"
            f"   • <b>Status:</b> {_child_badge(st)}\n"
            f"   • <b>Label / User:</b> {html.escape(st['display_name'])} (<code>{html.escape(uname)}</code>)\n"
            f"   • <b>Token:</b> <code>{html.escape(st['masked_token'])}</code>"
            f"{err}"
        )

    text = (
        f"🤖 <b>Linked Bots Control</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👑 <b>Mother Controller:</b> <b>{mother_name}</b>\n"
        f"<i>Child bots never show admin menus — control them exclusively here.</i>\n\n"
        f"{_slot_block('📩 <b>1. OTP Forwarder Bot</b> (Groups)', otp_st)}\n\n"
        f"{_slot_block('📲 <b>2. Number Distribution Bot</b> (Users &amp; Inbox)', num_st)}"
    )

    def _role_rows(role: str, label: str, st: Dict[str, Any]) -> List[List[InlineKeyboardButton]]:
        toggle_label = "⏸️ Pause" if st["enabled"] else "▶️ Start"
        rows = [
            [
                InlineKeyboardButton(f"🔑 Link {label} Token", callback_data=f"bot_link_{role}"),
                InlineKeyboardButton(toggle_label, callback_data=f"bot_toggle_{role}"),
            ]
        ]
        if st["has_token"]:
            rows.append([
                InlineKeyboardButton("🔄 Restart", callback_data=f"bot_restart_{role}"),
                InlineKeyboardButton("✏️ Rename", callback_data=f"bot_rename_{role}"),
                InlineKeyboardButton("🗑️ Unlink", callback_data=f"bot_unlink_{role}"),
            ])
        return rows

    rows: List[List[InlineKeyboardButton]] = []
    rows.extend(_role_rows("otp", "OTP Bot", otp_st))
    rows.extend(_role_rows("number", "Number Bot", num_st))
    rows.append([
        InlineKeyboardButton("✏️ Rename Mother Bot", callback_data="bot_rename_mother"),
        InlineKeyboardButton("🔙 Dashboard", callback_data="m_dash"),
    ])
    return text, InlineKeyboardMarkup(rows)


def build_format_inbox_view(target: str = "group") -> Tuple[str, InlineKeyboardMarkup]:
    """Unified single selector for independent Group OTP vs Inbox DM formats + ON/OFF toggles."""
    target = "inbox" if target == "inbox" else "group"
    grp_fmt = get_group_sms_format()
    inb_fmt = get_inbox_sms_format()
    grp_on = is_group_forwarding_enabled()
    inb_on = is_inbox_otp_enabled()

    active_fmt = inb_fmt if target == "inbox" else grp_fmt
    _, grp_fmt_name, _ = _fmt_meta(grp_fmt)
    _, inb_fmt_name, _ = _fmt_meta(inb_fmt)
    _, active_name, active_desc = _fmt_meta(active_fmt)
    target_title = "📬 Inbox DM Format (Number Bot)" if target == "inbox" else "📩 Group OTP Format (OTP Bot)"
    mask_note = "Full Phone Unmasked (ZWNJ-protected)" if target == "inbox" else "Phone Masked (+977****123)"

    text = (
        f"📩 <b>SMS Format &amp; Inbox Routing</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ <b>Group OTP (OTP Bot):</b> {'🟢 ON' if grp_on else '🔴 OFF'} · <code>{html.escape(grp_fmt_name)}</code>\n"
        f"🔔 <b>Inbox DM (Number Bot):</b> {'🟢 ON' if inb_on else '🔴 OFF'} · <code>{html.escape(inb_fmt_name)}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>Editing:</b> {target_title}\n"
        f"🛡️ <b>Privacy:</b> <code>{mask_note}</code>\n"
        f"✨ <b>Active Style:</b> <code>{html.escape(active_name)}</code>\n"
        f"<i>{html.escape(active_desc)}</i>"
    )

    grp_tab = "• [📩 Group OTP Format]" if target == "group" else "📩 Group OTP Format"
    inb_tab = "• [📬 Inbox DM Format]" if target == "inbox" else "📬 Inbox DM Format"

    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(f"⚡ Group OTP: {'ON ✅' if grp_on else 'OFF ❌'}", callback_data="tog_group_otp"),
            InlineKeyboardButton(f"🔔 Inbox DM: {'ON ✅' if inb_on else 'OFF ❌'}", callback_data="tog_inbox_otp"),
        ],
        [
            InlineKeyboardButton(grp_tab, callback_data="fmt_tab_group"),
            InlineKeyboardButton(inb_tab, callback_data="fmt_tab_inbox"),
        ],
    ]

    format_keys = list(SMS_FORMAT_META.keys())
    for i in range(0, len(format_keys), 2):
        pair_row: List[InlineKeyboardButton] = []
        for fk in format_keys[i:i + 2]:
            btn_lbl, _, _ = _fmt_meta(fk)
            prefix = "✅ " if fk == active_fmt else ""
            pair_row.append(
                InlineKeyboardButton(
                    f"{prefix}{btn_lbl}",
                    callback_data=f"fmt_set_{target}_{fk}",
                )
            )
        rows.append(pair_row)

    rows.append([
        InlineKeyboardButton("👁️ Preview Card", callback_data=f"fmt_prev_{target}"),
        InlineKeyboardButton("🔙 Dashboard", callback_data="m_dash"),
    ])
    return text, InlineKeyboardMarkup(rows)


def build_stock_users_hub_view() -> Tuple[str, InlineKeyboardMarkup]:
    stats = get_system_stats()
    fixed_qty = get_admin_fixed_quantity()
    qty_desc = f"Fixed ({fixed_qty} / request)" if fixed_qty is not None else "Default (10 / request)"

    text = (
        f"📦 <b>Stock, Users &amp; Broadcasts</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🌍 <b>Countries:</b> <code>{stats['total_countries']}</code>\n"
        f"📊 <b>Available Stock:</b> <code>{stats['total_available']}</code>\n"
        f"🔒 <b>Secret Pool:</b> <code>{stats['total_sec_available']}</code>\n"
        f"✅ <b>Delivered Numbers:</b> <code>{stats['total_consumed']}</code>\n"
        f"👥 <b>Registered Users:</b> <code>{stats['total_users']}</code>\n"
        f"🔢 <b>Quantity Policy:</b> <code>{qty_desc}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Note: Broadcasts and DMs are sent from your linked Number Bot.</i>"
    )
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📤 Upload / Remove .txt", callback_data="adm_upload_prompt"),
            InlineKeyboardButton("📁 Stock Pools", callback_data="adm_stock_pools"),
        ],
        [
            InlineKeyboardButton("👥 Manage Users", callback_data="adm_users_0"),
            InlineKeyboardButton("🔢 Qty Policy", callback_data="adm_qty_menu"),
        ],
        [
            InlineKeyboardButton("📢 Custom Broadcast", callback_data="adm_bcast_custom"),
            InlineKeyboardButton("🗑️ Broadcast History", callback_data="adm_bcast_hist"),
        ],
        [InlineKeyboardButton("🔙 Dashboard", callback_data="m_dash")],
    ])
    return text, kb


def build_groups_links_view() -> Tuple[str, InlineKeyboardMarkup]:
    groups = get_target_group_chat_ids()
    buttons = get_custom_link_buttons()

    grp_lines = "\n".join(f"• <code>{html.escape(str(g))}</code>" for g in groups) if groups else "• <i>No target groups linked yet</i>"
    btn_lines = "\n".join(
        f"• <b>{html.escape(b['text'])}</b> → <code>{html.escape(b['url'][:45])}</code>"
        for b in buttons
    ) if buttons else "• <i>No custom link buttons</i>"

    text = (
        f"👥 <b>Groups &amp; Custom Link Buttons</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📣 <b>OTP Target Groups ({len(groups)}):</b>\n{grp_lines}\n\n"
        f"🔗 <b>Custom Link Buttons ({len(buttons)}):</b>\n{btn_lines}"
    )
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("➕ Add Group ID", callback_data="grp_add"),
            InlineKeyboardButton("🗑️ Remove Group", callback_data="grp_rm_menu"),
        ],
        [InlineKeyboardButton("🧪 Test Send via OTP Bot", callback_data="grp_test_send")],
        [
            InlineKeyboardButton("➕ Add Link Button", callback_data="btn_add"),
            InlineKeyboardButton("✏️ Manage Buttons", callback_data="btn_manage"),
        ],
        [InlineKeyboardButton("🔙 Dashboard", callback_data="m_dash")],
    ])
    return text, kb


def build_system_api_view() -> Tuple[str, InlineKeyboardMarkup]:
    base_url = get_effective_base_url() or "Not configured"
    api_key = get_effective_api_key()
    masked_key = f"{api_key[:4]}••••{api_key[-4:]}" if len(api_key) >= 10 else ("Set" if api_key else "Not set")
    admins = get_all_admin_details()
    gist_state = "🟢 Connected" if gist_storage.enabled else "🟡 Local Only (no Gist env)"
    gh_state = "🟢 Ready" if os.getenv("GITHUB_REPOSITORY") else "🟡 Local Mode"

    text = (
        f"⚙️ <b>System, API &amp; Mother Admins</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🌐 <b>Thirdwave URL:</b> <code>{html.escape(base_url)}</code>\n"
        f"🔑 <b>Thirdwave Key:</b> <code>{html.escape(masked_key)}</code>\n"
        f"⏱️ <b>Poll Interval:</b> <code>{POLL_INTERVAL_SECONDS}s</code>\n"
        f"☁️ <b>Gist Cloud Backup:</b> {gist_state}\n"
        f"🚀 <b>GitHub Handover:</b> {gh_state}\n"
        f"👑 <b>Mother Admins:</b> <code>{len(admins)}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>API polling runs once centrally and feeds both child bots.</i>"
    )
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🌐 Set API URL", callback_data="sys_set_url"),
            InlineKeyboardButton("🔑 Set API Key", callback_data="sys_set_key"),
        ],
        [
            InlineKeyboardButton("🩺 Test Thirdwave API", callback_data="sys_test_api"),
            InlineKeyboardButton("👑 Mother Admins", callback_data="sys_admins"),
        ],
        [
            InlineKeyboardButton("☁️ Sync Gist Now", callback_data="sys_sync_gist"),
            InlineKeyboardButton("🚀 Restart / Handover", callback_data="sys_handover"),
        ],
        [InlineKeyboardButton("🔙 Dashboard", callback_data="m_dash")],
    ])
    return text, kb


# ==============================================================================
# MOTHER BOT APPLICATION BUILDER & AUTHORIZATION
# ==============================================================================
def build_mother_application(token: str) -> Application:
    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", mother_start_command))
    app.add_handler(CommandHandler("status", mother_status_command))
    app.add_handler(CommandHandler("cancel", mother_cancel_command))
    app.add_handler(CallbackQueryHandler(mother_callback_handler))
    app.add_handler(MessageHandler(filters.Document.ALL & filters.ChatType.PRIVATE, mother_document_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, mother_text_handler))
    app.add_error_handler(mother_error_handler)
    return app


async def configure_mother_commands(bot: Bot) -> None:
    try:
        cmds = [
            BotCommand("start", "Open Unified Mother Admin Dashboard"),
            BotCommand("status", "Check Mother & Child Bot status"),
            BotCommand("cancel", "Cancel active admin prompt"),
        ]
        await bot.set_my_commands(cmds, scope=BotCommandScopeAllPrivateChats())
        await bot.delete_my_commands(scope=BotCommandScopeAllGroupChats())
        await bot.set_chat_menu_button(menu_button=MenuButtonCommands())
    except Exception:
        pass


async def _require_mother_admin(update: Update) -> Optional[Any]:
    user = update.effective_user
    if not user or not is_admin(user.id):
        msg = "🔒 <b>Mother Bot — Private Admin Control</b>\nAccess denied."
        if update.callback_query:
            await update.callback_query.answer("🔒 Access denied.", show_alert=True)
        elif update.effective_message and update.effective_chat and update.effective_chat.type == ChatType.PRIVATE:
            await update.effective_message.reply_text(msg, parse_mode=ParseMode.HTML)
        return None
    return user


async def mother_start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or update.effective_chat.type != ChatType.PRIVATE:
        return
    user = await _require_mother_admin(update)
    if not user:
        return
    ADMIN_STATES.pop(user.id, None)
    await update.effective_message.reply_text(
        "👑 <b>Mother Control Center Ready</b>\nUse the keyboard below to manage your linked OTP & Number bots.",
        parse_mode=ParseMode.HTML,
        reply_markup=MOTHER_REPLY_KEYBOARD,
    )
    text, kb = build_dashboard_view()
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


async def mother_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or update.effective_chat.type != ChatType.PRIVATE:
        return
    user = await _require_mother_admin(update)
    if not user:
        return
    text, kb = build_dashboard_view()
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


async def mother_cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or update.effective_chat.type != ChatType.PRIVATE:
        return
    user = await _require_mother_admin(update)
    if not user:
        return
    ADMIN_STATES.pop(user.id, None)
    await update.effective_message.reply_text("✅ Cancelled active action.", reply_markup=MOTHER_REPLY_KEYBOARD)


# ==============================================================================
# SUBMENU VIEW HELPERS (Countries, Users, Broadcasts, Groups, Admins)
# ==============================================================================
def _build_stock_pools_view() -> Tuple[str, InlineKeyboardMarkup]:
    countries = get_all_countries_with_stock(only_active=False, include_used=True)
    if not countries:
        text = "📁 <b>Stock Pools</b>\n━━━━━━━━━━━━━━━━━━━━\n<i>No countries created yet. Upload a .txt stock file to start.</i>"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 Upload .txt Stock", callback_data="adm_upload_prompt")],
            [InlineKeyboardButton("🔙 Stock & Users", callback_data="m_stock")],
        ])
        return text, kb

    rows: List[List[InlineKeyboardButton]] = []
    lines: List[str] = []
    for c in countries[:25]:
        cname = format_country_name(c["name"])
        lines.append(
            f"• <b>{html.escape(cname)}</b> — Std: <code>{c['available_std']}</code> | Sec: <code>{c['available_sec']}</code> | Used: <code>{c['total_used']}</code>"
        )
        rows.append([
            InlineKeyboardButton(
                f"{cname} (S:{c['available_std']} | 🔒:{c['available_sec']})",
                callback_data=f"pool_c_{c['id']}",
            )
        ])
    rows.append([
        InlineKeyboardButton("📤 Upload .txt", callback_data="adm_upload_prompt"),
        InlineKeyboardButton("🔙 Stock & Users", callback_data="m_stock"),
    ])
    text = f"📁 <b>Country Stock Pools ({len(countries)})</b>\n━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines)
    return text, InlineKeyboardMarkup(rows)


def _build_country_detail_view(cid: int) -> Tuple[str, InlineKeyboardMarkup]:
    all_c = get_all_countries_with_stock(only_active=False, include_used=True)
    c = next((x for x in all_c if x["id"] == cid), None)
    if not c:
        return "⚠️ <b>Country not found.</b>", InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Pools", callback_data="adm_stock_pools")]])

    cname = format_country_name(c["name"])
    btn_name = c.get("button_name") or cname
    svc_name = c.get("service_name") or "All Services"
    text = (
        f"🌍 <b>{html.escape(cname)}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🏷️ <b>Button Label:</b> <code>{html.escape(btn_name)}</code>\n"
        f"🧩 <b>Service Tag:</b> <code>{html.escape(svc_name)}</code>\n"
        f"👥 <b>Standard Available:</b> <code>{c['available_std']}</code>\n"
        f"🔒 <b>Secret Available:</b> <code>{c['available_sec']}</code>\n"
        f"✅ <b>Delivered (Used):</b> <code>{c['total_used']}</code>"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Broadcast Stock via Number Bot", callback_data=f"adm_bcast_c_{cid}")],
        [
            InlineKeyboardButton("✏️ Rename Button", callback_data=f"pool_ren_{cid}"),
            InlineKeyboardButton("🧩 Set Service Tag", callback_data=f"pool_svc_{cid}"),
        ],
        [InlineKeyboardButton("🗑️ Delete Country & Stock", callback_data=f"pool_del_ask_{cid}")],
        [InlineKeyboardButton("🔙 Back to Pools", callback_data="adm_stock_pools")],
    ])
    return text, kb


def _build_users_list_view(page: int = 0) -> Tuple[str, InlineKeyboardMarkup]:
    per_page = 8
    users, total = get_all_users_detailed(limit=per_page, offset=page * per_page)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, total_pages - 1))

    rows: List[List[InlineKeyboardButton]] = []
    for u in users:
        badges = []
        if u.get("has_secret_access"):
            badges.append("🔒")
        if u.get("is_banned"):
            badges.append("🚫")
        badge_str = "".join(badges) + (" " if badges else "")
        label = f"{badge_str}{u.get('first_name') or u.get('username') or u['user_id']} ({u.get('numbers_consumed', 0)})"
        rows.append([InlineKeyboardButton(label[:55], callback_data=f"usr_v_{u['user_id']}_{page}")])

    nav: List[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"adm_users_{page - 1}"))
    nav.append(InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"adm_users_{page + 1}"))
    if nav:
        rows.append(nav)

    rows.append([
        InlineKeyboardButton("🔍 Find User by ID", callback_data="usr_search"),
        InlineKeyboardButton("🔙 Stock & Users", callback_data="m_stock"),
    ])
    text = f"👥 <b>Registered Number Bot Users ({total})</b>\n━━━━━━━━━━━━━━━━━━━━\n<i>Tap any user to manage Secret Pool access, bans, or DM.</i>"
    return text, InlineKeyboardMarkup(rows)


def _build_user_detail_view(uid: int, back_page: int = 0) -> Tuple[str, InlineKeyboardMarkup]:
    d = get_user_details(uid)
    if not d:
        return "⚠️ <b>User not found.</b>", InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"adm_users_{back_page}")]])

    sec = bool(d.get("has_secret_access"))
    ban = bool(d.get("is_banned"))
    uname = f"@{d['username']}" if d.get("username") else "—"
    text = (
        f"👤 <b>User Details</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 <b>ID:</b> <code>{uid}</code>\n"
        f"📛 <b>Name:</b> {html.escape(d.get('first_name') or '—')} (<code>{html.escape(uname)}</code>)\n"
        f"📦 <b>Claimed:</b> <code>{d.get('numbers_consumed', 0)}</code>\n"
        f"🔒 <b>Secret Pool:</b> {'🔓 Allowed' if sec else '🔒 Locked'}\n"
        f"🛡️ <b>Account Status:</b> {'🚫 BANNED' if ban else '✅ Active'}"
    )
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔓 Revoke Secret" if sec else "🔒 Grant Secret", callback_data=f"usr_sec_{uid}_{0 if sec else 1}_{back_page}"),
            InlineKeyboardButton("✅ Unban" if ban else "🚫 Ban User", callback_data=f"usr_ban_{uid}_{0 if ban else 1}_{back_page}"),
        ],
        [
            InlineKeyboardButton("✉️ DM via Number Bot", callback_data=f"usr_dm_{uid}_{back_page}"),
            InlineKeyboardButton("🗑️ Delete User", callback_data=f"usr_del_{uid}_{back_page}"),
        ],
        [InlineKeyboardButton("🔙 Back to Users", callback_data=f"adm_users_{back_page}")],
    ])
    return text, kb


def _build_admins_view() -> Tuple[str, InlineKeyboardMarkup]:
    admins = get_all_admin_details()
    lines: List[str] = []
    rows: List[List[InlineKeyboardButton]] = []
    for a in admins:
        aid = a["user_id"]
        is_root = bool(a.get("is_super"))
        role_tag = "ROOT" if is_root else "ADMIN"
        name = a.get("first_name") or a.get("username") or str(aid)
        lines.append(f"• <code>{aid}</code> — <b>{html.escape(name)}</b> [<code>{role_tag}</code>]")
        if not is_root:
            rows.append([InlineKeyboardButton(f"🗑️ Remove {aid}", callback_data=f"sys_adm_rm_{aid}")])

    rows.append([
        InlineKeyboardButton("➕ Add Mother Admin", callback_data="sys_adm_add"),
        InlineKeyboardButton("🔙 System & API", callback_data="m_sys"),
    ])
    text = f"👑 <b>Mother Bot Administrators ({len(admins)})</b>\n━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines)
    return text, InlineKeyboardMarkup(rows)


async def _send_broadcast_via_number_bot(
    reply_query: CallbackQuery,
    admin_id: int,
    country_id: Optional[int],
    custom_text: str,
    btn_label: Optional[str] = None,
    c_name: str = "Announcement",
    qty: int = 0,
) -> None:
    number_bot = child_manager.get_bot("number")
    if not number_bot:
        await _safe_edit(
            reply_query,
            "⚠️ <b>Number Bot is not running!</b>\nLink and start the Number Bot first so broadcasts are sent through it.",
            InlineKeyboardMarkup([[InlineKeyboardButton("🤖 Go to Linked Bots", callback_data="m_bots")]]),
        )
        return

    all_uids = get_all_user_ids()
    bcast_id = f"bc_{country_id or 'custom'}_{int(time.time())}"
    user_kb = (
        InlineKeyboardMarkup([[InlineKeyboardButton(btn_label or f"📥 Get {c_name} Number", callback_data=f"bcast_getnum_{country_id}")]])
        if country_id
        else None
    )

    await _safe_edit(
        reply_query,
        f"⏳ <b>Broadcasting via Number Bot to {len(all_uids)} users...</b>",
        None,
    )

    sent = 0
    failed = 0
    for uid in all_uids:
        if is_user_banned(uid):
            continue
        try:
            msg = await number_bot.send_message(
                chat_id=uid,
                text=custom_text,
                parse_mode=ParseMode.HTML,
                reply_markup=user_kb,
                disable_web_page_preview=True,
            )
            record_broadcast_message(bcast_id, uid, msg.message_id)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.04)

    record_broadcast_history(
        broadcast_id=bcast_id,
        admin_id=admin_id,
        target_type="country" if country_id else "all",
        message_text=custom_text,
        total_sent=sent,
        total_failed=failed,
        country_id=country_id or 0,
        button_label=btn_label or "",
        service_name=c_name,
        quantity=qty,
    )
    if gist_storage.enabled:
        asyncio.create_task(gist_storage.export_and_sync(immediate=True))

    await _safe_edit(
        reply_query,
        f"✅ <b>Broadcast Complete!</b>\n━━━━━━━━━━━━━━━━━━━━\n📤 <b>Delivered:</b> <code>{sent}</code>\n⚠️ <b>Failed:</b> <code>{failed}</code>",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑️ Recall History", callback_data="adm_bcast_hist")],
            [InlineKeyboardButton("🔙 Stock & Users", callback_data="m_stock")],
        ]),
    )


# ==============================================================================
# DOCUMENT HANDLER (.txt Stock Upload in Mother Bot)
# ==============================================================================
async def mother_document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await _require_mother_admin(update)
    if not user or not update.effective_message or not update.effective_message.document:
        return
    doc = update.effective_message.document
    filename = doc.file_name or "numbers.txt"
    if not filename.lower().endswith(".txt"):
        await update.effective_message.reply_text("⚠️ Please upload a valid <code>.txt</code> file containing phone numbers.", parse_mode=ParseMode.HTML)
        return

    tg_file = await context.bot.get_file(doc.file_id)
    raw_bytes = await tg_file.download_as_bytearray()
    content = raw_bytes.decode("utf-8", errors="ignore")

    parsed: List[str] = []
    seen = set()
    for line in re.split(r"[\r\n,;]+", content):
        num = sanitize_phone_number(line)
        if num and num not in seen:
            seen.add(num)
            parsed.append(num)

    if not parsed:
        await update.effective_message.reply_text("⚠️ No valid phone numbers found in that file.", parse_mode=ParseMode.HTML)
        return

    ADMIN_STATES[user.id] = {
        "upload": {
            "numbers": parsed,
            "filename": filename,
            "doc_msg_id": update.effective_message.message_id,
        }
    }
    text = (
        f"📄 <b>File Parsed:</b> <code>{html.escape(filename)}</code>\n"
        f"🔢 <b>Unique Valid Numbers:</b> <code>{len(parsed)}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Choose whether to <b>Add</b> or <b>Remove</b> these numbers:"
    )
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("➕ Add to Stock", callback_data="up_mode_add"),
            InlineKeyboardButton("🗑️ Remove from Stock", callback_data="up_mode_remove"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="up_cancel")],
    ])
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


def _render_upload_country_picker(pending_upload: Dict[str, Any]) -> Tuple[str, InlineKeyboardMarkup]:
    mode = pending_upload.get("mode", "add")
    countries = get_all_countries_with_stock(only_active=False)
    rows: List[List[InlineKeyboardButton]] = []
    for i in range(0, len(countries), 2):
        row = []
        for c in countries[i:i + 2]:
            cname = format_country_name(c["name"])
            row.append(InlineKeyboardButton(f"{cname} ({c['total_available']})", callback_data=f"up_c_{c['id']}"))
        rows.append(row)
    if mode == "add":
        rows.append([InlineKeyboardButton("🆕 Create New Country", callback_data="up_new_country")])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data="up_cancel")])
    text = (
        f"🌍 <b>Step 1: Select Country ({'Add' if mode == 'add' else 'Remove'})</b>\n"
        f"📄 <b>File:</b> <code>{html.escape(pending_upload['filename'])}</code> (<code>{len(pending_upload['numbers'])}</code> numbers)"
    )
    return text, InlineKeyboardMarkup(rows)


# ==============================================================================
# MOTHER BOT TEXT INPUT WIZARD & KEYBOARD HANDLER
# ==============================================================================
async def mother_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await _require_mother_admin(update)
    if not user or not update.effective_message:
        return
    msg_text = (update.effective_message.text or "").strip()

    # Check persistent keyboard navigation first (also clears pending prompts)
    nav_map = {
        "📊 Dashboard": build_dashboard_view,
        "🤖 Linked Bots": build_linked_bots_view,
        "📩 Format & Inbox": lambda: build_format_inbox_view("group"),
        "📦 Stock & Users": build_stock_users_hub_view,
        "👥 Groups & Links": build_groups_links_view,
        "⚙️ System & API": build_system_api_view,
    }
    if msg_text in nav_map:
        ADMIN_STATES.pop(user.id, None)
        text, kb = nav_map[msg_text]()
        await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
        return

    state = ADMIN_STATES.get(user.id)
    if not state:
        text, kb = build_dashboard_view()
        await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    action = state.get("await")

    # 1. Link child bot token
    if action in ("link_otp", "link_number"):
        role = "otp" if action == "link_otp" else "number"
        # Delete admin's sensitive token message from Telegram chat immediately
        try:
            await update.effective_message.delete()
        except Exception:
            pass
        ok, err_msg, info = await child_manager.validate_token(msg_text, role)
        if not ok or not info:
            await context.bot.send_message(
                chat_id=user.id,
                text=f"❌ <b>Token Verification Failed:</b>\n<code>{html.escape(err_msg)}</code>\n\nPaste a valid token or /cancel.",
                parse_mode=ParseMode.HTML,
            )
            return

        ADMIN_STATES.pop(user.id, None)
        set_child_token(role, msg_text)
        set_child_enabled(role, True)
        set_bot_display_name(role, info.get("first_name") or f"{role.upper()} Bot")
        started, start_msg = await child_manager.start_child(role, msg_text)
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))

        text, kb = build_linked_bots_view()
        status_note = f"✅ Linked &amp; started @{html.escape(info['username'])}" if started else f"⚠️ Saved token, start issue: {html.escape(start_msg)}"
        await context.bot.send_message(
            chat_id=user.id,
            text=f"{status_note}\n\n{text}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return

    # 2. Rename Mother / OTP / Number Bot display label
    if action in ("rename_mother", "rename_otp", "rename_number"):
        role = action.split("_", 1)[1]
        ADMIN_STATES.pop(user.id, None)
        set_bot_display_name(role, msg_text[:48])
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        text, kb = build_linked_bots_view()
        await update.effective_message.reply_text(f"✅ Renamed <b>{role.upper()}</b> label.\n\n{text}", parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    # 3. Create new country during .txt upload
    if action == "up_new_country" and "upload" in state:
        cname = format_country_name(msg_text)
        if not cname:
            await update.effective_message.reply_text("⚠️ Enter a valid country name or /cancel.")
            return
        cid = get_or_create_country(cname)
        pending = state["upload"]
        pending["country_id"] = cid
        pending["country_name"] = cname
        state.pop("await", None)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("👥 Standard Stock", callback_data=f"up_pool_std_{cid}")],
            [InlineKeyboardButton("🔒 Secret Stock", callback_data=f"up_pool_sec_{cid}")],
            [InlineKeyboardButton("❌ Cancel", callback_data="up_cancel")],
        ])
        await update.effective_message.reply_text(
            f"🌍 <b>Country Created:</b> <code>{html.escape(cname)}</code>\n🔒 <b>Step 2: Choose Destination Pool:</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return

    # 4. Pool country button rename / service tag
    if action in ("pool_ren", "pool_svc"):
        cid = state["cid"]
        ADMIN_STATES.pop(user.id, None)
        if action == "pool_ren":
            update_country_button_name(cid, msg_text[:48])
        else:
            update_country_service_name(cid, msg_text[:48])
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        text, kb = _build_country_detail_view(cid)
        await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    # 5. Country broadcast edits & custom broadcast
    if action in ("bcast_c_text", "bcast_c_btn") and "country_bcast" in state:
        state.pop("await", None)
        if action == "bcast_c_text":
            state["country_bcast"]["custom_text"] = msg_text
        else:
            state["country_bcast"]["button_label"] = msg_text[:60]
        text, kb = render_country_broadcast_panel(state["country_bcast"])
        await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    if action == "bcast_custom_text":
        ADMIN_STATES[user.id] = {"custom_bcast_text": msg_text}
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 Send Broadcast via Number Bot", callback_data="bcast_custom_send")],
            [InlineKeyboardButton("❌ Cancel", callback_data="m_stock")],
        ])
        await update.effective_message.reply_text(
            f"📢 <b>Custom Broadcast Preview:</b>\n<blockquote>{html.escape(msg_text)}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return

    # 6. Groups & Custom Link Buttons
    if action == "grp_add":
        ADMIN_STATES.pop(user.id, None)
        if not re.match(r"^-?\d+$", msg_text):
            await update.effective_message.reply_text("⚠️ Enter a numeric chat ID like <code>-1001234567890</code>.", parse_mode=ParseMode.HTML)
            return
        ok, status_msg = add_dynamic_linked_group(int(msg_text))
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        text, kb = build_groups_links_view()
        prefix = f"✅ {status_msg}\n\n" if ok else f"⚠️ {status_msg}\n\n"
        await update.effective_message.reply_text(prefix + text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
        return

    if action == "btn_add_step1":
        state["await"] = "btn_add_step2"
        state["btn_text"] = msg_text[:48]
        await update.effective_message.reply_text(
            f"🔗 Now send the URL or <code>@username</code> for button <b>{html.escape(state['btn_text'])}</b>:",
            parse_mode=ParseMode.HTML,
        )
        return

    if action == "btn_add_step2":
        norm_url = normalize_button_url(msg_text)
        btn_text = state.get("btn_text", "Link")
        ADMIN_STATES.pop(user.id, None)
        if not norm_url:
            await update.effective_message.reply_text("❌ Invalid URL. Expected `https://...` or `@username`.", parse_mode=ParseMode.HTML)
            return
        btns = get_custom_link_buttons()
        btns.append({"text": btn_text, "url": norm_url})
        save_custom_link_buttons(btns)
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        text, kb = build_groups_links_view()
        await update.effective_message.reply_text(f"✅ Custom button added.\n\n{text}", parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
        return

    # 7. User search & DM
    if action == "usr_search":
        ADMIN_STATES.pop(user.id, None)
        if not msg_text.isdigit():
            await update.effective_message.reply_text("⚠️ Enter a numeric Telegram User ID.")
            return
        text, kb = _build_user_detail_view(int(msg_text), back_page=0)
        await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    if action == "usr_dm":
        target_uid = state["uid"]
        ADMIN_STATES.pop(user.id, None)
        number_bot = child_manager.get_bot("number")
        if not number_bot:
            await update.effective_message.reply_text("⚠️ Number Bot is not running — link & start it first.")
            return
        try:
            await number_bot.send_message(chat_id=target_uid, text=msg_text, parse_mode=ParseMode.HTML)
            await update.effective_message.reply_text(f"✅ DM sent to <code>{target_uid}</code> via Number Bot.", parse_mode=ParseMode.HTML)
        except Exception as e:
            await update.effective_message.reply_text(f"❌ Failed to send DM: <code>{html.escape(str(e))}</code>", parse_mode=ParseMode.HTML)
        return

    # 8. System API URL / Key / Add Mother Admin / Custom Qty
    if action == "sys_set_url":
        ADMIN_STATES.pop(user.id, None)
        clean_url = msg_text.rstrip("/")
        set_bot_setting("thirdwave_base_url", clean_url)
        thirdwave_client.update_credentials(base_url=clean_url)
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        text, kb = build_system_api_view()
        await update.effective_message.reply_text(f"✅ API URL updated.\n\n{text}", parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    if action == "sys_set_key":
        try:
            await update.effective_message.delete()
        except Exception:
            pass
        ADMIN_STATES.pop(user.id, None)
        set_bot_setting("thirdwave_api_key", msg_text)
        thirdwave_client.update_credentials(api_key=msg_text)
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        text, kb = build_system_api_view()
        await context.bot.send_message(chat_id=user.id, text=f"✅ API Key updated.\n\n{text}", parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    if action == "sys_adm_add":
        ADMIN_STATES.pop(user.id, None)
        if not msg_text.isdigit():
            await update.effective_message.reply_text("⚠️ Enter a valid numeric Telegram User ID.")
            return
        add_admin(int(msg_text), added_by=user.id)
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        text, kb = _build_admins_view()
        await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return


# ==============================================================================
# MOTHER BOT CALLBACK HANDLER
# ==============================================================================
async def mother_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    user = await _require_mother_admin(update)
    if not user:
        return
    data = query.data or ""

    if data == "noop":
        await query.answer()
        return

    # Top-level tabs
    if data in ("m_dash", "m_bots", "m_fmt", "m_stock", "m_groups", "m_sys"):
        ADMIN_STATES.pop(user.id, None)
        await query.answer()
        builders = {
            "m_dash": build_dashboard_view,
            "m_bots": build_linked_bots_view,
            "m_fmt": lambda: build_format_inbox_view("group"),
            "m_stock": build_stock_users_hub_view,
            "m_groups": build_groups_links_view,
            "m_sys": build_system_api_view,
        }
        text, kb = builders[data]()
        await _safe_edit(query, text, kb)
        return

    # ---------------- LINKED BOTS MANAGEMENT ----------------
    if data.startswith("bot_link_"):
        role = data.split("_")[2]
        ADMIN_STATES[user.id] = {"await": f"link_{role}"}
        label = "OTP Forwarder Bot" if role == "otp" else "Number Distribution Bot"
        await query.answer()
        await _safe_edit(
            query,
            f"🔑 <b>Link {label} Token</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"Send your <code>@BotFather</code> bot token in chat now.\n"
            f"<i>Your message will be deleted immediately after verification.</i>",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="m_bots")]]),
        )
        return

    if data.startswith("bot_toggle_"):
        role = data.split("_")[2]
        if not child_manager.get_status(role)["has_token"]:
            await query.answer("⚠️ Link a bot token first.", show_alert=True)
            return
        new_state = not is_child_enabled(role)
        set_child_enabled(role, new_state)
        if new_state:
            await query.answer("▶️ Starting child bot...")
            await child_manager.start_child(role)
        else:
            await query.answer("⏸️ Pausing child bot...")
            await child_manager.stop_child(role)
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        text, kb = build_linked_bots_view()
        await _safe_edit(query, text, kb)
        return

    if data.startswith("bot_restart_"):
        role = data.split("_")[2]
        await query.answer("🔄 Restarting child bot...")
        set_child_enabled(role, True)
        await child_manager.start_child(role)
        text, kb = build_linked_bots_view()
        await _safe_edit(query, text, kb)
        return

    if data.startswith("bot_rename_"):
        role = data.split("_")[2]
        ADMIN_STATES[user.id] = {"await": f"rename_{role}"}
        await query.answer()
        await _safe_edit(
            query,
            f"✏️ <b>Rename {role.upper()} Display Name</b>\nSend the new label in chat:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="m_bots")]]),
        )
        return

    if data.startswith("bot_unlink_"):
        role = data.split("_")[2]
        await query.answer("🗑️ Unlinking bot...")
        await child_manager.stop_child(role)
        clear_child_token(role)
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        text, kb = build_linked_bots_view()
        await _safe_edit(query, text, kb)
        return

    # ---------------- FORMAT & INBOX ROUTING (INDEPENDENT) ----------------
    if data == "tog_group_otp":
        new_val = not is_group_forwarding_enabled()
        set_group_forwarding_enabled(new_val)
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        await query.answer(f"⚡ Group OTP {'Enabled' if new_val else 'Disabled'}")
        text, kb = build_format_inbox_view("group")
        await _safe_edit(query, text, kb)
        return

    if data == "tog_inbox_otp":
        new_val = not is_inbox_otp_enabled()
        set_inbox_otp_enabled(new_val)
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        await query.answer(f"🔔 Inbox DM {'Enabled' if new_val else 'Disabled'}")
        text, kb = build_format_inbox_view("inbox")
        await _safe_edit(query, text, kb)
        return

    if data.startswith("fmt_tab_"):
        target = data.split("_")[2]
        await query.answer()
        text, kb = build_format_inbox_view(target)
        await _safe_edit(query, text, kb)
        return

    if data.startswith("fmt_set_"):
        _, _, target, fmt_mode = data.split("_", 3)
        if fmt_mode not in SMS_FORMAT_META:
            await query.answer("⚠️ Invalid format.", show_alert=True)
            return
        if target == "inbox":
            set_inbox_sms_format(fmt_mode)
        else:
            set_group_sms_format(fmt_mode)
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        _, chosen_name, _ = _fmt_meta(fmt_mode)
        await query.answer(f"✅ {target.title()} format set to {chosen_name}")
        text, kb = build_format_inbox_view(target)
        await _safe_edit(query, text, kb)
        return

    if data.startswith("fmt_prev_"):
        target = data.split("_")[2]
        fmt_mode = get_inbox_sms_format() if target == "inbox" else get_group_sms_format()
        masked = target != "inbox"
        card_text, prev_otp, _ = format_otp_card(SAMPLE_OTP_PREVIEW_ITEM, sms_format=fmt_mode, masked=masked)
        kb = build_otp_keyboard(prev_otp, msg_id="preview_001", include_custom_buttons=(target == "group"))
        label = "📬 Inbox DM Preview" if target == "inbox" else "📩 Group OTP Preview"
        await query.answer("👁️ Sending live preview card...")
        await context.bot.send_message(
            chat_id=user.id,
            text=f"<b>{label}</b> (<code>{html.escape(fmt_mode)}</code>)\n━━━━━━━━━━━━━━━━━━━━\n{card_text}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
            disable_web_page_preview=True,
        )
        return

    if data.startswith("sms_") or data.startswith("copy_num_"):
        # Allow clicking preview buttons directly inside Mother Admin preview cards
        await query.answer("📋 Sample preview button — live OTP buttons work identically on child bots.", show_alert=True)
        return

    # ---------------- STOCK UPLOAD, POOLS, QTY, USERS, BROADCASTS ----------------
    if data == "adm_upload_prompt":
        await query.answer()
        await _safe_edit(
            query,
            "📤 <b>Upload Number Stock (.txt)</b>\n━━━━━━━━━━━━━━━━━━━━\nSend any <code>.txt</code> file with one phone number per line right here in this chat.",
            InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Stock & Users", callback_data="m_stock")]]),
        )
        return

    if data == "up_cancel":
        ADMIN_STATES.pop(user.id, None)
        await query.answer("❌ Cancelled")
        text, kb = build_stock_users_hub_view()
        await _safe_edit(query, text, kb)
        return

    if data in ("up_mode_add", "up_mode_remove"):
        pending = ADMIN_STATES.get(user.id, {}).get("upload")
        if not pending:
            await query.answer("⚠️ Upload session expired.", show_alert=True)
            return
        pending["mode"] = "add" if data == "up_mode_add" else "remove"
        await query.answer()
        text, kb = _render_upload_country_picker(pending)
        await _safe_edit(query, text, kb)
        return

    if data == "up_new_country":
        state = ADMIN_STATES.get(user.id)
        if not state or "upload" not in state:
            await query.answer("⚠️ Upload session expired.", show_alert=True)
            return
        state["await"] = "up_new_country"
        await query.answer()
        await _safe_edit(
            query,
            "🆕 <b>Enter New Country Name:</b>\nExample: <code>Nepal</code> or <code>🇳🇵 Nepal</code>",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="up_cancel")]]),
        )
        return

    if data.startswith("up_c_"):
        cid = int(data.split("_")[2])
        pending = ADMIN_STATES.get(user.id, {}).get("upload")
        if not pending:
            await query.answer("⚠️ Upload session expired.", show_alert=True)
            return
        c_info = get_country_by_id(cid) or {"name": f"Country {cid}"}
        c_name = c_info["name"]

        if pending.get("mode") == "remove":
            removed = remove_numbers_from_country(cid, pending["numbers"])
            doc_id = pending.get("doc_msg_id")
            ADMIN_STATES.pop(user.id, None)
            if doc_id:
                try:
                    await context.bot.delete_message(chat_id=user.id, message_id=doc_id)
                except Exception:
                    pass
            if gist_storage.enabled:
                asyncio.create_task(gist_storage.export_and_sync(immediate=True))
            await query.answer(f"🗑️ Removed {removed} numbers")
            await _safe_edit(
                query,
                f"🗑️ <b>Removed <code>{removed}</code> numbers from {html.escape(c_name)}</b>",
                InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Stock & Users", callback_data="m_stock")]]),
            )
            return

        pending["country_id"] = cid
        pending["country_name"] = c_name
        await query.answer()
        await _safe_edit(
            query,
            f"🌍 <b>{html.escape(c_name)}</b> · <code>{len(pending['numbers'])}</code> numbers\n🔒 <b>Step 2: Choose Pool:</b>",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("👥 Standard Stock", callback_data=f"up_pool_std_{cid}")],
                [InlineKeyboardButton("🔒 Secret Stock", callback_data=f"up_pool_sec_{cid}")],
                [InlineKeyboardButton("❌ Cancel", callback_data="up_cancel")],
            ]),
        )
        return

    if data.startswith("up_pool_std_") or data.startswith("up_pool_sec_"):
        is_sec = data.startswith("up_pool_sec_")
        cid = int(data.split("_")[3])
        pending = ADMIN_STATES.get(user.id, {}).get("upload")
        if not pending:
            await query.answer("⚠️ Upload session expired.", show_alert=True)
            return
        added, dups = add_numbers_to_country(cid, pending["numbers"], is_secret=is_sec)
        c_name = pending.get("country_name") or (get_country_by_id(cid) or {}).get("name", f"Country {cid}")
        doc_id = pending.get("doc_msg_id")
        ADMIN_STATES.pop(user.id, None)
        if doc_id:
            try:
                await context.bot.delete_message(chat_id=user.id, message_id=doc_id)
            except Exception:
                pass
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        await query.answer(f"✅ Added {added} numbers")
        await _safe_edit(
            query,
            f"✅ <b>Upload Successful!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🌍 <b>Country:</b> <code>{html.escape(c_name)}</code>\n"
            f"📂 <b>Pool:</b> {'🔒 Secret Stock' if is_sec else 'Standard Stock'}\n"
            f"➕ <b>Added:</b> <code>{added}</code> · ⚠️ <b>Duplicates:</b> <code>{dups}</code>",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("📢 Broadcast This Country", callback_data=f"adm_bcast_c_{cid}")],
                [InlineKeyboardButton("🔙 Stock & Users", callback_data="m_stock")],
            ]),
        )
        return

    if data == "adm_stock_pools":
        await query.answer()
        text, kb = _build_stock_pools_view()
        await _safe_edit(query, text, kb)
        return

    if data.startswith("pool_c_"):
        cid = int(data.split("_")[2])
        await query.answer()
        text, kb = _build_country_detail_view(cid)
        await _safe_edit(query, text, kb)
        return

    if data.startswith("pool_ren_") or data.startswith("pool_svc_"):
        _, action, cid_s = data.split("_")
        cid = int(cid_s)
        ADMIN_STATES[user.id] = {"await": f"pool_{action}", "cid": cid}
        await query.answer()
        await _safe_edit(
            query,
            f"✏️ Send the new <b>{'button label' if action == 'ren' else 'service tag'}</b> in chat:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"pool_c_{cid}")]]),
        )
        return

    if data.startswith("pool_del_ask_"):
        cid = int(data.split("_")[3])
        await query.answer()
        await _safe_edit(
            query,
            "⚠️ <b>Confirm Permanent Deletion</b>\nThis removes the country and its stock DB permanently.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🗑️ Yes, Delete Country", callback_data=f"pool_del_ok_{cid}")],
                [InlineKeyboardButton("❌ Cancel", callback_data=f"pool_c_{cid}")],
            ]),
        )
        return

    if data.startswith("pool_del_ok_"):
        cid = int(data.split("_")[3])
        delete_country_and_stock(cid)
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        await query.answer("🗑️ Country deleted")
        text, kb = _build_stock_pools_view()
        await _safe_edit(query, text, kb)
        return

    # Delivery Quantity Policy
    if data == "adm_qty_menu" or data.startswith("qty_set_"):
        if data.startswith("qty_set_"):
            val = int(data.split("_")[2])
            set_admin_fixed_quantity("" if val == 0 else str(val))
            if gist_storage.enabled:
                asyncio.create_task(gist_storage.export_and_sync(immediate=True))
            await query.answer(f"✅ Updated quantity policy ({val or 'Default 10'})")
        else:
            await query.answer()
        curr = get_admin_fixed_quantity()
        rows = [
            [
                InlineKeyboardButton(f"{'✅ ' if curr is None else ''}Default (10)", callback_data="qty_set_0"),
                InlineKeyboardButton(f"{'✅ ' if curr == 1 else ''}Fixed: 1", callback_data="qty_set_1"),
            ],
            [
                InlineKeyboardButton(f"{'✅ ' if curr == 2 else ''}Fixed: 2", callback_data="qty_set_2"),
                InlineKeyboardButton(f"{'✅ ' if curr == 3 else ''}Fixed: 3", callback_data="qty_set_3"),
                InlineKeyboardButton(f"{'✅ ' if curr == 5 else ''}Fixed: 5", callback_data="qty_set_5"),
            ],
            [InlineKeyboardButton("🔙 Stock & Users", callback_data="m_stock")],
        ]
        await _safe_edit(
            query,
            f"🔢 <b>Numbers Per Request Policy</b>\n━━━━━━━━━━━━━━━━━━━━\nCurrent setting: <code>{curr if curr is not None else 'Default (10)'}</code>",
            InlineKeyboardMarkup(rows),
        )
        return

    # Users Management
    if data.startswith("adm_users_"):
        page = int(data.split("_")[2])
        await query.answer()
        text, kb = _build_users_list_view(page)
        await _safe_edit(query, text, kb)
        return

    if data == "usr_search":
        ADMIN_STATES[user.id] = {"await": "usr_search"}
        await query.answer()
        await _safe_edit(
            query,
            "🔍 <b>Find User by ID</b>\nSend the user's numeric Telegram ID in chat:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="adm_users_0")]]),
        )
        return

    if data.startswith("usr_v_"):
        _, _, uid_s, page_s = data.split("_")
        await query.answer()
        text, kb = _build_user_detail_view(int(uid_s), int(page_s))
        await _safe_edit(query, text, kb)
        return

    if data.startswith("usr_sec_") or data.startswith("usr_ban_"):
        parts = data.split("_")
        action = parts[1]
        uid, flag, page = int(parts[2]), bool(int(parts[3])), int(parts[4])
        if action == "sec":
            set_user_secret_access(uid, flag)
            await query.answer("🔒 Secret pool updated")
        else:
            set_user_ban_status(uid, flag)
            await query.answer("🛡️ Ban status updated")
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        text, kb = _build_user_detail_view(uid, page)
        await _safe_edit(query, text, kb)
        return

    if data.startswith("usr_dm_"):
        _, _, uid_s, page_s = data.split("_")
        uid = int(uid_s)
        ADMIN_STATES[user.id] = {"await": "usr_dm", "uid": uid}
        await query.answer()
        await _safe_edit(
            query,
            f"✉️ <b>DM User <code>{uid}</code> via Number Bot</b>\nSend the message text in chat:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"usr_v_{uid}_{page_s}")]]),
        )
        return

    if data.startswith("usr_del_"):
        _, _, uid_s, page_s = data.split("_")
        delete_user(int(uid_s))
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        await query.answer("🗑️ User deleted")
        text, kb = _build_users_list_view(int(page_s))
        await _safe_edit(query, text, kb)
        return

    # Broadcasts (Country Studio & Custom & Recall History)
    if data.startswith("adm_bcast_c_"):
        cid = int(data.split("_")[3])
        all_c = get_all_countries_with_stock(only_active=False)
        c_info = next((x for x in all_c if x["id"] == cid), {})
        c_name = c_info.get("name", "Unknown")
        btn_name = c_info.get("button_name") or c_name
        bcast_data = {
            "country_id": cid,
            "country_name": c_name,
            "quantity": c_info.get("total_available", 0),
            "button_label": f"📥 Get {btn_name} Number",
            "custom_text": f"🔥 Fresh {c_name} numbers are loaded! Tap the button below to claim yours now.",
        }
        ADMIN_STATES[user.id] = {"country_bcast": bcast_data}
        await query.answer()
        text, kb = render_country_broadcast_panel(bcast_data)
        await _safe_edit(query, text, kb)
        return

    if data.startswith("bcast_c_edit_text_") or data.startswith("bcast_c_edit_btn_"):
        cid = int(data.split("_")[4])
        state = ADMIN_STATES.setdefault(user.id, {})
        state["await"] = "bcast_c_text" if "edit_text" in data else "bcast_c_btn"
        await query.answer()
        await _safe_edit(
            query,
            "✏️ Send the new broadcast text/label in chat:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"adm_bcast_c_{cid}")]]),
        )
        return

    if data.startswith("bcast_c_refresh_"):
        cid = int(data.split("_")[3])
        all_c = get_all_countries_with_stock(only_active=False)
        c_info = next((x for x in all_c if x["id"] == cid), {})
        bcast_data = ADMIN_STATES.get(user.id, {}).get("country_bcast")
        if bcast_data:
            bcast_data["quantity"] = c_info.get("total_available", 0)
            await query.answer(f"🔄 Live stock: {bcast_data['quantity']}")
            text, kb = render_country_broadcast_panel(bcast_data)
            await _safe_edit(query, text, kb)
        return

    if data.startswith("bcast_c_send_"):
        cid = int(data.split("_")[3])
        bcast_data = ADMIN_STATES.pop(user.id, {}).get("country_bcast") or {}
        c_info = get_country_by_id(cid) or {}
        c_name = bcast_data.get("country_name") or c_info.get("name", "Country")
        await query.answer("🚀 Starting broadcast...")
        await _send_broadcast_via_number_bot(
            query,
            admin_id=user.id,
            country_id=cid,
            custom_text=bcast_data.get("custom_text") or f"🔥 Fresh {c_name} numbers ready!",
            btn_label=bcast_data.get("button_label"),
            c_name=c_name,
            qty=bcast_data.get("quantity", 0),
        )
        return

    if data == "adm_bcast_custom":
        ADMIN_STATES[user.id] = {"await": "bcast_custom_text"}
        await query.answer()
        await _safe_edit(
            query,
            "📢 <b>Custom Broadcast (via Number Bot)</b>\nSend the announcement message in chat (HTML supported):",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="m_stock")]]),
        )
        return

    if data == "bcast_custom_send":
        msg_body = ADMIN_STATES.pop(user.id, {}).get("custom_bcast_text", "")
        if not msg_body:
            await query.answer("⚠️ Empty broadcast.", show_alert=True)
            return
        await query.answer("🚀 Broadcasting...")
        await _send_broadcast_via_number_bot(query, admin_id=user.id, country_id=None, custom_text=msg_body)
        return

    if data == "adm_bcast_hist":
        await query.answer()
        items = get_recent_broadcasts(limit=10)
        if not items:
            await _safe_edit(
                query,
                "🗑️ <b>Broadcast History</b>\n━━━━━━━━━━━━━━━━━━━━\n<i>No broadcasts sent yet.</i>",
                InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Stock & Users", callback_data="m_stock")]]),
            )
            return
        rows = []
        lines = []
        for it in items:
            title = it.get("service_name") or (f"Country #{it['country_id']}" if it.get("country_id") else "Announcement")
            st = "🗑️ Recalled" if it.get("is_deleted") else f"✅ {it.get('total_sent', 0)} sent"
            lines.append(f"• <b>{html.escape(title)}</b> ({st})")
            if not it.get("is_deleted"):
                rows.append([InlineKeyboardButton(f"🗑️ Recall: {title[:22]}", callback_data=f"bcast_recall_{it['broadcast_id']}")])
        rows.append([InlineKeyboardButton("🔙 Stock & Users", callback_data="m_stock")])
        await _safe_edit(query, "🗑️ <b>Recent Broadcasts</b>\n━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines), InlineKeyboardMarkup(rows))
        return

    if data.startswith("bcast_recall_"):
        bid = data[len("bcast_recall_"):]
        number_bot = child_manager.get_bot("number")
        if not number_bot:
            await query.answer("⚠️ Number Bot must be running to recall messages.", show_alert=True)
            return
        msgs = get_broadcast_messages(bid)
        await query.answer(f"🗑️ Recalling {len(msgs)} messages...")
        deleted_ok = 0
        for uid, mid in msgs:
            try:
                await number_bot.delete_message(chat_id=uid, message_id=mid)
                deleted_ok += 1
            except Exception:
                pass
        mark_broadcast_deleted(bid)
        await _safe_edit(
            query,
            f"🗑️ <b>Broadcast Recalled</b>\nRemoved <code>{deleted_ok}/{len(msgs)}</code> messages from user chats.",
            InlineKeyboardMarkup([[InlineKeyboardButton("🔙 History", callback_data="adm_bcast_hist")]]),
        )
        return

    # ---------------- GROUPS & CUSTOM BUTTONS ----------------
    if data == "grp_add":
        ADMIN_STATES[user.id] = {"await": "grp_add"}
        await query.answer()
        await _safe_edit(
            query,
            "➕ <b>Link Target OTP Group/Channel</b>\nSend the numeric chat ID (e.g. <code>-1001234567890</code>) or <code>@username</code>:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="m_groups")]]),
        )
        return

    if data == "grp_rm_menu":
        groups = get_target_group_chat_ids()
        await query.answer()
        rows = [[InlineKeyboardButton(f"🗑️ Remove {g}", callback_data=f"grp_rm_{g[:48]}")] for g in groups]
        if groups:
            rows.append([InlineKeyboardButton("🧹 Clear Dynamic Groups", callback_data="grp_rm_all")])
        rows.append([InlineKeyboardButton("🔙 Back", callback_data="m_groups")])
        await _safe_edit(query, "🗑️ <b>Remove Linked Group</b>", InlineKeyboardMarkup(rows))
        return

    if data == "grp_rm_all":
        clear_dynamic_linked_groups()
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        await query.answer("🧹 Dynamic groups cleared")
        text, kb = build_groups_links_view()
        await _safe_edit(query, text, kb)
        return

    if data.startswith("grp_rm_"):
        gid_s = data[7:]
        try:
            removed, rm_msg = remove_dynamic_linked_group(int(gid_s))
        except ValueError:
            removed, rm_msg = False, "Invalid group ID."
        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        await query.answer("🗑️ Removed" if removed else rm_msg)
        text, kb = build_groups_links_view()
        await _safe_edit(query, text, kb)
        return

    if data == "grp_test_send":
        otp_bot = child_manager.get_bot("otp")
        groups = get_target_group_chat_ids()
        if not otp_bot:
            await query.answer("⚠️ OTP Bot is not running.", show_alert=True)
            return
        if not groups:
            await query.answer("⚠️ No target groups configured.", show_alert=True)
            return
        await query.answer("🧪 Sending test card...")
        ok = 0
        for g in groups:
            try:
                await otp_bot.send_message(
                    chat_id=g,
                    text="🧪 <b>Test Ping from Mother Bot → OTP Bot</b>\nConnection verified!",
                    parse_mode=ParseMode.HTML,
                )
                ok += 1
            except Exception:
                pass
        await _safe_edit(
            query,
            f"🧪 <b>Group Test Complete:</b> <code>{ok}/{len(groups)}</code> groups delivered.",
            InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Groups & Links", callback_data="m_groups")]]),
        )
        return

    if data == "btn_add":
        ADMIN_STATES[user.id] = {"await": "btn_add_step1"}
        await query.answer()
        await _safe_edit(
            query,
            "➕ <b>Add Custom Link Button</b>\nSend the button label text (e.g. <code>📥 Get Number</code>):",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="m_groups")]]),
        )
        return

    if data == "btn_manage":
        btns = get_custom_link_buttons()
        await query.answer()
        rows = [[InlineKeyboardButton(f"🗑️ {b['text'][:40]}", callback_data=f"btn_del_{i}")] for i, b in enumerate(btns)]
        rows.append([InlineKeyboardButton("🔙 Back", callback_data="m_groups")])
        await _safe_edit(query, "✏️ <b>Delete Custom Link Buttons</b>", InlineKeyboardMarkup(rows))
        return

    if data.startswith("btn_del_"):
        idx = int(data.split("_")[2])
        btns = get_custom_link_buttons()
        if 0 <= idx < len(btns):
            btns.pop(idx)
            save_custom_link_buttons(btns)
            if gist_storage.enabled:
                asyncio.create_task(gist_storage.export_and_sync(immediate=True))
        await query.answer("🗑️ Button deleted")
        text, kb = build_groups_links_view()
        await _safe_edit(query, text, kb)
        return

    # ---------------- SYSTEM, API & MOTHER ADMINS ----------------
    if data in ("sys_set_url", "sys_set_key"):
        ADMIN_STATES[user.id] = {"await": data}
        await query.answer()
        prompt = "🌐 Send the new Thirdwave API Base URL:" if data == "sys_set_url" else "🔑 Send the new Thirdwave API Key (message will be auto-deleted):"
        await _safe_edit(query, prompt, InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="m_sys")]]))
        return

    if data == "sys_test_api":
        await query.answer("🩺 Probing Thirdwave API...")
        msgs = await thirdwave_client.fetch_incoming_messages(limit=5)
        status_line = f"✅ <b>API Healthy</b> — fetched <code>{len(msgs)}</code> recent records." if msgs else "❌ <b>API Check Failed or Empty</b> — verify URL & Key."
        await _safe_edit(
            query,
            f"🩺 <b>Thirdwave API Probe</b>\n━━━━━━━━━━━━━━━━━━━━\n{status_line}",
            InlineKeyboardMarkup([[InlineKeyboardButton("🔙 System & API", callback_data="m_sys")]]),
        )
        return

    if data == "sys_admins":
        await query.answer()
        text, kb = _build_admins_view()
        await _safe_edit(query, text, kb)
        return

    if data == "sys_adm_add":
        ADMIN_STATES[user.id] = {"await": "sys_adm_add"}
        await query.answer()
        await _safe_edit(
            query,
            "➕ <b>Add Mother Admin</b>\nSend the Telegram numeric User ID to promote:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="sys_admins")]]),
        )
        return

    if data.startswith("sys_adm_rm_"):
        aid = int(data.split("_")[3])
        if remove_admin(aid):
            if gist_storage.enabled:
                asyncio.create_task(gist_storage.export_and_sync(immediate=True))
            await query.answer("🗑️ Admin removed")
        else:
            await query.answer("⚠️ Root admin cannot be removed", show_alert=True)
        text, kb = _build_admins_view()
        await _safe_edit(query, text, kb)
        return

    if data == "sys_sync_gist":
        if not gist_storage.enabled:
            await query.answer("⚠️ GIST_TOKEN / GIST_ID not set.", show_alert=True)
            return
        await query.answer("☁️ Syncing state to Gist...")
        ok = await gist_storage.export_and_sync(immediate=True)
        await _safe_edit(
            query,
            f"☁️ <b>Gist Cloud Backup:</b> {'✅ Synced successfully' if ok else '❌ Sync failed'}",
            InlineKeyboardMarkup([[InlineKeyboardButton("🔙 System & API", callback_data="m_sys")]]),
        )
        return

    if data == "sys_handover":
        await query.answer("🚀 Triggering handover...")
        dispatched = await trigger_proactive_handover(manual_restart=True)
        msg = "🚀 <b>Zero-Restart Handover Triggered!</b>\nThe next runner is booting now." if dispatched else "ℹ️ Running locally (no GitHub Actions repo configured)."
        await _safe_edit(query, msg, InlineKeyboardMarkup([[InlineKeyboardButton("🔙 System & API", callback_data="m_sys")]]))
        return

    await query.answer()


async def mother_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    if isinstance(err, Conflict):
        logger.warning("⚠️ Mother bot Conflict (previous instance shutting down) — waiting cleanly...")
        return
    if isinstance(err, (NetworkError, TimedOut)):
        logger.warning(f"⚠️ Mother bot transient network error: {err}")
        return
    logger.warning(f"⚠️ Mother bot error: {err}", exc_info=err)
