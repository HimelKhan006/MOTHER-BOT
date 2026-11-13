import asyncio
import html
import io
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

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
    MenuButtonDefault,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.constants import ChatType, ParseMode
from telegram.error import Conflict, Forbidden, InvalidToken, NetworkError, RetryAfter, TelegramError, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from shared_core import (
    MOTHER_BOT_TOKEN,
    build_otp_keyboard,
    consume_numbers_for_user,
    extract_otp_code,
    format_country_name,
    format_otp_card,
    get_all_countries_with_stock,
    get_bot_display_name,
    get_cached_sms_message,
    get_child_token,
    get_country_by_id,
    get_custom_link_buttons,
    get_effective_quantity_for_user,
    get_group_sms_format,
    get_inbox_sms_format,
    get_user_details,
    get_user_plus_preference,
    get_user_recent_claims,
    gist_storage,
    is_admin,
    is_child_enabled,
    is_group_forwarding_enabled,
    is_inbox_otp_enabled,
    is_user_banned,
    logger,
    mask_bot_token,
    register_user,
    send_with_retry,
    set_bot_display_name,
    set_user_plus_preference,
    user_has_secret_access,
)

# Number Bot UI constants
COUNTRIES_PER_PAGE = 8


# ==============================================================================
# NUMBER BOT SHARED UI HELPERS (User-Facing Only — Zero Admin UI)
# ==============================================================================
def _clean_number(num: str, with_plus: bool = True) -> str:
    clean = str(num).strip().lstrip("+")
    return f"+{clean}" if with_plus else clean


def format_delivered_numbers(
    numbers: List[str],
    country_name: str,
    available_count: Optional[int] = None,
    with_plus: bool = True,
    is_secret: bool = False,
) -> str:
    """Builds compact delivery confirmation header for Number Bot DM."""
    display_cname = format_country_name(country_name)
    title_prefix = "🔒 " if is_secret else ""
    assigned_label = "Secret Number Assigned" if is_secret else "Number Assigned"
    num_hint = "Tap any number below to copy" if len(numbers) > 1 else "Tap the number below to copy"
    divider = "━" * 20

    lines = [
        f"{title_prefix}<b>{html.escape(display_cname)} — {assigned_label}</b>",
        divider,
        f"👉 <b>{num_hint}</b>",
        "⚡ <i>OTP will appear here instantly.</i>",
        "📢 <i>No OTP? Check our OTP Group.</i>",
    ]
    if available_count is not None and available_count > 0:
        lines.append(f"📦 <b>Pool Stock:</b> {available_count} remaining")
    lines.append(divider)
    return "\n".join(lines)


def get_select_country_text(secret_mode: bool = False) -> str:
    divider = "━" * 20
    if secret_mode:
        return f"🔒 <b>Secret Numbers Pool:</b>\n{divider}"
    return f"🌍 <b>Select Country:</b>\n{divider}"


def get_user_reply_keyboard(user_id: int = 0) -> ReplyKeyboardMarkup:
    """
    User-only reply keyboard for the Number Bot.
    Never shows any Admin Panel button (all administration lives in Mother Bot).
    """
    has_secret = is_admin(user_id) or user_has_secret_access(user_id)
    row1 = [KeyboardButton("⚡ Get Number")]
    if has_secret:
        row1.append(KeyboardButton("🔒 Secret Numbers"))
    row2 = [KeyboardButton("👤 My Account"), KeyboardButton("ℹ️ Help")]
    return ReplyKeyboardMarkup(
        [row1, row2],
        resize_keyboard=True,
        is_persistent=True,
    )


def get_countries_keyboard(
    page: int = 0,
    secret_mode: bool = False,
    for_user: bool = True,
) -> InlineKeyboardMarkup:
    countries = get_all_countries_with_stock(only_active=for_user, secret_mode=secret_mode)
    total = len(countries)
    total_pages = max(1, (total + COUNTRIES_PER_PAGE - 1) // COUNTRIES_PER_PAGE)
    page = max(0, min(page, total_pages - 1))

    start_idx = page * COUNTRIES_PER_PAGE
    end_idx = start_idx + COUNTRIES_PER_PAGE
    page_items = countries[start_idx:end_idx]

    keyboard: List[List[InlineKeyboardButton]] = []
    prefix = "sec_c_" if secret_mode else "c_"
    page_prefix = "sec_page_" if secret_mode else "page_"
    refresh_cb = "btn_get_secret_number" if secret_mode else "btn_get_number"

    if not page_items:
        keyboard.append([InlineKeyboardButton("📭 No Numbers Available", callback_data="noop")])
    else:
        for idx in range(0, len(page_items), 2):
            row: List[InlineKeyboardButton] = []
            for c in page_items[idx:idx + 2]:
                display_c = format_country_name(c.get("button_name") or c["name"])
                avail = c.get("available_sec", 0) if secret_mode else c.get("available_std", c.get("total_available", 0))
                btn_text = f"{display_c} ({avail})"
                row.append(InlineKeyboardButton(btn_text, callback_data=f"{prefix}{c['id']}"))
            keyboard.append(row)

    if total_pages > 1:
        nav_row: List[InlineKeyboardButton] = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"{page_prefix}{page - 1}"))
        nav_row.append(InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"{page_prefix}{page + 1}"))
        keyboard.append(nav_row)

    keyboard.append([
        InlineKeyboardButton("🔄 Refresh Stock", callback_data=refresh_cb),
        InlineKeyboardButton("👤 My Account", callback_data="btn_my_account"),
    ])

    for btn in get_custom_link_buttons():
        keyboard.append([InlineKeyboardButton(btn["text"], url=btn["url"])])

    return InlineKeyboardMarkup(keyboard)


def get_numbers_view_keyboard(
    country_id: int,
    is_secret: bool = False,
    with_plus: bool = True,
    quantity: int = 1,
    is_fixed: bool = True,
    numbers: Optional[List[str]] = None,
) -> InlineKeyboardMarkup:
    change_cb = f"sec_change_num_{country_id}" if is_secret else f"change_num_{country_id}"
    back_cb = "btn_get_secret_number" if is_secret else "btn_get_number"
    toggle_val = 0 if with_plus else 1
    sec_flag = 1 if is_secret else 0
    plus_label = "Format: + (Tap to hide +)" if with_plus else "Format: No + (Tap to add +)"

    rows: List[List[InlineKeyboardButton]] = []
    if numbers:
        for num in numbers:
            clean = _clean_number(num, with_plus=with_plus)
            try:
                rows.append([InlineKeyboardButton(f"📋 {clean}", copy_text={"text": clean})])
            except Exception:
                rows.append([InlineKeyboardButton(f"📋 {clean}", callback_data=f"copy_num_{clean[:40]}")])

    rows.append([
        InlineKeyboardButton("🔄 Change Number", callback_data=change_cb),
        InlineKeyboardButton("🌍 Countries", callback_data=back_cb),
    ])
    rows.append([
        InlineKeyboardButton(
            f"➕ {plus_label}",
            callback_data=f"toggle_plus_{country_id}_{sec_flag}_{toggle_val}",
        )
    ])

    for btn in get_custom_link_buttons():
        rows.append([InlineKeyboardButton(btn["text"], url=btn["url"])])

    return InlineKeyboardMarkup(rows)


def render_country_broadcast_panel(bcast_data: Dict[str, Any]) -> Tuple[str, InlineKeyboardMarkup]:
    """Renders Mother Admin preview & controls for Number Bot country stock broadcasts."""
    cid = bcast_data["country_id"]
    c_name = bcast_data.get("country_name", "Unknown")
    qty = bcast_data.get("quantity", 0)
    btn_label = bcast_data.get("button_label", f"📥 Get {c_name} Number")
    custom_text = bcast_data.get("custom_text", "")

    text = (
        f"📢 <b>Stock Broadcast Studio (via Number Bot)</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🌍 <b>Country:</b> <code>{html.escape(c_name)}</code>\n"
        f"📦 <b>Live Available Stock:</b> <code>{qty}</code>\n"
        f"🔘 <b>Action Button:</b> <code>{html.escape(btn_label)}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📝 <b>Message Preview:</b>\n"
        f"<blockquote>{html.escape(custom_text)}</blockquote>"
    )
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✏️ Edit Text", callback_data=f"bcast_c_edit_text_{cid}"),
            InlineKeyboardButton("🏷️ Edit Button", callback_data=f"bcast_c_edit_btn_{cid}"),
        ],
        [InlineKeyboardButton("🔄 Refresh Live Stock", callback_data=f"bcast_c_refresh_{cid}")],
        [InlineKeyboardButton("🚀 Send Broadcast via Number Bot", callback_data=f"bcast_c_send_{cid}")],
        [InlineKeyboardButton("🔙 Back to Stock Pools", callback_data="adm_stock_pools")],
    ])
    return text, keyboard


async def _safe_edit(query: CallbackQuery, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    """Safely edits a callback message, ignoring Telegram's harmless 'Message is not modified' error."""
    try:
        await query.edit_message_text(
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
    except TelegramError as e:
        if "message is not modified" not in str(e).lower():
            raise


# ==============================================================================
# CHILD BOT MANAGER (OTP BOT + NUMBER BOT — ZERO ADMIN ACCESS ON EITHER)
# ==============================================================================
class ChildBotManager:
    """Runs and manages the linked OTP Child Bot and Number Child Bot by token."""

    def __init__(self) -> None:
        self.apps: Dict[str, Optional[Application]] = {"otp": None, "number": None}
        self.bot_info: Dict[str, Dict[str, Any]] = {"otp": {}, "number": {}}
        self.last_error: Dict[str, str] = {"otp": "", "number": ""}
        self._lock = asyncio.Lock()

    async def validate_token(
        self,
        token: str,
        role: str,
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Validates a BotFather token before linking it to a child role."""
        clean = (token or "").strip()
        if not clean or ":" not in clean or len(clean) < 20:
            return False, "Invalid bot token format. Expected format like `123456:ABC...`.", None

        if MOTHER_BOT_TOKEN and clean == MOTHER_BOT_TOKEN.strip():
            return False, "Child bot token cannot be the same as the Mother Bot token.", None

        other_role = "number" if role == "otp" else "otp"
        other_token = get_child_token(other_role)
        if other_token and clean == other_token.strip():
            other_label = "Number Bot" if other_role == "number" else "OTP Bot"
            return False, f"This token is already linked as the {other_label}. Use a separate bot token.", None

        try:
            async with Bot(token=clean) as probe_bot:
                me = await probe_bot.get_me()
            info = {
                "id": me.id,
                "username": me.username or "",
                "first_name": me.first_name or "",
            }
            return True, "Token verified.", info
        except InvalidToken:
            return False, "Telegram rejected this token (InvalidToken).", None
        except TelegramError as e:
            return False, f"Telegram API check failed: {e}", None
        except Exception as e:
            return False, f"Unexpected verification error: {e}", None

    def get_bot(self, role: str) -> Optional[Bot]:
        app = self.apps.get(role)
        return app.bot if app is not None else None

    def is_running(self, role: str) -> bool:
        app = self.apps.get(role)
        return bool(app and getattr(app, "running", False))

    def get_status(self, role: str) -> Dict[str, Any]:
        token = get_child_token(role)
        info = self.bot_info.get(role, {})
        running = self.is_running(role)
        return {
            "role": role,
            "has_token": bool(token),
            "masked_token": mask_bot_token(token),
            "enabled": is_child_enabled(role),
            "running": running,
            "bot_id": info.get("id"),
            "username": info.get("username", ""),
            "first_name": info.get("first_name", ""),
            "display_name": get_bot_display_name(role),
            "last_error": self.last_error.get(role, ""),
        }

    async def start_child(self, role: str, token: Optional[str] = None) -> Tuple[bool, str]:
        if role not in ("otp", "number"):
            return False, f"Unknown child bot role: {role}"

        async with self._lock:
            await self._stop_child_unlocked(role)
            use_token = (token if token is not None else get_child_token(role)).strip()
            if not use_token:
                self.last_error[role] = "No token configured"
                return False, "No bot token linked for this slot."

            if not is_child_enabled(role):
                self.last_error[role] = ""
                return False, "Child bot is paused/disabled."

            app: Optional[Application] = None
            try:
                app = self._build_otp_app(use_token) if role == "otp" else self._build_number_app(use_token)
                await app.initialize()
                me = await app.bot.get_me()
                self.bot_info[role] = {
                    "id": me.id,
                    "username": me.username or "",
                    "first_name": me.first_name or "",
                }
                if me.first_name:
                    curr_label = get_bot_display_name(role)
                    if curr_label in ("OTP Child Bot", "Number Child Bot", "SMS WAVE", "NUMBER BOT"):
                        set_bot_display_name(role, me.first_name)

                await self._configure_child_commands(app.bot, role)
                await app.start()
                if app.updater:
                    for attempt in range(1, 6):
                        try:
                            await app.updater.start_polling(
                                drop_pending_updates=True,
                                allowed_updates=Update.ALL_TYPES,
                            )
                            break
                        except Conflict:
                            if attempt >= 5:
                                raise
                            logger.warning(f"⚠️ {role.upper()} child conflict (previous session releasing), waiting 4s ({attempt}/5)...")
                            await asyncio.sleep(4.0)
                self.apps[role] = app
                self.last_error[role] = ""
                logger.info(f"🟢 Started {role.upper()} child bot (@{me.username})")
                return True, f"Started @{me.username or me.first_name}"
            except Exception as e:
                self.last_error[role] = str(e)
                logger.error(f"❌ Failed to start {role} child bot: {e}", exc_info=True)
                if app is not None:
                    await self._safe_teardown(app)
                self.apps[role] = None
                return False, str(e)

    async def stop_child(self, role: str) -> None:
        async with self._lock:
            await self._stop_child_unlocked(role)

    async def _stop_child_unlocked(self, role: str) -> None:
        app = self.apps.get(role)
        self.apps[role] = None
        if app is not None:
            await self._safe_teardown(app)
            logger.info(f"🛑 Stopped {role.upper()} child bot")

    async def _safe_teardown(self, app: Application) -> None:
        try:
            if app.updater and app.updater.running:
                await app.updater.stop()
        except Exception:
            pass
        try:
            if app.running:
                await app.stop()
        except Exception:
            pass
        try:
            await app.shutdown()
        except Exception:
            pass

    async def start_all_saved(self) -> Dict[str, Tuple[bool, str]]:
        results: Dict[str, Tuple[bool, str]] = {}
        for role in ("otp", "number"):
            if get_child_token(role) and is_child_enabled(role):
                results[role] = await self.start_child(role)
            else:
                results[role] = (False, "Not configured or disabled")
        return results

    async def stop_all(self) -> None:
        for role in ("otp", "number"):
            await self.stop_child(role)

    async def _configure_child_commands(self, bot: Bot, role: str) -> None:
        """Sets clean user-only commands on child bots in private chats; wipes group commands."""
        try:
            if role == "otp":
                cmds = [BotCommand("start", "Check OTP forwarder info & links")]
            else:
                cmds = [
                    BotCommand("start", "Start & select country number"),
                    BotCommand("help", "How to use this bot"),
                ]
            await bot.set_my_commands(cmds, scope=BotCommandScopeAllPrivateChats())
            await bot.set_chat_menu_button(menu_button=MenuButtonCommands())
            await bot.set_my_commands([], scope=BotCommandScopeAllGroupChats())
        except Exception:
            pass

    # --------------------------------------------------------------------------
    # SHARED SINGLE-POLLER DELIVERY HELPERS
    # --------------------------------------------------------------------------
    async def deliver_group_otp(self, item: Dict[str, Any], dest_ids: List[int]) -> Tuple[int, int]:
        """Delivers one OTP card to all linked groups using the OTP Child Bot & Group Format."""
        if not is_group_forwarding_enabled():
            return 0, 0
        otp_bot = self.get_bot("otp")
        if not otp_bot or not dest_ids:
            return 0, 0

        fmt = get_group_sms_format()
        msg_id = str(item.get("_resolved_mid", ""))
        card_text, otp_code, _ = format_otp_card(item, sms_format=fmt, masked=True)
        kb = build_otp_keyboard(otp_code, msg_id=msg_id, include_custom_buttons=True)

        results = await asyncio.gather(
            *[send_with_retry(otp_bot, gid, card_text, reply_markup=kb) for gid in dest_ids],
            return_exceptions=True,
        )
        ok = sum(1 for r in results if r is True)
        fail = len(results) - ok
        return ok, fail

    async def deliver_inbox_otp(self, item: Dict[str, Any], owner_id: int) -> bool:
        """Delivers one private OTP card to the claiming user via the Number Child Bot & Inbox Format."""
        if not is_inbox_otp_enabled() or not owner_id:
            return False
        if is_user_banned(owner_id):
            return False
        number_bot = self.get_bot("number")
        if not number_bot:
            return False

        fmt = get_inbox_sms_format()
        msg_id = str(item.get("_resolved_mid", ""))
        card_text, otp_code, _ = format_otp_card(item, sms_format=fmt, masked=False)
        kb = build_otp_keyboard(otp_code, msg_id=msg_id, include_custom_buttons=False)

        return await send_with_retry(number_bot, owner_id, card_text, reply_markup=kb)

    # --------------------------------------------------------------------------
    # CHILD BOT 1: OTP BOT (Zero Admin Access)
    # --------------------------------------------------------------------------
    def _build_otp_app(self, token: str) -> Application:
        app = Application.builder().token(token).build()
        app.add_handler(CommandHandler("start", self._otp_start_handler))
        app.add_handler(CallbackQueryHandler(self._otp_callback_handler))
        app.add_error_handler(self._child_error_handler)
        return app

    async def _otp_start_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_chat or update.effective_chat.type != ChatType.PRIVATE:
            return
        bot_name = html.escape(get_bot_display_name("otp"))
        buttons = [[InlineKeyboardButton(b["text"], url=b["url"])] for b in get_custom_link_buttons()]
        text = (
            f"📩 <b>{bot_name}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🟢 Instant SMS &amp; OTP forwarding service is active.\n\n"
            f"<i>All incoming verification codes are delivered directly to linked groups and channels.</i>"
        )
        await update.effective_message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons) if buttons else None,
            disable_web_page_preview=True,
        )

    async def _otp_callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if not query:
            return
        data = query.data or ""
        if data.startswith("sms_"):
            msg_key = data[4:]
            full_sms = get_cached_sms_message(msg_key)
            if not full_sms:
                await query.answer("⚠️ Full SMS expired from cache.", show_alert=True)
                return
            alert_text = full_sms[:197] + "..." if len(full_sms) > 200 else full_sms
            await query.answer(alert_text, show_alert=True)
            return

        if data.startswith("copy_num_"):
            await query.answer(f"📋 Code/Number: {data[9:]}", show_alert=True)
            return

        await query.answer()

    # --------------------------------------------------------------------------
    # CHILD BOT 2: NUMBER BOT (Public User Flows Only — Zero Admin Access)
    # --------------------------------------------------------------------------
    def _build_number_app(self, token: str) -> Application:
        app = Application.builder().token(token).build()
        app.add_handler(CommandHandler("start", self._num_start_handler))
        app.add_handler(CommandHandler("help", self._num_help_handler))
        app.add_handler(CallbackQueryHandler(self._num_callback_handler))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, self._num_text_handler))
        app.add_error_handler(self._child_error_handler)
        return app

    async def _ensure_allowed_user(self, update: Update) -> Optional[Any]:
        user = update.effective_user
        if not user:
            return None
        register_user(user.id, user.username or "", user.first_name or "")
        if is_user_banned(user.id):
            msg = "🚫 <b>Access Restricted.</b>\nYour account has been suspended by an administrator."
            if update.callback_query:
                await update.callback_query.answer("🚫 Account suspended.", show_alert=True)
            elif update.effective_message:
                await update.effective_message.reply_text(msg, parse_mode=ParseMode.HTML)
            return None
        return user

    async def _num_start_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_chat or update.effective_chat.type != ChatType.PRIVATE:
            return
        user = await self._ensure_allowed_user(update)
        if not user:
            return

        # Support deep-link ?start=c_<country_id>
        if context.args and len(context.args) == 1 and context.args[0].startswith("c_"):
            try:
                cid = int(context.args[0][2:])
                await self._assign_numbers_and_send(update.effective_message.reply_text, user.id, cid, context, is_secret=False)
                return
            except Exception:
                pass

        bot_name = html.escape(get_bot_display_name("number"))
        countries = get_all_countries_with_stock(only_active=True, secret_mode=False)
        total_avail = sum(c.get("available_std", 0) for c in countries)

        welcome = (
            f"👋 <b>Welcome to {bot_name}, {html.escape(user.first_name or 'User')}!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🌍 <b>Available Countries:</b> <code>{len(countries)}</code>\n"
            f"📦 <b>Available Stock:</b> <code>{total_avail}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 Choose a country below or use the keyboard:"
        )
        await update.effective_message.reply_text(
            welcome,
            parse_mode=ParseMode.HTML,
            reply_markup=get_user_reply_keyboard(user.id),
        )
        await update.effective_message.reply_text(
            get_select_country_text(secret_mode=False),
            parse_mode=ParseMode.HTML,
            reply_markup=get_countries_keyboard(page=0, secret_mode=False, for_user=True),
        )

    async def _num_help_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_chat or update.effective_chat.type != ChatType.PRIVATE:
            return
        user = await self._ensure_allowed_user(update)
        if not user:
            return
        await self._send_user_help(update.effective_message)

    async def _send_user_help(self, message: Any) -> None:
        text = (
            "ℹ️ <b>How to Use This Bot</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "1️⃣ Tap <b>⚡ Get Number</b> and pick a country.\n"
            "2️⃣ Copy your assigned number and use it in your target app.\n"
            "3️⃣ Wait here — your SMS / OTP code will arrive automatically in this chat.\n"
            "4️⃣ Need another number? Tap <b>🔄 Change Number</b>.\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "💡 <i>Tip: Use 👤 My Account to toggle + prefix preference or download your claimed numbers.</i>"
        )
        buttons = [[InlineKeyboardButton(b["text"], url=b["url"])] for b in get_custom_link_buttons()]
        buttons.append([InlineKeyboardButton("⚡ Get Number", callback_data="btn_get_number")])
        await message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
            disable_web_page_preview=True,
        )

    async def _build_account_view(self, user_id: int) -> Tuple[str, InlineKeyboardMarkup]:
        details = get_user_details(user_id) or {}
        pref_plus = get_user_plus_preference(user_id)
        has_secret = is_admin(user_id) or bool(details.get("has_secret_access"))
        plus_status = "✅ Include + Prefix" if pref_plus else "➖ Hide + Prefix"
        secret_status = "🔓 Unlocked" if has_secret else "🔒 Locked"
        limit, is_fixed = get_effective_quantity_for_user(user_id)
        recent = get_user_recent_claims(user_id, limit=8)

        recent_block = ""
        if recent:
            lines = [f"• <code>{_clean_number(r['phone'], pref_plus)}</code> ({html.escape(r['country'])})" for r in recent]
            recent_block = "\n\n🕘 <b>Recent Numbers:</b>\n" + "\n".join(lines)

        text = (
            f"👤 <b>My Account</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 <b>User ID:</b> <code>{user_id}</code>\n"
            f"📦 <b>Total Claimed:</b> <code>{details.get('numbers_consumed', 0)}</code>\n"
            f"🔢 <b>Numbers / Request:</b> <code>{limit}</code> ({'Fixed' if is_fixed else 'Default'})\n"
            f"🔒 <b>Secret Pool:</b> {secret_status}\n"
            f"➕ <b>Number Format:</b> {plus_status}"
            f"{recent_block}"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📥 Download My Numbers (.txt)", callback_data="btn_download_my_numbers")],
            [InlineKeyboardButton(f"🔄 Format: {'With +' if pref_plus else 'No +'}", callback_data="btn_toggle_plus_pref")],
            [
                InlineKeyboardButton("⚡ Get Number", callback_data="btn_get_number"),
                InlineKeyboardButton("🔄 Refresh", callback_data="btn_refresh_profile"),
            ],
        ])
        return text, kb

    async def _assign_numbers_and_send(
        self,
        send_fn: Any,
        user_id: int,
        cid: int,
        context: ContextTypes.DEFAULT_TYPE,
        is_secret: bool = False,
        edit_message: bool = False,
    ) -> None:
        if is_secret and not (is_admin(user_id) or user_has_secret_access(user_id)):
            return

        limit, is_fixed = get_effective_quantity_for_user(user_id)
        numbers, remaining, country_name = consume_numbers_for_user(
            country_id=cid,
            user_id=user_id,
            limit=limit,
            is_secret=is_secret,
        )
        if not numbers:
            empty_msg = "⚠️ <b>Out of Stock!</b> All available numbers in this pool have been claimed."
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🌍 Back to Countries", callback_data="btn_get_secret_number" if is_secret else "btn_get_number")]])
            await send_fn(text=empty_msg, parse_mode=ParseMode.HTML, reply_markup=kb)
            return

        if gist_storage.enabled:
            asyncio.create_task(gist_storage.export_and_sync(immediate=True))

        context.user_data[f"last_nums_{cid}_{1 if is_secret else 0}"] = numbers
        pref_plus = get_user_plus_preference(user_id)
        text = format_delivered_numbers(
            numbers,
            country_name,
            available_count=remaining,
            with_plus=pref_plus,
            is_secret=is_secret,
        )
        kb = get_numbers_view_keyboard(
            cid,
            is_secret=is_secret,
            with_plus=pref_plus,
            quantity=limit,
            is_fixed=is_fixed,
            numbers=numbers,
        )
        await send_fn(
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
            disable_web_page_preview=True,
        )

    async def _num_text_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user = await self._ensure_allowed_user(update)
        if not user or not update.effective_message:
            return
        msg_text = (update.effective_message.text or "").strip()

        if msg_text == "⚡ Get Number":
            await update.effective_message.reply_text(
                get_select_country_text(secret_mode=False),
                parse_mode=ParseMode.HTML,
                reply_markup=get_countries_keyboard(page=0, secret_mode=False, for_user=True),
            )
            return

        if msg_text == "🔒 Secret Numbers":
            if not (is_admin(user.id) or user_has_secret_access(user.id)):
                await update.effective_message.reply_text(
                    "🔒 <b>Secret Numbers Pool is Restricted.</b>\nContact support for VIP access.",
                    parse_mode=ParseMode.HTML,
                )
                return
            await update.effective_message.reply_text(
                get_select_country_text(secret_mode=True),
                parse_mode=ParseMode.HTML,
                reply_markup=get_countries_keyboard(page=0, secret_mode=True, for_user=True),
            )
            return

        if msg_text == "👤 My Account":
            text, kb = await self._build_account_view(user.id)
            await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
            return

        if msg_text == "ℹ️ Help":
            await self._send_user_help(update.effective_message)
            return

        # Default fallback: gently guide user without exposing anything internal
        await update.effective_message.reply_text(
            "👇 Please use the menu buttons below to request a number or view your account.",
            reply_markup=get_user_reply_keyboard(user.id),
        )

    async def _num_callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query: Optional[CallbackQuery] = update.callback_query
        if not query:
            return
        user = await self._ensure_allowed_user(update)
        if not user:
            return

        data = query.data or ""

        if data == "noop":
            await query.answer()
            return

        if data.startswith("sms_"):
            full_sms = get_cached_sms_message(data[4:])
            if not full_sms:
                await query.answer("⚠️ Full SMS expired from cache.", show_alert=True)
                return
            await query.answer(full_sms[:197] + "..." if len(full_sms) > 200 else full_sms, show_alert=True)
            return

        if data.startswith("copy_num_"):
            await query.answer(f"📋 Copied: {data[9:]}", show_alert=True)
            return

        if data == "btn_get_number":
            await query.answer()
            await _safe_edit(
                query,
                get_select_country_text(secret_mode=False),
                get_countries_keyboard(page=0, secret_mode=False, for_user=True),
            )
            return

        if data == "btn_get_secret_number":
            if not (is_admin(user.id) or user_has_secret_access(user.id)):
                await query.answer("🔒 Restricted to whitelisted users.", show_alert=True)
                return
            await query.answer()
            await _safe_edit(
                query,
                get_select_country_text(secret_mode=True),
                get_countries_keyboard(page=0, secret_mode=True, for_user=True),
            )
            return

        if data.startswith("page_") or data.startswith("sec_page_"):
            is_sec = data.startswith("sec_page_")
            if is_sec and not (is_admin(user.id) or user_has_secret_access(user.id)):
                await query.answer("🔒 Restricted.", show_alert=True)
                return
            page = int(data.split("_")[-1])
            await query.answer()
            await _safe_edit(
                query,
                get_select_country_text(secret_mode=is_sec),
                get_countries_keyboard(page=page, secret_mode=is_sec, for_user=True),
            )
            return

        if data.startswith("c_") or data.startswith("sec_c_") or data.startswith("change_num_") or data.startswith("sec_change_num_"):
            is_sec = data.startswith("sec_")
            if is_sec and not (is_admin(user.id) or user_has_secret_access(user.id)):
                await query.answer("🔒 Restricted to whitelisted users.", show_alert=True)
                return
            cid = int(data.split("_")[-1])
            await query.answer("⚡ Assigning number...")
            await self._assign_numbers_and_send(query.edit_message_text, user.id, cid, context, is_secret=is_sec, edit_message=True)
            return

        if data.startswith("bcast_getnum_"):
            cid = int(data.split("_")[2])
            limit, is_fixed = get_effective_quantity_for_user(user.id)
            numbers, remaining, country_name = consume_numbers_for_user(cid, user.id, limit=limit, is_secret=False)
            if not numbers:
                await query.answer("⚠️ Out of Stock! All numbers for this country were claimed.", show_alert=True)
                return
            await query.answer("⚡ Number delivered!")
            if gist_storage.enabled:
                asyncio.create_task(gist_storage.export_and_sync(immediate=True))
            context.user_data[f"last_nums_{cid}_0"] = numbers
            pref_plus = get_user_plus_preference(user.id)
            text = format_delivered_numbers(numbers, country_name, available_count=remaining, with_plus=pref_plus)
            kb = get_numbers_view_keyboard(cid, is_secret=False, with_plus=pref_plus, quantity=limit, is_fixed=is_fixed, numbers=numbers)
            await context.bot.send_message(
                chat_id=user.id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
                disable_web_page_preview=True,
            )
            return

        if data.startswith("toggle_plus_"):
            parts = data.split("_")
            cid = int(parts[2])
            is_sec = bool(int(parts[3]))
            new_val = bool(int(parts[4]))
            set_user_plus_preference(user.id, new_val)
            if gist_storage.enabled:
                asyncio.create_task(gist_storage.export_and_sync(immediate=True))
            await query.answer(f"➕ Prefix {'enabled (+)' if new_val else 'hidden'}!")
            c_info = get_country_by_id(cid) or {"name": "Selected Country"}
            show_nums = context.user_data.get(f"last_nums_{cid}_{1 if is_sec else 0}")
            limit, is_fixed = get_effective_quantity_for_user(user.id)
            if not show_nums:
                claims = get_user_recent_claims(user.id, limit=25)
                matching = [r["phone"] for r in claims if r.get("country") == c_info["name"]]
                show_nums = matching[:limit] if matching else [r["phone"] for r in claims[:limit]]
            text = format_delivered_numbers(show_nums or ["—"], c_info["name"], with_plus=new_val, is_secret=is_sec)
            kb = get_numbers_view_keyboard(cid, is_secret=is_sec, with_plus=new_val, quantity=limit, is_fixed=is_fixed, numbers=show_nums)
            await _safe_edit(query, text, kb)
            return

        if data in ("btn_my_account", "btn_refresh_profile", "btn_refresh_history"):
            await query.answer()
            text, kb = await self._build_account_view(user.id)
            await _safe_edit(query, text, kb)
            return

        if data == "btn_toggle_plus_pref":
            new_val = not get_user_plus_preference(user.id)
            set_user_plus_preference(user.id, new_val)
            if gist_storage.enabled:
                asyncio.create_task(gist_storage.export_and_sync(immediate=True))
            await query.answer(f"Format set to: {'With +' if new_val else 'No +'}")
            text, kb = await self._build_account_view(user.id)
            await _safe_edit(query, text, kb)
            return

        if data == "btn_download_my_numbers":
            pref_plus = get_user_plus_preference(user.id)
            recent = get_user_recent_claims(user.id, limit=500)
            if not recent:
                await query.answer("📭 You haven't claimed any numbers yet.", show_alert=True)
                return
            await query.answer("📥 Preparing your numbers file...")
            lines = [f"{_clean_number(r['phone'], pref_plus)} | {r['country']} | {r['claimed_at']}" for r in recent]
            bio = io.BytesIO("\n".join(lines).encode("utf-8"))
            bio.name = f"my_numbers_{user.id}.txt"
            await context.bot.send_document(
                chat_id=user.id,
                document=bio,
                caption=f"📥 <b>Your Recent Claimed Numbers ({len(recent)})</b>",
                parse_mode=ParseMode.HTML,
            )
            return

        if data == "btn_help":
            await query.answer()
            await self._send_user_help(query.message)
            return

        await query.answer()

    async def _child_error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        err = context.error
        if isinstance(err, Conflict):
            logger.warning("⚠️ Child bot polling conflict (another instance or handover in progress).")
            return
        if isinstance(err, (NetworkError, TimedOut)):
            logger.debug(f"Transient child bot network notice: {err}")
            return
        logger.warning(f"⚠️ Child bot error: {err}")


child_manager = ChildBotManager()
