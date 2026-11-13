"""
Core shared state, SQLite storage, Thirdwave API client, formatters, and Gist cloud sync
for the unified MOTHER_BOT control plane.
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import logging
import os
import re
import sqlite3
import sys
import textwrap
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx
from telegram import Bot, CopyTextButton, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import BadRequest, NetworkError, RetryAfter, TimedOut

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

logger = logging.getLogger("MOTHER_BOT")
BOT_PROCESS_START_TIME: float = time.time()


def get_bangladesh_now() -> datetime:
    return datetime.now(timezone(timedelta(hours=6)))


def format_bangladesh_12h(dt: Optional[datetime] = None) -> Tuple[str, str]:
    bd_dt = dt if dt is not None else get_bangladesh_now()
    return bd_dt.strftime("%d %b %Y"), bd_dt.strftime("%I:%M:%S %p")


def get_current_commit() -> str:
    env_sha = os.getenv("GITHUB_SHA", "").strip()
    if env_sha:
        return env_sha[:7]
    try:
        import subprocess
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stderr=subprocess.DEVNULL,
        )
        return out.decode().strip() or "local"
    except Exception:
        return "local"


# ==========================================
# Environment Configuration
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.getenv("DB_FILE", "").strip() or os.path.join(BASE_DIR, "mother_bot.db")
STOCKS_DIR = os.path.join(BASE_DIR, "country_stocks")
DATA_FILE = os.path.join(BASE_DIR, "bot_data.json")
os.makedirs(STOCKS_DIR, exist_ok=True)


def load_environment() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(BASE_DIR, ".env"))
    except Exception:
        pass
    env_path = os.path.join(BASE_DIR, ".env")
    if os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8", errors="ignore") as f:
                for raw in f:
                    line = raw.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip().strip("\"'").strip()
                    if key and key not in os.environ:
                        os.environ[key] = val
        except Exception:
            pass


load_environment()

MOTHER_BOT_TOKEN = os.getenv("MOTHER_BOT_TOKEN", os.getenv("TELEGRAM_BOT_TOKEN", "")).strip()
ENV_OTP_BOT_TOKEN = os.getenv("OTP_BOT_TOKEN", "").strip()
ENV_NUMBER_BOT_TOKEN = os.getenv("NUMBER_BOT_TOKEN", "").strip()
STARTUP_TYPE = os.getenv("STARTUP_TYPE", "workflow_dispatch").strip().lower()

_admin_raw = os.getenv("ADMIN_USER_IDS", "").strip()
ADMIN_USER_IDS: List[int] = [int(u.strip()) for u in _admin_raw.split(",") if u.strip().isdigit()]

THIRDWAVE_API_KEY = os.getenv("THIRDWAVE_API_KEY", "").strip()
_raw_tw_base = os.getenv("THIRDWAVE_BASE_URL", "https://thirdwave.im").strip().rstrip("/")
if _raw_tw_base.endswith("/api/v1/traffic"):
    _raw_tw_base = _raw_tw_base[:-len("/api/v1/traffic")].rstrip("/")
if _raw_tw_base and not _raw_tw_base.startswith(("http://", "https://")):
    _raw_tw_base = f"https://{_raw_tw_base}"
THIRDWAVE_BASE_URL = _raw_tw_base or "https://thirdwave.im"

TELEGRAM_GROUP_CHAT_ID = os.getenv("TELEGRAM_GROUP_CHAT_ID", "").strip()
SECONDARY_GROUP_CHAT_ID = os.getenv(
    "SECONDARY_GROUP_CHAT_ID",
    os.getenv("TELEGRAM_SECONDARY_GROUP_CHAT_ID", ""),
).strip()
try:
    POLL_INTERVAL_SECONDS = max(6.0, float(os.getenv("POLL_INTERVAL_SECONDS", "").strip() or "7.5"))
except (ValueError, TypeError):
    POLL_INTERVAL_SECONDS = 7.5

MAX_FORWARDING_GROUPS = 10


def _resolve_gist_tokens() -> List[str]:
    candidates: List[str] = []
    for key in ("GIST_TOKEN", "PAT", "GH_TOKEN", "GITHUB_TOKEN"):
        val = os.getenv(key, "").strip()
        if val and val.lower() not in ("true", "false", "null", "none") and val not in candidates:
            candidates.append(val)
    pats = [t for t in candidates if not t.startswith("ghs_")]
    ephem = [t for t in candidates if t.startswith("ghs_")]
    return pats + ephem


_GIST_TOKEN_CANDIDATES = _resolve_gist_tokens()
GIST_ID = os.getenv("GIST_ID", os.getenv("GITHUB_GIST_ID", "")).strip()
GIST_TOKEN = _GIST_TOKEN_CANDIDATES[0] if _GIST_TOKEN_CANDIDATES else ""


# ==========================================
# Background Task Registry
# ==========================================
_BACKGROUND_TASKS: Set[asyncio.Task] = set()


def create_safe_task(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_BACKGROUND_TASKS.discard)
    return task


# ==========================================
# Shared Runtime Caches & Dedup State
# ==========================================
CONSUMED_NUMBERS_CACHE: Set[str] = set()
CLAIMED_NUMBER_OWNER_CACHE: Dict[str, Tuple[int, int, float]] = {}
_INITIALIZED_COUNTRY_DBS: Set[int] = set()
_SMS_MESSAGE_CACHE: Dict[str, Tuple[str, float]] = {}

seen_message_ids: Set[str] = set()
seen_timestamps: Dict[str, float] = {}
seen_content_signatures: Dict[str, float] = {}

_COUNTRIES_STOCK_CACHE_MAP: Dict[Tuple[bool, bool, bool], Tuple[float, List[Dict[str, Any]]]] = {}
_COUNTRIES_STOCK_CACHE_TTL: float = 3.0


# ==========================================
# Country / Phone / Flag Lookup Tables
# ==========================================
COUNTRY_ISO_DATA: Dict[str, Tuple[str, str]] = {
    "sri lanka": ("🇱🇰", "LK"), "lk": ("🇱🇰", "LK"),
    "indonesia": ("🇮🇩", "ID"), "id": ("🇮🇩", "ID"),
    "india": ("🇮🇳", "IN"), "in": ("🇮🇳", "IN"),
    "bangladesh": ("🇧🇩", "BD"), "bd": ("🇧🇩", "BD"),
    "pakistan": ("🇵🇰", "PK"), "pk": ("🇵🇰", "PK"),
    "vietnam": ("🇻🇳", "VN"), "vn": ("🇻🇳", "VN"),
    "philippines": ("🇵🇭", "PH"), "ph": ("🇵🇭", "PH"),
    "thailand": ("🇹🇭", "TH"), "th": ("🇹🇭", "TH"),
    "malaysia": ("🇲🇾", "MY"), "my": ("🇲🇾", "MY"),
    "cambodia": ("🇰🇭", "KH"), "kh": ("🇰🇭", "KH"),
    "myanmar": ("🇲🇲", "MM"), "mm": ("🇲🇲", "MM"),
    "nepal": ("🇳🇵", "NP"), "np": ("🇳🇵", "NP"),
    "china": ("🇨🇳", "CN"), "cn": ("🇨🇳", "CN"),
    "taiwan": ("🇹🇼", "TW"), "tw": ("🇹🇼", "TW"),
    "japan": ("🇯🇵", "JP"), "jp": ("🇯🇵", "JP"),
    "south korea": ("🇰🇷", "KR"), "kr": ("🇰🇷", "KR"),
    "singapore": ("🇸🇬", "SG"), "sg": ("🇸🇬", "SG"),
    "hong kong": ("🇭🇰", "HK"), "hk": ("🇭🇰", "HK"),
    "russia": ("🇷🇺", "RU"), "ru": ("🇷🇺", "RU"),
    "ukraine": ("🇺🇦", "UA"), "ua": ("🇺🇦", "UA"),
    "turkey": ("🇹🇷", "TR"), "tr": ("🇹🇷", "TR"),
    "saudi arabia": ("🇸🇦", "SA"), "sa": ("🇸🇦", "SA"),
    "uae": ("🇦🇪", "AE"), "ae": ("🇦🇪", "AE"), "united arab emirates": ("🇦🇪", "AE"),
    "iran": ("🇮🇷", "IR"), "ir": ("🇮🇷", "IR"),
    "iraq": ("🇮🇶", "IQ"), "iq": ("🇮🇶", "IQ"),
    "israel": ("🇮🇱", "IL"), "il": ("🇮🇱", "IL"),
    "jordan": ("🇯🇴", "JO"), "jo": ("🇯🇴", "JO"),
    "kuwait": ("🇰🇼", "KW"), "kw": ("🇰🇼", "KW"),
    "lebanon": ("🇱🇧", "LB"), "lb": ("🇱🇧", "LB"),
    "oman": ("🇴🇲", "OM"), "om": ("🇴🇲", "OM"),
    "qatar": ("🇶🇦", "QA"), "qa": ("🇶🇦", "QA"),
    "syria": ("🇸🇾", "SY"), "sy": ("🇸🇾", "SY"),
    "yemen": ("🇾🇪", "YE"), "ye": ("🇾🇪", "YE"),
    "bahrain": ("🇧🇭", "BH"), "bh": ("🇧🇭", "BH"),
    "uzbekistan": ("🇺🇿", "UZ"), "uz": ("🇺🇿", "UZ"),
    "kazakhstan": ("🇰🇿", "KZ"), "kz": ("🇰🇿", "KZ"),
    "kyrgyzstan": ("🇰🇬", "KG"), "kg": ("🇰🇬", "KG"),
    "tajikistan": ("🇹🇯", "TJ"), "tj": ("🇹🇯", "TJ"),
    "turkmenistan": ("🇹🇲", "TM"), "tm": ("🇹🇲", "TM"),
    "afghanistan": ("🇦🇫", "AF"), "af": ("🇦🇫", "AF"),
    "azerbaijan": ("🇦🇿", "AZ"), "az": ("🇦🇿", "AZ"),
    "armenia": ("🇦🇲", "AM"), "am": ("🇦🇲", "AM"),
    "georgia": ("🇬🇪", "GE"), "ge": ("🇬🇪", "GE"),
    "laos": ("🇱🇦", "LA"), "la": ("🇱🇦", "LA"),
    "bhutan": ("🇧🇹", "BT"), "bt": ("🇧🇹", "BT"),
    "maldives": ("🇲🇻", "MV"), "mv": ("🇲🇻", "MV"),
    "ivory coast": ("🇨🇮", "CI"), "cote d'ivoire": ("🇨🇮", "CI"), "ci": ("🇨🇮", "CI"),
    "egypt": ("🇪🇬", "EG"), "eg": ("🇪🇬", "EG"),
    "nigeria": ("🇳🇬", "NG"), "ng": ("🇳🇬", "NG"),
    "kenya": ("🇰🇪", "KE"), "ke": ("🇰🇪", "KE"),
    "ghana": ("🇬🇭", "GH"), "gh": ("🇬🇭", "GH"),
    "south africa": ("🇿🇦", "ZA"), "za": ("🇿🇦", "ZA"),
    "morocco": ("🇲🇦", "MA"), "ma": ("🇲🇦", "MA"),
    "algeria": ("🇩🇿", "DZ"), "dz": ("🇩🇿", "DZ"),
    "tunisia": ("🇹🇳", "TN"), "tn": ("🇹🇳", "TN"),
    "libya": ("🇱🇾", "LY"), "ly": ("🇱🇾", "LY"),
    "tanzania": ("🇹🇿", "TZ"), "tz": ("🇹🇿", "TZ"),
    "uganda": ("🇺🇬", "UG"), "ug": ("🇺🇬", "UG"),
    "ethiopia": ("🇪🇹", "ET"), "et": ("🇪🇹", "ET"),
    "somalia": ("🇸🇴", "SO"), "so": ("🇸🇴", "SO"),
    "sudan": ("🇸🇩", "SD"), "sd": ("🇸🇩", "SD"),
    "senegal": ("🇸🇳", "SN"), "sn": ("🇸🇳", "SN"),
    "cameroon": ("🇨🇲", "CM"), "cm": ("🇨🇲", "CM"),
    "rwanda": ("🇷🇼", "RW"), "rw": ("🇷🇼", "RW"),
    "zambia": ("🇿🇲", "ZM"), "zm": ("🇿🇲", "ZM"),
    "zimbabwe": ("🇿🇼", "ZW"), "zw": ("🇿🇼", "ZW"),
    "mozambique": ("🇲🇿", "MZ"), "mz": ("🇲🇿", "MZ"),
    "angola": ("🇦🇴", "AO"), "ao": ("🇦🇴", "AO"),
    "mali": ("🇲🇱", "ML"), "ml": ("🇲🇱", "ML"),
    "burkina faso": ("🇧🇫", "BF"), "bf": ("🇧🇫", "BF"),
    "guinea": ("🇬🇳", "GN"), "gn": ("🇬🇳", "GN"),
    "benin": ("🇧🇯", "BJ"), "bj": ("🇧🇯", "BJ"),
    "togo": ("🇹🇬", "TG"), "tg": ("🇹🇬", "TG"),
    "madagascar": ("🇲🇬", "MG"), "mg": ("🇲🇬", "MG"),
    "mauritius": ("🇲🇺", "MU"), "mu": ("🇲🇺", "MU"),
    "botswana": ("🇧🇼", "BW"), "bw": ("🇧🇼", "BW"),
    "namibia": ("🇳🇦", "NA"), "na": ("🇳🇦", "NA"),
    "kosovo": ("🇽🇰", "XK"), "xk": ("🇽🇰", "XK"),
    "united kingdom": ("🇬🇧", "GB"), "uk": ("🇬🇧", "GB"), "gb": ("🇬🇧", "GB"),
    "germany": ("🇩🇪", "DE"), "de": ("🇩🇪", "DE"),
    "france": ("🇫🇷", "FR"), "fr": ("🇫🇷", "FR"),
    "italy": ("🇮🇹", "IT"), "it": ("🇮🇹", "IT"),
    "spain": ("🇪🇸", "ES"), "es": ("🇪🇸", "ES"),
    "poland": ("🇵🇱", "PL"), "pl": ("🇵🇱", "PL"),
    "netherlands": ("🇳🇱", "NL"), "nl": ("🇳🇱", "NL"),
    "portugal": ("🇵🇹", "PT"), "pt": ("🇵🇹", "PT"),
    "sweden": ("🇸🇪", "SE"), "se": ("🇸🇪", "SE"),
    "norway": ("🇳🇴", "NO"), "no": ("🇳🇴", "NO"),
    "denmark": ("🇩🇰", "DK"), "dk": ("🇩🇰", "DK"),
    "finland": ("🇫🇮", "FI"), "fi": ("🇫🇮", "FI"),
    "switzerland": ("🇨🇭", "CH"), "ch": ("🇨🇭", "CH"),
    "austria": ("🇦🇹", "AT"), "at": ("🇦🇹", "AT"),
    "belgium": ("🇧🇪", "BE"), "be": ("🇧🇪", "BE"),
    "greece": ("🇬🇷", "GR"), "gr": ("🇬🇷", "GR"),
    "ireland": ("🇮🇪", "IE"), "ie": ("🇮🇪", "IE"),
    "czech": ("🇨🇿", "CZ"), "cz": ("🇨🇿", "CZ"), "czech republic": ("🇨🇿", "CZ"),
    "romania": ("🇷🇴", "RO"), "ro": ("🇷🇴", "RO"),
    "hungary": ("🇭🇺", "HU"), "hu": ("🇭🇺", "HU"),
    "albania": ("🇦🇱", "AL"), "al": ("🇦🇱", "AL"),
    "serbia": ("🇷🇸", "RS"), "rs": ("🇷🇸", "RS"),
    "croatia": ("🇭🇷", "HR"), "hr": ("🇭🇷", "HR"),
    "bulgaria": ("🇧🇬", "BG"), "bg": ("🇧🇬", "BG"),
    "slovakia": ("🇸🇰", "SK"), "sk": ("🇸🇰", "SK"),
    "slovenia": ("🇸🇮", "SI"), "si": ("🇸🇮", "SI"),
    "estonia": ("🇪🇪", "EE"), "ee": ("🇪🇪", "EE"),
    "latvia": ("🇱🇻", "LV"), "lv": ("🇱🇻", "LV"),
    "lithuania": ("🇱🇹", "LT"), "lt": ("🇱🇹", "LT"),
    "belarus": ("🇧🇾", "BY"), "by": ("🇧🇾", "BY"),
    "moldova": ("🇲🇩", "MD"), "md": ("🇲🇩", "MD"),
    "bosnia": ("🇧🇦", "BA"), "ba": ("🇧🇦", "BA"),
    "north macedonia": ("🇲🇰", "MK"), "mk": ("🇲🇰", "MK"),
    "cyprus": ("🇨🇾", "CY"), "cy": ("🇨🇾", "CY"),
    "malta": ("🇲🇹", "MT"), "mt": ("🇲🇹", "MT"),
    "iceland": ("🇮🇸", "IS"), "is": ("🇮🇸", "IS"),
    "luxembourg": ("🇱🇺", "LU"), "lu": ("🇱🇺", "LU"),
    "united states": ("🇺🇸", "US"), "us": ("🇺🇸", "US"), "usa": ("🇺🇸", "US"),
    "canada": ("🇨🇦", "CA"), "ca": ("🇨🇦", "CA"),
    "brazil": ("🇧🇷", "BR"), "br": ("🇧🇷", "BR"),
    "mexico": ("🇲🇽", "MX"), "mx": ("🇲🇽", "MX"),
    "colombia": ("🇨🇴", "CO"), "co": ("🇨🇴", "CO"),
    "argentina": ("🇦🇷", "AR"), "ar": ("🇦🇷", "AR"),
    "peru": ("🇵🇪", "PE"), "pe": ("🇵🇪", "PE"),
    "chile": ("🇨🇱", "CL"), "cl": ("🇨🇱", "CL"),
    "venezuela": ("🇻🇪", "VE"), "ve": ("🇻🇪", "VE"),
    "ecuador": ("🇪🇨", "EC"), "ec": ("🇪🇨", "EC"),
    "guatemala": ("🇬🇹", "GT"), "gt": ("🇬🇹", "GT"),
    "cuba": ("🇨🇺", "CU"), "cu": ("🇨🇺", "CU"),
    "bolivia": ("🇧🇴", "BO"), "bo": ("🇧🇴", "BO"),
    "dominican republic": ("🇩🇴", "DO"), "do": ("🇩🇴", "DO"),
    "honduras": ("🇭🇳", "HN"), "hn": ("🇭🇳", "HN"),
    "paraguay": ("🇵🇾", "PY"), "py": ("🇵🇾", "PY"),
    "el salvador": ("🇸🇻", "SV"), "sv": ("🇸🇻", "SV"),
    "nicaragua": ("🇳🇮", "NI"), "ni": ("🇳🇮", "NI"),
    "costa rica": ("🇨🇷", "CR"), "cr": ("🇨🇷", "CR"),
    "panama": ("🇵🇦", "PA"), "pa": ("🇵🇦", "PA"),
    "uruguay": ("🇺🇾", "UY"), "uy": ("🇺🇾", "UY"),
    "jamaica": ("🇯🇲", "JM"), "jm": ("🇯🇲", "JM"),
    "haiti": ("🇭🇹", "HT"), "ht": ("🇭🇹", "HT"),
    "australia": ("🇦🇺", "AU"), "au": ("🇦🇺", "AU"),
    "new zealand": ("🇳🇿", "NZ"), "nz": ("🇳🇿", "NZ"),
    "fiji": ("🇫🇯", "FJ"), "fj": ("🇫🇯", "FJ"),
    "papua new guinea": ("🇵🇬", "PG"), "pg": ("🇵🇬", "PG"),
    "celtel": ("🇱🇰", "LK"),
}

PREFIX_ISO_MAP: Dict[str, Tuple[str, str]] = {
    "1": ("🇺🇸", "US"), "7": ("🇷🇺", "RU"),
    "20": ("🇪🇬", "EG"), "27": ("🇿🇦", "ZA"), "30": ("🇬🇷", "GR"),
    "31": ("🇳🇱", "NL"), "32": ("🇧🇪", "BE"), "33": ("🇫🇷", "FR"),
    "34": ("🇪🇸", "ES"), "36": ("🇭🇺", "HU"), "39": ("🇮🇹", "IT"),
    "40": ("🇷🇴", "RO"), "41": ("🇨🇭", "CH"), "43": ("🇦🇹", "AT"),
    "44": ("🇬🇧", "GB"), "45": ("🇩🇰", "DK"), "46": ("🇸🇪", "SE"),
    "47": ("🇳🇴", "NO"), "48": ("🇵🇱", "PL"), "49": ("🇩🇪", "DE"),
    "51": ("🇵🇪", "PE"), "52": ("🇲🇽", "MX"), "53": ("🇨🇺", "CU"),
    "54": ("🇦🇷", "AR"), "55": ("🇧🇷", "BR"), "56": ("🇨🇱", "CL"),
    "57": ("🇨🇴", "CO"), "58": ("🇻🇪", "VE"), "60": ("🇲🇾", "MY"),
    "61": ("🇦🇺", "AU"), "62": ("🇮🇩", "ID"), "63": ("🇵🇭", "PH"),
    "64": ("🇳🇿", "NZ"), "65": ("🇸🇬", "SG"), "66": ("🇹🇭", "TH"),
    "81": ("🇯🇵", "JP"), "82": ("🇰🇷", "KR"), "84": ("🇻🇳", "VN"),
    "86": ("🇨🇳", "CN"), "90": ("🇹🇷", "TR"), "91": ("🇮🇳", "IN"),
    "92": ("🇵🇰", "PK"), "93": ("🇦🇫", "AF"), "94": ("🇱🇰", "LK"),
    "95": ("🇲🇲", "MM"), "98": ("🇮🇷", "IR"),
    "212": ("🇲🇦", "MA"), "213": ("🇩🇿", "DZ"), "216": ("🇹🇳", "TN"),
    "218": ("🇱🇾", "LY"), "220": ("🇬🇲", "GM"), "221": ("🇸🇳", "SN"),
    "222": ("🇲🇷", "MR"), "223": ("🇲🇱", "ML"), "224": ("🇬🇳", "GN"),
    "225": ("🇨🇮", "CI"), "226": ("🇧🇫", "BF"), "227": ("🇳🇪", "NE"),
    "228": ("🇹🇬", "TG"), "229": ("🇧🇯", "BJ"), "230": ("🇲🇺", "MU"),
    "231": ("🇱🇷", "LR"), "232": ("🇸🇱", "SL"), "233": ("🇬🇭", "GH"),
    "234": ("🇳🇬", "NG"), "235": ("🇹🇩", "TD"), "236": ("🇨🇫", "CF"),
    "237": ("🇨🇲", "CM"), "238": ("🇨🇻", "CV"), "239": ("🇸🇹", "ST"),
    "240": ("🇬🇶", "GQ"), "241": ("🇬🇦", "GA"), "242": ("🇨🇬", "CG"),
    "243": ("🇨🇩", "CD"), "244": ("🇦🇴", "AO"), "245": ("🇬🇼", "GW"),
    "248": ("🇸🇨", "SC"), "249": ("🇸🇩", "SD"), "250": ("🇷🇼", "RW"),
    "251": ("🇪🇹", "ET"), "252": ("🇸🇴", "SO"), "253": ("🇩🇯", "DJ"),
    "254": ("🇰🇪", "KE"), "255": ("🇹🇿", "TZ"), "256": ("🇺🇬", "UG"),
    "257": ("🇧🇮", "BI"), "258": ("🇲🇿", "MZ"), "260": ("🇿🇲", "ZM"),
    "261": ("🇲🇬", "MG"), "263": ("🇿🇼", "ZW"), "264": ("🇳🇦", "NA"),
    "265": ("🇲🇼", "MW"), "266": ("🇱🇸", "LS"), "267": ("🇧🇼", "BW"),
    "268": ("🇸🇿", "SZ"),
    "350": ("🇬🇮", "GI"), "351": ("🇵🇹", "PT"), "352": ("🇱🇺", "LU"),
    "353": ("🇮🇪", "IE"), "354": ("🇮🇸", "IS"), "355": ("🇦🇱", "AL"),
    "356": ("🇲🇹", "MT"), "357": ("🇨🇾", "CY"), "358": ("🇫🇮", "FI"),
    "359": ("🇧🇬", "BG"), "370": ("🇱🇹", "LT"), "371": ("🇱🇻", "LV"),
    "372": ("🇪🇪", "EE"), "373": ("🇲🇩", "MD"), "374": ("🇦🇲", "AM"),
    "375": ("🇧🇾", "BY"), "376": ("🇦🇩", "AD"), "377": ("🇲🇨", "MC"),
    "380": ("🇺🇦", "UA"), "381": ("🇷🇸", "RS"), "382": ("🇲🇪", "ME"),
    "383": ("🇽🇰", "XK"), "385": ("🇭🇷", "HR"), "386": ("🇸🇮", "SI"),
    "387": ("🇧🇦", "BA"), "389": ("🇲🇰", "MK"), "420": ("🇨🇿", "CZ"),
    "421": ("🇸🇰", "SK"), "852": ("🇭🇰", "HK"), "853": ("🇲🇴", "MO"),
    "855": ("🇰🇭", "KH"), "856": ("🇱🇦", "LA"), "880": ("🇧🇩", "BD"),
    "886": ("🇹🇼", "TW"), "960": ("🇲🇻", "MV"), "961": ("🇱🇧", "LB"),
    "962": ("🇯🇴", "JO"), "963": ("🇸🇾", "SY"), "964": ("🇮🇶", "IQ"),
    "965": ("🇰🇼", "KW"), "966": ("🇸🇦", "SA"), "967": ("🇾🇪", "YE"),
    "968": ("🇴🇲", "OM"), "970": ("🇵🇸", "PS"), "971": ("🇦🇪", "AE"),
    "972": ("🇮🇱", "IL"), "973": ("🇧🇭", "BH"), "974": ("🇶🇦", "QA"),
    "975": ("🇧🇹", "BT"), "976": ("🇲🇳", "MN"), "977": ("🇳🇵", "NP"),
    "992": ("🇹🇯", "TJ"), "993": ("🇹🇲", "TM"), "994": ("🇦🇿", "AZ"),
    "995": ("🇬🇪", "GE"), "996": ("🇰🇬", "KG"), "998": ("🇺🇿", "UZ"),
    "501": ("🇧🇿", "BZ"), "502": ("🇬🇹", "GT"), "503": ("🇸🇻", "SV"),
    "504": ("🇭🇳", "HN"), "505": ("🇳🇮", "NI"), "506": ("🇨🇷", "CR"),
    "507": ("🇵🇦", "PA"), "509": ("🇭🇹", "HT"), "591": ("🇧🇴", "BO"),
    "592": ("🇬🇾", "GY"), "593": ("🇪🇨", "EC"), "595": ("🇵🇾", "PY"),
    "597": ("🇸🇷", "SR"), "598": ("🇺🇾", "UY"), "675": ("🇵🇬", "PG"),
    "679": ("🇫🇯", "FJ"),
}

ISO_TO_COUNTRY_NAME: Dict[str, str] = {
    "AD": "Andorra", "AE": "UAE", "AF": "Afghanistan", "AL": "Albania",
    "AM": "Armenia", "AO": "Angola", "AR": "Argentina", "AT": "Austria", "AU": "Australia",
    "AZ": "Azerbaijan", "BA": "Bosnia", "BD": "Bangladesh", "BE": "Belgium",
    "BF": "Burkina Faso", "BG": "Bulgaria", "BH": "Bahrain", "BI": "Burundi", "BJ": "Benin",
    "BO": "Bolivia", "BR": "Brazil", "BT": "Bhutan", "BW": "Botswana", "BY": "Belarus",
    "BZ": "Belize", "CA": "Canada", "CD": "DR Congo", "CF": "Cent. Africa",
    "CG": "Congo", "CH": "Switzerland", "CI": "Ivory Coast", "CL": "Chile", "CM": "Cameroon",
    "CN": "China", "CO": "Colombia", "CR": "Costa Rica", "CU": "Cuba", "CV": "Cape Verde",
    "CY": "Cyprus", "CZ": "Czechia", "DE": "Germany", "DJ": "Djibouti", "DK": "Denmark",
    "DO": "Dominican R.", "DZ": "Algeria", "EC": "Ecuador", "EE": "Estonia", "EG": "Egypt",
    "ES": "Spain", "ET": "Ethiopia", "FI": "Finland", "FJ": "Fiji", "FR": "France",
    "GA": "Gabon", "GB": "UK", "GE": "Georgia", "GH": "Ghana", "GI": "Gibraltar",
    "GM": "Gambia", "GN": "Guinea", "GQ": "Eq. Guinea", "GR": "Greece", "GT": "Guatemala",
    "GW": "Guinea-B.", "GY": "Guyana", "HK": "Hong Kong", "HN": "Honduras", "HR": "Croatia",
    "HT": "Haiti", "HU": "Hungary", "ID": "Indonesia", "IE": "Ireland", "IL": "Israel",
    "IN": "India", "IQ": "Iraq", "IR": "Iran", "IS": "Iceland", "IT": "Italy",
    "JM": "Jamaica", "JO": "Jordan", "JP": "Japan", "KE": "Kenya", "KG": "Kyrgyzstan",
    "KH": "Cambodia", "KR": "South Korea", "KW": "Kuwait", "KZ": "Kazakhstan", "LA": "Laos",
    "LB": "Lebanon", "LK": "Sri Lanka", "LR": "Liberia", "LS": "Lesotho", "LT": "Lithuania",
    "LU": "Luxembourg", "LV": "Latvia", "LY": "Libya", "MA": "Morocco", "MC": "Monaco",
    "MD": "Moldova", "ME": "Montenegro", "MG": "Madagascar", "MK": "N. Macedonia", "ML": "Mali",
    "MM": "Myanmar", "MN": "Mongolia", "MO": "Macau", "MR": "Mauritania", "MT": "Malta",
    "MU": "Mauritius", "MV": "Maldives", "MW": "Malawi", "MX": "Mexico", "MY": "Malaysia",
    "MZ": "Mozambique", "NA": "Namibia", "NE": "Niger", "NG": "Nigeria", "NI": "Nicaragua",
    "NL": "Netherlands", "NO": "Norway", "NP": "Nepal", "NZ": "New Zealand", "OM": "Oman",
    "PA": "Panama", "PE": "Peru", "PG": "Papua NG", "PH": "Philippines", "PK": "Pakistan",
    "PL": "Poland", "PS": "Palestine", "PT": "Portugal", "PY": "Paraguay", "QA": "Qatar",
    "RO": "Romania", "RS": "Serbia", "RU": "Russia", "RW": "Rwanda", "SA": "Saudi Arabia",
    "SC": "Seychelles", "SD": "Sudan", "SE": "Sweden", "SG": "Singapore", "SI": "Slovenia",
    "SK": "Slovakia", "SL": "Sierra Leone", "SN": "Senegal", "SO": "Somalia", "SR": "Suriname",
    "ST": "Sao Tome", "SV": "El Salvador", "SY": "Syria", "SZ": "Eswatini", "TD": "Chad",
    "TG": "Togo", "TH": "Thailand", "TJ": "Tajikistan", "TL": "Timor-Leste", "TM": "Turkmenistan",
    "TN": "Tunisia", "TO": "Tonga", "TR": "Turkey", "TT": "Trinidad", "TW": "Taiwan",
    "TZ": "Tanzania", "UA": "Ukraine", "UG": "Uganda", "US": "USA", "UY": "Uruguay",
    "UZ": "Uzbekistan", "VC": "St. Vincent", "VE": "Venezuela", "VN": "Vietnam", "VU": "Vanuatu",
    "WS": "Samoa", "XK": "Kosovo", "YE": "Yemen", "ZA": "South Africa", "ZM": "Zambia", "ZW": "Zimbabwe",
}


def extract_flag_emoji(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"[\U0001F1E6-\U0001F1FF]{2}", text)
    return m.group(0) if m else None


def get_country_flag(name: str) -> Optional[str]:
    low = (name or "").strip().lower()
    if not low:
        return None
    cleaned = re.sub(r"[\U0001F1E6-\U0001F1FF]{2}", "", low).strip()
    if cleaned in COUNTRY_ISO_DATA:
        return COUNTRY_ISO_DATA[cleaned][0]
    for key, (flag, _) in COUNTRY_ISO_DATA.items():
        if len(key) >= 3 and key in cleaned:
            return flag
    return None


def format_country_name(name: str, fallback_country: str = "") -> str:
    cleaned = (name or "").strip()
    if not cleaned:
        return cleaned
    if extract_flag_emoji(cleaned):
        return cleaned
    flag = get_country_flag(cleaned)
    if not flag and fallback_country:
        flag = extract_flag_emoji(fallback_country) or get_country_flag(fallback_country)
    return f"{flag} {cleaned}" if flag else cleaned


def sanitize_phone_number(raw_num: str) -> Optional[str]:
    s = (raw_num or "").strip()
    if not s:
        return None
    digits = re.sub(r"[^\d]", "", s)
    if len(digits) < 5:
        return None
    return f"+{digits}"


def mask_bot_token(token: str) -> str:
    t = (token or "").strip()
    if not t:
        return "Not Linked"
    if len(t) <= 16:
        return t[:4] + "..." + t[-3:]
    return f"{t[:10]}...{t[-5:]}"


# ==========================================
# SQLite Database Engine
# ==========================================
class ClosingConnection:
    """Wraps sqlite3.Connection so context management commits/rollbacks and always closes."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def __enter__(self) -> sqlite3.Connection:
        self._conn.__enter__()
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            return self._conn.__exit__(exc_type, exc_val, exc_tb)
        finally:
            self._conn.close()

    def close(self):
        self._conn.close()

    def __getattr__(self, name: str):
        return getattr(self._conn, name)


def get_main_db() -> ClosingConnection:
    conn = sqlite3.connect(DB_FILE, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=15000;")
    except Exception:
        pass
    return ClosingConnection(conn)


def _ensure_country_db_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS available_numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number TEXT UNIQUE NOT NULL,
            is_secret INTEGER DEFAULT 0
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS used_numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number TEXT UNIQUE NOT NULL,
            user_id INTEGER,
            is_secret INTEGER DEFAULT 0,
            used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    for col_sql in (
        "ALTER TABLE available_numbers ADD COLUMN is_secret INTEGER DEFAULT 0;",
        "ALTER TABLE used_numbers ADD COLUMN is_secret INTEGER DEFAULT 0;",
    ):
        try:
            conn.execute(col_sql)
        except Exception:
            pass


def get_country_db(country_id: int) -> ClosingConnection:
    os.makedirs(STOCKS_DIR, exist_ok=True)
    db_path = os.path.join(STOCKS_DIR, f"country_{country_id}.db")
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=15000;")
    except Exception:
        pass
    if country_id not in _INITIALIZED_COUNTRY_DBS:
        _ensure_country_db_schema(conn)
        conn.commit()
        _INITIALIZED_COUNTRY_DBS.add(country_id)
    return ClosingConnection(conn)


def init_db() -> None:
    with get_main_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS countries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                button_name TEXT DEFAULT '',
                service_name TEXT DEFAULT 'WhatsApp',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                numbers_consumed INTEGER DEFAULT 0,
                has_secret_access INTEGER DEFAULT 0,
                prefer_plus INTEGER DEFAULT 1,
                is_banned INTEGER DEFAULT 0,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_admins (
                user_id INTEGER PRIMARY KEY,
                added_by INTEGER DEFAULT 0,
                username TEXT,
                first_name TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS delivery_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                country_id INTEGER,
                number_count INTEGER,
                is_secret INTEGER DEFAULT 0,
                delivered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_claimed_numbers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                number TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                country_id INTEGER DEFAULT 0,
                is_secret INTEGER DEFAULT 0,
                claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ucn_num ON user_claimed_numbers(number);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ucn_user ON user_claimed_numbers(user_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ucn_claimed ON user_claimed_numbers(claimed_at);")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_history (
                broadcast_id TEXT PRIMARY KEY,
                admin_id INTEGER,
                target_type TEXT,
                target_user_id INTEGER DEFAULT 0,
                message_text TEXT,
                total_sent INTEGER DEFAULT 0,
                total_failed INTEGER DEFAULT 0,
                is_deleted INTEGER DEFAULT 0,
                country_id INTEGER DEFAULT 0,
                button_label TEXT DEFAULT '',
                service_name TEXT DEFAULT '',
                quantity INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                broadcast_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_otps (
                id TEXT PRIMARY KEY,
                source TEXT,
                country TEXT,
                number TEXT,
                otp_code TEXT,
                raw_message TEXT,
                rate TEXT,
                message_time TEXT,
                received_at TEXT,
                received_at_epoch REAL,
                expires_at TEXT,
                expires_at_epoch REAL,
                group_forwarded INTEGER DEFAULT 0,
                inbox_delivered INTEGER DEFAULT 0,
                user_id INTEGER DEFAULT 0,
                forwarded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'processed'
            );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_po_fwd ON processed_otps(forwarded_at);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_po_num_otp ON processed_otps(number, otp_code);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_po_num_msg ON processed_otps(number, raw_message);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_po_exp ON processed_otps(expires_at_epoch);")
        conn.commit()

    load_consumed_cache()
    # Seed env child tokens only if none are already stored in SQLite
    if ENV_OTP_BOT_TOKEN and not get_bot_setting("otp_bot_token", ""):
        set_bot_setting("otp_bot_token", ENV_OTP_BOT_TOKEN)
        set_bot_setting("otp_bot_enabled", "1")
    if ENV_NUMBER_BOT_TOKEN and not get_bot_setting("number_bot_token", ""):
        set_bot_setting("number_bot_token", ENV_NUMBER_BOT_TOKEN)
        set_bot_setting("number_bot_enabled", "1")


def get_bot_setting(key: str, default: str = "") -> str:
    try:
        with get_main_db() as conn:
            row = conn.execute("SELECT value FROM bot_settings WHERE key = ?;", (key,)).fetchone()
            if row and row["value"] is not None:
                return str(row["value"])
    except Exception:
        pass
    return default


def set_bot_setting(key: str, value: str, from_gist: bool = False) -> bool:
    try:
        with get_main_db() as conn:
            conn.execute(
                """INSERT INTO bot_settings (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP;""",
                (key, str(value)),
            )
            if not from_gist and key not in (
                "_settings_updated_at", "country_updated_at", "deleted_countries",
                "deleted_users", "revoked_admins", "last_active_epoch", "manual_restart",
            ):
                conn.execute(
                    """INSERT INTO bot_settings (key, value, updated_at) VALUES ('_settings_updated_at', ?, CURRENT_TIMESTAMP)
                       ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP;""",
                    (f"{time.time():.6f}",),
                )
            conn.commit()
        if not from_gist and "gist_storage" in globals() and gist_storage.enabled:
            gist_storage.mark_dirty()
        return True
    except Exception as e:
        logger.error(f"Setting save error ({key}): {e}")
        return False


def delete_bot_setting(key: str) -> bool:
    try:
        with get_main_db() as conn:
            conn.execute("DELETE FROM bot_settings WHERE key = ?;", (key,))
            conn.commit()
        if "gist_storage" in globals() and gist_storage.enabled:
            gist_storage.mark_dirty()
        return True
    except Exception:
        return False


def _get_json_dict_setting(key: str) -> Dict[str, float]:
    import json
    raw = get_bot_setting(key, "")
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return {str(k): float(v) for k, v in parsed.items() if v is not None}
    except Exception:
        pass
    return {}


# ==========================================
# Linked Child Bot & Mother Settings
# ==========================================
def get_child_token(role: str) -> str:
    return get_bot_setting(f"{role}_bot_token", "").strip()


def set_child_token(role: str, token: str) -> bool:
    set_bot_setting(f"{role}_bot_enabled", "1")
    return set_bot_setting(f"{role}_bot_token", token.strip())


def clear_child_token(role: str) -> bool:
    set_bot_setting(f"{role}_bot_enabled", "0")
    return set_bot_setting(f"{role}_bot_token", "")


def is_child_enabled(role: str) -> bool:
    return get_bot_setting(f"{role}_bot_enabled", "1").strip() != "0"


def set_child_enabled(role: str, enabled: bool) -> bool:
    return set_bot_setting(f"{role}_bot_enabled", "1" if enabled else "0")


def get_bot_display_name(role: str = "mother") -> str:
    defaults = {
        "mother": "MOTHER CONTROL",
        "otp": "SMS WAVE",
        "number": "NUMBER BOT",
    }
    val = get_bot_setting(f"{role}_bot_name", "").strip()
    return val[:64] if val else defaults.get(role, "BOT")


def set_bot_display_name(role: str, name: str) -> bool:
    return set_bot_setting(f"{role}_bot_name", name.strip()[:64])


# ==========================================
# Independent SMS Format & Delivery Config
# ==========================================
SMS_FORMAT_META: Dict[str, Tuple[str, str, str, str]] = {
    "short": (
        "Short ⚡",
        "Short (Header Country)",
        "Country in header (NEW SMS • NP 🇳🇵), Service & Number with ✦, clean spacing.",
        "Clean card with spacing.",
    ),
    "long": (
        "Long 📜",
        "Long (Expandable SMS)",
        "Clean card with expandable '💬 FULL SMS' blockquote.",
        "Full card with expandable blockquote.",
    ),
    "classic_short": (
        "Classic Short ⏳",
        "Classic Short (Compact)",
        "Country in header (NEW SMS • NP 🇳🇵), Service & Number with ✦, no extra spacing.",
        "Compact card without extra blank lines.",
    ),
    "classic_long": (
        "Classic Long ⏳",
        "Classic Long (Compact + SMS)",
        "Compact header layout with ✦ marks and expandable '💬 FULL SMS' blockquote.",
        "Compact layout with expandable blockquote.",
    ),
    "glass_short": (
        "Direct Short 📩",
        "Direct Short (Signal Ticket)",
        "Top bar (📩 WhatsApp • NP 🇳🇵), blank line + ✦ Number below, fixed width.",
        "Fixed-width Signal Ticket card.",
    ),
    "glass_long": (
        "Direct Full 📩",
        "Direct Full (Open SMS Card)",
        "Top bar + ✦ Number + 💬 Full SMS open on card (no quote box).",
        "Open SMS directly on card with spacing.",
    ),
    "compact_short": (
        "Direct Compact 📩",
        "Direct Compact (No Space)",
        "Top bar (📩 WhatsApp • NP 🇳🇵) + ✦ Number directly below, zero blank lines.",
        "Tight Direct card with zero blank lines.",
    ),
    "compact_long": (
        "Compact Full 📩",
        "Compact Full (No Space + SMS)",
        "Top bar + ✦ Number + 💬 Full SMS stacked tightly with zero blank lines.",
        "Tight Direct card + Full SMS with zero blank lines.",
    ),
}


def normalize_sms_format(fmt: str) -> str:
    val = str(fmt or "").strip().lower()
    aliases = {
        "compact_long": "compact_long",
        "direct_compact_long": "compact_long",
        "glass_compact_long": "compact_long",
        "compact": "compact_long",
        "compact_short": "compact_short",
        "direct_compact_short": "compact_short",
        "glass_compact_short": "compact_short",
        "glass_long": "glass_long",
        "glass": "glass_long",
        "direct_long": "glass_long",
        "direct": "glass_long",
        "glass_short": "glass_short",
        "direct_short": "glass_short",
        "classic_long": "classic_long",
        "old_long": "classic_long",
        "classic_short": "classic_short",
        "old_short": "classic_short",
        "classic": "classic_short",
        "short": "short",
        "clean": "short",
        "off": "short",
    }
    return aliases.get(val, "long")


def get_group_sms_format() -> str:
    return normalize_sms_format(get_bot_setting("group_sms_format", "glass_long"))


def set_group_sms_format(fmt: str) -> bool:
    return set_bot_setting("group_sms_format", normalize_sms_format(fmt))


def get_inbox_sms_format() -> str:
    return normalize_sms_format(get_bot_setting("inbox_sms_format", "compact_short"))


def set_inbox_sms_format(fmt: str) -> bool:
    return set_bot_setting("inbox_sms_format", normalize_sms_format(fmt))


def is_group_forwarding_enabled() -> bool:
    return get_bot_setting("group_forwarding_enabled", "1").strip() != "0"


def set_group_forwarding_enabled(enabled: bool) -> bool:
    if enabled:
        set_bot_setting("forwarding_resumed_epoch", str(time.time()))
    return set_bot_setting("group_forwarding_enabled", "1" if enabled else "0")


def get_forwarding_resumed_epoch() -> float:
    try:
        return float(get_bot_setting("forwarding_resumed_epoch", "0") or 0.0)
    except Exception:
        return 0.0


def is_inbox_otp_enabled() -> bool:
    return get_bot_setting("inbox_otp_enabled", "1").strip() != "0"


def set_inbox_otp_enabled(enabled: bool) -> bool:
    return set_bot_setting("inbox_otp_enabled", "1" if enabled else "0")


# ==========================================
# Admin Management (Mother Bot Only)
# ==========================================
def is_admin(user_id: int) -> bool:
    if not user_id:
        return False
    if ADMIN_USER_IDS and user_id in ADMIN_USER_IDS:
        return True
    try:
        with get_main_db() as conn:
            row = conn.execute("SELECT 1 FROM bot_admins WHERE user_id = ?;", (user_id,)).fetchone()
            return row is not None
    except Exception:
        return False


def get_all_admin_ids() -> List[int]:
    ids: List[int] = list(ADMIN_USER_IDS)
    try:
        with get_main_db() as conn:
            for r in conn.execute("SELECT user_id FROM bot_admins;").fetchall():
                aid = int(r["user_id"])
                if aid not in ids:
                    ids.append(aid)
    except Exception:
        pass
    return ids


def get_all_admin_details() -> List[Dict[str, Any]]:
    details: Dict[int, Dict[str, Any]] = {}
    for sid in ADMIN_USER_IDS:
        details[sid] = {
            "user_id": sid,
            "is_super": True,
            "username": "",
            "first_name": "Super Admin",
            "added_by": 0,
            "added_at": "Environment",
        }
    try:
        with get_main_db() as conn:
            for r in conn.execute(
                "SELECT user_id, added_by, username, first_name, added_at FROM bot_admins ORDER BY added_at ASC;"
            ).fetchall():
                uid = int(r["user_id"])
                if uid in details:
                    details[uid]["username"] = r["username"] or ""
                    if r["first_name"]:
                        details[uid]["first_name"] = r["first_name"]
                else:
                    details[uid] = {
                        "user_id": uid,
                        "is_super": False,
                        "username": r["username"] or "",
                        "first_name": r["first_name"] or "Admin",
                        "added_by": int(r["added_by"] or 0),
                        "added_at": str(r["added_at"] or "")[:19],
                    }
    except Exception:
        pass
    return list(details.values())


def add_admin(user_id: int, added_by: int = 0, username: str = "", first_name: str = "") -> bool:
    import json
    if not user_id:
        return False
    try:
        rev_map = _get_json_dict_setting("revoked_admins")
        if str(user_id) in rev_map:
            rev_map.pop(str(user_id), None)
            set_bot_setting("revoked_admins", json.dumps(rev_map))
        with get_main_db() as conn:
            conn.execute(
                """INSERT INTO bot_admins (user_id, added_by, username, first_name)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(user_id) DO UPDATE SET
                       username = CASE WHEN excluded.username != '' THEN excluded.username ELSE bot_admins.username END,
                       first_name = CASE WHEN excluded.first_name != '' THEN excluded.first_name ELSE bot_admins.first_name END;""",
                (user_id, added_by, username, first_name),
            )
            conn.commit()
        if "gist_storage" in globals() and gist_storage.enabled:
            gist_storage.mark_dirty()
        return True
    except Exception as e:
        logger.error(f"add_admin error: {e}")
        return False


def remove_admin(user_id: int) -> bool:
    import json
    if not user_id or (ADMIN_USER_IDS and user_id in ADMIN_USER_IDS):
        return False
    try:
        with get_main_db() as conn:
            conn.execute("DELETE FROM bot_admins WHERE user_id = ?;", (user_id,))
            conn.commit()
        rev_map = _get_json_dict_setting("revoked_admins")
        rev_map[str(user_id)] = time.time()
        set_bot_setting("revoked_admins", json.dumps(rev_map))
        return True
    except Exception as e:
        logger.error(f"remove_admin error: {e}")
        return False


# ==========================================
# Linked Groups & Custom Action Buttons
# ==========================================
def get_target_group_chat_ids() -> List[int]:
    import json
    raw_dyn = get_bot_setting("linked_group_ids", "")
    ids: List[int] = []
    if raw_dyn:
        try:
            parsed = json.loads(raw_dyn)
            if isinstance(parsed, list):
                for item in parsed:
                    try:
                        cid = int(str(item).strip())
                        if cid and cid not in ids:
                            ids.append(cid)
                    except Exception:
                        pass
        except Exception:
            pass
    if not ids:
        for env_raw in (TELEGRAM_GROUP_CHAT_ID, SECONDARY_GROUP_CHAT_ID):
            for tok in re.split(r"[\s,;]+", (env_raw or "").strip()):
                if not tok:
                    continue
                try:
                    cid = int(tok)
                    if cid and cid not in ids:
                        ids.append(cid)
                except Exception:
                    pass
    return ids[:MAX_FORWARDING_GROUPS]


def add_dynamic_linked_group(chat_id: int) -> Tuple[bool, str]:
    import json
    if not chat_id:
        return False, "Invalid group chat ID."
    current = get_target_group_chat_ids()
    if chat_id in current:
        return False, f"Group <code>{chat_id}</code> is already linked."
    if len(current) >= MAX_FORWARDING_GROUPS:
        return False, f"Maximum limit of {MAX_FORWARDING_GROUPS} linked groups reached."
    current.append(chat_id)
    set_bot_setting("linked_group_ids", json.dumps(current))
    return True, f"Group <code>{chat_id}</code> linked."


def remove_dynamic_linked_group(chat_id: int) -> Tuple[bool, str]:
    import json
    current = get_target_group_chat_ids()
    if chat_id not in current:
        return False, f"Group <code>{chat_id}</code> is not in the linked list."
    current = [g for g in current if g != chat_id]
    set_bot_setting("linked_group_ids", json.dumps(current))
    return True, f"Group <code>{chat_id}</code> removed."


def clear_dynamic_linked_groups() -> None:
    set_bot_setting("linked_group_ids", "[]")


def normalize_button_url(raw_url: str) -> Optional[str]:
    url = (raw_url or "").strip()
    if not url or " " in url:
        return None
    if url.startswith("@"):
        uname = url[1:].strip()
        return f"https://t.me/{uname}" if uname else None
    low = url.lower()
    if low.startswith(("t.me/", "telegram.me/", "www.")):
        return f"https://{url}"
    if low.startswith(("http://", "https://", "tg://")):
        return url
    if "." in url and not url.startswith("."):
        return f"https://{url}"
    return None


def get_custom_link_buttons() -> List[Dict[str, str]]:
    import json
    raw = get_bot_setting("custom_link_buttons", "")
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, list):
            return []
        cleaned: List[Dict[str, str]] = []
        for idx, item in enumerate(parsed):
            if not isinstance(item, dict):
                continue
            b_text = str(item.get("text", "")).strip()
            b_url = normalize_button_url(str(item.get("url", ""))) or ""
            b_id = str(item.get("id") or f"btn_{idx+1}")
            if b_text and b_url:
                cleaned.append({"id": b_id, "text": b_text[:64], "url": b_url})
        return cleaned[:8]
    except Exception:
        return []


def save_custom_link_buttons(buttons: List[Dict[str, str]]) -> bool:
    import json
    return set_bot_setting("custom_link_buttons", json.dumps(buttons[:8]))


# ==========================================
# Country Pools, Numbers & User Operations
# ==========================================
def invalidate_countries_cache() -> None:
    _COUNTRIES_STOCK_CACHE_MAP.clear()


def _country_base_key(name: str) -> str:
    return re.sub(r"[\U0001F1E6-\U0001F1FF]{2}", "", (name or "")).strip().lower()


def _mark_country_active(country_name: str) -> None:
    import json
    bkey = _country_base_key(country_name)
    if not bkey:
        return
    up_map = _get_json_dict_setting("country_updated_at")
    up_map[bkey] = time.time()
    set_bot_setting("country_updated_at", json.dumps(up_map))
    del_map = _get_json_dict_setting("deleted_countries")
    if bkey in del_map:
        del_map.pop(bkey, None)
        set_bot_setting("deleted_countries", json.dumps(del_map))


def _mark_country_deleted(country_name: str) -> None:
    import json
    bkey = _country_base_key(country_name)
    if not bkey:
        return
    del_map = _get_json_dict_setting("deleted_countries")
    del_map[bkey] = time.time()
    set_bot_setting("deleted_countries", json.dumps(del_map))


def _is_country_explicitly_deleted(country_name: str) -> bool:
    bkey = _country_base_key(country_name)
    if not bkey:
        return False
    return _get_json_dict_setting("deleted_countries").get(bkey, 0.0) > _get_json_dict_setting("country_updated_at").get(bkey, 0.0)


def load_consumed_cache() -> None:
    CONSUMED_NUMBERS_CACHE.clear()
    try:
        with get_main_db() as conn:
            for r in conn.execute("SELECT number FROM user_claimed_numbers;").fetchall():
                s = sanitize_phone_number(str(r[0]))
                if s:
                    CONSUMED_NUMBERS_CACHE.add(s)
        if os.path.exists(STOCKS_DIR):
            for fname in os.listdir(STOCKS_DIR):
                if fname.endswith(".db"):
                    try:
                        with ClosingConnection(sqlite3.connect(os.path.join(STOCKS_DIR, fname), timeout=10.0)) as cconn:
                            for r in cconn.execute("SELECT number FROM used_numbers;").fetchall():
                                s = sanitize_phone_number(str(r[0]))
                                if s:
                                    CONSUMED_NUMBERS_CACHE.add(s)
                    except Exception:
                        pass
    except Exception:
        pass


def get_or_create_country(country_name: str, from_gist: bool = False) -> int:
    clean = (country_name or "").strip()
    formatted = format_country_name(clean)
    if not from_gist:
        _mark_country_active(formatted)
    with get_main_db() as conn:
        row = conn.execute(
            "SELECT id, name FROM countries WHERE LOWER(name) = LOWER(?) OR LOWER(name) = LOWER(?);",
            (clean, formatted),
        ).fetchone()
        if row:
            return int(row["id"])
        cur = conn.execute("INSERT INTO countries (name) VALUES (?);", (formatted,))
        conn.commit()
        cid = int(cur.lastrowid)
        get_country_db(cid).close()
        invalidate_countries_cache()
        return cid


def get_country_by_id(country_id: int) -> Optional[Dict[str, Any]]:
    with get_main_db() as conn:
        row = conn.execute("SELECT id, name, button_name, service_name FROM countries WHERE id = ?;", (country_id,)).fetchone()
        return dict(row) if row else None


def get_country_available_count(country_id: int, is_secret: bool = False) -> int:
    sval = 1 if is_secret else 0
    try:
        with get_country_db(country_id) as cconn:
            return int(cconn.execute("SELECT COUNT(*) FROM available_numbers WHERE is_secret = ?;", (sval,)).fetchone()[0])
    except Exception:
        return 0


def update_country_button_name(country_id: int, button_name: str) -> bool:
    with get_main_db() as conn:
        conn.execute("UPDATE countries SET button_name = ? WHERE id = ?;", (button_name.strip(), country_id))
        conn.commit()
    invalidate_countries_cache()
    if "gist_storage" in globals() and gist_storage.enabled:
        gist_storage.mark_dirty()
    return True


def update_country_service_name(country_id: int, service_name: str) -> bool:
    with get_main_db() as conn:
        conn.execute("UPDATE countries SET service_name = ? WHERE id = ?;", (service_name.strip(), country_id))
        conn.commit()
    invalidate_countries_cache()
    if "gist_storage" in globals() and gist_storage.enabled:
        gist_storage.mark_dirty()
    return True


def get_all_countries_with_stock(only_active: bool = True, secret_mode: bool = False, include_used: bool = False) -> List[Dict[str, Any]]:
    cache_key = (only_active, secret_mode, include_used)
    now_ts = time.monotonic()
    cached = _COUNTRIES_STOCK_CACHE_MAP.get(cache_key)
    if cached is not None and (now_ts - cached[0]) < _COUNTRIES_STOCK_CACHE_TTL:
        return cached[1]

    with get_main_db() as conn:
        countries = conn.execute("SELECT id, name, button_name, service_name FROM countries ORDER BY name ASC;").fetchall()

    results: List[Dict[str, Any]] = []
    for c in countries:
        cid = int(c["id"])
        cname = format_country_name(c["name"])
        try:
            with get_country_db(cid) as cconn:
                avail_std = int(cconn.execute("SELECT COUNT(*) FROM available_numbers WHERE is_secret = 0;").fetchone()[0])
                avail_sec = int(cconn.execute("SELECT COUNT(*) FROM available_numbers WHERE is_secret = 1;").fetchone()[0])
                if include_used:
                    used_std = int(cconn.execute("SELECT COUNT(*) FROM used_numbers WHERE is_secret = 0;").fetchone()[0])
                    used_sec = int(cconn.execute("SELECT COUNT(*) FROM used_numbers WHERE is_secret = 1;").fetchone()[0])
                else:
                    used_std, used_sec = 0, 0
        except Exception:
            avail_std = avail_sec = used_std = used_sec = 0

        target_avail = avail_sec if secret_mode else avail_std
        if only_active and target_avail == 0:
            continue

        btn_val = format_country_name(c["button_name"], fallback_country=cname) if c["button_name"] else cname
        results.append({
            "id": cid,
            "name": cname,
            "button_name": btn_val,
            "service_name": c["service_name"] or "WhatsApp",
            "available": target_avail,
            "available_std": avail_std,
            "available_sec": avail_sec,
            "total_available": avail_std + avail_sec,
            "used_std": used_std,
            "used_sec": used_sec,
            "total_used": used_std + used_sec,
        })

    _COUNTRIES_STOCK_CACHE_MAP[cache_key] = (now_ts, results)
    return results


def add_numbers_to_country(country_id: int, numbers: List[str], is_secret: bool = False, from_gist: bool = False) -> Tuple[int, int]:
    if not from_gist:
        try:
            c = get_country_by_id(country_id)
            if c and c.get("name"):
                _mark_country_active(c["name"])
        except Exception:
            pass
    added = 0
    duplicates = 0
    sval = 1 if is_secret else 0
    with get_country_db(country_id) as conn:
        conn.execute("BEGIN TRANSACTION;")
        for raw_num in numbers:
            num = sanitize_phone_number(raw_num)
            if not num:
                continue
            if num in CONSUMED_NUMBERS_CACHE:
                duplicates += 1
                continue
            if conn.execute("SELECT 1 FROM used_numbers WHERE number = ? LIMIT 1;", (num,)).fetchone():
                CONSUMED_NUMBERS_CACHE.add(num)
                duplicates += 1
                continue
            try:
                conn.execute("INSERT INTO available_numbers (number, is_secret) VALUES (?, ?);", (num, sval))
                added += 1
            except sqlite3.IntegrityError:
                duplicates += 1
        conn.commit()
    invalidate_countries_cache()
    if not from_gist and added > 0 and "gist_storage" in globals() and gist_storage.enabled:
        gist_storage.mark_dirty()
    return added, duplicates


def remove_numbers_from_country(country_id: int, numbers: List[str]) -> int:
    removed = 0
    cleaned_set: Set[str] = set()
    with get_country_db(country_id) as conn:
        conn.execute("BEGIN TRANSACTION;")
        for raw_num in numbers:
            num = sanitize_phone_number(raw_num)
            if not num:
                continue
            cleaned_set.add(num)
            cur = conn.execute("DELETE FROM available_numbers WHERE number = ?;", (num,))
            removed += cur.rowcount
            conn.execute("DELETE FROM used_numbers WHERE number = ?;", (num,))
            CONSUMED_NUMBERS_CACHE.discard(num)
            CONSUMED_NUMBERS_CACHE.discard(num.lstrip("+"))
        conn.commit()
    if cleaned_set:
        with get_main_db() as mconn:
            for num in cleaned_set:
                mconn.execute("DELETE FROM user_claimed_numbers WHERE number IN (?, ?);", (num, num.lstrip("+")))
                dig = re.sub(r"\D", "", num)
                CLAIMED_NUMBER_OWNER_CACHE.pop(dig, None)
                CLAIMED_NUMBER_OWNER_CACHE.pop(f"+{dig}", None)
            mconn.commit()
    invalidate_countries_cache()
    if "gist_storage" in globals() and gist_storage.enabled:
        gist_storage.mark_dirty()
    return removed


def delete_country_and_stock(country_id: int, record_deletion: bool = True) -> None:
    if record_deletion:
        c = get_country_by_id(country_id)
        if c and c.get("name"):
            _mark_country_deleted(c["name"])
    try:
        with get_country_db(country_id) as cconn:
            for r in cconn.execute("SELECT number FROM used_numbers;").fetchall():
                s = sanitize_phone_number(str(r[0]))
                if s:
                    CONSUMED_NUMBERS_CACHE.discard(s)
            cconn.execute("DELETE FROM available_numbers;")
            cconn.execute("DELETE FROM used_numbers;")
            cconn.commit()
    except Exception:
        pass
    _INITIALIZED_COUNTRY_DBS.discard(country_id)
    with get_main_db() as conn:
        for r in conn.execute("SELECT number FROM user_claimed_numbers WHERE country_id = ?;", (country_id,)).fetchall():
            dig = re.sub(r"\D", "", str(r[0]))
            CLAIMED_NUMBER_OWNER_CACHE.pop(dig, None)
            CLAIMED_NUMBER_OWNER_CACHE.pop(f"+{dig}", None)
        conn.execute("DELETE FROM user_claimed_numbers WHERE country_id = ?;", (country_id,))
        conn.execute("DELETE FROM countries WHERE id = ?;", (country_id,))
        conn.execute("DELETE FROM delivery_log WHERE country_id = ?;", (country_id,))
        conn.commit()
    invalidate_countries_cache()
    c_db = os.path.join(STOCKS_DIR, f"country_{country_id}.db")
    for ext in ("", "-wal", "-shm"):
        p = c_db + ext
        if os.path.exists(p):
            try:
                os.remove(p)
            except Exception:
                pass
    if "gist_storage" in globals() and gist_storage.enabled:
        gist_storage.mark_dirty()


def consume_numbers_for_user(country_id: int, user_id: int, limit: int = 10, is_secret: bool = False) -> Tuple[List[str], int, str]:
    secret_val = 1 if is_secret else 0
    c = get_country_by_id(country_id)
    country_name = format_country_name(c["name"]) if c else "Unknown"

    with get_country_db(country_id) as cconn:
        cconn.execute("BEGIN IMMEDIATE;")
        rows = cconn.execute(
            "SELECT id, number FROM available_numbers WHERE is_secret = ? ORDER BY id ASC LIMIT ?;",
            (secret_val, limit),
        ).fetchall()
        if not rows:
            cconn.commit()
            return [], 0, country_name
        numbers = [str(r["number"]) for r in rows]
        ids = [int(r["id"]) for r in rows]
        cconn.execute(f"DELETE FROM available_numbers WHERE id IN ({','.join(['?'] * len(ids))});", ids)
        for num in numbers:
            cconn.execute("INSERT OR IGNORE INTO used_numbers (number, user_id, is_secret) VALUES (?, ?, ?);", (num, user_id, secret_val))
            CONSUMED_NUMBERS_CACHE.add(num)
        cconn.commit()
        remaining = int(cconn.execute("SELECT COUNT(*) FROM available_numbers WHERE is_secret = ?;", (secret_val,)).fetchone()[0])

    invalidate_countries_cache()
    with get_main_db() as mconn:
        mconn.execute("UPDATE users SET numbers_consumed = numbers_consumed + ?, last_seen = CURRENT_TIMESTAMP WHERE user_id = ?;", (len(numbers), user_id))
        mconn.execute("INSERT INTO delivery_log (user_id, country_id, number_count, is_secret) VALUES (?, ?, ?, ?);", (user_id, country_id, len(numbers), secret_val))
        c_now = time.time()
        for num in numbers:
            mconn.execute(
                "INSERT INTO user_claimed_numbers (number, user_id, country_id, is_secret, claimed_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP);",
                (num, user_id, country_id, secret_val),
            )
            dig = re.sub(r"\D", "", num)
            if dig:
                CLAIMED_NUMBER_OWNER_CACHE[dig] = (user_id, country_id, c_now)
                CLAIMED_NUMBER_OWNER_CACHE[f"+{dig}"] = (user_id, country_id, c_now)
        mconn.commit()
    if "gist_storage" in globals() and gist_storage.enabled:
        gist_storage.mark_dirty()
    return numbers, remaining, country_name


def get_number_owner(phone_number: str, max_age_hours: int = 28) -> Optional[int]:
    if not phone_number:
        return None
    raw_num = str(phone_number).strip()
    digits = re.sub(r"\D", "", raw_num)
    if not digits:
        return None
    now_t = time.time()
    cutoff_epoch = now_t - (max_age_hours * 3600)
    for k in (digits, f"+{digits}", raw_num):
        if k in CLAIMED_NUMBER_OWNER_CACHE:
            uid, _, ctime = CLAIMED_NUMBER_OWNER_CACHE[k]
            if ctime >= cutoff_epoch:
                return uid

    candidates = [raw_num, f"+{digits}", digits]
    cutoff_dt = (datetime.now(timezone.utc) - timedelta(hours=max_age_hours)).strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_main_db() as conn:
            ph = ",".join(["?"] * len(candidates))
            row = conn.execute(
                f"SELECT user_id FROM user_claimed_numbers WHERE number IN ({ph}) AND datetime(claimed_at) >= datetime(?) ORDER BY id DESC LIMIT 1;",
                (*candidates, cutoff_dt),
            ).fetchone()
            if not row:
                row = conn.execute(
                    """SELECT user_id FROM user_claimed_numbers
                       WHERE REPLACE(REPLACE(REPLACE(REPLACE(number, '+', ''), ' ', ''), '-', ''), '\u00a0', '') = ?
                         AND datetime(claimed_at) >= datetime(?)
                       ORDER BY id DESC LIMIT 1;""",
                    (digits, cutoff_dt),
                ).fetchone()
            if row and row["user_id"]:
                uid = int(row["user_id"])
                CLAIMED_NUMBER_OWNER_CACHE[digits] = (uid, 0, now_t)
                CLAIMED_NUMBER_OWNER_CACHE[f"+{digits}"] = (uid, 0, now_t)
                return uid
    except Exception:
        pass
    return None


def get_system_stats() -> Dict[str, Any]:
    with get_main_db() as conn:
        total_users = int(conn.execute("SELECT COUNT(*) FROM users;").fetchone()[0])
        total_consumed = int(conn.execute("SELECT COALESCE(SUM(numbers_consumed), 0) FROM users;").fetchone()[0])
        total_secret_users = int(conn.execute("SELECT COUNT(*) FROM users WHERE has_secret_access = 1;").fetchone()[0])
    all_c = get_all_countries_with_stock(only_active=False)
    std_avail = sum(c["available_std"] for c in all_c)
    sec_avail = sum(c["available_sec"] for c in all_c)
    active_cnt = sum(1 for c in all_c if (c["available_std"] + c["available_sec"]) > 0)
    return {
        "total_available": std_avail + sec_avail,
        "total_std_available": std_avail,
        "total_sec_available": sec_avail,
        "total_consumed": total_consumed,
        "total_users": total_users,
        "total_secret_users": total_secret_users,
        "total_countries": len(all_c),
        "active_countries": active_cnt,
    }


def get_admin_fixed_quantity() -> Optional[int]:
    val = get_bot_setting("fixed_quantity", "")
    if val.isdigit():
        num = int(val)
        if 1 <= num <= 1000:
            return num
    return None


def set_admin_fixed_quantity(val: str) -> bool:
    return set_bot_setting("fixed_quantity", val.strip())


def get_effective_quantity_for_user(user_id: int = 0) -> Tuple[int, bool]:
    fixed = get_admin_fixed_quantity()
    return (fixed, True) if fixed is not None else (10, False)


def register_user(user_id: int, username: str = "", first_name: str = "") -> None:
    import json
    if not user_id:
        return
    del_map = _get_json_dict_setting("deleted_users")
    if str(user_id) in del_map:
        del_map.pop(str(user_id), None)
        set_bot_setting("deleted_users", json.dumps(del_map))
    with get_main_db() as conn:
        conn.execute(
            """INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET
                   username = CASE WHEN excluded.username != '' THEN excluded.username ELSE users.username END,
                   first_name = CASE WHEN excluded.first_name != '' THEN excluded.first_name ELSE users.first_name END,
                   last_seen = CURRENT_TIMESTAMP;""",
            (user_id, username or "", first_name or ""),
        )
        conn.commit()


def get_user_details(user_id: int) -> Optional[Dict[str, Any]]:
    with get_main_db() as conn:
        row = conn.execute("SELECT * FROM users WHERE user_id = ?;", (user_id,)).fetchone()
        return dict(row) if row else None


def get_user_recent_claims(user_id: int, limit: int = 25) -> List[Dict[str, Any]]:
    """Returns a user's recently claimed numbers with resolved country names."""
    with get_main_db() as conn:
        rows = conn.execute(
            """SELECT ucn.number, c.name AS country_name, ucn.claimed_at
               FROM user_claimed_numbers ucn
               LEFT JOIN countries c ON c.id = ucn.country_id
               WHERE ucn.user_id = ?
               ORDER BY ucn.id DESC
               LIMIT ?;""",
            (user_id, max(1, limit)),
        ).fetchall()
    return [
        {
            "phone": str(r["number"]),
            "country": format_country_name(r["country_name"] or "Unknown"),
            "claimed_at": str(r["claimed_at"] or ""),
        }
        for r in rows
    ]


def get_all_users_detailed(limit: int = 10, offset: int = 0) -> Tuple[List[Dict[str, Any]], int]:
    with get_main_db() as conn:
        total = int(conn.execute("SELECT COUNT(*) FROM users;").fetchone()[0])
        rows = conn.execute("SELECT * FROM users ORDER BY numbers_consumed DESC, last_seen DESC LIMIT ? OFFSET ?;", (limit, offset)).fetchall()
        return [dict(r) for r in rows], total


def get_all_user_ids() -> List[int]:
    with get_main_db() as conn:
        return [int(r[0]) for r in conn.execute("SELECT user_id FROM users WHERE COALESCE(is_banned, 0) = 0;").fetchall()]


def is_user_banned(user_id: int) -> bool:
    u = get_user_details(user_id)
    return bool(u and u.get("is_banned"))


def set_user_ban_status(user_id: int, banned: bool) -> bool:
    with get_main_db() as conn:
        conn.execute("UPDATE users SET is_banned = ? WHERE user_id = ?;", (1 if banned else 0, user_id))
        conn.commit()
    return True


def user_has_secret_access(user_id: int) -> bool:
    if is_admin(user_id):
        return True
    u = get_user_details(user_id)
    return bool(u and u.get("has_secret_access"))


def set_user_secret_access(user_id: int, granted: bool) -> bool:
    with get_main_db() as conn:
        conn.execute("UPDATE users SET has_secret_access = ? WHERE user_id = ?;", (1 if granted else 0, user_id))
        conn.commit()
    if "gist_storage" in globals() and gist_storage.enabled:
        gist_storage.mark_dirty()
    return True


def get_user_plus_preference(user_id: int) -> bool:
    u = get_user_details(user_id)
    return bool(u["prefer_plus"]) if u and u.get("prefer_plus") is not None else True


def set_user_plus_preference(user_id: int, prefer_plus: bool) -> bool:
    with get_main_db() as conn:
        conn.execute("UPDATE users SET prefer_plus = ? WHERE user_id = ?;", (1 if prefer_plus else 0, user_id))
        conn.commit()
    return True


def delete_user(user_id: int) -> bool:
    import json
    if ADMIN_USER_IDS and user_id in ADMIN_USER_IDS:
        return False
    with get_main_db() as conn:
        conn.execute("DELETE FROM users WHERE user_id = ?;", (user_id,))
        conn.execute("DELETE FROM delivery_log WHERE user_id = ?;", (user_id,))
        conn.execute("DELETE FROM user_claimed_numbers WHERE user_id = ?;", (user_id,))
        conn.commit()
    del_map = _get_json_dict_setting("deleted_users")
    del_map[str(user_id)] = time.time()
    set_bot_setting("deleted_users", json.dumps(del_map))
    return True


def record_broadcast_history(
    broadcast_id: str,
    admin_id: int,
    target_type: str,
    message_text: str,
    total_sent: int,
    total_failed: int,
    target_user_id: int = 0,
    country_id: int = 0,
    button_label: str = "",
    service_name: str = "",
    quantity: int = 0,
) -> None:
    with get_main_db() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO broadcast_history
               (broadcast_id, admin_id, target_type, target_user_id, message_text,
                total_sent, total_failed, country_id, button_label, service_name, quantity)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""",
            (broadcast_id, admin_id, target_type, target_user_id, message_text,
             total_sent, total_failed, country_id, button_label, service_name, quantity),
        )
        conn.commit()


def record_broadcast_message(broadcast_id: str, user_id: int, message_id: int) -> None:
    with get_main_db() as conn:
        conn.execute("INSERT INTO broadcast_messages (broadcast_id, user_id, message_id) VALUES (?, ?, ?);", (broadcast_id, user_id, message_id))
        conn.commit()


def get_recent_broadcasts(limit: int = 15) -> List[Dict[str, Any]]:
    with get_main_db() as conn:
        rows = conn.execute("SELECT * FROM broadcast_history ORDER BY created_at DESC LIMIT ?;", (limit,)).fetchall()
        return [dict(r) for r in rows]


def get_broadcast_messages(broadcast_id: str) -> List[Tuple[int, int]]:
    with get_main_db() as conn:
        return [(int(r[0]), int(r[1])) for r in conn.execute("SELECT user_id, message_id FROM broadcast_messages WHERE broadcast_id = ?;", (broadcast_id,)).fetchall()]


def mark_broadcast_deleted(broadcast_id: str) -> None:
    with get_main_db() as conn:
        conn.execute("UPDATE broadcast_history SET is_deleted = 1 WHERE broadcast_id = ?;", (broadcast_id,))
        conn.execute("DELETE FROM broadcast_messages WHERE broadcast_id = ?;", (broadcast_id,))
        conn.commit()


# ==========================================
# OTP Parsing, Deduplication & Card Formatter
# ==========================================
def get_country_iso_display(item: Dict[str, Any]) -> str:
    for field in ("countryCode", "iso", "iso2", "country"):
        val = str(item.get(field) or "").strip().lower()
        if val in COUNTRY_ISO_DATA:
            flag, code = COUNTRY_ISO_DATA[val]
            return f"{flag} {code}"
    raw_range = str(item.get("range_name") or item.get("rangeName") or item.get("destinationName") or "").strip()
    if raw_range:
        parts = re.split(r"[-–—,:/]", raw_range)
        name_key = parts[0].strip().lower()
        if name_key in COUNTRY_ISO_DATA:
            flag, code = COUNTRY_ISO_DATA[name_key]
            return f"{flag} {code}"
        for cname, (flag, code) in COUNTRY_ISO_DATA.items():
            if len(cname) > 3 and cname in raw_range.lower():
                return f"{flag} {code}"
    raw_num = str(item.get("number") or item.get("destinationNumber") or item.get("range_template") or item.get("rangeTemplate") or item.get("dst") or "")
    digits = re.sub(r"\D", "", raw_num)
    for p_len in (4, 3, 2, 1):
        pref = digits[:p_len]
        if pref in PREFIX_ISO_MAP:
            flag, code = PREFIX_ISO_MAP[pref]
            return f"{flag} {code}"
    return "🌐 XX"


def get_country_full_name(iso_code: str, item: Optional[Dict[str, Any]] = None) -> str:
    iso = (iso_code or "").strip().upper()
    if iso in ISO_TO_COUNTRY_NAME:
        return ISO_TO_COUNTRY_NAME[iso]
    if item:
        raw_range = str(item.get("range_name") or item.get("rangeName") or item.get("destinationName") or "").strip()
        if raw_range:
            cand = re.split(r"[-–—,:/]", raw_range)[0].strip().title()
            if len(cand) > 2:
                return cand if len(cand) <= 14 else cand[:13].rstrip() + "."
    return "Global"


def mask_phone_number(num_str: str) -> str:
    if not num_str:
        return ""
    clean = str(num_str).strip().replace(" ", "").replace("\u00a0", "")
    if "*" in clean:
        masked = re.sub(r"\*+", "****", re.sub(r"\d?\*{2,}", "****", clean))
        return masked if masked.startswith("+") else f"+{masked}"
    digits = re.sub(r"\D", "", clean)
    if not digits:
        return clean
    found_prefix = ""
    for p_len in (4, 3, 2, 1):
        cand = digits[:p_len]
        if cand in PREFIX_ISO_MAP and len(digits) > p_len:
            found_prefix = cand
            break
    if found_prefix:
        rest = digits[len(found_prefix):]
        if len(rest) >= 6:
            return f"+{found_prefix}{rest[:3]}****{rest[-3:]}"
        if len(rest) >= 4:
            return f"+{found_prefix}{rest[:2]}****{rest[-2:]}"
        if len(rest) > 2:
            return f"+{found_prefix}{rest[:1]}****{rest[-1:]}"
        return f"+{found_prefix}{rest}"
    if len(digits) >= 8:
        return f"+{digits[:4]}****{digits[-3:]}"
    if len(digits) >= 5:
        return f"+{digits[:2]}****{digits[-2:]}"
    return f"+{digits}"


def format_phone_for_card(raw_num: str) -> str:
    """Formats unmasked phone with ZWNJ separators so Telegram does not make it a tappable link."""
    clean = str(raw_num or "").strip()
    digits = re.sub(r"\D", "", clean)
    if not digits:
        return clean if clean.startswith("+") else f"+{clean}"
    chunks = [digits[i:i + 4] for i in range(0, len(digits), 4)]
    return "+" + "\u200c".join(chunks)


def detect_service_name(raw_source: str, raw_message: str) -> str:
    low_src = (raw_source or "").lower()
    low_msg = (raw_message or "").lower()
    rules = [
        ("WhatsApp", ["whatsapp", "‏واتساب‏", "واتساب", "ватсап", "wa code", "wa.me"], ["whatsapp", "wa.me"]),
        ("Instagram", ["instagram", "ig code"], ["instagram"]),
        ("Telegram", ["telegram", "telegram code", "t.me"], ["telegram", "t.me"]),
        ("Facebook", ["facebook", "fb-", "fb code", "fb.me"], ["facebook"]),
        ("Google", ["google", "g-"], ["google"]),
        ("TikTok", ["tiktok", "bytedance"], ["tiktok"]),
        ("Microsoft", ["microsoft", "live.com", "outlook", "msft"], ["microsoft"]),
        ("Apple", ["apple id", "apple account", "icloud", "apple"], ["apple"]),
        ("Discord", ["discord"], ["discord"]),
        ("PayPal", ["paypal"], ["paypal"]),
        ("Binance", ["binance"], ["binance"]),
        ("Coinbase", ["coinbase"], ["coinbase"]),
        ("Bybit", ["bybit"], ["bybit"]),
        ("OKX", ["okx"], ["okx"]),
        ("OpenAI", ["openai", "chatgpt"], ["openai"]),
        ("Amazon", ["amazon", "aws"], ["amazon"]),
        ("Snapchat", ["snapchat", "snap code"], ["snapchat"]),
        ("Twitter (X)", ["twitter", "x.com"], ["twitter"]),
        ("Steam", ["steam", "steam guard"], ["steam"]),
        ("Uber", ["uber"], ["uber"]),
        ("Bolt", ["bolt"], ["bolt"]),
        ("Netflix", ["netflix"], ["netflix"]),
        ("Signal", ["signal"], ["signal"]),
        ("IMO", ["imo"], ["imo"]),
        ("Viber", ["viber"], ["viber"]),
        ("Tinder", ["tinder"], ["tinder"]),
        ("Yahoo", ["yahoo"], ["yahoo"]),
        ("1xBet", ["1xbet"], ["1xbet"]),
    ]
    for label, msg_keys, _ in rules:
        if label == "Google" and (raw_message or "").strip().upper().startswith("G-"):
            return "Google"
        for k in msg_keys:
            if k == "g-":
                continue
            if len(k) <= 4 and k.isalpha():
                if re.search(rf"\b{re.escape(k)}\b", low_msg):
                    return label
            elif k in low_msg:
                return label
    for label, _, src_keys in rules:
        for k in src_keys:
            if len(k) <= 3 and k.isalpha():
                if low_src == k or re.search(rf"\b{re.escape(k)}\b", low_src):
                    return label
            elif k in low_src:
                return label
    clean_src = re.sub(r"\s+", " ", raw_source or "").strip()
    if len(clean_src) > 15:
        clean_src = clean_src[:14].rstrip() + "."
    return clean_src or "SMS Service"


def extract_otp_code(text: str) -> str:
    if not text:
        return ""
    clean_text = re.sub(r"[\u200b-\u200f\u202a-\u202e\ufeff]", "", str(text))

    def _is_non_otp(s: str, start_pos: int, end_pos: int, tok: str) -> bool:
        before = s[max(0, start_pos - 12):start_pos]
        after = s[end_pos:end_pos + 16]
        if before.endswith(":") or after.startswith(":"):
            return True
        if before.endswith(("/", "=", "?", "$", "€", "£", "₹")):
            return True
        if re.search(r"https?://\S*$", before, re.IGNORECASE):
            return True
        if re.search(r"\+\d{0,4}[\s\-]?$", before):
            return True
        if re.match(r"^\d{4}$", tok) and (after.startswith(("-", "/")) or before.endswith(("-", "/"))):
            return True
        if re.match(r"^\s*(?:min(?:ute)?s?|sec(?:ond)?s?|hours?|hrs?|days?|weeks?|months?|years?|%)\b", after, re.IGNORECASE):
            return True
        return False

    pref_m = re.search(r"(?<![A-Za-z0-9/=\-])((?:G|FB|IG|WA|VK|TG|TK|MS|GOOG|OTP|PIN|AUTH)-\d{4,8})\b", clean_text, re.IGNORECASE)
    if pref_m:
        return pref_m.group(1).upper()

    pre_phrase = r"(?:is\s+(?:your|the|one-time)|to\s+verify|for\s+(?:your|the)\s+(?:\w+\s+){0,2}(?:code|otp|pin|verification))"
    pre_spaced = re.search(rf"(?<![\d\-+])(\d{{2,4}}(?:[ \t]+\d{{2,4}}){{1,3}})\s+{pre_phrase}\b", clean_text, re.IGNORECASE)
    if pre_spaced:
        col = re.sub(r"\s+", "", pre_spaced.group(1))
        if 4 <= len(col) <= 8:
            return col

    kw_regex = re.compile(r"(?:\b(?:code|otp|pin|passcode|password|secret|token|verif\w*|confirm\w*|security|login|auth\w*|kod\w*|codigo|código|kode|mã|код|رمز|كود)\b)", re.IGNORECASE)
    patterns = [
        ("spaced", re.compile(r"(?<![\d\-+])(\d{2,4}(?:[ \t]+\d{2,4}){1,3})(?![\d\-])")),
        ("hyphen", re.compile(r"(?<![\d\-])(\d{3}-\d{3}|\d{3}-\d{4}|\d{4}-\d{4})(?![\d\-])")),
        ("digits", re.compile(r"(?<![\dA-Za-z\-])(\d{4,8})(?![\dA-Za-z\-])")),
    ]
    best = None
    for kw_m in kw_regex.finditer(clean_text):
        tail = clean_text[kw_m.end():kw_m.end() + 72]
        for kind, pat in patterns:
            for cm in pat.finditer(tail):
                cand = cm.group(1)
                abs_s, abs_e = kw_m.end() + cm.start(1), kw_m.end() + cm.end(1)
                if _is_non_otp(clean_text, abs_s, abs_e, cand):
                    continue
                val = re.sub(r"\s+", "", cand) if kind == "spaced" else cand
                if kind == "spaced" and not (4 <= len(val) <= 8):
                    continue
                prio = 1 if kind in ("spaced", "hyphen") else 2
                score = (prio, abs_s)
                if best is None or score < best[0]:
                    best = (score, val)
    if best is not None:
        return best[1]

    san = re.sub(r"https?://\S+|\+\d[\d\s\-]{5,}|\b\d{1,2}:\d{2}(?::\d{2})?\b|\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b", " ", clean_text)
    for pat in (
        r"(?<![\d\-])(?:\d{3}-\d{3}|\d{3}-\d{4}|\d{4}-\d{4})(?![\d\-])",
        r"(?<![\dA-Za-z\-])[0-9]{4,8}(?![\dA-Za-z\-])",
    ):
        found = re.findall(pat, san)
        if found:
            return found[0]
    return ""


def parse_message_timestamp(time_str: str) -> float:
    if not time_str:
        return 0.0
    clean = str(time_str).strip()
    try:
        if clean.replace(".", "", 1).isdigit():
            val = float(clean)
            return (val / 1000.0) if val > 1e11 else val
        if "T" in clean:
            dt = datetime.fromisoformat(clean.replace("Z", "+00:00"))
            return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(clean[:19], fmt).replace(tzinfo=timezone.utc).timestamp()
            except ValueError:
                pass
        dt = datetime.fromisoformat(clean)
        return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()
    except Exception:
        return 0.0


COLLAPSED_SMS_HINT = "<b>✦\u00a0\u00a0TAP TO READ SMS..</b>"
QUOTE_DIVIDER = "━" * 14
CARD_WIDTH_PAD = "\u2800" * 7


def style_sms_with_copyable_otp(msg: str, otp: str, width: int = 36) -> str:
    if not msg:
        return ""
    raw_otp = (otp or "").strip()
    otp_alnum = re.sub(r"[^A-Za-z0-9]", "", raw_otp)
    otp_span = (
        re.compile(r"(?<![\dA-Za-z])" + r"[-\s]?".join(re.escape(c) for c in otp_alnum) + r"(?![\dA-Za-z])")
        if len(otp_alnum) >= 3 else None
    )
    lines: List[str] = []
    for para in str(msg).splitlines():
        p = para.strip()
        if p:
            if otp_span:
                p = otp_span.sub(lambda m: re.sub(r"[ \t]+", "\u00a0", m.group(0)), p)
            lines.extend(textwrap.wrap(p, width=width, break_long_words=True, break_on_hyphens=False))
        else:
            lines.append("")
    escaped = html.escape("\n".join(lines), quote=False)
    if raw_otp:
        esc_otp = html.escape(raw_otp, quote=False)
        pat = re.compile(rf"(?<![\dA-Za-z]){re.escape(esc_otp)}(?![\dA-Za-z])")
        if pat.search(escaped):
            escaped = pat.sub(lambda _: f"<code>{esc_otp}</code>", escaped, count=1)
        elif otp_span:
            flex = re.compile(r"(?<![\dA-Za-z])" + r"[-\s\u00a0]?".join(re.escape(c) for c in otp_alnum) + r"(?![\dA-Za-z])")
            m = flex.search(escaped)
            if m:
                escaped = escaped[:m.start()] + f"<code>{m.group(0).replace('\u00a0', ' ')}</code>" + escaped[m.end():]
    return escaped.replace("\u00a0", " ")


def build_fixed_width_direct_header(source_raw: str, iso_fixed: str, flag: str) -> str:
    return f"📩 <b>{html.escape(source_raw)} • {html.escape(iso_fixed)}\u00a0{flag}{CARD_WIDTH_PAD}</b>"


def format_otp_card(item: Dict[str, Any], sms_format: str, masked: bool = True) -> Tuple[str, str, str]:
    """Unified 8-mode OTP notification card renderer for both Group OTP and DM Inbox."""
    mode = normalize_sms_format(sms_format)
    raw_source = str(item.get("sender_id") or item.get("sourceAddress") or item.get("source") or item.get("caller") or item.get("sender") or "SMS Service").strip()
    raw_number = str(item.get("number") or item.get("destinationNumber") or item.get("range_template") or item.get("rangeTemplate") or item.get("dst") or "").strip()
    raw_message = str(item.get("sms") or item.get("messageBody") or item.get("message") or item.get("text") or item.get("body") or item.get("otp") or "").strip()
    otp_code = extract_otp_code(raw_message)

    country_iso = get_country_iso_display(item).strip()
    parts = country_iso.split(maxsplit=1)
    flag = parts[0] if parts else "🌐"
    iso = parts[1] if len(parts) > 1 else "XX"
    country_name = get_country_full_name(iso, item)
    iso_fixed = iso if (iso and iso != "XX" and len(iso) == 2) else "GL"
    country_part = f"({html.escape(iso_fixed)} {flag})"

    source_raw = detect_service_name(raw_source, raw_message)
    source_escaped = html.escape(source_raw)

    if masked:
        num_display = mask_phone_number(raw_number) if raw_number else country_name
    else:
        num_display = format_phone_for_card(raw_number) if raw_number else country_name
    num_escaped = html.escape(num_display)

    if mode in ("glass_short", "glass_long", "compact_short", "compact_long"):
        is_compact = mode.startswith("compact_")
        include_sms = mode.endswith("_long")
        lines = [build_fixed_width_direct_header(source_raw, iso_fixed, flag)]
        if not is_compact:
            lines.append("")
        lines.append(f"<b>✦\u00a0\u00a0{num_escaped}</b>")
        if include_sms and raw_message:
            disp = raw_message if len(raw_message) <= 3000 else (raw_message[:3000] + "... [truncated]")
            styled = style_sms_with_copyable_otp(disp, otp_code, width=36)
            compact_sms = "\n".join(ln for ln in styled.splitlines() if ln.strip())
            if not is_compact:
                lines.append("")
            lines.append(f"💬 <b>Full SMS</b>\n{compact_sms}")
        return "\n".join(lines), otp_code, raw_message

    if mode in ("classic_short", "classic_long"):
        lines = [
            f"📩 <b>NEW SMS • {country_part}{CARD_WIDTH_PAD}</b>",
            f"<b>✦\u00a0\u00a0{source_escaped}</b>",
            f"<b>✦\u00a0\u00a0{num_escaped}</b>",
        ]
        if mode == "classic_long" and raw_message:
            disp = raw_message if len(raw_message) <= 3000 else (raw_message[:3000] + "... [truncated]")
            styled = style_sms_with_copyable_otp(disp, otp_code, width=18)
            lines.append(
                f"<blockquote expandable>⠀\n{COLLAPSED_SMS_HINT}\n⠀\n{QUOTE_DIVIDER}\n💬 <b>FULL SMS</b>\n{styled}</blockquote>"
            )
        return "\n".join(lines), otp_code, raw_message

    lines = [
        f"📩 <b>NEW SMS • {country_part}{CARD_WIDTH_PAD}</b>",
        "",
        f"<b>✦\u00a0\u00a0{source_escaped}</b>",
        f"<b>✦\u00a0\u00a0{num_escaped}</b>",
    ]
    if mode == "long" and raw_message:
        disp = raw_message if len(raw_message) <= 3000 else (raw_message[:3000] + "... [truncated]")
        styled = style_sms_with_copyable_otp(disp, otp_code, width=18)
        lines.append("")
        lines.append(
            f"<blockquote expandable>⠀\n{COLLAPSED_SMS_HINT}\n⠀\n{QUOTE_DIVIDER}\n💬 <b>FULL SMS</b>\n{styled}</blockquote>"
        )
    else:
        lines.append("⠀")
    return "\n".join(lines), otp_code, raw_message


def build_otp_keyboard(otp_code: str, msg_id: str = "", include_custom_buttons: bool = True) -> Optional[InlineKeyboardMarkup]:
    rows: List[List[InlineKeyboardButton]] = []
    otp_btn: Optional[InlineKeyboardButton] = None
    otp_clean = str(otp_code or "").strip()[:256]
    if otp_clean:
        if CopyTextButton is not None:
            otp_btn = InlineKeyboardButton(otp_clean[:64], copy_text=CopyTextButton(text=otp_clean))
        elif msg_id:
            otp_btn = InlineKeyboardButton(otp_clean[:64], callback_data=f"sms_{msg_id[:40]}")
        else:
            otp_btn = InlineKeyboardButton(otp_clean[:64], callback_data=f"copy_num_{otp_clean[:40]}")

    custom_btns: List[InlineKeyboardButton] = []
    if include_custom_buttons:
        for cb in get_custom_link_buttons():
            u = normalize_button_url(cb.get("url", "")) or ""
            if cb.get("text") and u.startswith(("http://", "https://", "tg://")):
                custom_btns.append(InlineKeyboardButton(cb["text"], url=u))

    if otp_btn and custom_btns:
        if len(custom_btns) <= 2:
            rows.append([otp_btn] + custom_btns)
        else:
            rows.append([otp_btn, custom_btns[0]])
            for i in range(1, len(custom_btns), 2):
                rows.append(custom_btns[i:i + 2])
    elif otp_btn:
        rows.append([otp_btn])
    elif custom_btns:
        for i in range(0, len(custom_btns), 2):
            rows.append(custom_btns[i:i + 2])

    return InlineKeyboardMarkup(rows) if rows else None


# ==========================================
# OTP Dedup, Cache & Retention
# ==========================================
def cache_sms_message(key: str, message: str) -> None:
    if not key or not message:
        return
    now_t = time.time()
    _SMS_MESSAGE_CACHE[key] = (message, now_t)
    if len(_SMS_MESSAGE_CACHE) > 2500:
        cutoff = now_t - (28 * 3600)
        for k in [k for k, v in _SMS_MESSAGE_CACHE.items() if v[1] < cutoff]:
            _SMS_MESSAGE_CACHE.pop(k, None)


def get_cached_sms_message(key: str) -> Optional[str]:
    entry = _SMS_MESSAGE_CACHE.get(key)
    if entry:
        return entry[0]
    with get_main_db() as conn:
        row = conn.execute("SELECT raw_message FROM processed_otps WHERE id = ? LIMIT 1;", (key,)).fetchone()
        return str(row["raw_message"]) if row and row["raw_message"] else None


def is_message_duplicate(mid: str, item: Optional[Dict[str, Any]] = None) -> bool:
    if mid and mid in seen_message_ids:
        return True
    raw_num = str(item.get("number") or item.get("destinationNumber") or "") if item else ""
    digits = re.sub(r"\D", "", raw_num)
    raw_msg = str(item.get("sms") or item.get("messageBody") or item.get("message") or "") if item else ""
    otp_code = extract_otp_code(raw_msg)
    item_ts = parse_message_timestamp(str(item.get("received_at") or item.get("receivedAt") or "")) if item else 0.0

    if digits:
        if otp_code and f"otp:{digits}:{otp_code}" in seen_content_signatures:
            prev_ts = seen_content_signatures[f"otp:{digits}:{otp_code}"]
            if item_ts <= 0 or prev_ts <= 0 or abs(item_ts - prev_ts) <= 20.0:
                if mid:
                    seen_message_ids.add(mid)
                return True
        if raw_msg:
            sig = f"body:{digits}:{re.sub(r'\s+', ' ', raw_msg).strip()}"
            if sig in seen_content_signatures:
                prev_ts = seen_content_signatures[sig]
                if item_ts <= 0 or prev_ts <= 0 or abs(item_ts - prev_ts) <= 20.0:
                    if mid:
                        seen_message_ids.add(mid)
                    return True

    try:
        with get_main_db() as conn:
            if mid and conn.execute("SELECT 1 FROM processed_otps WHERE id = ? LIMIT 1;", (mid,)).fetchone():
                seen_message_ids.add(mid)
                return True
    except Exception:
        pass
    return False


def save_processed_otp_message(
    item: Dict[str, Any],
    country: str = "",
    otp_code: str = "",
    group_forwarded: bool = False,
    inbox_delivered: bool = False,
    user_id: int = 0,
    status: str = "processed",
) -> None:
    mid = str(item.get("id") or "").strip()
    if not mid:
        return
    raw_msg = str(item.get("sms") or item.get("messageBody") or item.get("message") or "").strip()
    raw_num = str(item.get("number") or item.get("destinationNumber") or "").strip()
    raw_src = str(item.get("sender_id") or item.get("sourceAddress") or item.get("source") or "").strip()
    raw_recv = str(item.get("received_at") or item.get("receivedAt") or "").strip()
    recv_ep = parse_message_timestamp(raw_recv) or time.time()
    exp_ep = recv_ep + (28 * 3600)
    exp_iso = datetime.fromtimestamp(exp_ep, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with get_main_db() as conn:
        conn.execute(
            """INSERT INTO processed_otps
               (id, source, country, number, otp_code, raw_message, received_at,
                received_at_epoch, expires_at, expires_at_epoch, group_forwarded, inbox_delivered, user_id, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                   group_forwarded = MAX(processed_otps.group_forwarded, excluded.group_forwarded),
                   inbox_delivered = MAX(processed_otps.inbox_delivered, excluded.inbox_delivered),
                   user_id = CASE WHEN excluded.user_id > 0 THEN excluded.user_id ELSE processed_otps.user_id END,
                   status = excluded.status;""",
            (mid, raw_src, country, raw_num, otp_code, raw_msg, raw_recv,
             recv_ep, exp_iso, exp_ep, 1 if group_forwarded else 0, 1 if inbox_delivered else 0, user_id, status),
        )
        conn.commit()
    set_bot_setting("last_active_epoch", str(time.time()), from_gist=True)


def preload_seen_otps_from_db() -> None:
    cleanup_old_otp_messages(max_age_hours=28)
    now_t = time.time()
    cutoff = now_t - (28 * 3600)
    with get_main_db() as conn:
        rows = conn.execute(
            "SELECT id, number, otp_code, raw_message, received_at_epoch FROM processed_otps WHERE COALESCE(received_at_epoch, 0) >= ? ORDER BY forwarded_at DESC LIMIT 10000;",
            (cutoff,),
        ).fetchall()
        for r in rows:
            mid = str(r["id"])
            seen_message_ids.add(mid)
            ts = float(r["received_at_epoch"] or now_t)
            seen_timestamps[mid] = ts
            dig = re.sub(r"\D", "", str(r["number"] or ""))
            if dig and r["otp_code"]:
                seen_content_signatures[f"otp:{dig}:{r['otp_code']}"] = ts
            if dig and r["raw_message"]:
                seen_content_signatures[f"body:{dig}:{re.sub(r'\s+', ' ', str(r['raw_message'])).strip()}"] = ts


def cleanup_old_otp_messages(max_age_hours: int = 28) -> None:
    cutoff = time.time() - (max_age_hours * 3600)
    with get_main_db() as conn:
        conn.execute("DELETE FROM processed_otps WHERE expires_at_epoch IS NOT NULL AND expires_at_epoch < ?;", (time.time(),))
        conn.commit()
    for mid in [k for k, v in seen_timestamps.items() if v < cutoff]:
        seen_timestamps.pop(mid, None)
        seen_message_ids.discard(mid)


def get_otp_dashboard_stats() -> Tuple[int, int]:
    now_bd = get_bangladesh_now()
    start_bd = now_bd.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).timestamp()
    with get_main_db() as conn:
        today = int(conn.execute("SELECT COUNT(*) FROM processed_otps WHERE COALESCE(received_at_epoch, 0) >= ?;", (start_bd,)).fetchone()[0])
        total_28h = int(conn.execute("SELECT COUNT(*) FROM processed_otps;").fetchone()[0])
    return today, total_28h


# ==========================================
# Thirdwave API Client & Telegram Retry Sender
# ==========================================
class ThirdwaveClient:
    def __init__(self, base_url: str, api_key: str, timeout: float = 8.0):
        self.base_url = self.clean_base_url(base_url)
        self.api_key = (api_key or "").strip()
        self.timeout = timeout
        self.cooldown_until: float = 0.0
        self._client: Optional[httpx.AsyncClient] = None

    @staticmethod
    def clean_base_url(url: str) -> str:
        clean = (url or "").strip().rstrip("/")
        if clean.endswith("/api/v1/traffic"):
            clean = clean[:-len("/api/v1/traffic")].rstrip("/")
        if clean and not clean.startswith(("http://", "https://")):
            clean = f"https://{clean}"
        return clean or "https://thirdwave.im"

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                limits=httpx.Limits(max_keepalive_connections=10, max_connections=20, keepalive_expiry=60.0),
                headers={"Accept": "application/json", "User-Agent": "MotherBot/1.0"},
            )
        return self._client

    def update_credentials(self, base_url: Optional[str] = None, api_key: Optional[str] = None) -> None:
        if base_url is not None:
            self.base_url = self.clean_base_url(base_url)
        if api_key is not None:
            self.api_key = api_key.strip()
        self._client = None

    async def close(self) -> None:
        if self._client is not None and not self._client.is_closed:
            try:
                await self._client.aclose()
            except Exception:
                pass
            self._client = None

    async def fetch_incoming_messages(self, limit: int = 100) -> List[Dict[str, Any]]:
        if time.time() < self.cooldown_until or not self.api_key:
            return []
        try:
            res = await self._get_client().get(
                f"{self.base_url}/api/v1/traffic",
                params={"api_key": self.api_key, "limit": min(max(1, limit), 100)},
            )
            if res.is_success:
                data = res.json()
                raw_items = data.get("items") or data.get("rows") or [] if isinstance(data, dict) else (data if isinstance(data, list) else [])
                seen: Set[str] = set()
                out: List[Dict[str, Any]] = []
                for r in raw_items:
                    if not isinstance(r, dict):
                        continue
                    num = str(r.get("number") or r.get("destinationNumber") or "").strip()
                    src = str(r.get("sender_id") or r.get("sourceAddress") or r.get("source") or r.get("caller") or r.get("sender") or "").strip()
                    sms = str(r.get("sms") or r.get("messageBody") or r.get("message") or r.get("text") or r.get("body") or r.get("otp") or "").strip()
                    recv = str(r.get("received_at") or r.get("receivedAt") or r.get("messageTime") or r.get("createdAt") or "").strip()
                    mid = str(r.get("id") or "").strip() or hashlib.sha256(f"{num}|{recv}|{src}|{sms}".encode("utf-8")).hexdigest()[:20]
                    if mid in seen:
                        continue
                    seen.add(mid)
                    d = dict(r)
                    d["id"] = mid
                    out.append(d)
                return out
            if res.status_code == 429:
                ra = res.headers.get("Retry-After", "")
                try:
                    wait = max(5.0, min(float(ra), 60.0)) if ra else 10.0
                except Exception:
                    wait = 10.0
                self.cooldown_until = time.time() + wait
                logger.warning(f"Thirdwave 429 rate limit, backing off {wait:.1f}s")
            elif res.status_code == 503:
                self.cooldown_until = time.time() + 5.0
        except Exception as e:
            logger.debug(f"Thirdwave fetch error: {e}")
        return []


def get_effective_base_url() -> str:
    return ThirdwaveClient.clean_base_url(get_bot_setting("thirdwave_base_url", "") or THIRDWAVE_BASE_URL)


def get_effective_api_key() -> str:
    return (get_bot_setting("thirdwave_api_key", "") or THIRDWAVE_API_KEY).strip()


thirdwave_client = ThirdwaveClient(base_url=get_effective_base_url(), api_key=get_effective_api_key())


async def send_with_retry(
    bot: Bot,
    chat_id: int,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    max_retries: int = 3,
) -> bool:
    safe_text = text if len(text) <= 4090 else (text[:4080] + "\n... [truncated]")
    for attempt in range(1, max_retries + 1):
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=safe_text,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
                disable_web_page_preview=True,
            )
            return True
        except RetryAfter as ra:
            await asyncio.sleep(float(ra.retry_after) + 1.0)
        except (TimedOut, NetworkError):
            await asyncio.sleep(2.0 * attempt)
        except BadRequest as b_err:
            logger.warning(f"Telegram BadRequest ({b_err}) for {chat_id}; retrying plain text...")
            plain = re.sub(r"<[^>]+>", "", safe_text)
            for fb_markup in ((reply_markup, None) if reply_markup is not None else (None,)):
                try:
                    await bot.send_message(chat_id=chat_id, text=plain, reply_markup=fb_markup, disable_web_page_preview=True)
                    return True
                except Exception:
                    pass
            return False
        except Exception as e:
            logger.warning(f"Telegram send error ({chat_id}): {e}")
            return False
    return False


# ==========================================
# Unified Gist Cloud Backup & Restore
# ==========================================
GIST_HEADERS = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}


class GistStorage:
    def __init__(self, gist_id: str, token: str, filename: str = "mother_bot_state.json"):
        self.gist_id = gist_id or get_bot_setting("gist_id", "")
        self.candidate_tokens: List[str] = list(_GIST_TOKEN_CANDIDATES)
        if token and token not in self.candidate_tokens:
            self.candidate_tokens.insert(0, token)
        self.token = self.candidate_tokens[0] if self.candidate_tokens else token
        self.filename = filename
        self.enabled = bool(self.candidate_tokens)
        self.api_url = f"https://api.github.com/gists/{self.gist_id}" if self.gist_id else ""
        self._lock: Optional[asyncio.Lock] = None
        self._dirty: bool = False
        self._last_sync_time: float = 0.0

    def mark_dirty(self) -> None:
        self._dirty = True

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _auth_headers(self, tok: Optional[str] = None) -> Dict[str, str]:
        return {**GIST_HEADERS, "Authorization": f"Bearer {tok or self.token}"}

    async def ensure_gist(self) -> bool:
        """Finds existing private Gist matching filename, cleans duplicates, or auto-creates one (no GIST_ID needed)."""
        import json
        if not self.candidate_tokens:
            return False
        if not self.gist_id:
            try:
                saved_id = get_bot_setting("gist_id", "")
                if saved_id:
                    self.gist_id = saved_id.strip()
                    self.api_url = f"https://api.github.com/gists/{self.gist_id}"
            except Exception:
                pass
        for tok in list(self.candidate_tokens):
            headers = self._auth_headers(tok)
            try:
                async with httpx.AsyncClient(timeout=15.0) as http:
                    if self.gist_id:
                        chk = await http.get(f"https://api.github.com/gists/{self.gist_id}", headers=headers)
                        if chk.is_success:
                            self.token = tok
                            self.api_url = f"https://api.github.com/gists/{self.gist_id}"
                            try:
                                set_bot_setting("gist_id", self.gist_id, from_gist=True)
                            except Exception:
                                pass
                            return True
                        elif chk.status_code == 404:
                            self.gist_id = ""
                            self.api_url = ""
                    res = await http.get("https://api.github.com/gists", headers=headers, params={"per_page": 100})
                    if res.is_success:
                        self.token = tok
                        matching = [g for g in res.json() if self.filename in g.get("files", {})]
                        if matching:
                            primary = matching[0]
                            self.gist_id = primary["id"]
                            self.api_url = f"https://api.github.com/gists/{self.gist_id}"
                            set_bot_setting("gist_id", self.gist_id, from_gist=True)
                            logger.info(f"☁️ Reusing primary existing Mother Bot Gist: {self.gist_id}")
                            for dup in matching[1:]:
                                dup_id = dup.get("id")
                                if dup_id:
                                    try:
                                        await http.delete(f"https://api.github.com/gists/{dup_id}", headers=headers)
                                        logger.info(f"🗑️ Deleted duplicate leftover Gist: {dup_id}")
                                    except Exception:
                                        pass
                            return True
                        payload = {
                            "description": "Mother Control Bot — Unified Cloud State",
                            "public": False,
                            "files": {self.filename: {"content": json.dumps({"bot": "MOTHER_BOT", "bot_settings": {}}, indent=2)}},
                        }
                        cres = await http.post("https://api.github.com/gists", headers=headers, json=payload)
                        if cres.is_success:
                            self.gist_id = cres.json()["id"]
                            self.api_url = f"https://api.github.com/gists/{self.gist_id}"
                            set_bot_setting("gist_id", self.gist_id, from_gist=True)
                            logger.info(f"☁️ Auto-created new private persistent Mother Bot Gist: {self.gist_id}")
                            return True
            except Exception as e:
                logger.warning(f"Gist ensure error: {e}")
        return False

    async def export_and_sync(self, is_handover: bool = False, immediate: bool = False) -> bool:
        import json
        if not self.enabled:
            return False
        now = time.monotonic()
        lock = self._get_lock()
        if not is_handover and not immediate:
            if lock.locked() or (now - self._last_sync_time) < 15.0:
                self._dirty = True
                return True
        async with lock:
            if not self.gist_id and not await self.ensure_gist():
                return False
            self._dirty = False
            try:
                countries_data: Dict[str, Any] = {}
                used_data: Dict[str, List[str]] = {}
                with get_main_db() as conn:
                    for c in conn.execute("SELECT id, name, button_name, service_name FROM countries;").fetchall():
                        cid = int(c["id"])
                        cname = format_country_name(c["name"])
                        with get_country_db(cid) as cconn:
                            countries_data[cname] = {
                                "standard": [r[0] for r in cconn.execute("SELECT number FROM available_numbers WHERE is_secret = 0 ORDER BY id ASC;").fetchall()],
                                "secret": [r[0] for r in cconn.execute("SELECT number FROM available_numbers WHERE is_secret = 1 ORDER BY id ASC;").fetchall()],
                                "button_name": c["button_name"] or "",
                                "service_name": c["service_name"] or "WhatsApp",
                            }
                            u_list = [r[0] for r in cconn.execute("SELECT number FROM used_numbers ORDER BY id ASC;").fetchall()]
                            if u_list:
                                used_data[cname] = u_list
                    users_data = [dict(r) for r in conn.execute("SELECT user_id, username, first_name, numbers_consumed, has_secret_access, prefer_plus, is_banned, joined_at FROM users;").fetchall()]
                    admins_data = [dict(r) for r in conn.execute("SELECT user_id, added_by, username, first_name FROM bot_admins;").fetchall()]
                    settings_data = {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM bot_settings;").fetchall()}

                cutoff_seen = time.time() - (28 * 3600)
                valid_seen = {k: v for k, v in seen_timestamps.items() if v >= cutoff_seen}
                payload = {
                    "description": "Mother Control Bot — Unified Cloud State",
                    "files": {
                        self.filename: {
                            "content": json.dumps({
                                "bot": "MOTHER_BOT",
                                "updated_at": datetime.now(timezone.utc).isoformat(),
                                "bot_settings": settings_data,
                                "countries": countries_data,
                                "used_countries": used_data,
                                "users": users_data,
                                "admins": admins_data,
                                "seen_otps": valid_seen,
                                "handover": is_handover,
                                "handover_epoch": time.time() if is_handover else 0.0,
                            }, indent=2)
                        }
                    },
                }
                async with httpx.AsyncClient(timeout=20.0) as http:
                    for tok in list(self.candidate_tokens):
                        res = await http.patch(self.api_url, headers=self._auth_headers(tok), json=payload)
                        if res.status_code == 404:
                            self.gist_id = ""
                            self.api_url = ""
                            if await self.ensure_gist():
                                res = await http.patch(self.api_url, headers=self._auth_headers(self.token), json=payload)
                            else:
                                break
                        if res.is_success:
                            self.token = tok
                            self._last_sync_time = time.monotonic()
                            return True
            except Exception as e:
                logger.warning(f"Gist export error: {e}")
        return False

    async def restore_from_gist(self) -> Dict[str, Any]:
        import json
        if not self.enabled or (not self.gist_id and not await self.ensure_gist()):
            return {}
        try:
            async with httpx.AsyncClient(timeout=20.0) as http:
                for tok in list(self.candidate_tokens):
                    res = await http.get(self.api_url, headers=self._auth_headers(tok))
                    if res.status_code == 404:
                        self.gist_id = ""
                        self.api_url = ""
                        if await self.ensure_gist():
                            res = await http.get(self.api_url, headers=self._auth_headers(self.token))
                    if not res.is_success:
                        continue
                    files = res.json().get("files", {})
                    if self.filename not in files:
                        return {}
                    parsed = json.loads(files[self.filename].get("content", "{}"))
                    saved_settings = parsed.get("bot_settings", {})
                    if isinstance(saved_settings, dict):
                        local_ts = float(get_bot_setting("_settings_updated_at", "0") or 0.0)
                        cloud_ts = float(saved_settings.get("_settings_updated_at") or 0.0)
                        cloud_newer = (cloud_ts >= local_ts) or (local_ts == 0.0)
                        for sk, sv in saved_settings.items():
                            if cloud_newer or not get_bot_setting(str(sk), ""):
                                set_bot_setting(str(sk), str(sv), from_gist=True)
                        thirdwave_client.update_credentials(get_effective_base_url(), get_effective_api_key())

                    cutoff = time.time() - (28 * 3600)
                    for smid, sts in (parsed.get("seen_otps") or {}).items():
                        try:
                            if float(sts) >= cutoff:
                                seen_message_ids.add(smid)
                                seen_timestamps[smid] = float(sts)
                        except Exception:
                            pass

                    del_users = _get_json_dict_setting("deleted_users")
                    rev_admins = _get_json_dict_setting("revoked_admins")
                    with get_main_db() as mconn:
                        for u in parsed.get("users", []):
                            uid = u.get("user_id")
                            if not uid or str(uid) in del_users:
                                continue
                            mconn.execute(
                                """INSERT INTO users (user_id, username, first_name, numbers_consumed, has_secret_access, prefer_plus, is_banned)
                                   VALUES (?, ?, ?, ?, ?, ?, ?)
                                   ON CONFLICT(user_id) DO UPDATE SET
                                       numbers_consumed = max(users.numbers_consumed, excluded.numbers_consumed),
                                       has_secret_access = excluded.has_secret_access,
                                       prefer_plus = excluded.prefer_plus;""",
                                (uid, u.get("username", ""), u.get("first_name", ""), u.get("numbers_consumed", 0),
                                 u.get("has_secret_access", 0), u.get("prefer_plus", 1), u.get("is_banned", 0)),
                            )
                        for a in parsed.get("admins", []):
                            aid = a.get("user_id")
                            if aid and str(aid) not in rev_admins:
                                mconn.execute(
                                    "INSERT OR IGNORE INTO bot_admins (user_id, added_by, username, first_name) VALUES (?, ?, ?, ?);",
                                    (aid, a.get("added_by", 0), a.get("username", ""), a.get("first_name", "")),
                                )
                        mconn.commit()

                    for cname, ulist in (parsed.get("used_countries") or {}).items():
                        if _is_country_explicitly_deleted(cname):
                            continue
                        cid = get_or_create_country(cname, from_gist=True)
                        with get_country_db(cid) as cconn:
                            for unum in ulist:
                                snum = sanitize_phone_number(unum)
                                if snum:
                                    CONSUMED_NUMBERS_CACHE.add(snum)
                                    cconn.execute("DELETE FROM available_numbers WHERE number = ?;", (snum,))
                                    cconn.execute("INSERT OR IGNORE INTO used_numbers (number, user_id) VALUES (?, 0);", (snum,))
                            cconn.commit()

                    for cname, cstock in (parsed.get("countries") or {}).items():
                        if _is_country_explicitly_deleted(cname):
                            continue
                        cid = get_or_create_country(cname, from_gist=True)
                        if isinstance(cstock, dict):
                            if cstock.get("button_name"):
                                update_country_button_name(cid, str(cstock["button_name"]))
                            if cstock.get("service_name"):
                                update_country_service_name(cid, str(cstock["service_name"]))
                            std_nums = [n for n in cstock.get("standard", []) if sanitize_phone_number(n) not in CONSUMED_NUMBERS_CACHE]
                            sec_nums = [n for n in cstock.get("secret", []) if sanitize_phone_number(n) not in CONSUMED_NUMBERS_CACHE]
                            if std_nums:
                                add_numbers_to_country(cid, std_nums, is_secret=False, from_gist=True)
                            if sec_nums:
                                add_numbers_to_country(cid, sec_nums, is_secret=True, from_gist=True)
                    return parsed
        except Exception as e:
            logger.warning(f"Gist restore error: {e}")
        return {}


gist_storage = GistStorage(gist_id=GIST_ID, token=GIST_TOKEN)

