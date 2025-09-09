#!/usr/bin/env python3
import os
import json
import csv
import asyncio
import logging
from datetime import datetime, timedelta, date, time
from typing import Dict, Optional, List, Tuple
import re
import io

import pytz
import psycopg2
import psycopg2.extras
from telegram import Update
from telegram.constants import ChatType, ParseMode
from telegram.ext import Application, ContextTypes, MessageHandler, filters, JobQueue

# =============================
# LOGGING
# =============================
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("bot")

# =============================
# CONFIG
# =============================
def get_env_variable(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable '{var_name}'. "
            "Please set it in your hosting environment."
        )
    return value

BOT_TOKEN = get_env_variable("BOT_TOKEN")
DATABASE_URL = get_env_variable("DATABASE_URL")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "excelmerge")  # telegram username (without @)
WA_DAILY_LIMIT = int(os.getenv("WA_DAILY_LIMIT", "4"))      # max sends per number per logical day
REMINDER_DELAY_MINUTES = int(os.getenv("REMINDER_DELAY_MINUTES", "30")) # Delay for reminders

TIMEZONE = pytz.timezone("Asia/Phnom_Penh")

# =============================
# DATABASE SETUP & HELPERS
# =============================
db_lock = asyncio.Lock()

def get_db_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except psycopg2.OperationalError as e:
        log.error("Could not connect to database: %s", e)
        raise

def setup_database():
    log.info("Setting up database schema...")
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # KV store
            cur.execute("""
                CREATE TABLE IF NOT EXISTS kv_storage (
                    key TEXT PRIMARY KEY,
                    data JSONB NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            # Audit log
            cur.execute("""
                CREATE TABLE IF NOT EXISTS audit_log (
                    id SERIAL PRIMARY KEY,
                    ts_local TIMESTAMPTZ NOT NULL,
                    chat_id BIGINT,
                    message_id BIGINT,
                    user_id BIGINT,
                    user_first TEXT,
                    user_username TEXT,
                    kind TEXT,
                    action TEXT,
                    value TEXT,
                    owner TEXT
                );
            """)
            # Daily quota per WhatsApp number
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wa_daily_usage (
                    day DATE NOT NULL,
                    number_norm TEXT NOT NULL,
                    sent_count INTEGER NOT NULL DEFAULT 0,
                    last_sent TIMESTAMPTZ,
                    PRIMARY KEY (day, number_norm)
                );
            """)
            conn.commit()
        log.info("Database schema is ready.")
    finally:
        conn.close()

# =============================
# STATE (DB-backed)
# =============================
BASE_STATE = {
    "user_names": {},
    "rr": {
        "username_owner_idx": 0, "username_entry_idx": {},
        "wa_owner_idx": 0, "wa_entry_idx": {},
    },
    "issued": {"username": {}, "whatsapp": {}},
    "priority_queue": {
        "active": False,
        "owner": None,
        "remaining": 0,
        "stop_after": False,
        "saved_rr_indices": {}
    }
}
state: Dict = {k: (v.copy() if isinstance(v, dict) else v) for k, v in BASE_STATE.items()}

def load_state():
    global state
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM kv_storage WHERE key = 'state'")
            result = cur.fetchone()
            if result:
                loaded = result[0]
                state = {**BASE_STATE, **loaded}
                log.info("Bot state loaded from database.")
            else:
                state = {k: (v.copy() if isinstance(v, dict) else v) for k, v in BASE_STATE.items()}
                save_state()
    except Exception as e:
        log.warning("Failed to load state from DB: %s", e)
    finally:
        conn.close()

    state.setdefault("rr", {}).setdefault("username_entry_idx", {})
    state["rr"].setdefault("wa_entry_idx", {})
    state.setdefault("issued", {}).setdefault("username", {})
    state["issued"].setdefault("whatsapp", {})
    state.setdefault("priority_queue", BASE_STATE["priority_queue"])


def save_state():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO kv_storage (key, data)
                VALUES ('state', %s)
                ON CONFLICT (key) DO UPDATE
                SET data = EXCLUDED.data, updated_at = NOW();
            """, (json.dumps(state),))
            conn.commit()
    except Exception as e:
        log.warning("Failed to save state to DB: %s", e)
    finally:
        conn.close()

def _migrate_state_if_needed():
    """Ensure the 'issued' structure uses lists for multi-item tracking."""
    log.info("Checking state structure for migration...")
    state_was_changed = False
    issued = state.setdefault("issued", {})
    for kind in ("username", "whatsapp"):
        bucket = issued.setdefault(kind, {})
        for user_id, item_or_list in bucket.items():
            if isinstance(item_or_list, dict):
                bucket[user_id] = [item_or_list]
                state_was_changed = True
                log.info(f"Migrated user {user_id}'s '{kind}' data to new list format.")
    
    if state_was_changed:
        log.info("State structure was migrated. Saving new format to database.")
        save_state()
    else:
        log.info("State structure is already up-to-date.")

# =============================
# OWNER DIRECTORY (DB-backed)
# =============================
OWNER_DATA: List[dict] = []
HANDLE_INDEX: Dict[str, List[dict]] = {}
PHONE_INDEX: Dict[str, dict] = {}
USERNAME_POOL: List[Dict] = []
WHATSAPP_POOL: List[Dict] = []

def _norm_handle(h: str) -> str: return re.sub(r"^@", "", (h or "").strip().lower())
def _norm_phone(p: str) -> str: return re.sub(r"\D+", "", (p or ""))
def _norm_owner_name(s: str) -> str:
    s = (s or "").strip()
    if s.startswith("@"): s = s[1:]
    return s.lower()
def _is_admin(update: Update) -> bool:
    u = update.effective_user
    if not u: return False
    return (u.username or "").lower() == ADMIN_USERNAME.lower()
def _owner_is_paused(group: dict) -> bool:
    if group.get("disabled") or group.get("active") is False: return True
    until = group.get("disabled_until")
    if until:
        try:
            dt = datetime.fromisoformat(until)
            if dt.tzinfo is None: dt = TIMEZONE.localize(dt)
            return datetime.now(tz=TIMEZONE) < dt
        except Exception:
            return False
    return False

def _ensure_owner_shape(g: dict) -> dict:
    g.setdefault("owner", "")
    g.setdefault("disabled", False)
    g.setdefault("entries", [])
    g.setdefault("whatsapp", [])
    # entries
    norm_entries = []
    for e in g.get("entries", []):
        if isinstance(e, dict):
            e.setdefault("telegram", ""); e.setdefault("phone", ""); e.setdefault("disabled", False)
            norm_entries.append(e)
    g["entries"] = norm_entries
    # whatsapp
    norm_wa = []
    for w in g.get("whatsapp", []):
        if isinstance(w, dict):
            w.setdefault("number", w.get("number") or w.get("phone") or "")
            w.setdefault("disabled", False)
            if (w["number"] or "").strip():
                norm_wa.append({"number": w["number"].strip(), "disabled": bool(w.get("disabled", False))})
        elif isinstance(w, str) and w.strip():
            norm_wa.append({"number": w.strip(), "disabled": False})
    g["whatsapp"] = norm_wa
    return g

def save_owner_directory():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO kv_storage (key, data)
                VALUES ('owners', %s)
                ON CONFLICT (key) DO UPDATE
                SET data = EXCLUDED.data, updated_at = NOW();
            """, (json.dumps(OWNER_DATA),))
            conn.commit()
    except Exception as e:
        log.error("Failed to save owner directory to DB: %s", e)
    finally:
        conn.close()

def load_owner_directory():
    global OWNER_DATA, HANDLE_INDEX, PHONE_INDEX, USERNAME_POOL, WHATSAPP_POOL
    OWNER_DATA, HANDLE_INDEX, PHONE_INDEX = [], {}, {}
    USERNAME_POOL, WHATSAPP_POOL = [], []

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM kv_storage WHERE key = 'owners'")
            result = cur.fetchone()
            OWNER_DATA = (result[0] or []) if result else []
    except Exception as e:
        log.error("Failed to load owners from DB: %s", e)
        OWNER_DATA = []
    finally:
        conn.close()

    OWNER_DATA = [_ensure_owner_shape(dict(g)) for g in OWNER_DATA]

    for group in OWNER_DATA:
        owner = _norm_owner_name(group.get("owner") or "")
        if not owner or _owner_is_paused(group): continue
        # usernames
        usernames: List[str] = []
        for entry in group.get("entries", []):
            if entry.get("disabled"): continue
            tel = (entry.get("telegram") or "").strip()
            ph  = (entry.get("phone") or "").strip()
            if tel:
                handle_shown = tel if tel.startswith("@") else f"@{tel}"
                usernames.append(handle_shown)
                HANDLE_INDEX.setdefault(_norm_handle(tel), []).append(
                    {"owner": owner, "phone": ph, "telegram": tel, "channel": "telegram"})
            if ph:
                PHONE_INDEX[_norm_phone(ph)] = {"owner": owner, "phone": ph, "telegram": tel, "channel": "telegram"}
        if usernames: USERNAME_POOL.append({"owner": owner, "usernames": usernames})
        # whatsapp
        numbers: List[str] = []
        for w in group.get("whatsapp", []):
            if w.get("disabled"): continue
            num = (w.get("number") or "").strip()
            if num:
                numbers.append(num)
                PHONE_INDEX[_norm_phone(num)] = {"owner": owner, "phone": num, "telegram": None, "channel": "whatsapp"}
        if numbers: WHATSAPP_POOL.append({"owner": owner, "numbers": numbers})

    log.info("[owner_directory] owners(active): usernames=%d, whatsapp=%d | handles=%d, phones=%d",
             len(USERNAME_POOL), len(WHATSAPP_POOL), len(HANDLE_INDEX), len(PHONE_INDEX))

# =============================
# QUOTA: per-number per-day
# =============================
def _logical_day_today() -> date:
    now = datetime.now(TIMEZONE)
    return (now - timedelta(hours=5, minutes=30)).date()

def _wa_get_count(number_norm: str, day: date) -> int:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT sent_count FROM wa_daily_usage WHERE day=%s AND number_norm=%s",
                        (day, number_norm))
            row = cur.fetchone()
            return int(row[0]) if row else 0
    except Exception as e:
        log.warning("Quota read failed: %s", e)
        return 0
    finally:
        conn.close()

def _wa_inc_count(number_norm: str, day: date):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO wa_daily_usage (day, number_norm, sent_count, last_sent)
                VALUES (%s, %s, 1, %s)
                ON CONFLICT (day, number_norm)
                DO UPDATE SET sent_count = wa_daily_usage.sent_count + 1,
                              last_sent = EXCLUDED.last_sent
            """, (day, number_norm, datetime.now(TIMEZONE)))
            conn.commit()
    except Exception as e:
        log.warning("Quota write failed: %s", e)
    finally:
        conn.close()

def _wa_quota_reached(number_raw: str) -> bool:
    n = _norm_phone(number_raw)
    cnt = _wa_get_count(n, _logical_day_today())
    return cnt >= WA_DAILY_LIMIT

# =============================
# Rotation-preserving rebuild & Round-robin helpers
# =============================
def _owner_list_from_pool(pool) -> List[str]: return [blk["owner"] for blk in pool]
def _preserve_owner_pointer(old_list: List[str], new_list: List[str], old_idx: int) -> int:
    if not new_list: return 0
    old_list = list(old_list or [])
    if not old_list: return 0
    old_idx = old_idx % len(old_list)
    start_owner = old_list[old_idx]
    if start_owner in new_list: return new_list.index(start_owner)
    n = len(old_list)
    for step in range(1, n + 1):
        cand = old_list[(old_idx + step) % n]
        if cand in new_list: return new_list.index(cand)
    return 0

def _preserve_entry_indices(rr_map: Dict[str, int], new_pool: List[Dict], list_key: str):
    valid_owners = {blk["owner"]: len(blk.get(list_key, []) or []) for blk in new_pool}
    for owner in list(rr_map.keys()):
        if owner not in valid_owners: rr_map.pop(owner, None)
    for owner, sz in valid_owners.items():
        if sz <= 0: rr_map.pop(owner, None)
        else: rr_map[owner] = rr_map.get(owner, 0) % sz

async def _rebuild_pools_preserving_rotation():
    """Persist OWNER_DATA to DB, then rebuild pools while preserving rotation pointers."""
    old_user_owner_list = _owner_list_from_pool(USERNAME_POOL)
    old_wa_owner_list   = _owner_list_from_pool(WHATSAPP_POOL)

    rr = state.setdefault("rr", {})
    old_user_owner_idx = rr.get("username_owner_idx", 0)
    old_wa_owner_idx   = rr.get("wa_owner_idx", 0)
    old_user_entry_idx = dict(rr.get("username_entry_idx", {}))
    old_wa_entry_idx   = dict(rr.get("wa_entry_idx", {}))

    # IMPORTANT: save first so in-memory edits aren’t lost
    save_owner_directory()
    # then reload + rebuild indexes/pools from DB snapshot
    load_owner_directory()

    new_user_owner_list = _owner_list_from_pool(USERNAME_POOL)
    new_wa_owner_list   = _owner_list_from_pool(WHATSAPP_POOL)

    rr["username_owner_idx"] = _preserve_owner_pointer(
        old_user_owner_list, new_user_owner_list, old_user_owner_idx
    )
    rr["wa_owner_idx"] = _preserve_owner_pointer(
        old_wa_owner_list, new_wa_owner_list, old_wa_owner_idx
    )

    rr.setdefault("username_entry_idx", old_user_entry_idx)
    rr.setdefault("wa_entry_idx", old_wa_entry_idx)
    _preserve_entry_indices(rr["username_entry_idx"], USERNAME_POOL, "usernames")
    _preserve_entry_indices(rr["wa_entry_idx"], WHATSAPP_POOL, "numbers")

    save_state()

async def _decrement_priority_and_end_if_needed():
    pq = state.get("priority_queue", {})
    if not pq.get("active"):
        return

    pq["remaining"] -= 1
    
    if pq["remaining"] <= 0:
        log.info(f"Priority queue for owner {pq['owner']} completed.")
        saved_indices = pq.get("saved_rr_indices", {})
        state["rr"]["username_owner_idx"] = saved_indices.get("username_owner_idx", 0)
        state["rr"]["wa_owner_idx"] = saved_indices.get("wa_owner_idx", 0)

        stop_after = pq.get("stop_after", False)
        owner_to_stop = pq.get("owner")

        # Reset priority queue state
        state["priority_queue"] = BASE_STATE["priority_queue"]

        if stop_after and owner_to_stop:
            log.info(f"Auto-stopping owner {owner_to_stop} after priority queue completion.")
            owner_group = _find_owner_group(owner_to_stop)
            if owner_group:
                owner_group["disabled"] = True
            await _rebuild_pools_preserving_rotation() # This saves owner_data and reloads pools
        else:
            save_state()
    else:
        save_state()

async def _next_from_username_pool() -> Optional[Dict[str, str]]:
    pq = state.get("priority_queue", {})
    if pq.get("active"):
        priority_owner = pq.get("owner")
        for block in USERNAME_POOL:
            if block["owner"] == priority_owner:
                arr = block.get("usernames", [])
                if arr:
                    ei = state["rr"]["username_entry_idx"].get(priority_owner, 0) % len(arr)
                    result = {"owner": priority_owner, "username": arr[ei]}
                    state["rr"]["username_entry_idx"][priority_owner] = (ei + 1) % len(arr)
                    await _decrement_priority_and_end_if_needed()
                    return result
        log.warning(f"Priority owner {priority_owner} not found in USERNAME_POOL or has no usernames.")
        return None

    if not USERNAME_POOL: return None
    rr = state["rr"]
    idx = rr.get("username_owner_idx", 0) % len(USERNAME_POOL)
    for _ in range(len(USERNAME_POOL)):
        block = USERNAME_POOL[idx]
        arr = block.get("usernames", [])
        if arr:
            ei = rr["username_entry_idx"].get(block["owner"], 0) % len(arr)
            result = {"owner": block["owner"], "username": arr[ei]}
            rr["username_entry_idx"][block["owner"]] = (ei + 1) % len(arr)
            rr["username_owner_idx"] = (idx + 1) % len(USERNAME_POOL)
            save_state()
            return result
        idx = (idx + 1) % len(USERNAME_POOL)
    return None

async def _next_from_whatsapp_pool() -> Optional[Dict[str, str]]:
    pq = state.get("priority_queue", {})
    if pq.get("active"):
        priority_owner = pq.get("owner")
        for block in WHATSAPP_POOL:
            if block["owner"] == priority_owner:
                numbers = block.get("numbers", []) or []
                if numbers:
                    start = state["rr"]["wa_entry_idx"].get(priority_owner, 0) % len(numbers)
                    for step in range(len(numbers)):
                        cand = numbers[(start + step) % len(numbers)]
                        if not _wa_quota_reached(cand):
                            state["rr"]["wa_entry_idx"][priority_owner] = ((start + step) + 1) % len(numbers)
                            await _decrement_priority_and_end_if_needed()
                            return {"owner": priority_owner, "number": cand}
        log.warning(f"Priority owner {priority_owner} not found in WHATSAPP_POOL or has no available numbers.")
        return None

    if not WHATSAPP_POOL: return None
    rr = state["rr"]
    owner_idx = rr.get("wa_owner_idx", 0) % len(WHATSAPP_POOL)
    for _ in range(len(WHATSAPP_POOL)):
        block = WHATSAPP_POOL[owner_idx]
        owner = block["owner"]
        numbers = block.get("numbers", []) or []
        if numbers:
            start = rr["wa_entry_idx"].get(owner, 0) % len(numbers)
            for step in range(len(numbers)):
                cand = numbers[(start + step) % len(numbers)]
                if _wa_quota_reached(cand):
                    continue
                rr["wa_entry_idx"][owner] = ((start + step) + 1) % len(numbers)
                rr["wa_owner_idx"] = (owner_idx + 1) % len(WHATSAPP_POOL)
                save_state()
                return {"owner": owner, "number": cand}
        owner_idx = (owner_idx + 1) % len(WHATSAPP_POOL)
    return None

# =============================
# REGEXES & HELPERS
# =============================
WHO_USING_REGEX = re.compile(
    r"^\s*who(?:['\u2019]s| is)\s+using\s+(?:@?([A-Za-z0-9_\.]+)|(\+?\d[\d\s\-]{6,}\d))\s*$",
    re.IGNORECASE
)
NEED_USERNAME_RX = re.compile(r"^\s*i\s*need\s*(?:user\s*name|username)\s*$", re.IGNORECASE)
NEED_WHATSAPP_RX = re.compile(r"^\s*i\s*need\s*(?:id\s*)?whats?app\s*$", re.IGNORECASE)

STOP_OPEN_RX          = re.compile(r"^\s*(stop|open)\s+(.+?)\s*$", re.IGNORECASE)
ADD_OWNER_RX          = re.compile(r"^\s*add\s+owner\s+@?(.+?)\s*$", re.IGNORECASE)
ADD_USERNAME_RX       = re.compile(r"^\s*add\s+username\s+@([A-Za-z0-9_]{3,})\s+to\s+@?(.+?)\s*$", re.IGNORECASE)
ADD_WHATSAPP_RX       = re.compile(r"^\s*add\s+whats?app\s+(\+?\d[\d\s\-]{6,}\d)\s+to\s+@?(.+?)\s*$", re.IGNORECASE)
DEL_OWNER_RX          = re.compile(r"^\s*delete\s+owner\s+@?(.+?)\s*$", re.IGNORECASE)
DEL_USERNAME_RX       = re.compile(r"^\s*delete\s+username\s+@([A-Za-z0-9_]{3,})\s*$", re.IGNORECASE)
DEL_WHATSAPP_RX       = re.compile(r"^\s*delete\s+whats?app\s+(\+?\d[\d\s\-]{6,}\d)\s*$", re.IGNORECASE)
LIST_OWNERS_RX        = re.compile(r"^\s*list\s+owners\s*$", re.IGNORECASE)
LIST_OWNER_DETAIL_RX  = re.compile(r"^\s*list\s+owner\s+@?(.+?)\s*$", re.IGNORECASE)
LIST_DISABLED_RX      = re.compile(r"^\s*list\s+disabled\s*$", re.IGNORECASE)
SEND_REPORT_RX        = re.compile(r"^\s*(?:send\s+report|report)(?:\s+(yesterday|today|\d{4}-\d{2}-\d{2}))?\s*$", re.IGNORECASE)
PHONE_LIKE_RX         = re.compile(r"^\+?\d[\d\s\-]{6,}\d$")
LIST_OWNER_ALIAS_RX   = re.compile(r"^\s*list\s+@?(.+?)\s*$", re.IGNORECASE)
REMIND_ALL_RX         = re.compile(r"^\s*remind\s+user\s*$", re.IGNORECASE)
TAKE_CUSTOMER_RX      = re.compile(r"^\s*take\s+(\d+)\s+customer(?:s)?\s+to\s+owner\s+@?(.+?)(?:\s+and\s+stop)?\s*$", re.IGNORECASE)


def _looks_like_phone(s: str) -> bool:
    return bool(PHONE_LIKE_RX.fullmatch((s or "").strip()))

def _parse_stop_open_target(raw: str) -> Tuple[str, str]:
    """
    Returns ('username'|'phone'|'owner', value)
    Accepts: '@handle', 'username @handle', '+123...', 'whatsapp +123...', 'Owner Name'
    """
    s = (raw or "").strip()
    low = s.lower()
    # explicit prefixes
    for pref in ("username ", "user ", "handle "):
        if low.startswith(pref):
            t = s[len(pref):].strip()
            if not t.startswith("@"): t = f"@{t}"
            return ("username", t)
    for pref in ("whatsapp ", "wa ", "phone ", "number ", "num "):
        if low.startswith(pref):
            return ("phone", s[len(pref):].strip())
    # infer from syntax
    if s.startswith("@"):
        return ("username", s)
    if _looks_like_phone(s):
        return ("phone", s)
    return ("owner", s)

# =============================
# AUDIT LOG (DB)
# =============================
def _log_event(kind: str, action: str, update: Update, value: str, owner: str = ""):
    conn = get_db_connection()
    try:
        u = update.effective_user
        m = update.effective_message
        c = update.effective_chat
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO audit_log (
                    ts_local, chat_id, message_id, user_id, user_first,
                    user_username, kind, action, value, owner
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                datetime.now(TIMEZONE), c.id if c else None, m.message_id if m else None,
                u.id if u else None, u.first_name if u else None, u.username if u else None,
                kind, action, value, owner or ""
            ))
            conn.commit()
    except Exception as e:
        log.warning("Log write to DB failed: %s", e)
    finally:
        conn.close()

# =============================
# UTIL
# =============================
def cache_user_info(user):
    state.setdefault("user_names", {})[str(user.id)] = {
        "first_name": user.first_name or "",
        "username": user.username or ""
    }

def mention_user_html(user_id: int) -> str:
    info = state.setdefault("user_names", {}).get(str(user_id), {})
    name = info.get("first_name") or info.get("username") or str(user_id)
    return f'<a href="tg://user?id={user_id}">{name}</a>'

def _issued_bucket(kind: str) -> Dict[str, list]: 
    return state.setdefault("issued", {}).setdefault(kind, {})

def _set_issued(user_id: int, chat_id: int, kind: str, value: str):
    bucket = _issued_bucket(kind)
    user_id_str = str(user_id)
    if user_id_str not in bucket:
        bucket[user_id_str] = []
    
    bucket[user_id_str].append({
        "value": value, 
        "ts": datetime.now(TIMEZONE).isoformat(),
        "chat_id": chat_id
    })
    save_state()

def _clear_issued(user_id: int, kind: str, value_to_clear: str):
    bucket = _issued_bucket(kind)
    user_id_str = str(user_id)
    if user_id_str in bucket:
        bucket[user_id_str] = [
            item for item in bucket[user_id_str] if item.get("value") != value_to_clear
        ]
        if not bucket[user_id_str]: # remove user if list is empty
            del bucket[user_id_str]
        save_state()

def _value_in_text(value: Optional[str], text: str) -> bool:
    if not value:
        return False
    
    v_norm = value.strip()
    text_norm = text or ""

    if v_norm.startswith('@'):
        potential_matches = re.findall(r'@([A-Za-z0-9_]+)', text_norm)
        normalized_value = v_norm.lstrip('@').lower()
        for match in potential_matches:
            if match.lower() == normalized_value:
                return True
        return False
    else:
        v_digits = re.sub(r'\D', '', v_norm)
        text_digits = re.sub(r'\D', '', text_norm)
        return v_digits and v_digits in text_digits

# =============================
# EXCEL (reads audit_log)
# =============================
def _logical_day_of(ts: datetime) -> date:
    shifted = ts.astimezone(TIMEZONE) - timedelta(hours=5, minutes=30)
    return shifted.date()

def _read_log_rows() -> List[dict]:
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT * FROM audit_log ORDER BY ts_local")
            return [dict(row) for row in cur.fetchall()]
    except Exception as e:
        log.error("Failed to read log rows from DB: %s", e)
        return []
    finally:
        conn.close()

def _compute_daily_summary(target_day: date) -> List[dict]:
    rows = _read_log_rows()
    day_rows = [r for r in rows if _logical_day_of(r["ts_local"]) == target_day]
    users: Dict[str, dict] = {}
    for r in day_rows:
        user_key = r.get("user_first") or r.get("user_username") or str(r.get("user_id"))
        d = users.setdefault(user_key, {"username_issued": [], "username_cleared": [], "wa_issued": [], "wa_cleared": []})
        kind, action, value, owner = r["kind"], r["action"], r["value"], r.get("owner","")
        if kind == "username":
            if action == "issued": d["username_issued"].append((value, owner))
            if action == "cleared": d["username_cleared"].append(value)
        elif kind == "whatsapp":
            if action == "issued": d["wa_issued"].append((value, owner))
            if action == "cleared": d["wa_cleared"].append(value)
    out = []
    for user, d in users.items():
        issued_user_pairs = d["username_issued"]; issued_wa_pairs = d["wa_issued"]
        issued_user_vals  = [v for v, _ in issued_user_pairs]; issued_wa_vals = [v for v, _ in issued_wa_pairs]
        notback_user = [v for v in issued_user_vals if v not in d["username_cleared"]]
        notback_wa = [v for v in issued_wa_vals if v not in d["wa_cleared"]]
        owners_user = sorted({own for (v, own) in issued_user_pairs if v in notback_user and own})
        owners_wa = sorted({own for (v, own) in issued_wa_pairs if v in notback_wa and own})
        out.append({
            "Day": target_day.isoformat(), "User": str(user),
            "Total username receive": len(issued_user_vals), "Total whatsapp receive": len(issued_wa_vals),
            "Total username provide back": len(d["username_cleared"]), "Total whatsapp provide back": len(d["wa_cleared"]),
            "Username not provide back": ", ".join(notback_user), "Owner of username": ", ".join(owners_user),
            "Whatsapp not provide back": ", ".join(notback_wa), "Owner of whatsapp": ", ".join(owners_wa),
        })
    out.sort(key=lambda r: r["User"].lower())
    return out

def _style_and_save_excel(rows: List[dict]) -> io.BytesIO:
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
    except ImportError:
        raise RuntimeError("openpyxl not installed. Please add it to requirements.txt")

    headers = ["Day","User","Total username receive","Total whatsapp receive","Total username provide back","Total whatsapp provide back", "Username not provide back","Owner of username","Whatsapp not provide back","Owner of whatsapp"]
    wb = Workbook(); ws = wb.active; ws.title = "Summary"; ws.append(headers)
    for r in rows: ws.append([r.get(h, "") for h in headers])
    header_fill = PatternFill(start_color="1F497D", end_color="1F497D", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True, size=11)
    center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    thin = Side(style="thin"); border = Border(left=thin, right=thin, top=thin, bottom=thin)
    for cell in ws[1]: cell.fill = header_fill; cell.font = header_font; cell.alignment = center; cell.border = border
    for i, row in enumerate(ws.iter_rows(min_row=2, max_row=ws.max_row, max_col=ws.max_column), start=2):
        fill = PatternFill(start_color="F2F2F2" if i % 2 == 0 else "FFFFFF", fill_type="solid")
        for cell in row: cell.alignment = center; cell.border = border; cell.fill = fill
    for col in ws.columns:
        max_len = 0; letter = col[0].column_letter
        for c in col:
            if c.value: max_len = max(max_len, len(str(c.value)))
        ws.column_dimensions[letter].width = min(max_len + 2, 60)
    
    excel_buffer = io.BytesIO()
    wb.save(excel_buffer)
    excel_buffer.seek(0)
    return excel_buffer

def _get_daily_excel_report(target_day: date) -> Tuple[Optional[str], Optional[io.BytesIO]]:
    rows = _compute_daily_summary(target_day)
    if not rows: return ("No data for that day.", None)
    
    try:
        excel_buffer = _style_and_save_excel(rows)
        return (None, excel_buffer)
    except Exception as e:
        log.error("Failed to generate Excel report in memory: %s", e)
        return (f"Failed to generate report: {e}", None)


def _parse_report_day(arg: Optional[str]) -> date:
    now = datetime.now(TIMEZONE)
    if not arg or arg.lower() == "today": return (now - timedelta(hours=5, minutes=30)).date()
    if arg.lower() == "yesterday": return (now - timedelta(hours=5, minutes=30, days=1)).date()
    try: return datetime.strptime(arg, "%Y-%m-%d").date()
    except Exception: return (now - timedelta(hours=5, minutes=30)).date()

# =============================
# ADMIN COMMANDS (subset; owners/usernames/numbers)
# =============================
def _parse_duration(duration_str: str) -> Optional[timedelta]:
    m = re.match(r"(\d+)\s*(m|h|d|w)", (duration_str or "").strip().lower())
    if not m: return None
    val, unit = m.groups(); val = int(val)
    return {"m": timedelta(minutes=val), "h": timedelta(hours=val),
            "d": timedelta(days=val), "w": timedelta(weeks=val)}[unit]

def _find_owner_group(name: str) -> Optional[dict]:
    norm_name = _norm_owner_name(name)
    for group in OWNER_DATA:
        if _norm_owner_name(group["owner"]) == norm_name: return group
    return None

async def _handle_admin_command(text: str, context: ContextTypes.DEFAULT_TYPE) -> Optional[str]:
    # take customer command
    m = TAKE_CUSTOMER_RX.match(text)
    if m:
        count_str, owner_name, and_stop_str = m.groups()
        count = int(count_str)
        owner_norm = _norm_owner_name(owner_name)
        
        owner_group = _find_owner_group(owner_norm)
        if not owner_group:
            return f"Owner '{owner_name}' not found."
        
        if _owner_is_paused(owner_group):
            return f"Owner '{owner_name}' is currently paused and cannot take customers."

        state["priority_queue"] = {
            "active": True,
            "owner": owner_norm,
            "remaining": count,
            "stop_after": "and stop" in text.lower(),
            "saved_rr_indices": {
                "username_owner_idx": state["rr"]["username_owner_idx"],
                "wa_owner_idx": state["rr"]["wa_owner_idx"],
            }
        }
        save_state()
        stop_msg = " and will be stopped" if state["priority_queue"]["stop_after"] else ""
        return f"Priority queue activated: Next {count} customers will be directed to {owner_name}{stop_msg}."


    # stop/open (owner | username | phone)
    m = STOP_OPEN_RX.match(text)
    if m:
        action, target_raw = m.groups()
        is_stop = action.lower() == "stop"

        # ---- new: stop/open ALL whatsapp ----
        t = target_raw.lower()
        if t in ("all whatsapp", "all whatsapps", "whatsapp all", "all wa", "wa all"):
            total = changed = 0
            for owner in OWNER_DATA:
                for w in owner.get("whatsapp", []):
                    total += 1
                    if w.get("disabled") != is_stop:
                        w["disabled"] = is_stop
                        changed += 1
            await _rebuild_pools_preserving_rotation()
            return f"{'Stopped' if is_stop else 'Opened'} all WhatsApp numbers — changed {changed}/{total}."

        # ---- new: stop/open ALL usernames ----
        if t in ("all username", "all usernames", "username all", "usernames"):
            total = changed = 0
            for owner in OWNER_DATA:
                for e in owner.get("entries", []):
                    total += 1
                    if e.get("disabled") != is_stop:
                        e["disabled"] = is_stop
                        changed += 1
            await _rebuild_pools_preserving_rotation()
            return f"{'Stopped' if is_stop else 'Opened'} all usernames — changed {changed}/{total}."
        
        kind, value = _parse_stop_open_target(target_raw)

        if kind == "phone":
            norm_n = _norm_phone(value)
            found = False
            for owner in OWNER_DATA:
                for w in owner.get("whatsapp", []):
                    if _norm_phone(w.get("number")) == norm_n:
                        w["disabled"] = is_stop
                        found = True
            if not found:
                return f"WhatsApp number {value} not found."
            await _rebuild_pools_preserving_rotation()
            return f"{'Stopped' if is_stop else 'Opened'} WhatsApp {value}."

        if kind == "username":
            norm_h = _norm_handle(value)
            found = False
            for owner in OWNER_DATA:
                for e in owner.get("entries", []):
                    if _norm_handle(e.get("telegram")) == norm_h:
                        e["disabled"] = is_stop
                        found = True
            if not found:
                return f"Username {value} not found."
            await _rebuild_pools_preserving_rotation()
            return f"{'Stopped' if is_stop else 'Opened'} username {value}."

        # owner
        owner = _find_owner_group(value)
        if not owner:
            return f"Owner '{value}' not found."
        owner["disabled"] = bool(is_stop)
        owner.pop("disabled_until", None)
        await _rebuild_pools_preserving_rotation()
        return f"{'Stopped' if is_stop else 'Opened'} owner {value}."

    # add / delete / list
    m = ADD_OWNER_RX.match(text)
    if m:
        name = _norm_owner_name(m.group(1))
        if _find_owner_group(name):
            return f"Owner '{name}' already exists."
        OWNER_DATA.append(_ensure_owner_shape({"owner": name}))
        await _rebuild_pools_preserving_rotation()
        return f"Owner '{name}' added."

    m = DEL_OWNER_RX.match(text)
    if m:
        name = _norm_owner_name(m.group(1))
        before = len(OWNER_DATA)
        OWNER_DATA[:] = [g for g in OWNER_DATA if _norm_owner_name(g.get("owner","")) != name]
        if len(OWNER_DATA) == before:
            return f"Owner '{name}' not found."
        await _rebuild_pools_preserving_rotation()
        return f"Owner '{name}' deleted."

    m = ADD_USERNAME_RX.match(text)
    if m:
        handle, owner_name = m.groups()
        owner = _find_owner_group(owner_name)
        if not owner:
            return f"Owner '{owner_name}' not found."
        norm_h = _norm_handle(handle)
        if any(_norm_handle(e.get("telegram")) == norm_h for e in owner["entries"]):
            return f"@{handle} already exists for owner {owner_name}."
        owner["entries"].append({"telegram": handle, "phone": "", "disabled": False})
        await _rebuild_pools_preserving_rotation()
        return f"Added username @{handle} to {owner_name}."

    m = ADD_WHATSAPP_RX.match(text)
    if m:
        num, owner_name = m.groups()
        owner = _find_owner_group(owner_name)
        if not owner:
            return f"Owner '{owner_name}' not found."
        norm_n = _norm_phone(num)
        if any(_norm_phone(w.get("number")) == norm_n for w in owner["whatsapp"]):
            return f"Number {num} already exists for owner {owner_name}."
        owner["whatsapp"].append({"number": num, "disabled": False})
        await _rebuild_pools_preserving_rotation()
        return f"Added WhatsApp {num} to {owner_name}."

    m = DEL_USERNAME_RX.match(text)
    if m:
        handle = m.group(1); norm_h = _norm_handle(handle); found = False
        for owner in OWNER_DATA:
            before = len(owner["entries"])
            owner["entries"] = [e for e in owner["entries"] if _norm_handle(e.get("telegram")) != norm_h]
            if len(owner["entries"]) < before:
                found = True
        if not found:
            return f"Username @{handle} not found."
        await _rebuild_pools_preserving_rotation()
        return f"Deleted username @{handle} from all owners."

    m = DEL_WHATSAPP_RX.match(text)
    if m:
        num = m.group(1); norm_n = _norm_phone(num); found = False
        for owner in OWNER_DATA:
            before = len(owner["whatsapp"])
            owner["whatsapp"] = [w for w in owner["whatsapp"] if _norm_phone(w.get("number")) != norm_n]
            if len(owner["whatsapp"]) < before:
                found = True
        if not found:
            return f"WhatsApp number {num} not found."
        await _rebuild_pools_preserving_rotation()
        return f"Deleted WhatsApp number {num} from all owners."

    # lists
    if LIST_OWNERS_RX.match(text):
        if not OWNER_DATA:
            return "No owners configured."
        lines = ["<b>Owner Roster:</b>"]
        for o in OWNER_DATA:
            status = "PAUSED" if _owner_is_paused(o) else "active"
            u_count = len([e for e in o.get("entries", []) if not e.get("disabled")])
            w_count = len([w for w in o.get("whatsapp", []) if not w.get("disabled")])
            lines.append(f"- <code>{o['owner']}</code> ({status}): {u_count} usernames, {w_count} whatsapps")
        return "\n".join(lines)

    if LIST_DISABLED_RX.match(text):
        disabled = [o for o in OWNER_DATA if _owner_is_paused(o)]
        if not disabled:
            return "No owners are currently disabled/paused."
        lines = ["<b>Disabled/Paused Owners:</b>"]
        for o in disabled:
            reason = f" (until {o['disabled_until']})" if o.get("disabled_until") else ""
            lines.append(f"- <code>{o['owner']}</code>{reason}")
        return "\n".join(lines)

    # allow: "list @Owner" (alias)
    m = LIST_OWNER_DETAIL_RX.match(text) or LIST_OWNER_ALIAS_RX.match(text)
    if m:
        name = m.group(1).strip()
        if name.lower() in ("owners", "disabled"):  # already handled above
            return None
        owner = _find_owner_group(name)
        if not owner:
            return f"Owner '{name}' not found."
        
        owner_status_flag = " ⛔" if _owner_is_paused(owner) else ""
        lines = [f"<b>Details for {owner['owner']}{owner_status_flag}:</b>"]
        if owner.get("entries"):
            lines.append("<u>Usernames:</u>")
            for e in owner["entries"]:
                flag = " ⛔" if e.get("disabled") else ""
                h = e.get("telegram") or ""
                if h and not h.startswith("@"):
                    h = "@" + h
                lines.append(f"- {h}{flag}")
        if owner.get("whatsapp"):
            lines.append("<u>WhatsApp Numbers:</u>")
            for w in owner["whatsapp"]:
                flag = " ⛔" if w.get("disabled") else ""
                lines.append(f"- {w['number']}{flag}")
        if len(lines) == 1:
            lines.append("No entries found.")
        return "\n".join(lines)

    m = REMIND_ALL_RX.match(text)
    if m:
        return await _send_all_pending_reminders(context)

    return None

# =============================
# REMINDER & RESET TASKS
# =============================
async def _send_all_pending_reminders(context: ContextTypes.DEFAULT_TYPE) -> str:
    """Manually sends reminders for ALL currently issued items."""
    total_reminders_sent = 0
    reminded_users = set()
    
    # This function is called from within on_message, which already has the lock.
    # No need to acquire db_lock here.
    try:
        # No state change, just reading and sending messages
        for kind in ("username", "whatsapp"):
            bucket = _issued_bucket(kind)
            for user_id_str, items in bucket.items():
                for item in items:
                    try:
                        user_id = int(user_id_str)
                        chat_id = item.get("chat_id")
                        value = item.get("value")
                        
                        if chat_id and value:
                            label = "username" if kind == "username" else "WhatsApp"
                            reminder_text = (
                                f"សូមរំលឹក: {mention_user_html(user_id)}, "
                                f"អ្នកនៅមិនទាន់បានផ្តល់ព័ត៌មានសម្រាប់ {label} {value} ដែលអ្នកបានស្នើសុំ។"
                            )
                            await context.bot.send_message(
                                chat_id=chat_id,
                                text=reminder_text,
                                parse_mode=ParseMode.HTML
                            )
                            total_reminders_sent += 1
                            reminded_users.add(user_id)
                    except Exception as e:
                        log.error(f"Error sending manual reminder for user {user_id_str}: {e}")
    except Exception as e:
        log.error(f"Error in _send_all_pending_reminders: {e}")


    if total_reminders_sent == 0:
        return "No pending items found to send reminders for."
    else:
        return f"Successfully sent {total_reminders_sent} reminder(s) to {len(reminded_users)} user(s)."

async def check_reminders(context: ContextTypes.DEFAULT_TYPE):
    await db_lock.acquire()
    try:
        now = datetime.now(TIMEZONE)
        state_changed = False
        
        for kind in ("username", "whatsapp"):
            bucket = _issued_bucket(kind)
            for user_id_str, items in list(bucket.items()):
                for item in list(items): # Iterate over a copy
                    if item.get("reminder_sent"):
                        continue

                    try:
                        issue_ts = datetime.fromisoformat(item["ts"])
                        if (now - issue_ts) > timedelta(minutes=REMINDER_DELAY_MINUTES):
                            user_id = int(user_id_str)
                            chat_id = item.get("chat_id")
                            value = item.get("value")
                            
                            if chat_id and value:
                                label = "username" if kind == "username" else "WhatsApp"
                                reminder_text = (
                                    f"សូមរំលឹក: {mention_user_html(user_id)}, "
                                    f"អ្នកនៅមិនទាន់បានផ្តល់ព័ត៌មានសម្រាប់ {label} {value} ដែលអ្នកបានស្នើសុំ។"
                                )
                                await context.bot.send_message(
                                    chat_id=chat_id,
                                    text=reminder_text,
                                    parse_mode=ParseMode.HTML
                                )
                                item["reminder_sent"] = True
                                state_changed = True
                                log.info(f"Sent reminder to user {user_id} for {kind} in chat {chat_id}")
                    except Exception as e:
                        log.error(f"Error processing reminder for user {user_id_str}: {e}")

        if state_changed:
            save_state()
    finally:
        db_lock.release()

async def daily_reset(context: ContextTypes.DEFAULT_TYPE):
    """Clears daily WhatsApp quotas and the 'issued' state for a new day."""
    log.info("Performing daily reset...")
    await db_lock.acquire()
    try:
        # Clear the daily WhatsApp usage table in the database
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM wa_daily_usage;")
                conn.commit()
                log.info("Cleared daily WhatsApp usage quotas.")
        except Exception as e:
            log.error(f"Failed to clear wa_daily_usage table: {e}")
        finally:
            conn.close()

        # Clear the in-memory 'issued' state
        if "issued" in state:
            state["issued"]["username"].clear()
            state["issued"]["whatsapp"].clear()
        
        save_state()
        log.info("Daily reset complete. Cleared issued items.")

    except Exception as e:
        log.error(f"An error occurred during daily reset: {e}")
    finally:
        db_lock.release()

# =============================
# MESSAGE HANDLER
# =============================
async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or not update.effective_user or \
       update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    msg = update.effective_message
    text = (msg.text or msg.caption or "").strip()
    uid = update.effective_user.id
    chat_id = msg.chat_id
    cache_user_info(update.effective_user)

    await db_lock.acquire()
    try:
        # Admin: Report
        mrep = SEND_REPORT_RX.match(text)
        if _is_admin(update) and mrep:
            target_day = _parse_report_day(mrep.group(1))
            err, excel_buffer = _get_daily_excel_report(target_day)
            if err:
                await msg.chat.send_message(err, reply_to_message_id=msg.message_id)
            elif excel_buffer:
                file_name = f"daily_summary_{target_day.isoformat()}.xlsx"
                await msg.chat.send_document(
                    document=excel_buffer,
                    filename=file_name,
                    caption=f"Daily summary (logical day starting 05:30) — {target_day}",
                    reply_to_message_id=msg.message_id
                )
            return

        # Admin: Console
        if _is_admin(update):
            admin_reply = await _handle_admin_command(text, context)
            if admin_reply:
                await msg.chat.send_message(admin_reply, reply_to_message_id=msg.message_id, parse_mode=ParseMode.HTML)
                return
            elif any(text.lower().startswith(cmd) for cmd in ['add ', 'delete ', 'list ', 'stop ', 'open ', 'remind ', 'take ']):
                await msg.chat.send_message(
                    "I don't recognize that command. Please check the syntax or use `list owners` to see available commands.",
                    reply_to_message_id=msg.message_id
                )
                return


        # Auto-clear holds on content match
        for kind in ("username", "whatsapp"):
            bucket = _issued_bucket(kind)
            user_id_str = str(uid)
            if user_id_str in bucket:
                items_to_clear = []
                for item in bucket[user_id_str]:
                    if _value_in_text(item.get("value"), text):
                        items_to_clear.append(item.get("value"))
                
                for value in items_to_clear:
                    _clear_issued(uid, kind, value)
                    _log_event(kind, "cleared", update, value, owner="")
                    log.info(f"Cleared pending {kind} for user {uid}: {value}")


        # Feeders
        if NEED_USERNAME_RX.match(text):
            rec = await _next_from_username_pool()
            reply = "No available username." if not rec else f"@{rec['owner']}\n{rec['username']}"
            await msg.chat.send_message(reply, reply_to_message_id=msg.message_id)
            if rec:
                _set_issued(uid, chat_id, "username", rec["username"])
                _log_event("username", "issued", update, rec["username"], owner=rec["owner"])
            return

        if NEED_WHATSAPP_RX.match(text):
            rec = await _next_from_whatsapp_pool()
            if not rec:
                await msg.chat.send_message("No available WhatsApp.", reply_to_message_id=msg.message_id)
                return
            # enforce and record quota
            if _wa_quota_reached(rec["number"]):
                await msg.chat.send_message("No available WhatsApp (daily limit may be reached).", reply_to_message_id=msg.message_id)
                return
            _wa_inc_count(_norm_phone(rec["number"]), _logical_day_today())

            reply = f"@{rec['owner']}\n{rec['number']}"
            await msg.chat.send_message(reply, reply_to_message_id=msg.message_id)
            _set_issued(uid, chat_id, "whatsapp", rec["number"])
            _log_event("whatsapp", "issued", update, rec["number"], owner=rec["owner"])
            return

        # Lookups
        m_owner = WHO_USING_REGEX.match(text)
        if m_owner:
            handle, phone = m_owner.groups()
            if handle:
                key = _norm_handle(handle); hits = HANDLE_INDEX.get(key, [])
                owners = sorted({h['owner'] for h in hits}) if hits else []
                reply = f"Owner of username @{key} → " + (", ".join(f"@{o}" for o in owners) if owners else "not found")
            else:
                pnorm = _norm_phone(phone); rec = PHONE_INDEX.get(pnorm)
                if rec and rec.get("channel") == "whatsapp": reply = f"Owner of WhatsApp {phone} → @{rec['owner']}"
                elif rec: reply = f"Owner of number {phone} → @{rec['owner']} (@{rec.get('telegram') or '-'})"
                else: reply = f"Owner of number {phone} → not found"
            await msg.chat.send_message(reply, reply_to_message_id=msg.message_id)
            return

    finally:
        db_lock.release()

# =============================
# MAIN
# =============================
if __name__ == "__main__":
    setup_database()
    load_state()
    _migrate_state_if_needed()
    load_owner_directory()

    app = Application.builder().token(BOT_TOKEN).build()
    
    # Add the scheduled tasks to the queue
    if app.job_queue:
        app.job_queue.run_repeating(check_reminders, interval=60, first=60)
        
        # Schedule the daily reset at 05:31 Phnom Penh time
        reset_time = time(hour=5, minute=31, tzinfo=TIMEZONE)
        app.job_queue.run_daily(daily_reset, time=reset_time)


    app.add_handler(MessageHandler(filters.ALL & ~filters.StatusUpdate.ALL, on_message))

    log.info("Bot is starting...")
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)

