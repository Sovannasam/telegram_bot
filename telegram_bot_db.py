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
import asyncpg
from telegram import Update
from telegram.constants import ChatType, ParseMode
from telegram.ext import Application, ContextTypes, MessageHandler, filters

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
WA_DAILY_LIMIT = int(os.getenv("WA_DAILY_LIMIT", "2"))      # max sends per number per logical day
REMINDER_DELAY_MINUTES = int(os.getenv("REMINDER_DELAY_MINUTES", "20")) # Delay for reminders
USER_WHATSAPP_LIMIT = int(os.getenv("USER_WHATSAPP_LIMIT", "10"))
USERNAME_THRESHOLD_FOR_BONUS = int(os.getenv("USERNAME_THRESHOLD_FOR_BONUS", "35"))
REQUEST_GROUP_ID = int(os.getenv("REQUEST_GROUP_ID", "-1002438185636")) # Group for 'i need ...' commands
CLEARING_GROUP_ID = int(os.getenv("CLEARING_GROUP_ID", "-1002624324856")) # Group for auto-clearing pendings
CONFIRMATION_GROUP_ID = int(os.getenv("CONFIRMATION_GROUP_ID", "-1002694540582"))


# Whitelist of allowed countries (lowercase for case-insensitive matching)
ALLOWED_COUNTRIES = {
    'morocco', 'panama', 'saudi arabia', 'united arab emirates', 'uae',
    'oman', 'jordan', 'italy', 'germany', 'indonesia', 'colombia',
    'bulgaria', 'brazil', 'spain', 'belgium', 'algeria', 'south africa',
    'philippines', 'indian', 'india'
}

TIMEZONE = pytz.timezone("Asia/Phnom_Penh")

# =============================
# DATABASE SETUP & HELPERS
# =============================
db_lock = asyncio.Lock()
DB_POOL: Optional[asyncpg.Pool] = None

async def get_db_pool() -> asyncpg.Pool:
    global DB_POOL
    if DB_POOL is None or DB_POOL.is_closing():
        try:
            DB_POOL = await asyncpg.create_pool(
                dsn=DATABASE_URL,
                max_inactive_connection_lifetime=60,
                min_size=1,
                max_size=10
            )
            if DB_POOL is None:
                raise ConnectionError("Database pool initialization failed, create_pool returned None.")
            log.info("Database connection pool established.")
        except Exception as e:
            log.error(f"Could not create database connection pool: {e}")
            raise
    return DB_POOL

async def close_db_pool():
    global DB_POOL
    if DB_POOL and not DB_POOL.is_closing():
        log.info("Closing database connection pool.")
        await DB_POOL.close()
        DB_POOL = None
        log.info("Database connection pool closed.")


async def setup_database():
    log.info("Setting up database schema...")
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS kv_storage (
                key TEXT PRIMARY KEY,
                data JSONB NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        await conn.execute("""
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
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS wa_daily_usage (
                day DATE NOT NULL,
                number_norm TEXT NOT NULL,
                sent_count INTEGER NOT NULL DEFAULT 0,
                last_sent TIMESTAMPTZ,
                PRIMARY KEY (day, number_norm)
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_daily_activity (
                day DATE NOT NULL,
                user_id BIGINT NOT NULL,
                username_requests INTEGER DEFAULT 0,
                whatsapp_requests INTEGER DEFAULT 0,
                PRIMARY KEY (day, user_id)
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS whatsapp_bans (
                user_id BIGINT PRIMARY KEY
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_daily_country_counts (
                day DATE NOT NULL,
                user_id BIGINT NOT NULL,
                country TEXT NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (day, user_id, country)
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_daily_confirmations (
                day DATE NOT NULL,
                user_id BIGINT NOT NULL,
                confirm_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (day, user_id)
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS owner_daily_performance (
                day DATE NOT NULL,
                owner_name TEXT NOT NULL,
                telegram_count INTEGER NOT NULL DEFAULT 0,
                whatsapp_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (day, owner_name)
            );
        """)
    log.info("Database schema is ready.")


# =============================
# STATE (DB-backed)
# =============================
BASE_STATE = {
    "user_names": {},
    "rr": {
        "username_owner_idx": 0, "username_entry_idx": {},
        "wa_owner_idx": 0, "wa_entry_idx": {},
    },
    "issued": {"username": {}, "whatsapp": {}, "app_id": {}},
    "priority_queue": {
        "active": False,
        "owner": None,
        "remaining": 0,
        "stop_after": False,
        "saved_rr_indices": {}
    }
}
state: Dict = {k: (v.copy() if isinstance(v, dict) else v) for k, v in BASE_STATE.items()}
WHATSAPP_BANNED_USERS: set[int] = set()

async def load_whatsapp_bans():
    global WHATSAPP_BANNED_USERS
    WHATSAPP_BANNED_USERS = set()
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id FROM whatsapp_bans")
            for row in rows:
                WHATSAPP_BANNED_USERS.add(row['user_id'])
        log.info(f"Loaded {len(WHATSAPP_BANNED_USERS)} WhatsApp bans from database.")
    except Exception as e:
        log.error(f"Failed to load WhatsApp bans from DB: %s", e)

async def load_state():
    global state
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT data FROM kv_storage WHERE key = 'state'")
            if result:
                loaded = json.loads(result)
                temp_state = {k: (v.copy() if isinstance(v, dict) else v) for k, v in BASE_STATE.items()}
                for key, value in loaded.items():
                    if isinstance(value, dict) and key in temp_state:
                        temp_state[key].update(value)
                    else:
                        temp_state[key] = value
                state = temp_state
                log.info("Bot state loaded from database.")
            else:
                state = {k: (v.copy() if isinstance(v, dict) else v) for k, v in BASE_STATE.items()}
                await save_state()
    except Exception as e:
        log.warning(f"Failed to load state from DB: %s. Using default state.", e)
        state = {k: (v.copy() if isinstance(v, dict) else v) for k, v in BASE_STATE.items()}

    state.setdefault("rr", {}).setdefault("username_entry_idx", {})
    state["rr"].setdefault("wa_entry_idx", {})
    state.setdefault("issued", {}).setdefault("username", {})
    state["issued"].setdefault("whatsapp", {})
    state["issued"].setdefault("app_id", {})
    state.setdefault("priority_queue", BASE_STATE["priority_queue"])


async def save_state():
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO kv_storage (key, data)
                VALUES ('state', $1)
                ON CONFLICT (key) DO UPDATE
                SET data = EXCLUDED.data, updated_at = NOW();
            """, json.dumps(state))
    except Exception as e:
        log.warning("Failed to save state to DB: %s", e)


async def _migrate_state_if_needed():
    log.info("Checking state structure for migration...")
    state_was_changed = False
    issued = state.setdefault("issued", {})
    for kind in ("username", "whatsapp", "app_id"):
        bucket = issued.setdefault(kind, {})
        for user_id, item_or_list in bucket.items():
            if isinstance(item_or_list, dict):
                bucket[user_id] = [item_or_list]
                state_was_changed = True
                log.info(f"Migrated user {user_id}'s '{kind}' data to new list format.")

    if state_was_changed:
        log.info("State structure was migrated. Saving new format to database.")
        await save_state()
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
def _normalize_app_id(app_id: str) -> str:
    """Removes leading '@' and all non-alphanumeric characters."""
    if not app_id:
        return ""
    return re.sub(r'[^a-zA-Z0-9]', '', app_id)

def _norm_owner_name(s: str) -> str:
    s = (s or "").strip()
    if s.startswith("@"): s = s[1:]
    return s.lower()
def _is_admin(update: Update) -> bool:
    u = update.effective_user
    if not u: return False
    return (u.username or "").lower() == ADMIN_USERNAME.lower()

def _is_owner(user: Optional[Update.effective_user]) -> bool:
    if not user or not user.username:
        return False
    norm_username = _norm_owner_name(user.username)
    return any(_norm_owner_name(g.get("owner", "")) == norm_username for g in OWNER_DATA)

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
    norm_entries = []
    for e in g.get("entries", []):
        if isinstance(e, dict):
            e.setdefault("telegram", ""); e.setdefault("phone", ""); e.setdefault("disabled", False)
            norm_entries.append(e)
    g["entries"] = norm_entries
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

async def save_owner_directory():
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO kv_storage (key, data)
                VALUES ('owners', $1)
                ON CONFLICT (key) DO UPDATE
                SET data = EXCLUDED.data, updated_at = NOW();
            """, json.dumps(OWNER_DATA))
    except Exception as e:
        log.error("Failed to save owner directory to DB: %s", e)

async def load_owner_directory():
    global OWNER_DATA, HANDLE_INDEX, PHONE_INDEX, USERNAME_POOL, WHATSAPP_POOL
    OWNER_DATA, HANDLE_INDEX, PHONE_INDEX = [], {}, {}
    USERNAME_POOL, WHATSAPP_POOL = [], []

    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT data FROM kv_storage WHERE key = 'owners'")
            OWNER_DATA = (json.loads(result) or []) if result else []
    except Exception as e:
        log.error("Failed to load owners from DB: %s", e)
        OWNER_DATA = []

    OWNER_DATA = [_ensure_owner_shape(dict(g)) for g in OWNER_DATA]

    for group in OWNER_DATA:
        owner = _norm_owner_name(group.get("owner") or "")
        if not owner or _owner_is_paused(group): continue
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
# QUOTA & USER ACTIVITY
# =============================
def _logical_day_today() -> date:
    now = datetime.now(TIMEZONE)
    return (now - timedelta(hours=5, minutes=30)).date()

async def _get_user_activity(user_id: int) -> Tuple[int, int]:
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT username_requests, whatsapp_requests FROM user_daily_activity WHERE day=$1 AND user_id=$2",
                _logical_day_today(), user_id
            )
            return (row['username_requests'], row['whatsapp_requests']) if row else (0, 0)
    except Exception as e:
        log.warning(f"User activity read failed for {user_id}: {e}")
        return (0, 0)

async def _increment_user_activity(user_id: int, kind: str):
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            if kind == "username":
                await conn.execute("""
                    INSERT INTO user_daily_activity (day, user_id, username_requests)
                    VALUES ($1, $2, 1)
                    ON CONFLICT (day, user_id) DO UPDATE
                    SET username_requests = user_daily_activity.username_requests + 1;
                """, _logical_day_today(), user_id)
            elif kind == "whatsapp":
                await conn.execute("""
                    INSERT INTO user_daily_activity (day, user_id, whatsapp_requests)
                    VALUES ($1, $2, 1)
                    ON CONFLICT (day, user_id) DO UPDATE
                    SET whatsapp_requests = user_daily_activity.whatsapp_requests + 1;
                """, _logical_day_today(), user_id)
    except Exception as e:
        log.warning(f"User activity write failed for {user_id}: {e}")

async def _increment_user_country_count(user_id: int, country: str):
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO user_daily_country_counts (day, user_id, country, count)
                VALUES ($1, $2, $3, 1)
                ON CONFLICT (day, user_id, country) DO UPDATE
                SET count = user_daily_country_counts.count + 1;
            """, _logical_day_today(), user_id, country)
    except Exception as e:
        log.warning(f"User country count write failed for {user_id} and country {country}: {e}")

async def _increment_user_confirmation_count(user_id: int):
    """Increments the successful confirmation count for a user on the current logical day."""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO user_daily_confirmations (day, user_id, confirm_count)
                VALUES ($1, $2, 1)
                ON CONFLICT (day, user_id) DO UPDATE
                SET confirm_count = user_daily_confirmations.confirm_count + 1;
            """, _logical_day_today(), user_id)
    except Exception as e:
        log.warning(f"User confirmation count write failed for {user_id}: {e}")

async def _increment_owner_performance(owner_name: str, kind: str):
    """Increments telegram or whatsapp count for an owner on the current day."""
    if not owner_name: return
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            if kind == 'username':
                await conn.execute("""
                    INSERT INTO owner_daily_performance (day, owner_name, telegram_count)
                    VALUES ($1, $2, 1)
                    ON CONFLICT (day, owner_name) DO UPDATE
                    SET telegram_count = owner_daily_performance.telegram_count + 1;
                """, _logical_day_today(), owner_name)
            elif kind == 'whatsapp':
                await conn.execute("""
                    INSERT INTO owner_daily_performance (day, owner_name, whatsapp_count)
                    VALUES ($1, $2, 1)
                    ON CONFLICT (day, owner_name) DO UPDATE
                    SET whatsapp_count = owner_daily_performance.whatsapp_count + 1;
                """, _logical_day_today(), owner_name)
    except Exception as e:
        log.warning(f"Owner performance write failed for {owner_name}: {e}")


async def _wa_get_count(number_norm: str, day: date) -> int:
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT sent_count FROM wa_daily_usage WHERE day=$1 AND number_norm=$2",
                day, number_norm
            )
            return int(count) if count is not None else 0
    except Exception as e:
        log.warning("Quota read failed: %s", e)
        return 0

async def _wa_inc_count(number_norm: str, day: date):
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO wa_daily_usage (day, number_norm, sent_count, last_sent)
                VALUES ($1, $2, 1, $3)
                ON CONFLICT (day, number_norm)
                DO UPDATE SET sent_count = wa_daily_usage.sent_count + 1,
                              last_sent = EXCLUDED.last_sent
            """, day, number_norm, datetime.now(TIMEZONE))
    except Exception as e:
        log.warning("Quota write failed: %s", e)

async def _wa_quota_reached(number_raw: str) -> bool:
    n = _norm_phone(number_raw)
    cnt = await _wa_get_count(n, _logical_day_today())
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
    old_user_owner_list = _owner_list_from_pool(USERNAME_POOL)
    old_wa_owner_list   = _owner_list_from_pool(WHATSAPP_POOL)

    rr = state.setdefault("rr", {})
    old_user_owner_idx = rr.get("username_owner_idx", 0)
    old_wa_owner_idx   = rr.get("wa_owner_idx", 0)
    old_user_entry_idx = dict(rr.get("username_entry_idx", {}))
    old_wa_entry_idx   = dict(rr.get("wa_entry_idx", {}))

    await save_owner_directory()
    await load_owner_directory()

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

    await save_state()

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

        state["priority_queue"] = BASE_STATE["priority_queue"]

        if stop_after and owner_to_stop:
            log.info(f"Auto-stopping owner {owner_to_stop} after priority queue completion.")
            owner_group = _find_owner_group(owner_to_stop)
            if owner_group:
                owner_group["disabled"] = True
            await _rebuild_pools_preserving_rotation()
        else:
            await save_state()
    else:
        await save_state()

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
        log.warning(f"Priority owner {priority_owner} has no available usernames. Falling back to normal rotation for this request.")

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
            await save_state()
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
                        if not await _wa_quota_reached(cand):
                            state["rr"]["wa_entry_idx"][priority_owner] = ((start + step) + 1) % len(numbers)
                            await _decrement_priority_and_end_if_needed()
                            return {"owner": priority_owner, "number": cand}
        log.warning(f"Priority owner {priority_owner} has no available WhatsApp numbers. Falling back to normal rotation for this request.")

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
                if await _wa_quota_reached(cand):
                    continue
                rr["wa_entry_idx"][owner] = ((start + step) + 1) % len(numbers)
                rr["wa_owner_idx"] = (owner_idx + 1) % len(WHATSAPP_POOL)
                await save_state()
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
APP_ID_RX = re.compile(r"\bapp\b.*?\@([^\s]+)", re.IGNORECASE)

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
TAKE_CUSTOMER_RX      = re.compile(r"^\s*take\s+(\d+)\s+customer(?:s)?\s+to\s+owner\s+@?(.+?)(?:\s+(and\s+stop))?\s*$", re.IGNORECASE)
CLEAR_PENDING_RX      = re.compile(r"^\s*clear\s+pending\s+(.+)\s*$", re.IGNORECASE)
CLEAR_ALL_PENDING_RX  = re.compile(r"^\s*clear\s+all\s+pending\s*$", re.IGNORECASE)
BAN_WHATSAPP_RX       = re.compile(r"^\s*ban\s+whatsapp\s+@?(\S+)\s*$", re.IGNORECASE)
UNBAN_WHATSAPP_RX     = re.compile(r"^\s*unban\s+whatsapp\s+@?(\S+)\s*$", re.IGNORECASE)
LIST_BANNED_RX        = re.compile(r"^\s*list\s+banned\s*$", re.IGNORECASE)
OWNER_REPORT_RX       = re.compile(r"^\s*owner\s+report(?:\s+(yesterday|today|\d{4}-\d{2}-\d{2}))?\s*$", re.IGNORECASE)
COMMANDS_RX           = re.compile(r"^\s*commands\s*$", re.IGNORECASE)
MY_DETAIL_RX          = re.compile(r"^\s*my\s+detail\s*$", re.IGNORECASE)
DETAIL_USER_RX        = re.compile(r"^\s*detail\s+@?(\S+)\s*$", re.IGNORECASE)
MY_PERFORMANCE_RX     = re.compile(r"^\s*my\s+performance(?:\s+(yesterday|today|\d{4}-\d{2}-\d{2}))?\s*$", re.IGNORECASE)
PERFORMANCE_OWNER_RX  = re.compile(r"^\s*performance\s+@?(\S+?)(?:\s+(yesterday|today|\d{4}-\d{2}-\d{2}))?\s*$", re.IGNORECASE)


def _looks_like_phone(s: str) -> bool:
    return bool(PHONE_LIKE_RX.fullmatch((s or "").strip()))

def _parse_stop_open_target(raw: str) -> Tuple[str, str]:
    s = (raw or "").strip()
    low = s.lower()
    for pref in ("username ", "user ", "handle "):
        if low.startswith(pref):
            t = s[len(pref):].strip()
            if not t.startswith("@"): t = f"@{t}"
            return ("username", t)
    for pref in ("whatsapp ", "wa ", "phone ", "number ", "num "):
        if low.startswith(pref):
            return ("phone", s[len(pref):].strip())
    if s.startswith("@"):
        return ("username", s)
    if _looks_like_phone(s):
        return ("phone", s)
    return ("owner", s)

# =============================
# AUDIT LOG (DB)
# =============================
async def _log_event(kind: str, action: str, update: Update, value: str, owner: str = ""):
    try:
        u = update.effective_user
        m = update.effective_message
        c = update.effective_chat
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO audit_log (
                    ts_local, chat_id, message_id, user_id, user_first,
                    user_username, kind, action, value, owner
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
                datetime.now(TIMEZONE), c.id if c else None, m.message_id if m else None,
                u.id if u else None, u.first_name if u else None, u.username if u else None,
                kind, action, value, owner or ""
            )
    except Exception as e:
        log.warning("Log write to DB failed: %s", e)

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

# MODIFIED: _set_issued now accepts additional context
async def _set_issued(user_id: int, chat_id: int, kind: str, value: str, context_data: Optional[Dict] = None):
    bucket = _issued_bucket(kind)
    user_id_str = str(user_id)
    if user_id_str not in bucket:
        bucket[user_id_str] = []
    
    item_data = {
        "value": value,
        "ts": datetime.now(TIMEZONE).isoformat(),
        "chat_id": chat_id
    }
    if context_data:
        item_data.update(context_data)
        
    bucket[user_id_str].append(item_data)
    await save_state()

async def _clear_issued(user_id: int, kind: str, value_to_clear: str) -> bool:
    bucket = _issued_bucket(kind)
    user_id_str = str(user_id)
    if user_id_str in bucket:
        original_len = len(bucket[user_id_str])
        bucket[user_id_str] = [
            item for item in bucket[user_id_str] if item.get("value") != value_to_clear
        ]
        if not bucket[user_id_str]:
            del bucket[user_id_str]

        if len(bucket.get(user_id_str, [])) < original_len:
            await save_state()
            return True
    return False

async def _clear_one_issued(user_id: int, kind: str, value_to_clear: str) -> bool:
    bucket = _issued_bucket(kind)
    user_id_str = str(user_id)
    if user_id_str in bucket:
        item_to_remove = None
        for item in bucket[user_id_str]:
            if item.get("value") == value_to_clear:
                item_to_remove = item
                break

        if item_to_remove:
            bucket[user_id_str].remove(item_to_remove)
            if not bucket[user_id_str]:
                del bucket[user_id_str]
            await save_state()
            return True
    return False

def _value_in_text(value: Optional[str], text: str) -> bool:
    if not value:
        return False

    v_norm = value.strip()
    text_norm = text or ""

    if v_norm.startswith('@'):
        pattern = re.compile(r'(?<!\S)' + re.escape(v_norm) + r'(?!\S)')
        return bool(pattern.search(text_norm))
    else:
        v_digits = re.sub(r'\D', '', v_norm)
        text_digits = re.sub(r'\D', '', text_norm)
        return v_digits and v_digits in text_digits

# =============================
# COUNTRY & AGE FILTERING
# =============================
def _find_age_in_text(text: str) -> Optional[int]:
    match = re.search(
        r'\b(?:age|old)\s*:?\s*(\d{1,2})\b|\b(\d{1,2})\s*(?:yrs|yr|years|year old)\b',
        text.lower()
    )
    if match:
        age_str = match.group(1) or match.group(2)
        if age_str:
            return int(age_str)
    return None

def _find_country_in_text(text: str) -> Tuple[Optional[str], Optional[str]]:
    match = re.search(r'\b(?:from|country)\s*:?\s*([a-zA-Z\s,]+)', text, re.IGNORECASE)
    if not match:
        return None, None

    potential_country = match.group(1).split(',')[0].strip().lower()

    for allowed in ALLOWED_COUNTRIES:
        if allowed in potential_country:
            if allowed in ['indian', 'india']:
                return potential_country, 'india'
            return potential_country, allowed

    return potential_country, 'not_allowed'

# =============================
# DETAIL & PERFORMANCE COMMANDS
# =============================
async def _get_user_country_counts(user_id: int) -> List[Tuple[str, int]]:
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT country, count FROM user_daily_country_counts WHERE day=$1 AND user_id=$2 ORDER BY country",
                _logical_day_today(), user_id
            )
            return [(row['country'], row['count']) for row in rows]
    except Exception as e:
        log.warning(f"User country count read failed for {user_id}: {e}")
        return []

async def _get_user_confirmation_count(user_id: int) -> int:
    """Fetches a user's successful confirmation count for the current logical day."""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT confirm_count FROM user_daily_confirmations WHERE day=$1 AND user_id=$2",
                _logical_day_today(), user_id
            )
            return int(count) if count is not None else 0
    except Exception as e:
        log.warning(f"User confirmation count read failed for {user_id}: {e}")
        return 0

async def _get_owner_performance(owner_name: str, day: date) -> Tuple[int, int]:
    """Fetches an owner's performance stats for a given day."""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT telegram_count, whatsapp_count FROM owner_daily_performance WHERE day=$1 AND owner_name=$2",
                day, owner_name
            )
            return (row['telegram_count'], row['whatsapp_count']) if row else (0, 0)
    except Exception as e:
        log.warning(f"Owner performance read failed for {owner_name}: {e}")
        return (0, 0)

async def _get_user_detail_text(user_id: int) -> str:
    user_info = state.get("user_names", {}).get(str(user_id), {})
    user_display = user_info.get('username') or user_info.get('first_name') or f"ID: {user_id}"
    if user_info.get('username'):
        user_display = f"@{user_display}"

    username_reqs, whatsapp_reqs = await _get_user_activity(user_id)
    country_counts = await _get_user_country_counts(user_id)
    confirmation_count = await _get_user_confirmation_count(user_id)

    pending_usernames = [item['value'] for item in _issued_bucket("username").get(str(user_id), [])]
    pending_whatsapps = [item['value'] for item in _issued_bucket("whatsapp").get(str(user_id), [])]

    lines = [f"<b>📊 Daily Detail for {user_display}</b>"]
    lines.append(f"<b>- Usernames Received:</b> {username_reqs}")
    lines.append(f"<b>- WhatsApps Received:</b> {whatsapp_reqs}")
    lines.append(f"<b>- Customers Added:</b> {confirmation_count}")
    
    if country_counts:
        lines.append("")
        lines.append("<b>🌍 Country Submissions:</b>")
        for country, count in country_counts:
            lines.append(f"  - {country.title()}: {count}")

    lines.append("")

    if pending_usernames:
        lines.append("<b>⏳ Pending Usernames:</b>")
        for u in pending_usernames:
            lines.append(f"  - <code>{u}</code>")
    else:
        lines.append("<b>✅ No Pending Usernames</b>")

    if pending_whatsapps:
        lines.append("\n<b>⏳ Pending WhatsApps:</b>")
        for w in pending_whatsapps:
            lines.append(f"  - <code>{w}</code>")
    else:
        lines.append("\n<b>✅ No Pending WhatsApps</b>")
    
    return "\n".join(lines)


# NEW: Function to generate the performance report text
async def _get_owner_performance_text(owner_name: str, day: date) -> str:
    """Generates a formatted string of an owner's daily performance."""
    tg_count, wa_count = await _get_owner_performance(owner_name, day)
    total = tg_count + wa_count
    
    lines = [f"<b>📊 Performance for @{owner_name} on {day.isoformat()}</b>"]
    lines.append(f"<b>- Customers via Telegram:</b> {tg_count}")
    lines.append(f"<b>- Customers via WhatsApp:</b> {wa_count}")
    lines.append(f"<b>- Total Customers:</b> {total}")
    return "\n".join(lines)


# =============================
# EXCEL (reads audit_log)
# =============================
def _logical_day_of(ts: datetime) -> date:
    shifted = ts.astimezone(TIMEZONE) - timedelta(hours=5, minutes=30)
    return shifted.date()

async def _read_log_rows() -> List[dict]:
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM audit_log ORDER BY ts_local")
            return [dict(row) for row in rows]
    except Exception as e:
        log.error("Failed to read log rows from DB: %s", e)
        return []

async def _compute_daily_summary(target_day: date) -> Tuple[List[dict], List[dict]]:
    rows = await _read_log_rows()
    day_rows = [r for r in rows if _logical_day_of(r["ts_local"]) == target_day]

    users: Dict[str, dict] = {}
    owner_stats: Dict[str, dict] = {}

    for r in day_rows:
        user_key = r.get("user_first") or r.get("user_username") or str(r.get("user_id"))
        d = users.setdefault(user_key, {"username_issued": [], "username_cleared": [], "wa_issued": [], "wa_cleared": []})
        kind, action, value, owner = r["kind"], r["action"], r["value"], (r.get("owner","") or "").lower()

        if kind == "username":
            if action == "issued":
                d["username_issued"].append((value, owner))
                if owner:
                    s = owner_stats.setdefault(owner, {"total": 0, "tg": 0, "wa": 0})
                    s["total"] += 1
                    s["tg"] += 1
            if action == "cleared": d["username_cleared"].append(value)
        elif kind == "whatsapp":
            if action == "issued":
                d["wa_issued"].append((value, owner))
                if owner:
                    s = owner_stats.setdefault(owner, {"total": 0, "tg": 0, "wa": 0})
                    s["total"] += 1
                    s["wa"] += 1
            if action == "cleared": d["wa_cleared"].append(value)

    out_users = []
    for user, d in users.items():
        issued_user_pairs = d["username_issued"]; issued_wa_pairs = d["wa_issued"]
        issued_user_vals  = [v for v, _ in issued_user_pairs]; issued_wa_vals = [v for v, _ in issued_wa_pairs]
        notback_user = [v for v in issued_user_vals if v not in d["username_cleared"]]
        notback_wa = [v for v in issued_wa_vals if v not in d["wa_cleared"]]
        owners_user = sorted({own for (v, own) in issued_user_pairs if v in notback_user and own})
        owners_wa = sorted({own for (v, own) in issued_wa_pairs if v in notback_wa and own})
        out_users.append({
            "Day": target_day.isoformat(), "User": str(user),
            "Total username receive": len(issued_user_vals), "Total whatsapp receive": len(issued_wa_vals),
            "Total username provide back": len(d["username_cleared"]), "Total whatsapp provide back": len(d["wa_cleared"]),
            "Username not provide back": ", ".join(notback_user), "Owner of username": ", ".join(('@'+o) for o in owners_user),
            "Whatsapp not provide back": ", ".join(notback_wa), "Owner of whatsapp": ", ".join(('@'+o) for o in owners_wa),
        })
    out_users.sort(key=lambda r: r["User"].lower())

    out_owners = []
    for owner, s in sorted(owner_stats.items(), key=lambda kv: kv[0]):
        out_owners.append({
            "Day": target_day.isoformat(),
            "Owner": f"@{owner}",
            "Customers total": s["total"],
            "Customers via Telegram": s["tg"],
            "Customers via WhatsApp": s["wa"],
        })
    return out_users, out_owners

def _style_and_save_excel(user_rows: List[dict], owner_rows: List[dict]) -> io.BytesIO:
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
    except ImportError:
        raise RuntimeError("openpyxl not installed. Please add it to requirements.txt")

    wb = Workbook()

    ws = wb.active; ws.title = "Summary"
    headers = ["Day","User","Total username receive","Total whatsapp receive","Total username provide back",
               "Total whatsapp provide back","Username not provide back","Owner of username",
               "Whatsapp not provide back","Owner of whatsapp"]
    ws.append(headers)
    for r in user_rows: ws.append([r.get(h, "") for h in headers])

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

    ws2 = wb.create_sheet("Owners")
    headers2 = ["Day","Owner","Customers total","Customers via Telegram","Customers via WhatsApp"]
    ws2.append(headers2)
    for r in owner_rows: ws2.append([r.get(h, "") for h in headers2])
    for cell in ws2[1]: cell.fill = header_fill; cell.font = header_font; cell.alignment = center; cell.border = border
    for i, row in enumerate(ws2.iter_rows(min_row=2, max_row=ws2.max_row, max_col=ws2.max_column), start=2):
        fill = PatternFill(start_color="F2F2F2" if i % 2 == 0 else "FFFFFF", fill_type="solid")
        for cell in row: cell.alignment = center; cell.border = border; cell.fill = fill
    for col in ws2.columns:
        max_len = 0; letter = col[0].column_letter
        for c in col:
            if c.value: max_len = max(max_len, len(str(c.value)))
        ws2.column_dimensions[letter].width = min(max_len + 2, 40)

    excel_buffer = io.BytesIO()
    wb.save(excel_buffer)
    excel_buffer.seek(0)
    return excel_buffer

async def _get_daily_excel_report(target_day: date) -> Tuple[Optional[str], Optional[io.BytesIO]]:
    user_rows, owner_rows = await _compute_daily_summary(target_day)
    if not user_rows and not owner_rows:
        return ("No data for that day.", None)
    try:
        excel_buffer = _style_and_save_excel(user_rows, owner_rows)
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
# ADMIN COMMANDS
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

def _find_user_id_by_name(name: str) -> Optional[int]:
    norm_name = name.lower().lstrip('@').strip()
    user_names = state.get("user_names", {})
    for uid, data in user_names.items():
        if data.get("username", "").lower() == norm_name:
            return int(uid)
        if data.get("first_name", "").lower() == norm_name:
            return int(uid)
    return None

def _get_commands_text() -> str:
    return """
<b>Bot Command List</b>

<b>--- User Commands ---</b>
<code>i need username</code> - Request a username.
<code>i need whatsapp</code> - Request a WhatsApp number.
<code>who's using @item</code> - Check the owner of an item.
<code>my detail</code> - See your own daily stats.
<code>my performance</code> - See your daily customer stats (owners only).

<b>--- Admin: Owner & Item Management ---</b>
<code>add owner @owner</code>
<code>delete owner @owner</code>
<code>add username @user to @owner</code>
<code>delete username @user</code>
<code>add whatsapp +123... to @owner</code>
<code>delete whatsapp +123...</code>

<b>--- Admin: Availability Control ---</b>
<code>stop @owner/@user/+123...</code>
<code>open @owner/@user/+123...</code>
<code>stop all usernames</code>
<code>open all usernames</code>
<code>stop all whatsapp</code>
<code>open all whatsapp</code>

<b>--- Admin: Priority & User Management ---</b>
<code>take 5 customer to owner @owner</code>
<code>take 5 customer to owner @owner and stop</code>
<code>ban whatsapp @user</code>
<code>unban whatsapp @user</code>
<code>list banned</code>

<b>--- Admin: Reports & Manual Actions ---</b>
<code>report [today|yesterday|YYYY-MM-DD]</code>
<code>owner report [today|yesterday|YYYY-MM-DD]</code>
<code>performance @owner [day]</code> - See owner's customer stats.
<code>remind user</code>
<code>clear pending @item_or_number</code>
<code>clear all pending</code>

<b>--- Admin: Viewing Information ---</b>
<code>list owners</code>
<code>list disabled</code>
<code>list @owner</code>
<code>detail @user</code> - See a user's daily stats.
"""

async def _handle_admin_command(text: str, context: ContextTypes.DEFAULT_TYPE, update: Update) -> Optional[str]:
    m = TAKE_CUSTOMER_RX.match(text)
    if m:
        count_str, owner_name, and_stop_str = m.groups()
        count = int(count_str)
        owner_norm = _norm_owner_name(owner_name)

        owner_group = _find_owner_group(owner_norm)
        if not owner_group: return f"Owner '{owner_name}' not found."
        if _owner_is_paused(owner_group): return f"Owner '{owner_name}' is currently paused and cannot take customers."

        state["priority_queue"] = {
            "active": True, "owner": owner_norm, "remaining": count,
            "stop_after": bool(and_stop_str),
            "saved_rr_indices": { "username_owner_idx": state["rr"]["username_owner_idx"], "wa_owner_idx": state["rr"]["wa_owner_idx"] }
        }
        await save_state()
        stop_msg = " and will be stopped" if state["priority_queue"]["stop_after"] else ""
        return f"Priority queue activated: Next {count} customers will be directed to {owner_name}{stop_msg}."

    m = STOP_OPEN_RX.match(text)
    if m:
        action, target_raw = m.groups(); is_stop = action.lower() == "stop"
        t = target_raw.lower()
        if t in ("all whatsapp", "all whatsapps", "whatsapp all", "all wa", "wa all"):
            total = changed = 0
            for owner in OWNER_DATA:
                for w in owner.get("whatsapp", []):
                    total += 1
                    if w.get("disabled") != is_stop: w["disabled"] = is_stop; changed += 1
            await _rebuild_pools_preserving_rotation()
            return f"{'Stopped' if is_stop else 'Opened'} all WhatsApp numbers — changed {changed}/{total}."

        if t in ("all username", "all usernames", "username all", "usernames"):
            total = changed = 0
            for owner in OWNER_DATA:
                for e in owner.get("entries", []):
                    total += 1
                    if e.get("disabled") != is_stop: e["disabled"] = is_stop; changed += 1
            await _rebuild_pools_preserving_rotation()
            return f"{'Stopped' if is_stop else 'Opened'} all usernames — changed {changed}/{total}."

        kind, value = _parse_stop_open_target(target_raw)
        if kind == "phone":
            norm_n = _norm_phone(value); found = False
            for owner in OWNER_DATA:
                for w in owner.get("whatsapp", []):
                    if _norm_phone(w.get("number")) == norm_n: w["disabled"] = is_stop; found = True
            if not found: return f"WhatsApp number {value} not found."
            await _rebuild_pools_preserving_rotation()
            return f"{'Stopped' if is_stop else 'Opened'} WhatsApp {value}."
        if kind == "username":
            norm_h = _norm_handle(value); found = False
            for owner in OWNER_DATA:
                for e in owner.get("entries", []):
                    if _norm_handle(e.get("telegram")) == norm_h: e["disabled"] = is_stop; found = True
            if not found: return f"Username {value} not found."
            await _rebuild_pools_preserving_rotation()
            return f"{'Stopped' if is_stop else 'Opened'} username {value}."
        owner = _find_owner_group(value)
        if not owner: return f"Owner '{value}' not found."
        owner["disabled"] = bool(is_stop); owner.pop("disabled_until", None)
        await _rebuild_pools_preserving_rotation()
        return f"{'Stopped' if is_stop else 'Opened'} owner {value}."

    m = ADD_OWNER_RX.match(text)
    if m:
        name = _norm_owner_name(m.group(1))
        if _find_owner_group(name): return f"Owner '{name}' already exists."
        OWNER_DATA.append(_ensure_owner_shape({"owner": name}))
        await _rebuild_pools_preserving_rotation()
        return f"Owner '{name}' added."

    m = DEL_OWNER_RX.match(text)
    if m:
        name = _norm_owner_name(m.group(1)); before = len(OWNER_DATA)
        OWNER_DATA[:] = [g for g in OWNER_DATA if _norm_owner_name(g.get("owner","")) != name]
        if len(OWNER_DATA) == before: return f"Owner '{name}' not found."
        await _rebuild_pools_preserving_rotation()
        return f"Owner '{name}' deleted."

    m = ADD_USERNAME_RX.match(text)
    if m:
        handle, owner_name = m.groups(); owner = _find_owner_group(owner_name)
        if not owner: return f"Owner '{owner_name}' not found."
        norm_h = _norm_handle(handle)
        if any(_norm_handle(e.get("telegram")) == norm_h for e in owner["entries"]): return f"@{handle} already exists for owner {owner_name}."
        owner["entries"].append({"telegram": handle, "phone": "", "disabled": False})
        await _rebuild_pools_preserving_rotation()
        return f"Added username @{handle} to {owner_name}."

    m = ADD_WHATSAPP_RX.match(text)
    if m:
        num, owner_name = m.groups(); owner = _find_owner_group(owner_name)
        if not owner: return f"Owner '{owner_name}' not found."
        norm_n = _norm_phone(num)
        if any(_norm_phone(w.get("number")) == norm_n for w in owner["whatsapp"]): return f"Number {num} already exists for owner {owner_name}."
        owner["whatsapp"].append({"number": num, "disabled": False})
        await _rebuild_pools_preserving_rotation()
        return f"Added WhatsApp {num} to {owner_name}."

    m = DEL_USERNAME_RX.match(text)
    if m:
        handle = m.group(1); norm_h = _norm_handle(handle); found = False
        for owner in OWNER_DATA:
            before = len(owner["entries"])
            owner["entries"] = [e for e in owner["entries"] if _norm_handle(e.get("telegram")) != norm_h]
            if len(owner["entries"]) < before: found = True
        if not found: return f"Username @{handle} not found."
        await _rebuild_pools_preserving_rotation()
        return f"Deleted username @{handle} from all owners."

    m = DEL_WHATSAPP_RX.match(text)
    if m:
        num = m.group(1); norm_n = _norm_phone(num); found = False
        for owner in OWNER_DATA:
            before = len(owner["whatsapp"])
            owner["whatsapp"] = [w for w in owner["whatsapp"] if _norm_phone(w.get("number")) != norm_n]
            if len(owner["whatsapp"]) < before: found = True
        if not found: return f"WhatsApp number {num} not found."
        await _rebuild_pools_preserving_rotation()
        return f"Deleted WhatsApp number {num} from all owners."

    if LIST_OWNERS_RX.match(text):
        if not OWNER_DATA: return "No owners configured."
        lines = ["<b>Owner Roster:</b>"]
        for o in OWNER_DATA:
            status = "PAUSED" if _owner_is_paused(o) else "active"
            u_count = len([e for e in o.get("entries", []) if not e.get("disabled")])
            w_count = len([w for w in o.get("whatsapp", []) if not w.get("disabled")])
            lines.append(f"- <code>{o['owner']}</code> ({status}): {u_count} usernames, {w_count} whatsapps")
        return "\n".join(lines)

    if LIST_DISABLED_RX.match(text):
        disabled = [o for o in OWNER_DATA if _owner_is_paused(o)]
        if not disabled: return "No owners are currently disabled/paused."
        lines = ["<b>Disabled/Paused Owners:</b>"]
        for o in disabled:
            reason = f" (until {o['disabled_until']})" if o.get("disabled_until") else ""
            lines.append(f"- <code>{o['owner']}</code>{reason}")
        return "\n".join(lines)

    m = LIST_OWNER_DETAIL_RX.match(text) or LIST_OWNER_ALIAS_RX.match(text)
    if m:
        name = m.group(1).strip()
        if name.lower() in ("owners", "disabled"): return None
        owner = _find_owner_group(name)
        if not owner: return f"Owner '{name}' not found."

        owner_status_flag = " ⛔" if _owner_is_paused(owner) else ""
        lines = [f"<b>Details for {owner['owner']}{owner_status_flag}:</b>"]
        if owner.get("entries"):
            lines.append("<u>Usernames:</u>")
            for e in owner["entries"]:
                flag = " ⛔" if e.get("disabled") else ""; h = e.get("telegram") or ""
                if h and not h.startswith("@"): h = "@" + h
                lines.append(f"- {h}{flag}")
        if owner.get("whatsapp"):
            lines.append("<u>WhatsApp Numbers:</u>")
            for w in owner["whatsapp"]:
                flag = " ⛔" if w.get("disabled") else ""
                lines.append(f"- {w['number']}{flag}")
        if len(lines) == 1: lines.append("No entries found.")
        return "\n".join(lines)

    m = REMIND_ALL_RX.match(text)
    if m: return await _send_all_pending_reminders(context)

    m = CLEAR_ALL_PENDING_RX.match(text)
    if m:
        issued_data = state.setdefault("issued", {})
        username_count = sum(len(items) for items in issued_data.get("username", {}).values())
        whatsapp_count = sum(len(items) for items in issued_data.get("whatsapp", {}).values())
        app_id_count = sum(len(items) for items in issued_data.get("app_id", {}).values())
        
        if username_count == 0 and whatsapp_count == 0 and app_id_count == 0:
            return "There were no pending items to clear."
        
        issued_data["username"] = {}
        issued_data["whatsapp"] = {}
        issued_data["app_id"] = {}
        
        await save_state()
        log.info(f"Admin cleared all pending items. Removed {username_count} usernames, {whatsapp_count} whatsapps, {app_id_count} app IDs.")
        return f"✅ All pending items have been cleared ({username_count} usernames, {whatsapp_count} whatsapps, {app_id_count} app IDs)."


    m = CLEAR_PENDING_RX.match(text)
    if m:
        item_to_clear = m.group(1).strip()
        for kind in ("username", "whatsapp", "app_id"):
            for user_id_str, items in list(_issued_bucket(kind).items()):
                for item in items:
                    stored_value = item.get("value")
                    match_found = False
                    if kind in ("username", "app_id"):
                        if stored_value and item_to_clear.lower() == stored_value.lower():
                            match_found = True
                    elif kind == "whatsapp":
                        if stored_value and _norm_phone(item_to_clear) == _norm_phone(stored_value):
                            match_found = True
                    
                    if match_found:
                        user_id = int(user_id_str)
                        if await _clear_issued(user_id, kind, stored_value):
                            user_info = state.get("user_names", {}).get(user_id_str, {})
                            user_name = user_info.get("username") or user_info.get("first_name") or f"ID {user_id}"
                            return f"✅ Cleared pending {kind} <code>{stored_value}</code> for user {user_name}."

        return f"❌ Could not find any user with the pending item <code>{item_to_clear}</code>."

    m = BAN_WHATSAPP_RX.match(text)
    if m:
        target_name = m.group(1)
        user_id_to_ban = _find_user_id_by_name(target_name)
        if not user_id_to_ban:
            return f"User '{target_name}' not found."

        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO whatsapp_bans (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", user_id_to_ban)

        WHATSAPP_BANNED_USERS.add(user_id_to_ban)
        return f"User {target_name} has been banned from requesting WhatsApp numbers."

    m = UNBAN_WHATSAPP_RX.match(text)
    if m:
        target_name = m.group(1)
        user_id_to_unban = _find_user_id_by_name(target_name)
        if not user_id_to_unban:
            return f"User '{target_name}' not found."

        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM whatsapp_bans WHERE user_id = $1", user_id_to_unban)

        WHATSAPP_BANNED_USERS.discard(user_id_to_unban)
        return f"User {target_name} has been unbanned from requesting WhatsApp numbers."

    if LIST_BANNED_RX.match(text):
        if not WHATSAPP_BANNED_USERS:
            return "No users are currently banned from requesting WhatsApp numbers."

        lines = ["<b>WhatsApp Banned Users:</b>"]
        for user_id in WHATSAPP_BANNED_USERS:
            user_info = state.get("user_names", {}).get(str(user_id), {})
            user_display = user_info.get('username') or user_info.get('first_name') or f"ID: {user_id}"
            lines.append(f"- {user_display}")
        return "\n".join(lines)

    m = OWNER_REPORT_RX.match(text)
    if m:
        target_day = _parse_report_day(m.group(1))
        _, owner_rows = await _compute_daily_summary(target_day)
        if not owner_rows:
            return f"No owner activity found for {target_day.isoformat()}."

        lines = [f"<b>Owner Performance for {target_day.isoformat()}:</b>"]
        for row in owner_rows:
            lines.append(f"- <b>{row['Owner']}</b>: {row['Customers total']} total ({row['Customers via Telegram']} usernames, {row['Customers via WhatsApp']} whatsapps)")
        return "\n".join(lines)

    if COMMANDS_RX.match(text):
        command_list_text = _get_commands_text()
        return command_list_text

    m = DETAIL_USER_RX.match(text)
    if m:
        target_name = m.group(1)
        target_user_id = _find_user_id_by_name(target_name)
        if not target_user_id:
            return f"User '{target_name}' not found."
        return await _get_user_detail_text(target_user_id)


    return None

# =============================
# REMINDER & RESET TASKS
# =============================
async def _send_all_pending_reminders(context: ContextTypes.DEFAULT_TYPE) -> str:
    total_reminders_sent = 0
    reminded_users = set()

    reminders_to_send = []
    # MODIFIED: Removed 'app_id' from the reminder loop
    for kind in ("username", "whatsapp"):
        bucket = _issued_bucket(kind)
        for user_id_str, items in bucket.items():
            for item in items:
                user_id = int(user_id_str)
                chat_id = item.get("chat_id")
                value = item.get("value")
                if chat_id and value:
                    label = "username" if kind == "username" else "WhatsApp"
                    reminder_text = (
                        f"សូមរំលឹក: {mention_user_html(user_id)}, "
                        f"អ្នកនៅមិនទាន់បានផ្តល់ព័ត៌មានសម្រាប់ {label} {value} ដែលអ្នកបានស្នើសុំ។"
                    )
                    reminders_to_send.append({'chat_id': chat_id, 'text': reminder_text, 'user_id': user_id})

    if not reminders_to_send:
        return "No pending items found to send reminders for."

    for r in reminders_to_send:
        try:
            await context.bot.send_message(
                chat_id=r['chat_id'],
                text=r['text'],
                parse_mode=ParseMode.HTML
            )
            total_reminders_sent += 1
            reminded_users.add(r['user_id'])
        except Exception as e:
            log.error(f"Error sending manual reminder for user {r['user_id']}: {e}")

    return f"Successfully sent {total_reminders_sent} reminder(s) to {len(reminded_users)} user(s)."


async def check_reminders(context: ContextTypes.DEFAULT_TYPE):
    reminders_to_send = []
    state_changed = False

    async with db_lock:
        now = datetime.now(TIMEZONE)

        # MODIFIED: Removed 'app_id' from the reminder loop
        for kind in ("username", "whatsapp"):
            bucket = _issued_bucket(kind)
            for user_id_str, items in list(bucket.items()):
                for item in list(items):
                    try:
                        last_reminder_ts_str = item.get("last_reminder_ts")
                        if last_reminder_ts_str:
                            base_ts = datetime.fromisoformat(last_reminder_ts_str)
                        else:
                            base_ts = datetime.fromisoformat(item["ts"])

                        if (now - base_ts) > timedelta(minutes=REMINDER_DELAY_MINUTES):
                            user_id = int(user_id_str)
                            chat_id = item.get("chat_id")
                            value = item.get("value")
                            if chat_id and value:
                                label = "username" if kind == "username" else "WhatsApp"
                                reminder_text = (
                                    f"សូមរំលឹក: {mention_user_html(user_id)}, "
                                    f"អ្នកនៅមិនទាន់បានផ្តល់ព័ត៌មានសម្រាប់ {label} {value} ដែលអ្នកបានស្នើសុំ។"
                                )
                                reminders_to_send.append({'chat_id': chat_id, 'text': reminder_text})
                                item["last_reminder_ts"] = now.isoformat()
                                item.pop("reminder_sent", None)
                                state_changed = True
                                log.info(f"Queued recurring reminder for user {user_id} for {kind} '{value}' in chat {chat_id}")
                    except Exception as e:
                        log.error(f"Error processing recurring reminder for user {user_id_str}: {e}")

        if state_changed:
            await save_state()

    for r in reminders_to_send:
        try:
            await context.bot.send_message(chat_id=r['chat_id'], text=r['text'], parse_mode=ParseMode.HTML)
        except Exception as e:
            log.error(f"Failed to send recurring reminder to chat {r['chat_id']}: {e}")


async def daily_reset(context: ContextTypes.DEFAULT_TYPE):
    log.info("Performing daily reset...")
    async with db_lock:
        try:
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                await conn.execute("DELETE FROM wa_daily_usage;")
                await conn.execute("DELETE FROM user_daily_activity;")
                await conn.execute("DELETE FROM user_daily_country_counts;")
                await conn.execute("DELETE FROM user_daily_confirmations;")
                await conn.execute("DELETE FROM owner_daily_performance;") # NEW
                log.info("Cleared daily WhatsApp, user activity, country, confirmation, and performance quotas from database.")
        except Exception as e:
            log.error(f"Failed to clear daily tables: {e}")

        await save_state()
        log.info("Daily reset complete. Pending items from previous days are preserved.")

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

    async with db_lock:
        # User "my detail" command
        if MY_DETAIL_RX.match(text):
            detail_text = await _get_user_detail_text(uid)
            await msg.reply_html(detail_text)
            return
            
        # NEW: Owner "my performance" command
        m_my_perf = MY_PERFORMANCE_RX.match(text)
        if m_my_perf:
            if _is_owner(update.effective_user):
                owner_name = _norm_owner_name(update.effective_user.username)
                target_day = _parse_report_day(m_my_perf.group(1))
                perf_text = await _get_owner_performance_text(owner_name, target_day)
                await msg.reply_html(perf_text)
            else:
                await msg.reply_text("This command is only for registered owners.")
            return

        # Admin: Report
        mrep = SEND_REPORT_RX.match(text)
        if _is_admin(update) and mrep:
            target_day = _parse_report_day(mrep.group(1))
            err, excel_buffer = await _get_daily_excel_report(target_day)

            if err:
                await msg.reply_text(err)
            elif excel_buffer:
                file_name = f"daily_summary_{target_day.isoformat()}.xlsx"
                await msg.reply_document(
                    document=excel_buffer, filename=file_name,
                    caption=f"Daily summary (logical day starting 05:30) — {target_day}"
                )
            return
        
        # NEW: Admin "performance @owner" command
        m_owner_perf = PERFORMANCE_OWNER_RX.match(text)
        if _is_admin(update) and m_owner_perf:
            owner_name_raw, day_str = m_owner_perf.groups()
            owner_name = _norm_owner_name(owner_name_raw)
            if not _find_owner_group(owner_name):
                await msg.reply_text(f"Owner '{owner_name}' not found.")
                return
            target_day = _parse_report_day(day_str)
            perf_text = await _get_owner_performance_text(owner_name, target_day)
            await msg.reply_html(perf_text)
            return

        # Admin: Console
        if _is_admin(update):
            admin_reply = await _handle_admin_command(text, context, update)
            if admin_reply:
                await msg.reply_html(admin_reply)
                return
            # Prevent non-admin commands from being misinterpreted as admin commands
            elif any(text.lower().startswith(cmd) for cmd in ['add ', 'delete ', 'list ', 'stop ', 'open ', 'remind ', 'take ', 'clear ', 'ban ', 'unban ', '+', 'owner report', 'commands', 'detail ']):
                await msg.reply_text("I don't recognize that admin command.")
                return

        if chat_id == CONFIRMATION_GROUP_ID:
            if '+1' in text:
                match = re.search(r'@([^\s]+)', text)
                if match:
                    app_id_confirmed_raw = f"@{match.group(1)}"
                    found_and_counted = False
                    
                    # Search all users to find who this App ID belongs to
                    for user_id_str, items in list(_issued_bucket("app_id").items()):
                        if found_and_counted: break
                        for item in items:
                            stored_app_id_raw = item.get("value", "")
                            
                            # MODIFIED: Try both direct and normalized match
                            match_is_found = (stored_app_id_raw == app_id_confirmed_raw or 
                                              _normalize_app_id(stored_app_id_raw) == _normalize_app_id(app_id_confirmed_raw))

                            if match_is_found:
                                user_id_of_item = int(user_id_str)
                                confirming_owner_name = _norm_owner_name(update.effective_user.username)
                                source_kind = item.get("source_kind")

                                await _increment_user_confirmation_count(user_id_of_item)
                                await _increment_owner_performance(confirming_owner_name, source_kind)
                                await _log_event("app_id", "confirmed", update, stored_app_id_raw, owner=confirming_owner_name)
                                await _clear_one_issued(user_id_of_item, "app_id", stored_app_id_raw)
                                log.info(f"Owner {confirming_owner_name} confirmed App ID {stored_app_id_raw}. Counted and cleared for user {user_id_of_item}")
                                found_and_counted = True
                                break
                    
                    if not found_and_counted:
                        await msg.reply_text("Wrong ID, please check.")
                        log.warning(f"Received confirmation for incorrect App ID '{app_id_confirmed_raw}' from {update.effective_user.username}.")
            return

        elif chat_id == CLEARING_GROUP_ID:
            values_found_in_message = set()
            user_id_str = str(uid)
            for kind in ("username", "whatsapp"):
                bucket = _issued_bucket(kind)
                if user_id_str in bucket:
                    for item in bucket[user_id_str]:
                        pending_value = item.get("value")
                        if _value_in_text(pending_value, text):
                            values_found_in_message.add(pending_value)
            
            app_id_match = APP_ID_RX.search(text)
            if app_id_match:
                app_id = f"@{app_id_match.group(1)}"
                
                context_data = {}
                # MODIFIED: Determine source kind based on what's being cleared, with a fallback to last requested item.
                if values_found_in_message:
                    # WhatsApp takes priority
                    cleared_item_value = None
                    kind_to_check = "username" # Default
                    for v in values_found_in_message:
                        if _looks_like_phone(v):
                            kind_to_check = "whatsapp"
                            cleared_item_value = v
                            break # Found whatsapp, no need to check further
                    
                    if not cleared_item_value:
                        cleared_item_value = next(iter(values_found_in_message))

                    user_items = _issued_bucket(kind_to_check).get(str(uid), [])
                    for item in user_items:
                        if item.get("value") == cleared_item_value:
                            context_data["source_owner"] = item.get("owner")
                            context_data["source_kind"] = kind_to_check
                            break
                else: # Fallback if nothing is being cleared in this message
                    last_item = None
                    last_ts = datetime.min.replace(tzinfo=TIMEZONE)
                    for kind_to_check in ("username", "whatsapp"):
                        user_items = _issued_bucket(kind_to_check).get(str(uid), [])
                        if user_items:
                            latest_in_kind = user_items[-1] # Get the most recently issued item of this kind
                            item_ts = datetime.fromisoformat(latest_in_kind["ts"])
                            if item_ts > last_ts:
                                last_ts = item_ts
                                last_item = latest_in_kind
                                last_item['kind'] = kind_to_check
                    
                    if last_item:
                        context_data["source_owner"] = last_item.get("owner")
                        context_data["source_kind"] = last_item.get("kind")
                
                await _set_issued(uid, chat_id, "app_id", app_id, context_data=context_data)
                await _log_event("app_id", "issued", update, app_id, owner=context_data.get("source_owner", ""))
                log.info(f"Recorded new pending App ID '{app_id}' for user {uid} linked to owner {context_data.get('source_owner')} and kind {context_data.get('source_kind')}")

            if values_found_in_message:
                found_country, country_status = _find_country_in_text(text)
                age = _find_age_in_text(text)
                is_allowed = True
                rejection_reason = ""

                if country_status:
                    if country_status == 'not_allowed':
                        is_allowed = False
                        rejection_reason = f"Country '{found_country}' is not allowed."
                    elif country_status == 'india' and (age is None or age <= 30):
                        is_allowed = False
                        rejection_reason = f"Age must be provided and > 30 for India (got: {age})."

                if not is_allowed:
                    first_offending_value = next(iter(values_found_in_message))
                    item_type = "username" if first_offending_value.startswith('@') else "whatsapp"
                    reply_text = (f"{mention_user_html(uid)}, this country is not allowed. "
                                  f"Please use that {item_type} (<code>{first_offending_value}</code>) for another customer.")
                    await msg.reply_html(reply_text)
                    log.warning(f"Rejected post from user {uid}. Reason: {rejection_reason}.")
                else:
                    if country_status and country_status != 'not_allowed':
                        await _increment_user_country_count(uid, country_status)
                        log.info(f"Incremented country count for user {uid} for '{country_status}'")
                    
                    for kind in ("username", "whatsapp"):
                        for value_to_clear in values_found_in_message:
                            if await _clear_one_issued(uid, kind, value_to_clear):
                                await _log_event(kind, "cleared", update, value_to_clear, owner="")
                                log.info(f"Auto-cleared ONE pending {kind} for user {uid}: {value_to_clear}")
            return

        elif chat_id == REQUEST_GROUP_ID:
            if NEED_USERNAME_RX.match(text):
                rec = await _next_from_username_pool()
                reply = "No available username." if not rec else f"@{rec['owner']}\n{rec['username']}"
                await msg.reply_text(reply)
                if rec:
                    await _set_issued(uid, chat_id, "username", rec["username"], context_data={"owner": rec["owner"]})
                    await _log_event("username", "issued", update, rec["username"], owner=rec["owner"])
                    await _increment_user_activity(uid, "username")
                return

            if NEED_WHATSAPP_RX.match(text):
                if uid in WHATSAPP_BANNED_USERS:
                    await msg.reply_text("អ្នកត្រូវបានហាមឃាត់ពីការស្នើសុំលេខ WhatsApp ។")
                    return

                username_count, whatsapp_count = await _get_user_activity(uid)
                has_bonus = username_count > USERNAME_THRESHOLD_FOR_BONUS

                if not has_bonus and whatsapp_count >= USER_WHATSAPP_LIMIT:
                    await msg.reply_text(f"អ្នកបានស្នើសុំ WhatsApp គ្រប់ចំនួនកំណត់សម្រាប់ថ្ងៃនេះហើយ។\nសូមស្នើសុំ username ឱ្យលើសពី {USERNAME_THRESHOLD_FOR_BONUS} ដើម្បីទទួលបានការស្នើសុំ WhatsApp បន្ថែមទៀតដោយគ្មានដែនកំណត់។")
                    return

                rec = await _next_from_whatsapp_pool()
                reply = "No available WhatsApp."
                if rec:
                    if await _wa_quota_reached(rec["number"]):
                        reply = "No available WhatsApp (daily limit may be reached)."
                        rec = None
                    else:
                        reply = f"@{rec['owner']}\n{rec['number']}"

                await msg.reply_text(reply)
                if rec:
                    await _wa_inc_count(_norm_phone(rec["number"]), _logical_day_today())
                    await _set_issued(uid, chat_id, "whatsapp", rec["number"], context_data={"owner": rec["owner"]})
                    await _log_event("whatsapp", "issued", update, rec["number"], owner=rec["owner"])
                    await _increment_user_activity(uid, "whatsapp")
                return

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

            await msg.reply_text(reply)
            return

# =============================
# MAIN
# =============================
async def post_initialization(application: Application):
    """Runs once after the bot is initialized."""
    await get_db_pool()
    await setup_database()
    await load_state()
    await _migrate_state_if_needed()
    await load_owner_directory()
    await load_whatsapp_bans()

async def post_shutdown(application: Application):
    """Runs once before the bot shuts down."""
    await close_db_pool()


if __name__ == "__main__":
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_initialization)
        .post_shutdown(post_shutdown)
        .build()
    )

    if app.job_queue:
        app.job_queue.run_repeating(check_reminders, interval=60, first=60)
        reset_time = time(hour=5, minute=31, tzinfo=TIMEZONE)
        app.job_queue.run_daily(daily_reset, time=reset_time)

    app.add_handler(MessageHandler(filters.ALL & ~filters.StatusUpdate.ALL, on_message))

    log.info("Bot is starting...")
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)

