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
import unicodedata
import pytz
import asyncpg
from telegram import Update
from telegram.constants import ChatType, ParseMode
from telegram.ext import Application, ContextTypes, MessageHandler, filters
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s: %(message)s", level=logging.INFO)
log = logging.getLogger("bot")
def get_env_variable(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value: raise RuntimeError(f"Missing required environment variable '{var_name}'.")
    return value
BOT_TOKEN = get_env_variable("BOT_TOKEN")
DATABASE_URL = get_env_variable("DATABASE_URL")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "excelmerge")
WA_DAILY_LIMIT = int(os.getenv("WA_DAILY_LIMIT", "2"))
REMINDER_DELAY_MINUTES = int(os.getenv("REMINDER_DELAY_MINUTES", "30"))
USER_WHATSAPP_LIMIT = int(os.getenv("USER_WHATSAPP_LIMIT", "10"))
USERNAME_THRESHOLD_FOR_BONUS = int(os.getenv("USERNAME_THRESHOLD_FOR_BONUS", "25"))
REQUEST_GROUP_ID = int(os.getenv("REQUEST_GROUP_ID", "-1002438185636"))
CLEARING_GROUP_ID = int(os.getenv("CLEARING_GROUP_ID", "-1002624324856"))
CONFIRMATION_GROUP_ID = int(os.getenv("CONFIRMATION_GROUP_ID", "-1002694540582"))
DETAIL_GROUP_ID = int(os.getenv("DETAIL_GROUP_ID", "-1002598927727"))
FORWARD_GROUP_ID = int(os.getenv("FORWARD_GROUP_ID", "-1003109226804"))
PERFORMANCE_GROUP_IDS = {-1002670785417, -1002659012767, -1002790753092, -1002520117752}
COUNTRY_DAILY_LIMIT = int(os.getenv("COUNTRY_DAILY_LIMIT", "17"))
ALLOWED_COUNTRIES = {'panama', 'united arab emirates','oman', 'jordan', 'italy', 'germany', 'indonesia', 'bulgaria', 'brazil', 'spain', 'belgium','portugal', 'netherlands', 'poland', 'qatar', 'france', 'switzerland', 'argentina', 'costa rica', 'kuwait', 'bahrain', 'malaysia','canada','mauritania', 'greece','belarus', 'slovakia', 'hungary', 'romania', 'luxembourg', 'czechia', 'india', 'austria', 'tunisia', 'iran', 'mexico', 'russia'}
TIMEZONE = pytz.timezone("Asia/Phnom_Penh")
db_lock = asyncio.Lock()
DB_POOL: Optional[asyncpg.Pool] = None
async def get_db_pool() -> asyncpg.Pool:
    global DB_POOL
    if DB_POOL is None or DB_POOL.is_closing():
        try:
            DB_POOL = await asyncpg.create_pool(dsn=DATABASE_URL, max_inactive_connection_lifetime=60, min_size=1, max_size=10)
            if DB_POOL is None: raise ConnectionError("Database pool initialization failed.")
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
        await conn.execute("CREATE TABLE IF NOT EXISTS kv_storage ( key TEXT PRIMARY KEY, data JSONB NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW() );")
        await conn.execute("CREATE TABLE IF NOT EXISTS audit_log (id SERIAL PRIMARY KEY, ts_local TIMESTAMPTZ NOT NULL, chat_id BIGINT, message_id BIGINT, user_id BIGINT, user_first TEXT, user_username TEXT, kind TEXT, action TEXT, value TEXT, owner TEXT );")
        await conn.execute("CREATE TABLE IF NOT EXISTS wa_daily_usage ( day DATE NOT NULL, number_norm TEXT NOT NULL, sent_count INTEGER NOT NULL DEFAULT 0, last_sent TIMESTAMPTZ, PRIMARY KEY (day, number_norm));")
        await conn.execute("CREATE TABLE IF NOT EXISTS user_daily_activity ( day DATE NOT NULL, user_id BIGINT NOT NULL, username_requests INTEGER DEFAULT 0, whatsapp_requests INTEGER DEFAULT 0, PRIMARY KEY (day, user_id));")
        await conn.execute("CREATE TABLE IF NOT EXISTS whatsapp_bans (user_id BIGINT PRIMARY KEY);")
        await conn.execute("CREATE TABLE IF NOT EXISTS whitelisted_users ( user_id BIGINT PRIMARY KEY );")
        await conn.execute("CREATE TABLE IF NOT EXISTS user_daily_country_counts ( day DATE NOT NULL, user_id BIGINT NOT NULL, country TEXT NOT NULL, count INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (day, user_id, country));")
        await conn.execute("CREATE TABLE IF NOT EXISTS user_daily_confirmations ( day DATE NOT NULL, user_id BIGINT NOT NULL, confirm_count INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (day, user_id));")
        await conn.execute("CREATE TABLE IF NOT EXISTS owner_daily_performance ( day DATE NOT NULL, owner_name TEXT NOT NULL, telegram_count INTEGER NOT NULL DEFAULT 0, whatsapp_count INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (day, owner_name) );")
        await conn.execute("CREATE TABLE IF NOT EXISTS admins ( username TEXT PRIMARY KEY, permissions JSONB NOT NULL );")
        await conn.execute("CREATE TABLE IF NOT EXISTS user_country_bans ( user_id BIGINT NOT NULL, country TEXT NOT NULL, PRIMARY KEY (user_id, country));")
    log.info("Database schema is ready.")
BASE_STATE = {"user_names": {},"rr": {"username_owner_idx": 0, "username_entry_idx": {},"wa_owner_idx": 0, "wa_entry_idx": {},}, "issued": {"username": {}, "whatsapp": {}, "app_id": {}}, "priority_queue": {}, "whatsapp_temp_bans": {}, "whatsapp_last_request_ts": {}, "username_last_request_ts": {}, "whatsapp_offense_count": {}, "username_round_count": 0, "whatsapp_round_count": 0, "wa_45min_counter": 0}
state: Dict = {k: (v.copy() if isinstance(v, dict) else v) for k, v in BASE_STATE.items()}
WHATSAPP_BANNED_USERS: set[int] = set()
WHITELISTED_USERS: set[int] = set()
ADMIN_PERMISSIONS: Dict[str, List[str]] = {}
USER_COUNTRY_BANS: Dict[int, set[str]] = {}
COMMAND_PERMISSIONS = {'add owner', 'delete owner', 'add username', 'delete username', 'add whatsapp', 'delete whatsapp', 'stop open', 'take customer', 'ban whatsapp', 'unban whatsapp','performance', 'remind user', 'clear pending', 'list disabled', 'detail user', 'list banned', 'list admins', 'data today', 'list enabled', 'add user', 'delete user', 'ban country', 'unban country', 'list country bans', 'user performance', 'user stats', 'inventory', 'list priority', 'round count', 'cancel priority', 'list owner'}
async def load_admins():
    global ADMIN_PERMISSIONS
    ADMIN_PERMISSIONS = {}
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT username, permissions FROM admins")
            for row in rows: ADMIN_PERMISSIONS[row['username']] = json.loads(row['permissions'])
        log.info(f"Loaded {len(ADMIN_PERMISSIONS)} admins.")
    except Exception as e: log.error(f"Failed to load admins: {e}")
def _is_super_admin(user: Optional[Update.effective_user]) -> bool:
    if not user: return False
    return (user.username or "").lower() == ADMIN_USERNAME.lower()
def _is_admin(user: Optional[Update.effective_user]) -> bool:
    if not user or not user.username: return False
    return _is_super_admin(user) or _norm_owner_name(user.username) in ADMIN_PERMISSIONS
def _has_permission(user: Optional[Update.effective_user], permission: str) -> bool:
    if not user or not user.username: return False
    if _is_super_admin(user): return True
    return permission in ADMIN_PERMISSIONS.get(_norm_owner_name(user.username), [])
async def load_whatsapp_bans():
    global WHATSAPP_BANNED_USERS
    WHATSAPP_BANNED_USERS = set()
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id FROM whatsapp_bans")
            for row in rows: WHATSAPP_BANNED_USERS.add(row['user_id'])
        log.info(f"Loaded {len(WHATSAPP_BANNED_USERS)} bans.")
    except Exception as e: log.error(f"Failed to load bans: {e}")
async def load_user_country_bans():
    global USER_COUNTRY_BANS
    USER_COUNTRY_BANS = {}
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id, country FROM user_country_bans")
            for row in rows:
                uid, c = row['user_id'], row['country'].lower()
                if uid not in USER_COUNTRY_BANS: USER_COUNTRY_BANS[uid] = set()
                USER_COUNTRY_BANS[uid].add(c)
        log.info(f"Loaded manual country bans.")
    except Exception as e: log.error(f"Failed to load country bans: {e}")
async def load_whitelisted_users():
    global WHITELISTED_USERS
    WHITELISTED_USERS = set()
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id FROM whitelisted_users")
            for row in rows: WHITELISTED_USERS.add(row['user_id'])
        log.info(f"Loaded {len(WHITELISTED_USERS)} whitelist.")
    except Exception as e: log.error(f"Failed to load whitelist: {e}")
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
                    if isinstance(value, dict) and key in temp_state: temp_state[key].update(value)
                    else: temp_state[key] = value
                state = temp_state
                log.info("State loaded.")
            else:
                state = {k: (v.copy() if isinstance(v, dict) else v) for k, v in BASE_STATE.items()}
                await save_state()
    except Exception as e:
        log.warning(f"Failed load state: {e}. Using default.")
        state = {k: (v.copy() if isinstance(v, dict) else v) for k, v in BASE_STATE.items()}
    state.setdefault("rr", {}).setdefault("username_entry_idx", {})
    state["rr"].setdefault("wa_entry_idx", {})
    state.setdefault("issued", {}).setdefault("username", {})
    state["issued"].setdefault("whatsapp", {})
    state["issued"].setdefault("app_id", {})
    state.setdefault("priority_queue", BASE_STATE["priority_queue"])
    state.setdefault("whatsapp_temp_bans", {})
    state.setdefault("whatsapp_last_request_ts", {})
    state.setdefault("username_last_request_ts", {})
    state.setdefault("whatsapp_offense_count", {})
    state.setdefault("username_round_count", 0)
    state.setdefault("whatsapp_round_count", 0)
    state.setdefault("wa_45min_counter", 0)
async def save_state():
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO kv_storage (key, data) VALUES ('state', $1) ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data, updated_at = NOW();", json.dumps(state))
    except Exception as e: log.warning(f"Failed save state: {e}")
async def _migrate_state_if_needed():
    log.info("Checking migration...")
    chg = False
    issued = state.setdefault("issued", {})
    for kind in ("username", "whatsapp", "app_id"):
        bucket = issued.setdefault(kind, {})
        for uid, item in bucket.items():
            if isinstance(item, dict): bucket[uid] = [item]; chg = True
    pq = state.get("priority_queue", {})
    if isinstance(pq, dict) and ("active" in pq or "owner" in pq):
        new_pq = {}
        if pq.get("active") and pq.get("owner") and pq.get("remaining", 0) > 0:
            new_pq[pq["owner"]] = {"remaining": pq["remaining"], "stop_after": pq.get("stop_after", False)}
        state["priority_queue"] = new_pq; chg = True
    if chg: await save_state()
OWNER_DATA: List[dict] = []
HANDLE_INDEX: Dict[str, List[dict]] = {}
PHONE_INDEX: Dict[str, dict] = {}
USERNAME_POOL: List[Dict] = []
WHATSAPP_POOL: List[Dict] = []
def _norm_handle(h: str) -> str: return re.sub(r"^@", "", (h or "").strip().lower())
def _norm_phone(p: str) -> str: return re.sub(r"\D+", "", (p or ""))
def _normalize_app_id(app_id: str) -> str:
    if not app_id: return ""
    return re.sub(r'[^a-zA-Z0-9]', '', unicodedata.normalize('NFKC', app_id)).lower()
def _norm_owner_name(s: str) -> str: return (s or "").strip().lstrip("@").lower()
def _is_owner(user: Optional[Update.effective_user]) -> bool:
    if not user or not user.username: return False
    norm = _norm_owner_name(user.username)
    return any(_norm_owner_name(g.get("owner", "")) == norm for g in OWNER_DATA)
def _owner_is_paused(group: dict) -> bool:
    if group.get("disabled") or group.get("active") is False: return True
    if group.get("disabled_until"):
        try:
            if datetime.now(tz=TIMEZONE) < datetime.fromisoformat(group["disabled_until"]).replace(tzinfo=TIMEZONE): return True
        except: pass
    return False
def _ensure_owner_shape(g: dict) -> dict:
    g.setdefault("owner", ""); g.setdefault("disabled", False); g.setdefault("managed_by", None); g.setdefault("entries", []); g.setdefault("whatsapp", []); g.setdefault("forward_group_id", None)
    g["entries"] = [dict(e, telegram=e.get("telegram",""), phone=e.get("phone",""), disabled=e.get("disabled",False)) for e in g.get("entries",[]) if isinstance(e, dict)]
    norm_wa = []
    for w in g.get("whatsapp", []):
        entry = w.copy() if isinstance(w, dict) else {"number": w}
        if (entry.get("number") or "").strip():
            entry.setdefault("disabled", False); entry.setdefault("managed_by", None)
            norm_wa.append(entry)
    g["whatsapp"] = norm_wa
    return g
async def save_owner_directory():
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn: await conn.execute("INSERT INTO kv_storage (key, data) VALUES ('owners', $1) ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data, updated_at = NOW();", json.dumps(OWNER_DATA))
    except Exception as e: log.error(f"Failed save owners: {e}")
async def load_owner_directory():
    global OWNER_DATA, HANDLE_INDEX, PHONE_INDEX, USERNAME_POOL, WHATSAPP_POOL
    OWNER_DATA, HANDLE_INDEX, PHONE_INDEX, USERNAME_POOL, WHATSAPP_POOL = [], {}, {}, [], []
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            r = await conn.fetchval("SELECT data FROM kv_storage WHERE key = 'owners'")
            OWNER_DATA = (json.loads(r) or []) if r else []
    except Exception as e: log.error(f"Failed load owners: {e}")
    OWNER_DATA = [_ensure_owner_shape(dict(g)) for g in OWNER_DATA]
    for group in OWNER_DATA:
        owner = _norm_owner_name(group.get("owner") or "")
        if not owner or _owner_is_paused(group): continue
        usernames = []
        for entry in group.get("entries", []):
            if entry.get("disabled"): continue
            tel, ph = (entry.get("telegram") or "").strip(), (entry.get("phone") or "").strip()
            if tel:
                usernames.append(tel if tel.startswith("@") else f"@{tel}")
                HANDLE_INDEX.setdefault(_norm_handle(tel), []).append({"owner": owner, "phone": ph, "telegram": tel, "channel": "telegram"})
            if ph: PHONE_INDEX[_norm_phone(ph)] = {"owner": owner, "phone": ph, "telegram": tel, "channel": "telegram"}
        if usernames: USERNAME_POOL.append({"owner": owner, "usernames": usernames})
        numbers = []
        for w in group.get("whatsapp", []):
            if w.get("disabled"): continue
            num = (w.get("number") or "").strip()
            if num:
                numbers.append(num)
                PHONE_INDEX[_norm_phone(num)] = {"owner": owner, "phone": num, "telegram": None, "channel": "whatsapp"}
        if numbers: WHATSAPP_POOL.append({"owner": owner, "numbers": numbers})
    log.info(f"Owners loaded: {len(USERNAME_POOL)} u-pools, {len(WHATSAPP_POOL)} w-pools.")
def _logical_day_today() -> date: return (datetime.now(TIMEZONE) - timedelta(hours=3, minutes=30)).date()
async def _get_user_activity(user_id: int) -> Tuple[int, int]:
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            r = await conn.fetchrow("SELECT username_requests, whatsapp_requests FROM user_daily_activity WHERE day=$1 AND user_id=$2", _logical_day_today(), user_id)
            return (r['username_requests'], r['whatsapp_requests']) if r else (0, 0)
    except: return (0, 0)
async def _increment_user_activity(user_id: int, kind: str):
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            col = "username_requests" if kind == "username" else "whatsapp_requests"
            await conn.execute(f"INSERT INTO user_daily_activity (day, user_id, {col}) VALUES ($1, $2, 1) ON CONFLICT (day, user_id) DO UPDATE SET {col} = user_daily_activity.{col} + 1;", _logical_day_today(), user_id)
    except Exception as e: log.warning(f"Inc activity failed: {e}")
async def _increment_user_country_count(user_id: int, country: str):
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn: await conn.execute("INSERT INTO user_daily_country_counts (day, user_id, country, count) VALUES ($1, $2, $3, 1) ON CONFLICT (day, user_id, country) DO UPDATE SET count = user_daily_country_counts.count + 1;", _logical_day_today(), user_id, country)
    except: pass
async def _get_user_country_count(user_id: int, country: str) -> int:
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            c = await conn.fetchval("SELECT count FROM user_daily_country_counts WHERE day=$1 AND user_id=$2 AND country=$3", _logical_day_today(), user_id, country)
            return int(c) if c else 0
    except: return 0
async def _increment_user_confirmation_count(user_id: int):
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn: await conn.execute("INSERT INTO user_daily_confirmations (day, user_id, confirm_count) VALUES ($1, $2, 1) ON CONFLICT (day, user_id) DO UPDATE SET confirm_count = user_daily_confirmations.confirm_count + 1;", _logical_day_today(), user_id)
    except: pass
async def _increment_owner_performance(owner_name: str, kind: Optional[str]):
    if not owner_name: return
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            col = "telegram_count" if kind == 'username' else "whatsapp_count" if kind == 'whatsapp' else None
            if col: await conn.execute(f"INSERT INTO owner_daily_performance (day, owner_name, {col}) VALUES ($1, $2, 1) ON CONFLICT (day, owner_name) DO UPDATE SET {col} = owner_daily_performance.{col} + 1;", _logical_day_today(), owner_name)
    except: pass
async def _wa_get_count(number_norm: str, day: date) -> int:
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            c = await conn.fetchval("SELECT sent_count FROM wa_daily_usage WHERE day=$1 AND number_norm=$2", day, number_norm)
            return int(c) if c else 0
    except: return 0
async def _wa_inc_count(number_norm: str, day: date):
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn: await conn.execute("INSERT INTO wa_daily_usage (day, number_norm, sent_count, last_sent) VALUES ($1, $2, 1, $3) ON CONFLICT (day, number_norm) DO UPDATE SET sent_count = wa_daily_usage.sent_count + 1, last_sent = EXCLUDED.last_sent", day, number_norm, datetime.now(TIMEZONE))
    except: pass
async def _wa_quota_reached(number_raw: str) -> bool:
    return (await _wa_get_count(_norm_phone(number_raw), _logical_day_today())) >= WA_DAILY_LIMIT
def _owner_list_from_pool(pool) -> List[str]: return [blk["owner"] for blk in pool]
def _preserve_owner_pointer(old_list: List[str], new_list: List[str], old_idx: int) -> int:
    if not new_list: return 0
    old_list = list(old_list or [])
    if not old_list: return 0
    if old_idx >= len(old_list): old_idx = 0
    if old_list[old_idx] in new_list: return new_list.index(old_list[old_idx])
    for step in range(1, len(old_list) + 1):
        cand = old_list[(old_idx + step) % len(old_list)]
        if cand in new_list: return new_list.index(cand)
    return 0
def _preserve_entry_indices(rr_map: Dict[str, int], new_pool: List[Dict], list_key: str):
    valid_owners = {blk["owner"]: len(blk.get(list_key, []) or []) for blk in new_pool}
    for owner in list(rr_map.keys()):
        if owner not in valid_owners or valid_owners[owner] <= 0: rr_map.pop(owner, None)
        else: rr_map[owner] %= valid_owners[owner]
async def _rebuild_pools_preserving_rotation():
    old_u_list, old_w_list = _owner_list_from_pool(USERNAME_POOL), _owner_list_from_pool(WHATSAPP_POOL)
    rr = state.setdefault("rr", {})
    old_u_idx, old_w_idx = rr.get("username_owner_idx", 0), rr.get("wa_owner_idx", 0)
    await save_owner_directory()
    await load_owner_directory()
    new_u_list, new_w_list = _owner_list_from_pool(USERNAME_POOL), _owner_list_from_pool(WHATSAPP_POOL)
    rr["username_owner_idx"] = _preserve_owner_pointer(old_u_list, new_u_list, old_u_idx)
    rr["wa_owner_idx"] = _preserve_owner_pointer(old_w_list, new_w_list, old_w_idx)
    _preserve_entry_indices(rr.setdefault("username_entry_idx", {}), USERNAME_POOL, "usernames")
    _preserve_entry_indices(rr.setdefault("wa_entry_idx", {}), WHATSAPP_POOL, "numbers")
    await save_state()
async def _decrement_priority_and_end_if_needed(owner_name: str):
    pq_map = state.setdefault("priority_queue", {})
    owner_pq = pq_map.get(owner_name)
    if not owner_pq: return
    owner_pq["remaining"] -= 1
    if owner_pq["remaining"] <= 0:
        log.info(f"Priority queue for {owner_name} completed.")
        stop = owner_pq.get("stop_after", False)
        del pq_map[owner_name]
        if stop:
            grp = _find_owner_group(owner_name)
            if grp: grp["disabled"] = True
            await _rebuild_pools_preserving_rotation()
        else: await save_state()
    else: await save_state()
async def _next_from_username_pool() -> Optional[Dict[str, str]]:
    if not USERNAME_POOL: return None
    idx = state['rr'].get("username_owner_idx", 0)
    if idx >= len(USERNAME_POOL): idx = 0
    for i in range(len(USERNAME_POOL)):
        curr = (idx + i) % len(USERNAME_POOL)
        blk = USERNAME_POOL[curr]
        owner, arr = blk["owner"], blk.get("usernames", [])
        if arr:
            ei = state["rr"]["username_entry_idx"].get(owner, 0) % len(arr)
            res = {"owner": owner, "username": arr[ei]}
            state["rr"]["username_entry_idx"][owner] = (ei + 1) % len(arr)
            if (curr + 1) % len(USERNAME_POOL) == 0 and curr == len(USERNAME_POOL) - 1:
                state['username_round_count'] = state.get('username_round_count', 0) + 1
            if owner in state.get("priority_queue", {}):
                log.info(f"Priority Serve: {owner}")
                await _decrement_priority_and_end_if_needed(owner)
            else:
                state["rr"]["username_owner_idx"] = (curr + 1) % len(USERNAME_POOL)
                await save_state()
            return res
    return None
async def _next_from_whatsapp_pool() -> Optional[Dict[str, str]]:
    if not WHATSAPP_POOL: return None
    idx = state['rr'].get("wa_owner_idx", 0)
    if idx >= len(WHATSAPP_POOL): idx = 0
    for i in range(len(WHATSAPP_POOL)):
        curr = (idx + i) % len(WHATSAPP_POOL)
        blk = WHATSAPP_POOL[curr]
        owner, nums = blk["owner"], blk.get("numbers", [])
        if nums:
            start = state["rr"]["wa_entry_idx"].get(owner, 0) % len(nums)
            for step in range(len(nums)):
                cand = nums[(start + step) % len(nums)]
                if await _wa_quota_reached(cand): continue
                state["rr"]["wa_entry_idx"][owner] = ((start + step) + 1) % len(nums)
                if (curr + 1) % len(WHATSAPP_POOL) == 0 and curr == len(WHATSAPP_POOL) - 1:
                    state['whatsapp_round_count'] = state.get('whatsapp_round_count', 0) + 1
                if owner in state.get("priority_queue", {}):
                    await _decrement_priority_and_end_if_needed(owner)
                else:
                    state["rr"]["wa_owner_idx"] = (curr + 1) % len(WHATSAPP_POOL)
                    await save_state()
                return {"owner": owner, "number": cand}
    return None
WHO_USING_REGEX = re.compile(r"^\s*who(?:['\u2019']s| is)\s+using\s+(?:@?([A-Za-z0-9_\.]+)|(\+?\d[\d\s\-]{6,}\d))\s*$", re.IGNORECASE)
NEED_USERNAME_RX = re.compile(r"^\s*i\s*need\s*(?:user\s*name|username)\s*$", re.IGNORECASE)
NEED_WHATSAPP_RX = re.compile(r"^\s*i\s*need\s*(?:id\s*)?whats?app\s*$", re.IGNORECASE)
APP_ID_RX = re.compile(r"\b(app|add|id)\b.*?\@([^\s]+)", re.IGNORECASE)
EXTRACT_USERNAMES_RX = re.compile(r'@([a-zA-Z0-9_]{4,})')
EXTRACT_PHONES_RX = re.compile(r'(\+?\d[\d\s\-()]{8,}\d)')
STOP_OPEN_RX = re.compile(r"^\s*(stop|open)\s+(.+?)\s*$", re.IGNORECASE)
ADD_OWNER_RX = re.compile(r"^\s*add\s+owner\s+@?(.+?)\s*$", re.IGNORECASE)
ADD_USERNAME_RX = re.compile(r"^\s*add\s+username\s+@([A-Za-z0-9_]{3,})\s+to\s+@?(.+?)\s*$", re.IGNORECASE)
ADD_WHATSAPP_RX = re.compile(r"^\s*add\s+whats?app\s+(\+?\d[\d\s\-]{6,}\d)\s+to\s+@?(.+?)\s*$", re.IGNORECASE)
DEL_OWNER_RX = re.compile(r"^\s*delete\s+owner\s+@?(.+?)\s*$", re.IGNORECASE)
DEL_USERNAME_RX = re.compile(r"^\s*delete\s+username\s+@([A-Za-z0-9_]{3,})\s*$", re.IGNORECASE)
DEL_WHATSAPP_RX = re.compile(r"^\s*delete\s+whats?app\s+(\+?\d[\d\s\-]{6,}\d)\s*$", re.IGNORECASE)
LIST_OWNER_DETAIL_RX = re.compile(r"^\s*list\s+owner\s+@?([A-Za-z0-9_]{3,})\s*$", re.IGNORECASE)
LIST_DISABLED_RX = re.compile(r"^\s*list\s+disabled\s*$", re.IGNORECASE)
PHONE_LIKE_RX = re.compile(r"^\+?\d[\d\s\-]{6,}\d$")
LIST_OWNER_ALIAS_RX = re.compile(r"^\s*list\s+@?([A-Za-z0-9_]{3,})\s*$", re.IGNORECASE)
REMIND_ALL_RX = re.compile(r"^\s*remind\s+user\s*$", re.IGNORECASE)
TAKE_CUSTOMER_RX = re.compile(r"^\s*take\s+(\d+)\s+customer(?:s)?\s+to\s+owners?\s+(.+?)(?:\s+(and\s+stop))?\s*$", re.IGNORECASE)
CLEAR_PENDING_RX = re.compile(r"^\s*clear\s+pending\s+(.+)\s*$", re.IGNORECASE)
BAN_WHATSAPP_RX = re.compile(r"^\s*ban\s+whatsapp\s+@?(\S+)\s*$", re.IGNORECASE)
UNBAN_WHATSAPP_RX = re.compile(r"^\s*unban\s+whatsapp\s+@?(\S+)\s*$", re.IGNORECASE)
LIST_BANNED_RX = re.compile(r"^\s*list\s+banned\s*$", re.IGNORECASE)
COMMANDS_RX = re.compile(r"^\s*commands\s*$", re.IGNORECASE)
MY_DETAIL_RX = re.compile(r"^\s*my\s+detail\s*$", re.IGNORECASE)
DETAIL_USER_RX = re.compile(r"^\s*detail\s+@?(\S+)\s*$", re.IGNORECASE)
MY_PERFORMANCE_RX = re.compile(r"^\s*my\s+performance(?:\s+(yesterday|today|\d{4}-\d{2}-\d{2}))?\s*$", re.IGNORECASE)
PERFORMANCE_OWNER_RX = re.compile(r"^\s*performance\s+@?(\S+?)(?:\s+(yesterday|today|\d{4}-\d{2}-\d{2}))?\s*$", re.IGNORECASE)
ADD_ADMIN_RX = re.compile(r"^\s*add\s+admin\s+@?(\S+)\s*$", re.IGNORECASE)
DELETE_ADMIN_RX = re.compile(r"^\s*delete\s+admin\s+@?(\S+)\s*$", re.IGNORECASE)
ALLOW_ADMIN_CMD_RX = re.compile(r"^\s*allow\s+@?(\S+)\s+to\s+use\s+command\s+(.+)\s*$", re.IGNORECASE)
STOP_ALLOW_ADMIN_CMD_RX = re.compile(r"^\s*stop\s+allow\s+@?(\S+)\s+to\s+use\s+command\s+(.+)\s*$", re.IGNORECASE)
LIST_ADMINS_RX = re.compile(r"^\s*list\s+admins\s*$", re.IGNORECASE)
DATA_TODAY_RX = re.compile(r"^\s*data\s+today\s*$", re.IGNORECASE)
LIST_ENABLED_RX = re.compile(r"^\s*list\s+enabled\s*$", re.IGNORECASE)
ADD_USER_RX = re.compile(r"^\s*add\s+user\s+@?(\S+)\s*$", re.IGNORECASE)
DELETE_USER_RX = re.compile(r"^\s*delete\s+user\s+@?(\S+)\s*$", re.IGNORECASE)
BAN_COUNTRY_RX = re.compile(r"^\s*ban\s+country\s+([a-zA-Z\s]+)\s+for\s+@?(\S+)\s*$", re.IGNORECASE)
UNBAN_COUNTRY_RX = re.compile(r"^\s*unban\s+country\s+([a-zA-Z\s]+)\s+for\s+@?(\S+)\s*$", re.IGNORECASE)
LIST_COUNTRY_BANS_RX = re.compile(r"^\s*list\s+country\s+bans(?:\s+@?(\S+))?\s*$", re.IGNORECASE)
LIST_PRIORITY_RX = re.compile(r"^\s*list\s+priority\s*$", re.IGNORECASE)
CANCEL_PRIORITY_RX = re.compile(r"^\s*cancel\s+priority\s+(@?\S+)\s*$", re.IGNORECASE)
ROUND_COUNT_RX = re.compile(r"^\s*round\s+count\s*$", re.IGNORECASE)
USER_PERFORMANCE_RX = re.compile(r"^\s*user\s+performance(?:\s+(today|yesterday|\d{4}-\d{2}-\d{2}))?\s*$", re.IGNORECASE)
USER_STATS_RX = re.compile(r"^\s*user\s+stats(?:\s+(today|yesterday|\d{4}-\d{2}-\d{2}))?\s*$", re.IGNORECASE)
INVENTORY_RX = re.compile(r"^\s*inventory\s*$", re.IGNORECASE)
SET_FORWARD_GROUP_RX = re.compile(r"^\s*set\s+forward\s+group\s+@?(\S+?)\s+(-?\d+)\s*$", re.IGNORECASE)
def _looks_like_phone(s: str) -> bool: return bool(PHONE_LIKE_RX.fullmatch((s or "").strip()))
def _parse_stop_open_target(raw: str) -> Tuple[str, str]:
    s = (raw or "").strip().lower()
    if s.startswith("@"): return ("username", s)
    if _looks_like_phone(s): return ("phone", s)
    for p in ("username ", "user ", "handle "):
        if s.startswith(p): return ("username", "@" + s[len(p):].strip().lstrip("@"))
    for p in ("whatsapp ", "wa ", "phone ", "number ", "num "):
        if s.startswith(p): return ("phone", s[len(p):].strip())
    return ("owner", s)
async def _log_event(kind: str, action: str, update: Update, value: str, owner: str = ""):
    try:
        u, m, c = update.effective_user, update.effective_message, update.effective_chat
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO audit_log (ts_local, chat_id, message_id, user_id, user_first, user_username, kind, action, value, owner) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", datetime.now(TIMEZONE), c.id if c else None, m.message_id if m else None, u.id if u else None, u.first_name if u else None, u.username if u else None, kind, action, value, owner or "")
    except: pass
def cache_user_info(user): state.setdefault("user_names", {})[str(user.id)] = {"first_name": user.first_name or "", "username": user.username or ""}
def mention_user_html(user_id: int) -> str:
    info = state.setdefault("user_names", {}).get(str(user_id), {})
    return f'<a href="tg://user?id={user_id}">{info.get("first_name") or info.get("username") or str(user_id)}</a>'
def _issued_bucket(kind: str) -> Dict[str, list]: return state.setdefault("issued", {}).setdefault(kind, {})
async def _set_issued(user_id: int, chat_id: int, kind: str, value: str, context_data: Optional[Dict] = None):
    b = _issued_bucket(kind); uid = str(user_id)
    if uid not in b: b[uid] = []
    data = {"value": value, "ts": datetime.now(TIMEZONE).isoformat(), "chat_id": chat_id}
    if context_data: data.update(context_data)
    b[uid].append(data)
    await save_state()
async def _clear_issued(user_id: int, kind: str, value_to_clear: str) -> bool:
    b = _issued_bucket(kind); uid = str(user_id)
    if uid in b:
        orig = len(b[uid])
        b[uid] = [i for i in b[uid] if i.get("value") != value_to_clear]
        if not b[uid]: del b[uid]
        if len(b.get(uid, [])) < orig: await save_state(); return True
    return False
async def _clear_one_issued(user_id: int, kind: str, value_to_clear: str) -> bool:
    b = _issued_bucket(kind); uid = str(user_id)
    if uid in b:
        for i in b[uid]:
            if i.get("value") == value_to_clear:
                b[uid].remove(i)
                if not b[uid]: del b[uid]
                await save_state(); return True
    return False
def _value_in_text(value: Optional[str], text: str) -> bool:
    if not value: return False
    if value.strip().startswith('@'): return bool(re.search(r'(?<!\S)' + re.escape(value.strip()) + r'(?!\S)', text or ""))
    return bool(re.sub(r'\D', '', value.strip()) and re.sub(r'\D', '', value.strip()) in re.sub(r'\D', '', text or ""))
def _find_closest_app_id(typed_id: str) -> Optional[str]:
    def levenshtein(s1, s2):
        if len(s1) < len(s2): return levenshtein(s2, s1)
        if len(s2) == 0: return len(s1)
        prev = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            curr = [i + 1]
            for j, c2 in enumerate(s2): curr.append(min(prev[j + 1] + 1, curr[j] + 1, prev[j] + (c1 != c2)))
            prev = curr
        return prev[-1]
    all_ids = [i.get("value") for items in _issued_bucket("app_id").values() for i in items if i.get("value")]
    if not all_ids: return None
    norm, closest, min_dist = _normalize_app_id(typed_id), None, 3
    for pid in all_ids:
        dist = levenshtein(norm, _normalize_app_id(pid))
        if dist < min_dist: min_dist = dist; closest = pid
    return closest
def _find_age_in_text(text: str) -> Optional[int]:
    m = re.search(r'\b(?:age|old)\s*:?\s*(\d{1,2})\b|\b(\d{1,2})\s*(?:yrs|yr|years|year old)\b', text.lower())
    return int(m.group(1) or m.group(2)) if m else None
def _find_country_in_text(text: str) -> Tuple[Optional[str], Optional[str]]:
    m = re.search(r'\b(?:from|country)\s*:?\s*(.*)', text, re.IGNORECASE)
    if not m: return None, None
    line = m.group(1).split('\n')[0].strip().lower()
    for c in ALLOWED_COUNTRIES:
        if re.search(r'\b' + re.escape(c) + r'\b', line): return c, 'india' if c in ['indian', 'india'] else c
    return line.split(',')[0].strip(), 'not_allowed'
async def _get_user_country_counts(user_id: int) -> List[Tuple[str, int]]:
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT country, count FROM user_daily_country_counts WHERE day=$1 AND user_id=$2 ORDER BY country", _logical_day_today(), user_id)
            return [(r['country'], r['count']) for r in rows]
    except: return []
async def _get_user_confirmation_count(user_id: int) -> int:
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            c = await conn.fetchval("SELECT confirm_count FROM user_daily_confirmations WHERE day=$1 AND user_id=$2", _logical_day_today(), user_id)
            return int(c) if c else 0
    except: return 0
async def _get_owner_performance(owner_name: str, day: date) -> Tuple[int, int]:
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            r = await conn.fetchrow("SELECT telegram_count, whatsapp_count FROM owner_daily_performance WHERE day=$1 AND owner_name=$2", day, owner_name)
            return (r['telegram_count'], r['whatsapp_count']) if r else (0, 0)
    except: return (0, 0)
async def _get_owner_distribution_counts(owner_name: str, day: date) -> Tuple[int, int]:
    start = TIMEZONE.localize(datetime.combine(day, time(3, 30))); end = start + timedelta(days=1); tg, wa = 0, 0
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT kind, COUNT(*) as count FROM audit_log WHERE owner = $1 AND action = 'issued' AND ts_local >= $2 AND ts_local < $3 GROUP BY kind", owner_name, start, end)
            for r in rows:
                if r['kind'] == 'username': tg = r['count']
                elif r['kind'] == 'whatsapp': wa = r['count']
            return (tg, wa)
    except: return (0, 0)
async def _get_user_performance_text(day: date) -> str:
    lines = [f"<b>🏆 User Performance for {day.isoformat()}</b>"]; perfs = []
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id, confirm_count FROM user_daily_confirmations WHERE day = $1 AND confirm_count > 0 ORDER BY confirm_count DESC", day)
            for r in rows:
                i = state.get("user_names", {}).get(str(r['user_id']), {})
                d = f"@{i['username']}" if i.get('username') else i.get('first_name') or f"ID: {r['user_id']}"
                perfs.append({'name': d, 'count': r['confirm_count']})
    except: return "Error fetching data."
    if not perfs: return f"No data for {day.isoformat()}."
    for i, p in enumerate(perfs, 1): lines.append(f"<b>{i}.</b> {p['name']}: {p['count']} customers")
    return "\n".join(lines)
async def _get_user_stats_text(day: date) -> str:
    lines = [f"<b>📊 User Success Rate for {day.isoformat()}</b>"]; stats = []
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            act = {r['user_id']: {'u': r['username_requests'], 'w': r['whatsapp_requests']} for r in await conn.fetch("SELECT user_id, username_requests, whatsapp_requests FROM user_daily_activity WHERE day = $1", day)}
            conf = {r['user_id']: r['confirm_count'] for r in await conn.fetch("SELECT user_id, confirm_count FROM user_daily_confirmations WHERE day = $1", day)}
            for uid in set(act.keys()) | set(conf.keys()):
                a, c = act.get(uid, {'u': 0, 'w': 0}), conf.get(uid, 0)
                tot = a['u'] + a['w']
                info = state.get("user_names", {}).get(str(uid), {})
                disp = f"@{info['username']}" if info.get('username') else info.get('first_name') or f"ID: {uid}"
                stats.append({'name': disp, 'rate': (c/tot)*100 if tot else 0, 'confirm': c, 'reqs': tot})
    except: return "Error."
    stats.sort(key=lambda x: x['rate'], reverse=True)
    for i, s in enumerate(stats, 1): lines.append(f"<b>{i}.</b> {s['name']}: <b>{s['rate']:.1f}%</b> ({s['confirm']} / {s['reqs']})")
    return "\n".join(lines)
async def _get_user_detail_text(user_id: int) -> str:
    i = state.get("user_names", {}).get(str(user_id), {})
    disp = f"@{i['username']}" if i.get('username') else i.get('first_name') or f"ID: {user_id}"
    u_req, w_req = await _get_user_activity(user_id)
    cnt_counts = await _get_user_country_counts(user_id)
    conf = await _get_user_confirmation_count(user_id)
    p_u = [i['value'] for i in _issued_bucket("username").get(str(user_id), [])]
    p_w = [i['value'] for i in _issued_bucket("whatsapp").get(str(user_id), [])]
    lines = [f"<b>📊 Daily Detail for {disp}</b>", f"<b>- Usernames:</b> {u_req}", f"<b>- WhatsApps:</b> {w_req}", f"<b>- Customers:</b> {conf}"]
    if cnt_counts:
        lines.append("\n<b>🌍 Countries:</b>")
        for c, n in cnt_counts: lines.append(f"  - {c.title()}: {n}")
    lines.append("\n<b>⏳ Pending Usernames:</b>")
    if p_u:
        for u in p_u: lines.append(f"  - <code>{u}</code>")
    else: lines.append("  None")
    lines.append("<b>⏳ Pending WhatsApps:</b>")
    if p_w:
        for w in p_w: lines.append(f"  - <code>{w}</code>")
    else: lines.append("  None")
    return "\n".join(lines)
async def _get_owner_performance_text(owner_name: str, day: date) -> str:
    tg, wa = await _get_owner_performance(owner_name, day)
    tg_d, wa_d = await _get_owner_distribution_counts(owner_name, day)
    lines = [f"<b>📊 Performance @{owner_name} on {day.isoformat()}</b>", f"<b>- Telegram Customers:</b> {tg}", f"<b>- WhatsApp Customers:</b> {wa}", f"<b>- Total:</b> {tg+wa}", "", "<b>🤖 Bot Distribution</b>", f"<b>- Usernames Sent:</b> {tg_d}", f"<b>- WhatsApps Sent:</b> {wa_d}", "", "<b>📋 Inventory</b>"]
    g = _find_owner_group(owner_name)
    if g:
        lines.append(f"<b>- Total Telegram:</b> {len(g.get('entries',[]))}")
        lines.append(f"<b>- Total WhatsApp:</b> {len(g.get('whatsapp',[]))}")
        s_t = [e['telegram'] for e in g.get('entries',[]) if e['disabled']]
        s_w = [e['number'] for e in g.get('whatsapp',[]) if e['disabled']]
        if s_t: lines.append("\n<b>⛔ Stopped Telegram:</b>"); lines.extend([f"  - <code>{x}</code>" for x in s_t])
        if s_w: lines.append("\n<b>⛔ Stopped WhatsApp:</b>"); lines.extend([f"  - <code>{x}</code>" for x in s_w])
    return "\n".join(lines)
async def _get_daily_data_summary_text() -> str:
    d = _logical_day_today(); lines = [f"<b>📊 Daily Summary {d.isoformat()}</b>"]; perfs = []
    for o in OWNER_DATA:
        n = _norm_owner_name(o['owner'])
        tg, wa = await _get_owner_performance(n, d)
        if tg+wa > 0: perfs.append({'name': n, 'total': tg+wa})
    if not perfs: return "No customers added today."
    perfs.sort(key=lambda x: x['total'], reverse=True)
    for p in perfs: lines.append(f"- @{p['name']}: {p['total']}")
    return "\n".join(lines)
def _parse_report_day(arg: Optional[str]) -> date:
    if not arg or arg.lower() == "today": return _logical_day_today()
    if arg.lower() == "yesterday": return _logical_day_today() - timedelta(days=1)
    try: return datetime.strptime(arg, "%Y-%m-%d").date()
    except: return _logical_day_today()
def _parse_duration(s: str) -> Optional[timedelta]:
    m = re.match(r"(\d+)\s*(m|h|d|w)", (s or "").strip().lower())
    return {"m": timedelta(minutes=int(m.group(1))), "h": timedelta(hours=int(m.group(1))), "d": timedelta(days=int(m.group(1))), "w": timedelta(weeks=int(m.group(1)))}[m.group(2)] if m else None
def _find_owner_group(name: str) -> Optional[dict]:
    n = _norm_owner_name(name)
    for g in OWNER_DATA:
        if _norm_owner_name(g["owner"]) == n: return g
    return None
def _find_user_id_by_name(name: str) -> Optional[int]:
    n = name.lower().lstrip('@').strip()
    for uid, d in state.get("user_names", {}).items():
        if d.get("username", "").lower() == n or d.get("first_name", "").lower() == n: return int(uid)
    return None
def _get_commands_text() -> str: return "Commands list is too long, please check source."
def _get_inventory_text() -> str:
    u, au, w, aw = 0, 0, 0, 0
    for g in OWNER_DATA:
        for e in g.get("entries", []):
            u += 1
            if not e.get("disabled"): au += 1
        for e in g.get("whatsapp", []):
            w += 1
            if not e.get("disabled"): aw += 1
    return f"<b>📋 Inventory</b>\n<b>- Usernames:</b> {au}/{u}\n<b>- WhatsApp:</b> {aw}/{w}"
async def _handle_admin_command(text: str, context: ContextTypes.DEFAULT_TYPE, update: Update) -> Optional[str]:
    user = update.effective_user
    if _is_super_admin(user):
        m = SET_FORWARD_GROUP_RX.match(text)
        if m:
            on, gid = m.groups(); o = _find_owner_group(on)
            if not o: return "Owner not found."
            o["forward_group_id"] = int(gid)
            await save_owner_directory(); await load_owner_directory()
            return f"Forward group for {on} set to {gid}."
        m = ADD_ADMIN_RX.match(text)
        if m:
            pool = await get_db_pool()
            async with pool.acquire() as conn: await conn.execute("INSERT INTO admins (username, permissions) VALUES ($1, '[]') ON CONFLICT(username) DO NOTHING", _norm_owner_name(m.group(1)))
            await load_admins(); return f"Admin added."
        m = ADD_USER_RX.match(text)
        if m:
            uid = _find_user_id_by_name(m.group(1))
            if not uid: return "User not found."
            pool = await get_db_pool()
            async with pool.acquire() as conn: await conn.execute("INSERT INTO whitelisted_users (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", uid)
            await load_whitelisted_users(); return "User whitelisted."
        m = DELETE_USER_RX.match(text)
        if m:
            uid = _find_user_id_by_name(m.group(1))
            if not uid: return "User not found."
            pool = await get_db_pool()
            async with pool.acquire() as conn: await conn.execute("DELETE FROM whitelisted_users WHERE user_id = $1", uid)
            await load_whitelisted_users(); return "User removed from whitelist."
        m = DELETE_ADMIN_RX.match(text)
        if m:
            pool = await get_db_pool()
            async with pool.acquire() as conn: await conn.execute("DELETE FROM admins WHERE username = $1", _norm_owner_name(m.group(1)))
            await load_admins(); return "Admin deleted."
        m = ALLOW_ADMIN_CMD_RX.match(text)
        if m:
            n, c = _norm_owner_name(m.group(1)), m.group(2).lower().strip()
            if c not in COMMAND_PERMISSIONS: return "Invalid command."
            perms = set(ADMIN_PERMISSIONS.get(n, [])); perms.add(c)
            pool = await get_db_pool()
            async with pool.acquire() as conn: await conn.execute("INSERT INTO admins (username, permissions) VALUES ($1, $2) ON CONFLICT(username) DO UPDATE SET permissions = $2", n, json.dumps(list(perms)))
            await load_admins(); return f"Perm {c} granted to {n}."
        m = STOP_ALLOW_ADMIN_CMD_RX.match(text)
        if m:
            n, c = _norm_owner_name(m.group(1)), m.group(2).lower().strip()
            perms = set(ADMIN_PERMISSIONS.get(n, [])); perms.discard(c)
            pool = await get_db_pool()
            async with pool.acquire() as conn: await conn.execute("UPDATE admins SET permissions = $1 WHERE username = $2", json.dumps(list(perms)), n)
            await load_admins(); return f"Perm {c} revoked from {n}."
        if LIST_ADMINS_RX.match(text): return "\n".join([f"- {n}: {p}" for n, p in ADMIN_PERMISSIONS.items()]) or "No admins."
    m = TAKE_CUSTOMER_RX.match(text)
    if m:
        if not _has_permission(user, 'take customer'): return "No permission."
        cnt, own_str, stop = int(m.group(1)), m.group(2), bool(m.group(3))
        pq = state.setdefault("priority_queue", {})
        added = []
        for n in [_norm_owner_name(x) for x in re.split(r'[\s,]+', own_str) if x.strip()]:
            g = _find_owner_group(n)
            if g and not _owner_is_paused(g):
                pq[n] = {"remaining": cnt, "stop_after": stop}; added.append(n)
        await save_state()
        return f"Priority queue: {cnt} customers to {', '.join(added)}."
    m = STOP_OPEN_RX.match(text)
    if m:
        if not _has_permission(user, 'stop open'): return "No permission."
        act, raw = m.group(1).lower() == "stop", m.group(2)
        tgt = raw.lower()
        if tgt in ("all whatsapp", "all username", "all owners"):
            if not _is_super_admin(user): return "Super admin only."
            for o in OWNER_DATA:
                if "whatsapp" in tgt:
                    for w in o.get("whatsapp", []): w["disabled"] = act
                elif "username" in tgt:
                    for e in o.get("entries", []): e["disabled"] = act
                elif "owners" in tgt:
                    o["disabled"] = act; o.pop("disabled_until", None)
            await _rebuild_pools_preserving_rotation()
            return f"{'Stopped' if act else 'Opened'} all."
        kind, val = _parse_stop_open_target(raw)
        if kind == "phone":
            found = None; n = _norm_phone(val)
            for o in OWNER_DATA:
                for w in o.get("whatsapp",[]):
                    if _norm_phone(w.get("number")) == n: found = w; break
                if found: break
            if not found: return "Not found."
            if found.get("managed_by") and found["managed_by"] != _norm_owner_name(user.username) and not _is_super_admin(user): return "Not managed by you."
            found["disabled"] = act
        elif kind == "username":
            found = None; h = _norm_handle(val)
            for o in OWNER_DATA:
                for e in o.get("entries",[]):
                    if _norm_handle(e.get("telegram")) == h: found = e; break
                if found: break
            if not found: return "Not found."
            if found.get("managed_by") and found["managed_by"] != _norm_owner_name(user.username) and not _is_super_admin(user): return "Not managed by you."
            found["disabled"] = act
        elif kind == "owner":
            o = _find_owner_group(val)
            if not o: return "Owner not found."
            if o.get("managed_by") and o["managed_by"] != _norm_owner_name(user.username) and not _is_super_admin(user): return "Not managed by you."
            o["disabled"] = act; o.pop("disabled_until", None)
        await _rebuild_pools_preserving_rotation()
        return f"Done."
    m = ADD_OWNER_RX.match(text)
    if m:
        if not _has_permission(user, 'add owner'): return "No permission."
        n = _norm_owner_name(m.group(1))
        if _find_owner_group(n): return "Exists."
        d = {"owner": n}; 
        if not _is_super_admin(user): d["managed_by"] = _norm_owner_name(user.username)
        OWNER_DATA.append(_ensure_owner_shape(d))
        await _rebuild_pools_preserving_rotation(); return "Added."
    m = DEL_OWNER_RX.match(text)
    if m:
        if not _has_permission(user, 'delete owner'): return "No permission."
        n = _norm_owner_name(m.group(1))
        g = _find_owner_group(n)
        if not g: return "Not found."
        if g.get("managed_by") and g["managed_by"] != _norm_owner_name(user.username) and not _is_super_admin(user): return "Not managed by you."
        OWNER_DATA[:] = [x for x in OWNER_DATA if _norm_owner_name(x.get("owner")) != n]
        await _rebuild_pools_preserving_rotation(); return "Deleted."
    m = ADD_USERNAME_RX.match(text)
    if m:
        if not _has_permission(user, 'add username'): return "No permission."
        h, on = m.groups(); o = _find_owner_group(on)
        if not o: return "Owner not found."
        entry = {"telegram": h, "phone": "", "disabled": False}
        if not _is_super_admin(user): entry["managed_by"] = _norm_owner_name(user.username)
        o["entries"].append(entry)
        await _rebuild_pools_preserving_rotation(); return "Added."
    m = ADD_WHATSAPP_RX.match(text)
    if m:
        if not _has_permission(user, 'add whatsapp'): return "No permission."
        ph, on = m.groups(); o = _find_owner_group(on)
        if not o: return "Owner not found."
        wa = {"number": ph, "disabled": False}
        if not _is_super_admin(user): wa["managed_by"] = _norm_owner_name(user.username)
        o["whatsapp"].append(wa)
        await _rebuild_pools_preserving_rotation(); return "Added."
    m = DEL_USERNAME_RX.match(text)
    if m:
        if not _has_permission(user, 'delete username'): return "No permission."
        h = _norm_handle(m.group(1)); found = False
        for o in OWNER_DATA:
            tbd = [e for e in o["entries"] if _norm_handle(e.get("telegram")) == h]
            for e in tbd:
                if e.get("managed_by") and e["managed_by"] != _norm_owner_name(user.username) and not _is_super_admin(user): return "Not managed by you."
                o["entries"].remove(e); found = True
        if found: await _rebuild_pools_preserving_rotation(); return "Deleted."
        return "Not found."
    m = DEL_WHATSAPP_RX.match(text)
    if m:
        if not _has_permission(user, 'delete whatsapp'): return "No permission."
        n = _norm_phone(m.group(1)); found = False
        for o in OWNER_DATA:
            tbd = [w for w in o["whatsapp"] if _norm_phone(w.get("number")) == n]
            for w in tbd:
                if w.get("managed_by") and w["managed_by"] != _norm_owner_name(user.username) and not _is_super_admin(user): return "Not managed by you."
                o["whatsapp"].remove(w); found = True
        if found: await _rebuild_pools_preserving_rotation(); return "Deleted."
        return "Not found."
    if LIST_DISABLED_RX.match(text): return "\n".join([o['owner'] for o in OWNER_DATA if _owner_is_paused(o)]) or "None."
    if LIST_ENABLED_RX.match(text): return "\n".join([f"{o['owner']} ({len(o.get('entries',[]))}/{len(o.get('whatsapp',[]))})" for o in OWNER_DATA if not _owner_is_paused(o)]) or "None."
    if LIST_PRIORITY_RX.match(text): return "\n".join([f"@{k}: {v['remaining']}" for k,v in state.get("priority_queue",{}).items()]) or "Empty."
    if CANCEL_PRIORITY_RX.match(text):
        if not _has_permission(user, 'cancel priority'): return "No permission."
        t = m.group(1).strip().lower()
        if t == "all": state["priority_queue"] = {}
        elif _norm_owner_name(t) in state.get("priority_queue",{}): del state["priority_queue"][_norm_owner_name(t)]
        else: return "Not found."
        await save_state(); return "Cancelled."
    if ROUND_COUNT_RX.match(text): return f"Username Rounds: {state.get('username_round_count',0)}\nWhatsApp Rounds: {state.get('whatsapp_round_count',0)}"
    if LIST_OWNER_DETAIL_RX.match(text) or LIST_OWNER_ALIAS_RX.match(text):
        n = m.group(1); o = _find_owner_group(n)
        if not o: return "Not found."
        return f"Owner: {o['owner']}\nUsernames: {len(o.get('entries',[]))}\nWA: {len(o.get('whatsapp',[]))}"
    if REMIND_ALL_RX.match(text):
        if not _has_permission(user, 'remind user'): return "No permission."
        return await _send_all_pending_reminders(context)
    if CLEAR_PENDING_RX.match(text):
        if not _is_super_admin(user): return "Super admin only."
        val = m.group(1).strip()
        for kind in ("username", "whatsapp", "app_id"):
            for uid, items in _issued_bucket(kind).items():
                for i in items:
                    if (kind=="whatsapp" and _norm_phone(i.get("value"))==_norm_phone(val)) or (kind!="whatsapp" and i.get("value")==val):
                        if await _clear_issued(int(uid), kind, i.get("value")):
                            if kind == "whatsapp" and uid in state.get("whatsapp_temp_bans", {}): del state["whatsapp_temp_bans"][uid]; await save_state()
                            return f"Cleared {val} for {uid}."
        return "Not found."
    if BAN_WHATSAPP_RX.match(text):
        if not _has_permission(user, 'ban whatsapp'): return "No permission."
        uid = _find_user_id_by_name(m.group(1))
        if not uid: return "User not found."
        pool = await get_db_pool()
        async with pool.acquire() as conn: await conn.execute("INSERT INTO whatsapp_bans (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", uid)
        WHATSAPP_BANNED_USERS.add(uid); return "Banned."
    if UNBAN_WHATSAPP_RX.match(text):
        if not _has_permission(user, 'unban whatsapp'): return "No permission."
        uid = _find_user_id_by_name(m.group(1))
        if not uid: return "User not found."
        pool = await get_db_pool()
        async with pool.acquire() as conn: await conn.execute("DELETE FROM whatsapp_bans WHERE user_id = $1", uid)
        WHATSAPP_BANNED_USERS.discard(uid); return "Unbanned."
    if LIST_BANNED_RX.match(text): return "\n".join(map(str, WHATSAPP_BANNED_USERS)) or "None."
    if BAN_COUNTRY_RX.match(text):
        if not _has_permission(user, 'ban country'): return "No permission."
        c, n = m.groups(); uid = _find_user_id_by_name(n)
        if not uid: return "User not found."
        pool = await get_db_pool()
        async with pool.acquire() as conn: await conn.execute("INSERT INTO user_country_bans (user_id, country) VALUES ($1, $2) ON CONFLICT (user_id, country) DO NOTHING", uid, c.lower())
        await load_user_country_bans(); return "Banned."
    if UNBAN_COUNTRY_RX.match(text):
        if not _has_permission(user, 'unban country'): return "No permission."
        c, n = m.groups(); uid = _find_user_id_by_name(n)
        if not uid: return "User not found."
        pool = await get_db_pool()
        async with pool.acquire() as conn: await conn.execute("DELETE FROM user_country_bans WHERE user_id = $1 AND country = $2", uid, c.lower())
        await load_user_country_bans(); return "Unbanned."
    if LIST_COUNTRY_BANS_RX.match(text): return "Check logs."
    if USER_PERFORMANCE_RX.match(text): return await _get_user_performance_text(_parse_report_day(m.group(1)))
    if USER_STATS_RX.match(text): return await _get_user_stats_text(_parse_report_day(m.group(1)))
    if INVENTORY_RX.match(text): return _get_inventory_text()
    if COMMANDS_RX.match(text): return _get_commands_text()
    if DATA_TODAY_RX.match(text): return await _get_daily_data_summary_text()
    if DETAIL_USER_RX.match(text):
        uid = _find_user_id_by_name(m.group(1))
        return await _get_user_detail_text(uid) if uid else "User not found."
    return None
async def _send_all_pending_reminders(context: ContextTypes.DEFAULT_TYPE) -> str:
    cnt = 0
    for k in ("username", "whatsapp"):
        for uid, items in _issued_bucket(k).items():
            for i in items:
                try:
                    await context.bot.send_message(chat_id=i['chat_id'], text=f"Reminder: Provide info for {i['value']}", parse_mode=ParseMode.HTML)
                    cnt += 1
                except: pass
    return f"Sent {cnt} reminders."
async def _clear_expired_app_ids(context: ContextTypes.DEFAULT_TYPE):
    log.info("Clearing expired IDs...")
    now, chg = datetime.now(TIMEZONE), False
    for uid, items in list(_issued_bucket("app_id").items()):
        keep = []
        for i in items:
            try:
                if (now - datetime.fromisoformat(i["ts"])) <= timedelta(hours=48): keep.append(i)
                else: chg = True
            except: keep.append(i)
        if keep: _issued_bucket("app_id")[uid] = keep
        else: del _issued_bucket("app_id")[uid]
    if chg: await save_state()
async def check_reminders(context: ContextTypes.DEFAULT_TYPE):
    async with db_lock:
        now, chg = datetime.now(TIMEZONE), False
        pool = await get_db_pool()
        for uid_str, items in _issued_bucket("username").items():
            for i in items:
                ts = datetime.fromisoformat(i.get("last_reminder_ts", i["ts"]))
                if (now - ts) > timedelta(minutes=REMINDER_DELAY_MINUTES):
                    try:
                        await context.bot.send_message(chat_id=i['chat_id'], text=f"Reminder: username {i['value']}", parse_mode=ParseMode.HTML)
                        i["last_reminder_ts"] = now.isoformat(); chg = True
                    except: pass
        for uid_str, items in _issued_bucket("whatsapp").items():
            uid = int(uid_str)
            for i in items:
                ts = datetime.fromisoformat(i["ts"])
                if (now - ts) > timedelta(minutes=REMINDER_DELAY_MINUTES) and not i.get("punished"):
                    cnt = state.setdefault("whatsapp_offense_count", {}).get(uid_str, 0) + 1
                    state["whatsapp_offense_count"][uid_str] = cnt
                    dur = 30 if cnt == 1 else 120 if cnt == 2 else 0
                    msg = ""
                    if dur:
                        state.setdefault("whatsapp_temp_bans", {})[uid_str] = (now + timedelta(minutes=dur)).isoformat()
                        msg = f"Banned for {dur} mins."
                    else:
                        WHATSAPP_BANNED_USERS.add(uid)
                        async with pool.acquire() as conn: await conn.execute("INSERT INTO whatsapp_bans (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", uid)
                        msg = "Permanently banned."
                    try: await context.bot.send_message(chat_id=i['chat_id'], text=msg, parse_mode=ParseMode.HTML)
                    except: pass
                    i["punished"] = True; chg = True
        if chg: await save_state()
async def reset_45min_wa_counter(context: ContextTypes.DEFAULT_TYPE):
    async with db_lock: state['wa_45min_counter'] = 0; await save_state()
async def daily_reset(context: ContextTypes.DEFAULT_TYPE):
    async with db_lock:
        state['whatsapp_offense_count'] = {}; state['username_round_count'] = 0; state['whatsapp_round_count'] = 0
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            for t in ("wa_daily_usage", "user_daily_activity", "user_daily_country_counts", "user_daily_confirmations", "owner_daily_performance"):
                await conn.execute(f"DELETE FROM {t};")
        await save_state()
async def _on_owner_change(c, p, ch, pay): async with db_lock: await load_owner_directory()
async def _listen_for_owner_changes(app: Application):
    while True:
        try:
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                await conn.add_listener('owners_changed', _on_owner_change)
                while True: await asyncio.sleep(3600)
        except: await asyncio.sleep(10)
async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or not update.effective_user or update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP): return
    msg, text, uid, chat_id = update.effective_message, (update.effective_message.text or update.effective_message.caption or "").strip(), update.effective_user.id, update.effective_message.chat_id
    cache_user_info(update.effective_user)
    async with db_lock:
        if MY_DETAIL_RX.match(text) and chat_id == DETAIL_GROUP_ID:
            if uid in WHITELISTED_USERS or _is_admin(update.effective_user): await msg.reply_html(await _get_user_detail_text(uid))
            return
        if MY_PERFORMANCE_RX.match(text) and chat_id in PERFORMANCE_GROUP_IDS and _is_owner(update.effective_user):
            await msg.reply_html(await _get_owner_performance_text(_norm_owner_name(update.effective_user.username), _parse_report_day(MY_PERFORMANCE_RX.match(text).group(1))))
            return
        if PERFORMANCE_OWNER_RX.match(text) and _is_admin(update.effective_user):
            m = PERFORMANCE_OWNER_RX.match(text)
            await msg.reply_html(await _get_owner_performance_text(_norm_owner_name(m.group(1)), _parse_report_day(m.group(2))))
            return
        if _is_admin(update.effective_user):
            resp = await _handle_admin_command(text, context, update)
            if resp: await msg.reply_html(resp); return
        if chat_id == CONFIRMATION_GROUP_ID and '+1' in text:
            m = re.search(r'@([^\s]+)', text)
            if m:
                aid_raw = f"@{m.group(1)}"; done = False
                for uid_str, items in list(_issued_bucket("app_id").items()):
                    if done: break
                    for i in items:
                        if _normalize_app_id(i.get("value")) == _normalize_app_id(aid_raw):
                            await _increment_user_confirmation_count(int(uid_str))
                            await _increment_owner_performance(_norm_owner_name(update.effective_user.username), i.get("source_kind"))
                            await _clear_one_issued(int(uid_str), "app_id", i.get("value"))
                            done = True; break
                if not done:
                    sugg = _find_closest_app_id(aid_raw)
                    await msg.reply_html(f"Did you mean <code>{sugg}</code>?" if sugg else "Wrong ID.")
            return
        elif chat_id == CLEARING_GROUP_ID:
            p_u = {i['value'] for i in _issued_bucket("username").get(str(uid), [])}
            p_w = {i['value'] for i in _issued_bucket("whatsapp").get(str(uid), [])}
            f_u = {f"@{u}" for u in EXTRACT_USERNAMES_RX.findall(text)}
            f_p = EXTRACT_PHONES_RX.findall(text)
            vals = {u for u in f_u if u in p_u} | {p for p in f_p for pp in p_w if _norm_phone(p) == _norm_phone(pp)}
            country, c_stat = _find_country_in_text(text)
            aid_match = APP_ID_RX.search(text)
            if not vals and not aid_match and not country: return
            src_item, src_kind, src_owner = None, None, None
            if vals:
                v = next(iter(vals)); k = "whatsapp" if _looks_like_phone(v) else "username"
                for i in _issued_bucket(k).get(str(uid), []):
                    if i.get("value") == v: src_item, src_kind, src_owner = i, k, i.get("owner"); break
            elif aid_match:
                last_ts = datetime.min.replace(tzinfo=TIMEZONE)
                for k in ("username", "whatsapp"):
                    for i in _issued_bucket(k).get(str(uid), []):
                        try:
                            if datetime.fromisoformat(i["ts"]) > last_ts: last_ts = datetime.fromisoformat(i["ts"]); src_item, src_kind, src_owner = i, k, i.get("owner")
                        except: pass
            for u in f_u:
                if _find_owner_group(u): src_owner = _find_owner_group(u)['owner']; break
            fwd_grp = FORWARD_GROUP_ID
            if src_owner:
                og = _find_owner_group(src_owner)
                if og and og.get("forward_group_id") is not None: fwd_grp = og["forward_group_id"]
            allowed, reason = True, ""
            if c_stat == 'not_allowed': allowed, reason = False, f"Country '{country}' not allowed."
            elif c_stat:
                if c_stat in USER_COUNTRY_BANS.get(uid, set()): allowed, reason = False, "Manually banned."
                elif await _get_user_country_count(uid, c_stat) >= COUNTRY_DAILY_LIMIT: allowed, reason = False, "Daily limit reached."
                elif c_stat == 'india' and (_find_age_in_text(text) or 0) < 30: allowed, reason = False, "Age < 30."
            if not allowed:
                await msg.reply_html(f"{mention_user_html(uid)}, rejected: {reason}")
                return
            if c_stat: await _increment_user_country_count(uid, c_stat)
            if fwd_grp and fwd_grp != 0:
                try: await context.bot.forward_message(chat_id=fwd_grp, from_chat_id=chat_id, message_id=msg.message_id)
                except: pass
            if aid_match:
                aid = f"@{aid_match.group(2)}"
                if src_item:
                    await _set_issued(uid, chat_id, "app_id", aid, {"source_owner": src_owner, "source_kind": src_kind})
                    await _clear_one_issued(uid, src_kind, src_item.get("value"))
                else: await _set_issued(uid, chat_id, "app_id", aid, {"source_owner": "unknown", "source_kind": "app_id"})
            return
        elif chat_id == REQUEST_GROUP_ID:
            if uid not in WHITELISTED_USERS and not _is_admin(update.effective_user): return
            if NEED_USERNAME_RX.match(text):
                now = datetime.now(TIMEZONE)
                l = datetime.fromisoformat(state.setdefault("username_last_request_ts", {}).get(str(uid), "2000-01-01T00:00:00+07:00"))
                if (now - l) < timedelta(minutes=1): await msg.reply_text("Wait 1 min."); return
                rec = await _next_from_username_pool()
                await msg.reply_text(f"@{rec['owner']}\n{rec['username']}" if rec else "No username.")
                if rec:
                    state["username_last_request_ts"][str(uid)] = now.isoformat()
                    await _set_issued(uid, chat_id, "username", rec["username"], {"owner": rec["owner"]})
                    await _increment_user_activity(uid, "username")
                return
            if NEED_WHATSAPP_RX.match(text):
                if uid in WHATSAPP_BANNED_USERS: await msg.reply_text("Banned."); return
                tb = state.get("whatsapp_temp_bans", {}).get(str(uid))
                if tb:
                    if datetime.now(TIMEZONE) < datetime.fromisoformat(tb): await msg.reply_text("Temp banned."); return
                    del state["whatsapp_temp_bans"][str(uid)]; await save_state()
                if state.setdefault("wa_45min_counter", 0) >= 10: await msg.reply_text("Limit reached."); return
                now = datetime.now(TIMEZONE)
                l = datetime.fromisoformat(state.setdefault("whatsapp_last_request_ts", {}).get(str(uid), "2000-01-01T00:00:00+07:00"))
                if (now - l) < timedelta(minutes=3): await msg.reply_text("Wait 3 mins."); return
                u_cnt, w_cnt = await _get_user_activity(uid)
                if u_cnt <= USERNAME_THRESHOLD_FOR_BONUS and w_cnt >= USER_WHATSAPP_LIMIT: await msg.reply_text(f"Limit reached (need >{USERNAME_THRESHOLD_FOR_BONUS} usernames)."); return
                rec = await _next_from_whatsapp_pool()
                if rec and await _wa_quota_reached(rec["number"]): rec = None
                await msg.reply_text(f"@{rec['owner']}\n{rec['number']}" if rec else "No WhatsApp.")
                if rec:
                    await _wa_inc_count(_norm_phone(rec["number"]), _logical_day_today())
                    state["wa_45min_counter"] += 1; state["whatsapp_last_request_ts"][str(uid)] = now.isoformat()
                    await _set_issued(uid, chat_id, "whatsapp", rec["number"], {"owner": rec["owner"]})
                    await _increment_user_activity(uid, "whatsapp")
                return
            m = WHO_USING_REGEX.match(text)
            if m:
                h, p = m.groups()
                if h:
                    hits = HANDLE_INDEX.get(_norm_handle(h), [])
                    await msg.reply_text(f"Owner: {', '.join(sorted({x['owner'] for x in hits}))}" if hits else "Not found.")
                else:
                    rec = PHONE_INDEX.get(_norm_phone(p))
                    await msg.reply_text(f"Owner: @{rec['owner']}" if rec else "Not found.")
                return
async def post_init(app: Application):
    await get_db_pool(); await setup_database(); await load_state(); await _migrate_state_if_needed(); await load_owner_directory(); await load_whatsapp_bans(); await load_user_country_bans(); await load_admins(); await load_whitelisted_users(); asyncio.create_task(_listen_for_owner_changes(app))
async def post_down(app: Application): await close_db_pool()
if __name__ == "__main__":
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).post_shutdown(post_down).build()
    if app.job_queue:
        app.job_queue.run_repeating(check_reminders, interval=60, first=60)
        app.job_queue.run_repeating(_clear_expired_app_ids, interval=3600, first=3600)
        app.job_queue.run_daily(daily_reset, time=time(hour=5, minute=31, tzinfo=TIMEZONE))
        app.job_queue.run_repeating(reset_45min_wa_counter, interval=2700, first=2700)
    app.add_handler(MessageHandler(filters.ALL & ~filters.StatusUpdate.ALL, on_message))
    log.info("Bot starting...")
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)
