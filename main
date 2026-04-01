"""
Twitch Bot v3.0  -  Production Ready
FastAPI + SQLite + Twitch IRC
"""
import asyncio, json, random, re, sqlite3, time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional
import uvicorn
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

DB_PATH = Path("bot_data.db")

# ─── Database ──────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=15, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    conn = get_db()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS points (
                username      TEXT PRIMARY KEY,
                points        INTEGER DEFAULT 0,
                watch_minutes INTEGER DEFAULT 0,
                last_seen     REAL    DEFAULT 0,
                is_vip        INTEGER DEFAULT 0,
                vip_until     REAL    DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY, value TEXT
            );
            CREATE TABLE IF NOT EXISTS chat_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL, username TEXT, message TEXT, deleted INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS clips (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL, label TEXT, duration INTEGER
            );
            CREATE TABLE IF NOT EXISTS warn_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT, reason TEXT, ts REAL
            );
            CREATE TABLE IF NOT EXISTS cooldowns (
                username TEXT, cmd TEXT, last_use REAL DEFAULT 0,
                PRIMARY KEY (username, cmd)
            );
            CREATE TABLE IF NOT EXISTS custom_commands (
                cmd TEXT PRIMARY KEY, response TEXT, enabled INTEGER DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS song_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL, username TEXT, song TEXT, done INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_cd          ON cooldowns(username, cmd);
            CREATE INDEX IF NOT EXISTS idx_warn        ON warn_log(username);
            CREATE INDEX IF NOT EXISTS idx_chat_ts     ON chat_log(ts);
            CREATE INDEX IF NOT EXISTS idx_songs_done  ON song_queue(done, ts);
        """)
        conn.commit()

        # Миграции  -  добавляем новые колонки если их нет (совместимость со старой БД)
        for migration in [
            "ALTER TABLE points ADD COLUMN is_vip INTEGER DEFAULT 0",
            "ALTER TABLE points ADD COLUMN vip_until REAL DEFAULT 0",
            "ALTER TABLE points ADD COLUMN watch_minutes INTEGER DEFAULT 0",
            "ALTER TABLE points ADD COLUMN last_seen REAL DEFAULT 0",
            "CREATE INDEX IF NOT EXISTS idx_points_pts ON points(points DESC)",
            "CREATE INDEX IF NOT EXISTS idx_points_vip ON points(is_vip)",
        ]:
            try:
                conn.execute(migration)
                conn.commit()
            except sqlite3.OperationalError:
                pass  # колонка уже есть

        _apply_defaults(conn)
    finally:
        conn.close()

# ─── Economy constants (единый источник правды) ────────────────────────────────
# Активный зритель (5 сообщ/мин, 120 мин):
#   Чат:    5 msg * 8 pts * 120 min = 4800 pts
#   Просмотр: 5 pts * 120 min      =  600 pts
#   Ежедн. бонус (среднее)         =  700 pts
#   Мини-игры (умеренно)           ~ 1200 pts
#   Итого за 2 ч ≈ 7300-8000 pts  → VIP = 8000 ✓

ECONOMY = {
    "points_per_message":       "8",    # за каждое сообщение
    "points_interval_messages": "1",    # каждые N сообщений (1 = каждое)
    "points_interval_seconds":  "0",    # или раз в N сек (0 = отключено)
    "points_per_minute":        "5",    # пассивно за просмотр
    "vip_cost":                 "8000", # стоимость VIP
    "vip_hours":                "24",   # длительность VIP в часах
    "vip_points_bonus":         "50",   # % бонус к очкам для VIP
    "vip_cd_multiplier":        "2",    # кулдауны у VIP в N раз короче
    "hydrate_cost":             "150",  # стоимость !hydrate
    "daily_min":                "400",  # мин ежедневный бонус
    "daily_max":                "1000", # макс ежедневный бонус
}

COOLDOWNS = {
    "gamble":   30,   # сек
    "slots":    30,
    "duel":     60,
    "roulette": 60,
    "heist":    120,
    "hydrate":  300,  # 5 мин
    "give":     300,
    "daily":    86400,
    "vip":      86400,
}

def _apply_defaults(conn):
    defaults = {
        **ECONOMY,
        "twitch_channel": "", "twitch_token": "", "bot_username": "",
        "tg_link": "", "tiktok_link": "", "discord_link": "", "youtube_link": "",
        "autopost_interval": "15", "clip_duration": "10",
        "autopost_links": "[]",
        "feat_points": "1", "feat_minigames": "1", "feat_moderation": "1",
        "feat_clips": "1", "feat_autopost": "1", "feat_commands": "1",
        "feat_trivia": "1", "feat_roulette": "1", "feat_heist": "1",
        "feat_deaths": "1", "feat_goals": "1", "feat_tts": "0",
        "feat_vip": "1", "feat_followage": "1",
        "mod_links": "1", "mod_caps": "1", "mod_spam": "1", "mod_banned_words": "1",
        "caps_threshold": "70", "spam_threshold": "3", "banned_words": "",
        "death_count": "0", "goal_current": "0", "goal_target": "100",
        "goal_label": "Подписчики",
        "feat_events": "1",
        "evt_follow": "1", "evt_sub": "1", "evt_giftsub": "1",
        "evt_raid": "1", "evt_bits": "1", "evt_firstmsg": "1",
        "evt_return": "1", "evt_milestone": "1",
        "evt_stream_hello": "1", "evt_stream_bye": "1",
        "msg_follow":       "Спасибо за фолловер, {name}! Добро пожаловать!",
        "msg_sub":          "{name} оформил подписку! Спасибо за поддержку! +500 баллов",
        "msg_resub":        "{name} продлил подписку  -  {months} мес подряд! Легенда!",
        "msg_giftsub":      "{name} подарил {count} подписок! Щедрость!",
        "msg_raid":         "РЕЙД! {name} ведёт {count} зрителей! Всем привет!",
        "msg_bits":         "{name} задонатил {count} bits! Огромное спасибо!",
        "msg_firstmsg":     "{name} впервые написал в чат  -  добро пожаловать!",
        "msg_return":       "{name} вернулся! Давно не виделись!",
        "msg_stream_hello": "Стрим начался! Всем привет, погнали!",
        "msg_stream_bye":   "Стрим окончен! Всем спасибо, до встречи!",
        "milestones":       "100,500,1000,5000,10000",
        "msg_milestone":    "УРА! Уже {count} фолловеров! Спасибо всем!",
        "return_days": "30", "event_delay": "2", "follower_count": "0",
        "current_game": "",
        "trivia_reward": "200",
        "trivia_timeout": "60",
        "trivia_questions": "[]",
    }
    for k, v in defaults.items():
        conn.execute("INSERT OR IGNORE INTO settings (key,value) VALUES (?,?)", (k, v))
    conn.commit()

# ─── Settings helpers ──────────────────────────────────────────────────────────

def get_setting(key: str, default: str = "") -> str:
    conn = get_db()
    try:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default
    finally:
        conn.close()

def set_setting(key: str, value: str):
    conn = get_db()
    try:
        conn.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", (key, value))
        conn.commit()
    finally:
        conn.close()

# ─── Points ────────────────────────────────────────────────────────────────────

def get_points(username: str) -> int:
    conn = get_db()
    try:
        row = conn.execute("SELECT points FROM points WHERE username=?", (username.lower(),)).fetchone()
        return int(row["points"]) if row else 0
    finally:
        conn.close()

def add_points(username: str, amount: int):
    """amount может быть отрицательным (списание). MIN ограничение только в SQL."""
    conn = get_db()
    try:
        conn.execute("""
            INSERT INTO points (username, points) VALUES (?,MAX(0,?))
            ON CONFLICT(username) DO UPDATE SET points=MAX(0, points+excluded.points)
        """, (username.lower(), amount))
        conn.commit()
    finally:
        conn.close()

def get_top_points(n: int = 10) -> list:
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT username, points, is_vip FROM points ORDER BY points DESC LIMIT ?", (n,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

def get_rank(username: str) -> tuple:
    conn = get_db()
    try:
        uname = username.lower()
        total = conn.execute("SELECT COUNT(*) AS c FROM points").fetchone()["c"]
        pts_row = conn.execute("SELECT points FROM points WHERE username=?", (uname,)).fetchone()
        if not pts_row:
            return total + 1, total
        pts = pts_row["points"]
        rank = conn.execute(
            "SELECT COUNT(*) AS c FROM points WHERE points > ?", (pts,)
        ).fetchone()["c"] + 1
        return rank, total
    finally:
        conn.close()

# ─── VIP ───────────────────────────────────────────────────────────────────────

def is_vip(username: str) -> bool:
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT is_vip, vip_until FROM points WHERE username=?", (username.lower(),)
        ).fetchone()
        if not row or not row["is_vip"]:
            return False
        if row["vip_until"] > 0 and time.time() > row["vip_until"]:
            conn.execute("UPDATE points SET is_vip=0, vip_until=0 WHERE username=?", (username.lower(),))
            conn.commit()
            return False
        return True
    finally:
        conn.close()

def grant_vip(username: str, hours: int = 24):
    until = time.time() + max(1, hours) * 3600
    conn = get_db()
    try:
        conn.execute("""
            INSERT INTO points (username, is_vip, vip_until) VALUES (?,1,?)
            ON CONFLICT(username) DO UPDATE SET is_vip=1, vip_until=excluded.vip_until
        """, (username.lower(), until))
        conn.commit()
    finally:
        conn.close()

def revoke_vip(username: str):
    conn = get_db()
    try:
        conn.execute("UPDATE points SET is_vip=0, vip_until=0 WHERE username=?", (username,))
        conn.commit()
    finally:
        conn.close()

def get_all_vips() -> list:
    now = time.time()
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT username, points, vip_until FROM points WHERE is_vip=1 ORDER BY points DESC"
        ).fetchall()
        result = []
        for r in rows:
            if r["vip_until"] > 0 and now > r["vip_until"]:
                conn.execute("UPDATE points SET is_vip=0, vip_until=0 WHERE username=?",
                             (r["username"],))
            else:
                remaining = int(r["vip_until"] - now) if r["vip_until"] > 0 else -1
                result.append({**dict(r), "remaining_seconds": remaining})
        conn.commit()
        return result
    finally:
        conn.close()

# ─── Cooldowns ─────────────────────────────────────────────────────────────────

def check_cooldown(username: str, cmd: str, base_seconds: int) -> int:
    seconds = base_seconds
    if is_vip(username):
        mult = max(1, int(get_setting("vip_cd_multiplier", "2")))
        seconds = max(1, seconds // mult)
    now = time.time()
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT last_use FROM cooldowns WHERE username=? AND cmd=?", (username, cmd)
        ).fetchone()
        last = float(row["last_use"]) if row else 0.0
        remaining = int(seconds - (now - last))
        if remaining > 0:
            return remaining
        conn.execute("""
            INSERT INTO cooldowns (username, cmd, last_use) VALUES (?,?,?)
            ON CONFLICT(username, cmd) DO UPDATE SET last_use=excluded.last_use
        """, (username, cmd, now))
        conn.commit()
        return 0
    finally:
        conn.close()

def fmt_cd(s: int) -> str:
    if s >= 3600:
        return f"{s//3600}ч {(s%3600)//60}м"
    if s >= 60:
        m, sec = s // 60, s % 60
        return f"{m}м {sec}с" if sec else f"{m}м"
    return f"{s} сек"

# ─── Warnings ──────────────────────────────────────────────────────────────────

def add_warning(username: str, reason: str) -> int:
    conn = get_db()
    try:
        conn.execute("INSERT INTO warn_log (username, reason, ts) VALUES (?,?,?)",
                     (username.lower(), reason, time.time()))
        conn.commit()
        return conn.execute(
            "SELECT COUNT(*) AS c FROM warn_log WHERE username=?", (username.lower(),)
        ).fetchone()["c"]
    finally:
        conn.close()

# ─── Last seen ─────────────────────────────────────────────────────────────────

def get_last_seen(username: str) -> Optional[float]:
    conn = get_db()
    try:
        row = conn.execute("SELECT last_seen FROM points WHERE username=?", (username.lower(),)).fetchone()
        v = row["last_seen"] if row else None
        return float(v) if v else None
    finally:
        conn.close()

def update_last_seen(username: str):
    conn = get_db()
    try:
        conn.execute("""
            INSERT INTO points (username, last_seen) VALUES (?,?)
            ON CONFLICT(username) DO UPDATE SET last_seen=excluded.last_seen
        """, (username, time.time()))
        conn.commit()
    finally:
        conn.close()

# ─── Custom commands ───────────────────────────────────────────────────────────

def get_custom_cmd(cmd: str) -> Optional[str]:
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT response FROM custom_commands WHERE cmd=? AND enabled=1", (cmd,)
        ).fetchone()
        return row["response"] if row else None
    finally:
        conn.close()

# ─── Helpers ───────────────────────────────────────────────────────────────────

def fmt_msg(template: str, name: str = "", count: int = 0,
            months: int = 0, game: str = "") -> str:
    return (template
            .replace("{name}", name)
            .replace("{count}", str(count))
            .replace("{months}", str(months))
            .replace("{game}", game or get_setting("current_game") or "игру"))

def safe_int(val, default: int = 0) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return default

# ─── Bot state ─────────────────────────────────────────────────────────────────

class BotState:
    __slots__ = (
        "stream_start", "recent_messages", "heist_active", "heist_pot",
        "heist_players", "trivia_active", "trivia_answer", "trivia_question",
        "permitted_users", "ws_clients", "chat_history", "seen_this_stream",
        "stream_hello_sent", "lurkers", "msg_counters", "msg_timer_start",
        "stream_chat_count",
        "trivia_reward",
        "trivia_timer_task",)
    def __init__(self):
        self.stream_start:      Optional[float] = None
        self.recent_messages:   list  = []
        self.heist_active:      bool  = False
        self.heist_pot:         int   = 0
        self.heist_players:     dict  = {}
        self.trivia_active:     bool  = False
        self.trivia_answer:     str   = ""
        self.trivia_question:   str   = ""
        self.permitted_users:   set   = set()
        self.ws_clients:        list  = []
        self.chat_history:      list  = []
        self.seen_this_stream:  set   = set()
        self.stream_hello_sent: bool  = False
        self.lurkers:           set   = set()
        self.msg_counters:      dict  = {}
        self.msg_timer_start:   dict  = {}
        self.stream_chat_count: int   = 0
        self.trivia_reward:     int   = 200
        self.trivia_timer_task         = None

state = BotState()

TRIVIA = [
    {"q": "Сколько планет в Солнечной системе?",          "a": "8"},
    {"q": "Столица Франции?",                              "a": "париж"},
    {"q": "Сколько будет 7 умножить на 8?",                "a": "56"},
    {"q": "Синий + жёлтый = ?",                            "a": "зелёный"},
    {"q": "Год выхода первого iPhone?",                    "a": "2007"},
    {"q": "Сколько струн у стандартной гитары?",           "a": "6"},
    {"q": "Самая высокая гора в мире?",                    "a": "эверест"},
    {"q": "Химическая формула воды?",                      "a": "h2o"},
    {"q": "Сколько цветов в радуге?",                      "a": "7"},
    {"q": "Столица Японии?",                               "a": "токио"},
    {"q": "Сколько минут в сутках?",                       "a": "1440"},
    {"q": "Сколько сторон у шестиугольника?",              "a": "6"},
    {"q": "Химический символ золота?",                     "a": "au"},
    {"q": "В каком году началась Вторая мировая война?",   "a": "1939"},
    {"q": "Сколько байт в одном килобайте?",               "a": "1024"},
]

# ─── Broadcast ─────────────────────────────────────────────────────────────────

async def broadcast(event: str, data: dict):
    if not state.ws_clients:
        return
    msg = json.dumps({"event": event, **data}, ensure_ascii=False)
    dead = []
    for ws in state.ws_clients[:]:
        try:
            await ws.send_text(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        try:
            state.ws_clients.remove(ws)
        except ValueError:
            pass

async def send_bot_message(text: str):
    await broadcast("bot_message", {"message": text, "ts": time.time()})
    await twitch_client.send_message(text)

# ─── Points accumulation ───────────────────────────────────────────────────────

def maybe_award_points(username: str) -> int:
    pts     = safe_int(get_setting("points_per_message", "8"), 8)
    n_msgs  = max(1, safe_int(get_setting("points_interval_messages", "1"), 1))
    n_secs  = safe_int(get_setting("points_interval_seconds", "0"), 0)
    now     = time.time()
    if n_secs > 0:
        last = state.msg_timer_start.get(username, 0.0)
        if now - last >= n_secs:
            state.msg_timer_start[username] = now
            return pts
        return 0
    state.msg_counters[username] = state.msg_counters.get(username, 0) + 1
    if state.msg_counters[username] >= n_msgs:
        state.msg_counters[username] = 0
        return pts
    return 0

# ═══════════════════════════════════════════════════════════════════════════════
# ─── Twitch IRC ────────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

class TwitchClient:
    IRC_HOST = "irc.chat.twitch.tv"
    IRC_PORT = 6667

    def __init__(self):
        self.reader:       Optional[asyncio.StreamReader] = None
        self.writer:       Optional[asyncio.StreamWriter] = None
        self.is_connected: bool = False
        self._running:     bool = False
        self._send_lock          = asyncio.Lock()
        self._channel:     str  = ""
        self._delay:       int  = 5
        self._delay_max:   int  = 120

    async def start(self):
        self._running = True
        asyncio.create_task(self._run_loop())
        print("[IRC] Запущен")

    async def stop(self):
        self._running = False
        await self._close()

    async def send_message(self, text: str):
        if self.is_connected and self._channel:
            await self._raw(f"PRIVMSG #{self._channel} :{text}")

    async def timeout_user(self, username: str, duration: int):
        if self.is_connected and self._channel:
            await self._raw(f"PRIVMSG #{self._channel} :/timeout {username} {duration}")

    async def ban_user(self, username: str):
        if self.is_connected and self._channel:
            await self._raw(f"PRIVMSG #{self._channel} :/ban {username}")

    async def grant_vip_irc(self, username: str):
        if self.is_connected and self._channel:
            await self._raw(f"PRIVMSG #{self._channel} :/vip {username}")

    async def _run_loop(self):
        delay = self._delay
        while self._running:
            try:
                await self._connect()
                delay = self._delay
                await self._listen()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[IRC] Ошибка: {type(e).__name__}: {e}")
            finally:
                self.is_connected = False
                self._channel = ""
                await self._close()
                if self._running:
                    await broadcast("twitch_disconnected", {})
            if not self._running:
                break
            print(f"[IRC] Реконнект через {delay}с")
            await asyncio.sleep(delay)
            delay = min(delay * 2, self._delay_max)

    async def _connect(self):
        token = get_setting("twitch_token").strip()
        nick  = get_setting("bot_username").strip().lower()
        ch    = get_setting("twitch_channel").strip().lower().lstrip("#")
        if not (token and nick and ch):
            print("[IRC] Настройки не заполнены, ожидаю...")
            while self._running:
                await asyncio.sleep(10)
                token = get_setting("twitch_token").strip()
                nick  = get_setting("bot_username").strip().lower()
                ch    = get_setting("twitch_channel").strip().lower().lstrip("#")
                if token and nick and ch:
                    break
            if not self._running:
                raise asyncio.CancelledError()
        if not token.startswith("oauth:"):
            token = "oauth:" + token
        print(f"[IRC] Подключение nick={nick} channel=#{ch}")
        self.reader, self.writer = await asyncio.open_connection(self.IRC_HOST, self.IRC_PORT)
        for line in [f"PASS {token}", f"NICK {nick}",
                     "CAP REQ :twitch.tv/tags",
                     "CAP REQ :twitch.tv/commands",
                     "CAP REQ :twitch.tv/membership",
                     f"JOIN #{ch}"]:
            await self._raw(line)
        self._channel = ch
        self.is_connected = True
        print(f"[IRC] Подключён к #{ch}")
        await broadcast("twitch_connected", {"channel": ch})

    async def _close(self):
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
        self.reader = self.writer = None
        self.is_connected = False

    async def _listen(self):
        while self._running and self.reader:
            try:
                raw = await asyncio.wait_for(self.reader.readline(), timeout=300)
            except asyncio.TimeoutError:
                await self._raw("PING :tmi.twitch.tv")
                continue
            if not raw:
                raise ConnectionResetError("EOF")
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            if line:
                await self._dispatch(line)

    async def _dispatch(self, line: str):
        if line.startswith("PING"):
            await self._raw(f"PONG :{line.split(':',1)[1] if ':' in line else 'tmi.twitch.tv'}")
            return
        tags: dict = {}
        rest = line
        if rest.startswith("@"):
            tag_str, rest = rest[1:].split(" ", 1)
            for pair in tag_str.split(";"):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    tags[k] = v
        prefix = ""
        if rest.startswith(":"):
            prefix, rest = rest[1:].split(" ", 1)
        parts   = rest.split(" ", 2)
        command = parts[0] if parts else ""
        trailing = parts[2].lstrip(":") if len(parts) > 2 else ""
        if command.isdigit():
            return
        if command == "PRIVMSG":
            await self._on_privmsg(prefix, trailing, tags)
        elif command == "USERNOTICE":
            await self._on_usernotice(trailing, tags)
        elif command == "RECONNECT":
            raise ConnectionResetError("Server RECONNECT")

    async def _on_privmsg(self, prefix: str, text: str, tags: dict):
        username = prefix.split("!")[0] if "!" in prefix else prefix
        display  = tags.get("display-name", username) or username
        badges   = tags.get("badges", "")
        is_mod   = tags.get("mod") == "1" or "moderator" in badges or "broadcaster" in badges
        is_sub   = tags.get("subscriber") == "1" or "subscriber" in badges
        bits     = safe_int(tags.get("bits", "0"))
        if bits > 0:
            asyncio.create_task(on_bits(display, bits))
        await process_message(display, text, is_mod=is_mod, is_sub=is_sub)

    async def _on_usernotice(self, text: str, tags: dict):
        mid     = tags.get("msg-id", "")
        login   = tags.get("login", "")
        display = tags.get("display-name", login) or login
        if   mid == "sub":
            asyncio.create_task(on_sub(display, 1, False))
        elif mid == "resub":
            asyncio.create_task(on_sub(display, safe_int(tags.get("msg-param-cumulative-months","1"),1), True))
        elif mid == "subgift":
            asyncio.create_task(on_giftsub(display, 1))
        elif mid == "submysterygift":
            asyncio.create_task(on_giftsub(display, safe_int(tags.get("msg-param-mass-gift-count","1"),1)))
        elif mid == "anonsubgift":
            asyncio.create_task(on_giftsub("Аноним", 1))
        elif mid == "raid":
            raider  = tags.get("msg-param-displayName", display) or display
            viewers = safe_int(tags.get("msg-param-viewerCount","0"))
            asyncio.create_task(on_raid(raider, viewers))

    async def _raw(self, message: str):
        if not self.writer:
            return
        async with self._send_lock:
            try:
                self.writer.write((message + "\r\n").encode("utf-8"))
                await self.writer.drain()
            except Exception as e:
                print(f"[IRC] send error: {e}")
                raise

twitch_client = TwitchClient()

# ─── Event handlers ────────────────────────────────────────────────────────────

async def _evt_delay():
    d = safe_int(get_setting("event_delay", "2"))
    if d > 0:
        await asyncio.sleep(d)

async def on_follow(username: str):
    if get_setting("feat_events") != "1" or get_setting("evt_follow") != "1":
        return
    await _evt_delay()
    count = safe_int(get_setting("follower_count", "0")) + 1
    set_setting("follower_count", str(count))
    await send_bot_message(fmt_msg(get_setting("msg_follow"), name=username))
    await broadcast("event_follow", {"username": username, "follower_count": count})
    await check_milestone(count)

async def on_sub(username: str, months: int = 1, is_resub: bool = False):
    if get_setting("feat_events") != "1" or get_setting("evt_sub") != "1":
        return
    await _evt_delay()
    tmpl = get_setting("msg_resub") if (is_resub and months > 1) else get_setting("msg_sub")
    add_points(username, 500)
    await send_bot_message(fmt_msg(tmpl, name=username, months=months))
    await broadcast("event_sub", {"username": username, "months": months, "is_resub": is_resub})

async def on_giftsub(gifter: str, count: int):
    if get_setting("feat_events") != "1" or get_setting("evt_giftsub") != "1":
        return
    await _evt_delay()
    add_points(gifter, count * 200)
    await send_bot_message(fmt_msg(get_setting("msg_giftsub"), name=gifter, count=count))
    await broadcast("event_giftsub", {"gifter": gifter, "count": count})

async def on_raid(raider: str, viewers: int):
    if get_setting("feat_events") != "1" or get_setting("evt_raid") != "1":
        return
    await _evt_delay()
    add_points(raider, viewers * 5)
    await send_bot_message(fmt_msg(get_setting("msg_raid"), name=raider, count=viewers))
    await broadcast("event_raid", {"raider": raider, "viewers": viewers})

async def on_bits(username: str, amount: int):
    if get_setting("feat_events") != "1" or get_setting("evt_bits") != "1":
        return
    await _evt_delay()
    add_points(username, amount // 10)
    await send_bot_message(fmt_msg(get_setting("msg_bits"), name=username, count=amount))
    await broadcast("event_bits", {"username": username, "amount": amount})

async def on_first_message(username: str):
    if get_setting("feat_events") != "1" or get_setting("evt_firstmsg") != "1":
        return
    await send_bot_message(fmt_msg(get_setting("msg_firstmsg"), name=username))
    await broadcast("event_firstmsg", {"username": username})

async def on_return(username: str):
    if get_setting("feat_events") != "1" or get_setting("evt_return") != "1":
        return
    await send_bot_message(fmt_msg(get_setting("msg_return"), name=username))
    await broadcast("event_return", {"username": username})

async def check_milestone(count: int):
    if get_setting("feat_events") != "1" or get_setting("evt_milestone") != "1":
        return
    ms = [int(x.strip()) for x in get_setting("milestones","100,500,1000").split(",")
          if x.strip().isdigit()]
    if count in ms:
        await send_bot_message(fmt_msg(get_setting("msg_milestone"), count=count))
        await broadcast("event_milestone", {"count": count})

async def on_stream_start_hello():
    if get_setting("feat_events") != "1" or get_setting("evt_stream_hello") != "1":
        return
    if state.stream_hello_sent:
        return
    await asyncio.sleep(3)
    if not state.stream_start:  # Стрим остановили пока ждали
        return
    await send_bot_message(get_setting("msg_stream_hello"))
    state.stream_hello_sent = True

async def on_stream_stop_bye():
    if get_setting("feat_events") != "1" or get_setting("evt_stream_bye") != "1":
        return
    await send_bot_message(get_setting("msg_stream_bye"))

# ─── Moderation ────────────────────────────────────────────────────────────────

async def handle_punishment(username: str, warn_count: int, reason: str):
    """1-2 варна = предупреждение, 3 = тайм-аут 10 мин, 4+ = бан."""
    if warn_count <= 2:
        await send_bot_message(f"⚠️ {username}, предупреждение {warn_count}/3! ({reason})")
    elif warn_count == 3:
        await send_bot_message(f"🔇 {username} получает тайм-аут 10 минут (3 нарушения: {reason})")
        await twitch_client.timeout_user(username, 600)
        await broadcast("timeout_user", {"username": username, "duration": 600})
    else:
        await send_bot_message(f"🔨 {username} забанен за систематические нарушения")
        await twitch_client.ban_user(username)
        await broadcast("ban_user", {"username": username})

async def moderate(username: str, message: str) -> tuple:
    now = time.time()
    ml  = message.lower()
    ul  = username.lower()

    banned = get_setting("banned_words", "")
    if banned and get_setting("mod_banned_words") == "1":
        for word in banned.split(","):
            w = word.strip().lower()
            if w and w in ml:
                warns = add_warning(username, f"Слово: {w}")
                await handle_punishment(username, warns, "запрещённое слово")
                return True, "запрещённое слово"

    if get_setting("mod_links") == "1":
        if re.search(r"https?://|www\.", ml):
            if ul not in state.permitted_users:
                warns = add_warning(username, "Ссылка")
                await handle_punishment(username, warns, "ссылка без разрешения")
                return True, "ссылка"
            else:
                # Permit использован - снимаем после разрешённой ссылки
                state.permitted_users.discard(ul)

    if get_setting("mod_caps") == "1" and len(message) > 10:
        letters = [c for c in message if c.isalpha()]
        if letters:
            pct = sum(1 for c in letters if c.isupper()) / len(letters) * 100
            if pct >= safe_int(get_setting("caps_threshold", "70"), 70):
                warns = add_warning(username, "Капс")
                await handle_punishment(username, warns, "капс")
                return True, "капс"

    if get_setting("mod_spam") == "1":
        recent = [m for m in state.recent_messages
                  if m["user"] == ul and now - m["ts"] < 30]
        thr = safe_int(get_setting("spam_threshold", "3"), 3)
        if len(recent) >= thr:
            same = sum(1 for m in recent[-thr:] if m["text"] == message)
            if same >= 2:
                warns = add_warning(username, "Спам")
                await handle_punishment(username, warns, "спам")
                return True, "спам"

    state.recent_messages.append({"user": ul, "text": message, "ts": now})
    # Лимит: храним только последние 60 секунд
    if len(state.recent_messages) > 1000:
        state.recent_messages = [m for m in state.recent_messages if now - m["ts"] < 60]
    return False, ""

# ─── Mini-games ────────────────────────────────────────────────────────────────

async def cmd_gamble(username: str, amount_str: str):
    amount = safe_int(amount_str, -1)
    if amount <= 0:
        await send_bot_message("❌ Используй: !gamble [сумма]")
        return
    pts = get_points(username)
    if amount > pts:
        await send_bot_message(f"❌ {username}, у тебя только {pts} баллов")
        return
    if random.random() > 0.5:
        add_points(username, amount)
        await send_bot_message(f"🎰 {username} выиграл {amount} баллов! Итого: {pts + amount}")
    else:
        add_points(username, -amount)
        await send_bot_message(f"🎰 {username} проиграл {amount} баллов. Итого: {pts - amount}")

async def cmd_slots(username: str, amount_str: str):
    amount = safe_int(amount_str, -1)
    if amount <= 0:
        await send_bot_message("❌ Используй: !slots [сумма]")
        return
    pts = get_points(username)
    if amount > pts:
        await send_bot_message(f"❌ {username}, у тебя только {pts} баллов")
        return
    emojis = ["🍒", "🍋", "🍊", "⭐", "💎", "7️⃣"]
    s = [random.choice(emojis) for _ in range(3)]
    res = " ".join(s)
    if s[0] == s[1] == s[2]:
        mult = 10 if s[0] == "💎" else 5
        win = amount * mult
        add_points(username, win - amount)
        await send_bot_message(f"🎰 {res}  -  ДЖЕКПОТ! {username} выиграл {win} баллов!")
    elif s[0] == s[1] or s[1] == s[2] or s[0] == s[2]:
        await send_bot_message(f"🎰 {res}  -  {username} два совпадения! Ставка возвращена.")
    else:
        add_points(username, -amount)
        await send_bot_message(f"🎰 {res}  -  {username} потерял {amount} баллов.")

async def cmd_duel(username: str, target: str, amount_str):
    amount = max(1, safe_int(amount_str, 50))
    if username.lower() == target.lower():
        await send_bot_message("❌ Нельзя вызвать самого себя!")
        return
    pa = get_points(username)
    pb = get_points(target)
    if pa < amount:
        await send_bot_message(f"❌ {username}, нужно {amount} баллов, у тебя {pa}")
        return
    if pb < amount:
        await send_bot_message(f"❌ У {target} нет {amount} баллов")
        return
    winner = random.choice([username, target])
    loser  = target if winner == username else username
    add_points(winner, amount)
    add_points(loser, -amount)
    await send_bot_message(f"⚔️ {username} vs {target} на {amount}  -  победил {winner}!")

async def cmd_roulette(username: str, amount_str: str):
    amount = safe_int(amount_str, -1)
    if amount <= 0:
        await send_bot_message("❌ Используй: !roulette [ставка]")
        return
    pts = get_points(username)
    if amount > pts:
        await send_bot_message(f"❌ {username}, у тебя только {pts} баллов")
        return
    if random.randint(1, 6) == 1:
        add_points(username, -amount)
        add_warning(username, "Рулетка")
        await send_bot_message(f"🔫 {username}  -  ВЫСТРЕЛ! -{amount} баллов + тайм-аут 60 сек")
        await twitch_client.timeout_user(username, 60)
        await broadcast("timeout_user", {"username": username, "duration": 60})
    else:
        reward = int(amount * 0.5)
        add_points(username, reward)
        await send_bot_message(f"🔫 {username}  -  осечка! +{reward} баллов (выжил)")

async def cmd_heist(username: str, amount_str: str):
    amount = safe_int(amount_str, -1)
    if amount <= 0:
        await send_bot_message("❌ Используй: !heist [ставка]")
        return
    pts = get_points(username)
    if amount > pts:
        await send_bot_message(f"❌ {username}, у тебя только {pts} баллов")
        return
    if not state.heist_active:
        state.heist_active = True
        state.heist_pot    = 0
        state.heist_players = {}
        await send_bot_message(
            f"🚨 {username} начинает ограбление! !heist [ставка] чтобы вступить (60 сек)"
        )
        asyncio.create_task(_finish_heist())
    if username not in state.heist_players:
        add_points(username, -amount)
        state.heist_players[username] = amount
        state.heist_pot += amount
        await send_bot_message(f"💰 {username} вложил {amount}. Банк: {state.heist_pot}")

async def _finish_heist():
    await asyncio.sleep(60)
    if not state.heist_active:
        return
    state.heist_active = False
    if not state.heist_players:
        return
    names = ", ".join(state.heist_players.keys())
    n = len(state.heist_players)
    if random.random() > 0.4:
        per = (state.heist_pot + state.heist_pot // 2) // n
        for p in state.heist_players:
            add_points(p, per)
        await send_bot_message(f"🎉 Ограбление удалось! {names} получают по {per} баллов!")
    else:
        await send_bot_message(f"🚔 Ограбление провалилось! {names} потеряли ставки!")
    state.heist_players = {}
    state.heist_pot = 0

async def _trivia_timeout(secs: int):
    await asyncio.sleep(secs)
    if state.trivia_active:
        state.trivia_active = False
        await send_bot_message(f"⏰ Время вышло! Никто не ответил. Ответ: {state.trivia_answer}")

async def cmd_trivia():
    if hasattr(state, 'trivia_timer_task') and state.trivia_timer_task:
        try: state.trivia_timer_task.cancel()
        except Exception: pass
    raw = get_setting("trivia_questions", "[]")
    try:
        custom_qs = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        custom_qs = []
    pool = custom_qs if custom_qs else TRIVIA
    q = random.choice(pool)
    reward  = safe_int(get_setting("trivia_reward", "200"), 200)
    timeout = safe_int(get_setting("trivia_timeout", "60"), 60)
    state.trivia_active   = True
    state.trivia_question = q["q"]
    state.trivia_answer   = q["a"]
    state.trivia_reward   = reward
    if timeout > 0:
        state.trivia_timer_task = asyncio.create_task(_trivia_timeout(timeout))
    await send_bot_message(
        f"🧠 Викторина: {q['q']} "
        f"(правильный ответ = +{reward} баллов"
        + (f", {timeout} сек)" if timeout > 0 else ")")
    )

# ─── Help ───────────────────────────────────────────────────────────────────────

def get_help():
    cost = get_setting("vip_cost", "8000")
    return [
        "💰 Баллы: !points, !top, !rank, !daily, !give @ник N",
        "📋 Инфо: !uptime, !game, !sr [песня], !hug @ник, !lurk",
        "🔗 Соцсети: !tg, !tiktok, !discord",
        f"⭐ VIP: !vip ({cost} баллов, 24ч)  -  +50% баллов, без модерации",
    ]

# ─── Process message ───────────────────────────────────────────────────────────

async def process_message(username: str, message: str,
                           is_mod: bool = False, is_sub: bool = False):
    now      = time.time()
    ml       = message.lower().strip()
    vip_user = is_vip(username)

    # Лог в БД
    conn = get_db()
    try:
        conn.execute("INSERT INTO chat_log (ts, username, message) VALUES (?,?,?)",
                     (now, username, message[:1000]))
        conn.commit()
    finally:
        conn.close()

    # Счётчик сообщений текущего стрима
    if state.stream_start:
        state.stream_chat_count += 1
    # Кэш в памяти
    state.chat_history.append({
        "ts": now, "username": username, "message": message[:500],
        "is_mod": is_mod, "is_sub": is_sub, "is_vip": vip_user
    })
    if len(state.chat_history) > 200:
        state.chat_history = state.chat_history[-200:]

    await broadcast("chat_message", {
        "username": username, "message": message[:500],
        "is_mod": is_mod, "is_sub": is_sub, "is_vip": vip_user, "ts": now
    })

    # Первое сообщение / возвращение
    if get_setting("feat_events") == "1":
        ul = username.lower()
        if ul not in state.seen_this_stream:
            state.seen_this_stream.add(ul)
            last = get_last_seen(ul)
            rd   = safe_int(get_setting("return_days", "30"), 30)
            if last is None:
                asyncio.create_task(on_first_message(username))
            elif (now - last) > rd * 86400:
                asyncio.create_task(on_return(username))
            elif state.stream_start:
                asyncio.create_task(on_first_message(username))
        update_last_seen(ul)

    # Модерация (моды и VIP свободны)
    if get_setting("feat_moderation") == "1" and not is_mod and not vip_user:
        deleted, reason = await moderate(username, message)
        if deleted:
            await broadcast("moderation_action", {"username": username, "reason": reason})
            return

    # Начисление баллов
    if get_setting("feat_points") == "1":
        earned = maybe_award_points(username)
        if earned > 0:
            if vip_user:
                bonus = safe_int(get_setting("vip_points_bonus", "50"), 50)
                earned = int(earned * (1 + bonus / 100))
            add_points(username, earned)

    state.lurkers.discard(username.lower())

    # Викторина
    if not message.startswith("!"):
        if state.trivia_active and get_setting("feat_trivia") == "1":
            if ml == state.trivia_answer.lower():
                reward = getattr(state, 'trivia_reward', 200)
                add_points(username, reward)
                state.trivia_active = False
                if hasattr(state, 'trivia_timer_task') and state.trivia_timer_task:
                    try: state.trivia_timer_task.cancel()
                    except Exception: pass
                await send_bot_message(
                    f"🎉 {username} ответил верно! +{reward} баллов. Ответ: {state.trivia_answer}"
                )
        return

    parts = message[1:].split()
    cmd   = parts[0].lower() if parts else ""
    args  = parts[1:] if len(parts) > 1 else []

    # Кастомные команды
    custom = get_custom_cmd(cmd)
    if custom:
        resp = custom.replace("{name}", username)\
                     .replace("{game}", get_setting("current_game") or "игру")
        await send_bot_message(resp)
        return

    if get_setting("feat_commands") != "1" and cmd not in ["clip", "points", "help"]:
        return

    # ─── Команды ───────────────────────────────────────────────────────────────

    if cmd in ("help", "commands", "cmd"):
        for line in get_help():
            await send_bot_message(line)
            await asyncio.sleep(0.4)

    elif cmd == "points":
        pts = get_points(username)
        tag = " ⭐VIP" if vip_user else ""
        await send_bot_message(f"💰 {username}{tag}: {pts} баллов")

    elif cmd == "top":
        top = get_top_points(5)
        if top:
            s = " | ".join(
                f"{i+1}. {'⭐' if r['is_vip'] else ''}{r['username']} ({r['points']})"
                for i, r in enumerate(top)
            )
            await send_bot_message(f"🏆 Топ: {s}")

    elif cmd == "rank":
        pts = get_points(username)
        rank, total = get_rank(username)
        tag = " ⭐" if vip_user else ""
        await send_bot_message(f"📊 {username}{tag}  -  #{rank} из {total} ({pts} баллов)")

    elif cmd == "daily":
        cd = check_cooldown(username, "daily", COOLDOWNS["daily"])
        if cd:
            await send_bot_message(f"⏳ {username}, бонус уже получен. Следующий через {fmt_cd(cd)}")
        else:
            mn  = safe_int(get_setting("daily_min", "400"), 400)
            mx  = safe_int(get_setting("daily_max", "1000"), 1000)
            bonus = random.randint(mn, mx)
            if vip_user:
                bonus = int(bonus * 1.5)
            add_points(username, bonus)
            await send_bot_message(
                f"🎁 {username} получает ежедневный бонус +{bonus} баллов! Итого: {get_points(username)}"
            )

    elif cmd == "give":
        if len(args) < 2:
            await send_bot_message("❌ Используй: !give @ник [сумма]")
            return
        cd = check_cooldown(username, "give", COOLDOWNS["give"])
        if cd:
            await send_bot_message(f"⏳ {username}, !give доступен через {fmt_cd(cd)}")
            return
        target = args[0].lstrip("@")
        amount = safe_int(args[1], -1)
        if amount <= 0:
            await send_bot_message("❌ Сумма должна быть больше 0")
            return
        if target.lower() == username.lower():
            await send_bot_message("❌ Нельзя дарить самому себе")
            return
        pts = get_points(username)
        if pts < amount:
            await send_bot_message(f"❌ {username}, у тебя только {pts} баллов")
        else:
            add_points(username, -amount)
            add_points(target, amount)
            await send_bot_message(
                f"🎁 {username} подарил {amount} баллов -> {target}! (осталось: {pts-amount})"
            )

    elif cmd == "hug":
        if not args:
            await send_bot_message("❌ Используй: !hug @ник")
            return
        target = args[0].lstrip("@")
        action = random.choice(["обнял", "крепко обнял", "дал обнимашки"])
        await send_bot_message(f"🤗 {username} {action} {target}!")

    elif cmd == "lurk":
        state.lurkers.add(username.lower())
        await send_bot_message(f"👻 {username} уходит в лурк. Баллы за просмотр продолжают капать!")

    elif cmd == "sr":
        if not args:
            await send_bot_message("❌ Используй: !sr [название песни]")
            return
        song = " ".join(args)[:200]
        conn = get_db()
        try:
            conn.execute("INSERT INTO song_queue (ts, username, song) VALUES (?,?,?)",
                         (time.time(), username, song))
            conn.commit()
        finally:
            conn.close()
        await send_bot_message(f"🎵 {username} заказал: {song}")
        await broadcast("song_requested", {"username": username, "song": song})

    elif cmd == "game":
        g = get_setting("current_game", "")
        await send_bot_message(f"🎮 Сейчас играем в {g}" if g else "🎮 Игра не указана")

    elif cmd == "uptime":
        if state.stream_start:
            e = int(time.time() - state.stream_start)
            h, m = divmod(e // 60, 60)
            await send_bot_message(f"⏱ Стрим идёт {h}ч {m}м")
        else:
            await send_bot_message("Стрим не запущен")

    elif cmd == "tg":
        v = get_setting("tg_link")
        await send_bot_message(f"Telegram: {v}" if v else "Ссылка не задана")

    elif cmd == "tiktok":
        v = get_setting("tiktok_link")
        await send_bot_message(f"TikTok: {v}" if v else "Ссылка не задана")

    elif cmd == "discord":
        v = get_setting("discord_link")
        await send_bot_message(f"Discord: {v}" if v else "Ссылка не задана")

    elif cmd == "so" and is_mod and args:
        t = args[0].lstrip("@")
        await send_bot_message(f"👏 Шаутаут {t}! twitch.tv/{t}")

    elif cmd == "clip" and get_setting("feat_clips") == "1":
        dur   = safe_int(get_setting("clip_duration", "10"), 10)
        ts    = time.time()
        label = " ".join(args)[:100] if args else f"Клип от {username}"
        conn  = get_db()
        try:
            conn.execute("INSERT INTO clips (ts, label, duration) VALUES (?,?,?)",
                         (ts, label, dur))
            conn.commit()
        finally:
            conn.close()
        await send_bot_message(f"🎬 Клип сохранён: {label}")
        await broadcast("clip_created", {"label": label, "duration": dur, "ts": ts, "by": username})

    elif cmd == "deaths" and get_setting("feat_deaths") == "1":
        await send_bot_message(f"💀 Смертей: {get_setting('death_count','0')}")

    elif cmd == "goal" and get_setting("feat_goals") == "1":
        cur = safe_int(get_setting("goal_current", "0"))
        tgt = safe_int(get_setting("goal_target", "100"))
        lbl = get_setting("goal_label", "Цель")
        pct = min(100, round(cur / tgt * 100)) if tgt > 0 else 0
        await send_bot_message(f"🎯 {lbl}: {cur}/{tgt} ({pct}%)")

    elif cmd == "permit" and is_mod and args:
        t = args[0].lstrip("@")
        state.permitted_users.add(t.lower())
        await send_bot_message(f"✅ {t} может отправить ссылку")

    elif cmd in ("gamble","slots","duel","roulette","heist"):
        await send_bot_message(f"⚠️ Мини-игры временно отключены.")

    elif cmd == "trivia" and is_mod and get_setting("feat_trivia") == "1":
        await cmd_trivia()

    elif cmd == "vip" and get_setting("feat_vip") == "1":
        cd = check_cooldown(username, "vip", COOLDOWNS["vip"])
        if cd:
            await send_bot_message(f"⏳ {username}, VIP уже куплен. Снова через {fmt_cd(cd)}")
        else:
            cost = safe_int(get_setting("vip_cost", "8000"), 8000)
            hrs  = safe_int(get_setting("vip_hours", "24"), 24)
            pts  = get_points(username)
            if pts >= cost:
                add_points(username, -cost)
                grant_vip(username, hrs)
                await send_bot_message(
                    f"⭐ {username} получает VIP на {hrs}ч! (-{cost} баллов) "
                    f"+50% баллов, без модерации, кулдауны в 2 раза короче!"
                )
                await twitch_client.grant_vip_irc(username)
                await broadcast("vip_granted", {"username": username})
            else:
                await send_bot_message(
                    f"❌ {username}, нужно {cost} баллов, у тебя {pts}. "
                    f"Не хватает {cost - pts}!"
                )

    elif cmd == "hydrate":
        await send_bot_message("⚠️ Мини-игры временно отключены.")

    elif cmd == "followage" and get_setting("feat_followage") == "1":
        await send_bot_message(
            f"📅 {username}, эта функция требует Twitch API. "
            f"Точные данные о дате фолловера недоступны в текущей версии."
        )

    # Мод-команды
    elif cmd == "death" and is_mod and get_setting("feat_deaths") == "1":
        dc = safe_int(get_setting("death_count", "0")) + 1
        set_setting("death_count", str(dc))
        await send_bot_message(f"💀 Смертей: {dc}")
        await broadcast("death_updated", {"count": dc})

    elif cmd == "setgoal" and is_mod and get_setting("feat_goals") == "1":
        if not args:
            await send_bot_message("❌ Используй: !setgoal [число]"); return
        val = safe_int(args[0], -1)
        if val < 0:
            await send_bot_message("❌ Укажи число"); return
        set_setting("goal_current", str(val))
        await broadcast("goal_updated", {"current": val})

    elif cmd == "setgame" and is_mod:
        if not args:
            await send_bot_message("❌ Используй: !setgame [название]"); return
        game = " ".join(args)[:100]
        set_setting("current_game", game)
        await send_bot_message(f"🎮 Игра: {game}")
        await broadcast("game_updated", {"game": game})

    elif cmd == "timeout" and is_mod and args:
        t   = args[0].lstrip("@")
        dur = safe_int(args[1], 300) if len(args) > 1 else 300
        await twitch_client.timeout_user(t, dur)
        await send_bot_message(f"🔇 {t} получил тайм-аут {dur} сек")

    elif cmd == "ban" and is_mod and args:
        t = args[0].lstrip("@")
        await twitch_client.ban_user(t)
        await send_bot_message(f"🔨 {t} забанен")

    elif cmd == "unvip" and is_mod and args:
        t = args[0].lstrip("@")
        revoke_vip(t)
        await send_bot_message(f"VIP снят с {t}")

# ─── Background tasks ──────────────────────────────────────────────────────────

async def watch_time_task():
    """Каждую минуту начисляет баллы за просмотр активным зрителям."""
    while True:
        await asyncio.sleep(60)
        if not state.stream_start or get_setting("feat_points") != "1":
            continue
        pts = safe_int(get_setting("points_per_minute", "5"), 5)
        if pts <= 0:
            continue
        now    = time.time()
        active = {m["username"] for m in state.chat_history if now - m["ts"] < 600}
        active |= state.lurkers
        if not active:
            continue
        bonus_pct = safe_int(get_setting("vip_points_bonus", "50"), 50)
        for uname in active:
            earned = pts
            if is_vip(uname):
                earned = int(pts * (1 + bonus_pct / 100))
            add_points(uname, earned)
        await broadcast("watch_tick", {"pts_per_min": pts, "active_count": len(active)})

async def autopost_task():
    while True:
        interval = max(5, safe_int(get_setting("autopost_interval", "15"), 15)) * 60
        await asyncio.sleep(interval)
        if get_setting("feat_autopost") != "1":
            continue
        # Гибкие ссылки: [{name, url, enabled}, ...]
        raw = get_setting("autopost_links", "[]")
        try:
            link_list = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            link_list = []
        # Fallback на старые поля если новых нет
        if not link_list:
            for k, lbl in [("tg_link","Telegram"),("tiktok_link","TikTok"),
                            ("discord_link","Discord"),("youtube_link","YouTube")]:
                v = get_setting(k)
                if v:
                    link_list.append({"name": lbl, "url": v, "enabled": True})
        active = [x for x in link_list if x.get("enabled") and x.get("url")]
        if active:
            parts = [f"{x['name']}: {x['url']}" for x in active]
            await send_bot_message("Наши соцсети: " + " | ".join(parts))

# ─── OBS overlay renderer ──────────────────────────────────────────────────────


OBS_OVERLAY_PAGE = """<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Unbounded:wght@400;700;900&family=Roboto:wght@400;700&family=Montserrat:wght@400;700;900&family=Oswald:wght@400;700&family=Anton&family=Bebas+Neue&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{width:1920px;height:1080px;overflow:hidden;background:transparent;position:relative}
.obs-widget{position:absolute;overflow:hidden}
@keyframes marquee{0%{transform:translateX(100%)}100%{transform:translateX(-100%)}}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.6}}
@keyframes glow{0%,100%{text-shadow:0 0 10px currentColor}50%{text-shadow:0 0 30px currentColor,0 0 60px currentColor}}
@keyframes alertIn{from{opacity:0;transform:scale(.85) translateY(24px)}to{opacity:1;transform:scale(1) translateY(0)}}
@keyframes alertOut{from{opacity:1;transform:scale(1)}to{opacity:0;transform:scale(.9)}}
@keyframes particle{0%{top:-8px;opacity:1}100%{top:110%;opacity:0;transform:rotate(360deg)}}
</style>
</head>
<body>
<div id="root"></div>
<script>
var SCENE_ID = '{{SCENE_ID}}';
var ws, alertQueue = [], alertShowing = false;

// ── Shared widget renderer (same logic as editor preview) ──────────────────
function renderWidget(w) {
  var p = w.props || {};
  var op = ((p.opacity != null ? p.opacity : 100)) / 100;
  var base = 'position:absolute;left:'+w.x+'px;top:'+w.y+'px;width:'+w.w+'px;height:'+w.h+'px;overflow:hidden;opacity:'+op;
  var fnt  = 'font-family:\''+(p.fontFamily||'Space Mono')+'\',monospace;font-size:'+(p.fontSize||14)+'px;font-weight:'+(p.fontWeight||'400')+';';
  var bg   = (p.bgEnabled && p.bgColor) ? 'background:'+p.bgColor+';' : 'background:transparent;';
  var br   = 'border-radius:'+(p.borderRadius||0)+'px;';
  var el   = document.createElement('div');
  el.className = 'obs-widget';
  el.style.cssText = base;
  el.setAttribute('data-wtype', w.type);
  el.setAttribute('data-wid', w.id);
  el.innerHTML = buildWidgetHTML(w, p, fnt, bg, br);
  return el;
}

function buildWidgetHTML(w, p, fnt, bg, br) {
  var t = w.type;
  var uc = p.usernameColor||'#a855f7', mc=p.modColor||'#eab308', sc=p.subColor||'#22c55e', vc=p.vipColor||'#f59e0b';
  if (t==='chat') {
    return '<div class="chat-inner" data-max="'+(p.maxMessages||30)+'" '+
      'data-uc="'+uc+'" data-mc="'+mc+'" data-sc="'+sc+'" data-vc="'+vc+'" '+
      'style="'+fnt+'color:'+(p.textColor||'#e2e8f0')+';'+bg+br+'height:100%;overflow:hidden;display:flex;flex-direction:column;justify-content:flex-end;gap:2px;padding:8px"></div>';
  }
  if (t==='alert') {
    var ac = p.accentColor||'#a855f7';
    return '<div class="alert-box" data-dur="'+(p.duration||4000)+'" data-ac="'+ac+'" '+
      'style="display:none;'+bg+br+'border:2px solid '+ac+';height:100%;width:100%;'+
      'align-items:center;justify-content:center;flex-direction:column;text-align:center;padding:16px;'+
      'box-shadow:0 0 40px '+ac+'80;position:absolute;top:0;left:0">'+
      '<div class="a-particles" style="position:absolute;inset:0;pointer-events:none;overflow:hidden;'+br+'"></div>'+
      '<div class="a-icon" style="font-size:36px;margin-bottom:4px">!</div>'+
      '<div class="a-title" style="'+fnt+'color:'+ac+';font-size:'+(Math.round((p.fontSize||28)*0.5))+'px;letter-spacing:.08em">АЛЕРТ</div>'+
      '<div class="a-name" style="'+fnt+'color:'+(p.textColor||'#fff')+';font-size:'+(p.fontSize||28)+'px;font-weight:900">Username</div>'+
      '<div class="a-sub" style="font-size:12px;color:#94a3b8;margin-top:3px"></div>'+
      '</div>';
  }
  if (t==='text') {
    var ef = p.textEffect||'none';
    var efStyle = ef==='gradient' ? 'background:linear-gradient(90deg,#a855f7,#06b6d4);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text' :
                  ef==='pulse'    ? 'animation:pulse 2s infinite' :
                  ef==='glow'     ? 'animation:glow 2s infinite;color:'+(p.textColor||'#fff') : '';
    var sk = p.strokeEnabled ? '-webkit-text-stroke:'+(p.strokeWidth||2)+'px '+(p.strokeColor||'#000')+';' : '';
    var sh = p.shadowEnabled ? 'text-shadow:0 2px '+(p.shadowBlur||8)+'px '+(p.shadowColor||'rgba(0,0,0,.8)')+';' : '';
    var txt = esc(p.content||'');
    return '<div style="'+fnt+bg+br+'color:'+(p.textColor||'#fff')+';padding:8px;height:100%;'+
      'text-align:'+(p.align||'left')+';letter-spacing:'+(p.letterSpacing||0)+'px;line-height:'+(p.lineHeight||1.4)+';'+sk+sh+efStyle+'">'+txt+'</div>';
  }
  if (t==='ticker') {
    var sp = Math.max(1, p.speed||60), dur = Math.max(5, Math.round(1920/sp));
    var ac2 = p.accentColor||'#7c3aed';
    var lbl = p.showLabel ? '<div style="background:'+ac2+';color:#fff;padding:0 12px;font-size:10px;font-weight:700;white-space:nowrap;height:100%;display:flex;align-items:center;flex-shrink:0">'+esc(p.labelText||'LIVE')+'</div>' : '';
    return '<div style="'+bg+'border-top:2px solid '+ac2+';display:flex;align-items:center;overflow:hidden;height:100%">'+lbl+
      '<div style="overflow:hidden;flex:1;height:100%;display:flex;align-items:center">'+
      '<div class="ticker-inner" style="'+fnt+'color:'+(p.textColor||'#e2e8f0')+';white-space:nowrap;animation:marquee '+dur+'s linear infinite">'+esc(p.content||'')+'</div></div></div>';
  }
  if (t==='image') {
    return '<img src="'+esc(p.url||'')+'" style="width:100%;height:100%;object-fit:'+(p.fit||'contain')+';border-radius:'+(p.borderRadius||0)+'px" onerror="this.style.display=\'none\'">';
  }
  if (t==='background') {
    var bt = p.type||'solid', bgcss;
    if (bt==='gradient') bgcss='linear-gradient('+(p.gradientAngle||135)+'deg,'+(p.gradientFrom||'#0a0a1a')+','+(p.gradientTo||'#1a0a2e')+')';
    else if (bt==='image' && p.imageUrl) bgcss='url('+p.imageUrl+') center/'+(p.imageFit||'cover')+' no-repeat';
    else bgcss = p.color||'#0a0a1a';
    return '<div style="width:100%;height:100%;background:'+bgcss+'"></div>';
  }
  if (t==='deaths') {
    var gw = p.glowEnabled ? 'text-shadow:0 0 20px '+(p.glowColor||'rgba(239,68,68,.5)')+';' : '';
    var lblH = p.labelEnabled ? '<div style="font-size:'+(p.labelSize||12)+'px;color:'+(p.labelColor||'#94a3b8')+'">'+esc(p.labelText||'СМЕРТЕЙ')+'</div>' : '';
    return '<div style="'+bg+br+'height:100%;display:flex;flex-direction:column;align-items:center;justify-content:center">'+
      '<div class="deaths-val" style="'+fnt+'color:'+(p.textColor||'#ef4444')+';'+gw+'">0</div>'+lblH+'</div>';
  }
  if (t==='goal') {
    var bc = p.barColor||'#a855f7', gw2 = p.glowEnabled ? 'box-shadow:0 0 10px '+bc+';' : '';
    var lblH2 = p.showLabel ? '<div class="goal-lbl" style="'+fnt+'color:'+(p.textColor||'#e2e8f0')+';margin-bottom:4px">Цель</div>' : '';
    var numsH = p.showNumbers ? '<div style="display:flex;justify-content:space-between;font-size:11px;color:'+(p.textColor||'#e2e8f0')+';margin-top:3px"><span class="goal-cur">0</span>'+(p.showPercent?'<span class="goal-pct">0%</span>':'')+'<span class="goal-tgt">100</span></div>' : '';
    return '<div style="'+bg+br+'height:100%;padding:10px;display:flex;flex-direction:column;justify-content:center">'+lblH2+
      '<div style="background:'+(p.barBgColor||'rgba(30,30,50,.8)')+';border-radius:'+(p.borderRadius||6)+'px;height:'+(p.barHeight||12)+'px;overflow:hidden">'+
      '<div class="goal-fill" style="width:0%;height:100%;background:'+bc+';'+gw2+'border-radius:'+(p.borderRadius||6)+'px;transition:width .5s ease"></div></div>'+numsH+'</div>';
  }
  if (t==='toppoints') {
    var ac3 = p.accentColor||'#a855f7';
    return '<div style="'+bg+br+'height:100%;padding:10px;overflow:hidden">'+
      '<div style="'+fnt+'color:'+(p.titleColor||'#c084fc')+';margin-bottom:6px">'+esc(p.titleText||'ТОП')+'</div>'+
      '<div class="toppoints-inner" data-max="'+(p.count||5)+'" data-ac="'+ac3+'" style="'+fnt+'color:'+(p.textColor||'#e2e8f0')+'"></div></div>';
  }
  if (t==='uptime') {
    var lblU = p.labelEnabled ? '<div style="font-size:'+(p.labelSize||10)+'px;color:'+(p.labelColor||'#475569')+';letter-spacing:.1em">'+esc(p.labelText||'В ЭФИРЕ')+'</div>' : '';
    return '<div style="'+bg+br+'height:100%;display:flex;flex-direction:column;align-items:center;justify-content:center">'+
      '<div class="uptime-val" style="'+fnt+'color:'+(p.textColor||'#06b6d4')+'">--:--</div>'+lblU+'</div>';
  }
  if (t==='clock') {
    return '<div style="'+bg+br+'height:100%;display:flex;align-items:center;justify-content:center">'+
      '<div class="clock-val" data-secs="'+(p.showSeconds!==false?'1':'0')+'" style="'+fnt+'color:'+(p.textColor||'#e2e8f0')+'">00:00:00</div></div>';
  }
  if (t==='timer') {
    var tgt = p.targetTime||'', lblT = p.labelEnabled ? '<div style="font-size:'+(p.labelSize||10)+'px;color:'+(p.labelColor||'#475569')+'">'+esc(p.labelText||'')+'</div>' : '';
    return '<div style="'+bg+br+'height:100%;display:flex;flex-direction:column;align-items:center;justify-content:center">'+
      '<div class="timer-val" data-target="'+esc(tgt)+'" style="'+fnt+'color:'+(p.textColor||'#eab308')+'">00:00:00</div>'+lblT+'</div>';
  }
  if (t==='songqueue') {
    var ac4=p.accentColor||'#a855f7', lblS=p.showLabel?'<div style="font-size:10px;color:'+ac4+';margin-bottom:4px">'+esc(p.labelText||'NOW PLAYING')+'</div>':'';
    return '<div style="'+bg+br+'height:100%;padding:10px 14px;display:flex;flex-direction:column;justify-content:center">'+lblS+
      '<div class="song-title" style="'+fnt+'color:'+(p.textColor||'#e2e8f0')+';white-space:nowrap;overflow:hidden;text-overflow:ellipsis">Очередь пуста</div>'+
      '<div class="song-user" style="font-size:10px;color:'+ac4+';margin-top:2px"></div></div>';
  }
  if (t==='lastfollower'||t==='lastsub') {
    var ac5=p.accentColor||(t==='lastfollower'?'#ec4899':'#a855f7'), cls=t==='lastfollower'?'lastfollower-val':'lastsub-val';
    return '<div style="'+bg+br+'height:100%;padding:8px 12px;display:flex;align-items:center">'+
      '<span style="'+fnt+'color:'+(p.textColor||'#e2e8f0')+'"><span style="color:'+ac5+'">'+esc(p.prefix||(t==='lastfollower'?'Последний фолловер: ':'Последний саб: '))+'</span>'+
      '<span class="'+cls+'">Ожидание...</span></span></div>';
  }
  if (t==='eventlist') {
    return '<div style="'+bg+br+'height:100%;padding:10px;overflow:hidden">'+
      '<div class="eventlist-inner" data-max="'+(p.count||5)+'" style="'+fnt+'color:'+(p.textColor||'#e2e8f0')+'"></div></div>';
  }
  if (t==='rectangle') {
    var sk2 = p.strokeEnabled ? 'border:'+(p.strokeWidth||2)+'px solid '+(p.strokeColor||'#7c3aed')+';' : '';
    return '<div style="width:100%;height:100%;background:'+(p.fillColor||'rgba(124,58,237,.2)')+';'+br+sk2+'"></div>';
  }
  if (t==='video') {
    return '<div style="width:100%;height:100%;border-radius:'+(p.borderRadius||0)+'px;overflow:hidden">'+
      '<video src="'+esc(p.url||'')+'" style="width:100%;height:100%;object-fit:cover" '+(p.loop?'loop':'')+' '+(p.muted?'muted':'')+' autoplay></video></div>';
  }
  return '<div style="width:100%;height:100%;background:rgba(124,58,237,.1);border:1px dashed rgba(124,58,237,.3)"></div>';
}

// ── Alert system ───────────────────────────────────────────────────────────
var ACFG = {
  event_follow:   {icon:'❤️',title:'НОВЫЙ ФОЛЛОВЕР', name:function(d){return d.username||'';}, sub:function(){return '';}},
  event_sub:      {icon:'🎉',title:'ПОДПИСКА',        name:function(d){return d.username||'';}, sub:function(d){return d.is_resub?(d.months+' мес'):''}},
  event_giftsub:  {icon:'🎁',title:'GIFT SUB',         name:function(d){return d.gifter||'';},  sub:function(d){return d.count+' подписок'}},
  event_raid:     {icon:'🚨',title:'РЕЙД!',            name:function(d){return d.raider||'';},  sub:function(d){return d.viewers+' зрителей'}},
  event_bits:     {icon:'💎',title:'BITS',             name:function(d){return d.username||'';},sub:function(d){return d.amount+' bits'}},
  event_milestone:{icon:'🏆',title:'MILESTONE!',       name:function(d){return d.count+' фолловеров';},sub:function(){return ''}},
};
function queueAlert(msg){if(!ACFG[msg.event])return;alertQueue.push(msg);if(!alertShowing)showAlert();}
function showAlert(){
  if(!alertQueue.length){alertShowing=false;return;}
  alertShowing=true;
  var msg=alertQueue.shift(), cfg=ACFG[msg.event];
  document.querySelectorAll('.alert-box').forEach(function(box){
    box.querySelector('.a-icon').textContent=cfg.icon;
    box.querySelector('.a-title').textContent=cfg.title;
    box.querySelector('.a-name').textContent=cfg.name(msg);
    box.querySelector('.a-sub').textContent=cfg.sub(msg);
    spawnP(box);
    box.style.display='flex';
    box.style.animation='alertIn .4s cubic-bezier(0.34,1.56,0.64,1) forwards';
    setTimeout(function(){
      box.style.animation='alertOut .3s ease forwards';
      setTimeout(function(){box.style.display='none';alertShowing=false;showAlert();},300);
    }, parseInt(box.getAttribute('data-dur'))||4000);
  });
}
function spawnP(box){
  var pc=box.querySelector('.a-particles');if(!pc)return;pc.innerHTML='';
  var color=box.getAttribute('data-ac')||'#a855f7';
  for(var i=0;i<16;i++){
    var p=document.createElement('div');
    p.style.cssText='position:absolute;width:6px;height:6px;border-radius:50%;background:'+color+';left:'+Math.random()*100+'%;top:-8px;animation:particle '+(1+Math.random())+'s ease-in '+(Math.random()*.5)+'s forwards';
    pc.appendChild(p);
  }
}

// ── Live event handling ────────────────────────────────────────────────────
var evtItems=[];
function handleEvent(msg){
  // Chat
  if(msg.event==='chat_message'){
    document.querySelectorAll('.chat-inner').forEach(function(chat){
      var max=parseInt(chat.getAttribute('data-max')||30);
      var div=document.createElement('div');div.style.padding='2px 0';
      var nc=msg.is_vip?chat.getAttribute('data-vc')||'#f59e0b':msg.is_mod?chat.getAttribute('data-mc')||'#eab308':msg.is_sub?chat.getAttribute('data-sc')||'#22c55e':chat.getAttribute('data-uc')||'#a855f7';
      div.innerHTML='<span style="color:'+nc+';font-weight:700">'+esc(msg.username||'')+(msg.is_vip?'⭐':msg.is_mod?'🔧':'')+'</span>: <span>'+esc(msg.message||'')+'</span>';
      chat.appendChild(div);
      while(chat.children.length>max)chat.removeChild(chat.firstChild);
      chat.scrollTop=chat.scrollHeight;
    });
  }
  // Stats
  if(msg.event==='death_updated') qsa('.deaths-val',function(el){el.textContent=msg.count;});
  if(msg.event==='goal_updated'){
    var pct=msg.target?Math.min(100,Math.round(msg.current/msg.target*100)):0;
    qsa('.goal-fill',function(el){el.style.width=pct+'%';});
    qsa('.goal-cur',function(el){el.textContent=msg.current;});
    qsa('.goal-pct',function(el){el.textContent=pct+'%';});
    if(msg.label)qsa('.goal-lbl',function(el){el.textContent=msg.label;});
  }
  if(msg.event==='event_follow'){qsa('.lastfollower-val',function(el){el.textContent=msg.username||'';});}
  if(msg.event==='event_sub'){qsa('.lastsub-val',function(el){el.textContent=msg.username||'';});}
  if(msg.event==='song_requested'){
    qsa('.song-title',function(el){el.textContent=msg.song||'';});
    qsa('.song-user',function(el){el.textContent='от: '+(msg.username||'');});
  }
  // Eventlist
  var et=null;
  if(msg.event==='event_follow')et='Новый фолловер: '+(msg.username||'');
  if(msg.event==='event_sub')et='Подписка: '+(msg.username||'');
  if(msg.event==='event_raid')et='Рейд от '+(msg.raider||'')+' ('+(msg.viewers||0)+')';
  if(msg.event==='event_bits')et=(msg.username||'')+'  -  '+(msg.amount||0)+' bits';
  if(et){evtItems.unshift(et);if(evtItems.length>20)evtItems.pop();
    qsa('.eventlist-inner',function(el){
      var max=parseInt(el.getAttribute('data-max')||5);
      el.innerHTML=evtItems.slice(0,max).map(function(s){return '<div style="padding:3px 0;border-bottom:1px solid rgba(255,255,255,.05)">'+esc(s)+'</div>';}).join('');
    });
  }
  // Alerts
  if(ACFG[msg.event])queueAlert(msg);
}

// ── Clock + Timer tick ─────────────────────────────────────────────────────
setInterval(function(){
  var n=new Date();
  var hh=('0'+n.getHours()).slice(-2),mm=('0'+n.getMinutes()).slice(-2),ss=('0'+n.getSeconds()).slice(-2);
  qsa('.clock-val',function(el){el.textContent=el.getAttribute('data-secs')==='1'?hh+':'+mm+':'+ss:hh+':'+mm;});
  qsa('.timer-val',function(el){
    var tg=el.getAttribute('data-target');if(!tg)return;
    var d=Math.max(0,new Date(tg)-n);
    var th=Math.floor(d/3600000),tm=Math.floor((d%3600000)/60000),ts=Math.floor((d%60000)/1000);
    el.textContent=('0'+th).slice(-2)+':'+('0'+tm).slice(-2)+':'+('0'+ts).slice(-2);
  });
},1000);

// ── Uptime polling ─────────────────────────────────────────────────────────
function pollStats(){
  fetch('/api/stats').then(function(r){return r.json();}).then(function(d){
    qsa('.uptime-val',function(el){el.textContent=d.uptime||'--:--';});
    qsa('.deaths-val',function(el){el.textContent=d.death_count||0;});
    var pct=d.goal_target?Math.min(100,Math.round((d.goal_current||0)/d.goal_target*100)):0;
    qsa('.goal-fill',function(el){el.style.width=pct+'%';});
    qsa('.goal-cur',function(el){el.textContent=d.goal_current||0;});
    qsa('.goal-pct',function(el){el.textContent=pct+'%';});
    qsa('.goal-tgt',function(el){el.textContent=d.goal_target||100;});
    qsa('.goal-lbl',function(el){el.textContent=d.goal_label||'Цель';});
    if(d.song_queue&&d.song_queue.length){
      qsa('.song-title',function(el){el.textContent=d.song_queue[0].song||'';});
      qsa('.song-user',function(el){el.textContent='от: '+(d.song_queue[0].username||'');});
    }
    var tp=d.top_points||[];
    qsa('.toppoints-inner',function(el){
      var max=parseInt(el.getAttribute('data-max')||5),ac=el.getAttribute('data-ac')||'#a855f7';
      el.innerHTML=tp.slice(0,max).map(function(r,i){
        return '<div style="display:flex;gap:8px;margin:2px 0"><span>'+(i+1)+'.</span><span style="flex:1">'+(r.is_vip?'⭐':'')+esc(r.username||'')+'</span><span style="color:'+ac+'">'+(r.points||0)+'</span></div>';
      }).join('');
    });
  }).catch(function(){});
}
pollStats();
setInterval(pollStats, 30000);

// ── WebSocket ──────────────────────────────────────────────────────────────
function connectWS(){
  ws=new WebSocket('ws://'+location.host+'/ws');
  ws.onmessage=function(e){try{handleEvent(JSON.parse(e.data));}catch(ex){}};
  ws.onclose=function(){setTimeout(connectWS,3000);};
}

// ── Scene loader ───────────────────────────────────────────────────────────
async function loadScene(){
  try {
    var scenes = await fetch('/api/overlays').then(function(r){return r.json();});
    var scene  = scenes.find(function(s){return s.id===SCENE_ID;});
    if(!scene){
      document.body.innerHTML='<div style="color:red;font-family:monospace;padding:20px">Сцена не найдена: '+esc(SCENE_ID)+'</div>';
      return;
    }
    var root = document.getElementById('root');
    root.innerHTML='';
    (scene.widgets||[]).forEach(function(w){ root.appendChild(renderWidget(w)); });
    connectWS();
  } catch(err) {
    document.body.innerHTML='<div style="color:red;font-family:monospace;padding:20px">Ошибка загрузки: '+esc(String(err))+'</div>';
  }
}

function qsa(sel,fn){document.querySelectorAll(sel).forEach(fn);}
function esc(s){var d=document.createElement('div');d.textContent=String(s||'');return d.innerHTML;}

loadScene();
</script>
</body>
</html>"""

@app.get("/overlay/scene/{scene_id}", response_class=HTMLResponse)
async def render_scene(scene_id: str):
    """
    Отдаёт чистую HTML-страницу для OBS Browser Source.
    Страница сама загружает сцену через /api/overlays и рендерит её
    тем же JS-рендерером что и редактор  -  гарантированное совпадение.
    """
    page = OBS_OVERLAY_PAGE.replace("'{{SCENE_ID}}'", json.dumps(scene_id))
    return HTMLResponse(page)



# ─── FastAPI ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    await twitch_client.start()
    asyncio.create_task(watch_time_task())
    asyncio.create_task(autopost_task())
    yield
    await twitch_client.stop()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

FRONTEND = Path(__file__).parent / "frontend"

@app.get("/")
async def root():
    p = FRONTEND / "index.html"
    return FileResponse(p) if p.exists() else {"status": "Place index.html in ./frontend/"}

if (FRONTEND / "static").exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND / "static")), name="static")

@app.get("/editor", response_class=HTMLResponse)
async def editor():
    p = FRONTEND / "overlay_editor.html"
    return HTMLResponse(p.read_text(encoding="utf-8")) if p.exists() \
        else HTMLResponse("<h1>Положи overlay_editor.html в frontend/</h1>")

# ── API ────────────────────────────────────────────────────────────────────────

class SettingsPayload(BaseModel):
    settings: dict

@app.get("/api/settings")
async def api_get_settings():
    conn = get_db()
    try:
        return {r["key"]: r["value"] for r in conn.execute("SELECT key,value FROM settings")}
    finally:
        conn.close()

@app.post("/api/settings")
async def api_save_settings(payload: SettingsPayload):
    for k, v in payload.settings.items():
        set_setting(str(k)[:100], str(v)[:2000])
    return {"ok": True}

@app.get("/api/stats")
async def api_stats():
    top = get_top_points(10)
    conn = get_db()
    try:
        clips      = conn.execute("SELECT * FROM clips ORDER BY ts DESC LIMIT 20").fetchall()
        warns      = conn.execute("SELECT username, COUNT(*) AS c FROM warn_log GROUP BY username ORDER BY c DESC LIMIT 10").fetchall()
        chat_count = conn.execute("SELECT COUNT(*) AS c FROM chat_log").fetchone()["c"]
        songs      = conn.execute("SELECT * FROM song_queue WHERE done=0 ORDER BY ts ASC LIMIT 10").fetchall()
    finally:
        conn.close()
    uptime = None
    if state.stream_start:
        e = int(time.time() - state.stream_start)
        h, m = divmod(e // 60, 60)
        uptime = f"{h}ч {m}м"
    return {
        "top_points": top, "clips": [dict(c) for c in clips],
        "warnings": [dict(w) for w in warns], "chat_count": state.stream_chat_count,
        "uptime": uptime, "stream_active": state.stream_start is not None,
        "death_count": safe_int(get_setting("death_count","0")),
        "goal_current": safe_int(get_setting("goal_current","0")),
        "goal_target": max(1, safe_int(get_setting("goal_target","100"))),
        "goal_label": get_setting("goal_label","Подписчики"),
        "twitch_connected": twitch_client.is_connected,
        "current_game": get_setting("current_game",""),
        "song_queue": [dict(s) for s in songs],
        "stream_chat_count": state.stream_chat_count,
    }

@app.post("/api/stream/start")
async def stream_start():
    state.stream_start      = time.time()
    state.seen_this_stream  = set()
    state.stream_hello_sent = False
    state.stream_chat_count = 0
    await broadcast("stream_started", {"ts": state.stream_start})
    asyncio.create_task(on_stream_start_hello())
    return {"ok": True}

@app.post("/api/stream/stop")
async def stream_stop():
    state.stream_start = None
    asyncio.create_task(on_stream_stop_bye())
    await broadcast("stream_stopped", {})
    return {"ok": True}

@app.post("/api/clip")
async def manual_clip(label: str = "Ручной клип"):
    dur = safe_int(get_setting("clip_duration","10"), 10)
    ts  = time.time()
    conn = get_db()
    try:
        conn.execute("INSERT INTO clips (ts, label, duration) VALUES (?,?,?)",
                     (ts, label[:100], dur))
        conn.commit()
    finally:
        conn.close()
    await broadcast("clip_created", {"label": label, "duration": dur, "ts": ts, "by": "dashboard"})
    return {"ok": True}

@app.post("/api/death")
async def increment_death():
    dc = safe_int(get_setting("death_count","0")) + 1
    set_setting("death_count", str(dc))
    await broadcast("death_updated", {"count": dc})
    return {"count": dc}

@app.post("/api/death/reset")
async def reset_death():
    set_setting("death_count","0")
    await broadcast("death_updated", {"count": 0})
    return {"count": 0}

@app.post("/api/goal")
async def update_goal(current: int = 0, target: int = 100, label: str = "Подписчики"):
    target = max(1, target)
    set_setting("goal_current", str(current))
    set_setting("goal_target",  str(target))
    set_setting("goal_label",   label[:100])
    await broadcast("goal_updated", {"current": current, "target": target, "label": label})
    return {"ok": True}

@app.post("/api/chat/simulate")
async def simulate_chat(username: str, message: str,
                        is_mod: bool = False, is_sub: bool = False):
    await process_message(username[:50], message[:500], is_mod, is_sub)
    return {"ok": True}

@app.post("/api/chat/broadcast")
async def broadcast_msg(message: str):
    await send_bot_message(message[:500])
    return {"ok": True}

@app.post("/api/send_message")
async def send_to_twitch(message: str):
    if not twitch_client.is_connected:
        return {"ok": False, "error": "Не подключён к IRC"}
    await twitch_client.send_message(message[:500])
    await broadcast("bot_message", {"message": message[:500], "ts": time.time()})
    return {"ok": True}

@app.get("/api/twitch/status")
async def twitch_status():
    return {"connected": twitch_client.is_connected,
            "channel":   get_setting("twitch_channel"),
            "nick":      get_setting("bot_username")}

@app.get("/api/chat/history")
async def chat_history():
    return {"messages": state.chat_history[-100:]}

@app.get("/api/points/{username}")
async def api_get_points(username: str):
    return {"username": username, "points": get_points(username), "is_vip": is_vip(username)}

@app.post("/api/points/{username}/add")
async def api_add_points(username: str, amount: int):
    add_points(username, amount)
    return {"username": username, "points": get_points(username)}

@app.delete("/api/warnings/{username}")
async def api_clear_warnings(username: str):
    conn = get_db()
    try:
        conn.execute("DELETE FROM warn_log WHERE username=?", (username,))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}

# Trivia settings
@app.get("/api/trivia/questions")
async def api_get_trivia_questions():
    raw = get_setting("trivia_questions", "[]")
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return []

@app.post("/api/trivia/questions")
async def api_save_trivia_questions(request: Request):
    try:
        data = await request.json()
        questions = data if isinstance(data, list) else []
    except Exception:
        questions = []
    set_setting("trivia_questions", json.dumps(questions, ensure_ascii=False))
    return {"ok": True}

@app.post("/api/trivia/start")
async def api_start_trivia():
    asyncio.create_task(cmd_trivia())
    return {"ok": True}

# VIP
@app.post("/api/vip/{username}/grant")
async def api_grant_vip(username: str, hours: int = 24):
    grant_vip(username, max(1, min(720, hours)))
    await broadcast("vip_granted", {"username": username})
    return {"ok": True}

@app.post("/api/vip/{username}/revoke")
async def api_revoke_vip(username: str):
    revoke_vip(username)
    return {"ok": True}

@app.get("/api/vip/list")
async def api_vip_list():
    return get_all_vips()

# Custom commands
@app.get("/api/custom_commands")
async def api_get_cmds():
    conn = get_db()
    try:
        return [dict(r) for r in conn.execute("SELECT cmd,response,enabled FROM custom_commands ORDER BY cmd")]
    finally:
        conn.close()

@app.post("/api/custom_commands")
async def api_create_cmd(cmd: str, response: str):
    cmd = cmd.lower().lstrip("!")[:30]
    if not re.match(r"^[a-z0-9_]+$", cmd):
        return {"ok": False, "error": "Только латиница, цифры, _"}
    conn = get_db()
    try:
        conn.execute("INSERT OR REPLACE INTO custom_commands (cmd,response,enabled) VALUES (?,?,1)",
                     (cmd, response[:500]))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}

@app.delete("/api/custom_commands/{cmd}")
async def api_delete_cmd(cmd: str):
    conn = get_db()
    try:
        conn.execute("DELETE FROM custom_commands WHERE cmd=?", (cmd,))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}

@app.post("/api/warnings/{username}/add")
async def api_add_warning(username: str, reason: str = "Ручной варн"):
    warns = add_warning(username, reason)
    await handle_punishment(username, warns, reason)
    return {"ok": True, "warn_count": warns}


@app.post("/api/autopost/links")
async def api_save_autopost_links(request: Request):
    try:
        data = await request.json()
        links = data if isinstance(data, list) else []
    except Exception:
        links = []
    set_setting("autopost_links", json.dumps(links, ensure_ascii=False))
    return {"ok": True}

@app.get("/api/autopost/links")
async def api_get_autopost_links():
    raw = get_setting("autopost_links", "[]")
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return []

# Songs
@app.get("/api/songs")
async def api_get_songs():
    conn = get_db()
    try:
        return [dict(r) for r in conn.execute("SELECT * FROM song_queue WHERE done=0 ORDER BY ts ASC")]
    finally:
        conn.close()

@app.post("/api/songs/{sid}/done")
async def api_song_done(sid: int):
    conn = get_db()
    try:
        conn.execute("UPDATE song_queue SET done=1 WHERE id=?", (sid,))
        conn.commit()
    finally:
        conn.close()
    await broadcast("song_done", {"id": sid})
    return {"ok": True}

@app.delete("/api/songs/{sid}")
async def api_delete_song(sid: int):
    conn = get_db()
    try:
        conn.execute("DELETE FROM song_queue WHERE id=?", (sid,))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}

# Overlays
@app.get("/api/overlays")
async def api_get_overlays():
    v = get_setting("overlay_scenes","")
    try:
        return json.loads(v) if v else []
    except (json.JSONDecodeError, ValueError):
        return []

@app.post("/api/overlays")
async def api_save_overlays(request: Request):
    try:
        data = await request.json()
    except Exception:
        return {"ok": False, "error": "invalid json"}
    if isinstance(data, list):
        scenes = data
    elif isinstance(data, dict):
        scenes = data.get("scenes", [])
    else:
        scenes = []
    set_setting("overlay_scenes", json.dumps(scenes, ensure_ascii=False))
    return {"ok": True, "count": len(scenes)}


@app.post("/api/event/follow")
async def sim_follow(username: str):
    asyncio.create_task(on_follow(username)); return {"ok": True}
@app.post("/api/event/sub")
async def sim_sub(username: str, months: int = 1, resub: bool = False):
    asyncio.create_task(on_sub(username, months, resub)); return {"ok": True}
@app.post("/api/event/giftsub")
async def sim_giftsub(gifter: str, count: int = 1):
    asyncio.create_task(on_giftsub(gifter, count)); return {"ok": True}
@app.post("/api/event/raid")
async def sim_raid(raider: str, viewers: int = 10):
    asyncio.create_task(on_raid(raider, viewers)); return {"ok": True}
@app.post("/api/event/bits")
async def sim_bits(username: str, amount: int = 100):
    asyncio.create_task(on_bits(username, amount)); return {"ok": True}
@app.post("/api/event/milestone")
async def sim_milestone(count: int):
    asyncio.create_task(check_milestone(count)); return {"ok": True}

# WebSocket
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    state.ws_clients.append(ws)
    try:
        conn = get_db()
        try:
            settings_data = {r["key"]: r["value"] for r in conn.execute("SELECT key,value FROM settings")}
        finally:
            conn.close()
        await ws.send_text(json.dumps({
            "event": "init",
            "settings": settings_data,
            "history": state.chat_history[-50:],
            "twitch_connected": twitch_client.is_connected,
        }, ensure_ascii=False))
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                continue
            if msg.get("type") == "chat":
                uname = str(msg.get("username",""))[:50]
                text  = str(msg.get("message",""))[:500]
                is_mod = bool(msg.get("is_mod", False))
                if uname and text:
                    await process_message(uname, text, is_mod)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[WS] {type(e).__name__}: {e}")
    finally:
        try:
            state.ws_clients.remove(ws)
        except ValueError:
            pass

if __name__ == "__main__":
    print("Twitch Bot v3.0")
    print("Панель:   http://localhost:8000")
    print("Редактор: http://localhost:8000/editor")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
