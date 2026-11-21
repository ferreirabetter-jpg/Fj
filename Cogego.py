#!/usr/bin/env python3
"""
Prosa — um clone completo do Discord, sem bots.

Tudo é criado pelos próprios usuários: contas, servidores, canais de texto e
de voz, convites, cargos, mensagens, reações, arquivos, amigos e DMs.

Backend: Flask + SQLite. Tempo real via Server-Sent Events (SSE).
Voz: WebRTC (malha P2P); o servidor só faz a sinalização.

Arquivo único: rode (F5 no IDLE) e o navegador abre sozinho.
Requer:  pip install flask
"""
import argparse
import mimetypes
import os
import queue
import re
import secrets
import sqlite3
import threading
import time
import webbrowser
import uuid
from functools import wraps

from flask import (Flask, Response, abort, g, jsonify, request,
                   send_from_directory)
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.environ.get("PROSA_DATA", os.path.join(BASE, "data"))
UPLOADS = os.path.join(DATA, "uploads")
DB_PATH = os.path.join(DATA, "prosa.db")
os.makedirs(UPLOADS, exist_ok=True)

app = Flask(__name__, static_folder=None)
app.config["MAX_CONTENT_LENGTH"] = 105 * 1024 * 1024  # teto por requisição (Nitro: 100 MB por arquivo)
SESSION_DAYS = 30
RANK = {"member": 0, "admin": 1, "owner": 2}

# ----------------------------------------------------- configuração do irisk
# Mude os valores abaixo para ajustar a economia do site.
TRAILER_REWARD = 200          # irisk por trailer assistido
GAME_REWARD = 500             # irisk por 15 min de jogo
GAME_SECONDS = 15 * 60        # tempo de jogo exigido
GAME_DAILY_LIMIT = 4          # máx. de recompensas de jogo por 24 h
TRAILER_COOLDOWN_H = 24       # cada trailer só rende de novo após X horas
FRAME_PRICE = 500             # preço de cada moldura
NITRO_PRICE = 1000            # preço do Nitro
NITRO_DAYS = 3                # duração do Nitro
NITRO_FRAME = "prisma"        # moldura exclusiva de quem tem Nitro ativo
FRAMES = {"ouro": "Dourada", "neon": "Neon", "fogo": "Fogo", "aurora": "Aurora"}
GAMES = ["cobrinha", "memoria", "alvos"]   # minigames embutidos (sorteados)
# Donos do site: irisk infinito, Nitro permanente, todas as molduras liberadas
# e painel para dar irisk a outras pessoas. Coloque aqui o SEU nome de usuário
# (sem @), por exemplo: SITE_OWNERS = {"gabriel"}. Também vale a variável de
# ambiente PROSA_OWNER (separe vários por vírgula).
SITE_OWNERS = {"FjDono"}
SITE_OWNERS |= {x.strip() for x in os.environ.get("PROSA_OWNER", "").split(",") if x.strip()}
SITE_OWNERS = {x.lower() for x in SITE_OWNERS}
# Trailers dos patrocinadores. Para adicionar um, copie uma linha e troque os
# campos. "youtube" é o código depois de "v=" na URL do vídeo; "seconds" é
# quanto tempo a pessoa precisa assistir para resgatar.
TRAILERS = [
    {"id": "hades2", "sponsor": "Supergiant Games", "title": "Hades II — trailer",
     "youtube": "WzJ_UhPptBQ", "seconds": 60},
]
COLORS = ["#e0556b", "#d9822b", "#c9a227", "#4fa86b", "#2f9e9a",
          "#3f7fd9", "#7a5fd1", "#b04fc4", "#c4507f", "#6b7a8f"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS users(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT NOT NULL UNIQUE COLLATE NOCASE,
  display_name TEXT NOT NULL DEFAULT '',
  password_hash TEXT NOT NULL,
  avatar TEXT,
  bio TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'online',
  custom_status TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions(
  token TEXT PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS servers(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  owner_id INTEGER NOT NULL REFERENCES users(id),
  invite_code TEXT NOT NULL UNIQUE,
  color TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS members(
  server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  role TEXT NOT NULL DEFAULT 'member',
  nickname TEXT NOT NULL DEFAULT '',
  joined_at INTEGER NOT NULL,
  PRIMARY KEY(server_id, user_id)
);
CREATE TABLE IF NOT EXISTS bans(
  server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  PRIMARY KEY(server_id, user_id)
);
CREATE TABLE IF NOT EXISTS channels(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  server_id INTEGER REFERENCES servers(id) ON DELETE CASCADE,
  name TEXT NOT NULL DEFAULT '',
  kind TEXT NOT NULL,
  topic TEXT NOT NULL DEFAULT '',
  position INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS dm_members(
  channel_id INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  PRIMARY KEY(channel_id, user_id)
);
CREATE TABLE IF NOT EXISTS messages(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel_id INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
  author_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  content TEXT NOT NULL DEFAULT '',
  reply_to INTEGER REFERENCES messages(id) ON DELETE SET NULL,
  created_at INTEGER NOT NULL,
  edited_at INTEGER,
  pinned INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel_id, id);
CREATE TABLE IF NOT EXISTS attachments(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  filename TEXT NOT NULL,
  path TEXT NOT NULL,
  mime TEXT NOT NULL,
  size INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS reactions(
  message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  emoji TEXT NOT NULL,
  PRIMARY KEY(message_id, user_id, emoji)
);
CREATE TABLE IF NOT EXISTS friendships(
  requester_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  addressee_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  status TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY(requester_id, addressee_id)
);
CREATE TABLE IF NOT EXISTS wallets(
  user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  balance INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS coin_log(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  amount INTEGER NOT NULL,
  reason TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS perks(
  user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  frame TEXT NOT NULL DEFAULT '',
  nitro_until INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS owned_frames(
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  frame TEXT NOT NULL,
  PRIMARY KEY(user_id, frame)
);
CREATE TABLE IF NOT EXISTS earn_sessions(
  token TEXT PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  kind TEXT NOT NULL,
  ref TEXT NOT NULL,
  need INTEGER NOT NULL,
  done REAL NOT NULL DEFAULT 0,
  reward INTEGER NOT NULL,
  last_beat INTEGER NOT NULL,
  started_at INTEGER NOT NULL,
  claimed INTEGER NOT NULL DEFAULT 0,
  claimed_at INTEGER
);
CREATE TABLE IF NOT EXISTS reads(
  channel_id INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  last_message_id INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(channel_id, user_id)
);
"""


# ---------------------------------------------------------------- banco
def connect():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys=ON")
    return c


def db():
    if "db" not in g:
        g.db = connect()
    return g.db


@app.teardown_appcontext
def close_db(_exc):
    c = g.pop("db", None)
    if c is not None:
        c.close()


def init_db():
    c = connect()
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript(SCHEMA)
    c.execute("DELETE FROM sessions WHERE created_at < ?",
              (now() - SESSION_DAYS * 86400 * 1000,))
    c.commit()
    c.close()


def now():
    return int(time.time() * 1000)


# ------------------------------------------------------------ tempo real
class Hub:
    """Distribui eventos para as conexões SSE abertas de cada usuário."""

    def __init__(self):
        self.lock = threading.Lock()
        self.conns = {}

    def add(self, uid):
        q = queue.Queue(maxsize=1000)
        with self.lock:
            first = not self.conns.get(uid)
            self.conns.setdefault(uid, set()).add(q)
        return q, first

    def remove(self, uid, q):
        with self.lock:
            s = self.conns.get(uid)
            if s is not None:
                s.discard(q)
                if not s:
                    self.conns.pop(uid, None)
                    return True
        return False

    def online(self, uid):
        return bool(self.conns.get(uid))

    def send(self, uids, event, data):
        with self.lock:
            targets = [q for u in set(uids) for q in self.conns.get(u, ())]
        for q in targets:
            try:
                q.put_nowait((event, data))
            except queue.Full:
                pass


hub = Hub()
voice = {}  # channel_id -> {user_id: {"muted": bool, "deafened": bool}}
voice_lock = threading.Lock()
_rl = {}
_rl_lock = threading.Lock()


def rate(key, limit, window):
    t = time.time()
    with _rl_lock:
        lst = [x for x in _rl.get(key, []) if t - x < window]
        if len(lst) >= limit:
            _rl[key] = lst
            return False
        lst.append(t)
        _rl[key] = lst
        return True


# ------------------------------------------------------------- utilidades
def err(msg, code=400):
    return jsonify({"error": msg}), code


def body():
    d = request.get_json(silent=True)
    return d if isinstance(d, dict) else {}


def current_user():
    if "user" in g.__dict__:
        return g.user
    g.user = None
    tok = request.cookies.get("token")
    if tok:
        g.user = db().execute(
            "SELECT u.* FROM sessions s JOIN users u ON u.id=s.user_id "
            "WHERE s.token=? AND s.created_at>?",
            (tok, now() - SESSION_DAYS * 86400 * 1000)).fetchone()
    return g.user


def login_required(f):
    @wraps(f)
    def wrapper(*a, **k):
        if not current_user():
            return err("Você precisa entrar.", 401)
        return f(*a, **k)
    return wrapper


def eff_status(u):
    if not hub.online(u["id"]) or u["status"] == "invisible":
        return "offline"
    return u["status"]


def pub_user(u):
    p = db().execute("SELECT frame, nitro_until FROM perks WHERE user_id=?",
                     (u["id"],)).fetchone()
    frame, nitro = (p["frame"], p["nitro_until"]) if p else ("", 0)
    owner = u["username"].lower() in SITE_OWNERS
    if owner:
        nitro = max(nitro, now() + 3153600000000)  # ~100 anos
    if frame == NITRO_FRAME and nitro <= now():
        frame = ""
    return {
        "site_owner": owner,
        "id": u["id"], "username": u["username"],
        "display_name": u["display_name"] or u["username"],
        "avatar": f"/uploads/{u['avatar']}" if u["avatar"] else None,
        "bio": u["bio"], "status": eff_status(u),
        "custom_status": u["custom_status"], "created_at": u["created_at"],
        "frame": frame, "nitro_until": nitro,
    }


def me_user(u):
    d = pub_user(u)
    d["raw_status"] = u["status"]
    return d


def get_user(uid):
    return db().execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()


def member_row(sid, uid):
    return db().execute("SELECT * FROM members WHERE server_id=? AND user_id=?",
                        (sid, uid)).fetchone()


def need_member(sid, uid, min_role="member"):
    m = member_row(sid, uid)
    if not m or RANK[m["role"]] < RANK[min_role]:
        return None
    return m


def server_member_ids(sid):
    return [r[0] for r in db().execute(
        "SELECT user_id FROM members WHERE server_id=?", (sid,))]


def chan_dict(c):
    return {"id": c["id"], "server_id": c["server_id"], "name": c["name"],
            "kind": c["kind"], "topic": c["topic"], "position": c["position"]}


def access(cid, uid):
    ch = db().execute("SELECT * FROM channels WHERE id=?", (cid,)).fetchone()
    if not ch:
        return None
    if ch["kind"] == "dm":
        ok = db().execute(
            "SELECT 1 FROM dm_members WHERE channel_id=? AND user_id=?",
            (cid, uid)).fetchone()
    else:
        ok = member_row(ch["server_id"], uid)
    return ch if ok else None


def audience(ch):
    if ch["kind"] == "dm":
        return [r[0] for r in db().execute(
            "SELECT user_id FROM dm_members WHERE channel_id=?", (ch["id"],))]
    return server_member_ids(ch["server_id"])


def related_user_ids(uid):
    rows = db().execute(
        """SELECT m2.user_id FROM members m1 JOIN members m2
             ON m1.server_id=m2.server_id WHERE m1.user_id=?
           UNION SELECT addressee_id FROM friendships WHERE requester_id=?
           UNION SELECT requester_id FROM friendships WHERE addressee_id=?
           UNION SELECT d2.user_id FROM dm_members d1 JOIN dm_members d2
             ON d1.channel_id=d2.channel_id WHERE d1.user_id=?""",
        (uid, uid, uid, uid)).fetchall()
    return [r[0] for r in rows]


def announce_presence(uid):
    u = get_user(uid)
    if u:
        hub.send(related_user_ids(uid) + [uid], "presence",
                 {"user_id": uid, "status": eff_status(u)})


def slug_channel(name):
    n = name.strip().lower().replace(" ", "-")
    n = re.sub(r"[^a-z0-9\-_áàâãéêíóôõúç]", "", n)
    return re.sub(r"-{2,}", "-", n).strip("-")[:32]


def server_payload(sid, uid):
    d = db()
    s = d.execute("SELECT * FROM servers WHERE id=?", (sid,)).fetchone()
    m = member_row(sid, uid)
    chans = [chan_dict(c) for c in d.execute(
        "SELECT * FROM channels WHERE server_id=? ORDER BY "
        "CASE kind WHEN 'text' THEN 0 ELSE 1 END, position, id", (sid,))]
    cnt = d.execute("SELECT COUNT(*) FROM members WHERE server_id=?",
                    (sid,)).fetchone()[0]
    return {"id": s["id"], "name": s["name"], "color": s["color"],
            "owner_id": s["owner_id"], "invite_code": s["invite_code"],
            "my_role": m["role"], "member_count": cnt, "channels": chans}


def dm_payload(cid, uid):
    other = db().execute(
        "SELECT u.* FROM dm_members d JOIN users u ON u.id=d.user_id "
        "WHERE d.channel_id=? AND d.user_id!=?", (cid, uid)).fetchone()
    last = db().execute("SELECT MAX(id) FROM messages WHERE channel_id=?",
                        (cid,)).fetchone()[0]
    return {"id": cid, "kind": "dm", "user": pub_user(other),
            "last_message_id": last or 0}


def dms_for(uid):
    ids = [r[0] for r in db().execute(
        "SELECT c.id FROM channels c JOIN dm_members d ON d.channel_id=c.id "
        "WHERE d.user_id=? AND c.kind='dm'", (uid,))]
    out = [dm_payload(i, uid) for i in ids]
    out.sort(key=lambda x: x["last_message_id"], reverse=True)
    return out


def friends_for(uid):
    out = []
    for r in db().execute(
            "SELECT * FROM friendships WHERE requester_id=? OR addressee_id=?",
            (uid, uid)):
        other = r["addressee_id"] if r["requester_id"] == uid else r["requester_id"]
        u = get_user(other)
        if not u:
            continue
        if r["status"] == "accepted":
            rel = "friend"
        else:
            rel = "outgoing" if r["requester_id"] == uid else "incoming"
        out.append({"user": pub_user(u), "relation": rel})
    return out


def unread_for(uid, uname):
    rows = db().execute(
        """SELECT c.id AS cid, c.kind AS kind,
             (SELECT COUNT(*) FROM messages m WHERE m.channel_id=c.id
                AND m.id>COALESCE(r.last_message_id,0) AND m.author_id!=:uid) AS cnt,
             (SELECT COUNT(*) FROM messages m WHERE m.channel_id=c.id
                AND m.id>COALESCE(r.last_message_id,0) AND m.author_id!=:uid
                AND (instr(lower(m.content), :mention)>0
                     OR instr(lower(m.content), '@everyone')>0)) AS men
           FROM channels c
           LEFT JOIN reads r ON r.channel_id=c.id AND r.user_id=:uid
           WHERE (c.kind='text' AND c.server_id IN
                    (SELECT server_id FROM members WHERE user_id=:uid))
              OR (c.kind='dm' AND c.id IN
                    (SELECT channel_id FROM dm_members WHERE user_id=:uid))""",
        {"uid": uid, "mention": "@" + uname.lower()}).fetchall()
    out = {}
    for r in rows:
        if r["cnt"] > 0:
            out[r["cid"]] = {"count": r["cnt"],
                             "mentions": r["cnt"] if r["kind"] == "dm" else r["men"]}
    return out


def hydrate(rows, server_id=None):
    rows = list(rows)
    if not rows:
        return []
    d = db()
    ids = [r["id"] for r in rows]
    ph = ",".join("?" * len(ids))
    atts, reacts = {}, {}
    for a in d.execute(f"SELECT * FROM attachments WHERE message_id IN ({ph})", ids):
        atts.setdefault(a["message_id"], []).append({
            "id": a["id"], "filename": a["filename"], "mime": a["mime"],
            "url": f"/uploads/{a['path']}", "size": a["size"]})
    for r in d.execute(
            f"SELECT message_id, emoji, user_id FROM reactions "
            f"WHERE message_id IN ({ph}) ORDER BY rowid", ids):
        reacts.setdefault(r["message_id"], {}).setdefault(
            r["emoji"], []).append(r["user_id"])
    reply_ids = list({r["reply_to"] for r in rows if r["reply_to"]})
    replies = {}
    if reply_ids:
        rp = ",".join("?" * len(reply_ids))
        for r in d.execute(
                f"SELECT id, author_id, content FROM messages WHERE id IN ({rp})",
                reply_ids):
            replies[r["id"]] = r
    uids = list({r["author_id"] for r in rows} |
                {r["author_id"] for r in replies.values()})
    up = ",".join("?" * len(uids))
    users = {u["id"]: u for u in d.execute(
        f"SELECT * FROM users WHERE id IN ({up})", uids)}
    nicks = {}
    if server_id:
        for m in d.execute(
                f"SELECT user_id, nickname FROM members "
                f"WHERE server_id=? AND user_id IN ({up})", [server_id] + uids):
            nicks[m["user_id"]] = m["nickname"]

    def name(uid):
        u = users.get(uid)
        if not u:
            return "Usuário removido"
        return nicks.get(uid) or u["display_name"] or u["username"]

    out = []
    for r in rows:
        u = users.get(r["author_id"])
        rep = None
        rr = replies.get(r["reply_to"]) if r["reply_to"] else None
        if rr:
            rep = {"id": rr["id"], "author": name(rr["author_id"]),
                   "author_id": rr["author_id"],
                   "content": rr["content"][:120] or "Anexo"}
        out.append({
            "id": r["id"], "channel_id": r["channel_id"],
            "author": pub_user(u) if u else None,
            "nick": nicks.get(r["author_id"], ""),
            "content": r["content"], "reply_to": rep,
            "created_at": r["created_at"], "edited_at": r["edited_at"],
            "pinned": bool(r["pinned"]),
            "attachments": atts.get(r["id"], []),
            "reactions": [{"emoji": e, "users": us}
                          for e, us in reacts.get(r["id"], {}).items()],
        })
    return out


def one_message(mid):
    r = db().execute("SELECT * FROM messages WHERE id=?", (mid,)).fetchone()
    ch = db().execute("SELECT server_id FROM channels WHERE id=?",
                      (r["channel_id"],)).fetchone()
    return hydrate([r], ch["server_id"])[0]


def save_upload(f):
    name = secure_filename(f.filename or "") or "arquivo"
    ext = os.path.splitext(name)[1][:12]
    stored = uuid.uuid4().hex + ext
    path = os.path.join(UPLOADS, stored)
    f.save(path)
    mime = mimetypes.guess_type(name)[0] or "application/octet-stream"
    return name, stored, mime, os.path.getsize(path)


def remove_files(paths):
    for p in paths:
        try:
            os.remove(os.path.join(UPLOADS, p))
        except OSError:
            pass


def files_in_channels(cids):
    if not cids:
        return []
    ph = ",".join("?" * len(cids))
    return [r[0] for r in db().execute(
        f"SELECT a.path FROM attachments a JOIN messages m ON m.id=a.message_id "
        f"WHERE m.channel_id IN ({ph})", cids)]


# ------------------------------------------------------------------ voz
def voice_snapshot(cid):
    return [{"user_id": u, **st} for u, st in voice.get(cid, {}).items()]


def broadcast_voice(cid):
    ch = db().execute("SELECT * FROM channels WHERE id=?", (cid,)).fetchone()
    if not ch:
        return
    with voice_lock:
        snap = voice_snapshot(cid)
    hub.send(server_member_ids(ch["server_id"]), "voice_state",
             {"channel_id": cid, "server_id": ch["server_id"], "users": snap})


def voice_leave(uid):
    changed = []
    with voice_lock:
        for cid, users in list(voice.items()):
            if uid in users:
                users.pop(uid)
                if not users:
                    voice.pop(cid)
                changed.append(cid)
    for cid in changed:
        broadcast_voice(cid)


def voice_clear_channels(cids):
    with voice_lock:
        for c in cids:
            voice.pop(c, None)


def voice_channel_of(uid):
    with voice_lock:
        for cid, users in voice.items():
            if uid in users:
                return cid
    return None


# ------------------------------------------------------------------ irisk
def ensure_wallet(uid):
    db().execute("INSERT OR IGNORE INTO wallets(user_id,balance) VALUES(?,0)", (uid,))
    db().execute("INSERT OR IGNORE INTO perks(user_id) VALUES(?)", (uid,))


def balance_of(uid):
    ensure_wallet(uid)
    return db().execute("SELECT balance FROM wallets WHERE user_id=?", (uid,)).fetchone()[0]


def is_owner(uid):
    r = db().execute("SELECT username FROM users WHERE id=?", (uid,)).fetchone()
    return bool(r and r[0].lower() in SITE_OWNERS)


def nitro_on(uid):
    if is_owner(uid):
        return True
    r = db().execute("SELECT nitro_until FROM perks WHERE user_id=?", (uid,)).fetchone()
    return bool(r and r[0] > now())


def wallet_payload(uid):
    ensure_wallet(uid)
    db().commit()
    bal = db().execute("SELECT balance FROM wallets WHERE user_id=?", (uid,)).fetchone()[0]
    p = db().execute("SELECT frame, nitro_until FROM perks WHERE user_id=?", (uid,)).fetchone()
    owner = is_owner(uid)
    nitro = max(p["nitro_until"], now() + 3153600000000) if owner else p["nitro_until"]
    frame = p["frame"]
    if frame == NITRO_FRAME and nitro <= now():
        frame = ""
    return {"balance": bal, "frame": frame, "nitro_until": nitro, "infinite": owner}


def credit(uid, amount, reason):
    ensure_wallet(uid)
    db().execute("UPDATE wallets SET balance=balance+? WHERE user_id=?", (amount, uid))
    db().execute("INSERT INTO coin_log(user_id,amount,reason,created_at) VALUES(?,?,?,?)",
                 (uid, amount, reason, now()))
    db().commit()


def spend(uid, amount, reason):
    ensure_wallet(uid)
    if is_owner(uid):  # dono não gasta nada
        return True
    cur = db().execute("UPDATE wallets SET balance=balance-? WHERE user_id=? AND balance>=?",
                       (amount, uid, amount))
    if not cur.rowcount:
        db().rollback()
        return False
    db().execute("INSERT INTO coin_log(user_id,amount,reason,created_at) VALUES(?,?,?,?)",
                 (uid, -amount, reason, now()))
    db().commit()
    return True


def push_wallet(uid):
    hub.send([uid], "wallet", wallet_payload(uid))
    broadcast_user(uid)


def game_plays_24h(uid):
    return db().execute(
        "SELECT COUNT(*) FROM earn_sessions WHERE user_id=? AND kind='game' AND claimed=1 "
        "AND claimed_at>?", (uid, now() - 86400000)).fetchone()[0]


def trailer_available_at(uid, tid):
    last = db().execute(
        "SELECT MAX(claimed_at) FROM earn_sessions WHERE user_id=? AND kind='trailer' "
        "AND ref=? AND claimed=1", (uid, tid)).fetchone()[0]
    at = (last or 0) + TRAILER_COOLDOWN_H * 3600 * 1000
    return at if last and at > now() else 0


def irisk_state(uid):
    d = db()
    w = wallet_payload(uid)
    owned = {r[0] for r in d.execute("SELECT frame FROM owned_frames WHERE user_id=?", (uid,))}
    owner = is_owner(uid)
    frames = [{"id": k, "name": v, "price": FRAME_PRICE, "owned": owner or k in owned,
               "equipped": w["frame"] == k} for k, v in FRAMES.items()]
    log = [{"amount": r["amount"], "reason": r["reason"], "at": r["created_at"]} for r in d.execute(
        "SELECT * FROM coin_log WHERE user_id=? ORDER BY id DESC LIMIT 25", (uid,))]
    return {
        "wallet": w,
        "trailers": [{**t, "reward": TRAILER_REWARD, "available_at": trailer_available_at(uid, t["id"])}
                     for t in TRAILERS],
        "game": {"reward": GAME_REWARD, "seconds": GAME_SECONDS,
                 "plays": game_plays_24h(uid), "limit": GAME_DAILY_LIMIT},
        "shop": {"frames": frames, "nitro": {"price": NITRO_PRICE, "days": NITRO_DAYS},
                 "nitro_frame": {"id": NITRO_FRAME, "name": "Prisma",
                                 "equipped": w["frame"] == NITRO_FRAME}},
        "log": log,
    }


@app.get("/api/irisk")
@login_required
def irisk_get():
    return jsonify(irisk_state(current_user()["id"]))


@app.post("/api/irisk/earn/start")
@login_required
def earn_start():
    uid = current_user()["id"]
    d = body()
    kind = d.get("kind")
    t = now()
    ensure_wallet(uid)
    if kind == "trailer":
        tr = next((x for x in TRAILERS if x["id"] == d.get("ref")), None)
        if not tr:
            return err("Trailer não encontrado.", 404)
        if trailer_available_at(uid, tr["id"]):
            return err("Você já ganhou com este trailer. Volte mais tarde.", 429)
        ref, need, reward = tr["id"], tr["seconds"], TRAILER_REWARD
        row = db().execute("SELECT * FROM earn_sessions WHERE user_id=? AND kind='trailer' "
                           "AND ref=? AND claimed=0 AND last_beat>? ORDER BY started_at DESC",
                           (uid, ref, t - 6 * 3600 * 1000)).fetchone()
    elif kind == "game":
        if game_plays_24h(uid) >= GAME_DAILY_LIMIT:
            return err(f"Limite de {GAME_DAILY_LIMIT} jogos por dia atingido. Volte amanhã.", 429)
        need, reward = GAME_SECONDS, GAME_REWARD
        row = db().execute("SELECT * FROM earn_sessions WHERE user_id=? AND kind='game' "
                           "AND claimed=0 AND last_beat>? ORDER BY started_at DESC",
                           (uid, t - 6 * 3600 * 1000)).fetchone()
        ref = row["ref"] if row else secrets.choice(GAMES)
    else:
        return err("Tipo inválido.")
    if row:
        token, done = row["token"], row["done"]
    else:
        db().execute("DELETE FROM earn_sessions WHERE user_id=? AND claimed=0 AND started_at<?",
                     (uid, t - 86400000))
        token, done = secrets.token_urlsafe(16), 0
        db().execute("INSERT INTO earn_sessions(token,user_id,kind,ref,need,done,reward,last_beat,"
                     "started_at) VALUES(?,?,?,?,?,?,?,?,?)",
                     (token, uid, kind, ref, need, 0, reward, t, t))
    db().execute("UPDATE earn_sessions SET last_beat=? WHERE token=?", (t, token))
    db().commit()
    return jsonify({"token": token, "kind": kind, "ref": ref, "need": need,
                    "done": min(done, need), "reward": reward})


@app.post("/api/irisk/earn/beat")
@login_required
def earn_beat():
    uid = current_user()["id"]
    d = body()
    s = db().execute("SELECT * FROM earn_sessions WHERE token=? AND user_id=? AND claimed=0",
                     (str(d.get("token", "")), uid)).fetchone()
    if not s:
        return err("Sessão não encontrada.", 404)
    t = now()
    elapsed = (t - s["last_beat"]) / 1000
    done = s["done"]
    if elapsed >= 4:  # ignora batidas rápidas demais
        if d.get("active"):
            done = min(s["need"], done + min(elapsed, 15))
        db().execute("UPDATE earn_sessions SET done=?, last_beat=? WHERE token=?",
                     (done, t, s["token"]))
        db().commit()
    return jsonify({"done": done, "need": s["need"], "ready": done >= s["need"] - 1})


@app.post("/api/irisk/earn/claim")
@login_required
def earn_claim():
    uid = current_user()["id"]
    s = db().execute("SELECT * FROM earn_sessions WHERE token=? AND user_id=? AND claimed=0",
                     (str(body().get("token", "")), uid)).fetchone()
    if not s:
        return err("Sessão não encontrada ou já resgatada.", 404)
    if s["done"] < s["need"] - 1:
        return err("Ainda falta tempo para resgatar.", 400)
    if s["kind"] == "game" and game_plays_24h(uid) >= GAME_DAILY_LIMIT:
        return err("Limite diário de jogos atingido.", 429)
    if s["kind"] == "trailer" and trailer_available_at(uid, s["ref"]):
        return err("Este trailer já foi resgatado hoje.", 429)
    cur = db().execute("UPDATE earn_sessions SET claimed=1, claimed_at=? WHERE token=? AND claimed=0",
                       (now(), s["token"]))
    if not cur.rowcount:
        return err("Já resgatado.", 409)
    db().commit()
    why = ("Trailer: " + next((x["title"] for x in TRAILERS if x["id"] == s["ref"]), s["ref"])
           if s["kind"] == "trailer" else "Jogo de 15 minutos")
    credit(uid, s["reward"], why)
    push_wallet(uid)
    return jsonify({"message": f"+{s['reward']} irisk!", "reward": s["reward"]})


@app.post("/api/irisk/buy")
@login_required
def irisk_buy():
    uid = current_user()["id"]
    item = str(body().get("item", ""))
    ensure_wallet(uid)
    if item.startswith("frame:"):
        fid = item[6:]
        if fid not in FRAMES:
            return err("Moldura inexistente.", 404)
        if db().execute("SELECT 1 FROM owned_frames WHERE user_id=? AND frame=?",
                        (uid, fid)).fetchone():
            return err("Você já tem esta moldura.")
        if not spend(uid, FRAME_PRICE, f"Moldura {FRAMES[fid]}"):
            return err("irisk insuficiente.", 402)
        db().execute("INSERT INTO owned_frames(user_id,frame) VALUES(?,?)", (uid, fid))
        db().execute("UPDATE perks SET frame=? WHERE user_id=?", (fid, uid))
        db().commit()
        push_wallet(uid)
        return jsonify({"message": f"Moldura {FRAMES[fid]} comprada e equipada!"})
    if item == "nitro":
        if is_owner(uid):
            return jsonify({"message": "Como dono do site, seu Nitro é permanente."})
        if not spend(uid, NITRO_PRICE, f"Nitro ({NITRO_DAYS} dias)"):
            return err("irisk insuficiente.", 402)
        cur_until = db().execute("SELECT nitro_until FROM perks WHERE user_id=?", (uid,)).fetchone()[0]
        until = max(now(), cur_until) + NITRO_DAYS * 86400000
        db().execute("UPDATE perks SET nitro_until=? WHERE user_id=?", (until, uid))
        db().commit()
        push_wallet(uid)
        return jsonify({"message": f"Nitro ativo por mais {NITRO_DAYS} dias!"})
    return err("Item inexistente.", 404)


@app.post("/api/owner/grant")
@login_required
def owner_grant():
    me = current_user()
    if not is_owner(me["id"]):
        return err("Apenas o dono do site pode fazer isso.", 403)
    d = body()
    t = db().execute("SELECT * FROM users WHERE username=?",
                     (str(d.get("username", "")).strip().lstrip("@"),)).fetchone()
    if not t:
        return err("Usuário não encontrado.", 404)
    try:
        amount = int(d.get("amount"))
    except (TypeError, ValueError):
        return err("Informe uma quantidade válida.")
    if amount == 0 or abs(amount) > 10_000_000:
        return err("Quantidade deve ser diferente de 0 e até 10.000.000.")
    ensure_wallet(t["id"])
    if amount < 0:  # remover: nunca deixa o saldo negativo
        amount = -min(-amount, balance_of(t["id"]))
        if amount == 0:
            return err("Essa pessoa não tem irisk para remover.")
    credit(t["id"], amount, "Presente do dono" if amount > 0 else "Ajuste do dono")
    push_wallet(t["id"])
    return jsonify({"message": f"{'+' if amount > 0 else ''}{amount} irisk para @{t['username']}."})


@app.post("/api/irisk/equip")
@login_required
def irisk_equip():
    uid = current_user()["id"]
    fid = str(body().get("frame", ""))
    ensure_wallet(uid)
    if fid == NITRO_FRAME:
        if not nitro_on(uid):
            return err("A moldura Prisma exige Nitro ativo.", 403)
    elif fid:
        if is_owner(uid) and fid in FRAMES:
            pass
        elif not db().execute("SELECT 1 FROM owned_frames WHERE user_id=? AND frame=?",
                              (uid, fid)).fetchone():
            return err("Você não tem esta moldura.", 403)
    db().execute("UPDATE perks SET frame=? WHERE user_id=?", (fid, uid))
    db().commit()
    push_wallet(uid)
    return jsonify({"ok": True})


# ------------------------------------------------------------ estáticos
def serve_index():
    return Response(ASSETS["index.html"], mimetype="text/html")


@app.get("/static/<name>")
def static_asset(name):
    if name not in ASSETS:
        return err("Não encontrado.", 404)
    mime = "text/css" if name.endswith(".css") else "text/javascript" if name.endswith(".js") else "text/html"
    resp = Response(ASSETS[name], mimetype=mime)
    resp.headers["Cache-Control"] = "no-cache"
    return resp


@app.get("/")
def index():
    return serve_index()


@app.get("/uploads/<path:name>")
@login_required
def uploads(name):
    name = os.path.basename(name)
    mime = mimetypes.guess_type(name)[0] or "application/octet-stream"
    inline = mime.split("/")[0] in ("image", "video", "audio") and mime != "image/svg+xml"
    row = db().execute("SELECT filename FROM attachments WHERE path=?",
                       (name,)).fetchone()
    resp = send_from_directory(
        UPLOADS, name, mimetype=mime, as_attachment=not inline,
        download_name=row["filename"] if row else name,
        max_age=86400)
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["Content-Security-Policy"] = "sandbox"
    return resp


# ----------------------------------------------------------------- conta
USERNAME_RE = re.compile(r"^[a-zA-Z0-9_.]{3,24}$")


def start_session(u):
    tok = secrets.token_urlsafe(32)
    db().execute("INSERT INTO sessions(token,user_id,created_at) VALUES(?,?,?)",
                 (tok, u["id"], now()))
    db().commit()
    resp = jsonify(me_user(u))
    resp.set_cookie("token", tok, max_age=SESSION_DAYS * 86400,
                    httponly=True, samesite="Lax")
    return resp


@app.post("/api/register")
def register():
    if not rate("reg:" + (request.remote_addr or ""), 10, 3600):
        return err("Muitas tentativas. Tente novamente mais tarde.", 429)
    d = body()
    username = str(d.get("username", "")).strip()
    password = str(d.get("password", ""))
    display = str(d.get("display_name", "")).strip()[:32]
    if not USERNAME_RE.match(username):
        return err("Usuário deve ter 3–24 caracteres: letras, números, _ ou .")
    if len(password) < 6:
        return err("A senha precisa ter pelo menos 6 caracteres.")
    if db().execute("SELECT 1 FROM users WHERE username=?", (username,)).fetchone():
        return err("Esse nome de usuário já está em uso.", 409)
    cur = db().execute(
        "INSERT INTO users(username,display_name,password_hash,created_at) "
        "VALUES(?,?,?,?)",
        (username, display or username, generate_password_hash(password), now()))
    db().commit()
    return start_session(get_user(cur.lastrowid))


@app.post("/api/login")
def login():
    if not rate("login:" + (request.remote_addr or ""), 10, 60):
        return err("Muitas tentativas. Aguarde um minuto.", 429)
    d = body()
    u = db().execute("SELECT * FROM users WHERE username=?",
                     (str(d.get("username", "")).strip(),)).fetchone()
    if not u or not check_password_hash(u["password_hash"], str(d.get("password", ""))):
        return err("Usuário ou senha incorretos.", 401)
    return start_session(u)


@app.post("/api/logout")
def logout():
    tok = request.cookies.get("token")
    if tok:
        db().execute("DELETE FROM sessions WHERE token=?", (tok,))
        db().commit()
    resp = jsonify({"ok": True})
    resp.delete_cookie("token")
    return resp


@app.get("/api/me")
@login_required
def get_me():
    return jsonify(me_user(current_user()))


@app.patch("/api/me")
@login_required
def patch_me():
    u = current_user()
    d = body()
    name = str(d.get("display_name", u["display_name"])).strip()[:32] or u["username"]
    bio = str(d.get("bio", u["bio"]))[:300]
    cs = str(d.get("custom_status", u["custom_status"]))[:60]
    st = d.get("status", u["status"])
    if st not in ("online", "idle", "dnd", "invisible"):
        return err("Status inválido.")
    db().execute("UPDATE users SET display_name=?, bio=?, custom_status=?, status=? "
                 "WHERE id=?", (name, bio, cs, st, u["id"]))
    db().commit()
    broadcast_user(u["id"])
    return jsonify(me_user(get_user(u["id"])))


def broadcast_user(uid):
    u = get_user(uid)
    hub.send(related_user_ids(uid) + [uid], "user_update", pub_user(u))


@app.post("/api/me/avatar")
@login_required
def set_avatar():
    u = current_user()
    f = request.files.get("file")
    if not f:
        return err("Nenhum arquivo enviado.")
    mime = mimetypes.guess_type(f.filename or "")[0] or ""
    if mime not in ("image/png", "image/jpeg", "image/gif", "image/webp"):
        return err("Use uma imagem PNG, JPG, GIF ou WebP.")
    _name, stored, _mime, size = save_upload(f)
    if size > 4 * 1024 * 1024:
        remove_files([stored])
        return err("A imagem deve ter até 4 MB.")
    if u["avatar"]:
        remove_files([u["avatar"]])
    db().execute("UPDATE users SET avatar=? WHERE id=?", (stored, u["id"]))
    db().commit()
    broadcast_user(u["id"])
    return jsonify(me_user(get_user(u["id"])))


@app.delete("/api/me/avatar")
@login_required
def del_avatar():
    u = current_user()
    if u["avatar"]:
        remove_files([u["avatar"]])
    db().execute("UPDATE users SET avatar=NULL WHERE id=?", (u["id"],))
    db().commit()
    broadcast_user(u["id"])
    return jsonify(me_user(get_user(u["id"])))


@app.post("/api/me/password")
@login_required
def change_password():
    u = current_user()
    d = body()
    if not check_password_hash(u["password_hash"], str(d.get("old", ""))):
        return err("Senha atual incorreta.", 403)
    new = str(d.get("new", ""))
    if len(new) < 6:
        return err("A nova senha precisa ter pelo menos 6 caracteres.")
    db().execute("UPDATE users SET password_hash=? WHERE id=?",
                 (generate_password_hash(new), u["id"]))
    db().execute("DELETE FROM sessions WHERE user_id=? AND token!=?",
                 (u["id"], request.cookies.get("token")))
    db().commit()
    return jsonify({"ok": True})


@app.get("/api/users/<int:uid>")
@login_required
def user_profile(uid):
    u = get_user(uid)
    if not u:
        return err("Usuário não encontrado.", 404)
    return jsonify(pub_user(u))


# ------------------------------------------------------------- bootstrap
@app.get("/api/bootstrap")
@login_required
def bootstrap():
    u = current_user()
    uid = u["id"]
    sids = [r[0] for r in db().execute(
        "SELECT server_id FROM members WHERE user_id=? ORDER BY joined_at", (uid,))]
    vs = {}
    with voice_lock:
        for cid, users in voice.items():
            vs[cid] = voice_snapshot(cid)
    mine = {c["id"] for s in sids for c in server_payload(s, uid)["channels"]}
    return jsonify({
        "me": me_user(u),
        "servers": [server_payload(s, uid) for s in sids],
        "dms": dms_for(uid),
        "friends": friends_for(uid),
        "unread": unread_for(uid, u["username"]),
        "voice": {str(k): v for k, v in vs.items() if k in mine},
        "wallet": wallet_payload(uid),
    })


@app.get("/api/dms")
@login_required
def list_dms():
    return jsonify(dms_for(current_user()["id"]))


# --------------------------------------------------------------- eventos
def sse(event, data):
    import json
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


@app.get("/api/events")
@login_required
def events():
    uid = current_user()["id"]
    q, first = hub.add(uid)
    if first:
        announce_presence(uid)

    def gen():
        try:
            yield sse("ready", {"user_id": uid})
            while True:
                try:
                    ev, data = q.get(timeout=15)
                except queue.Empty:
                    yield ": ping\n\n"
                    continue
                yield sse(ev, data)
        finally:
            if hub.remove(uid, q):
                with app.app_context():
                    voice_leave(uid)
                    announce_presence(uid)

    return Response(gen(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ------------------------------------------------------------- servidores
@app.post("/api/servers")
@login_required
def create_server():
    u = current_user()
    name = str(body().get("name", "")).strip()[:50]
    if len(name) < 2:
        return err("O nome do servidor precisa ter pelo menos 2 caracteres.")
    if db().execute("SELECT COUNT(*) FROM members WHERE user_id=?",
                    (u["id"],)).fetchone()[0] >= 50:
        return err("Você já participa de servidores demais.")
    d = db()
    cur = d.execute(
        "INSERT INTO servers(name,owner_id,invite_code,color,created_at) VALUES(?,?,?,?,?)",
        (name, u["id"], secrets.token_urlsafe(6), secrets.choice(COLORS), now()))
    sid = cur.lastrowid
    d.execute("INSERT INTO members(server_id,user_id,role,joined_at) VALUES(?,?,?,?)",
              (sid, u["id"], "owner", now()))
    d.execute("INSERT INTO channels(server_id,name,kind,position,created_at) "
              "VALUES(?,?,?,?,?)", (sid, "geral", "text", 0, now()))
    d.execute("INSERT INTO channels(server_id,name,kind,position,created_at) "
              "VALUES(?,?,?,?,?)", (sid, "Sala de voz", "voice", 0, now()))
    d.commit()
    p = server_payload(sid, u["id"])
    hub.send([u["id"]], "server_create", p)
    return jsonify(p)


@app.post("/api/servers/join")
@login_required
def join_server():
    u = current_user()
    code = str(body().get("code", "")).strip().split("/")[-1]
    s = db().execute("SELECT * FROM servers WHERE invite_code=?", (code,)).fetchone()
    if not s:
        return err("Convite inválido ou expirado.", 404)
    if db().execute("SELECT 1 FROM bans WHERE server_id=? AND user_id=?",
                    (s["id"], u["id"])).fetchone():
        return err("Você foi banido deste servidor.", 403)
    if not member_row(s["id"], u["id"]):
        db().execute("INSERT INTO members(server_id,user_id,role,joined_at) "
                     "VALUES(?,?,?,?)", (s["id"], u["id"], "member", now()))
        db().commit()
        mem = pub_user(u)
        mem.update({"role": "member", "nickname": "", "joined_at": now()})
        hub.send(server_member_ids(s["id"]), "member_join",
                 {"server_id": s["id"], "member": mem})
    p = server_payload(s["id"], u["id"])
    hub.send([u["id"]], "server_create", p)
    return jsonify(p)


@app.patch("/api/servers/<int:sid>")
@login_required
def update_server(sid):
    u = current_user()
    if not need_member(sid, u["id"], "admin"):
        return err("Sem permissão.", 403)
    s = db().execute("SELECT * FROM servers WHERE id=?", (sid,)).fetchone()
    d = body()
    name = str(d.get("name", s["name"])).strip()[:50]
    if len(name) < 2:
        return err("Nome muito curto.")
    color = d.get("color", s["color"])
    if not re.match(r"^#[0-9a-fA-F]{6}$", str(color)):
        return err("Cor inválida.")
    db().execute("UPDATE servers SET name=?, color=? WHERE id=?", (name, color, sid))
    db().commit()
    hub.send(server_member_ids(sid), "server_update",
             {"id": sid, "name": name, "color": color,
              "invite_code": s["invite_code"]})
    return jsonify({"ok": True})


@app.post("/api/servers/<int:sid>/invite/reset")
@login_required
def reset_invite(sid):
    if not need_member(sid, current_user()["id"], "admin"):
        return err("Sem permissão.", 403)
    code = secrets.token_urlsafe(6)
    db().execute("UPDATE servers SET invite_code=? WHERE id=?", (code, sid))
    db().commit()
    hub.send(server_member_ids(sid), "server_update", {"id": sid, "invite_code": code})
    return jsonify({"invite_code": code})


@app.delete("/api/servers/<int:sid>")
@login_required
def delete_server(sid):
    if not need_member(sid, current_user()["id"], "owner"):
        return err("Só o dono pode excluir o servidor.", 403)
    ids = server_member_ids(sid)
    cids = [r[0] for r in db().execute(
        "SELECT id FROM channels WHERE server_id=?", (sid,))]
    files = files_in_channels(cids)
    db().execute("DELETE FROM servers WHERE id=?", (sid,))
    db().commit()
    remove_files(files)
    voice_clear_channels(cids)
    hub.send(ids, "server_delete", {"id": sid})
    return jsonify({"ok": True})


@app.post("/api/servers/<int:sid>/leave")
@login_required
def leave_server(sid):
    u = current_user()
    m = member_row(sid, u["id"])
    if not m:
        return err("Você não está neste servidor.", 404)
    if m["role"] == "owner":
        return err("O dono não pode sair. Transfira a propriedade ou exclua o servidor.")
    remove_member(sid, u["id"])
    return jsonify({"ok": True})


def remove_member(sid, uid):
    ids = server_member_ids(sid)
    db().execute("DELETE FROM members WHERE server_id=? AND user_id=?", (sid, uid))
    db().commit()
    voice_leave(uid)
    hub.send(ids, "member_leave", {"server_id": sid, "user_id": uid})
    hub.send([uid], "server_delete", {"id": sid})


@app.get("/api/servers/<int:sid>/members")
@login_required
def list_members(sid):
    if not need_member(sid, current_user()["id"]):
        return err("Sem acesso.", 403)
    out = []
    for m in db().execute(
            "SELECT u.*, m.role, m.nickname, m.joined_at AS mj FROM members m "
            "JOIN users u ON u.id=m.user_id WHERE m.server_id=?", (sid,)):
        d = pub_user(m)
        d.update({"role": m["role"], "nickname": m["nickname"], "joined_at": m["mj"]})
        out.append(d)
    return jsonify(out)


@app.post("/api/servers/<int:sid>/members/<int:tid>/role")
@login_required
def set_role(sid, tid):
    if not need_member(sid, current_user()["id"], "owner"):
        return err("Só o dono pode alterar cargos.", 403)
    t = member_row(sid, tid)
    role = body().get("role")
    if not t or t["role"] == "owner" or role not in ("admin", "member"):
        return err("Operação inválida.")
    db().execute("UPDATE members SET role=? WHERE server_id=? AND user_id=?",
                 (role, sid, tid))
    db().commit()
    hub.send(server_member_ids(sid), "member_update",
             {"server_id": sid, "user_id": tid, "role": role})
    return jsonify({"ok": True})


@app.post("/api/servers/<int:sid>/transfer")
@login_required
def transfer(sid):
    u = current_user()
    if not need_member(sid, u["id"], "owner"):
        return err("Só o dono pode transferir o servidor.", 403)
    tid = body().get("user_id")
    if not isinstance(tid, int) or tid == u["id"] or not member_row(sid, tid):
        return err("Membro inválido.")
    db().execute("UPDATE members SET role='admin' WHERE server_id=? AND user_id=?",
                 (sid, u["id"]))
    db().execute("UPDATE members SET role='owner' WHERE server_id=? AND user_id=?",
                 (sid, tid))
    db().execute("UPDATE servers SET owner_id=? WHERE id=?", (tid, sid))
    db().commit()
    ids = server_member_ids(sid)
    hub.send(ids, "member_update", {"server_id": sid, "user_id": u["id"], "role": "admin"})
    hub.send(ids, "member_update", {"server_id": sid, "user_id": tid, "role": "owner"})
    hub.send(ids, "server_update", {"id": sid, "owner_id": tid})
    return jsonify({"ok": True})


@app.post("/api/servers/<int:sid>/nickname")
@login_required
def set_nick(sid):
    u = current_user()
    if not member_row(sid, u["id"]):
        return err("Sem acesso.", 403)
    nick = str(body().get("nickname", "")).strip()[:32]
    db().execute("UPDATE members SET nickname=? WHERE server_id=? AND user_id=?",
                 (nick, sid, u["id"]))
    db().commit()
    hub.send(server_member_ids(sid), "member_update",
             {"server_id": sid, "user_id": u["id"], "nickname": nick})
    return jsonify({"ok": True})


def moderate(sid, tid):
    u = current_user()
    me = need_member(sid, u["id"], "admin")
    t = member_row(sid, tid)
    if not me or not t or tid == u["id"] or RANK[me["role"]] <= RANK[t["role"]]:
        return False
    return True


@app.post("/api/servers/<int:sid>/members/<int:tid>/kick")
@login_required
def kick(sid, tid):
    if not moderate(sid, tid):
        return err("Sem permissão para expulsar este membro.", 403)
    remove_member(sid, tid)
    return jsonify({"ok": True})


@app.post("/api/servers/<int:sid>/members/<int:tid>/ban")
@login_required
def ban(sid, tid):
    if not moderate(sid, tid):
        return err("Sem permissão para banir este membro.", 403)
    db().execute("INSERT OR IGNORE INTO bans(server_id,user_id) VALUES(?,?)", (sid, tid))
    db().commit()
    remove_member(sid, tid)
    return jsonify({"ok": True})


@app.get("/api/servers/<int:sid>/bans")
@login_required
def list_bans(sid):
    if not need_member(sid, current_user()["id"], "admin"):
        return err("Sem permissão.", 403)
    rows = db().execute("SELECT u.* FROM bans b JOIN users u ON u.id=b.user_id "
                        "WHERE b.server_id=?", (sid,))
    return jsonify([pub_user(r) for r in rows])


@app.delete("/api/servers/<int:sid>/bans/<int:tid>")
@login_required
def unban(sid, tid):
    if not need_member(sid, current_user()["id"], "admin"):
        return err("Sem permissão.", 403)
    db().execute("DELETE FROM bans WHERE server_id=? AND user_id=?", (sid, tid))
    db().commit()
    return jsonify({"ok": True})


# ----------------------------------------------------------------- canais
@app.post("/api/servers/<int:sid>/channels")
@login_required
def create_channel(sid):
    if not need_member(sid, current_user()["id"], "admin"):
        return err("Só administradores podem criar canais.", 403)
    d = body()
    kind = d.get("kind", "text")
    if kind not in ("text", "voice"):
        return err("Tipo de canal inválido.")
    name = slug_channel(str(d.get("name", ""))) if kind == "text" \
        else str(d.get("name", "")).strip()[:32]
    if not name:
        return err("Dê um nome ao canal.")
    if db().execute("SELECT COUNT(*) FROM channels WHERE server_id=?",
                    (sid,)).fetchone()[0] >= 50:
        return err("Limite de 50 canais atingido.")
    pos = db().execute("SELECT COALESCE(MAX(position),-1)+1 FROM channels "
                       "WHERE server_id=? AND kind=?", (sid, kind)).fetchone()[0]
    cur = db().execute(
        "INSERT INTO channels(server_id,name,kind,topic,position,created_at) "
        "VALUES(?,?,?,?,?,?)",
        (sid, name, kind, str(d.get("topic", ""))[:200], pos, now()))
    db().commit()
    ch = db().execute("SELECT * FROM channels WHERE id=?", (cur.lastrowid,)).fetchone()
    hub.send(server_member_ids(sid), "channel_create", chan_dict(ch))
    return jsonify(chan_dict(ch))


@app.patch("/api/channels/<int:cid>")
@login_required
def update_channel(cid):
    ch = db().execute("SELECT * FROM channels WHERE id=?", (cid,)).fetchone()
    if not ch or ch["kind"] == "dm" or not need_member(
            ch["server_id"], current_user()["id"], "admin"):
        return err("Sem permissão.", 403)
    d = body()
    name = slug_channel(str(d.get("name", ch["name"]))) if ch["kind"] == "text" \
        else str(d.get("name", ch["name"])).strip()[:32]
    if not name:
        return err("Nome inválido.")
    db().execute("UPDATE channels SET name=?, topic=? WHERE id=?",
                 (name, str(d.get("topic", ch["topic"]))[:200], cid))
    db().commit()
    ch = db().execute("SELECT * FROM channels WHERE id=?", (cid,)).fetchone()
    hub.send(server_member_ids(ch["server_id"]), "channel_update", chan_dict(ch))
    return jsonify(chan_dict(ch))


@app.delete("/api/channels/<int:cid>")
@login_required
def delete_channel(cid):
    ch = db().execute("SELECT * FROM channels WHERE id=?", (cid,)).fetchone()
    if not ch or ch["kind"] == "dm" or not need_member(
            ch["server_id"], current_user()["id"], "admin"):
        return err("Sem permissão.", 403)
    if ch["kind"] == "text" and db().execute(
            "SELECT COUNT(*) FROM channels WHERE server_id=? AND kind='text'",
            (ch["server_id"],)).fetchone()[0] <= 1:
        return err("O servidor precisa ter pelo menos um canal de texto.")
    files = files_in_channels([cid])
    ids = server_member_ids(ch["server_id"])
    db().execute("DELETE FROM channels WHERE id=?", (cid,))
    db().commit()
    remove_files(files)
    voice_clear_channels([cid])
    hub.send(ids, "channel_delete", {"server_id": ch["server_id"], "id": cid})
    return jsonify({"ok": True})


# --------------------------------------------------------------- mensagens
@app.get("/api/channels/<int:cid>/messages")
@login_required
def get_messages(cid):
    ch = access(cid, current_user()["id"])
    if not ch:
        return err("Canal não encontrado.", 404)
    try:
        limit = max(1, min(100, int(request.args.get("limit", 50))))
        before = int(request.args.get("before", 0))
    except ValueError:
        return err("Parâmetros inválidos.")
    sql = "SELECT * FROM messages WHERE channel_id=?"
    args = [cid]
    if before:
        sql += " AND id<?"
        args.append(before)
    rows = db().execute(sql + " ORDER BY id DESC LIMIT ?", args + [limit + 1]).fetchall()
    more = len(rows) > limit
    rows = list(reversed(rows[:limit]))
    return jsonify({"messages": hydrate(rows, ch["server_id"]), "has_more": more})


@app.post("/api/channels/<int:cid>/messages")
@login_required
def post_message(cid):
    u = current_user()
    ch = access(cid, u["id"])
    if not ch or ch["kind"] == "voice":
        return err("Canal não encontrado.", 404)
    files = []
    if (request.content_type or "").startswith("multipart/"):
        content = request.form.get("content", "")
        reply_to = request.form.get("reply_to")
        files = request.files.getlist("files")[:10]
    else:
        d = body()
        content, reply_to = str(d.get("content", "")), d.get("reply_to")
    content = content.strip()[:4000]
    if not content and not files:
        return err("Mensagem vazia.")
    if not rate(f"msg:{u['id']}", 15, 10):
        return err("Calma! Você está enviando mensagens rápido demais.", 429)
    rid = None
    if reply_to:
        try:
            r = db().execute("SELECT id FROM messages WHERE id=? AND channel_id=?",
                             (int(reply_to), cid)).fetchone()
            rid = r["id"] if r else None
        except (TypeError, ValueError):
            rid = None
    limit_mb = 100 if nitro_on(u["id"]) else 25
    saved = []
    for f in files:
        if not f or not f.filename:
            continue
        item = save_upload(f)
        saved.append(item)
        if item[3] > limit_mb * 1024 * 1024:
            remove_files([x[1] for x in saved])
            msg = f"Arquivo acima de {limit_mb} MB."
            return err(msg + (" O Nitro libera até 100 MB." if limit_mb == 25 else ""), 413)
    if not content and not saved:
        return err("Mensagem vazia.")
    cur = db().execute(
        "INSERT INTO messages(channel_id,author_id,content,reply_to,created_at) "
        "VALUES(?,?,?,?,?)", (cid, u["id"], content, rid, now()))
    mid = cur.lastrowid
    for name, stored, mime, size in saved:
        db().execute("INSERT INTO attachments(message_id,filename,path,mime,size) "
                     "VALUES(?,?,?,?,?)", (mid, name, stored, mime, size))
    db().execute(
        "INSERT INTO reads(channel_id,user_id,last_message_id) VALUES(?,?,?) "
        "ON CONFLICT(channel_id,user_id) DO UPDATE SET last_message_id=excluded.last_message_id",
        (cid, u["id"], mid))
    db().commit()
    msg = one_message(mid)
    hub.send(audience(ch), "message_create", msg)
    return jsonify(msg)


def message_for_edit(mid):
    m = db().execute("SELECT * FROM messages WHERE id=?", (mid,)).fetchone()
    if not m:
        return None, None
    return m, access(m["channel_id"], current_user()["id"])


@app.patch("/api/messages/<int:mid>")
@login_required
def edit_message(mid):
    m, ch = message_for_edit(mid)
    if not m or not ch:
        return err("Mensagem não encontrada.", 404)
    if m["author_id"] != current_user()["id"]:
        return err("Você só pode editar suas próprias mensagens.", 403)
    content = str(body().get("content", "")).strip()[:4000]
    has_att = db().execute("SELECT 1 FROM attachments WHERE message_id=?", (mid,)).fetchone()
    if not content and not has_att:
        return err("A mensagem não pode ficar vazia.")
    db().execute("UPDATE messages SET content=?, edited_at=? WHERE id=?",
                 (content, now(), mid))
    db().commit()
    msg = one_message(mid)
    hub.send(audience(ch), "message_update", msg)
    return jsonify(msg)


@app.delete("/api/messages/<int:mid>")
@login_required
def delete_message(mid):
    m, ch = message_for_edit(mid)
    if not m or not ch:
        return err("Mensagem não encontrada.", 404)
    uid = current_user()["id"]
    allowed = m["author_id"] == uid or (
        ch["kind"] == "text" and need_member(ch["server_id"], uid, "admin"))
    if not allowed:
        return err("Sem permissão.", 403)
    files = [r[0] for r in db().execute(
        "SELECT path FROM attachments WHERE message_id=?", (mid,))]
    aud = audience(ch)
    db().execute("DELETE FROM messages WHERE id=?", (mid,))
    db().commit()
    remove_files(files)
    hub.send(aud, "message_delete", {"id": mid, "channel_id": ch["id"]})
    return jsonify({"ok": True})


@app.post("/api/messages/<int:mid>/reactions")
@login_required
def react(mid):
    m, ch = message_for_edit(mid)
    if not m or not ch:
        return err("Mensagem não encontrada.", 404)
    emoji = str(body().get("emoji", "")).strip()
    if not emoji or len(emoji) > 16:
        return err("Emoji inválido.")
    uid = current_user()["id"]
    d = db()
    if d.execute("SELECT 1 FROM reactions WHERE message_id=? AND user_id=? AND emoji=?",
                 (mid, uid, emoji)).fetchone():
        d.execute("DELETE FROM reactions WHERE message_id=? AND user_id=? AND emoji=?",
                  (mid, uid, emoji))
    else:
        kinds = d.execute("SELECT COUNT(DISTINCT emoji) FROM reactions WHERE message_id=?",
                          (mid,)).fetchone()[0]
        exists = d.execute("SELECT 1 FROM reactions WHERE message_id=? AND emoji=?",
                           (mid, emoji)).fetchone()
        if kinds >= 20 and not exists:
            return err("Essa mensagem já tem reações demais.")
        d.execute("INSERT INTO reactions(message_id,user_id,emoji) VALUES(?,?,?)",
                  (mid, uid, emoji))
    d.commit()
    msg = one_message(mid)
    hub.send(audience(ch), "message_update", msg)
    return jsonify(msg)


@app.post("/api/messages/<int:mid>/pin")
@login_required
def pin(mid):
    m, ch = message_for_edit(mid)
    if not m or not ch:
        return err("Mensagem não encontrada.", 404)
    uid = current_user()["id"]
    if ch["kind"] == "text" and not need_member(ch["server_id"], uid, "admin"):
        return err("Só administradores podem fixar mensagens.", 403)
    db().execute("UPDATE messages SET pinned=1-pinned WHERE id=?", (mid,))
    db().commit()
    msg = one_message(mid)
    hub.send(audience(ch), "message_update", msg)
    return jsonify(msg)


@app.get("/api/channels/<int:cid>/pins")
@login_required
def pins(cid):
    ch = access(cid, current_user()["id"])
    if not ch:
        return err("Canal não encontrado.", 404)
    rows = db().execute("SELECT * FROM messages WHERE channel_id=? AND pinned=1 "
                        "ORDER BY id DESC", (cid,)).fetchall()
    return jsonify(hydrate(rows, ch["server_id"]))


@app.post("/api/channels/<int:cid>/read")
@login_required
def mark_read(cid):
    u = current_user()
    if not access(cid, u["id"]):
        return err("Canal não encontrado.", 404)
    last = db().execute("SELECT COALESCE(MAX(id),0) FROM messages WHERE channel_id=?",
                        (cid,)).fetchone()[0]
    db().execute(
        "INSERT INTO reads(channel_id,user_id,last_message_id) VALUES(?,?,?) "
        "ON CONFLICT(channel_id,user_id) DO UPDATE SET "
        "last_message_id=MAX(last_message_id, excluded.last_message_id)",
        (cid, u["id"], last))
    db().commit()
    hub.send([u["id"]], "read", {"channel_id": cid})
    return jsonify({"ok": True})


@app.post("/api/channels/<int:cid>/typing")
@login_required
def typing(cid):
    u = current_user()
    ch = access(cid, u["id"])
    if not ch:
        return err("Canal não encontrado.", 404)
    hub.send([x for x in audience(ch) if x != u["id"]], "typing",
             {"channel_id": cid, "user_id": u["id"],
              "name": u["display_name"] or u["username"]})
    return jsonify({"ok": True})


@app.get("/api/search")
@login_required
def search():
    u = current_user()
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return err("Digite pelo menos 2 caracteres.")
    sid = request.args.get("server_id", type=int)
    cid = request.args.get("channel_id", type=int)
    if cid:
        ch = access(cid, u["id"])
        if not ch:
            return err("Canal não encontrado.", 404)
        where, args, server = "m.channel_id=?", [cid], ch["server_id"]
    elif sid and need_member(sid, u["id"]):
        where = "m.channel_id IN (SELECT id FROM channels WHERE server_id=? AND kind='text')"
        args, server = [sid], sid
    else:
        return err("Informe um canal ou servidor.")
    rows = db().execute(
        f"SELECT m.*, c.name AS cname FROM messages m JOIN channels c ON c.id=m.channel_id "
        f"WHERE {where} AND instr(lower(m.content), lower(?))>0 "
        f"ORDER BY m.id DESC LIMIT 50", args + [q]).fetchall()
    out = hydrate(rows, server)
    names = {r["id"]: r["cname"] for r in rows}
    for m in out:
        m["channel_name"] = names[m["id"]]
    return jsonify(out)


# ------------------------------------------------------- amigos e DMs
def friend_event(a, b, rel_for_a, rel_for_b):
    ua, ub = get_user(a), get_user(b)
    hub.send([a], "friend_update", {"user": pub_user(ub), "relation": rel_for_a})
    hub.send([b], "friend_update", {"user": pub_user(ua), "relation": rel_for_b})


@app.post("/api/friends/request")
@login_required
def friend_request():
    u = current_user()
    name = str(body().get("username", "")).strip().lstrip("@")
    t = db().execute("SELECT * FROM users WHERE username=?", (name,)).fetchone()
    if not t:
        return err("Nenhum usuário encontrado com esse nome.", 404)
    if t["id"] == u["id"]:
        return err("Você não pode adicionar a si mesmo.")
    ex = db().execute(
        "SELECT * FROM friendships WHERE (requester_id=? AND addressee_id=?) "
        "OR (requester_id=? AND addressee_id=?)",
        (u["id"], t["id"], t["id"], u["id"])).fetchone()
    if ex:
        if ex["status"] == "accepted":
            return err("Vocês já são amigos.")
        if ex["requester_id"] == u["id"]:
            return err("Você já enviou um pedido para essa pessoa.")
        db().execute("UPDATE friendships SET status='accepted' "
                     "WHERE requester_id=? AND addressee_id=?", (t["id"], u["id"]))
        db().commit()
        friend_event(u["id"], t["id"], "friend", "friend")
        return jsonify({"relation": "friend"})
    db().execute("INSERT INTO friendships(requester_id,addressee_id,status,created_at) "
                 "VALUES(?,?,?,?)", (u["id"], t["id"], "pending", now()))
    db().commit()
    friend_event(u["id"], t["id"], "outgoing", "incoming")
    return jsonify({"relation": "outgoing"})


@app.post("/api/friends/<int:tid>/accept")
@login_required
def friend_accept(tid):
    u = current_user()
    cur = db().execute("UPDATE friendships SET status='accepted' WHERE requester_id=? "
                       "AND addressee_id=? AND status='pending'", (tid, u["id"]))
    db().commit()
    if not cur.rowcount:
        return err("Pedido não encontrado.", 404)
    friend_event(u["id"], tid, "friend", "friend")
    return jsonify({"ok": True})


@app.delete("/api/friends/<int:tid>")
@login_required
def friend_remove(tid):
    u = current_user()
    db().execute("DELETE FROM friendships WHERE (requester_id=? AND addressee_id=?) "
                 "OR (requester_id=? AND addressee_id=?)",
                 (u["id"], tid, tid, u["id"]))
    db().commit()
    if get_user(tid):
        friend_event(u["id"], tid, None, None)
    return jsonify({"ok": True})


@app.post("/api/dm")
@login_required
def open_dm():
    u = current_user()
    tid = body().get("user_id")
    if not isinstance(tid, int) or tid == u["id"] or not get_user(tid):
        return err("Usuário inválido.")
    row = db().execute(
        "SELECT d1.channel_id FROM dm_members d1 JOIN dm_members d2 "
        "ON d1.channel_id=d2.channel_id WHERE d1.user_id=? AND d2.user_id=?",
        (u["id"], tid)).fetchone()
    if row:
        cid = row[0]
    else:
        cur = db().execute("INSERT INTO channels(name,kind,created_at) VALUES('', 'dm', ?)",
                           (now(),))
        cid = cur.lastrowid
        db().executemany("INSERT INTO dm_members(channel_id,user_id) VALUES(?,?)",
                         [(cid, u["id"]), (cid, tid)])
        db().commit()
    return jsonify(dm_payload(cid, u["id"]))


# ----------------------------------------------------------------- voz
@app.post("/api/voice/<int:cid>/join")
@login_required
def voice_join(cid):
    u = current_user()
    ch = access(cid, u["id"])
    if not ch or ch["kind"] != "voice":
        return err("Canal de voz não encontrado.", 404)
    d = body()
    with voice_lock:
        room = voice.get(cid, {})
        if u["id"] not in room and len(room) >= 12:
            return err("Esta sala de voz está cheia (limite de 12).", 409)
    voice_leave(u["id"])
    with voice_lock:
        room = voice.setdefault(cid, {})
        peers = [x for x in room if x != u["id"]]
        room[u["id"]] = {"muted": bool(d.get("muted")), "deafened": bool(d.get("deafened"))}
    broadcast_voice(cid)
    return jsonify({"peers": peers})


@app.post("/api/voice/leave")
@login_required
def voice_leave_route():
    voice_leave(current_user()["id"])
    return jsonify({"ok": True})


@app.post("/api/voice/state")
@login_required
def voice_state():
    uid = current_user()["id"]
    d = body()
    cid = voice_channel_of(uid)
    if cid:
        with voice_lock:
            voice[cid][uid] = {"muted": bool(d.get("muted")),
                               "deafened": bool(d.get("deafened"))}
        broadcast_voice(cid)
    return jsonify({"ok": True})


@app.post("/api/voice/signal")
@login_required
def voice_signal():
    uid = current_user()["id"]
    d = body()
    to = d.get("to")
    cid = voice_channel_of(uid)
    with voice_lock:
        same = cid is not None and to in voice.get(cid, {})
    if not same:
        return err("Destinatário não está na sala.", 404)
    hub.send([to], "voice_signal", {"from": uid, "data": d.get("data")})
    return jsonify({"ok": True})


@app.errorhandler(404)
def not_found(_e):
    p = request.path
    if p.startswith(("/api/", "/static/", "/uploads/")):
        return err("Não encontrado.", 404)
    # qualquer outro endereço (/index.html, /login...) abre o app
    return serve_index()


@app.errorhandler(413)
def too_large(_e):
    return err("Arquivo grande demais.", 413)


# ===================== INTERFACE (arquivos embutidos) =====================
ASSETS = {}

ASSETS['index.html'] = r'''<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>Prosa</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Crect width='64' height='64' rx='18' fill='%23ffb454'/%3E%3Cpath d='M16 22h32v18H34l-8 8v-8H16z' fill='%2315111f'/%3E%3C/svg%3E">
<link rel="stylesheet" href="/static/style.css">
</head>
<body>

<div id="auth" class="auth hidden">
  <form id="auth-form" class="auth-card" autocomplete="on">
    <div class="auth-logo">Prosa</div>
    <h1 id="auth-title">Que bom te ver de novo</h1>
    <p id="auth-sub" class="muted">Entre para continuar suas conversas.</p>
    <label>Nome de usuário
      <input id="a-user" name="username" autocomplete="username" required minlength="3" maxlength="24">
    </label>
    <label id="a-display-wrap" class="hidden">Nome de exibição (opcional)
      <input id="a-display" maxlength="32">
    </label>
    <label>Senha
      <input id="a-pass" name="password" type="password" autocomplete="current-password" required minlength="6">
    </label>
    <div id="auth-error" class="form-error" role="alert"></div>
    <button class="btn primary wide" id="auth-submit">Entrar</button>
    <p class="muted small">
      <span id="auth-switch-text">Ainda não tem conta?</span>
      <a href="#" id="auth-switch">Criar conta</a>
    </p>
  </form>
</div>

<div id="app" class="app hidden">
  <nav id="rail" class="rail" aria-label="Servidores"></nav>
  <aside id="sidebar" class="sidebar"></aside>
  <main id="main" class="main"></main>
  <aside id="side-panel" class="side-panel"></aside>
</div>
<div id="scrim" class="scrim"></div>

<div id="modal-root"></div>
<div id="popover-root"></div>
<div id="toasts" class="toasts" aria-live="polite"></div>
<div id="lightbox" class="lightbox hidden"><img alt=""></div>
<div id="audio-sink"></div>

<script src="/static/util.js"></script>
<script src="/static/voice.js"></script>
<script src="/static/views.js"></script>
<script src="/static/modals.js"></script>
<script src="/static/loja.js"></script>
<script src="/static/main.js"></script>
</body>
</html>
'''

ASSETS['util.js'] = r''''use strict';

/* ------------------------------------------------------------ helpers */
const $ = (s, el = document) => el.querySelector(s);
const $$ = (s, el = document) => [...el.querySelectorAll(s)];
const esc = s => String(s ?? '').replace(/[&<>"']/g, c =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const AVATAR_COLORS = ['#e0556b', '#d9822b', '#c9a227', '#4fa86b', '#2f9e9a',
  '#3f7fd9', '#7a5fd1', '#b04fc4', '#c4507f', '#6b7a8f'];
const STATUS_LABEL = { online: 'Online', idle: 'Ausente', dnd: 'Não perturbe',
  invisible: 'Invisível', offline: 'Offline' };
const ROLE_LABEL = { owner: 'Dono', admin: 'Admin', member: 'Membro' };

/* -------------------------------------------------------------- estado */
const S = {
  me: null,
  servers: new Map(),
  dms: [],
  friends: new Map(),   // uid -> {user, relation}
  users: new Map(),     // cache de usuários
  members: new Map(),   // sid -> Map(uid -> membro)
  unread: {},           // cid -> {count, mentions}
  voice: {},            // cid -> [{user_id, muted, deafened}]
  msgs: new Map(),      // cid -> {list, hasMore, loaded}
  typing: {},           // cid -> {uid: {name, t}}
  drafts: {},
  lastChannel: {},
  view: { kind: 'home', sid: null, cid: null, tab: 'online' },
  panel: 'members',     // members | search | pins | null
  panelData: null,
  reply: null,
  editing: null,
  files: [],
  connectedOnce: false,
};

/* ----------------------------------------------------------------- API */
async function api(method, url, body) {
  const opt = { method, headers: {} };
  if (body instanceof FormData) opt.body = body;
  else if (body !== undefined) {
    opt.headers['Content-Type'] = 'application/json';
    opt.body = JSON.stringify(body);
  }
  const r = await fetch(url, opt);
  let data = null;
  try { data = await r.json(); } catch { /* sem corpo */ }
  if (r.status === 401 && !/\/(login|register)$/.test(url)) {
    showAuth();
    throw new Error('Sua sessão expirou. Entre novamente.');
  }
  if (!r.ok) throw new Error((data && data.error) || 'Algo deu errado.');
  return data;
}
async function act(fn) {
  try { return await fn(); } catch (e) { toast(e.message); }
}

function toast(msg) {
  const t = document.createElement('div');
  t.className = 'toast';
  t.textContent = msg;
  $('#toasts').appendChild(t);
  setTimeout(() => t.classList.add('out'), 3800);
  setTimeout(() => t.remove(), 4200);
}

/* ------------------------------------------------------------ usuários */
function cacheUser(u) {
  if (!u) return u;
  const cur = S.users.get(u.id);
  if (cur) Object.assign(cur, u); else S.users.set(u.id, { ...u });
  return S.users.get(u.id);
}
function U(u) { return (u && S.users.get(u.id)) || u; }
function patchUser(uid, patch) {
  const u = S.users.get(uid);
  if (u) Object.assign(u, patch);
  S.members.forEach(m => { const x = m.get(uid); if (x) Object.assign(x, patch); });
  const f = S.friends.get(uid);
  if (f) Object.assign(f.user, patch);
  S.dms.forEach(d => { if (d.user.id === uid) Object.assign(d.user, patch); });
  if (S.me && S.me.id === uid) Object.assign(S.me, patch);
}
function displayName(user, sid) {
  if (!user) return 'Usuário removido';
  const m = sid && S.members.get(sid)?.get(user.id);
  return (m && m.nickname) || user.display_name || user.username;
}
function avatar(user, size = 40, status = false, raw = false) {
  if (!raw) user = U(user);
  if (!user) return `<div class="avatar" style="--s:${size}px"></div>`;
  const inner = user.avatar
    ? `<img src="${esc(user.avatar)}" alt="" loading="lazy">`
    : `<span>${esc((user.display_name || user.username || '?')[0].toUpperCase())}</span>`;
  const bg = user.avatar ? '' : `background:${AVATAR_COLORS[user.id % AVATAR_COLORS.length]};`;
  const dot = status ? `<i class="dot ${esc(user.status)}"></i>` : '';
  const fr = frameOf(user);
  return `<div class="avatar${fr ? ' fr fr-' + esc(fr) : ''}" style="--s:${size}px;${bg}">${inner}${dot}</div>`;
}
function serverIcon(s, size = 48) {
  return `<div class="sicon" style="--s:${size}px;background:${esc(s.color)}">${esc(s.name.trim()[0]?.toUpperCase() || '?')}</div>`;
}

/* ------------------------------------------------------------- tempo */
const tf = new Intl.DateTimeFormat('pt-BR', { hour: '2-digit', minute: '2-digit' });
const df = new Intl.DateTimeFormat('pt-BR', { day: '2-digit', month: 'long', year: 'numeric' });
const sameDay = (a, b) => new Date(a).toDateString() === new Date(b).toDateString();
function fmtDay(ts) {
  const d = new Date(ts), t = new Date();
  if (sameDay(ts, t)) return 'Hoje';
  if (sameDay(ts, t.getTime() - 864e5)) return 'Ontem';
  return df.format(d);
}
function fmtStamp(ts) {
  const day = sameDay(ts, Date.now()) ? 'Hoje' : sameDay(ts, Date.now() - 864e5) ? 'Ontem'
    : new Date(ts).toLocaleDateString('pt-BR');
  return `${day} às ${tf.format(ts)}`;
}
function fmtSize(n) {
  if (n < 1024) return n + ' B';
  if (n < 1048576) return (n / 1024).toFixed(1) + ' KB';
  return (n / 1048576).toFixed(1) + ' MB';
}

/* ---------------------------------------------------- texto formatado */
function formatText(text, sid) {
  const stash = [];
  const keep = html => { stash.push(html); return `\uE000${stash.length - 1}\uE001`; };
  let s = esc(text);
  s = s.replace(/```(?:[\w-]+\n)?([\s\S]*?)```/g,
    (_, c) => keep(`<pre><code>${c.replace(/^\n+|\n+$/g, '')}</code></pre>`));
  s = s.replace(/`([^`\n]+)`/g, (_, c) => keep(`<code>${c}</code>`));
  s = s.replace(/(https?:\/\/[^\s<]+[^\s<.,;:!?)\]])/g,
    u => keep(`<a href="${u}" target="_blank" rel="noopener noreferrer">${u}</a>`));
  s = s.replace(/\*\*([^*\n]+)\*\*/g, '<b>$1</b>')
    .replace(/(^|[^*])\*([^*\n]+)\*/g, '$1<i>$2</i>')
    .replace(/__([^_\n]+)__/g, '<u>$1</u>')
    .replace(/~~([^~\n]+)~~/g, '<s>$1</s>')
    .replace(/(^|\n)&gt; ([^\n]*)/g, '$1<blockquote>$2</blockquote>');
  const known = new Set(['everyone']);
  S.users.forEach(u => known.add(u.username.toLowerCase()));
  s = s.replace(/@([a-zA-Z0-9_.]{2,24})/g, (m, n) => {
    if (!known.has(n.toLowerCase())) return m;
    const me = S.me && (n.toLowerCase() === S.me.username.toLowerCase() || n === 'everyone');
    return `<span class="mention${me ? ' me' : ''}">@${n}</span>`;
  });
  s = s.replace(/\n(?!<\/?blockquote)/g, '<br>').replace(/<\/blockquote><br>/g, '</blockquote>');
  return s.replace(/\uE000(\d+)\uE001/g, (_, i) => stash[+i]);
}
function mentionsMe(content) {
  const c = content.toLowerCase();
  return c.includes('@' + S.me.username.toLowerCase()) || c.includes('@everyone');
}

/* --------------------------------------------------------------- modal */
function openModal(html, { wide = false, onMount } = {}) {
  closeModal();
  const root = $('#modal-root');
  root.innerHTML = `<div class="modal-back"><div class="modal${wide ? ' wide' : ''}" role="dialog" aria-modal="true">${html}</div></div>`;
  const back = $('.modal-back', root);
  back.addEventListener('mousedown', e => { if (e.target === back) closeModal(); });
  root.addEventListener('click', e => { if (e.target.closest('[data-close]')) closeModal(); });
  const first = $('input:not([type=hidden]), textarea', root);
  if (first) setTimeout(() => first.focus(), 30);
  if (onMount) onMount($('.modal', root));
  return $('.modal', root);
}
let modalCleanup = null;
function closeModal() {
  if (modalCleanup) { const f = modalCleanup; modalCleanup = null; f(); }
  $('#modal-root').innerHTML = '';
}
const nitroActive = u => !!u && u.nitro_until > Date.now();
const frameOf = u => (u && u.frame && (u.frame !== 'prisma' || nitroActive(u))) ? u.frame : '';
function confirmBox(title, text, okLabel, onOk, danger = true) {
  const m = openModal(`<h2>${esc(title)}</h2><p class="muted">${esc(text)}</p>
    <div class="modal-actions"><button class="btn ghost" data-close>Cancelar</button>
    <button class="btn ${danger ? 'danger' : 'primary'}" id="cf-ok">${esc(okLabel)}</button></div>`);
  $('#cf-ok', m).onclick = async () => { closeModal(); await act(onOk); };
}

/* -------------------------------------------------- popover e menus */
function closePopover() { $('#popover-root').innerHTML = ''; }
function popover(anchor, html, { align = 'right', up = false, cls = '' } = {}) {
  closePopover();
  const root = $('#popover-root');
  const el = document.createElement('div');
  el.className = 'popover ' + cls;
  el.innerHTML = html;
  root.appendChild(el);
  const r = anchor.getBoundingClientRect ? anchor.getBoundingClientRect()
    : { left: anchor.x, right: anchor.x, top: anchor.y, bottom: anchor.y };
  const w = el.offsetWidth, h = el.offsetHeight;
  let left = align === 'right' ? r.right - w : r.left;
  let top = up ? r.top - h - 6 : r.bottom + 6;
  left = Math.max(8, Math.min(left, innerWidth - w - 8));
  if (top + h > innerHeight - 8) top = Math.max(8, r.top - h - 6);
  if (top < 8) top = 8;
  el.style.left = left + 'px';
  el.style.top = top + 'px';
  return el;
}
function menu(anchor, items, opts) {
  const el = popover(anchor, items.filter(Boolean).map((it, i) => it.sep
    ? '<hr>' : `<button class="menu-item${it.danger ? ' danger' : ''}" data-i="${i}">${esc(it.label)}</button>`).join(''),
  { cls: 'menu', ...opts });
  const list = items.filter(Boolean);
  el.addEventListener('click', e => {
    const b = e.target.closest('[data-i]');
    if (!b) return;
    closePopover();
    list[+b.dataset.i].run();
  });
}
document.addEventListener('mousedown', e => {
  if (!e.target.closest('.popover') && !e.target.closest('[data-keep-popover]')) closePopover();
});
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') { closePopover(); closeModal(); $('#lightbox').classList.add('hidden'); }
});

const EMOJIS = ('😀 😃 😄 😁 😆 😅 🤣 😂 🙂 😉 😊 😇 🥰 😍 🤩 😘 😋 😛 😜 🤪 🤔 🤨 😐 😑 😶 🙄 😏 😣 😥 😮 🤐 😯 😪 😫 🥱 😴 😌 😎 🤓 😕 🙁 😢 😭 😤 😠 😡 🤯 😳 🥵 🥶 😱 😰 🤗 🤭 🫡 🤝 👍 👎 👏 🙌 🙏 💪 👀 👋 ✌️ 🤞 👌 ❤️ 🧡 💛 💚 💙 💜 🖤 💔 ✨ 🔥 💯 🎉 🎂 🎮 🎵 ☕ 🍕 🍔 🍻 🌟 ⚡ 🌈 🐶 🐱 🦊 🐸 🚀 ✅ ❌ ❓ ❗').split(' ');
function emojiPicker(anchor, onPick, opts = {}) {
  const el = popover(anchor, `<div class="emoji-grid">${EMOJIS.map(e =>
    `<button data-e="${e}">${e}</button>`).join('')}</div>`, { cls: 'emoji-pop', ...opts });
  el.addEventListener('click', ev => {
    const b = ev.target.closest('[data-e]');
    if (b) { closePopover(); onPick(b.dataset.e); }
  });
}
'''

ASSETS['voice.js'] = r''''use strict';

/* Voz: cada participante conecta direto a todos os outros (malha P2P).
   O servidor só repassa mensagens de sinalização (SDP / ICE). */
const V = {
  cid: null, stream: null,
  pcs: new Map(), audios: new Map(), pending: new Map(),
  muted: false, deafened: false, prevMuted: false,
  ctx: null, meters: new Map(), speaking: new Set(), timer: null,
};
const ICE = { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] };

function voiceChannel() {
  if (!V.cid) return null;
  for (const s of S.servers.values()) {
    const c = s.channels.find(c => c.id === V.cid);
    if (c) return { ...c, server: s };
  }
  return null;
}

async function joinVoice(cid) {
  if (V.cid === cid) return;
  if (!navigator.mediaDevices?.getUserMedia) {
    toast('Seu navegador não permite usar o microfone aqui. Use http://localhost ou HTTPS.');
    return;
  }
  if (V.cid) await leaveVoice();
  let stream;
  try {
    stream = await navigator.mediaDevices.getUserMedia({
      audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true } });
  } catch (e) {
    toast('Não foi possível acessar o microfone: ' + e.message);
    return;
  }
  try {
    const r = await api('POST', `/api/voice/${cid}/join`,
      { muted: V.muted, deafened: V.deafened });
    V.stream = stream;
    V.cid = cid;
    applyMic();
    startMeters();
    watchSpeaking(S.me.id, stream);
    for (const uid of r.peers) await connectPeer(uid, true);
    renderSidebar(); renderMainIfVoice();
  } catch (e) {
    stream.getTracks().forEach(t => t.stop());
    V.cid = null; V.stream = null;
    toast(e.message);
  }
}

async function leaveVoice(notify = true) {
  const had = V.cid;
  [...V.pcs.keys()].forEach(closePeer);
  if (V.stream) V.stream.getTracks().forEach(t => t.stop());
  V.meters.forEach(m => { try { m.src.disconnect(); } catch { /* ok */ } });
  V.meters.clear(); V.speaking.clear();
  clearInterval(V.timer); V.timer = null;
  if (V.ctx) { V.ctx.close().catch(() => {}); V.ctx = null; }
  V.stream = null; V.cid = null;
  if (had && notify) { try { await api('POST', '/api/voice/leave'); } catch { /* ok */ } }
  renderSidebar(); renderMainIfVoice();
}

function applyMic() {
  const t = V.stream && V.stream.getAudioTracks()[0];
  if (t) t.enabled = !V.muted && !V.deafened;
  V.audios.forEach(a => { a.muted = V.deafened; });
}

function toggleMute() {
  if (V.deafened) { V.deafened = false; V.muted = V.prevMuted; }
  else V.muted = !V.muted;
  pushVoiceState();
}
function toggleDeafen() {
  if (V.deafened) { V.deafened = false; V.muted = V.prevMuted; }
  else { V.prevMuted = V.muted; V.deafened = true; V.muted = true; }
  pushVoiceState();
}
function pushVoiceState() {
  applyMic();
  if (V.cid) api('POST', '/api/voice/state', { muted: V.muted, deafened: V.deafened }).catch(() => {});
  renderSidebar(); renderMainIfVoice();
}

function signal(to, data) {
  api('POST', '/api/voice/signal', { to, data }).catch(() => {});
}

async function connectPeer(uid, initiator) {
  if (V.pcs.has(uid)) return V.pcs.get(uid);
  const pc = new RTCPeerConnection(ICE);
  V.pcs.set(uid, pc);
  V.stream.getTracks().forEach(t => pc.addTrack(t, V.stream));
  pc.onicecandidate = e => { if (e.candidate) signal(uid, { candidate: e.candidate }); };
  pc.ontrack = e => {
    let a = V.audios.get(uid);
    if (!a) {
      a = document.createElement('audio');
      a.autoplay = true;
      $('#audio-sink').appendChild(a);
      V.audios.set(uid, a);
    }
    a.srcObject = e.streams[0];
    a.muted = V.deafened;
    watchSpeaking(uid, e.streams[0]);
  };
  pc.onconnectionstatechange = () => {
    if (pc.connectionState === 'failed') closePeer(uid);
  };
  if (initiator) {
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    signal(uid, { sdp: pc.localDescription });
  }
  return pc;
}

async function onVoiceSignal({ from, data }) {
  if (!V.cid || !data) return;
  try {
    if (data.sdp) {
      const pc = V.pcs.get(from) || await connectPeer(from, false);
      await pc.setRemoteDescription(data.sdp);
      if (data.sdp.type === 'offer') {
        const ans = await pc.createAnswer();
        await pc.setLocalDescription(ans);
        signal(from, { sdp: pc.localDescription });
      }
      for (const c of V.pending.get(from) || []) await pc.addIceCandidate(c).catch(() => {});
      V.pending.delete(from);
    } else if (data.candidate) {
      const pc = V.pcs.get(from);
      if (pc && pc.remoteDescription) await pc.addIceCandidate(data.candidate).catch(() => {});
      else V.pending.set(from, [...(V.pending.get(from) || []), data.candidate]);
    }
  } catch (e) { console.warn('sinalização de voz falhou', e); }
}

function closePeer(uid) {
  const pc = V.pcs.get(uid);
  if (pc) { pc.close(); V.pcs.delete(uid); }
  const a = V.audios.get(uid);
  if (a) { a.srcObject = null; a.remove(); V.audios.delete(uid); }
  const m = V.meters.get(uid);
  if (m) { try { m.src.disconnect(); } catch { /* ok */ } V.meters.delete(uid); }
  V.pending.delete(uid);
  V.speaking.delete(uid);
}

/* Quem saiu da sala? Fecha as conexões correspondentes. */
function syncVoicePeers(users) {
  if (!V.cid) return;
  const ids = new Set(users.map(u => u.user_id));
  [...V.pcs.keys()].forEach(uid => { if (!ids.has(uid)) closePeer(uid); });
}

/* --------------------------------------------- indicador de quem fala */
function startMeters() {
  if (V.ctx) return;
  V.ctx = new (window.AudioContext || window.webkitAudioContext)();
  V.timer = setInterval(() => {
    const now = new Set();
    V.meters.forEach((m, uid) => {
      m.an.getByteFrequencyData(m.buf);
      let sum = 0;
      for (const v of m.buf) sum += v;
      const live = uid !== S.me.id || (!V.muted && !V.deafened);
      if (live && sum / m.buf.length > 14) now.add(uid);
    });
    let changed = now.size !== V.speaking.size;
    now.forEach(u => { if (!V.speaking.has(u)) changed = true; });
    if (changed) { V.speaking = now; paintSpeaking(); }
  }, 140);
}
function watchSpeaking(uid, stream) {
  if (!V.ctx) return;
  try {
    const src = V.ctx.createMediaStreamSource(stream);
    const an = V.ctx.createAnalyser();
    an.fftSize = 256;
    src.connect(an);
    V.meters.set(uid, { src, an, buf: new Uint8Array(an.frequencyBinCount) });
  } catch { /* stream sem áudio */ }
}
function paintSpeaking() {
  $$('[data-voice-uid]').forEach(el =>
    el.classList.toggle('speaking', V.speaking.has(+el.dataset.voiceUid)));
}
'''

ASSETS['views.js'] = r''''use strict';

/* ------------------------------------------------------------- ícones */
const ICONS = {
  hash: '<line x1="4" y1="9" x2="20" y2="9"/><line x1="4" y1="15" x2="20" y2="15"/><line x1="10" y1="3" x2="8" y2="21"/><line x1="16" y1="3" x2="14" y2="21"/>',
  volume: '<polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><path d="M15.5 8.5a5 5 0 0 1 0 7"/><path d="M19 5a10 10 0 0 1 0 14"/>',
  mic: '<path d="M12 2a3 3 0 0 0-3 3v7a3 3 0 0 0 6 0V5a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" y1="19" x2="12" y2="22"/>',
  'mic-off': '<path d="M12 2a3 3 0 0 0-3 3v7a3 3 0 0 0 6 0V5a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" y1="19" x2="12" y2="22"/><line x1="3" y1="3" x2="21" y2="21"/>',
  headphones: '<path d="M3 18v-6a9 9 0 0 1 18 0v6"/><path d="M21 19a2 2 0 0 1-2 2h-1a2 2 0 0 1-2-2v-3a2 2 0 0 1 2-2h3zM3 19a2 2 0 0 0 2 2h1a2 2 0 0 0 2-2v-3a2 2 0 0 0-2-2H3z"/>',
  'headphones-off': '<path d="M3 18v-6a9 9 0 0 1 18 0v6"/><path d="M21 19a2 2 0 0 1-2 2h-1a2 2 0 0 1-2-2v-3a2 2 0 0 1 2-2h3zM3 19a2 2 0 0 0 2 2h1a2 2 0 0 0 2-2v-3a2 2 0 0 0-2-2H3z"/><line x1="3" y1="3" x2="21" y2="21"/>',
  gear: '<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 1 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/>',
  plus: '<line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/>',
  search: '<circle cx="11" cy="11" r="7"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>',
  pin: '<path d="M12 17v5"/><path d="M9 3h6l-1 7 3 3v2H7v-2l3-3z"/>',
  users: '<path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/>',
  reply: '<polyline points="9 14 4 9 9 4"/><path d="M20 20v-7a4 4 0 0 0-4-4H4"/>',
  edit: '<path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4z"/>',
  trash: '<polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><path d="M10 11v6M14 11v6M9 6V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2"/>',
  smile: '<circle cx="12" cy="12" r="10"/><path d="M8 14s1.5 2 4 2 4-2 4-2"/><line x1="9" y1="9" x2="9.01" y2="9"/><line x1="15" y1="9" x2="15.01" y2="9"/>',
  paperclip: '<path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48"/>',
  send: '<line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/>',
  menu: '<line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="18" x2="21" y2="18"/>',
  x: '<line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>',
  'phone-off': '<path d="M10.68 13.31a16 16 0 0 0 3.41 2.6l1.27-1.27a2 2 0 0 1 2.11-.45 12.84 12.84 0 0 0 2.81.7 2 2 0 0 1 1.72 2v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.42 19.42 0 0 1-3.33-2.67m-2.67-3.34a19.79 19.79 0 0 1-3.07-8.63A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72 12.84 12.84 0 0 0 .7 2.81 2 2 0 0 1-.45 2.11L8.09 9.91"/><line x1="23" y1="1" x2="1" y2="23"/>',
  chevron: '<polyline points="6 9 12 15 18 9"/>',
  file: '<path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/><polyline points="13 2 13 9 20 9"/>',
  bubble: '<path d="M21 15a2 2 0 0 1-2 2H8l-5 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>',
  check: '<polyline points="20 6 9 17 4 12"/>',
};
function icon(name, size = 20) {
  return `<svg class="ic" width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${ICONS[name] || ''}</svg>`;
}

/* ------------------------------------------------------ seletores */
const curServer = () => S.servers.get(S.view.sid);
function curChannel() {
  if (S.view.kind === 'dm') return S.dms.find(d => d.id === S.view.cid);
  const s = curServer();
  return s && s.channels.find(c => c.id === S.view.cid);
}
function serverUnread(s) {
  let c = 0, m = 0;
  s.channels.forEach(ch => { const u = S.unread[ch.id]; if (u) { c += u.count; m += u.mentions; } });
  return { c, m };
}
const isAdmin = s => s && (s.my_role === 'admin' || s.my_role === 'owner');
const canModerate = () => S.view.kind === 'server' && isAdmin(curServer());

function renderAll() {
  renderRail(); renderSidebar(); renderMain(); renderPanel(); updateTitle();
}
function updateTitle() {
  let n = 0;
  Object.values(S.unread).forEach(u => { n += u.mentions; });
  document.title = (n ? `(${n}) ` : '') + 'Prosa';
}

/* --------------------------------------------------- barra de servidores */
function renderRail() {
  const home = S.view.kind !== 'server';
  const dmBadge = S.dms.reduce((n, d) => n + (S.unread[d.id]?.mentions || 0), 0);
  let h = `<button class="rail-btn${home ? ' active' : ''}" data-act="home" title="Mensagens diretas">
    <span class="sicon home-icon" style="--s:48px">${icon('bubble', 26)}</span><i class="pill"></i>${dmBadge ? `<b class="badge">${dmBadge}</b>` : ''}</button><hr>`;
  for (const s of S.servers.values()) {
    const u = serverUnread(s);
    h += `<button class="rail-btn${S.view.sid === s.id ? ' active' : ''}${u.c ? ' unread' : ''}" data-act="server" data-id="${s.id}" title="${esc(s.name)}">
      ${serverIcon(s)}<i class="pill"></i>${u.m ? `<b class="badge">${u.m}</b>` : ''}</button>`;
  }
  h += `<button class="rail-btn add" data-act="add-server" title="Criar ou entrar em um servidor"><span class="sicon add-icon" style="--s:48px">${icon('plus', 24)}</span></button>`;
  $('#rail').innerHTML = h;
}

/* -------------------------------------------------------------- lateral */
function renderSidebar() {
  const sb = $('#sidebar');
  const prev = $('.side-scroll', sb)?.scrollTop || 0;
  sb.innerHTML = (S.view.kind === 'server' ? serverSidebar() : homeSidebar()) + voiceBar() + walletChip() + userPanel();
  const sc = $('.side-scroll', sb);
  if (sc) sc.scrollTop = prev;
  paintSpeaking();
}
function badge(u) {
  if (!u) return '';
  return u.mentions ? `<b class="badge inline">${u.mentions}</b>` : '';
}
function homeSidebar() {
  const pending = [...S.friends.values()].filter(f => f.relation === 'incoming').length;
  const onFriends = S.view.kind === 'home';
  return `<div class="side-head"><span class="side-title">Conversas</span></div>
  <div class="side-scroll">
    <button class="side-item${onFriends ? ' active' : ''}" data-act="friends">${icon('users', 20)}<span>Amigos</span>${pending ? `<b class="badge inline">${pending}</b>` : ''}</button>
    <div class="side-label"><span>Mensagens diretas</span></div>
    ${S.dms.length ? S.dms.map(d => `<button class="side-item dm${S.view.cid === d.id ? ' active' : ''}${S.unread[d.id] ? ' unread' : ''}" data-act="dm" data-id="${d.id}">
        ${avatar(d.user, 32, true)}<span class="grow">${esc(U(d.user).display_name)}</span>${badge(S.unread[d.id])}</button>`).join('')
    : '<p class="side-empty">Nenhuma conversa ainda. Clique em um amigo para começar.</p>'}
  </div>`;
}
function serverSidebar() {
  const s = curServer();
  if (!s) return '';
  const adm = isAdmin(s);
  const chan = c => {
    const u = S.unread[c.id];
    const active = S.view.cid === c.id;
    const gear = adm ? `<span class="chan-gear" data-act="chan-settings" data-id="${c.id}" title="Editar canal" role="button">${icon('gear', 14)}</span>` : '';
    if (c.kind === 'text') {
      return `<div class="side-item chan${active ? ' active' : ''}${u ? ' unread' : ''}" data-act="channel" data-id="${c.id}" role="button" tabindex="0">
        ${icon('hash', 18)}<span class="grow">${esc(c.name)}</span>${badge(u)}${gear}</div>`;
    }
    const users = S.voice[c.id] || [];
    const here = V.cid === c.id;
    return `<div class="side-item chan voice${active ? ' active' : ''}${here ? ' live' : ''}" data-act="voice-channel" data-id="${c.id}" role="button" tabindex="0">
        ${icon('volume', 18)}<span class="grow">${esc(c.name)}</span>${gear}</div>
      ${users.map(vu => {
        const u = S.users.get(vu.user_id);
        return u ? `<div class="voice-user" data-voice-uid="${u.id}" data-act="profile" data-id="${u.id}" role="button">
          ${avatar(u, 22)}<span class="grow">${esc(displayName(u, s.id))}</span>
          ${vu.muted ? icon('mic-off', 14) : ''}${vu.deafened ? icon('headphones-off', 14) : ''}</div>` : '';
      }).join('')}`;
  };
  const text = s.channels.filter(c => c.kind === 'text');
  const voice = s.channels.filter(c => c.kind === 'voice');
  return `<button class="side-head server-head" data-act="server-menu" data-keep-popover><span class="side-title">${esc(s.name)}</span>${icon('chevron', 18)}</button>
  <div class="side-scroll">
    <div class="side-label"><span>Canais de texto</span>${adm ? `<button class="mini" data-act="new-channel" data-kind="text" title="Criar canal de texto">${icon('plus', 16)}</button>` : ''}</div>
    ${text.map(chan).join('')}
    <div class="side-label"><span>Canais de voz</span>${adm ? `<button class="mini" data-act="new-channel" data-kind="voice" title="Criar canal de voz">${icon('plus', 16)}</button>` : ''}</div>
    ${voice.map(chan).join('') || '<p class="side-empty">Sem canais de voz.</p>'}
  </div>`;
}
function voiceBar() {
  const vc = voiceChannel();
  if (!vc) return '';
  return `<div class="voice-bar"><div class="grow"><b>Voz conectada</b><span>${esc(vc.name)} · ${esc(vc.server.name)}</span></div>
    <button class="icon-btn" data-act="voice-leave" title="Desconectar">${icon('phone-off', 18)}</button></div>`;
}
function walletChip() {
  const b = S.wallet ? S.wallet.balance : 0;
  const shown = S.wallet && S.wallet.infinite ? '∞' : b.toLocaleString('pt-BR');
  return `<button class="wallet-chip" data-act="shop" title="Ganhe irisk e gaste na loja">${irisIcon(18)}<b>${shown}</b><span>irisk</span><em>Loja</em></button>`;
}
function userPanel() {
  const me = S.me;
  const st = me.raw_status === 'invisible' ? 'Invisível' : STATUS_LABEL[me.raw_status] || 'Online';
  return `<div class="user-panel">
    <button class="me" data-act="my-status" data-keep-popover title="Alterar status">${avatar({ ...me, status: me.raw_status === 'invisible' ? 'offline' : me.raw_status }, 34, true, true)}
      <span class="me-text"><b>${esc(me.display_name)}</b><small>${esc(me.custom_status || st)}</small></span></button>
    <button class="icon-btn${V.muted ? ' off' : ''}" data-act="mute" title="${V.muted ? 'Ativar microfone' : 'Silenciar microfone'}">${icon(V.muted ? 'mic-off' : 'mic', 19)}</button>
    <button class="icon-btn${V.deafened ? ' off' : ''}" data-act="deafen" title="${V.deafened ? 'Ativar áudio' : 'Desativar áudio'}">${icon(V.deafened ? 'headphones-off' : 'headphones', 19)}</button>
    <button class="icon-btn" data-act="settings" title="Configurações">${icon('gear', 19)}</button>
  </div>`;
}

/* ----------------------------------------------------------- principal */
function renderMain() {
  const m = $('#main');
  const v = S.view;
  $('#app').dataset.view = v.kind;
  if (v.kind === 'home') { m.innerHTML = friendsView(); return; }
  const ch = curChannel();
  if (!ch) {
    m.innerHTML = `<header class="main-head"><button class="icon-btn only-mobile" data-act="nav">${icon('menu')}</button></header>
      <div class="empty"><h2>Nada por aqui</h2><p class="muted">Escolha um canal na lateral.</p></div>`;
    return;
  }
  if (ch.kind === 'voice') { m.innerHTML = voiceView(ch); paintSpeaking(); return; }
  m.innerHTML = chatHeader(ch) + `<div class="chat"><div id="messages" class="messages"></div>${composerHTML(ch)}</div>`;
  const box = $('#messages');
  box._stick = true;
  box.addEventListener('scroll', () => {
    box._stick = box.scrollHeight - box.scrollTop - box.clientHeight < 140;
    if (box.scrollTop < 80) loadOlder();
  });
  box.addEventListener('load', () => { if (box._stick) box.scrollTop = box.scrollHeight; }, true);
  renderMessages('bottom');
  mountComposer();
}
function renderMainIfVoice() {
  const ch = curChannel();
  if (ch && ch.kind === 'voice') renderMain();
}

function chatHeader(ch) {
  const dm = ch.kind === 'dm';
  const left = dm
    ? `${avatar(ch.user, 28, true)}<h2>${esc(U(ch.user).display_name)}</h2><span class="topic">@${esc(ch.user.username)}</span>`
    : `<span class="head-icon">${icon('hash', 22)}</span><h2>${esc(ch.name)}</h2>${ch.topic ? `<span class="topic" title="${esc(ch.topic)}">${esc(ch.topic)}</span>` : ''}`;
  return `<header class="main-head">
    <button class="icon-btn only-mobile" data-act="nav">${icon('menu')}</button>
    ${left}<span class="grow"></span>
    <button class="icon-btn${S.panel === 'pins' ? ' active' : ''}" data-act="pins" title="Mensagens fixadas">${icon('pin')}</button>
    <button class="icon-btn${S.panel === 'members' ? ' active' : ''}" data-act="toggle-members" title="${dm ? 'Perfil' : 'Lista de membros'}">${icon('users')}</button>
    <form class="search-box" id="search-form">${icon('search', 16)}<input id="search-input" placeholder="Buscar" autocomplete="off" aria-label="Buscar mensagens"></form>
  </header>`;
}

/* --------------------------------------------------------- mensagens */
function beginning(ch) {
  if (!ch) return '';
  if (ch.kind === 'dm') return `<div class="beginning">${avatar(ch.user, 72)}<h3>${esc(U(ch.user).display_name)}</h3>
    <p class="muted">Este é o começo da sua conversa com <b>@${esc(ch.user.username)}</b>.</p></div>`;
  return `<div class="beginning"><div class="beginning-icon">${icon('hash', 34)}</div><h3>Bem-vindo a #${esc(ch.name)}</h3>
    <p class="muted">Este é o começo do canal.</p></div>`;
}

function attachmentHTML(a) {
  const url = esc(a.url);
  if (a.mime.startsWith('image/') && a.mime !== 'image/svg+xml')
    return `<img class="att-img" src="${url}" alt="${esc(a.filename)}" data-act="lightbox" loading="lazy">`;
  if (a.mime.startsWith('video/')) return `<video class="att-video" src="${url}" controls preload="metadata"></video>`;
  if (a.mime.startsWith('audio/')) return `<audio src="${url}" controls preload="metadata"></audio>`;
  return `<a class="att-file" href="${url}" download="${esc(a.filename)}">${icon('file', 28)}<span><b>${esc(a.filename)}</b><small>${fmtSize(a.size)}</small></span></a>`;
}

function msgHTML(m, prev, sid) {
  const au = U(m.author);
  const grouped = prev && prev.author && m.author && prev.author.id === m.author.id && !m.reply_to
    && m.created_at - prev.created_at < 7 * 60000;
  const mine = m.author && m.author.id === S.me.id;
  const mem = sid && au && S.members.get(sid)?.get(au.id);
  const name = esc(displayName(au, sid) || m.nick);
  const tag = (mem && mem.role !== 'member' ? `<span class="tag ${mem.role}">${ROLE_LABEL[mem.role]}</span>` : '')
    + (au && au.site_owner ? '<span class="tag site" title="Dono do site">DONO DO SITE</span>' : '')
    + (nitroActive(au) ? '<span class="tag nitro" title="Assinante Nitro">NITRO</span>' : '');
  const mention = !mine && mentionsMe(m.content);
  const dm = S.view.kind === 'dm';
  const canDel = mine || (!dm && canModerate());
  const canPin = dm || canModerate();
  let body;
  if (S.editing === m.id) {
    body = `<textarea id="edit-box" class="edit-box" rows="1">${esc(m.content)}</textarea>
      <div class="edit-hint">Esc para <a data-act="edit-cancel">cancelar</a> · Enter para <a data-act="edit-save">salvar</a></div>`;
  } else {
    body = (m.content ? `<div class="msg-text">${formatText(m.content, sid)}${m.edited_at ? '<span class="edited" title="' + esc(fmtStamp(m.edited_at)) + '"> (editada)</span>' : ''}</div>` : '')
      + (m.attachments.length ? `<div class="atts">${m.attachments.map(attachmentHTML).join('')}</div>` : '');
  }
  const reacts = m.reactions.length
    ? `<div class="reactions">${m.reactions.map(r => `<button class="react${r.users.includes(S.me.id) ? ' mine' : ''}" data-act="react" data-id="${m.id}" data-e="${esc(r.emoji)}" title="${r.users.length} reação(ões)">${esc(r.emoji)}<span>${r.users.length}</span></button>`).join('')}<button class="react add" data-act="react-add" data-id="${m.id}" data-keep-popover title="Adicionar reação">${icon('smile', 15)}</button></div>` : '';
  const reply = m.reply_to
    ? `<button class="reply-ref" data-act="jump" data-id="${m.reply_to.id}">${icon('reply', 14)}<b>${esc(m.reply_to.author)}</b><span>${esc(m.reply_to.content)}</span></button>` : '';
  const tools = S.editing === m.id ? '' : `<div class="msg-tools">
      <button data-act="react-add" data-id="${m.id}" data-keep-popover title="Reagir">${icon('smile', 17)}</button>
      <button data-act="reply" data-id="${m.id}" title="Responder">${icon('reply', 17)}</button>
      ${mine ? `<button data-act="edit" data-id="${m.id}" title="Editar">${icon('edit', 17)}</button>` : ''}
      ${canPin ? `<button data-act="msg-pin" data-id="${m.id}" title="${m.pinned ? 'Desafixar' : 'Fixar'}">${icon('pin', 17)}</button>` : ''}
      ${canDel ? `<button class="danger" data-act="msg-del" data-id="${m.id}" title="Excluir">${icon('trash', 17)}</button>` : ''}
    </div>`;
  const head = grouped ? '' : `<div class="msg-head"><button class="author${nitroActive(au) ? ' nitro-name' : ''}" data-act="profile" data-id="${au?.id}">${name}</button>${tag}<time>${fmtStamp(m.created_at)}</time>${m.pinned ? `<span class="pinned-flag" title="Fixada">${icon('pin', 13)}</span>` : ''}</div>`;
  const gutter = grouped
    ? `<time class="gutter-time">${tf.format(m.created_at)}</time>`
    : `<button class="gutter-avatar" data-act="profile" data-id="${au?.id}" tabindex="-1">${avatar(au, 40)}</button>`;
  return `<article class="msg${grouped ? ' grouped' : ''}${mention ? ' mentioned' : ''}${m.pinned ? ' is-pinned' : ''}" data-id="${m.id}">
    ${reply}<div class="msg-row">${gutter}<div class="msg-body">${head}${body}${reacts}</div></div>${tools}</article>`;
}

function renderMessages(mode = 'keep') {
  const box = $('#messages');
  if (!box) return;
  const st = S.msgs.get(S.view.cid);
  if (!st) { box.innerHTML = '<div class="loading">Carregando mensagens…</div>'; return; }
  const ch = curChannel();
  const prevH = box.scrollHeight, prevTop = box.scrollTop;
  const stick = box._stick;
  let h = st.hasMore ? '<div class="loading">Carregando mensagens antigas…</div>' : beginning(ch);
  let prev = null;
  for (const m of st.list) {
    if (!prev || !sameDay(prev.created_at, m.created_at)) {
      h += `<div class="day-sep"><span>${fmtDay(m.created_at)}</span></div>`;
      prev = null;
    }
    h += msgHTML(m, prev, S.view.sid);
    prev = m;
  }
  box.innerHTML = h;
  if (mode === 'bottom' || (mode === 'keep' && stick)) { box.scrollTop = box.scrollHeight; box._stick = true; }
  else if (mode === 'prepend') box.scrollTop = box.scrollHeight - prevH + prevTop;
  else box.scrollTop = prevTop;
  if (S.editing) {
    const eb = $('#edit-box');
    if (eb) { eb.focus(); eb.setSelectionRange(eb.value.length, eb.value.length); autosize(eb); }
  }
}

async function ensureMessages(cid) {
  let st = S.msgs.get(cid);
  if (st && st.loaded) return st;
  const d = await api('GET', `/api/channels/${cid}/messages?limit=50`);
  d.messages.forEach(m => { if (m.author && !S.users.has(m.author.id)) cacheUser(m.author); });
  st = { list: d.messages, hasMore: d.has_more, loaded: true };
  S.msgs.set(cid, st);
  return st;
}
async function loadOlder() {
  const cid = S.view.cid;
  const st = S.msgs.get(cid);
  if (!st || !st.hasMore || st.loading || !st.list.length) return;
  st.loading = true;
  try {
    const d = await api('GET', `/api/channels/${cid}/messages?limit=50&before=${st.list[0].id}`);
    d.messages.forEach(m => { if (m.author && !S.users.has(m.author.id)) cacheUser(m.author); });
    st.list = [...d.messages, ...st.list];
    st.hasMore = d.has_more;
  } catch (e) { toast(e.message); } finally { st.loading = false; }
  if (S.view.cid === cid) renderMessages('prepend');
}
function addMessage(m) {
  const st = S.msgs.get(m.channel_id);
  if (!st || !st.loaded || st.list.some(x => x.id === m.id)) return false;
  st.list.push(m);
  return true;
}

/* ------------------------------------------------------------ compositor */
function composerHTML(ch) {
  const ph = ch.kind === 'dm' ? `Conversar com @${ch.user.username}` : `Conversar em #${ch.name}`;
  return `<div class="composer-area">
    <div id="mention-pop" class="mention-pop hidden"></div>
    <div id="reply-bar"></div><div id="file-chips"></div>
    <div class="composer">
      <button class="icon-btn" data-act="attach" title="Anexar arquivos">${icon('paperclip')}</button>
      <input type="file" id="file-input" multiple hidden>
      <textarea id="composer-input" rows="1" maxlength="4000" placeholder="${esc(ph)}" aria-label="Mensagem"></textarea>
      <button class="icon-btn" data-act="emoji" data-keep-popover title="Emojis">${icon('smile')}</button>
      <button class="icon-btn send" data-act="send" title="Enviar">${icon('send')}</button>
    </div>
    <div id="typing" class="typing"></div>
  </div>`;
}
function autosize(ta) {
  ta.style.height = 'auto';
  ta.style.height = Math.min(ta.scrollHeight, 220) + 'px';
}
function mountComposer() {
  const ta = $('#composer-input');
  if (!ta) return;
  ta.value = S.drafts[S.view.cid] || '';
  autosize(ta);
  renderReplyBar(); renderChips(); renderTyping();
  if (innerWidth > 800) ta.focus();
}
function renderReplyBar() {
  const el = $('#reply-bar');
  if (!el) return;
  if (!S.reply) { el.innerHTML = ''; return; }
  const m = S.reply;
  el.innerHTML = `<div class="reply-bar">Respondendo a <b>${esc(displayName(U(m.author), S.view.sid))}</b>
    <button class="icon-btn" data-act="reply-cancel" title="Cancelar resposta">${icon('x', 16)}</button></div>`;
}
function renderChips() {
  const el = $('#file-chips');
  if (!el) return;
  el.innerHTML = S.files.length ? `<div class="chips">${S.files.map((f, i) =>
    `<span class="chip">${icon('file', 14)}<span>${esc(f.name)}</span><small>${fmtSize(f.size)}</small>
     <button data-act="chip-remove" data-i="${i}" title="Remover">${icon('x', 14)}</button></span>`).join('')}</div>` : '';
}
function renderTyping() {
  const el = $('#typing');
  if (!el) return;
  const t = S.typing[S.view.cid] || {};
  const names = Object.values(t).map(x => x.name);
  el.textContent = !names.length ? '' : names.length === 1 ? `${names[0]} está digitando…`
    : names.length === 2 ? `${names[0]} e ${names[1]} estão digitando…` : 'Várias pessoas estão digitando…';
}

/* --------------------------------------------------------------- painel */
function renderPanel() {
  const p = $('#side-panel');
  const v = S.view;
  let html = '';
  if (v.kind === 'server' && curChannel()) {
    if (S.panel === 'members') html = membersPanel();
    else if (S.panel === 'search' || S.panel === 'pins') html = listPanel();
  } else if (v.kind === 'dm') {
    if (S.panel === 'members') html = dmProfilePanel();
    else if (S.panel === 'search' || S.panel === 'pins') html = listPanel();
  }
  p.innerHTML = html;
  $('#app').classList.toggle('panel-open', !!html);
}
function membersPanel() {
  const map = S.members.get(S.view.sid);
  if (!map) return '<div class="panel-head">Membros</div><div class="loading">Carregando…</div>';
  const all = [...map.values()];
  const on = m => m.status !== 'offline';
  const byName = (a, b) => (a.nickname || a.display_name).localeCompare(b.nickname || b.display_name);
  const staff = all.filter(m => m.role !== 'member' && on(m)).sort(byName);
  const online = all.filter(m => m.role === 'member' && on(m)).sort(byName);
  const offline = all.filter(m => !on(m)).sort(byName);
  const row = m => `<button class="member${on(m) ? '' : ' off'}" data-act="profile" data-id="${m.id}">
    ${avatar(m, 32, true)}<span class="member-text"><b>${esc(m.nickname || m.display_name)}</b>${m.custom_status ? `<small>${esc(m.custom_status)}</small>` : ''}</span>
    ${m.role !== 'member' ? `<span class="tag ${m.role}">${ROLE_LABEL[m.role]}</span>` : ''}</button>`;
  const group = (t, l) => l.length ? `<div class="side-label"><span>${t} — ${l.length}</span></div>${l.map(row).join('')}` : '';
  return `<div class="panel-head">Membros</div><div class="panel-scroll">${group('Equipe online', staff)}${group('Online', online)}${group('Offline', offline)}</div>`;
}
function dmProfilePanel() {
  const ch = curChannel();
  if (!ch) return '';
  const u = U(ch.user);
  return `<div class="panel-head">Perfil</div><div class="panel-scroll"><div class="profile-card">
    <div class="banner" style="background:${AVATAR_COLORS[u.id % AVATAR_COLORS.length]}"></div>
    <div class="pc-avatar">${avatar(u, 76, true)}</div>
    <h3>${esc(u.display_name)}</h3><p class="muted">@${esc(u.username)} · ${STATUS_LABEL[u.status]}</p>
    ${u.custom_status ? `<p>${esc(u.custom_status)}</p>` : ''}
    ${u.bio ? `<div class="pc-section"><b>Sobre</b><p>${esc(u.bio)}</p></div>` : ''}
    <div class="pc-section"><b>Conta criada em</b><p>${df.format(u.created_at)}</p></div></div></div>`;
}
function listPanel() {
  const d = S.panelData;
  const title = S.panel === 'pins' ? 'Mensagens fixadas' : `Resultados para “${esc(d?.q || '')}”`;
  if (!d) return `<div class="panel-head">${title}<button class="icon-btn" data-act="panel-close">${icon('x', 16)}</button></div><div class="loading">Buscando…</div>`;
  const items = d.items.length ? d.items.map(m => {
    const au = U(m.author);
    return `<div class="mini-msg"><div class="mini-head"><b>${esc(displayName(au, S.view.sid))}</b>
      <time>${fmtStamp(m.created_at)}</time>${m.channel_name ? `<span class="muted">#${esc(m.channel_name)}</span>` : ''}</div>
      <div class="msg-text">${formatText(m.content || '(anexo)', S.view.sid)}</div>
      <div class="mini-actions"><button class="link" data-act="goto" data-cid="${m.channel_id}" data-id="${m.id}">Ir para a conversa</button>
      ${S.panel === 'pins' && (S.view.kind === 'dm' || canModerate()) ? `<button class="link" data-act="msg-pin" data-id="${m.id}">Desafixar</button>` : ''}</div></div>`;
  }).join('') : `<div class="empty small"><p class="muted">${S.panel === 'pins' ? 'Nenhuma mensagem fixada.' : 'Nada encontrado.'}</p></div>`;
  return `<div class="panel-head"><span>${title}</span><button class="icon-btn" data-act="panel-close" title="Fechar">${icon('x', 16)}</button></div><div class="panel-scroll">${items}</div>`;
}

/* --------------------------------------------------------------- amigos */
function friendsView() {
  const tab = S.view.tab || 'online';
  const list = [...S.friends.values()];
  const friends = list.filter(f => f.relation === 'friend');
  const pending = list.filter(f => f.relation !== 'friend');
  const tabBtn = (id, label, extra = '') =>
    `<button class="tab${tab === id ? ' active' : ''}" data-act="tab" data-tab="${id}">${label}${extra}</button>`;
  const inc = pending.filter(f => f.relation === 'incoming').length;
  let body;
  if (tab === 'add') {
    body = `<form class="add-friend" id="add-friend-form"><h3>Adicionar amigo</h3>
      <p class="muted">Digite o nome de usuário da pessoa (sem o @).</p>
      <div class="inline-form"><input id="friend-input" placeholder="nome_de_usuario" autocomplete="off" required>
      <button class="btn primary">Enviar pedido</button></div><div id="friend-msg" class="form-msg"></div></form>`;
  } else {
    const rows = tab === 'pending' ? pending : tab === 'online' ? friends.filter(f => f.user.status !== 'offline') : friends;
    const label = tab === 'pending' ? 'Pendentes' : tab === 'online' ? 'Online' : 'Todos os amigos';
    body = rows.length ? `<div class="side-label"><span>${label} — ${rows.length}</span></div>` + rows.map(f => {
      const u = f.user;
      const sub = f.relation === 'incoming' ? 'Pedido recebido' : f.relation === 'outgoing' ? 'Pedido enviado'
        : (u.custom_status || STATUS_LABEL[u.status]);
      const acts = f.relation === 'incoming'
        ? `<button class="round ok" data-act="friend-accept" data-id="${u.id}" title="Aceitar">${icon('check', 18)}</button><button class="round" data-act="friend-remove" data-id="${u.id}" title="Recusar">${icon('x', 18)}</button>`
        : f.relation === 'outgoing'
          ? `<button class="round" data-act="friend-remove" data-id="${u.id}" title="Cancelar pedido">${icon('x', 18)}</button>`
          : `<button class="round" data-act="dm-user" data-id="${u.id}" title="Enviar mensagem">${icon('bubble', 18)}</button><button class="round" data-act="friend-remove" data-id="${u.id}" data-confirm="1" title="Remover amigo">${icon('x', 18)}</button>`;
      return `<div class="friend-row" data-act="profile" data-id="${u.id}" role="button">${avatar(u, 40, true)}
        <div class="grow"><b>${esc(u.display_name)}</b><small>@${esc(u.username)} · ${esc(sub)}</small></div><div class="row-actions">${acts}</div></div>`;
    }).join('') : `<div class="empty"><h2>${tab === 'pending' ? 'Sem pedidos pendentes' : tab === 'online' ? 'Ninguém online agora' : 'Sua lista está vazia'}</h2>
      <p class="muted">${tab === 'all' || tab === 'online' ? 'Adicione amigos pela aba “Adicionar amigo”.' : 'Pedidos enviados e recebidos aparecem aqui.'}</p></div>`;
  }
  return `<header class="main-head"><button class="icon-btn only-mobile" data-act="nav">${icon('menu')}</button>
    <span class="head-icon">${icon('users', 22)}</span><h2>Amigos</h2>
    <nav class="tabs">${tabBtn('online', 'Online')}${tabBtn('all', 'Todos')}${tabBtn('pending', 'Pendentes', inc ? `<b class="badge inline">${inc}</b>` : '')}${tabBtn('add', 'Adicionar amigo')}</nav></header>
    <div class="friends-body">${body}</div>`;
}

/* ------------------------------------------------------------------ voz */
function voiceView(ch) {
  const users = S.voice[ch.id] || [];
  const inRoom = V.cid === ch.id;
  const sid = ch.server_id;
  const tiles = users.map(vu => {
    const u = S.users.get(vu.user_id) || { id: vu.user_id, username: '?', display_name: '?' };
    return `<div class="tile" data-voice-uid="${u.id}">${avatar(u, 92)}
      <div class="tile-name">${esc(displayName(u, sid))}${vu.muted ? icon('mic-off', 15) : ''}${vu.deafened ? icon('headphones-off', 15) : ''}</div></div>`;
  }).join('');
  const controls = inRoom
    ? `<button class="round big${V.muted ? ' off' : ''}" data-act="mute" title="Microfone">${icon(V.muted ? 'mic-off' : 'mic', 22)}</button>
       <button class="round big${V.deafened ? ' off' : ''}" data-act="deafen" title="Áudio">${icon(V.deafened ? 'headphones-off' : 'headphones', 22)}</button>
       <button class="round big danger" data-act="voice-leave" title="Sair da chamada">${icon('phone-off', 22)}</button>`
    : `<button class="btn primary" data-act="voice-join" data-id="${ch.id}">Entrar na sala de voz</button>`;
  return `<header class="main-head"><button class="icon-btn only-mobile" data-act="nav">${icon('menu')}</button>
    <span class="head-icon">${icon('volume', 22)}</span><h2>${esc(ch.name)}</h2><span class="grow"></span>
    <button class="icon-btn${S.panel === 'members' ? ' active' : ''}" data-act="toggle-members" title="Lista de membros">${icon('users')}</button></header>
    <div class="voice-stage">
      ${users.length ? `<div class="tiles">${tiles}</div>` : `<div class="empty"><h2>Ninguém na sala ainda</h2><p class="muted">Entre para começar a conversar por voz.</p></div>`}
      <div class="voice-controls">${controls}</div>
    </div>`;
}
'''

ASSETS['modals.js'] = r''''use strict';

/* -------------------------------------------- criar / entrar em servidor */
function openAddServer(code = '') {
  const m = openModal(`<h2>Adicionar um servidor</h2>
    <p class="muted">Crie um espaço para a sua comunidade ou entre em um que já existe.</p>
    <div class="tabs modal-tabs"><button class="tab${code ? '' : ' active'}" data-t="create">Criar</button><button class="tab${code ? ' active' : ''}" data-t="join">Entrar com convite</button></div>
    <form id="create-form" class="${code ? 'hidden' : ''}">
      <label>Nome do servidor<input id="srv-name" maxlength="50" required minlength="2" placeholder="Ex.: Clube do Livro"></label>
      <div class="modal-actions"><button type="button" class="btn ghost" data-close>Cancelar</button><button class="btn primary">Criar servidor</button></div>
    </form>
    <form id="join-form" class="${code ? '' : 'hidden'}">
      <label>Código ou link de convite<input id="srv-code" required placeholder="Ex.: aB3xY9q" value="${esc(code)}"></label>
      <div class="modal-actions"><button type="button" class="btn ghost" data-close>Cancelar</button><button class="btn primary">Entrar no servidor</button></div>
    </form>`);
  $$('.modal-tabs .tab', m).forEach(t => t.onclick = () => {
    $$('.modal-tabs .tab', m).forEach(x => x.classList.toggle('active', x === t));
    $('#create-form', m).classList.toggle('hidden', t.dataset.t !== 'create');
    $('#join-form', m).classList.toggle('hidden', t.dataset.t !== 'join');
    $(t.dataset.t === 'create' ? '#srv-name' : '#srv-code', m).focus();
  });
  $('#create-form', m).onsubmit = e => {
    e.preventDefault();
    act(async () => {
      const s = await api('POST', '/api/servers', { name: $('#srv-name', m).value });
      closeModal(); addServer(s); goServer(s.id);
    });
  };
  $('#join-form', m).onsubmit = e => {
    e.preventDefault();
    act(async () => {
      const s = await api('POST', '/api/servers/join', { code: $('#srv-code', m).value });
      closeModal(); addServer(s); goServer(s.id);
    });
  };
}

/* -------------------------------------------------------------- canais */
function openNewChannel(kind = 'text') {
  const s = curServer();
  const m = openModal(`<h2>Criar canal</h2>
    <form id="nc-form">
      <div class="radio-cards">
        <label class="radio-card"><input type="radio" name="kind" value="text" ${kind === 'text' ? 'checked' : ''}>${icon('hash', 22)}<span><b>Texto</b><small>Mensagens, imagens e arquivos</small></span></label>
        <label class="radio-card"><input type="radio" name="kind" value="voice" ${kind === 'voice' ? 'checked' : ''}>${icon('volume', 22)}<span><b>Voz</b><small>Converse por áudio em tempo real</small></span></label>
      </div>
      <label>Nome do canal<input id="nc-name" maxlength="32" required placeholder="novo-canal"></label>
      <label id="nc-topic-wrap">Descrição (opcional)<input id="nc-topic" maxlength="200"></label>
      <div class="modal-actions"><button type="button" class="btn ghost" data-close>Cancelar</button><button class="btn primary">Criar canal</button></div>
    </form>`);
  const sync = () => $('#nc-topic-wrap', m).classList.toggle('hidden',
    $('input[name=kind]:checked', m).value !== 'text');
  $$('input[name=kind]', m).forEach(r => r.onchange = sync);
  sync();
  $('#nc-form', m).onsubmit = e => {
    e.preventDefault();
    act(async () => {
      const k = $('input[name=kind]:checked', m).value;
      const c = await api('POST', `/api/servers/${s.id}/channels`,
        { name: $('#nc-name', m).value, kind: k, topic: $('#nc-topic', m).value });
      closeModal();
      if (k === 'text') openChannel(s.id, c.id);
    });
  };
}

function openChannelSettings(cid) {
  const s = curServer();
  const c = s.channels.find(x => x.id === cid);
  if (!c) return;
  const m = openModal(`<h2>Editar canal</h2>
    <form id="cs-form">
      <label>Nome<input id="cs-name" maxlength="32" required value="${esc(c.name)}"></label>
      ${c.kind === 'text' ? `<label>Descrição<input id="cs-topic" maxlength="200" value="${esc(c.topic)}"></label>` : ''}
      <div class="modal-actions split"><button type="button" class="btn danger ghost" id="cs-del">Excluir canal</button>
        <span><button type="button" class="btn ghost" data-close>Cancelar</button><button class="btn primary">Salvar</button></span></div>
    </form>`);
  $('#cs-form', m).onsubmit = e => {
    e.preventDefault();
    act(async () => {
      await api('PATCH', `/api/channels/${cid}`,
        { name: $('#cs-name', m).value, topic: $('#cs-topic', m)?.value ?? '' });
      closeModal();
    });
  };
  $('#cs-del', m).onclick = () => confirmBox('Excluir canal',
    `“${c.name}” e todas as suas mensagens serão apagadas para sempre.`, 'Excluir canal',
    () => api('DELETE', `/api/channels/${cid}`));
}

/* ------------------------------------------------------------- convite */
function inviteLink(s) { return `${location.origin}/#invite=${s.invite_code}`; }
function openInvite() {
  const s = curServer();
  const m = openModal(`<h2>Convidar pessoas para ${esc(s.name)}</h2>
    <p class="muted">Compartilhe este link. Quem tiver uma conta pode entrar usando-o.</p>
    <div class="inline-form"><input id="inv-link" readonly value="${esc(inviteLink(s))}"><button class="btn primary" id="inv-copy">Copiar</button></div>
    <p class="muted small">Código: <b>${esc(s.invite_code)}</b></p>
    <div class="modal-actions"><button class="btn ghost" data-close>Fechar</button></div>`);
  $('#inv-copy', m).onclick = () => copyText($('#inv-link', m).value);
  $('#inv-link', m).onfocus = e => e.target.select();
}
async function copyText(t) {
  try { await navigator.clipboard.writeText(t); toast('Copiado!'); }
  catch { toast('Não foi possível copiar. Selecione o texto e use Ctrl+C.'); }
}

/* ------------------------------------------------- configurações do servidor */
function openServerSettings() {
  const s = curServer();
  const owner = s.my_role === 'owner';
  let color = s.color;
  const m = openModal(`<h2>Configurações do servidor</h2>
    <form id="ss-form">
      <label>Nome<input id="ss-name" maxlength="50" required minlength="2" value="${esc(s.name)}"></label>
      <div class="field"><span class="label">Cor do ícone</span><div class="swatches">${AVATAR_COLORS.map(c =>
        `<button type="button" class="swatch${c === color ? ' active' : ''}" style="background:${c}" data-c="${c}" aria-label="Cor ${c}"></button>`).join('')}</div></div>
      <div class="field"><span class="label">Convite</span>
        <div class="inline-form"><input readonly id="ss-inv" value="${esc(inviteLink(s))}"><button type="button" class="btn ghost" id="ss-copy">Copiar</button><button type="button" class="btn ghost" id="ss-reset">Gerar novo</button></div></div>
      <div class="field"><span class="label">Membros banidos</span><div id="ss-bans" class="ban-list muted small">Carregando…</div></div>
      <div class="modal-actions split">
        <button type="button" class="btn danger ghost" id="ss-danger">${owner ? 'Excluir servidor' : 'Sair do servidor'}</button>
        <span><button type="button" class="btn ghost" data-close>Cancelar</button><button class="btn primary">Salvar</button></span></div>
    </form>`, { wide: false });
  $$('.swatch', m).forEach(b => b.onclick = () => {
    color = b.dataset.c;
    $$('.swatch', m).forEach(x => x.classList.toggle('active', x === b));
  });
  $('#ss-copy', m).onclick = () => copyText($('#ss-inv', m).value);
  $('#ss-reset', m).onclick = () => act(async () => {
    const r = await api('POST', `/api/servers/${s.id}/invite/reset`);
    s.invite_code = r.invite_code;
    $('#ss-inv', m).value = inviteLink(s);
    toast('Novo convite gerado. O link antigo não funciona mais.');
  });
  $('#ss-form', m).onsubmit = e => {
    e.preventDefault();
    act(async () => { await api('PATCH', `/api/servers/${s.id}`, { name: $('#ss-name', m).value, color }); closeModal(); });
  };
  $('#ss-danger', m).onclick = () => owner
    ? confirmBox('Excluir servidor', `“${s.name}” será apagado para todos, com todos os canais e mensagens.`, 'Excluir servidor',
      () => api('DELETE', `/api/servers/${s.id}`))
    : confirmBox('Sair do servidor', `Você sairá de “${s.name}”. Poderá voltar com um convite.`, 'Sair',
      () => api('POST', `/api/servers/${s.id}/leave`));
  api('GET', `/api/servers/${s.id}/bans`).then(list => {
    const box = $('#ss-bans', m);
    if (!box) return;
    box.innerHTML = list.length ? list.map(u => `<div class="ban-row"><span>@${esc(u.username)}</span>
      <button type="button" class="link" data-unban="${u.id}">Desbanir</button></div>`).join('') : 'Ninguém foi banido.';
    box.onclick = ev => {
      const b = ev.target.closest('[data-unban]');
      if (b) act(async () => { await api('DELETE', `/api/servers/${s.id}/bans/${b.dataset.unban}`); b.closest('.ban-row').remove(); });
    };
  }).catch(() => {});
}

function openNickname() {
  const s = curServer();
  const cur = S.members.get(s.id)?.get(S.me.id)?.nickname || '';
  const m = openModal(`<h2>Apelido neste servidor</h2><form id="nk-form">
    <label>Apelido<input id="nk-input" maxlength="32" value="${esc(cur)}" placeholder="${esc(S.me.display_name)}"></label>
    <p class="muted small">Deixe em branco para usar seu nome de exibição.</p>
    <div class="modal-actions"><button type="button" class="btn ghost" data-close>Cancelar</button><button class="btn primary">Salvar</button></div></form>`);
  $('#nk-form', m).onsubmit = e => {
    e.preventDefault();
    act(async () => { await api('POST', `/api/servers/${s.id}/nickname`, { nickname: $('#nk-input', m).value }); closeModal(); });
  };
}

function serverMenu(anchor) {
  const s = curServer();
  menu(anchor, [
    { label: 'Convidar pessoas', run: openInvite },
    isAdmin(s) && { label: 'Criar canal', run: () => openNewChannel('text') },
    isAdmin(s) && { label: 'Configurações do servidor', run: openServerSettings },
    { sep: true },
    { label: 'Alterar apelido', run: openNickname },
    s.my_role !== 'owner' && { label: 'Sair do servidor', danger: true, run: () => confirmBox('Sair do servidor',
      `Você sairá de “${s.name}”.`, 'Sair', () => api('POST', `/api/servers/${s.id}/leave`)) },
  ], { align: 'left' });
}

/* ---------------------------------------------------- status do usuário */
function statusMenu(anchor) {
  const set = st => act(async () => {
    const me = await api('PATCH', '/api/me', { status: st });
    Object.assign(S.me, me); renderSidebar();
  });
  menu(anchor, [
    { label: 'Online', run: () => set('online') },
    { label: 'Ausente', run: () => set('idle') },
    { label: 'Não perturbe', run: () => set('dnd') },
    { label: 'Invisível', run: () => set('invisible') },
    { sep: true },
    { label: 'Meu perfil e status personalizado…', run: openSettings },
  ], { align: 'left', up: true });
}

/* ------------------------------------------------------ configurações */
function openSettings() {
  const me = S.me;
  const notif = 'Notification' in window ? Notification.permission : 'denied';
  const m = openModal(`<h2>Configurações da conta</h2>
    <form id="st-form">
      <div class="st-avatar">${avatar(me, 72, false, true)}
        <div><button type="button" class="btn ghost" id="st-up">Trocar foto</button>
        ${me.avatar ? '<button type="button" class="btn ghost" id="st-rm">Remover</button>' : ''}
        <input type="file" id="st-file" accept="image/png,image/jpeg,image/gif,image/webp" hidden>
        <p class="muted small">PNG, JPG, GIF ou WebP até 4 MB.</p></div></div>
      <label>Nome de exibição<input id="st-name" maxlength="32" value="${esc(me.display_name)}"></label>
      <label>Status personalizado<input id="st-cs" maxlength="60" value="${esc(me.custom_status)}" placeholder="O que você está fazendo?"></label>
      <label>Sobre mim<textarea id="st-bio" maxlength="300" rows="3">${esc(me.bio)}</textarea></label>
      <div class="modal-actions"><button type="button" class="btn ghost" data-close>Fechar</button><button class="btn primary">Salvar perfil</button></div>
    </form>
    <hr class="sep">
    <h3>Notificações</h3>
    <p class="muted small">Receba avisos do navegador para menções e mensagens diretas quando a aba estiver em segundo plano.</p>
    <button class="btn ghost" id="st-notif" ${notif === 'granted' || notif === 'denied' ? 'disabled' : ''}>${notif === 'granted' ? 'Notificações ativadas' : notif === 'denied' ? 'Bloqueadas no navegador' : 'Ativar notificações'}</button>
    <hr class="sep">
    <h3>Alterar senha</h3>
    <form id="pw-form"><label>Senha atual<input type="password" id="pw-old" autocomplete="current-password" required></label>
      <label>Nova senha<input type="password" id="pw-new" autocomplete="new-password" minlength="6" required></label>
      <button class="btn ghost">Alterar senha</button></form>
    <hr class="sep">
    <button class="btn danger" id="st-logout">Sair da conta</button>`);
  $('#st-up', m).onclick = () => $('#st-file', m).click();
  $('#st-file', m).onchange = e => act(async () => {
    const f = e.target.files[0];
    if (!f) return;
    const fd = new FormData(); fd.append('file', f);
    Object.assign(S.me, await api('POST', '/api/me/avatar', fd));
    cacheUser(S.me); renderAll(); openSettings();
  });
  const rm = $('#st-rm', m);
  if (rm) rm.onclick = () => act(async () => {
    Object.assign(S.me, await api('DELETE', '/api/me/avatar'));
    S.me.avatar = null; cacheUser(S.me); renderAll(); openSettings();
  });
  $('#st-form', m).onsubmit = e => {
    e.preventDefault();
    act(async () => {
      Object.assign(S.me, await api('PATCH', '/api/me', {
        display_name: $('#st-name', m).value, custom_status: $('#st-cs', m).value, bio: $('#st-bio', m).value }));
      renderAll(); toast('Perfil salvo.');
    });
  };
  $('#st-notif', m).onclick = async () => {
    const r = await Notification.requestPermission();
    toast(r === 'granted' ? 'Notificações ativadas.' : 'Permissão negada.');
    openSettings();
  };
  $('#pw-form', m).onsubmit = e => {
    e.preventDefault();
    act(async () => {
      await api('POST', '/api/me/password', { old: $('#pw-old', m).value, new: $('#pw-new', m).value });
      $('#pw-form', m).reset(); toast('Senha alterada. Outras sessões foram encerradas.');
    });
  };
  $('#st-logout', m).onclick = () => act(async () => { await api('POST', '/api/logout'); location.reload(); });
}

/* ------------------------------------------------------------- perfil */
async function openProfile(uid, sid = S.view.sid) {
  let u = S.users.get(uid);
  if (!u) u = cacheUser(await api('GET', `/api/users/${uid}`));
  const mem = sid && S.members.get(sid)?.get(uid);
  const s = sid && S.servers.get(sid);
  const self = uid === S.me.id;
  const rel = S.friends.get(uid)?.relation;
  const myRank = { member: 0, admin: 1, owner: 2 }[s?.my_role || 'member'];
  const theirRank = mem ? { member: 0, admin: 1, owner: 2 }[mem.role] : 0;
  let actions = '';
  if (self) actions = '<button class="btn ghost" data-act="settings">Editar perfil</button>';
  else {
    actions = `<button class="btn primary" data-act="dm-user" data-id="${uid}">Enviar mensagem</button>`;
    if (rel === 'friend') actions += `<button class="btn ghost" data-act="friend-remove" data-id="${uid}" data-confirm="1">Remover amigo</button>`;
    else if (rel === 'outgoing') actions += `<button class="btn ghost" data-act="friend-remove" data-id="${uid}">Cancelar pedido</button>`;
    else if (rel === 'incoming') actions += `<button class="btn ghost" data-act="friend-accept" data-id="${uid}">Aceitar pedido</button>`;
    else actions += `<button class="btn ghost" data-act="friend-add" data-name="${esc(u.username)}">Adicionar amigo</button>`;
  }
  let mod = '';
  if (mem && !self && s) {
    if (myRank >= 1 && myRank > theirRank) mod += `<button class="btn ghost" data-act="kick" data-id="${uid}" data-sid="${sid}">Expulsar</button><button class="btn danger ghost" data-act="ban" data-id="${uid}" data-sid="${sid}">Banir</button>`;
    if (myRank === 2) {
      mod += `<button class="btn ghost" data-act="set-role" data-id="${uid}" data-sid="${sid}" data-role="${mem.role === 'admin' ? 'member' : 'admin'}">${mem.role === 'admin' ? 'Remover admin' : 'Tornar admin'}</button>`;
      mod += `<button class="btn ghost" data-act="transfer" data-id="${uid}" data-sid="${sid}">Transferir servidor</button>`;
    }
  }
  const color = AVATAR_COLORS[u.id % AVATAR_COLORS.length];
  openModal(`<div class="profile-card modal-profile">
    <div class="banner" style="background:${color}"></div>
    <div class="pc-avatar">${avatar(u, 84, true)}</div>
    <h3>${esc(mem?.nickname || u.display_name)}${mem && mem.role !== 'member' ? ` <span class="tag ${mem.role}">${ROLE_LABEL[mem.role]}</span>` : ''}</h3>
    <p class="muted">@${esc(u.username)} · ${STATUS_LABEL[u.status]}</p>
    ${u.custom_status ? `<p>${esc(u.custom_status)}</p>` : ''}
    ${u.bio ? `<div class="pc-section"><b>Sobre</b><p>${esc(u.bio)}</p></div>` : ''}
    <div class="pc-section"><b>Conta criada em</b><p>${df.format(u.created_at)}</p></div>
    ${mem ? `<div class="pc-section"><b>Entrou no servidor em</b><p>${df.format(mem.joined_at)}</p></div>` : ''}
    <div class="pc-actions">${actions}</div>${mod ? `<div class="pc-actions mod">${mod}</div>` : ''}
    <div class="modal-actions"><button class="btn ghost" data-close>Fechar</button></div></div>`);
}
'''

ASSETS['loja.js'] = r''''use strict';

/* ---------------------------------------------------- irisk: loja e ganhos */
const SHOP = { tab: 'earn', data: null };
let lastInput = 0;

function irisIcon(sz = 18) {
  return `<svg class="iris" width="${sz}" height="${sz}" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="11" fill="#ffb454"/><circle cx="12" cy="12" r="6.6" fill="#2a1800"/><circle cx="12" cy="12" r="3.2" fill="#ffb454"/><circle cx="14.2" cy="9.8" r="1.3" fill="#fff" opacity=".85"/></svg>`;
}
const nfmt = n => n.toLocaleString('pt-BR');
function fmtClock(sec) {
  const m = Math.floor(sec / 60), s = sec % 60;
  return `${m}:${String(s).padStart(2, '0')}`;
}
function fmtWait(ms) {
  const min = Math.ceil(ms / 60000);
  return min >= 60 ? `${Math.floor(min / 60)} h ${min % 60} min` : `${min} min`;
}

async function openShop(tab = 'earn') {
  try { SHOP.data = await api('GET', '/api/irisk'); } catch (e) { return toast(e.message); }
  SHOP.tab = tab;
  const m = openModal('', { wide: true });
  m.classList.add('shop-modal');
  renderShop();
}
async function refreshShop() {
  if (!$('.shop-modal')) return;
  try { SHOP.data = await api('GET', '/api/irisk'); renderShop(); } catch { /* ignora */ }
}

function renderShop() {
  const m = $('.shop-modal');
  if (!m || !SHOP.data) return;
  const d = SHOP.data, w = d.wallet, tab = SHOP.tab, inf = !!w.infinite;
  const broke = price => !inf && w.balance < price;
  const tabBtn = (id, label) => `<button class="tab${tab === id ? ' active' : ''}" data-act="shop-tab" data-tab="${id}">${label}</button>`;
  const nitroOn = w.nitro_until > Date.now();
  let body = '';

  if (tab === 'earn') {
    const trailers = d.trailers.map(t => {
      const wait = t.available_at - Date.now();
      return `<div class="earn-row"><div class="grow"><b>${esc(t.title)}</b><small>Patrocinado por ${esc(t.sponsor)} · assistir ${t.seconds}s</small></div>
        <span class="reward">+${t.reward} ${irisIcon(14)}</span>
        ${wait > 0 ? `<button class="btn ghost" disabled>Volta em ${fmtWait(wait)}</button>` : `<button class="btn primary" data-act="trailer-open" data-id="${esc(t.id)}">Assistir</button>`}</div>`;
    }).join('') || '<p class="muted small">Nenhum trailer disponível agora.</p>';
    const g = d.game;
    const left = g.limit - g.plays;
    body = `<div class="earn-card"><h3>Trailers dos patrocinadores</h3>
        <p class="muted small">Assista ao trailer inteiro com a aba aberta e resgate sua recompensa. Cada trailer rende uma vez a cada 24 horas.</p>${trailers}</div>
      <div class="earn-card"><h3>Jogue por ${g.seconds / 60} minutos</h3>
        <p class="muted small">Sorteamos um jogo para você. O tempo só conta enquanto você está jogando, com a aba aberta. Hoje: ${g.plays}/${g.limit}.</p>
        <div class="earn-row"><div class="grow"><b>Jogo aleatório</b><small>Cobrinha, Memória ou Alvos</small></div>
        <span class="reward">+${g.reward} ${irisIcon(14)}</span>
        ${left > 0 ? '<button class="btn primary" data-act="game-open">Jogar</button>' : '<button class="btn ghost" disabled>Limite diário</button>'}</div></div>`;
  } else if (tab === 'shop') {
    const frames = d.shop.frames.map(f => {
      const prev = avatar({ ...S.me, frame: f.id, nitro_until: S.me.nitro_until }, 64, false, true);
      const btn = f.equipped ? '<button class="btn ghost" data-act="shop-equip" data-frame="">Tirar</button>'
        : f.owned ? `<button class="btn primary" data-act="shop-equip" data-frame="${f.id}">Equipar</button>`
          : `<button class="btn primary" data-act="shop-buy" data-item="frame:${f.id}" ${broke(f.price) ? 'disabled' : ''}>${inf ? 'Pegar' : irisIcon(14) + ' ' + f.price}</button>`;
      return `<div class="shop-card">${prev}<b>${esc(f.name)}</b>${btn}</div>`;
    }).join('');
    const nf = d.shop.nitro_frame;
    const nfBtn = nf.equipped ? '<button class="btn ghost" data-act="shop-equip" data-frame="">Tirar</button>'
      : nitroOn ? `<button class="btn primary" data-act="shop-equip" data-frame="${nf.id}">Equipar</button>`
        : '<button class="btn ghost" disabled>Exige Nitro</button>';
    const n = d.shop.nitro;
    body = `<h3 class="shop-h">Molduras de perfil</h3>
      <div class="shop-grid">${frames}<div class="shop-card">${avatar({ ...S.me, frame: nf.id, nitro_until: Date.now() + 1e9 }, 64, false, true)}<b>${esc(nf.name)} <span class="tag nitro">NITRO</span></b>${nfBtn}</div></div>
      <h3 class="shop-h">Nitro</h3>
      <div class="nitro-card"><div class="grow"><b class="nitro-name">Nitro por ${n.days} dias</b>
        <ul><li>Selo NITRO e nome com degradê nas mensagens</li><li>Envio de arquivos de até 100 MB (o normal é 25 MB)</li><li>Moldura animada exclusiva “Prisma”</li></ul>
        ${inf ? '<small class="ok-text">Permanente: você é dono do site.</small>' : nitroOn ? `<small class="ok-text">Ativo até ${new Date(w.nitro_until).toLocaleString('pt-BR')}. Comprar de novo soma ${n.days} dias.</small>` : ''}</div>
        <button class="btn primary" data-act="shop-buy" data-item="nitro" ${broke(n.price) ? 'disabled' : ''}>${inf ? 'Grátis' : irisIcon(14) + ' ' + nfmt(n.price)}</button></div>`;
  } else if (tab === 'owner') {
    body = `<div class="earn-card"><h3>Painel do dono</h3>
      <p class="muted small">Dê irisk a qualquer usuário. Use um número negativo para remover (o saldo nunca fica abaixo de zero).</p>
      <form id="grant-form" class="grant-form"><input id="grant-user" placeholder="nome_de_usuario" required autocomplete="off">
        <input id="grant-amount" type="number" placeholder="Quantidade" required step="1">
        <button class="btn primary">Enviar</button></form></div>`;
  } else {
    body = d.log.length ? `<div class="log">${d.log.map(l => `<div class="log-row"><span class="grow">${esc(l.reason)}<small>${fmtStamp(l.at)}</small></span>
      <b class="${l.amount > 0 ? 'plus' : 'minus'}">${l.amount > 0 ? '+' : ''}${nfmt(l.amount)}</b></div>`).join('')}</div>`
      : '<div class="empty small"><p class="muted">Nenhuma movimentação ainda. Assista a um trailer ou jogue para ganhar irisk.</p></div>';
  }
  m.innerHTML = `<div class="shop-head"><h2>Loja irisk</h2><div class="bal">${irisIcon(24)}<b>${inf ? '∞' : nfmt(w.balance)}</b><span>irisk</span></div></div>
    <div class="tabs modal-tabs">${tabBtn('earn', 'Ganhar irisk')}${tabBtn('shop', 'Loja')}${tabBtn('log', 'Extrato')}${inf ? tabBtn('owner', 'Dono') : ''}</div>
    <div class="shop-body">${body}</div>
    <div class="modal-actions"><button class="btn ghost" data-close>Fechar</button></div>`;
}

/* ------------------------------------------- progresso de tarefas (trailer/jogo) */
function progressHTML() {
  return `<div class="earn-progress"><div class="bar"><i id="ep-bar"></i></div><div class="ep-text" id="ep-text"></div></div>`;
}
function earnLoop(m, s, { input }) {
  let done = s.done, ready = done >= s.need - 1, paused = false;
  const isActive = () => !document.hidden && (!input || Date.now() - lastInput < 30000);
  const paint = () => {
    const bar = $('#ep-bar', m), txt = $('#ep-text', m), btn = $('#claim', m);
    if (!bar) return;
    bar.style.width = Math.min(100, done / s.need * 100) + '%';
    const left = Math.max(0, Math.ceil(s.need - done));
    txt.textContent = ready ? 'Pronto! Resgate sua recompensa.'
      : paused ? `Pausado. ${input ? 'Mexa no jogo' : 'Volte para esta aba'} para continuar contando.`
        : `Faltam ${fmtClock(left)}`;
    btn.disabled = !ready;
  };
  const tick = async () => {
    if (ready) return;
    const active = isActive();
    paused = !active;
    try {
      const r = await api('POST', '/api/irisk/earn/beat', { token: s.token, active });
      done = r.done; ready = r.ready;
    } catch { /* tenta no próximo ciclo */ }
    paint();
  };
  const t1 = setInterval(tick, 10000);
  const t2 = setInterval(() => { const p = !isActive(); if (p !== paused && !ready) { paused = p; paint(); } }, 1000);
  modalCleanup = () => { clearInterval(t1); clearInterval(t2); };
  $('#claim', m).onclick = () => act(async () => {
    const r = await api('POST', '/api/irisk/earn/claim', { token: s.token });
    closeModal(); toast(r.message);
    openShop('earn');
  });
  paint();
}

async function openTrailer(id) {
  let s;
  try { s = await api('POST', '/api/irisk/earn/start', { kind: 'trailer', ref: id }); } catch (e) { return toast(e.message); }
  const t = SHOP.data.trailers.find(x => x.id === id);
  const yt = encodeURIComponent(t.youtube);
  const m = openModal(`<h2>${esc(t.title)}</h2><p class="muted">Patrocinado por <b>${esc(t.sponsor)}</b></p>
    <div class="video-wrap"><iframe src="https://www.youtube-nocookie.com/embed/${yt}?autoplay=1&rel=0" title="${esc(t.title)}"
      allow="autoplay; encrypted-media; picture-in-picture; fullscreen" allowfullscreen referrerpolicy="strict-origin-when-cross-origin"></iframe></div>
    <p class="muted small">O vídeo não carregou? <a href="https://www.youtube.com/watch?v=${yt}" target="_blank" rel="noopener noreferrer">Abra no YouTube</a> e deixe esta aba aberta.</p>
    ${progressHTML()}
    <div class="modal-actions"><button class="btn ghost" data-close>Fechar</button><button class="btn primary" id="claim" disabled>Resgatar +${s.reward} irisk</button></div>`, { wide: true });
  earnLoop(m, s, { input: false });
}

const GAME_NAMES = { cobrinha: 'Cobrinha', memoria: 'Memória', alvos: 'Alvos' };
async function openGame() {
  let s;
  try { s = await api('POST', '/api/irisk/earn/start', { kind: 'game' }); } catch (e) { return toast(e.message); }
  const m = openModal(`<h2>${esc(GAME_NAMES[s.ref] || 'Jogo')}</h2>
    <p class="muted small">Jogo sorteado para você. Jogue por ${Math.round(s.need / 60)} minutos. Se fechar, o progresso fica salvo.</p>
    <div id="game-stage" class="game-stage"></div>${progressHTML()}
    <div class="modal-actions"><button class="btn ghost" data-close>Fechar</button><button class="btn primary" id="claim" disabled>Resgatar +${s.reward} irisk</button></div>`, { wide: true });
  lastInput = Date.now();
  const bump = () => { lastInput = Date.now(); };
  const stop = GAME_IMPL[s.ref]($('#game-stage', m), bump);
  earnLoop(m, s, { input: true });
  const prev = modalCleanup;
  modalCleanup = () => { prev(); stop(); };
}

/* ----------------------------------------------------------------- minigames */
function gameCobrinha(el, bump) {
  const N = 18, C = 20;
  el.innerHTML = '<canvas width="360" height="360" aria-label="Jogo da cobrinha"></canvas><div class="game-score"></div><p class="muted small">Use as setas ou WASD. No celular, deslize o dedo.</p>';
  const cv = el.firstChild, g = cv.getContext('2d'), sc = el.querySelector('.game-score');
  let snake, dir, next, food, score = 0, best = 0;
  const place = () => { do { food = { x: Math.random() * N | 0, y: Math.random() * N | 0 }; } while (snake.some(p => p.x === food.x && p.y === food.y)); };
  const reset = () => { snake = [{ x: 9, y: 9 }, { x: 8, y: 9 }, { x: 7, y: 9 }]; dir = next = { x: 1, y: 0 }; score = 0; place(); };
  const draw = () => {
    g.fillStyle = '#150f1e'; g.fillRect(0, 0, 360, 360);
    g.fillStyle = '#ffb454'; g.beginPath(); g.arc(food.x * C + 10, food.y * C + 10, 7, 0, 7); g.fill();
    snake.forEach((p, i) => { g.fillStyle = i ? '#4fc38a' : '#8fe6b8'; g.fillRect(p.x * C + 1, p.y * C + 1, C - 2, C - 2); });
    sc.textContent = `Pontos: ${score} · Recorde: ${best}`;
  };
  const step = () => {
    dir = next;
    const h = { x: snake[0].x + dir.x, y: snake[0].y + dir.y };
    if (h.x < 0 || h.y < 0 || h.x >= N || h.y >= N || snake.some(p => p.x === h.x && p.y === h.y)) { best = Math.max(best, score); reset(); return draw(); }
    snake.unshift(h);
    if (h.x === food.x && h.y === food.y) { score++; place(); } else snake.pop();
    draw();
  };
  const turn = v => { if (v[0] !== -dir.x || v[1] !== -dir.y) next = { x: v[0], y: v[1] }; };
  const keys = { ArrowUp: [0, -1], w: [0, -1], ArrowDown: [0, 1], s: [0, 1], ArrowLeft: [-1, 0], a: [-1, 0], ArrowRight: [1, 0], d: [1, 0] };
  const key = e => {
    const v = keys[e.key.length === 1 ? e.key.toLowerCase() : e.key];
    if (!v) return;
    e.preventDefault(); bump(); turn(v);
  };
  let tx = 0, ty = 0;
  cv.style.touchAction = 'none';
  cv.addEventListener('pointerdown', e => { tx = e.clientX; ty = e.clientY; bump(); });
  cv.addEventListener('pointerup', e => {
    const dx = e.clientX - tx, dy = e.clientY - ty;
    if (Math.max(Math.abs(dx), Math.abs(dy)) < 20) return;
    turn(Math.abs(dx) > Math.abs(dy) ? [Math.sign(dx), 0] : [0, Math.sign(dy)]);
  });
  document.addEventListener('keydown', key);
  reset(); draw();
  const t = setInterval(step, 120);
  return () => { clearInterval(t); document.removeEventListener('keydown', key); };
}

function gameMemoria(el, bump) {
  const E = ['🍎', '🚀', '🎧', '🐙', '🌵', '⚡', '🎲', '🍄'];
  let cards, open = [], lock = false, moves = 0, found = 0, alive = true;
  const draw = () => {
    el.innerHTML = `<div class="mem-grid">${cards.map((c, i) => `<button class="mem${c.up || c.ok ? ' up' : ''}${c.ok ? ' ok' : ''}" data-i="${i}" aria-label="carta">${c.up || c.ok ? c.e : ''}</button>`).join('')}</div>
      <div class="game-score">Jogadas: ${moves} · Pares: ${found}/8</div>`;
  };
  const deal = () => {
    cards = [...E, ...E].sort(() => Math.random() - .5).map(e => ({ e, up: false, ok: false }));
    open = []; lock = false; moves = 0; found = 0; draw();
  };
  el.onclick = ev => {
    const b = ev.target.closest('.mem');
    if (!b || lock) return;
    bump();
    const c = cards[+b.dataset.i];
    if (c.up || c.ok) return;
    c.up = true; open.push(c); draw();
    if (open.length === 2) {
      moves++; lock = true;
      setTimeout(() => {
        if (!alive) return;
        if (open[0].e === open[1].e) { open.forEach(x => { x.ok = true; }); found++; }
        open.forEach(x => { x.up = false; });
        open = []; lock = false; draw();
        if (found === 8) setTimeout(() => { if (alive) deal(); }, 1200);
      }, 700);
    }
  };
  deal();
  return () => { alive = false; el.onclick = null; };
}

function gameAlvos(el, bump) {
  el.innerHTML = '<div class="alvos"><button class="alvo" aria-label="alvo"></button></div><div class="game-score"></div><p class="muted small">Clique no alvo antes que ele fuja.</p>';
  const area = el.firstChild, a = area.firstChild, sc = el.querySelector('.game-score');
  let score = 0, misses = 0;
  const show = () => { sc.textContent = `Acertos: ${score} · Erros: ${misses}`; };
  const move = () => {
    const s = 44 + Math.random() * 20;
    a.style.width = a.style.height = s + 'px';
    a.style.left = Math.random() * Math.max(1, area.clientWidth - s) + 'px';
    a.style.top = Math.random() * Math.max(1, area.clientHeight - s) + 'px';
  };
  a.onclick = e => { e.stopPropagation(); bump(); score++; show(); move(); };
  area.onclick = () => { bump(); misses++; show(); };
  const t = setInterval(() => { misses++; show(); move(); }, 1400);
  setTimeout(() => { move(); show(); }, 0);
  return () => clearInterval(t);
}
const GAME_IMPL = { cobrinha: gameCobrinha, memoria: gameMemoria, alvos: gameAlvos };

document.addEventListener('submit', e => {
  if (e.target.id !== 'grant-form') return;
  e.preventDefault();
  act(async () => {
    const r = await api('POST', '/api/owner/grant', { username: $('#grant-user').value, amount: parseInt($('#grant-amount').value, 10) });
    toast(r.message);
    $('#grant-amount').value = '';
  });
});
'''

ASSETS['main.js'] = r''''use strict';

/* ------------------------------------------------------------ autenticação */
let authMode = 'login';
function showAuth() {
  $('#app').classList.add('hidden');
  $('#auth').classList.remove('hidden');
  if (window.es) { window.es.close(); window.es = null; }
}
function setAuthMode(mode) {
  authMode = mode;
  const reg = mode === 'register';
  $('#auth-title').textContent = reg ? 'Crie sua conta' : 'Que bom te ver de novo';
  $('#auth-sub').textContent = reg ? 'Leva menos de um minuto.' : 'Entre para continuar suas conversas.';
  $('#auth-submit').textContent = reg ? 'Criar conta' : 'Entrar';
  $('#auth-switch-text').textContent = reg ? 'Já tem uma conta?' : 'Ainda não tem conta?';
  $('#auth-switch').textContent = reg ? 'Entrar' : 'Criar conta';
  $('#a-display-wrap').classList.toggle('hidden', !reg);
  $('#a-pass').autocomplete = reg ? 'new-password' : 'current-password';
  $('#auth-error').textContent = '';
}
$('#auth-switch').onclick = e => { e.preventDefault(); setAuthMode(authMode === 'login' ? 'register' : 'login'); };
$('#auth-form').onsubmit = async e => {
  e.preventDefault();
  const btn = $('#auth-submit');
  btn.disabled = true;
  try {
    await api('POST', authMode === 'login' ? '/api/login' : '/api/register', {
      username: $('#a-user').value, password: $('#a-pass').value, display_name: $('#a-display').value });
    $('#a-pass').value = '';
    await boot();
  } catch (err) { $('#auth-error').textContent = err.message; }
  btn.disabled = false;
};

/* -------------------------------------------------------------- bootstrap */
function applyBootstrap(d) {
  S.me = d.me;
  S.users = new Map();
  cacheUser(d.me);
  S.servers = new Map(d.servers.map(s => [s.id, s]));
  S.dms = d.dms;
  S.dms.forEach(x => cacheUser(x.user));
  S.friends = new Map(d.friends.map(f => [f.user.id, { user: cacheUser(f.user), relation: f.relation }]));
  S.unread = Object.fromEntries(Object.entries(d.unread).map(([k, v]) => [+k, v]));
  S.voice = Object.fromEntries(Object.entries(d.voice).map(([k, v]) => [+k, v]));
  S.wallet = d.wallet;
}
async function boot() {
  let d;
  try { d = await api('GET', '/api/bootstrap'); } catch { showAuth(); return; }
  applyBootstrap(d);
  $('#auth').classList.add('hidden');
  $('#app').classList.remove('hidden');
  S.msgs = new Map(); S.members = new Map();
  S.view = { kind: 'home', sid: null, cid: null, tab: 'online' };
  renderAll();
  connect();
  const inv = (location.hash.match(/^#invite=([\w-]+)/) || [])[1];
  if (inv) { history.replaceState(null, '', location.pathname); openAddServer(inv); }
}

/* -------------------------------------------------------------- navegação */
function addServer(s) { S.servers.set(s.id, s); renderRail(); }
function closeNav() { document.body.classList.remove('nav-open'); }

function goHome(tab) {
  S.view = { kind: 'home', sid: null, cid: null, tab: tab || S.view.tab || 'online' };
  S.reply = null; S.editing = null; S.files = [];
  closeNav(); renderAll();
}
async function goDM(cid) { await openChannel(null, cid); }
async function goServer(sid, cid) {
  const s = S.servers.get(sid);
  if (!s) return;
  await act(() => ensureMembers(sid));
  const valid = c => s.channels.some(x => x.id === c);
  let target = cid || (valid(S.lastChannel[sid]) ? S.lastChannel[sid] : null)
    || (s.channels.find(c => c.kind === 'text') || s.channels[0] || {}).id;
  await openChannel(sid, target || null);
}
async function ensureMembers(sid) {
  if (S.members.has(sid)) return;
  const list = await api('GET', `/api/servers/${sid}/members`);
  S.members.set(sid, new Map(list.map(m => { cacheUser(m); return [m.id, m]; })));
}
async function openChannel(sid, cid) {
  S.view = { kind: sid ? 'server' : 'dm', sid, cid, tab: S.view.tab };
  if (sid && cid) S.lastChannel[sid] = cid;
  S.reply = null; S.editing = null; S.files = [];
  if (S.panel === 'search' || S.panel === 'pins') S.panel = 'members';
  $('#app').classList.remove('panel-mobile');
  closeNav();
  renderAll();
  const ch = curChannel();
  if (ch && ch.kind !== 'voice') {
    await act(() => ensureMessages(cid));
    if (S.view.cid === cid) { renderMessages('bottom'); markRead(cid); }
  }
}

let readTimer;
function markRead(cid) {
  if (!S.unread[cid] && !document.hasFocus()) return;
  clearTimeout(readTimer);
  readTimer = setTimeout(() => {
    if (S.view.cid !== cid || document.hidden) return;
    if (S.unread[cid]) {
      delete S.unread[cid];
      renderRail(); renderSidebar(); updateTitle();
    }
    api('POST', `/api/channels/${cid}/read`).catch(() => {});
  }, 250);
}
window.addEventListener('focus', () => { if (S.view.cid) markRead(S.view.cid); });
document.addEventListener('visibilitychange', () => { if (!document.hidden && S.view.cid) markRead(S.view.cid); });

/* ---------------------------------------------------------------- envio */
async function sendMessage() {
  const ta = $('#composer-input');
  if (!ta) return;
  const cid = S.view.cid;
  const text = ta.value.trim();
  if (!text && !S.files.length) return;
  const btn = $('.send');
  if (btn) btn.disabled = true;
  try {
    let m;
    if (S.files.length) {
      const fd = new FormData();
      fd.append('content', text);
      if (S.reply) fd.append('reply_to', S.reply.id);
      S.files.forEach(f => fd.append('files', f));
      m = await api('POST', `/api/channels/${cid}/messages`, fd);
    } else {
      m = await api('POST', `/api/channels/${cid}/messages`, { content: text, reply_to: S.reply?.id });
    }
    ta.value = ''; autosize(ta);
    delete S.drafts[cid];
    S.reply = null; S.files = [];
    renderReplyBar(); renderChips(); hideMentionPop();
    if (addMessage(m)) { const b = $('#messages'); if (b) b._stick = true; renderMessages('bottom'); }
    bumpDM(cid, m.id);
  } catch (e) { toast(e.message); }
  if (btn) btn.disabled = false;
  if (ta) ta.focus();
}
function bumpDM(cid, mid) {
  const d = S.dms.find(x => x.id === cid);
  if (d) { d.last_message_id = Math.max(d.last_message_id, mid); S.dms.sort((a, b) => b.last_message_id - a.last_message_id); renderSidebar(); }
}
function addFiles(list) {
  for (const f of list) {
    const lim = nitroActive(S.me) ? 100 : 25;
    if (f.size > lim * 1024 * 1024) {
      toast(`“${f.name}” passa de ${lim} MB.${lim === 25 ? ' O Nitro libera até 100 MB.' : ''}`);
      continue;
    }
    if (S.files.length < 10) S.files.push(f);
  }
  renderChips();
}

/* -------------------------------------------------- menções (autocomplete) */
let mentionIdx = 0, mentionList = [];
function hideMentionPop() { const p = $('#mention-pop'); if (p) p.classList.add('hidden'); mentionList = []; }
function updateMentionPop() {
  const ta = $('#composer-input'), pop = $('#mention-pop');
  if (!ta || !pop) return;
  const before = ta.value.slice(0, ta.selectionStart);
  const mt = before.match(/(?:^|\s)@([\w.]{0,24})$/);
  if (!mt) return hideMentionPop();
  const q = mt[1].toLowerCase();
  let pool = [];
  if (S.view.kind === 'server') pool = [...(S.members.get(S.view.sid)?.values() || [])];
  else { const ch = curChannel(); if (ch) pool = [U(ch.user), S.me]; }
  mentionList = pool.filter(u => u.id !== S.me.id || S.view.kind === 'server')
    .filter(u => u.username.toLowerCase().includes(q) || u.display_name.toLowerCase().includes(q))
    .slice(0, 6).map(u => ({ username: u.username, label: u.display_name, user: u }));
  if (S.view.kind === 'server' && 'everyone'.startsWith(q)) mentionList.push({ username: 'everyone', label: 'Notificar todos' });
  if (!mentionList.length) return hideMentionPop();
  mentionIdx = Math.min(mentionIdx, mentionList.length - 1);
  pop.innerHTML = mentionList.map((m, i) => `<button class="${i === mentionIdx ? 'active' : ''}" data-act="mention-pick" data-u="${esc(m.username)}">
    ${m.user ? avatar(m.user, 24) : ''}<b>${esc(m.label)}</b><small>@${esc(m.username)}</small></button>`).join('');
  pop.classList.remove('hidden');
}
function pickMention(username) {
  const ta = $('#composer-input');
  const pos = ta.selectionStart;
  const before = ta.value.slice(0, pos).replace(/@[\w.]*$/, '@' + username + ' ');
  ta.value = before + ta.value.slice(pos);
  ta.setSelectionRange(before.length, before.length);
  ta.focus(); hideMentionPop(); autosize(ta);
  S.drafts[S.view.cid] = ta.value;
}

let lastTyping = 0;
document.addEventListener('input', e => {
  if (e.target.id === 'composer-input') {
    const ta = e.target;
    autosize(ta);
    S.drafts[S.view.cid] = ta.value;
    updateMentionPop();
    if (ta.value && Date.now() - lastTyping > 3000) {
      lastTyping = Date.now();
      api('POST', `/api/channels/${S.view.cid}/typing`).catch(() => {});
    }
  } else if (e.target.id === 'edit-box') autosize(e.target);
});
document.addEventListener('keydown', e => {
  const t = e.target;
  if (t.id === 'composer-input') {
    const pop = mentionList.length && !$('#mention-pop').classList.contains('hidden');
    if (pop && (e.key === 'ArrowDown' || e.key === 'ArrowUp')) {
      e.preventDefault();
      mentionIdx = (mentionIdx + (e.key === 'ArrowDown' ? 1 : -1) + mentionList.length) % mentionList.length;
      return updateMentionPop();
    }
    if (pop && (e.key === 'Enter' || e.key === 'Tab')) { e.preventDefault(); return pickMention(mentionList[mentionIdx].username); }
    if (e.key === 'Enter' && !e.shiftKey && !e.isComposing) { e.preventDefault(); sendMessage(); }
    else if (e.key === 'Escape') { if (pop) hideMentionPop(); else if (S.reply) { S.reply = null; renderReplyBar(); } }
    else if (e.key === 'ArrowUp' && !t.value) {
      const st = S.msgs.get(S.view.cid);
      const mine = st && [...st.list].reverse().find(m => m.author && m.author.id === S.me.id && m.content);
      if (mine) { e.preventDefault(); startEdit(mine.id); }
    }
  } else if (t.id === 'edit-box') {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); saveEdit(); }
    else if (e.key === 'Escape') { e.preventDefault(); cancelEdit(); }
  }
});
document.addEventListener('paste', e => {
  if (e.target.id !== 'composer-input') return;
  const files = [...(e.clipboardData?.files || [])];
  if (files.length) { e.preventDefault(); addFiles(files); }
});
document.addEventListener('dragover', e => { if ($('#composer-input')) e.preventDefault(); });
document.addEventListener('drop', e => {
  if (!$('#composer-input') || !e.dataTransfer?.files.length) return;
  e.preventDefault(); addFiles([...e.dataTransfer.files]);
});
document.addEventListener('change', e => {
  if (e.target.id === 'file-input') { addFiles([...e.target.files]); e.target.value = ''; }
});

function startEdit(id) { S.editing = id; renderMessages('keep'); }
function cancelEdit() { S.editing = null; renderMessages('keep'); const ta = $('#composer-input'); if (ta) ta.focus(); }
async function saveEdit() {
  const id = S.editing, box = $('#edit-box');
  if (!box) return;
  const content = box.value.trim();
  const m = S.msgs.get(S.view.cid)?.list.find(x => x.id === id);
  if (m && content === m.content) return cancelEdit();
  if (!content && !(m && m.attachments.length)) {
    return confirmBox('Excluir mensagem', 'Uma mensagem vazia será excluída. Continuar?', 'Excluir',
      () => api('DELETE', `/api/messages/${id}`));
  }
  try { await api('PATCH', `/api/messages/${id}`, { content }); S.editing = null; renderMessages('keep'); }
  catch (e) { toast(e.message); }
}

/* ------------------------------------------------------------ busca e fixadas */
document.addEventListener('submit', e => {
  if (e.target.id === 'search-form') {
    e.preventDefault();
    const q = $('#search-input').value.trim();
    if (q.length < 2) return toast('Digite pelo menos 2 caracteres.');
    S.panel = 'search'; S.panelData = null; S.panelData = { q, items: null };
    renderPanel(); $('#app').classList.add('panel-mobile');
    const url = S.view.kind === 'server'
      ? `/api/search?server_id=${S.view.sid}&q=${encodeURIComponent(q)}`
      : `/api/search?channel_id=${S.view.cid}&q=${encodeURIComponent(q)}`;
    api('GET', url).then(items => {
      items.forEach(m => { if (m.author && !S.users.has(m.author.id)) cacheUser(m.author); });
      if (S.panel === 'search') { S.panelData = { q, items }; renderPanel(); }
    }).catch(err => { toast(err.message); S.panel = 'members'; renderPanel(); });
  } else if (e.target.id === 'add-friend-form') {
    e.preventDefault();
    const inp = $('#friend-input'), msg = $('#friend-msg');
    api('POST', '/api/friends/request', { username: inp.value }).then(r => {
      msg.className = 'form-msg ok';
      msg.textContent = r.relation === 'friend' ? 'Vocês agora são amigos!' : `Pedido enviado para @${inp.value.replace(/^@/, '')}.`;
      inp.value = '';
    }).catch(err => { msg.className = 'form-msg err'; msg.textContent = err.message; });
  }
});
async function showPins() {
  if (S.panel === 'pins') { S.panel = 'members'; return renderAll2(); }
  S.panel = 'pins'; S.panelData = null; renderPanel(); renderHeaderButtons();
  try {
    const items = await api('GET', `/api/channels/${S.view.cid}/pins`);
    if (S.panel === 'pins') { S.panelData = { items }; renderPanel(); }
  } catch (e) { toast(e.message); }
}
function renderAll2() { renderMain(); renderPanel(); }
function renderHeaderButtons() {
  $$('.main-head [data-act=pins]').forEach(b => b.classList.toggle('active', S.panel === 'pins'));
  $$('.main-head [data-act=toggle-members]').forEach(b => b.classList.toggle('active', S.panel === 'members'));
}
function refreshPins() {
  if (S.panel === 'pins') api('GET', `/api/channels/${S.view.cid}/pins`)
    .then(items => { if (S.panel === 'pins') { S.panelData = { items }; renderPanel(); } }).catch(() => {});
}

/* ------------------------------------------------------------------ ações */
const ACTIONS = {
  shop: () => openShop(),
  'shop-tab': el => { SHOP.tab = el.dataset.tab; renderShop(); },
  'shop-buy': el => act(async () => { const r = await api('POST', '/api/irisk/buy', { item: el.dataset.item }); toast(r.message); await refreshShop(); }),
  'shop-equip': el => act(async () => { await api('POST', '/api/irisk/equip', { frame: el.dataset.frame }); await refreshShop(); }),
  'trailer-open': el => openTrailer(el.dataset.id),
  'game-open': () => openGame(),
  home: () => goHome(),
  friends: () => goHome(),
  tab: el => { S.view.tab = el.dataset.tab; renderMain(); },
  server: el => goServer(+el.dataset.id),
  'add-server': () => openAddServer(),
  'server-menu': el => serverMenu(el),
  channel: el => openChannel(S.view.sid, +el.dataset.id),
  'voice-channel': el => {
    const id = +el.dataset.id;
    openChannel(S.view.sid, id);
    if (V.cid !== id) joinVoice(id);
  },
  'voice-join': el => joinVoice(+el.dataset.id),
  'voice-leave': () => leaveVoice(),
  mute: () => toggleMute(),
  deafen: () => toggleDeafen(),
  'new-channel': el => openNewChannel(el.dataset.kind),
  'chan-settings': (el, e) => { e.stopPropagation(); openChannelSettings(+el.dataset.id); },
  dm: el => goDM(+el.dataset.id),
  'dm-user': el => act(async () => {
    const d = await api('POST', '/api/dm', { user_id: +el.dataset.id });
    if (!S.dms.some(x => x.id === d.id)) { cacheUser(d.user); S.dms.unshift(d); }
    closeModal(); await goDM(d.id);
  }),
  settings: () => openSettings(),
  'my-status': el => statusMenu(el),
  profile: el => { if (el.dataset.id && el.dataset.id !== 'undefined') act(() => openProfile(+el.dataset.id)); },
  nav: () => document.body.classList.toggle('nav-open'),
  'toggle-members': () => {
    S.panel = S.panel === 'members' ? null : 'members';
    renderPanel(); renderHeaderButtons();
    $('#app').classList.toggle('panel-mobile', !!S.panel);
  },
  'panel-close': () => { S.panel = null; renderPanel(); renderHeaderButtons(); },
  pins: () => showPins(),
  goto: el => act(async () => {
    const cid = +el.dataset.cid;
    if (cid !== S.view.cid) await openChannel(S.view.sid, cid);
    jumpTo(+el.dataset.id);
  }),
  jump: el => jumpTo(+el.dataset.id),
  send: () => sendMessage(),
  attach: () => $('#file-input').click(),
  'chip-remove': el => { S.files.splice(+el.dataset.i, 1); renderChips(); },
  emoji: el => emojiPicker(el, em => {
    const ta = $('#composer-input'); if (!ta) return;
    const p = ta.selectionStart;
    ta.value = ta.value.slice(0, p) + em + ta.value.slice(ta.selectionEnd);
    ta.focus(); ta.setSelectionRange(p + em.length, p + em.length);
    S.drafts[S.view.cid] = ta.value;
  }, { up: true }),
  'mention-pick': el => pickMention(el.dataset.u),
  reply: el => {
    const m = findMsg(+el.dataset.id);
    if (m) { S.reply = m; renderReplyBar(); const ta = $('#composer-input'); if (ta) ta.focus(); }
  },
  'reply-cancel': () => { S.reply = null; renderReplyBar(); },
  edit: el => startEdit(+el.dataset.id),
  'edit-save': () => saveEdit(),
  'edit-cancel': () => cancelEdit(),
  'msg-del': el => {
    const id = +el.dataset.id;
    confirmBox('Excluir mensagem', 'Esta ação não pode ser desfeita.', 'Excluir', () => api('DELETE', `/api/messages/${id}`));
  },
  'msg-pin': el => act(() => api('POST', `/api/messages/${el.dataset.id}/pin`)),
  react: el => act(() => api('POST', `/api/messages/${el.dataset.id}/reactions`, { emoji: el.dataset.e })),
  'react-add': el => emojiPicker(el, em => act(() => api('POST', `/api/messages/${el.dataset.id}/reactions`, { emoji: em }))),
  lightbox: el => { const lb = $('#lightbox'); $('img', lb).src = el.src; lb.classList.remove('hidden'); },
  'friend-add': el => act(async () => { await api('POST', '/api/friends/request', { username: el.dataset.name }); closeModal(); toast('Pedido enviado.'); }),
  'friend-accept': el => act(async () => { await api('POST', `/api/friends/${el.dataset.id}/accept`); closeModal(); }),
  'friend-remove': (el, e) => {
    e.stopPropagation();
    const id = +el.dataset.id;
    const run = () => api('DELETE', `/api/friends/${id}`).then(closeModal);
    if (el.dataset.confirm) confirmBox('Remover amigo', `Remover ${S.friends.get(id)?.user.display_name || 'esta pessoa'} dos seus amigos?`, 'Remover', run);
    else act(run);
  },
  kick: el => confirmBox('Expulsar membro', 'A pessoa poderá voltar com um convite.', 'Expulsar',
    () => api('POST', `/api/servers/${el.dataset.sid}/members/${el.dataset.id}/kick`).then(closeModal)),
  ban: el => confirmBox('Banir membro', 'A pessoa não poderá voltar até ser desbanida.', 'Banir',
    () => api('POST', `/api/servers/${el.dataset.sid}/members/${el.dataset.id}/ban`).then(closeModal)),
  'set-role': el => act(async () => { await api('POST', `/api/servers/${el.dataset.sid}/members/${el.dataset.id}/role`, { role: el.dataset.role }); closeModal(); }),
  transfer: el => confirmBox('Transferir servidor', 'Você virará administrador e a outra pessoa será a dona. Isso não pode ser desfeito por você.', 'Transferir',
    () => api('POST', `/api/servers/${el.dataset.sid}/transfer`, { user_id: +el.dataset.id }).then(closeModal)),
};
document.addEventListener('click', e => {
  if (e.target.id === 'lightbox' || e.target.closest('#lightbox')) { $('#lightbox').classList.add('hidden'); return; }
  const el = e.target.closest('[data-act]');
  if (!el) { if (e.target.closest('#scrim')) closeNav(); return; }
  const fn = ACTIONS[el.dataset.act];
  if (fn) fn(el, e);
});
$('#scrim').onclick = closeNav;

function findMsg(id) { return S.msgs.get(S.view.cid)?.list.find(m => m.id === id); }
function jumpTo(id) {
  const el = $(`#messages [data-id="${id}"]`);
  if (!el) return toast('Mensagem antiga: role para cima para carregá-la.');
  el.scrollIntoView({ block: 'center', behavior: 'smooth' });
  el.classList.add('flash');
  setTimeout(() => el.classList.remove('flash'), 1800);
}

/* ------------------------------------------------------- tempo real (SSE) */
function connect() {
  if (window.es) window.es.close();
  const es = window.es = new EventSource('/api/events');
  Object.keys(HANDLERS).forEach(ev => es.addEventListener(ev, e => {
    try { HANDLERS[ev](JSON.parse(e.data)); } catch (err) { console.error(ev, err); }
  }));
  es.onerror = async () => {
    try { const r = await fetch('/api/me'); if (r.status === 401) showAuth(); } catch { /* offline */ }
  };
}
function notify(m, title, body) {
  if (!document.hidden || !('Notification' in window) || Notification.permission !== 'granted') return;
  const n = new Notification(title, { body: body.slice(0, 140), tag: 'prosa-' + m.channel_id });
  n.onclick = () => { window.focus(); n.close(); };
}
function typingClear(cid, uid) {
  if (S.typing[cid]) { delete S.typing[cid][uid]; if (S.view.cid === cid) renderTyping(); }
}

const HANDLERS = {
  async ready() {
    if (!S.connectedOnce) { S.connectedOnce = true; return; }
    // reconexão: ressincroniza tudo
    try {
      const d = await api('GET', '/api/bootstrap');
      const view = { ...S.view };
      applyBootstrap(d);
      S.msgs = new Map(); S.members = new Map();
      if (view.kind === 'server' && S.servers.has(view.sid)) await act(() => ensureMembers(view.sid));
      S.view = view;
      if (view.cid) await act(() => ensureMessages(view.cid));
      renderAll();
      if (V.cid && !(S.voice[V.cid] || []).some(u => u.user_id === S.me.id)) leaveVoice(false);
    } catch { /* tenta de novo no próximo evento */ }
  },
  async message_create(m) {
    if (m.author) { if (!S.users.has(m.author.id)) cacheUser(m.author); }
    const dm = S.dms.find(d => d.id === m.channel_id);
    let known = dm || [...S.servers.values()].some(s => s.channels.some(c => c.id === m.channel_id));
    if (!known) {
      try { S.dms = await api('GET', '/api/dms'); S.dms.forEach(x => cacheUser(x.user)); known = S.dms.some(d => d.id === m.channel_id); } catch { /* ignora */ }
      if (!known) return;
    }
    const mine = m.author && m.author.id === S.me.id;
    const added = addMessage(m);
    const viewing = S.view.cid === m.channel_id;
    if (viewing && added) renderMessages('keep');
    if (m.author) typingClear(m.channel_id, m.author.id);
    bumpDM(m.channel_id, m.id);
    if (!mine) {
      if (viewing && document.hasFocus()) markRead(m.channel_id);
      else {
        const isDm = S.dms.some(d => d.id === m.channel_id);
        const u = S.unread[m.channel_id] || { count: 0, mentions: 0 };
        u.count++;
        if (isDm || mentionsMe(m.content)) u.mentions++;
        S.unread[m.channel_id] = u;
        renderRail(); renderSidebar(); updateTitle();
      }
      if (S.dms.some(d => d.id === m.channel_id) || mentionsMe(m.content))
        notify(m, displayName(U(m.author)), m.content || 'Enviou um anexo');
    }
  },
  message_update(m) {
    const st = S.msgs.get(m.channel_id);
    if (st) { const i = st.list.findIndex(x => x.id === m.id); if (i >= 0) st.list[i] = m; }
    if (S.view.cid === m.channel_id) renderMessages('keep');
    refreshPins();
  },
  message_delete({ id, channel_id }) {
    const st = S.msgs.get(channel_id);
    if (st) st.list = st.list.filter(x => x.id !== id);
    if (S.reply && S.reply.id === id) { S.reply = null; renderReplyBar(); }
    if (S.editing === id) S.editing = null;
    if (S.view.cid === channel_id) renderMessages('keep');
    refreshPins();
  },
  typing({ channel_id, user_id, name }) {
    const t = S.typing[channel_id] = S.typing[channel_id] || {};
    clearTimeout(t[user_id]?.timer);
    t[user_id] = { name, timer: setTimeout(() => typingClear(channel_id, user_id), 5500) };
    if (S.view.cid === channel_id) renderTyping();
  },
  presence({ user_id, status }) {
    patchUser(user_id, { status });
    renderSidebar(); renderPanel();
    if (S.view.kind === 'home') renderMain();
  },
  user_update(u) {
    cacheUser(u); patchUser(u.id, u);
    renderAll();
  },
  server_create(s) { if (!S.servers.has(s.id)) addServer(s); },
  server_update(p) {
    const s = S.servers.get(p.id);
    if (!s) return;
    Object.assign(s, p);
    renderRail(); renderSidebar();
  },
  server_delete({ id }) {
    const s = S.servers.get(id);
    if (!s) return;
    if (V.cid && s.channels.some(c => c.id === V.cid)) leaveVoice(false);
    S.servers.delete(id); S.members.delete(id);
    s.channels.forEach(c => { S.msgs.delete(c.id); delete S.unread[c.id]; delete S.voice[c.id]; });
    if (S.view.sid === id) { goHome(); toast(`Você não está mais em “${s.name}”.`); }
    else renderRail();
  },
  channel_create(c) {
    const s = S.servers.get(c.server_id);
    if (!s || s.channels.some(x => x.id === c.id)) return;
    s.channels.push(c);
    s.channels.sort((a, b) => (a.kind === b.kind ? a.position - b.position || a.id - b.id : a.kind === 'text' ? -1 : 1));
    renderSidebar();
  },
  channel_update(c) {
    const s = S.servers.get(c.server_id);
    if (!s) return;
    const i = s.channels.findIndex(x => x.id === c.id);
    if (i >= 0) s.channels[i] = c;
    renderSidebar();
    if (S.view.cid === c.id) renderMain();
  },
  channel_delete({ server_id, id }) {
    const s = S.servers.get(server_id);
    if (!s) return;
    s.channels = s.channels.filter(c => c.id !== id);
    S.msgs.delete(id); delete S.unread[id]; delete S.voice[id];
    if (V.cid === id) leaveVoice(false);
    if (S.view.cid === id) goServer(server_id);
    else { renderSidebar(); renderRail(); }
  },
  member_join({ server_id, member }) {
    cacheUser(member);
    const map = S.members.get(server_id);
    if (map) map.set(member.id, member);
    const s = S.servers.get(server_id);
    if (s) s.member_count++;
    renderPanel();
  },
  member_leave({ server_id, user_id }) {
    S.members.get(server_id)?.delete(user_id);
    const s = S.servers.get(server_id);
    if (s) s.member_count = Math.max(0, s.member_count - 1);
    renderPanel();
  },
  member_update({ server_id, user_id, ...patch }) {
    const m = S.members.get(server_id)?.get(user_id);
    if (m) Object.assign(m, patch);
    const s = S.servers.get(server_id);
    if (s && user_id === S.me.id && patch.role) { s.my_role = patch.role; renderSidebar(); }
    renderPanel();
    if (S.view.sid === server_id) renderMessages('keep');
  },
  voice_state({ channel_id, users }) {
    if (users.length) S.voice[channel_id] = users; else delete S.voice[channel_id];
    if (V.cid === channel_id) syncVoicePeers(users);
    renderSidebar(); renderMainIfVoice();
  },
  voice_signal(p) { onVoiceSignal(p); },
  friend_update({ user, relation }) {
    cacheUser(user);
    const prev = S.friends.get(user.id)?.relation;
    if (relation) S.friends.set(user.id, { user: S.users.get(user.id), relation });
    else S.friends.delete(user.id);
    if (relation === 'incoming' && prev !== 'incoming') toast(`${user.display_name} enviou um pedido de amizade.`);
    if (relation === 'friend' && prev === 'outgoing') toast(`${user.display_name} aceitou seu pedido!`);
    renderSidebar();
    if (S.view.kind === 'home') renderMain();
  },
  wallet(w) {
    S.wallet = w;
    Object.assign(S.me, { frame: w.frame, nitro_until: w.nitro_until });
    renderSidebar();
    refreshShop();
  },
  read({ channel_id }) {
    if (S.unread[channel_id]) { delete S.unread[channel_id]; renderRail(); renderSidebar(); updateTitle(); }
  },
};

/* -------------------------------------------------------------- inicialização */
setAuthMode('login');
boot();
'''

ASSETS['style.css'] = r''':root {
  --bg0: #150f1e; --bg1: #1c1629; --bg2: #231c33; --bg3: #2d2541; --bg4: #3a3053;
  --line: #3a3150; --text: #ece7f7; --muted: #a097b8;
  --accent: #ffb454; --accent-hi: #ffc47a; --accent-ink: #2a1800;
  --danger: #ec5b66; --ok: #4fc38a; --idle: #f2b84b;
  --radius: 10px;
  --font: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", sans-serif;
  color-scheme: dark;
}
* { box-sizing: border-box; }
html, body { height: 100%; margin: 0; }
body { background: var(--bg0); color: var(--text); font: 15px/1.45 var(--font); overflow: hidden; -webkit-font-smoothing: antialiased; }
button { font: inherit; color: inherit; background: none; border: 0; cursor: pointer; padding: 0; text-align: inherit; }
input, textarea, select { font: inherit; color: inherit; }
a { color: #8fc2ff; }
h1, h2, h3 { margin: 0; line-height: 1.2; }
hr { border: 0; border-top: 1px solid var(--line); margin: 0; }
:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.hidden { display: none !important; }
.muted { color: var(--muted); }
.small { font-size: 13px; }
.grow { flex: 1; min-width: 0; }
.ic { flex: none; }
::-webkit-scrollbar { width: 10px; height: 10px; }
::-webkit-scrollbar-thumb { background: var(--bg4); border-radius: 8px; border: 2px solid transparent; background-clip: padding-box; }
::-webkit-scrollbar-track { background: transparent; }

/* ------------------------------------------------------------- botões */
.btn { display: inline-flex; align-items: center; justify-content: center; gap: 6px; padding: 9px 16px; border-radius: 8px; font-weight: 600; background: var(--bg3); transition: background .15s; }
.btn:hover { background: var(--bg4); }
.btn.primary { background: var(--accent); color: var(--accent-ink); }
.btn.primary:hover { background: var(--accent-hi); }
.btn.ghost { background: transparent; border: 1px solid var(--line); }
.btn.ghost:hover { background: var(--bg3); }
.btn.danger { background: var(--danger); color: #fff; }
.btn.danger.ghost { background: transparent; color: var(--danger); border-color: var(--danger); }
.btn.danger.ghost:hover { background: rgba(236, 91, 102, .12); }
.btn.wide { width: 100%; }
.btn:disabled { opacity: .5; cursor: not-allowed; }
.icon-btn { display: inline-grid; place-items: center; width: 34px; height: 34px; border-radius: 8px; color: var(--muted); flex: none; }
.icon-btn:hover { background: var(--bg3); color: var(--text); }
.icon-btn.active { color: var(--text); background: var(--bg3); }
.icon-btn.off { color: var(--danger); }
.round { display: inline-grid; place-items: center; width: 36px; height: 36px; border-radius: 50%; background: var(--bg3); color: var(--muted); flex: none; }
.round:hover { color: var(--text); background: var(--bg4); }
.round.ok:hover { color: var(--ok); }
.round.big { width: 52px; height: 52px; color: var(--text); }
.round.off { background: var(--danger); color: #fff; }
.round.danger { background: var(--danger); color: #fff; }
.link { color: #8fc2ff; font-size: 13px; }
.link:hover { text-decoration: underline; }
.only-mobile { display: none; }

/* ------------------------------------------------------------ avatares */
.avatar { position: relative; width: var(--s); height: var(--s); border-radius: 50%; flex: none; display: grid; place-items: center; background: var(--bg4); color: #fff; font-weight: 700; font-size: calc(var(--s) * .42); }
.avatar img { width: 100%; height: 100%; border-radius: 50%; object-fit: cover; }
.avatar .dot { position: absolute; right: -2px; bottom: -2px; width: calc(var(--s) * .32); height: calc(var(--s) * .32); min-width: 10px; min-height: 10px; border-radius: 50%; border: 3px solid var(--bg1); background: #6e6785; }
.dot.online { background: var(--ok) !important; }
.dot.idle { background: var(--idle) !important; }
.dot.dnd { background: var(--danger) !important; }
.sicon { width: var(--s); height: var(--s); border-radius: 50%; display: grid; place-items: center; color: #fff; font-weight: 700; font-size: 20px; transition: border-radius .15s, background .15s; }
.badge { min-width: 18px; height: 18px; padding: 0 5px; border-radius: 9px; background: var(--danger); color: #fff; font-size: 12px; font-weight: 700; display: inline-grid; place-items: center; }
.tag { font-size: 11px; font-weight: 700; padding: 1px 6px; border-radius: 4px; background: var(--bg4); color: var(--muted); }
.tag.owner { background: rgba(255, 180, 84, .18); color: var(--accent); }
.tag.admin { background: rgba(143, 194, 255, .16); color: #8fc2ff; }

/* ------------------------------------------------------------- layout */
.app { display: grid; grid-template-columns: 72px 252px minmax(0, 1fr) auto; height: 100dvh; }
.rail { background: var(--bg0); display: flex; flex-direction: column; align-items: center; gap: 8px; padding: 12px 0; overflow-y: auto; scrollbar-width: none; }
.rail::-webkit-scrollbar { display: none; }
.rail hr { width: 32px; margin: 0; }
.rail-btn { position: relative; width: 72px; display: grid; place-items: center; }
.rail-btn .pill { position: absolute; left: 0; top: 50%; width: 4px; height: 8px; border-radius: 0 4px 4px 0; background: var(--text); transform: translateY(-50%) scaleY(0); transition: .15s; }
.rail-btn.unread .pill { transform: translateY(-50%) scaleY(1); }
.rail-btn.active .pill { height: 36px; transform: translateY(-50%) scaleY(1); }
.rail-btn:hover .sicon, .rail-btn.active .sicon { border-radius: 16px; }
.rail-btn .badge { position: absolute; right: 12px; bottom: -2px; border: 3px solid var(--bg0); box-sizing: content-box; }
.home-icon { background: var(--bg3); color: var(--accent); }
.rail-btn.active .home-icon { background: var(--accent); color: var(--accent-ink); }
.add-icon { background: var(--bg2); color: var(--ok); }
.rail-btn.add:hover .add-icon { background: var(--ok); color: #fff; }

.sidebar { background: var(--bg1); display: flex; flex-direction: column; min-height: 0; border-radius: 14px 0 0 0; }
.side-head { height: 52px; flex: none; display: flex; align-items: center; justify-content: space-between; padding: 0 16px; border-bottom: 1px solid var(--line); font-weight: 700; width: 100%; }
.server-head:hover { background: var(--bg2); }
.side-title { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.side-scroll { flex: 1; overflow-y: auto; padding: 8px 8px 12px; }
.side-label { display: flex; align-items: center; justify-content: space-between; margin: 16px 8px 4px; font-size: 12.5px; font-weight: 700; color: var(--muted); }
.side-label .mini { color: var(--muted); display: grid; place-items: center; }
.side-label .mini:hover { color: var(--text); }
.side-empty { color: var(--muted); font-size: 13px; margin: 8px; }
.side-item { display: flex; align-items: center; gap: 8px; width: 100%; padding: 6px 8px; border-radius: 8px; color: var(--muted); cursor: pointer; min-height: 36px; margin-bottom: 2px; }
.side-item:hover { background: var(--bg2); color: var(--text); }
.side-item.active { background: var(--bg3); color: var(--text); }
.side-item.unread { color: var(--text); font-weight: 700; }
.side-item.dm { padding: 4px 8px; }
.side-item .grow { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.chan-gear { display: none; padding: 2px; border-radius: 4px; }
.chan:hover .chan-gear { display: grid; }
.chan-gear:hover { color: var(--text); background: var(--bg4); }
.side-item.live { color: var(--ok); }
.voice-user { display: flex; align-items: center; gap: 8px; margin: 0 0 2px 28px; padding: 3px 8px; border-radius: 6px; color: var(--muted); font-size: 14px; cursor: pointer; }
.voice-user:hover { background: var(--bg2); color: var(--text); }
.voice-user .avatar, .tile .avatar { box-shadow: 0 0 0 0 var(--ok); transition: box-shadow .12s; }
.voice-user.speaking .avatar, .tile.speaking .avatar { box-shadow: 0 0 0 3px var(--ok); }
.voice-bar { display: flex; align-items: center; gap: 8px; padding: 8px 12px; background: var(--bg2); border-top: 1px solid var(--line); font-size: 13px; }
.voice-bar b { display: block; color: var(--ok); }
.voice-bar span { color: var(--muted); display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.user-panel { flex: none; display: flex; align-items: center; gap: 2px; padding: 8px; background: var(--bg0); }
.user-panel .me { flex: 1; min-width: 0; display: flex; align-items: center; gap: 8px; padding: 4px; border-radius: 8px; }
.user-panel .me:hover { background: var(--bg2); }
.user-panel .avatar .dot { border-color: var(--bg0); }
.me-text { min-width: 0; display: flex; flex-direction: column; line-height: 1.2; }
.me-text b { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 14px; }
.me-text small { color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 12px; }

.main { background: var(--bg2); display: flex; flex-direction: column; min-width: 0; min-height: 0; }
.main-head { height: 52px; flex: none; display: flex; align-items: center; gap: 10px; padding: 0 16px; border-bottom: 1px solid var(--line); }
.main-head h2 { font-size: 16px; white-space: nowrap; }
.head-icon { color: var(--muted); display: grid; }
.topic { color: var(--muted); font-size: 13px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 36ch; padding-left: 10px; border-left: 1px solid var(--line); }
.search-box { display: flex; align-items: center; gap: 6px; background: var(--bg0); border-radius: 8px; padding: 0 10px; height: 32px; color: var(--muted); width: 190px; transition: width .2s; }
.search-box:focus-within { width: 260px; }
.search-box input { flex: 1; min-width: 0; background: none; border: 0; outline: 0; font-size: 14px; }
.tabs { display: flex; gap: 6px; margin-left: 12px; flex-wrap: wrap; }
.tab { padding: 4px 12px; border-radius: 6px; color: var(--muted); font-weight: 600; font-size: 14px; display: inline-flex; gap: 6px; align-items: center; }
.tab:hover { background: var(--bg3); color: var(--text); }
.tab.active { background: var(--bg4); color: var(--text); }
.chat { flex: 1; min-height: 0; display: flex; flex-direction: column; }
.messages { flex: 1; overflow-y: auto; padding: 8px 0 12px; overflow-anchor: none; }
.loading { text-align: center; color: var(--muted); padding: 20px; }
.beginning { padding: 28px 16px 12px; }
.beginning-icon { width: 68px; height: 68px; border-radius: 50%; background: var(--bg4); display: grid; place-items: center; margin-bottom: 12px; }
.beginning h3 { font-size: 26px; margin: 8px 0 4px; }
.day-sep { display: flex; align-items: center; margin: 14px 16px 6px; color: var(--muted); font-size: 12px; font-weight: 600; }
.day-sep::before, .day-sep::after { content: ""; flex: 1; border-top: 1px solid var(--line); }
.day-sep span { padding: 0 10px; }

/* ----------------------------------------------------------- mensagens */
.msg { position: relative; padding: 2px 16px 2px 16px; margin-top: 12px; }
.msg.grouped { margin-top: 0; }
.msg:hover { background: rgba(255, 255, 255, .025); }
.msg.mentioned { background: rgba(255, 180, 84, .08); box-shadow: inset 3px 0 0 var(--accent); }
.msg.mentioned:hover { background: rgba(255, 180, 84, .12); }
.msg.flash { animation: flash 1.8s ease; }
@keyframes flash { 0%, 60% { background: rgba(255, 180, 84, .22); } 100% { background: transparent; } }
.msg-row { display: flex; gap: 14px; }
.gutter-avatar { width: 40px; flex: none; align-self: flex-start; margin-top: 2px; }
.gutter-time { width: 40px; flex: none; font-size: 11px; color: var(--muted); text-align: center; opacity: 0; align-self: center; }
.msg:hover .gutter-time { opacity: 1; }
.msg-body { min-width: 0; flex: 1; }
.msg-head { display: flex; align-items: baseline; gap: 8px; }
.author { font-weight: 700; }
.author:hover { text-decoration: underline; }
.msg-head time { font-size: 12px; color: var(--muted); }
.pinned-flag { color: var(--accent); align-self: center; display: grid; }
.msg-text { word-wrap: break-word; overflow-wrap: anywhere; }
.msg-text code { background: var(--bg0); padding: 1px 5px; border-radius: 4px; font: 13.5px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
.msg-text pre { background: var(--bg0); border: 1px solid var(--line); border-radius: 8px; padding: 10px 12px; margin: 6px 0; overflow-x: auto; max-width: 720px; }
.msg-text pre code { background: none; padding: 0; white-space: pre; }
.msg-text blockquote { margin: 2px 0; padding-left: 10px; border-left: 3px solid var(--bg4); color: #cfc7e0; }
.mention { background: rgba(143, 194, 255, .18); color: #a9d0ff; padding: 0 3px; border-radius: 4px; font-weight: 600; }
.mention.me { background: rgba(255, 180, 84, .25); color: var(--accent-hi); }
.edited { font-size: 11px; color: var(--muted); }
.atts { display: flex; flex-direction: column; align-items: flex-start; gap: 6px; margin-top: 4px; }
.att-img { max-width: min(420px, 100%); max-height: 320px; border-radius: 10px; cursor: zoom-in; background: var(--bg0); }
.att-video { max-width: min(480px, 100%); max-height: 320px; border-radius: 10px; background: #000; }
.att-file { display: flex; align-items: center; gap: 10px; padding: 10px 14px; background: var(--bg1); border: 1px solid var(--line); border-radius: 10px; text-decoration: none; color: var(--text); max-width: 360px; }
.att-file:hover { background: var(--bg3); }
.att-file span { display: flex; flex-direction: column; min-width: 0; }
.att-file b { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: #8fc2ff; }
.att-file small { color: var(--muted); }
.reactions { display: flex; flex-wrap: wrap; gap: 4px; margin-top: 6px; }
.react { display: inline-flex; align-items: center; gap: 5px; padding: 2px 8px; border-radius: 8px; background: var(--bg3); border: 1px solid transparent; font-size: 14px; }
.react span { font-size: 13px; color: var(--muted); font-weight: 600; }
.react:hover { border-color: var(--bg4); }
.react.mine { background: rgba(255, 180, 84, .16); border-color: var(--accent); }
.react.mine span { color: var(--accent-hi); }
.react.add { color: var(--muted); padding: 3px 7px; }
.reply-ref { display: flex; align-items: center; gap: 6px; margin: 0 0 2px 54px; font-size: 13px; color: var(--muted); max-width: calc(100% - 70px); }
.reply-ref b { color: var(--text); font-weight: 600; flex: none; }
.reply-ref span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.reply-ref:hover span { color: var(--text); }
.msg-tools { position: absolute; right: 16px; top: -14px; display: none; background: var(--bg1); border: 1px solid var(--line); border-radius: 8px; padding: 2px; box-shadow: 0 4px 14px rgba(0, 0, 0, .3); }
.msg:hover .msg-tools, .msg:focus-within .msg-tools { display: flex; }
.msg-tools button { width: 30px; height: 28px; display: grid; place-items: center; color: var(--muted); border-radius: 6px; }
.msg-tools button:hover { background: var(--bg3); color: var(--text); }
.msg-tools button.danger:hover { color: var(--danger); }
.edit-box { width: 100%; background: var(--bg0); border: 1px solid var(--line); border-radius: 8px; padding: 8px 10px; resize: none; outline: 0; max-height: 240px; }
.edit-box:focus { border-color: var(--accent); }
.edit-hint { font-size: 12px; color: var(--muted); margin-top: 4px; }
.edit-hint a { color: #8fc2ff; cursor: pointer; }

/* ---------------------------------------------------------- compositor */
.composer-area { flex: none; padding: 0 16px 6px; position: relative; }
.composer { display: flex; align-items: flex-end; gap: 4px; background: var(--bg3); border-radius: 12px; padding: 5px 6px; }
.composer textarea { flex: 1; min-width: 0; background: none; border: 0; outline: 0; resize: none; padding: 7px 6px; max-height: 220px; line-height: 1.4; }
.composer .send { color: var(--accent); }
.typing { height: 22px; font-size: 12.5px; color: var(--muted); padding: 2px 6px; font-weight: 600; }
.reply-bar { display: flex; align-items: center; justify-content: space-between; background: var(--bg1); border-radius: 10px 10px 0 0; padding: 4px 4px 4px 14px; font-size: 13px; color: var(--muted); }
.reply-bar + .composer, #reply-bar:not(:empty) + #file-chips + .composer { border-top-left-radius: 0; border-top-right-radius: 0; }
.chips { display: flex; flex-wrap: wrap; gap: 8px; padding: 8px; background: var(--bg1); border-radius: 10px 10px 0 0; }
.chip { display: inline-flex; align-items: center; gap: 6px; background: var(--bg3); border-radius: 8px; padding: 4px 6px 4px 10px; font-size: 13px; max-width: 260px; }
.chip > span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.chip small { color: var(--muted); }
.chip button { color: var(--muted); display: grid; }
.chip button:hover { color: var(--danger); }
.mention-pop { position: absolute; left: 16px; right: 16px; bottom: 62px; background: var(--bg1); border: 1px solid var(--line); border-radius: 10px; padding: 4px; z-index: 5; box-shadow: 0 8px 24px rgba(0, 0, 0, .35); }
.mention-pop button { display: flex; align-items: center; gap: 8px; width: 100%; padding: 6px 10px; border-radius: 6px; }
.mention-pop button.active, .mention-pop button:hover { background: var(--bg3); }
.mention-pop small { color: var(--muted); }

/* --------------------------------------------------------------- painel */
.side-panel { width: 0; overflow: hidden; background: var(--bg1); display: flex; flex-direction: column; min-height: 0; }
.panel-open .side-panel { width: 268px; border-left: 1px solid var(--line); }
.panel-head { height: 52px; flex: none; display: flex; align-items: center; justify-content: space-between; padding: 0 12px 0 16px; font-weight: 700; border-bottom: 1px solid var(--line); gap: 8px; }
.panel-head span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.panel-scroll { flex: 1; overflow-y: auto; padding: 4px 8px 16px; }
.member { display: flex; align-items: center; gap: 10px; width: 100%; padding: 5px 8px; border-radius: 8px; margin-bottom: 2px; }
.member:hover { background: var(--bg2); }
.member.off { opacity: .55; }
.member-text { flex: 1; min-width: 0; display: flex; flex-direction: column; line-height: 1.25; }
.member-text b { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-weight: 600; }
.member-text small { color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.mini-msg { background: var(--bg2); border-radius: 10px; padding: 10px 12px; margin: 8px 4px; }
.mini-head { display: flex; align-items: baseline; flex-wrap: wrap; gap: 6px; font-size: 13px; margin-bottom: 4px; }
.mini-head time { color: var(--muted); font-size: 12px; }
.mini-actions { display: flex; gap: 14px; margin-top: 6px; }
.empty { flex: 1; display: flex; flex-direction: column; align-items: center; justify-content: center; text-align: center; padding: 40px 20px; gap: 6px; }
.empty.small { padding: 24px 12px; }
.profile-card { position: relative; text-align: left; }
.profile-card .banner { height: 72px; border-radius: 10px 10px 0 0; margin: 0 -2px; }
.profile-card .pc-avatar { margin: -38px 0 6px 14px; width: max-content; border-radius: 50%; border: 5px solid var(--bg1); background: var(--bg1); }
.profile-card .pc-avatar .avatar .dot { border-color: var(--bg1); }
.profile-card h3 { padding: 0 14px; font-size: 20px; }
.profile-card > p { padding: 0 14px; margin: 2px 0; }
.pc-section { margin: 12px 14px 0; padding: 10px 12px; background: var(--bg2); border-radius: 10px; }
.pc-section b { font-size: 12px; color: var(--muted); }
.pc-section p { margin: 4px 0 0; white-space: pre-wrap; overflow-wrap: anywhere; }
.pc-actions { display: flex; flex-wrap: wrap; gap: 8px; margin: 14px 14px 0; }
.pc-actions.mod { margin-top: 8px; }
.modal-profile { margin: -24px -24px 0; }
.modal-profile .pc-avatar { border-color: var(--bg1); }
.modal-profile .modal-actions { padding: 0 14px 16px; }

/* --------------------------------------------------------------- amigos */
.friends-body { flex: 1; overflow-y: auto; padding: 8px 20px 20px; }
.friend-row { display: flex; align-items: center; gap: 12px; padding: 10px 12px; border-top: 1px solid var(--line); border-radius: 10px; cursor: pointer; }
.friend-row:hover { background: var(--bg3); border-color: transparent; }
.friend-row .grow { display: flex; flex-direction: column; line-height: 1.3; }
.friend-row small { color: var(--muted); }
.friend-row .avatar .dot { border-color: var(--bg2); }
.row-actions { display: flex; gap: 8px; }
.add-friend { max-width: 640px; padding: 20px 0; }
.add-friend h3 { margin-bottom: 6px; font-size: 20px; }
.inline-form { display: flex; gap: 8px; margin-top: 12px; }
.inline-form input { flex: 1; min-width: 0; }
.form-msg { margin-top: 10px; font-size: 14px; }
.form-msg.ok { color: var(--ok); }
.form-msg.err { color: var(--danger); }

/* ------------------------------------------------------------------ voz */
.voice-stage { flex: 1; display: flex; flex-direction: column; min-height: 0; }
.tiles { flex: 1; display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); grid-auto-rows: minmax(170px, 230px); gap: 12px; padding: 20px; align-content: center; overflow-y: auto; }
.tile { background: var(--bg1); border-radius: 14px; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 12px; border: 2px solid transparent; transition: border-color .12s; }
.tile.speaking { border-color: var(--ok); }
.tile .avatar .dot { border-color: var(--bg1); }
.tile-name { display: flex; align-items: center; gap: 6px; font-weight: 600; color: var(--text); }
.tile-name .ic { color: var(--danger); }
.voice-controls { flex: none; display: flex; justify-content: center; gap: 14px; padding: 16px; }

/* --------------------------------------------------------------- campos */
input:not([type=radio]):not([type=checkbox]):not([type=file]), textarea { background: var(--bg0); border: 1px solid var(--line); border-radius: 8px; padding: 10px 12px; width: 100%; outline: 0; }
input:focus, textarea:focus { border-color: var(--accent); }
.search-box input, .composer textarea, .edit-box { border: 0; border-radius: 0; padding-left: 0; }
.composer textarea { padding: 7px 6px; background: none; }
.edit-box { border: 1px solid var(--line) !important; border-radius: 8px !important; padding: 8px 10px !important; }
label, .field { display: block; font-size: 13px; font-weight: 600; color: var(--muted); margin: 14px 0 0; }
label input, label textarea { margin-top: 6px; color: var(--text); font-weight: 400; font-size: 15px; }
.field .label { display: block; margin-bottom: 8px; }
.form-error { color: var(--danger); min-height: 20px; font-size: 14px; margin-top: 8px; }

/* ----------------------------------------------------------------- auth */
.auth { position: fixed; inset: 0; display: grid; place-items: center; padding: 20px; background: radial-gradient(900px 500px at 15% 10%, #3a2a5e 0%, transparent 60%), radial-gradient(700px 500px at 90% 95%, #4a2a3a 0%, transparent 55%), var(--bg0); overflow-y: auto; }
.auth-card { width: min(420px, 100%); background: var(--bg1); border: 1px solid var(--line); border-radius: 18px; padding: 32px; box-shadow: 0 20px 60px rgba(0, 0, 0, .45); }
.auth-logo { font-size: 28px; font-weight: 800; color: var(--accent); letter-spacing: -.5px; margin-bottom: 16px; }
.auth-card h1 { font-size: 24px; }
.auth-card p { margin: 6px 0 0; }
.auth-card .btn { margin-top: 6px; padding: 12px; }

/* ---------------------------------------------------------------- modais */
.modal-back { position: fixed; inset: 0; background: rgba(8, 5, 14, .72); display: grid; place-items: center; padding: 16px; z-index: 50; overflow-y: auto; }
.modal { width: min(460px, 100%); max-height: calc(100dvh - 32px); overflow-y: auto; background: var(--bg1); border: 1px solid var(--line); border-radius: 16px; padding: 24px; box-shadow: 0 24px 70px rgba(0, 0, 0, .5); animation: pop .14s ease-out; }
.modal.wide { width: min(640px, 100%); }
@keyframes pop { from { transform: scale(.96); opacity: 0; } }
.modal h2 { font-size: 21px; margin-bottom: 6px; }
.modal h3 { font-size: 16px; margin-bottom: 4px; }
.modal-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 22px; }
.modal-actions.split { justify-content: space-between; align-items: center; }
.modal-actions.split > span { display: flex; gap: 8px; }
.modal-tabs { margin: 16px 0 0; }
.sep { margin: 22px 0 16px; }
.radio-cards { display: grid; gap: 8px; margin-top: 10px; }
.radio-card { display: flex; align-items: center; gap: 12px; padding: 12px 14px; margin: 0; border: 1px solid var(--line); border-radius: 10px; cursor: pointer; color: var(--text); }
.radio-card span { display: flex; flex-direction: column; flex: 1; }
.radio-card small { color: var(--muted); font-weight: 400; }
.radio-card input { width: auto; accent-color: var(--accent); order: 3; }
.radio-card:has(input:checked) { border-color: var(--accent); background: rgba(255, 180, 84, .08); }
.swatches { display: flex; flex-wrap: wrap; gap: 8px; }
.swatch { width: 32px; height: 32px; border-radius: 50%; border: 3px solid transparent; box-shadow: 0 0 0 1px var(--line); }
.swatch.active { border-color: var(--bg1); box-shadow: 0 0 0 2px var(--text); }
.st-avatar { display: flex; align-items: center; gap: 16px; margin-bottom: 4px; }
.st-avatar .btn { margin-right: 6px; padding: 7px 12px; }
.ban-list { max-height: 120px; overflow-y: auto; }
.ban-row { display: flex; justify-content: space-between; padding: 4px 0; }

/* ------------------------------------------------------ popover e toasts */
.popover { position: fixed; z-index: 60; background: var(--bg1); border: 1px solid var(--line); border-radius: 10px; box-shadow: 0 10px 34px rgba(0, 0, 0, .45); padding: 6px; }
.menu { min-width: 200px; }
.menu hr { margin: 6px 4px; }
.menu-item { display: block; width: 100%; padding: 8px 10px; border-radius: 6px; }
.menu-item:hover { background: var(--bg3); }
.menu-item.danger { color: var(--danger); }
.menu-item.danger:hover { background: rgba(236, 91, 102, .14); }
.emoji-grid { display: grid; grid-template-columns: repeat(8, 1fr); gap: 2px; width: 304px; max-height: 260px; overflow-y: auto; }
.emoji-grid button { font-size: 21px; padding: 4px; border-radius: 6px; line-height: 1.2; text-align: center; }
.emoji-grid button:hover { background: var(--bg3); }
.toasts { position: fixed; right: 16px; bottom: 16px; display: flex; flex-direction: column; gap: 8px; z-index: 100; align-items: flex-end; }
.toast { background: var(--bg4); border: 1px solid var(--line); padding: 10px 16px; border-radius: 10px; max-width: min(380px, 90vw); box-shadow: 0 8px 24px rgba(0, 0, 0, .4); animation: pop .16s ease-out; transition: opacity .4s; }
.toast.out { opacity: 0; }
.lightbox { position: fixed; inset: 0; background: rgba(0, 0, 0, .86); display: grid; place-items: center; z-index: 90; cursor: zoom-out; padding: 20px; }
.lightbox img { max-width: 100%; max-height: 100%; border-radius: 8px; }
.scrim { display: none; }
#audio-sink { position: fixed; width: 0; height: 0; overflow: hidden; }

/* ------------------------------------------------------------ responsivo */
@media (max-width: 1000px) { .topic { display: none; } .search-box { width: 140px; } }
@media (max-width: 800px) {
  .app { grid-template-columns: minmax(0, 1fr); }
  .only-mobile { display: inline-grid; }
  .rail, .sidebar { position: fixed; top: 0; bottom: 0; z-index: 30; transform: translateX(-110%); transition: transform .2s; }
  .rail { left: 0; width: 72px; }
  .sidebar { left: 72px; width: min(260px, calc(100vw - 72px)); border-radius: 0; }
  .nav-open .rail, .nav-open .sidebar { transform: none; }
  .nav-open .scrim { display: block; position: fixed; inset: 0; background: rgba(0, 0, 0, .55); z-index: 25; }
  .side-panel { position: fixed; top: 0; bottom: 0; right: 0; z-index: 20; width: 0 !important; border: 0 !important; }
  .panel-open.panel-mobile .side-panel { width: min(320px, 88vw) !important; border-left: 1px solid var(--line) !important; box-shadow: -10px 0 30px rgba(0, 0, 0, .4); }
  .search-box { width: 110px; }
  .search-box:focus-within { width: 160px; }
  .msg-tools { display: flex; top: -16px; opacity: .0; pointer-events: none; }
  .msg:focus-within .msg-tools, .msg:active .msg-tools { opacity: 1; pointer-events: auto; }
  .tabs { margin-left: 4px; }
  .modal { padding: 20px; }
  .modal-profile { margin: -20px -20px 0; }
}
@media (prefers-reduced-motion: reduce) { * { animation: none !important; transition: none !important; } }

/* ===================================================== irisk, molduras, loja */
.avatar .dot { z-index: 2; }
.avatar.fr::before { content: ""; position: absolute; inset: calc(var(--s) * -.1); border-radius: 50%; padding: max(2px, calc(var(--s) * .07)); background: var(--fr); -webkit-mask: linear-gradient(#000 0 0) content-box, linear-gradient(#000 0 0); -webkit-mask-composite: xor; mask-composite: exclude; pointer-events: none; z-index: 1; }
.fr-ouro { --fr: linear-gradient(135deg, #fff1a8, #d99a14 50%, #fff1a8); }
.fr-neon { --fr: conic-gradient(#00f5ff, #ff2bd6, #00f5ff); }
.fr-neon::before { animation: spin 3s linear infinite; }
.fr-fogo { --fr: linear-gradient(0deg, #ff2d00, #ff9a00 60%, #ffd000); }
.fr-fogo::before { animation: flicker 1.4s ease-in-out infinite alternate; }
.fr-aurora { --fr: conic-gradient(#7cffcb, #74b9ff, #b794f6, #7cffcb); }
.fr-aurora::before { animation: spin 6s linear infinite; }
.fr-prisma { --fr: conic-gradient(#ff5b5b, #ffb454, #fff06a, #5bff9a, #5bd7ff, #8a6bff, #ff5bd1, #ff5b5b); }
.fr-prisma::before { animation: spin 2.4s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }
@keyframes flicker { from { opacity: .75; filter: brightness(.95); } to { opacity: 1; filter: brightness(1.25); } }
.nitro-name { background: linear-gradient(90deg, #ffb454, #ff7ab8, #8fc2ff); -webkit-background-clip: text; background-clip: text; color: transparent; font-weight: 800; }
.tag.nitro { background: linear-gradient(90deg, #ffb454, #ff7ab8); color: #2a1800; }

.wallet-chip { display: flex; align-items: center; gap: 6px; width: calc(100% - 16px); margin: 0 8px 6px; padding: 8px 12px; border-radius: 10px; background: var(--bg2); border: 1px solid var(--line); font-size: 14px; }
.wallet-chip:hover { background: var(--bg3); }
.wallet-chip b { font-variant-numeric: tabular-nums; }
.wallet-chip span { color: var(--muted); flex: 1; }
.wallet-chip em { font-style: normal; font-size: 12px; font-weight: 700; background: var(--accent); color: var(--accent-ink); padding: 2px 8px; border-radius: 6px; }
.iris { flex: none; }

.shop-modal { width: min(680px, 100%); }
.shop-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
.bal { display: flex; align-items: center; gap: 8px; background: var(--bg0); padding: 6px 14px; border-radius: 999px; font-size: 18px; }
.bal span { color: var(--muted); font-size: 14px; }
.shop-body { margin-top: 14px; }
.earn-card { background: var(--bg2); border: 1px solid var(--line); border-radius: 12px; padding: 14px 16px; margin-bottom: 12px; }
.earn-card h3 { margin-bottom: 2px; }
.earn-row { display: flex; align-items: center; gap: 12px; padding: 10px 0 2px; flex-wrap: wrap; }
.earn-row .grow { display: flex; flex-direction: column; line-height: 1.3; min-width: 160px; }
.earn-row small { color: var(--muted); }
.reward { display: inline-flex; align-items: center; gap: 5px; font-weight: 700; color: var(--accent); }
.shop-h { margin: 16px 0 8px; }
.shop-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(124px, 1fr)); gap: 10px; }
.shop-card { display: flex; flex-direction: column; align-items: center; gap: 10px; background: var(--bg2); border: 1px solid var(--line); border-radius: 12px; padding: 18px 8px 12px; text-align: center; }
.shop-card .btn { padding: 6px 12px; font-size: 14px; }
.nitro-card { display: flex; align-items: center; gap: 14px; background: linear-gradient(135deg, rgba(255, 180, 84, .12), rgba(255, 122, 184, .12)); border: 1px solid var(--line); border-radius: 12px; padding: 14px 16px; }
.nitro-card ul { margin: 6px 0; padding-left: 18px; color: var(--muted); font-size: 14px; }
.ok-text { color: var(--ok); }
.log { max-height: 340px; overflow-y: auto; }
.log-row { display: flex; align-items: center; gap: 10px; padding: 9px 4px; border-bottom: 1px solid var(--line); }
.log-row .grow { display: flex; flex-direction: column; }
.log-row small { color: var(--muted); }
.plus { color: var(--ok); } .minus { color: var(--danger); }
.video-wrap { position: relative; aspect-ratio: 16 / 9; background: #000; border-radius: 10px; overflow: hidden; margin: 12px 0 8px; }
.video-wrap iframe { position: absolute; inset: 0; width: 100%; height: 100%; border: 0; }
.earn-progress { margin-top: 14px; }
.bar { height: 10px; background: var(--bg0); border-radius: 999px; overflow: hidden; }
.bar i { display: block; height: 100%; width: 0; background: linear-gradient(90deg, var(--accent), var(--ok)); transition: width .6s; }
.ep-text { font-size: 13px; color: var(--muted); margin-top: 6px; min-height: 20px; }
.game-stage { display: flex; flex-direction: column; align-items: center; gap: 8px; margin: 12px 0; }
.game-stage canvas { max-width: 100%; border-radius: 10px; border: 1px solid var(--line); }
.game-score { font-weight: 600; }
.mem-grid { display: grid; grid-template-columns: repeat(4, 76px); gap: 8px; }
.mem { height: 76px; font-size: 34px; display: grid; place-items: center; border-radius: 12px; background: var(--bg4); text-align: center; }
.mem.up { background: var(--bg0); }
.mem.ok { background: rgba(79, 195, 138, .2); }
.alvos { position: relative; width: min(100%, 460px); height: 300px; background: var(--bg0); border: 1px solid var(--line); border-radius: 12px; overflow: hidden; cursor: crosshair; }
.alvo { position: absolute; border-radius: 50%; background: radial-gradient(circle, #fff 0 12%, var(--danger) 13% 38%, #fff 39% 52%, var(--danger) 53%); box-shadow: 0 0 14px rgba(236, 91, 102, .7); cursor: pointer; }
@media (max-width: 800px) { .mem-grid { grid-template-columns: repeat(4, 64px); } .mem { height: 64px; } .nitro-card { flex-direction: column; align-items: stretch; } }

.tag.site { background: #ff5b8a; color: #fff; }
.grant-form { display: flex; gap: 8px; margin-top: 12px; flex-wrap: wrap; }
.grant-form input { flex: 1; min-width: 120px; }
'''


init_db()

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Prosa — clone do Discord")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5000)
    a, _ = ap.parse_known_args()  # tolera argumentos extras do IDLE
    url = f"http://{'127.0.0.1' if a.host == '0.0.0.0' else a.host}:{a.port}"
    print(f"Prosa rodando em {url}  (feche esta janela ou use Ctrl+C para parar)")
    threading.Timer(1.2, lambda: webbrowser.open(url)).start()
    app.run(host=a.host, port=a.port, threaded=True, debug=False, use_reloader=False)
