"""
TWITCH INSIGHTS PRO - MAIN (PYTHON VERSION)
Faithful conversion of main.ps1 - all features preserved
Requires: pip install requests pillow google-auth-oauthlib google-api-python-client
"""

VERSION = "1.3.0"

import sys, os, json, re, socket, threading, datetime, time, subprocess, ctypes
import logging, logging.handlers, hashlib, ssl, queue, functools, sqlite3
import tkinter as tk
from tkinter import messagebox

# ========================================================================
# DEPENDENCY CHECK — auto-install missing packages then restart
# ========================================================================
REQUIRED_PACKAGES = {
    "PIL":                    "pillow",
    "requests":               "requests",
    "google.auth.transport":  "google-auth",
    "google_auth_oauthlib":   "google-auth-oauthlib",
    "googleapiclient":        "google-api-python-client",
    "keyring":                "keyring",
    "pygame":                 "pygame",
    "cryptography":           "cryptography",
}

def _check_and_install():
    missing = []
    for module, package in REQUIRED_PACKAGES.items():
        try:
            __import__(module)
        except ImportError:
            missing.append(package)

    if not missing:
        return  # all good

    print(f"Installing missing packages: {', '.join(missing)}")
    root = tk.Tk()
    root.withdraw()
    answer = messagebox.askyesno(
        "Missing Packages",
        f"The following packages need to be installed:\n\n"
        f"{chr(10).join(missing)}\n\n"
        f"Install now? The app will restart automatically."
    )
    root.destroy()

    if not answer:
        sys.exit(0)

    for pkg in missing:
        print(f"  Installing {pkg}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

    # Restart the app
    messagebox.showinfo("Restart Required",
        "Packages installed! The app will now restart.")
    os.execv(sys.executable, [sys.executable] + sys.argv)

_check_and_install()

# Now safe to import everything
from PIL import Image, ImageTk
import io, requests

# ========================================================================
# ROTATING LOG SETUP
# ========================================================================
_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(_LOG_DIR, exist_ok=True)
_log_handler = logging.handlers.RotatingFileHandler(
    os.path.join(_LOG_DIR, "app.log"),
    maxBytes=5 * 1024 * 1024,  # 5 MB
    backupCount=3,
    encoding="utf-8"
)
_log_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
# File gets full INFO logs, console only shows WARNINGS and ERRORS
_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setLevel(logging.WARNING)
_console_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(_log_handler)
logging.getLogger().addHandler(_console_handler)
log = logging.getLogger("TIP")

print(f"Twitch Insights Pro v{VERSION} starting...")

# ========================================================================
# CONFIG
# ========================================================================
# ── EXE vs .py path resolution ─────────────────────────────────────
if getattr(sys, "frozen", False):
    # Running as PyInstaller EXE — persist data next to the .exe
    BASE_DIR   = os.path.dirname(sys.executable)
    BUNDLE_DIR = sys._MEIPASS          # bundled read-only resources
else:
    # Running as .py script
    BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
    BUNDLE_DIR = BASE_DIR

SCRIPT_DIR       = BASE_DIR            # keep alias so nothing else breaks
DB_PATH          = os.path.join(BASE_DIR, "insights.db")
YT_CLIENT_SECRET = os.path.join(BASE_DIR, "client_secret.json")

def get_resource(rel_path: str) -> str:
    """Return path to a bundled read-only resource (icon, sound, etc)."""
    return os.path.join(BUNDLE_DIR, rel_path)

ICON_PATH = get_resource("app_icon.ico")

# ── SQLite DB — single source of truth for all settings ─────────────────
_db_lock = threading.Lock()

def _db_connect():
    # check_same_thread=False is safe because we guard all access with _db_lock
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def _db_init():
    """Create settings table with all defaults on first run."""
    with _db_connect() as con:
        con.execute("PRAGMA journal_mode=WAL;")  # prevents DB locked errors
        con.execute("PRAGMA synchronous=NORMAL;")  # safe + faster
        con.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )""")
        con.commit()
    defaults = {
        "auto_channel":     "",
        "yt_channel_id":    "",
        "lurker_font_size": "11",
        "chat_font_size":   "18",
        "output_font_size": "12",
        "mute_list":        '["Nightbot", "StreamElements"]',
        "send_target":        "twitch",
        "channel_name_color": "#00e5ff",  # default cyan
        "opacity":            "1.0",       # window opacity 0.3–1.0
        "stay_on_top":        "0",         # always-on-top toggle
    }
    with _db_connect() as con:
        for k, v in defaults.items():
            con.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                        (k, v))
        con.commit()

def db_get(key, fallback=None):
    with _db_lock:
        try:
            with _db_connect() as con:
                row = con.execute("SELECT value FROM settings WHERE key=?",
                                  (key,)).fetchone()
                return row[0] if row else fallback
        except Exception:
            return fallback

def db_set(key, value):
    with _db_lock:
        try:
            with _db_connect() as con:
                con.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                            (key, str(value)))
                con.commit()
        except Exception as ex:
            log.error(f"db_set error: {ex}")

def db_get_mute_list():
    raw = db_get("mute_list", '["Nightbot", "StreamElements"]')
    try:
        return json.loads(raw)
    except Exception:
        return ["Nightbot", "StreamElements"]

def db_set_mute_list(lst):
    db_set("mute_list", json.dumps(lst))

_db_init()  # must run before any db_get calls

# ── Load all settings from DB ────────────────────────────────────────────
AUTO_CHANNEL  = db_get("auto_channel",  "")
YT_CHANNEL_ID = db_get("yt_channel_id", "")
MUTE_LIST     = db_get_mute_list()
SEND_TARGET         = db_get("send_target", "twitch")  # "twitch" | "youtube" | "all"
CHANNEL_NAME_COLOR  = db_get("channel_name_color", "#00e5ff")
WIN_OPACITY         = float(db_get("opacity", "1.0"))
WIN_STAY_ON_TOP     = db_get("stay_on_top", "0") == "1"
SAVE_PATH     = os.path.join(SCRIPT_DIR, "token.dat")  # legacy fallback only

# ── Secure credential vault ──────────────────────────────────────────────
def _kr_file_fallback_path():
    return os.path.join(SCRIPT_DIR, ".credentials.json")

def _kr_file_set(key, value):
    """Fallback file store when keyring is unavailable."""
    try:
        path = _kr_file_fallback_path()
        data = {}
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        data[key] = value
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
        log.info(f"Fallback file saved '{key}'")
    except Exception as ex:
        log.error(f"Fallback file save failed: {ex}")

def _kr_file_get(key):
    """Fallback file read when keyring is unavailable."""
    try:
        path = _kr_file_fallback_path()
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get(key, "")
    except Exception:
        pass
    return ""

# Keys too large for Windows Credential Manager go to file fallback
_FILE_ONLY_KEYS = set()  # yt_token now stored in DB instead

def _kr_get(key):
    # Large keys always from file
    if key in _FILE_ONLY_KEYS:
        return _kr_file_get(key)
    try:
        import keyring as kr
        val = kr.get_password("TwitchInsightsPro", key)
        if val:
            return val
        return _kr_file_get(key)
    except Exception as ex:
        log.warning(f"keyring get '{key}' failed: {ex}")
        return _kr_file_get(key)

def _kr_set(key, value):
    # Large keys always to file
    if key in _FILE_ONLY_KEYS:
        _kr_file_set(key, value)
        return
    try:
        import keyring as kr
        kr.set_password("TwitchInsightsPro", key, value)
        log.info(f"keyring saved '{key}' ok ({len(value)} chars)")
    except Exception as ex:
        log.warning(f"keyring set '{key}' failed: {ex} — using file fallback")
        _kr_file_set(key, value)

def _load_credentials():
    """Load credentials from keyring, migrating from config file if needed."""
    client_id  = _kr_get("client_id")
    yt_api_key = _kr_get("yt_api_key")

    # If missing — app will still launch, user clicks logo to set up
    return client_id, yt_api_key

CLIENT_ID, YT_API_KEY = _load_credentials()
EMOJI_FOLDER      = os.path.join(BASE_DIR, "emojis")
# Sounds always live in the sounds/ subfolder — same location for .py and .exe
SOUNDS_DIR      = os.path.join(BASE_DIR, "sounds")
SOUND_PATH      = os.path.join(SOUNDS_DIR, "notify.wav")
LURK_SOUND_PATH = os.path.join(SOUNDS_DIR, "lurk.wav")
CHAT_LOG_FOLDER   = os.path.join(BASE_DIR, "logs", "chat")
LURKER_LOG_FOLDER = os.path.join(BASE_DIR, "logs", "lurkers")

for folder in [EMOJI_FOLDER, CHAT_LOG_FOLDER, LURKER_LOG_FOLDER,
               SOUNDS_DIR]:
    os.makedirs(folder, exist_ok=True)

# ========================================================================
# GLOBALS
# ========================================================================
g = {
    "token": "",
    "irc_sock": None,   # active Twitch IRC socket for mod commands
    "msg_id_store": {},  # {username: [msg_id, ...]} last 20 IDs per user
    "my_id": None,
    "current_user_url": None,
    "chat_socket": None,
    "chat_username": None,
    "known_lurkers": {},
    "session_lurkers": {},
    "next_refresh_time": datetime.datetime.now() + datetime.timedelta(seconds=60),
    "is_refreshing": False,
    "last_sound_time": datetime.datetime.min,
    "sound_cooldown_seconds": 5,
    "chat_message_count": 0,
    "max_chat_messages": 200,
    "lurker_font_size": int(db_get("lurker_font_size", 11)),
    "chat_font_size":   int(db_get("chat_font_size",   18)),
    "output_font_size": int(db_get("output_font_size", 12)),
    "emote_image_cache":        {},   # EmoteId -> PhotoImage (28x28 for chat)
    "picker_emote_cache":        {},   # EmoteId -> PhotoImage (48x48 for picker)
    "channel_emotes":    [],   # your sub/custom emotes
    "global_emotes":     [],   # twitch global emotes (everyone can use)
    "chat_proc": None,         # Edge chat process
    "yt_service":      None,
    "yt_live_chat_id": None,
    "yt_next_page":    None,
    "yt_polling":      False,
    "yt_watching":     False,  # Lock for youtube watcher
    "yt_poll_job":     None,
    "twitch_icon":        None,
    "youtube_icon":       None,
    "twitch_icon_gray":   None,   # grayscale version for offline
    "youtube_icon_gray":  None,
    "twitch_icon_large":  None,   # 56x56 for stat-box display
    "youtube_icon_large": None,   # 56x56 for stat-box display
    "all_icon":           None,   # combined TW+YT side-by-side for ALL target
    # Connection states: "disconnected" | "connecting" | "connected" | "reconnecting" | "failed"
    "tw_state":        "disconnected",
    "yt_state":        "disconnected",
    # Shutdown events for clean thread termination
    "tw_stop_event":   threading.Event(),
    "yt_stop_event":   threading.Event(),
    # Token health
    "token_expires_at": None,
    "token_scopes":     [],
    # IRC reconnect backoff
    "tw_backoff":       2,
}

# ========================================================================
# COLORS
# ========================================================================
BG_DARK   = "#1f1f23"
BG_PANEL  = "#232328"
BG_PANEL2 = "#2d2d32"
BG_CHAT   = "#0e0e10"
BG_CTRL   = "#18181b"
BG_BTN    = "#2d2d32"
FG_GOLD   = "#ffd700"
FG_CYAN   = "#00e5ff"
FG_WHITE  = "#efeff1"
FG_GRAY   = "#6e6e76"
FG_LIME   = "#00ff00"
FG_PURPLE = "#bf94ff"

# ========================================================================
# HELPERS
# ========================================================================

def load_platform_icons(on_loaded=None):
    """Load platform icons from local images/ folder using branded assets."""

    # Use BASE_DIR so it works inside the compiled EXE
    # Also handles whether folder is named "image" or "images"
    if os.path.exists(os.path.join(BASE_DIR, "images")):
        IMAGES_DIR = os.path.join(BASE_DIR, "images")
    else:
        IMAGES_DIR = os.path.join(BASE_DIR, "image")

    def _fetch():
        def _open_asset(filename, size):
            """Load a branded asset, remove black background, resize."""
            path = os.path.join(IMAGES_DIR, filename)
            if not os.path.exists(path):
                return None, None
            try:
                img = Image.open(path).convert("RGBA").resize(size, Image.LANCZOS)
                # Remove black background — make near-black pixels transparent
                data = img.getdata()
                new_data = []
                for r, g, b, a in data:
                    if r < 30 and g < 30 and b < 30:
                        new_data.append((0, 0, 0, 0))
                    else:
                        new_data.append((r, g, b, a))
                img.putdata(new_data)
                return ImageTk.PhotoImage(img), path
            except Exception:
                return None, None

        # ── TWITCH ──────────────────────────────────────────────────────────
        try:
            tw_live, _  = _open_asset("glitch_flat_purple.png", (28, 28))
            tw_off, _   = _open_asset("glitch_flat_white.png",  (28, 28))
            tw_large, _ = _open_asset("glitch_flat_purple.png", (52, 52))
            tw_large_off, _ = _open_asset("glitch_flat_white.png", (52, 52))
            if tw_live: g["twitch_icon"]        = tw_live
            if tw_off:  g["twitch_icon_gray"]   = tw_off
            if tw_large: g["twitch_icon_large"] = tw_large
            if tw_large_off: g["twitch_icon_large_gray"] = tw_large_off
            g["_need_tw_pil_logos"] = not bool(tw_live)
        except Exception:
            g["_need_tw_pil_logos"] = True

        # ── YOUTUBE ─────────────────────────────────────────────────────────
        try:
            yt_live, _  = _open_asset("yt_icon_red_digital.png",   (32, 32))
            yt_off, _   = _open_asset("yt_icon_white_digital.png",  (32, 32))
            yt_badge, _ = _open_asset("yt_icon_red_digital.png",    (42, 30))
            if yt_live:  g["youtube_icon"]       = yt_live
            if yt_off:   g["youtube_icon_gray"]  = yt_off
            if yt_badge: g["youtube_badge_icon"] = yt_badge
        except Exception:
            pass

        if on_loaded:
            on_loaded()
    threading.Thread(target=_fetch, daemon=True).start()

# ========================================================================
# CANVAS DRAWING HELPERS
# ========================================================================
def _round_rect(c, x0, y0, x1, y1, r=8, tags=None, fill="", outline=""):
    """Draw a filled rounded rectangle on Canvas c."""
    ao = {"style": "pieslice", "fill": fill, "outline": outline or fill}
    fo = {"fill": fill, "outline": ""}
    if tags:
        ao["tags"] = tags; fo["tags"] = tags
    for start, xa, ya, xb, yb in [
        (90,  x0,     y0,     x0+2*r, y0+2*r),
        (0,   x1-2*r, y0,     x1,     y0+2*r),
        (180, x0,     y1-2*r, x0+2*r, y1    ),
        (270, x1-2*r, y1-2*r, x1,     y1    ),
    ]:
        c.create_arc(xa, ya, xb, yb, start=start, extent=90, **ao)
    c.create_rectangle(x0+r, y0, x1-r, y1, **fo)
    c.create_rectangle(x0, y0+r, x1, y1-r, **fo)


def _grad_rect(c, x0, y0, x1, y1, ct, cb, steps=22, r=0, tags=None):
    """Draw a vertical gradient rectangle, optionally rounded."""
    r1,g1,b1 = int(ct[1:3],16),int(ct[3:5],16),int(ct[5:7],16)
    r2,g2,b2 = int(cb[1:3],16),int(cb[3:5],16),int(cb[5:7],16)
    h = y1-y0; sh = h/steps
    t_opts = {"tags": tags} if tags else {}
    for i in range(steps):
        t   = i/steps
        col = "#{:02x}{:02x}{:02x}".format(
            int(r1+(r2-r1)*t), int(g1+(g2-g1)*t), int(b1+(b2-b1)*t))
        sy0 = y0+i*sh; sy1 = sy0+sh+1
        dx  = 0
        if r > 0:
            for dist in (sy0-y0, y1-sy1):
                if 0 <= dist < r:
                    dx = max(dx, r-int((r*r-(r-dist)**2)**0.5+.5))
        c.create_rectangle(x0+dx, sy0, x1-dx, sy1, fill=col, outline="", **t_opts)

def _fmt(n):
    """Format a number with K/M abbreviation to keep it short in the UI."""
    try:
        n = int(n)
    except (TypeError, ValueError):
        return "—"
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 10_000:
        return f"{n/1_000:.1f}K"
    return f"{n:,}"

def _make_yt_badge(width=40, height=30):
    """Render just the red play-button icon with anti-aliasing (no text)."""
    from PIL import Image, ImageDraw
    S = 4  # supersampling factor
    W, H = width * S, height * S
    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d   = ImageDraw.Draw(img)
    # Red rounded rectangle
    d.rounded_rectangle([0, 0, W-1, H-1], radius=10*S, fill=(255, 0, 0, 255))
    # White centered play triangle
    cx, cy = W // 2, H // 2
    tw, th = 12*S, 15*S
    d.polygon([
        (cx - tw//2,     cy - th//2),
        (cx + tw//2 + S, cy),
        (cx - tw//2,     cy + th//2),
    ], fill=(255, 255, 255, 255))
    img = img.resize((width, height), Image.LANCZOS)
    from PIL import ImageTk
    return ImageTk.PhotoImage(img)


def _make_tw_logo(size=68):
    """Draw the Twitch glitch logo using PIL — no network needed."""
    from PIL import Image, ImageDraw
    S = 3
    W = H = size * S
    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    # Purple rounded square background
    d.rounded_rectangle([0, 0, W-1, H-1], radius=10*S, fill=(100, 65, 165, 255))
    # White "T" shape (Twitch glitch mark simplified)
    m = int(W * 0.18)
    # Outer white rect
    d.rectangle([m, m, W-m, int(H*0.72)], fill=(255,255,255,255))
    # Inner cutout (mouth)
    im = int(W * 0.28)
    d.rectangle([im, int(H*0.44), W-im, int(H*0.72)], fill=(100, 65, 165, 255))
    # Left eye bar
    ew = int(W * 0.12); eh = int(H * 0.20)
    d.rectangle([int(W*0.30), int(H*0.24), int(W*0.30)+ew, int(H*0.24)+eh], fill=(100,65,165,255))
    # Right eye bar
    d.rectangle([int(W*0.56), int(H*0.24), int(W*0.56)+ew, int(H*0.24)+eh], fill=(100,65,165,255))
    # Bottom tail notch
    d.polygon([
        (int(W*0.30), int(H*0.72)),
        (int(W*0.30), int(H*0.88)),
        (int(W*0.44), int(H*0.72)),
    ], fill=(255,255,255,255))
    img = img.resize((size, size), Image.LANCZOS)
    from PIL import ImageTk
    return ImageTk.PhotoImage(img)


def _make_char_limit(widget, max_chars):
    """Prevent a tk.Entry from exceeding max_chars characters."""
    def _enforce(*_):
        val = widget.get()
        if len(val) > max_chars:
            widget.delete(max_chars, tk.END)
    widget.bind("<Key>",        _enforce)
    widget.bind("<KeyRelease>", _enforce)


def add_hover(btn, normal_bg, hover_bg=None, hover_fg=None, glow=True):
    """Add hover highlight + glow effect to a tkinter Button."""
    if hover_bg is None:
        # Auto-lighten the normal color
        import colorsys
        try:
            r = int(normal_bg[1:3], 16) / 255
            g = int(normal_bg[3:5], 16) / 255
            b = int(normal_bg[5:7], 16) / 255
            h, s, v = colorsys.rgb_to_hsv(r, g, b)
            v2 = min(1.0, v + 0.25)
            s2 = min(1.0, s + 0.1)
            r2, g2, b2 = colorsys.hsv_to_rgb(h, s2, v2)
            hover_bg = "#{:02x}{:02x}{:02x}".format(
                int(r2*255), int(g2*255), int(b2*255))
        except Exception:
            hover_bg = normal_bg

    def _enter(e):
        btn.configure(bg=hover_bg, relief=tk.FLAT)
        if hover_fg:
            btn.configure(fg=hover_fg)
        if glow:
            btn.configure(cursor="hand2")

    def _leave(e):
        btn.configure(bg=normal_bg, relief=tk.FLAT)
        if hover_fg:
            btn.configure(fg="white")

    btn.bind("<Enter>", _enter)
    btn.bind("<Leave>", _leave)

def save_font_sizes():
    """Save current font sizes to DB."""
    db_set("lurker_font_size", g["lurker_font_size"])
    db_set("chat_font_size",   g["chat_font_size"])
    db_set("output_font_size", g["output_font_size"])

KEYRING_SERVICE = "TwitchInsightsPro"
KEYRING_USER    = "oauth_token"

def save_token(token):
    """Save token securely in OS keyring (falls back to file)."""
    if not token or len(token) < 10:
        return
    try:
        import keyring as kr
        kr.set_password(KEYRING_SERVICE, KEYRING_USER, token.strip())
        log.info("Token saved to OS keyring")
    except Exception:
        try:
            with open(SAVE_PATH, "w", encoding="utf-8") as f:
                f.write(token.strip())
            log.info("Token saved to file (keyring unavailable)")
        except Exception as ex:
            log.error(f"save_token error: {ex}")

def load_token():
    """Load token from OS keyring (falls back to file)."""
    try:
        import keyring as kr
        token = kr.get_password(KEYRING_SERVICE, KEYRING_USER)
        if token:
            log.info("Token loaded from OS keyring")
            return token
    except Exception:
        pass
    # Fallback + migrate from file
    if os.path.exists(SAVE_PATH):
        try:
            with open(SAVE_PATH, "r", encoding="utf-8") as f:
                token = f.read().strip()
            if token:
                save_token(token)
                try:
                    os.remove(SAVE_PATH)
                    log.info("Token migrated from file to OS keyring")
                except Exception:
                    pass
                return token
        except Exception:
            pass
    return ""

def invoke_twitch_api(uri):
    try:
        r = requests.get(uri, headers={
            "Client-ID": CLIENT_ID,
            "Authorization": f"Bearer {g['token'].strip()}",
        }, timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

_mixer_ready = False  # singleton flag -- init pygame mixer at most once

def _ensure_pygame_mixer():
    """Initialize pygame mixer once at 44100 Hz.

    Using a fixed frequency prevents WAV files with different sample rates
    from sounding pitched-up or pitched-down at runtime.
    """
    global _mixer_ready
    if _mixer_ready:
        return True
    try:
        import pygame
        pygame.mixer.init(frequency=44100)
        _mixer_ready = True
        return True
    except Exception as ex:
        log.warning(f"pygame mixer init failed: {ex}")
        return False

def play_sound(path):
    """Play a WAV file via pygame so it shows up as its own app in
    the Windows Volume Mixer (letting the user control volume independently)."""
    if not os.path.exists(path):
        return
    try:
        if _ensure_pygame_mixer():
            import pygame

            def _play():
                try:
                    sound = pygame.mixer.Sound(path)
                    sound.play()
                except Exception as ex:
                    log.warning(f"pygame play_sound error: {ex}")

            threading.Thread(target=_play, daemon=True).start()
        elif sys.platform != "win32":
            # Non-Windows fallback when pygame unavailable
            subprocess.Popen(["aplay", path],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as ex:
        log.warning(f"play_sound error: {ex}")

def play_notify():
    play_sound(SOUND_PATH)

def play_lurk_notify():
    now = datetime.datetime.now()
    if (now - g["last_sound_time"]).total_seconds() >= g["sound_cooldown_seconds"]:
        g["last_sound_time"] = now
        play_sound(LURK_SOUND_PATH)

def log_lurker(username):
    now = datetime.datetime.now()
    path = os.path.join(LURKER_LOG_FOLDER, f"lurkers_{now.strftime('%Y-%m-%d')}.log")
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {username}\n")
    except Exception:
        pass

def log_chat(message):
    now  = datetime.datetime.now()
    path = os.path.join(CHAT_LOG_FOLDER, f"chat_log_{now.strftime('%Y-%m-%d')}.txt")
    try:
        with open(path, "a", encoding="utf-8") as f2:
            f2.write(message + "\n")
    except Exception:
        pass
    log.debug(f"[CHAT] {message}")  # debug only — not written to file by default

def validate_token(token):
    """Validate Twitch token, extract expiry and scopes. Returns True if valid."""
    try:
        r = requests.get("https://id.twitch.tv/oauth2/validate",
                         headers={"Authorization": f"OAuth {token}"}, timeout=10)
        if r.status_code != 200:
            log.warning(f"Token validation failed: {r.status_code}")
            return False
        data = r.json()
        expires_in = data.get("expires_in", 0)
        g["token_scopes"]     = data.get("scopes", [])
        g["token_expires_at"] = (datetime.datetime.now(datetime.timezone.utc) +
                                  datetime.timedelta(seconds=expires_in))
        required = {"chat:read", "moderator:read:chatters",
                    "channel:read:subscriptions"}
        missing  = required - set(g["token_scopes"])
        if missing:
            log.warning(f"Token missing scopes: {missing}")
        log.info(f"Token valid. Expires in {expires_in}s. Scopes: {g['token_scopes']}")
        return True
    except Exception as ex:
        log.error(f"Token validation error: {ex}")
        return False

def token_is_healthy():
    """Check token hasn't expired (with 5 min buffer)."""
    if not g.get("token"):
        return False
    if g["token_expires_at"] is None:
        return True  # unknown — try anyway
    buffer = datetime.timedelta(minutes=5)
    return datetime.datetime.now(datetime.timezone.utc) < (g["token_expires_at"] - buffer)

def get_emote_image(emote_id):
    """Download and cache a Twitch emote image. Returns PhotoImage or None."""
    if emote_id in g["emote_image_cache"]:
        return g["emote_image_cache"][emote_id]
    # Prune cache if over 500 entries to prevent RAM bloat
    if len(g["emote_image_cache"]) > 500:
        g["emote_image_cache"].pop(next(iter(g["emote_image_cache"])))
    path = os.path.join(EMOJI_FOLDER, f"{emote_id}.png")
    if not os.path.exists(path):
        try:
            url = f"https://static-cdn.jtvnw.net/emoticons/v2/{emote_id}/default/dark/2.0"
            r = requests.get(url, timeout=10)
            with open(path, "wb") as f:
                f.write(r.content)
        except Exception:
            return None
    try:
        sz    = max(20, min(48, int(g.get("chat_font_size", 20) * 1.4)))
        img   = Image.open(path).resize((sz, sz), Image.LANCZOS)
        photo = ImageTk.PhotoImage(img)
        g["emote_image_cache"][emote_id] = photo
        return photo
    except Exception:
        return None

def get_picker_emote_image(emote_id):
    """56x56 version of emote image for the picker — separate cache from chat."""
    if emote_id in g["picker_emote_cache"]:
        return g["picker_emote_cache"][emote_id]
    if len(g["picker_emote_cache"]) > 500:
        g["picker_emote_cache"].pop(next(iter(g["picker_emote_cache"])))
    path = os.path.join(EMOJI_FOLDER, f"{emote_id}.png")
    if not os.path.exists(path):
        # Download if not on disk yet
        try:
            url = f"https://static-cdn.jtvnw.net/emoticons/v2/{emote_id}/default/dark/2.0"
            r = requests.get(url, timeout=10)
            with open(path, "wb") as f:
                f.write(r.content)
        except Exception:
            return None
    try:
        img   = Image.open(path).resize((56, 56), Image.LANCZOS)
        photo = ImageTk.PhotoImage(img)
        g["picker_emote_cache"][emote_id] = photo
        return photo
    except Exception:
        return None


def dl_image(url, size, crop=False):
    try:
        r   = requests.get(url, timeout=10)
        img = Image.open(io.BytesIO(r.content)).convert("RGBA")
        if crop:
            # Auto-crop transparent/empty border padding
            bbox = img.getbbox()
            if bbox:
                img = img.crop(bbox)
        # Scale proportionally to fit within size — no stretching
        img.thumbnail(size, Image.LANCZOS)
        # Center on transparent canvas of exact target size
        canvas = Image.new("RGBA", size, (0, 0, 0, 0))
        x = (size[0] - img.width)  // 2
        y = (size[1] - img.height) // 2
        canvas.paste(img, (x, y), img)
        return ImageTk.PhotoImage(canvas)
    except Exception:
        return None

# ========================================================================
# SCROLLABLE COLUMN HELPER
# Returns inner Frame; scrollbar hidden until content overflows
# ========================================================================
def make_scroll_col(parent, row, col):
    outer = tk.Frame(parent, bg="black")
    outer.grid(row=row, column=col, sticky="nsew", padx=2, pady=2)
    outer.rowconfigure(0, weight=1)
    outer.columnconfigure(0, weight=1)

    canvas = tk.Canvas(outer, bg="black", highlightthickness=0, bd=0)
    vsb    = tk.Scrollbar(outer, orient=tk.VERTICAL, command=canvas.yview)
    canvas.grid(row=0, column=0, sticky="nsew")

    vsb_shown = [False]

    def _yscmd(first, last):
        if float(first) <= 0.0 and float(last) >= 1.0:
            if vsb_shown[0]:
                vsb.grid_remove()
                vsb_shown[0] = False
        else:
            if not vsb_shown[0]:
                vsb.grid(row=0, column=1, sticky="ns")
                vsb_shown[0] = True
        vsb.set(first, last)

    canvas.configure(yscrollcommand=_yscmd)
    vsb.configure(command=canvas.yview)

    inner  = tk.Frame(canvas, bg="black")
    wid    = canvas.create_window((0, 0), window=inner, anchor="nw")

    def _on_inner_resize(e):
        bb = canvas.bbox("all")
        if bb:
            canvas.configure(scrollregion=bb)
    inner.bind("<Configure>", _on_inner_resize)
    canvas.bind("<Configure>",
                lambda e: canvas.itemconfig(wid, width=e.width))
    canvas.bind("<Enter>", lambda e: canvas.bind_all(
        "<MouseWheel>",
        lambda ev: canvas.yview_scroll(int(-1 * (ev.delta / 120)), "units")))
    canvas.bind("<Leave>", lambda e: canvas.unbind_all("<MouseWheel>"))

    return inner

# ========================================================================
# MAIN APP
# ========================================================================
class App:
    def __init__(self, root):
        self.root = root
        self.root.title(f"Twitch Insights Pro v{VERSION}")
        self.root.configure(bg=BG_DARK)
        self.root.geometry("1800x900")
        self.root.minsize(1100, 650)

        # ── Windows icon setup ───────────────────────────────────────
        # 1. Tell Windows this is a unique app (not "python.exe") so
        #    the taskbar shows our icon instead of the Python snake.
        try:
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(
                f"TIP.Dashboard.v{VERSION}")
        except Exception:
            pass

        # 2. Locate the icon file (EXE-aware)
        _icon_path = ICON_PATH

        # 3. Set tkinter window + Alt-Tab icon
        if os.path.exists(_icon_path):
            try:
                self.root.iconbitmap(_icon_path)
            except Exception:
                pass

        # 4. Force taskbar icon via Windows API (LoadImage + SendMessage)
        #    This is the only reliable way when running as python.exe
        def _set_taskbar_icon():
            if not os.path.exists(_icon_path):
                return
            try:
                hwnd  = ctypes.windll.user32.GetParent(
                    self.root.winfo_id())
                # Load big (32x32) and small (16x16) icon from file
                LR_LOADFROMFILE = 0x00000010
                big_icon = ctypes.windll.user32.LoadImageW(
                    None, _icon_path, 1, 32, 32, LR_LOADFROMFILE)
                sml_icon = ctypes.windll.user32.LoadImageW(
                    None, _icon_path, 1, 16, 16, LR_LOADFROMFILE)
                WM_SETICON = 0x0080
                ctypes.windll.user32.SendMessageW(hwnd, WM_SETICON, 1, big_icon)
                ctypes.windll.user32.SendMessageW(hwnd, WM_SETICON, 0, sml_icon)
            except Exception:
                pass
        # Run after mainloop starts so the HWND is valid
        self.root.after(200, _set_taskbar_icon)
        self._lurker_timer_id = None
        self._owner_photo      = None
        self._owner_photo_orig = None
        self._owner_pil_img    = None
        self._glow_job         = None
        self._glow_frames      = []
        self._glow_item        = None
        self._sglow_job        = None
        self._sglow_item       = None
        self._sglow_frames     = []
        self._search_pil_img   = None
        self._search_photo_orig = None
        self._owner_photo_orig = None
        self._owner_pil_img   = None
        self._glow_job        = None
        self._glow_frames     = []
        self._search_photo    = None
        # UI queue — background threads post tasks here, main thread executes them
        self.ui_queue = queue.Queue()
        self._build()
        self.root.protocol("WM_DELETE_WINDOW", self._on_quit)
        self.root.attributes("-alpha", WIN_OPACITY)
        self.root.attributes("-topmost", WIN_STAY_ON_TOP)
        self.root.after(16, self._process_ui_queue)   # 16ms = ~60fps heartbeat
        self.root.after(200, self._on_shown)

    # ================================================================
    # ROOT LAYOUT — 3 columns
    # ================================================================
    def _build(self):
        # Use PanedWindow so columns resize properly at any window size
        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=1)

        pw = tk.PanedWindow(self.root, orient=tk.HORIZONTAL,
                            bg="#111", sashwidth=4, sashrelief=tk.FLAT,
                            borderwidth=0)
        pw.grid(row=0, column=0, sticky="nsew")

        lf = tk.Frame(pw, bg=BG_DARK)
        mf = tk.Frame(pw, bg="black")
        rf = tk.Frame(pw, bg=BG_CHAT)

        pw.add(lf, minsize=460, width=500, stretch="never")
        pw.add(mf, minsize=300, width=580, stretch="always")
        pw.add(rf, minsize=300, width=500, stretch="always")

        self._hover_btns = []
        self._build_left(lf)
        self._build_middle(mf)
        self._build_right(rf)
        self._apply_hover_effects()

    # ================================================================
    # LEFT PANEL
    # ================================================================
    def _build_left(self, p):

        # ── Local wrapper classes ─────────────────────────────────────────────
        class _CL:
            """Canvas text item wrapper — accepts .configure(text=, fg=) like a Label."""
            def __init__(self, c, iid): self._c, self._id = c, iid
            def configure(self, text=None, fg=None, **_):
                if text is not None: self._c.itemconfig(self._id, text=str(text))
                if fg   is not None: self._c.itemconfig(self._id, fill=fg)
            config = configure

        class _Stub:
            """No-op stub for widgets that no longer render visually."""
            def configure(self, **_): pass
            config = configure = lambda self, **_: None

        class _PillText:
            """Updates the status text item on a pill canvas."""
            def __init__(self, canvas): self._c = canvas
            def configure(self, text=None, fg=None, image=None, **_):
                if text is not None: self._c.itemconfig("status", text=str(text))
                if fg   is not None: self._c.itemconfig("status", fill=fg)
            config = configure

        class _PillIcon:
            """Updates the icon slot on a pill canvas (image or text fallback)."""
            def __init__(self, canvas): self._c = canvas; self._iid = None
            def configure(self, image=None, text=None, fg=None, **_):
                if self._iid:
                    try: self._c.delete(self._iid)
                    except Exception: pass
                    self._iid = None
                # Also remove original text placeholder with tag "icon"
                try: self._c.delete("icon")
                except Exception: pass
                cy = self._c.winfo_height() // 2 or 19
                if image:
                    self._iid = self._c.create_image(24, cy, image=image, anchor="center")
                    self._c._icon_ref = image
                elif text:
                    self._iid = self._c.create_text(24, cy, text=text,
                        fill=fg or "#555555", font=("Segoe UI", 13, "bold"), anchor="center")
            config = configure

        # ── TOP BAR: pill badges only (channel name is under stats) ──────────
        top_bar = tk.Frame(p, bg=BG_DARK)
        top_bar.pack(fill=tk.X, pady=(2, 2), padx=6)

        # Spacer pushes pills to the right
        tk.Label(top_bar, text="", bg=BG_DARK).pack(side=tk.LEFT, fill=tk.X, expand=True)

        PW, PH, PR = 175, 38, 9  # pill dimensions

        def _make_pill(parent, grad_top, grad_bot, border_col, icon_txt, icon_col):
            c = tk.Canvas(parent, width=PW, height=PH, bg=BG_DARK, highlightthickness=0)
            c.pack(side=tk.RIGHT, padx=(6, 0))
            _grad_rect(c, 1, 1, PW-1, PH-1, grad_top, grad_bot, steps=12, r=PR, tags="pill")
            _round_rect(c, 1, 1, PW-1, PH-1, PR, tags="pill", fill="", outline=border_col)
            # Icon on left (will be replaced with real favicon once loaded)
            c.create_text(24, PH//2, text=icon_txt, fill=icon_col,
                          font=("Segoe UI", 15, "bold"), anchor="center", tags="icon")
            # Divider
            c.create_line(46, 6, 46, PH-6, fill=border_col, width=1, tags="sep")
            c.create_text(PW//2 + 22, PH//2, text="OFFLINE", fill=FG_LIME,
                          font=("Segoe UI", 12, "bold"), anchor="center", tags="status")
            return c

        yt_pill = _make_pill(top_bar, "#250808", "#110303", "#441818", "▶", "#cc2020")
        tw_pill = _make_pill(top_bar, "#180e28", "#0c0818", "#3a2060", "TW", "#6441a5")

        # Wire up wrapper objects so existing _update() code works unchanged
        self.live_dot       = _PillText(tw_pill)
        self.yt_live_label  = _PillText(yt_pill)
        self.tw_status_icon = _PillIcon(tw_pill)
        self.yt_status_icon = _PillIcon(yt_pill)
        self.mic_icon       = _Stub()

        # ── MAIN ROW: profile pic LEFT | right_col (stats + name) RIGHT ─────────
        dash = tk.Frame(p, bg=BG_DARK, height=175)
        dash.pack_propagate(False)  # Forces the frame to stay exactly 175px tall
        dash.pack(fill=tk.X, pady=0)

        # Profile picture — strict 175×175, no padding, anchored to top of dash
        self.owner_canvas = tk.Canvas(dash, width=175, height=175,
                                       bg=BG_DARK, highlightthickness=0, bd=0)
        self.owner_canvas.pack(side=tk.LEFT, padx=(6, 0), pady=0, anchor="n")
        self._owner_img = self.owner_canvas.create_image(87, 87, anchor="center")
        self.owner_canvas.configure(cursor="hand2")
        self.owner_canvas.bind("<Enter>",    lambda e: self._apply_logo_glow(True))
        self.owner_canvas.bind("<Leave>",    lambda e: self._apply_logo_glow(False))
        self.owner_canvas.bind("<Button-1>", lambda e: self._show_auth_popup())

        # Right column — contains divider, stats panels, and channel name
        right_col = tk.Frame(dash, bg=BG_DARK)
        right_col.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Vertical glow divider inside right_col, anchored to top
        div_c = tk.Canvas(right_col, width=8, bg=BG_DARK, highlightthickness=0)
        div_c.pack(side=tk.LEFT, fill=tk.Y, padx=0, pady=0, anchor="n")
        def _draw_div(e=None):
            h = max(div_c.winfo_height(), 100)
            div_c.delete("all")
            segs = ["#1a1826","#242038","#3a3060","#5a4890","#3a3060","#242038","#1a1826"]
            sh   = h / len(segs)
            for i, col in enumerate(segs):
                div_c.create_line(4, int(i*sh), 4, int((i+1)*sh)+2, fill=col, width=2)
        div_c.bind("<Configure>", _draw_div)
        div_c.after(100, _draw_div)

        # Stats + name stacked in this sub-column
        stats_col = tk.Frame(right_col, bg=BG_DARK)
        stats_col.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(4, 6), pady=0)

        # ── TWITCH PANEL — gradient canvas ────────────────────────────────────
        TW_TOP = "#5a3090"; TW_BOT = "#1c0b44"; TW_HDR = "#3c1a70"
        TW_H   = 105

        twc = tk.Canvas(stats_col, height=TW_H, bg=BG_DARK, highlightthickness=0, bd=0)
        twc.pack(fill=tk.X, pady=0)

        _twi_hdr  = twc.create_text(0,0, text="TWITCH CHANNEL STATS", fill="white",
                                     font=("Segoe UI", 9, "bold"), anchor="w")
        _twi_kv   = twc.create_text(0,0, text="VIEWERS:",   fill="#cccccc", font=("Segoe UI",11,"bold"), anchor="e")
        _twi_kf   = twc.create_text(0,0, text="FOLLOWERS:", fill="#cccccc", font=("Segoe UI",11,"bold"), anchor="e")
        _twi_ks   = twc.create_text(0,0, text="SUBS:",      fill="#cccccc", font=("Segoe UI",11,"bold"), anchor="e")
        _twi_vv   = twc.create_text(0,0, text="0",  fill=FG_GOLD,   font=("Segoe UI",15,"bold"), anchor="e")
        _twi_vf   = twc.create_text(0,0, text="0",  fill=FG_WHITE,  font=("Segoe UI",15,"bold"), anchor="e")
        _twi_vs   = twc.create_text(0,0, text="—",  fill="#00bfff", font=("Segoe UI",15,"bold"), anchor="e")
        _twi_icon = twc.create_text(0,0, text="TW", fill="#9060cc", font=("Segoe UI",28,"bold"), anchor="w")

        self.viewer_count_label = _CL(twc, _twi_vv)
        self.follow_label       = _CL(twc, _twi_vf)
        self.subs_label         = _CL(twc, _twi_vs)
        self.acct_age_label     = _Stub()
        self._tw_icon_canvas    = twc
        self._tw_icon_item      = _twi_icon
        self._tw_big_icon       = _Stub()

        def _tw_redraw(e=None):
            w = twc.winfo_width()
            if w < 20: twc.after(40, _tw_redraw); return
            twc.delete("bg")
            _grad_rect(twc, 0, 0, w, TW_H, TW_TOP, TW_BOT, steps=22, r=10, tags="bg")
            twc.create_rectangle(0, 0, w, 24, fill=TW_HDR, outline="", tags="bg")
            twc.create_line(10, 0, w-10, 0, fill="#7a50b8", width=1, tags="bg")
            twc.create_line(10, TW_H-1, w-10, TW_H-1, fill="#2a1240", width=1, tags="bg")
            twc.tag_lower("bg")
            # Icon — left-anchored at x=10, vertically centered (mirrors YouTube layout)
            ix = 10; iy = 24 + (TW_H - 24) // 2
            twc.coords(_twi_icon, ix, iy)
            # Stats — values pinned to right edge, keys immediately left of values
            rx = w - 6          # value right edge
            kx = rx - 65        # key right edge (leaves ~55px for value numbers)
            twc.coords(_twi_hdr, 12, 12)
            twc.coords(_twi_kv, kx, 40); twc.coords(_twi_vv, rx, 40)
            twc.coords(_twi_kf, kx, 62); twc.coords(_twi_vf, rx, 62)
            twc.coords(_twi_ks, kx, 84); twc.coords(_twi_vs, rx, 84)

        twc.bind("<Configure>", _tw_redraw)
        twc.after(60, _tw_redraw)

        # ── YOUTUBE PANEL — gradient canvas ───────────────────────────────────
        YT_TOP = "#6e0c0c"; YT_BOT = "#160101"; YT_HDR = "#8b0000"
        YT_H   = 70

        ytc = tk.Canvas(stats_col, height=YT_H, bg=BG_DARK, highlightthickness=0, bd=0)
        ytc.pack(fill=tk.X)

        _yti_hdr = ytc.create_text(0,0, text="YOUTUBE CHANNEL STATS", fill="white",
                                    font=("Segoe UI", 9, "bold"), anchor="w")
        _yti_kv  = ytc.create_text(0,0, text="VIEWERS:", fill="#cccccc", font=("Segoe UI",11,"bold"), anchor="e")
        _yti_ks  = ytc.create_text(0,0, text="SUBS:",    fill="#cccccc", font=("Segoe UI",11,"bold"), anchor="e")
        _yti_vv  = ytc.create_text(0,0, text="—", fill=FG_GOLD,   font=("Segoe UI",15,"bold"), anchor="e")
        _yti_vs  = ytc.create_text(0,0, text="—", fill="#ff4444", font=("Segoe UI",15,"bold"), anchor="e")

        self.yt_viewers_label = _CL(ytc, _yti_vv)
        self.yt_subs_label    = _CL(ytc, _yti_vs)
        self._yt_big_icon     = _Stub()
        self._yt_stat_icon    = self._yt_big_icon
        self._yt_icon_canvas  = ytc

        # Pre-render smooth YouTube play button badge once
        _yt_badge_img = [None]
        def _get_yt_badge():
            if _yt_badge_img[0] is None:
                try:
                    _yt_badge_img[0] = _make_yt_badge(width=42, height=30)
                except Exception:
                    pass
            return _yt_badge_img[0]

        def _yt_redraw(e=None):
            w = ytc.winfo_width()
            if w < 20: ytc.after(40, _yt_redraw); return
            ytc.delete("bg")
            _grad_rect(ytc, 0, 0, w, YT_H, YT_TOP, YT_BOT, steps=20, r=10, tags="bg")
            ytc.create_rectangle(0, 0, w, 24, fill=YT_HDR, outline="", tags="bg")
            ytc.create_line(10, 0, w-10, 0, fill="#bb2020", width=1, tags="bg")
            ytc.create_line(10, YT_H-1, w-10, YT_H-1, fill="#220000", width=1, tags="bg")

            # Draw branded YouTube play-button icon
            body_mid = 24 + (YT_H - 24) // 2
            badge = g.get("youtube_badge_icon") or _get_yt_badge()
            if badge:
                ytc.create_image(10, body_mid, image=badge, anchor="w", tags="bg")
                ytc._yt_badge_ref = badge
            # "YouTube" wordmark as canvas text — never clips
            ytc.create_text(58, body_mid, text="YouTube", fill="white",
                             font=("Segoe UI", 12, "bold"), anchor="w", tags="bg")

            ytc.tag_lower("bg")

            # Stats — values pinned to right edge, keys immediately left
            rx = w - 6
            kx = rx - 65
            ytc.coords(_yti_hdr, 12, 12)
            ytc.coords(_yti_kv, kx, 38); ytc.coords(_yti_vv, rx, 38)
            ytc.coords(_yti_ks, kx, 58); ytc.coords(_yti_vs, rx, 58)

        ytc.bind("<Configure>", _yt_redraw)
        ytc.after(60, _yt_redraw)

        # Channel name sits directly under the profile picture and stats panels
        ext = tk.Frame(p, bg=BG_PANEL2)
        ext.pack(fill=tk.X, pady=(4, 0))
        self.channel_name_label = tk.Label(ext, text=AUTO_CHANNEL.upper(),
            fg=CHANNEL_NAME_COLOR, bg=BG_PANEL2,
            font=("Segoe UI", 13, "bold"), anchor="w")
        self.channel_name_label.pack(fill=tk.X, padx=10, pady=(4, 0))
        self.game_label = tk.Label(ext, text="CATEGORY: —", fg=FG_CYAN, bg=BG_PANEL2,
                                    font=("Segoe UI", 12, "bold"), anchor="w")
        self.game_label.pack(fill=tk.X, padx=10, pady=(2, 1))
        self.title_label = tk.Label(ext, text="TITLE: —", fg="#aaaaaa", bg=BG_PANEL2,
                                     font=("Segoe UI", 11), anchor="w",
                                     wraplength=460, justify="left")
        self.title_label.pack(fill=tk.X, padx=10, pady=(1, 4))
        ext.bind("<Configure>", lambda e: self.title_label.configure(
            wraplength=max(100, e.width - 20)))

        # Hidden token entry (accessed via logo click)
        self.token_box = tk.Entry(p)
        self.token_box.pack_forget()

        # ── Viewer card ───────────────────────────────────────────────────────
        vc = tk.Frame(p, bg=BG_PANEL)
        vc.pack(fill=tk.X, pady=(0, 4))
        self.search_canvas = tk.Canvas(vc, width=110, height=110,
                                        bg="#222226", highlightthickness=0, bd=0)
        self.search_canvas.pack(side=tk.LEFT, padx=(6, 6), pady=4)
        self._search_img = self.search_canvas.create_image(55, 55, anchor="center")
        self.search_canvas.configure(cursor="hand2")
        self.search_canvas.bind("<Enter>",    lambda e: self._pulse_search_glow(True))
        self.search_canvas.bind("<Leave>",    lambda e: self._pulse_search_glow(False))
        self.search_canvas.bind("<Button-1>", lambda e: self._open_profile())
        self.search_name_label = tk.Label(vc, text="VIEWER:", fg=FG_CYAN, bg=BG_PANEL,
                                           font=("Segoe UI", 11, "bold"))
        self.search_name_label.pack(side=tk.LEFT, padx=4)

        # ── Search bar ────────────────────────────────────────────────
        sr = tk.Frame(p, bg=BG_DARK)
        sr.pack(fill=tk.X, pady=(0, 4))

        self.user_box = tk.Entry(sr, font=("Segoe UI", 11),
                                  fg=FG_GRAY, bg="white",
                                  insertbackground="black", relief=tk.FLAT)
        self.user_box.insert(0, "SEARCH...")
        self.user_box.pack(side=tk.LEFT, fill=tk.X, expand=True, ipady=6)
        self.user_box.bind("<FocusIn>",  self._user_box_focus_in)
        self.user_box.bind("<FocusOut>", self._user_box_focus_out)
        self.user_box.bind("<Return>",   lambda e: self._run_user_lookup())
        _make_char_limit(self.user_box, 25)  # Twitch username max

        self.analyze_btn = tk.Button(sr, text="LOOKUP",
                                      bg="#1a6b1a", fg="white",
                                      font=("Segoe UI", 12, "bold"), relief=tk.FLAT,
                                      padx=14, pady=4, cursor="hand2",
                                      command=self._run_user_lookup)
        self.analyze_btn.pack(side=tk.LEFT, padx=(6, 0))

        # ── Output panel: one row with − + QUIT EDGE, then output box ─
        op = tk.Frame(p, bg=BG_DARK)
        op.pack(fill=tk.BOTH, expand=True, pady=(0, 0))

        # Single row: − + | QUIT | OPEN CHAT (EDGE)
        row1 = tk.Frame(op, bg=BG_DARK)
        row1.pack(fill=tk.X, pady=(0, 4))

        _ominus = tk.Button(row1, text="−", bg=BG_BTN, fg="white",
                  font=("Segoe UI", 14, "bold"), relief=tk.FLAT,
                  width=3, command=self._output_font_minus, cursor="hand2")
        _ominus.pack(side=tk.LEFT)
        self._hover_btns.append(_ominus)

        _oplus = tk.Button(row1, text="+", bg=BG_BTN, fg="white",
                  font=("Segoe UI", 14, "bold"), relief=tk.FLAT,
                  width=3, command=self._output_font_plus, cursor="hand2")
        _oplus.pack(side=tk.LEFT, padx=(2, 6))
        self._hover_btns.append(_oplus)

        _quit = tk.Button(row1, text="QUIT",
                  bg="#8b0000", fg="white",
                  font=("Segoe UI", 10, "bold"), relief=tk.FLAT, pady=4,
                  cursor="hand2", command=self._on_quit)
        _quit.pack(side=tk.LEFT, fill=tk.X, expand=True)
        add_hover(_quit, "#8b0000", "#cc0000")

        _edge = tk.Button(row1, text="OPEN CHAT (EDGE)",
                  bg="#800080", fg="white",
                  font=("Segoe UI", 10, "bold"), relief=tk.FLAT, pady=4,
                  cursor="hand2", command=self._open_chat_edge)
        _edge.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(4, 0))
        add_hover(_edge, "#800080", "#aa00aa")

        # Keep profile_btn as hidden ref (used elsewhere)
        self.profile_btn = tk.Button(op, command=self._open_profile)
        self.profile_btn.pack_forget()

        # Output RichText-equivalent
        out_frame = tk.Frame(op, bg=BG_DARK)
        out_frame.pack(fill=tk.BOTH, expand=True)
        out_frame.rowconfigure(0, weight=1)
        out_frame.columnconfigure(0, weight=1)

        self.output = tk.Text(out_frame, bg="black", fg=FG_WHITE,
                               state=tk.DISABLED,
                               font=("Segoe UI", g["output_font_size"]),
                               wrap=tk.WORD, relief=tk.FLAT, padx=6, pady=4)
        out_vsb = tk.Scrollbar(out_frame, orient=tk.VERTICAL,
                                command=self.output.yview)
        self.output.grid(row=0, column=0, sticky="nsew")
        # autohide scrollbar — only appears when content overflows
        out_vsb_shown = [False]
        def _out_yscmd(first, last):
            if float(first) <= 0.0 and float(last) >= 1.0:
                if out_vsb_shown[0]:
                    out_vsb.grid_remove()
                    out_vsb_shown[0] = False
            else:
                if not out_vsb_shown[0]:
                    out_vsb.grid(row=0, column=1, sticky="ns")
                    out_vsb_shown[0] = True
            out_vsb.set(first, last)
        self.output.configure(yscrollcommand=_out_yscmd)
        self._configure_output_tags()

    # ================================================================
    # MIDDLE PANEL (LURKERS)
    # ================================================================
    def _build_middle(self, p):
        p.rowconfigure(2, weight=1)
        p.columnconfigure(0, weight=1)
        p.columnconfigure(1, weight=1)

        # Compact header
        hdr = tk.Frame(p, bg="black", height=45)
        hdr.grid(row=0, column=0, columnspan=2, sticky="ew")
        hdr.grid_propagate(False)

        _lminus = tk.Button(hdr, text="−", bg=BG_BTN, fg="white",
                  font=("Segoe UI", 16, "bold"), relief=tk.FLAT, width=3,
                  cursor="hand2", command=self._lurker_font_minus)
        _lminus.pack(side=tk.LEFT, padx=(10, 2), pady=8)
        self._hover_btns.append(_lminus)

        _lplus = tk.Button(hdr, text="+", bg=BG_BTN, fg="white",
                  font=("Segoe UI", 16, "bold"), relief=tk.FLAT, width=3,
                  cursor="hand2", command=self._lurker_font_plus)
        _lplus.pack(side=tk.LEFT, padx=(0, 10), pady=8)
        self._hover_btns.append(_lplus)

        self.refresh_label = tk.Label(hdr, text="NEXT REFRESH IN: 60S",
                                       fg=FG_GOLD, bg="black",
                                       font=("Consolas", 12, "bold"))
        self.refresh_label.pack(side=tk.LEFT)

        self.lurker_title = tk.Label(hdr, text="--- LURKERS ---",
                                      fg=FG_GOLD, bg="black",
                                      font=("Consolas", 15, "bold"))
        self.lurker_title.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=8)

        # Column headers
        tk.Label(p, text="LONGEST (>5M)", fg="white", bg="black",
                 font=("Consolas", 12, "bold")).grid(
            row=1, column=0, sticky="ew", pady=(2, 0))
        tk.Label(p, text="RECENT ARRIVALS", fg="white", bg="black",
                 font=("Consolas", 12, "bold")).grid(
            row=1, column=1, sticky="ew", pady=(2, 0))

        # Scrollable lurker panels
        self.longest_box = make_scroll_col(p, row=2, col=0)
        self.recent_box  = make_scroll_col(p, row=2, col=1)

    # ================================================================
    # RIGHT PANEL (CHAT)
    # ================================================================
    def _build_right(self, p):
        p.rowconfigure(2, weight=1)
        p.columnconfigure(0, weight=1)

        # Title panel
        tb = tk.Frame(p, bg=BG_CHAT, height=45)
        tb.grid(row=0, column=0, sticky="ew")
        tb.grid_propagate(False)

        self.chat_title_label = tk.Label(
            tb, text="--- LIVE CHAT ---", fg=FG_GOLD, bg=BG_CHAT,
            font=("Consolas", 15, "bold"))
        self.chat_title_label.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10)


        # Status indicators — icon image + colored dot
        def _make_dot_widget(parent, dot_attr, dot_attr_lbl):
            f = tk.Frame(parent, bg=BG_CHAT)
            f.pack(side=tk.LEFT, padx=(0, 8))
            img_lbl = tk.Label(f, bg=BG_CHAT, text="", image="")
            img_lbl.pack(side=tk.LEFT, padx=(0, 2))
            dot_lbl = tk.Label(f, text="●", fg=FG_GRAY, bg=BG_CHAT,
                               font=("Segoe UI", 9, "bold"))
            dot_lbl.pack(side=tk.LEFT)
            setattr(self, dot_attr,     dot_lbl)
            setattr(self, dot_attr_lbl, img_lbl)
        _make_dot_widget(tb, "tw_dot", "tw_dot_img")
        _make_dot_widget(tb, "yt_dot", "yt_dot_img")

        # Buttons still exist as widgets (used by settings popup) but not packed
        self.chat_connect_btn    = tk.Button(tb, text="TW CONNECT",
            bg="#6441a5", fg="white", font=("Segoe UI", 9, "bold"),
            relief=tk.FLAT, padx=8, cursor="hand2",
            command=self._start_chat_irc)
        self.chat_disconnect_btn = tk.Button(tb, text="TW OFF",
            bg="#8b0000", fg="white", font=("Segoe UI", 9, "bold"),
            relief=tk.FLAT, padx=8, cursor="hand2",
            command=self._stop_chat_irc)
        self.yt_connect_btn      = tk.Button(tb, text="YT CONNECT",
            bg="#cc0000", fg="white", font=("Segoe UI", 9, "bold"),
            relief=tk.FLAT, padx=8, cursor="hand2",
            command=self._start_yt_chat)
        self.yt_disconnect_btn   = tk.Button(tb, text="YT OFF",
            bg="#8b0000", fg="white", font=("Segoe UI", 9, "bold"),
            relief=tk.FLAT, padx=8, cursor="hand2",
            command=self._stop_yt_chat)

        # Controls bar
        cb = tk.Frame(p, bg=BG_CTRL, height=45)
        cb.grid(row=1, column=0, sticky="ew")
        cb.grid_propagate(False)

        self.log_var   = tk.BooleanVar(value=True)
        self.sound_var = tk.BooleanVar(value=True)
        self.debug_var = tk.BooleanVar(value=False)

        ck = dict(fg="white", bg=BG_CTRL, selectcolor=BG_CTRL,
                  activebackground=BG_CTRL, font=("Segoe UI", 9),
                  relief=tk.FLAT, bd=0, cursor="hand2")
        tk.Checkbutton(cb, text="Log Chat",
                       variable=self.log_var, **ck).pack(
            side=tk.LEFT, padx=(10, 8), pady=10)
        tk.Checkbutton(cb, text="Enable Sound",
                       variable=self.sound_var, **ck).pack(
            side=tk.LEFT, padx=(0, 8))
        tk.Checkbutton(cb, text="Debug",
                       variable=self.debug_var, **ck).pack(
            side=tk.LEFT, padx=(0, 10))

        _test = tk.Button(cb, text="Test", bg=BG_BTN, fg="white",
                  font=("Segoe UI", 9), relief=tk.FLAT, padx=8,
                  cursor="hand2", command=play_notify)
        _test.pack(side=tk.LEFT, padx=(0, 10))
        self._hover_btns.append(_test)

        _cminus = tk.Button(cb, text="−", bg=BG_BTN, fg="white",
                  font=("Segoe UI", 14, "bold"), relief=tk.FLAT, width=3,
                  cursor="hand2", command=self._chat_font_minus)
        _cminus.pack(side=tk.LEFT, padx=(0, 3))
        self._hover_btns.append(_cminus)
        _cplus = tk.Button(cb, text="+", bg=BG_BTN, fg="white",
                  font=("Segoe UI", 14, "bold"), relief=tk.FLAT, width=3,
                  cursor="hand2", command=self._chat_font_plus)
        _cplus.pack(side=tk.LEFT)
        self._hover_btns.append(_cplus)

        # Chat text area
        chat_outer = tk.Frame(p, bg=BG_CHAT)
        chat_outer.grid(row=2, column=0, sticky="nsew")
        chat_outer.rowconfigure(0, weight=1)
        chat_outer.columnconfigure(0, weight=1)

        self.chat_text = tk.Text(
            chat_outer, bg=BG_CHAT, fg=FG_WHITE, state=tk.DISABLED,
            font=("Segoe UI", g["chat_font_size"]),
            wrap=tk.WORD, relief=tk.FLAT, padx=15, pady=10, cursor="arrow")
        chat_vsb = tk.Scrollbar(chat_outer, orient=tk.VERTICAL,
                                 command=self.chat_text.yview)
        self.chat_text.grid(row=0, column=0, sticky="nsew")
        chat_vsb_shown = [False]
        def _chat_yscmd(first, last):
            if float(first) <= 0.0 and float(last) >= 1.0:
                if chat_vsb_shown[0]:
                    chat_vsb.grid_remove()
                    chat_vsb_shown[0] = False
            else:
                if not chat_vsb_shown[0]:
                    chat_vsb.grid(row=0, column=1, sticky="ns")
                    chat_vsb_shown[0] = True
            chat_vsb.set(first, last)
        self.chat_text.configure(yscrollcommand=_chat_yscmd)
        self._configure_chat_tags()

        # Status label
        self.chat_status_label = tk.Label(
            p, text="● DISCONNECTED", fg=FG_GRAY, bg=BG_CHAT,
            font=("Segoe UI", 12, "bold"), anchor="center")
        self.chat_status_label.grid(row=3, column=0, sticky="ew", pady=(2, 0))

        # Input row
        ir = tk.Frame(p, bg=BG_CTRL, height=65)
        ir.grid(row=4, column=0, sticky="ew")
        ir.grid_propagate(False)

        self.chat_message_box = tk.Entry(
            ir, font=("Segoe UI", 12), bg="white", fg="black",
            insertbackground="black", relief=tk.FLAT)
        self.chat_message_box.pack(side=tk.LEFT, fill=tk.X, expand=True,
                                    padx=(15, 10), pady=13, ipady=4)
        self.chat_message_box.bind("<Return>", lambda e: self._send_chat_message())
        _make_char_limit(self.chat_message_box, 500)  # Twitch IRC cap
        self._emote_popup = None  # track open picker so we can toggle it
        self.emote_btn = tk.Button(
            ir, text="😀", font=("Segoe UI", 16),
            bg=BG_CTRL, fg=FG_WHITE, relief=tk.FLAT,
            cursor="hand2", bd=0,
            command=self._toggle_emote_picker)
        self.emote_btn.pack(side=tk.LEFT, padx=(0, 6), pady=13)

        # Split send button: [SEND → TW/YT/ALL] [▼]
        send_frame = tk.Frame(ir, bg="#6441a5", bd=0, highlightthickness=0)
        send_frame.pack(side=tk.LEFT, padx=(0, 15), pady=13)
        self.chat_send_btn = tk.Button(
            send_frame, text=self._send_btn_label(), bg="#6441a5", fg="white",
            font=("Segoe UI", 12, "bold"), relief=tk.FLAT, padx=12,
            cursor="hand2", command=self._send_chat_message)
        self.chat_send_btn.pack(side=tk.LEFT, ipady=5)
        self.chat_send_arrow = tk.Button(
            send_frame, text="▼", bg="#6441a5", fg="white",
            font=("Segoe UI", 9, "bold"), relief=tk.FLAT,
            padx=8, width=1, bd=0, highlightthickness=0,
            cursor="hand2", command=self._show_send_menu)
        self.chat_send_arrow.pack(side=tk.LEFT, ipady=5)

    # ================================================================
    # TAG SETUP
    # ================================================================
    def _apply_hover_effects(self):
        """Apply hover glow to all buttons."""
        add_hover(self.analyze_btn,         "#1a6b1a", "#22aa22")
        # connect/disconnect buttons now live in settings popup
        # Send button hover is managed by _set_send_target so colors stay in sync
        # Store refs to font +/- buttons for hover
        for btn in self._hover_btns:
            add_hover(btn, BG_BTN, "#555566")

    def _configure_output_tags(self):
        sz   = g["output_font_size"]
        bold = ("Segoe UI", sz, "bold")
        for tag, col in [
            ("gold",      FG_GOLD),   ("white",    FG_WHITE),
            ("lime",      FG_LIME),   ("cyan",     FG_CYAN),
            ("gray",      FG_GRAY),   ("red",      "#ff4444"),
            ("skyblue",   "#00bfff"), ("silver",   "#c0c0c0"),
            ("lightgray", "#d3d3d3"),
        ]:
            self.output.tag_configure(tag, foreground=col, font=bold)

    def _configure_chat_tags(self):
        sz = g["chat_font_size"]
        self.chat_text.tag_configure(
            "timestamp", foreground=FG_GRAY,
            font=("Segoe UI", max(8, sz - 4)))
        self.chat_text.tag_configure(
            "username", foreground=FG_PURPLE,
            font=("Segoe UI", sz, "bold"))
        self.chat_text.tag_configure(
            "own_username", foreground=FG_LIME,
            font=("Segoe UI", sz, "bold"))
        self.chat_text.tag_configure(
            "message", foreground=FG_WHITE,
            font=("Segoe UI", sz))
        self.chat_text.tag_configure(
            "emote_name", foreground=FG_GOLD,
            font=("Segoe UI", sz, "bold"))
        # YouTube username colours — update all 5 slot tags
        yt_colors = ["#ff6b6b", "#ff9f43", "#ffd700", "#7bed9f", "#70a1ff"]
        for i, yc in enumerate(yt_colors):
            self.chat_text.tag_configure(
                f"yt_username_{i}", foreground=yc,
                font=("Segoe UI", sz, "bold"))
        # Re-apply all existing per-message username tags too
        for tag in self.chat_text.tag_names():
            if tag.startswith("user_"):
                # Determine bold vs regular from existing config
                self.chat_text.tag_configure(
                    tag, font=("Segoe UI", sz, "bold"))

    # ================================================================
    # FONT SIZE CONTROLS
    # ================================================================
    def _output_font_minus(self):
        if g["output_font_size"] > 8:
            g["output_font_size"] -= 1
            self.output.configure(font=("Segoe UI", g["output_font_size"]))
            self._configure_output_tags()
            save_font_sizes()

    def _output_font_plus(self):
        if g["output_font_size"] < 24:
            g["output_font_size"] += 1
            self.output.configure(font=("Segoe UI", g["output_font_size"]))
            self._configure_output_tags()
            save_font_sizes()

    def _lurker_font_minus(self):
        if g["lurker_font_size"] > 8:
            g["lurker_font_size"] -= 1
            save_font_sizes()
            # Trigger immediate refresh like PS1
            g["next_refresh_time"] = datetime.datetime.now()

    def _lurker_font_plus(self):
        if g["lurker_font_size"] < 24:
            g["lurker_font_size"] += 1
            save_font_sizes()
            # Trigger immediate refresh like PS1
            g["next_refresh_time"] = datetime.datetime.now()

    def _chat_font_minus(self):
        if g["chat_font_size"] > 10:
            g["chat_font_size"] -= 2
            g["emote_image_cache"].clear()  # force re-render at new size
            self.chat_text.configure(font=("Segoe UI", g["chat_font_size"]))
            self._configure_chat_tags()
            save_font_sizes()

    def _chat_font_plus(self):
        if g["chat_font_size"] < 32:
            g["chat_font_size"] += 2
            g["emote_image_cache"].clear()  # force re-render at new size
            self.chat_text.configure(font=("Segoe UI", g["chat_font_size"]))
            self._configure_chat_tags()
            save_font_sizes()

    # ================================================================
    # SEARCH BOX
    # ================================================================
    def _user_box_focus_in(self, e=None):
        if self.user_box.get() == "SEARCH...":
            self.user_box.delete(0, tk.END)
            self.user_box.configure(fg="black")

    def _user_box_focus_out(self, e=None):
        if not self.user_box.get().strip():
            self.user_box.insert(0, "SEARCH...")
            self.user_box.configure(fg=FG_GRAY)

    # ================================================================
    # TOKEN
    # ================================================================
    def _apply_logo_glow(self, active):
        """Soft Pillow bloom glow — rendered as a separate canvas item BEHIND the logo."""
        if hasattr(self, "_glow_job") and self._glow_job:
            self.root.after_cancel(self._glow_job)
            self._glow_job = None

        if not active:
            # Hide glow layer, restore plain bg
            if hasattr(self, "_glow_item"):
                self.owner_canvas.delete(self._glow_item)
                self._glow_item = None
            self.owner_canvas.configure(bg=BG_PANEL)
            return

        def _build():
            try:
                from PIL import ImageFilter
                SIZE = 175
                base = self._owner_pil_img
                if base is None:
                    return

                # Scale logo to fit canvas
                logo = base.copy()
                logo.thumbnail((SIZE, SIZE), Image.LANCZOS)
                # Center on canvas
                cx = (SIZE - logo.width)  // 2
                cy = (SIZE - logo.height) // 2

                frames = []
                # Breathing intensities
                steps = [0.2, 0.35, 0.55, 0.75, 0.9, 1.0,
                         0.9, 0.75, 0.55, 0.35, 0.2]
                for intensity in steps:
                    # Start with dark bg
                    bg = Image.new("RGBA", (SIZE, SIZE), (34, 34, 38, 255))

                    # Build glow: white version of logo, heavily blurred
                    glow_src = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
                    white    = Image.new("RGBA", logo.size, (255, 255, 255, 255))
                    if logo.mode == "RGBA":
                        white.putalpha(logo.split()[3])
                    glow_src.paste(white, (cx, cy), white)

                    bloom = glow_src.filter(ImageFilter.GaussianBlur(radius=18))
                    # Apply intensity to alpha
                    r2, g2, b2, a2 = bloom.split()
                    a2 = a2.point(lambda x: int(x * intensity))
                    bloom = Image.merge("RGBA", (r2, g2, b2, a2))

                    # Composite: bg + bloom (glow only, no logo)
                    result = Image.alpha_composite(bg, bloom)
                    frames.append(ImageTk.PhotoImage(result))

                self.root.after(0, lambda: self._run_glow(frames, 0))
            except Exception as ex:
                print(f"Glow build error: {ex}")

        threading.Thread(target=_build, daemon=True).start()

    def _run_glow(self, frames, idx):
        """Draw glow frame BELOW the logo image — logo item stays on top."""
        if not frames:
            return
        try:
            # Delete old glow item and draw new one at bottom of stack
            if hasattr(self, "_glow_item") and self._glow_item:
                self.owner_canvas.delete(self._glow_item)
            self._glow_item = self.owner_canvas.create_image(
                0, 0, anchor="nw", image=frames[idx])
            # Make sure logo stays on top
            self.owner_canvas.tag_raise(self._owner_img)
            self._glow_frames = frames  # prevent GC
            self._glow_job = self.root.after(
                60, lambda: self._run_glow(frames, (idx + 1) % len(frames)))
        except Exception:
            pass

    def _pulse_search_glow(self, active):
        """Soft Pillow bloom glow for viewer canvas — same as owner logo."""
        if hasattr(self, "_sglow_job") and self._sglow_job:
            self.root.after_cancel(self._sglow_job)
            self._sglow_job = None

        if not active:
            if hasattr(self, "_sglow_item") and self._sglow_item:
                try:
                    self.search_canvas.delete(self._sglow_item)
                except Exception:
                    pass
                self._sglow_item = None
            self.search_canvas.configure(bg="#222226")
            return

        def _build():
            try:
                from PIL import ImageFilter
                SIZE = 110
                IMG_SIZE = 80
                base = getattr(self, "_search_pil_img", None)
                if base is None:
                    return  # PIL not ready — do lookup first

                # Prepare logo once
                logo = base.copy()
                logo.thumbnail((IMG_SIZE, IMG_SIZE), Image.LANCZOS)
                cx = (SIZE - logo.width)  // 2
                cy = (SIZE - logo.height) // 2

                # Build glow source once
                glow_src = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
                white = Image.new("RGBA", logo.size, (255, 255, 255, 255))
                if logo.mode == "RGBA":
                    white.putalpha(logo.split()[3])
                glow_src.paste(white, (cx, cy), white)
                bloom_base = glow_src.filter(ImageFilter.GaussianBlur(radius=10))

                # Build each frame with different intensity
                frames = []
                for intensity in [0.2, 0.4, 0.65, 0.85, 1.0,
                                  0.85, 0.65, 0.4, 0.2]:
                    bg = Image.new("RGBA", (SIZE, SIZE), (34, 34, 38, 255))  # matches canvas #222226
                    r2, g2, b2, a2 = bloom_base.split()
                    a2 = a2.point(lambda x: int(x * intensity))
                    bloom = Image.merge("RGBA", (r2, g2, b2, a2))
                    result = Image.alpha_composite(bg, bloom)
                    frames.append(ImageTk.PhotoImage(result))

                self.root.after(0, lambda: self._run_search_glow(frames, 0))
            except Exception as ex:
                print(f"Search glow error: {ex}")

        threading.Thread(target=_build, daemon=True).start()

    def _run_search_glow(self, frames, idx):
        """Animate bloom frames behind viewer image."""
        if not frames:
            return
        try:
            if hasattr(self, "_sglow_item") and self._sglow_item:
                self.search_canvas.delete(self._sglow_item)
            self._sglow_item = self.search_canvas.create_image(
                0, 0, anchor="nw", image=frames[idx])
            self.search_canvas.tag_raise(self._search_img)
            self._sglow_frames = frames
            self._sglow_job = self.root.after(
                60, lambda: self._run_search_glow(frames, (idx+1) % len(frames)))
        except Exception:
            pass

    def _show_auth_popup(self):
        """Full credentials & settings popup — triggered by clicking the logo."""
        global CLIENT_ID, YT_API_KEY

        popup = tk.Toplevel(self.root)
        popup.title("Settings & Credentials")
        popup.configure(bg=BG_DARK)
        popup.resizable(False, False)
        popup.grab_set()

        popup.update_idletasks()
        pw = 580
        ph = 960
        # Center the popup relative to the main app window, ignoring primary screen bounds
        x = self.root.winfo_x() + (self.root.winfo_width() // 2) - (pw // 2)
        y = self.root.winfo_y() + (self.root.winfo_height() // 2) - (ph // 2)
        
        popup.resizable(False, False)
        popup.geometry(f"{pw}x{ph}+{x}+{y}")

        _scroll_frame = popup  # no scroll needed — alias for btn_row compat

        # ── Title ────────────────────────────────────────────────────
        tk.Label(popup, text="⚙️  SETTINGS & CREDENTIALS",
                 fg=FG_CYAN, bg=BG_DARK,
                 font=("Segoe UI", 14, "bold")).pack(pady=(16, 2))
        tk.Label(popup, text="All credentials saved to OS keyring — never stored in files.",
                 fg=FG_GRAY, bg=BG_DARK,
                 font=("Segoe UI", 9)).pack(pady=(0, 10))

        # ── Form ─────────────────────────────────────────────────────
        form = tk.Frame(popup, bg=BG_PANEL2)
        form.pack(fill=tk.X, padx=20, pady=(0, 10))

        def _row(parent, row, label, show="", prefill="", color=FG_CYAN, maxchars=None):
            tk.Label(parent, text=label, fg=color, bg=BG_PANEL2,
                     font=("Segoe UI", 10, "bold"), anchor="w",
                     width=20).grid(row=row, column=0, sticky="w",
                                    padx=(12, 4), pady=6)
            cell = tk.Frame(parent, bg=BG_PANEL2)
            cell.grid(row=row, column=1, sticky="ew", padx=(0, 12), pady=6)
            cell.columnconfigure(0, weight=1)
            parent.columnconfigure(1, weight=1)

            e = tk.Entry(cell, show=show, font=("Segoe UI", 10),
                         bg="white", fg="black", relief=tk.FLAT,
                         insertbackground="black")
            e.pack(side=tk.LEFT, fill=tk.X, expand=True, ipady=5)
            if maxchars:
                _make_char_limit(e, maxchars)
            if prefill:
                e.insert(0, prefill)

            if show == "*":
                # Eye toggle button
                eye_var = tk.BooleanVar(value=False)
                def _toggle(entry=e, var=eye_var, btn_ref=[None]):
                    var.set(not var.get())
                    entry.configure(show="" if var.get() else "*")
                    btn_ref[0].configure(text="🙈" if var.get() else "👁")
                eye_btn = tk.Button(cell, text="👁", bg=BG_PANEL2, fg=FG_CYAN,
                                    font=("Segoe UI", 11), relief=tk.FLAT,
                                    cursor="hand2", bd=0,
                                    command=_toggle)
                eye_btn.pack(side=tk.LEFT, padx=(4, 0))
                # Store ref so toggle can update label
                def _toggle2(entry=e, var=eye_var, btn=eye_btn):
                    var.set(not var.get())
                    entry.configure(show="" if var.get() else "*")
                    btn.configure(text="🙈" if var.get() else "👁")
                eye_btn.configure(command=_toggle2)
            return e

        # Section: Twitch
        tk.Label(form, text="— TWITCH —", fg=FG_PURPLE, bg=BG_PANEL2,
                 font=("Segoe UI", 10, "bold")).grid(
            row=0, column=0, columnspan=2, pady=(10, 2))

        # OAuth Token with GET TOKEN button
        tk.Label(form, text="OAuth Token:", fg=FG_CYAN, bg=BG_PANEL2,
                 font=("Segoe UI", 10, "bold"), anchor="w",
                 width=20).grid(row=1, column=0, sticky="w", padx=(12, 4), pady=6)
        token_row = tk.Frame(form, bg=BG_PANEL2)
        token_row.grid(row=1, column=1, sticky="ew", padx=(0, 12), pady=6)
        form.columnconfigure(1, weight=1)
        token_row.columnconfigure(0, weight=1)
        e_token = tk.Entry(token_row, show="*", font=("Segoe UI", 10),
                           bg="white", fg="black", relief=tk.FLAT,
                           insertbackground="black")
        e_token.pack(side=tk.LEFT, fill=tk.X, expand=True, ipady=5)
        _make_char_limit(e_token, 512)  # OAuth token safe cap
        if g["token"]:
            e_token.insert(0, g["token"])

        # Eye toggle for token
        tok_vis = tk.BooleanVar(value=False)
        def _tok_toggle(e=e_token, v=tok_vis, b_ref=[None]):
            v.set(not v.get())
            e.configure(show="" if v.get() else "*")
            b_ref[0].configure(text="🙈" if v.get() else "👁")
        tok_eye = tk.Button(token_row, text="👁", bg=BG_PANEL2, fg=FG_CYAN,
                            font=("Segoe UI", 11), relief=tk.FLAT,
                            cursor="hand2", bd=0, command=_tok_toggle)
        tok_eye.pack(side=tk.LEFT, padx=(4, 0))
        def _tok_toggle2(e=e_token, v=tok_vis, b=tok_eye):
            v.set(not v.get())
            e.configure(show="" if v.get() else "*")
            b.configure(text="🙈" if v.get() else "👁")
        tok_eye.configure(command=_tok_toggle2)

        tk.Button(token_row, text="🌐  GET TW TOKEN",
                  bg="#483d8b", fg="white", font=("Segoe UI", 9, "bold"),
                  relief=tk.FLAT, padx=8, pady=4, cursor="hand2",
                  command=self._open_auth).pack(side=tk.LEFT, padx=(6, 0))

        e_client_id = _row(form, 2, "Client ID:", show="*", maxchars=40,
                           prefill=CLIENT_ID or _kr_get("client_id"))

        # Section: YouTube
        tk.Label(form, text="— YOUTUBE —", fg="#ff4444", bg=BG_PANEL2,
                 font=("Segoe UI", 10, "bold")).grid(
            row=3, column=0, columnspan=2, pady=(10, 2))

        e_yt_key = _row(form, 4, "API Key:", show="*", maxchars=45,
                        prefill=YT_API_KEY or _kr_get("yt_api_key"))

        # Re-authorize YouTube row
        tk.Label(form, text="Session Token:", fg=FG_CYAN, bg=BG_PANEL2,
                 font=("Segoe UI", 10, "bold"), anchor="w",
                 width=20).grid(row=5, column=0, sticky="w", padx=(12,4), pady=6)
        yt_tok_frame = tk.Frame(form, bg=BG_PANEL2)
        yt_tok_frame.grid(row=5, column=1, sticky="ew", padx=(0,12), pady=6)

        yt_tok_status = db_get("yt_token")
        yt_status_lbl = tk.Label(yt_tok_frame,
            text="✅ Saved" if yt_tok_status else "⚠️ Not authorized",
            fg=FG_LIME if yt_tok_status else "#ff8c00",
            bg=BG_PANEL2, font=("Segoe UI", 10))
        yt_status_lbl.pack(side=tk.LEFT, padx=(0, 10))

        def _reauth_yt():
            # Clear saved token and force browser login
            db_set("yt_token", "")
            popup.destroy()
            def _do_reauth():
                try:
                    from googleapiclient.discovery import build
                    creds = self._get_yt_creds(allow_browser=True)
                    if creds:
                        g["yt_service"] = build("youtube", "v3", credentials=creds)
                        self.ui(lambda: self._append_chat_message(
                            datetime.datetime.now().strftime("%I:%M %p"),
                            "SYSTEM", "✅ YouTube re-authorized successfully!",
                            platform="youtube"))
                        self.ui(self._start_yt_watcher)
                except Exception as ex:
                    log.warning(f"YT reauth failed: {ex}")
            threading.Thread(target=_do_reauth, daemon=True).start()

        tk.Button(yt_tok_frame, text="📺  GET YT TOKEN",
                  bg="#cc0000", fg="white",
                  font=("Segoe UI", 9, "bold"), relief=tk.FLAT,
                  padx=8, pady=4, cursor="hand2",
                  command=_reauth_yt).pack(side=tk.LEFT)

        # Section: App Settings
        tk.Label(form, text="— APP SETTINGS —", fg=FG_GOLD, bg=BG_PANEL2,
                 font=("Segoe UI", 10, "bold")).grid(
            row=7, column=0, columnspan=2, pady=(10, 2))

        # Font size spinboxes
        def _spin_row(row, label, key, min_v, max_v):
            tk.Label(form, text=label, fg=FG_CYAN, bg=BG_PANEL2,
                     font=("Segoe UI", 10, "bold"), anchor="w",
                     width=20).grid(row=row, column=0, sticky="w",
                                    padx=(12, 4), pady=4)
            var = tk.IntVar(value=g[key])
            spin = tk.Spinbox(form, from_=min_v, to=max_v, textvariable=var,
                              width=5, font=("Segoe UI", 10),
                              bg="white", fg="black", relief=tk.FLAT,
                              buttonbackground=BG_BTN)
            spin.grid(row=row, column=1, sticky="w", padx=(0, 12), pady=4, ipady=3)
            return var

        e_auto_ch  = _row(form, 8,  "Twitch Channel:",  prefill=AUTO_CHANNEL, maxchars=25)
        e_yt_ch_id = _row(form, 9,  "YouTube Channel ID:", prefill=YT_CHANNEL_ID, maxchars=30)
        var_lurker = _spin_row(10, "Lurker Font Size:",  "lurker_font_size", 8, 24)
        var_chat   = _spin_row(11, "Chat Font Size:",    "chat_font_size",   10, 32)
        var_output = _spin_row(12, "Output Font Size:",  "output_font_size", 8, 24)

        # Mute list editor
        tk.Label(form, text="Mute List:", fg=FG_CYAN, bg=BG_PANEL2,
                 font=("Segoe UI", 10, "bold"), anchor="w",
                 width=20).grid(row=13, column=0, sticky="nw",
                                padx=(12, 4), pady=6)
        mute_frame = tk.Frame(form, bg=BG_PANEL2)
        mute_frame.grid(row=13, column=1, sticky="ew", padx=(0, 12), pady=6)
        mute_frame.columnconfigure(0, weight=1)

        mute_text = tk.Text(mute_frame, height=3, font=("Segoe UI", 10),
                            bg="white", fg="black", relief=tk.FLAT,
                            wrap=tk.WORD)
        mute_text.pack(fill=tk.X)
        mute_text.insert(tk.END, ", ".join(db_get_mute_list()))
        tk.Label(mute_frame, text="Comma separated, e.g: Nightbot, StreamElements",
                 fg=FG_GRAY, bg=BG_PANEL2,
                 font=("Segoe UI", 8)).pack(anchor="w")

        # Default send target
        tk.Label(form, text="Default Send To:", fg=FG_CYAN, bg=BG_PANEL2,
                 font=("Segoe UI", 10, "bold"), anchor="w",
                 width=20).grid(row=14, column=0, sticky="w",
                                padx=(12, 4), pady=6)
        send_target_var = tk.StringVar(value=db_get("send_target", "twitch"))
        send_target_frame = tk.Frame(form, bg=BG_PANEL2)
        send_target_frame.grid(row=14, column=1, sticky="w", padx=(0, 12), pady=6)

        # ── Row 15: Channel name color picker ────────────────────────
        tk.Label(form, text="Name Color:", fg=FG_CYAN, bg=BG_PANEL2,
                 font=("Segoe UI", 10, "bold"), anchor="w",
                 width=20).grid(row=15, column=0, sticky="w",
                               padx=(12, 4), pady=6)
        color_row = tk.Frame(form, bg=BG_PANEL2)
        color_row.grid(row=15, column=1, sticky="w", padx=(0, 12), pady=6)
        color_preview = tk.Label(color_row, text="  Aa  ",
                                  bg=db_get("channel_name_color", "#00e5ff"),
                                  fg="white", font=("Segoe UI", 10, "bold"),
                                  relief=tk.FLAT, padx=4)
        color_preview.pack(side=tk.LEFT, padx=(0, 8))
        color_var = tk.StringVar(value=db_get("channel_name_color", "#00e5ff"))
        color_hex  = tk.Entry(color_row, textvariable=color_var, width=9,
                              font=("Segoe UI", 10), bg="white", fg="black",
                              relief=tk.FLAT, insertbackground="black")
        color_hex.pack(side=tk.LEFT, ipady=4)
        _make_char_limit(color_hex, 7)
        PRESET_COLORS = [
            ("#00e5ff","Cyan"), ("#6441a5","Purple"), ("#ff4444","Red"),
            ("#ffd700","Gold"), ("#00ff7f","Green"), ("#ff69b4","Pink"),
            ("#ffffff","White"),("custom","Custom…"),
        ]
        def _pick_color(hex_code):
            if hex_code == "custom":
                from tkinter import colorchooser
                result = colorchooser.askcolor(
                    color=color_var.get(), title="Pick channel name color",
                    parent=popup)
                if result and result[1]:
                    hex_code = result[1]
                else:
                    return
            color_var.set(hex_code)
            color_preview.configure(bg=hex_code)
        def _on_hex_change(*_):
            val = color_var.get()
            if len(val) == 7 and val.startswith("#"):
                try:
                    color_preview.configure(bg=val)
                except Exception:
                    pass
        color_var.trace_add("write", _on_hex_change)
        swatches = tk.Frame(color_row, bg=BG_PANEL2)
        swatches.pack(side=tk.LEFT, padx=(8, 0))
        for hx, name in PRESET_COLORS:
            bg = hx if hx != "custom" else "#333333"
            btn = tk.Button(swatches, bg=bg, text="", width=2, height=1,
                            relief=tk.FLAT, cursor="hand2", bd=0,
                            command=lambda h=hx: _pick_color(h))
            if hx == "custom":
                btn.configure(text="…", fg="white", font=("Segoe UI", 9))
            btn.pack(side=tk.LEFT, padx=2)

        # ── Row 16: Window options (opacity + stay on top) ───────────
        tk.Label(form, text="Window:", fg=FG_CYAN, bg=BG_PANEL2,
                 font=("Segoe UI", 10, "bold"), anchor="w",
                 width=20).grid(row=16, column=0, sticky="w",
                               padx=(12, 4), pady=6)
        win_row = tk.Frame(form, bg=BG_PANEL2)
        win_row.grid(row=16, column=1, sticky="w", padx=(0, 12), pady=6)

        # Stay on top checkbox
        self._sot_var = tk.BooleanVar(value=WIN_STAY_ON_TOP)
        tk.Checkbutton(win_row, text="📌 Stay on Top",
                       variable=self._sot_var, bg=BG_PANEL2,
                       fg=FG_WHITE, selectcolor=BG_PANEL2,
                       activebackground=BG_PANEL2, activeforeground=FG_GOLD,
                       font=("Segoe UI", 10), relief=tk.FLAT,
                       command=self._toggle_stay_on_top
                       ).pack(side=tk.LEFT, padx=(0, 16))

        # Opacity slider
        tk.Label(win_row, text="👁 Opacity:", fg=FG_CYAN, bg=BG_PANEL2,
                 font=("Segoe UI", 10)).pack(side=tk.LEFT, padx=(0, 6))
        self._opacity_slider = tk.Scale(
            win_row, from_=30, to=100, orient=tk.HORIZONTAL,
            length=100, showvalue=True, sliderlength=14,
            bg=BG_PANEL2, fg=FG_WHITE, troughcolor="#444455",
            highlightthickness=0, bd=0, relief=tk.FLAT,
            font=("Segoe UI", 8),
            command=self._set_opacity)
        self._opacity_slider.set(int(WIN_OPACITY * 100))
        self._opacity_slider.pack(side=tk.LEFT)
        for val, lbl in [("twitch", "⚡ Twitch"), ("youtube", "📺 YouTube"),
                          ("all", "🌍 All")]:
            tk.Radiobutton(send_target_frame, text=lbl,
                           variable=send_target_var, value=val,
                           bg=BG_PANEL2, fg=FG_WHITE,
                           selectcolor=BG_CTRL,
                           activebackground=BG_PANEL2,
                           font=("Segoe UI", 10)).pack(side=tk.LEFT, padx=(0, 12))

        # Section: Connections
        tk.Label(form, text="— CONNECTIONS —", fg=FG_LIME, bg=BG_PANEL2,
                 font=("Segoe UI", 10, "bold")).grid(
            row=18, column=0, columnspan=2, pady=(10, 2))

        conn_frame = tk.Frame(form, bg=BG_PANEL2)
        conn_frame.grid(row=19, column=0, columnspan=2, pady=(4, 10), padx=12, sticky="ew")

        # Twitch connect/disconnect
        tw_state = g.get("tw_state", "disconnected")
        self._settings_tw_btn = tk.Button(
            conn_frame,
            text="⚡ TW DISCONNECT" if tw_state == "connected" else "⚡ TW CONNECT",
            bg="#8b0000" if tw_state == "connected" else "#6441a5",
            fg="white", font=("Segoe UI", 10, "bold"),
            relief=tk.FLAT, padx=12, pady=6, cursor="hand2")
        self._settings_tw_btn.pack(side=tk.LEFT, padx=(0, 8))

        def _tw_toggle():
            if g.get("tw_state") == "connected":
                self._stop_chat_irc()
                self._settings_tw_btn.configure(
                    text="⚡ TW CONNECT", bg="#6441a5")
            else:
                popup.destroy()
                self._start_chat_irc()
        self._settings_tw_btn.configure(command=_tw_toggle)

        # YouTube connect/disconnect
        yt_state = g.get("yt_state", "disconnected")
        self._settings_yt_btn = tk.Button(
            conn_frame,
            text="▶ YT DISCONNECT" if yt_state == "connected" else "▶ YT CONNECT",
            bg="#8b0000" if yt_state == "connected" else "#cc0000",
            fg="white", font=("Segoe UI", 10, "bold"),
            relief=tk.FLAT, padx=12, pady=6, cursor="hand2")
        self._settings_yt_btn.pack(side=tk.LEFT)

        def _yt_toggle():
            if g.get("yt_state") == "connected":
                self._stop_yt_chat()
                self._settings_yt_btn.configure(
                    text="▶ YT CONNECT", bg="#cc0000")
            else:
                popup.destroy()
                self._start_yt_chat()
        self._settings_yt_btn.configure(command=_yt_toggle)

        # ── Save button ───────────────────────────────────────────────
        def _save():
            global CLIENT_ID, YT_API_KEY
            changed = False

            # Twitch Client ID — validate format (alphanumeric, ~30 chars)
            cid = e_client_id.get().strip()
            if cid and cid != CLIENT_ID:
                if len(cid) < 10:
                    messagebox.showwarning("Invalid Client ID",
                        "Twitch Client ID looks too short — please check it.")
                    return
                CLIENT_ID = cid
                _kr_set("client_id", cid)
                changed = True

            # OAuth token
            token = e_token.get().strip()
            if token and token != g["token"]:
                g["token"] = token
                self.token_box.delete(0, tk.END)
                self.token_box.insert(0, token)
                save_token(token)
                self._refresh_streamer_stats()
                self._stop_chat_irc()
                self.root.after(500, self._start_chat_irc)
                if not self._lurker_timer_id:
                    self._schedule_lurker_timer()
                changed = True

            # YouTube API Key — validate format (starts with AIza, 39 chars)
            yt = e_yt_key.get().strip()
            if yt and yt != YT_API_KEY:
                if not yt.startswith("AIza") or len(yt) < 30:
                    messagebox.showwarning("Invalid YouTube API Key",
                        "YouTube API Keys start with 'AIza' and are ~39 characters.\n"
                        "Please double-check your key.")
                    return
                YT_API_KEY = yt
                _kr_set("yt_api_key", yt)
                changed = True

            # AutoChannel
            global AUTO_CHANNEL, YT_CHANNEL_ID
            ac = e_auto_ch.get().strip().lower()
            if ac and ac != AUTO_CHANNEL:
                AUTO_CHANNEL = ac
                db_set("auto_channel", ac)
                # Update channel name label live
                if hasattr(self, "channel_name_label"):
                    self.channel_name_label.configure(text=ac.upper())
                # Reconnect IRC to the new channel immediately
                self._stop_chat_irc()
                self.root.after(500, self._start_chat_irc)
                changed = True

            # YT Channel ID
            ytc = e_yt_ch_id.get().strip()
            if ytc and ytc != YT_CHANNEL_ID:
                YT_CHANNEL_ID = ytc
                db_set("yt_channel_id", ytc)
                changed = True

            # Font sizes
            lf = var_lurker.get()
            cf = var_chat.get()
            of = var_output.get()
            if lf != g["lurker_font_size"]:
                g["lurker_font_size"] = lf
                changed = True
            if cf != g["chat_font_size"]:
                g["chat_font_size"] = cf
                self.chat_text.configure(font=("Segoe UI", cf))
                changed = True
            if of != g["output_font_size"]:
                g["output_font_size"] = of
                self.output.configure(font=("Segoe UI", of))
                changed = True
            if changed:
                save_font_sizes()

            # Mute list
            raw_mutes = mute_text.get("1.0", tk.END).strip()
            new_mutes = [m.strip() for m in raw_mutes.split(",") if m.strip()]
            db_set_mute_list(new_mutes)
            global MUTE_LIST
            MUTE_LIST = new_mutes

            # Save default send target and update button label live
            self._set_send_target(send_target_var.get())
            # Save channel name color
            global CHANNEL_NAME_COLOR
            new_color = color_var.get().strip()
            if len(new_color) == 7 and new_color.startswith("#"):
                CHANNEL_NAME_COLOR = new_color
                db_set("channel_name_color", new_color)
                if hasattr(self, "channel_name_label"):
                    self.channel_name_label.configure(fg=new_color)

            # Save opacity
            if hasattr(self, "_opacity_slider"):
                new_op = self._opacity_slider.get() / 100
                db_set("opacity", str(new_op))
                self.root.attributes("-alpha", new_op)

            popup.destroy()

            # Hot reload — apply changes immediately without waiting
            if lf != g.get("lurker_font_size", lf):
                pass  # already updated above
            # Trigger lurker refresh so new font size shows immediately
            self._do_lurker_refresh()

        # ── Backup / Restore row ─────────────────────────────────
        backup_row = tk.Frame(_scroll_frame, bg=BG_DARK)
        backup_row.pack(fill=tk.X, padx=20, pady=(0, 4))
        tk.Label(backup_row, text="Profile Backup:", fg=FG_CYAN, bg=BG_DARK,
                 font=("Segoe UI", 10, "bold")).pack(side=tk.LEFT, padx=(0, 10))
        def _do_backup():
            import backup_restore
            backup_restore.backup_profile(popup, DB_PATH)
        def _do_restore():
            import backup_restore
            backup_restore.restore_profile(popup, DB_PATH,
                on_restored=self._reload_after_restore)
        tk.Button(backup_row, text="📤  Export Backup",
                  bg="#4a4a6a", fg="white",
                  font=("Segoe UI", 10, "bold"), relief=tk.FLAT,
                  padx=10, pady=4, cursor="hand2",
                  command=_do_backup).pack(side=tk.LEFT, padx=(0, 8))
        tk.Button(backup_row, text="📥  Import Backup",
                  bg="#4a4a6a", fg="white",
                  font=("Segoe UI", 10, "bold"), relief=tk.FLAT,
                  padx=10, pady=4, cursor="hand2",
                  command=_do_restore).pack(side=tk.LEFT)

        btn_row = tk.Frame(_scroll_frame, bg=BG_DARK)
        btn_row.pack(pady=(4, 16))
        tk.Button(btn_row, text="💾  SAVE & APPLY",
                  bg="#1a6b1a", fg="white",
                  font=("Segoe UI", 12, "bold"), relief=tk.FLAT,
                  padx=16, pady=8, cursor="hand2",
                  command=_save).pack(side=tk.LEFT, padx=(0, 8))
        tk.Button(btn_row, text="❌  CLOSE",
                  bg="#cc0000", fg="white",
                  font=("Segoe UI", 12, "bold"), relief=tk.FLAT,
                  padx=16, pady=8, cursor="hand2",
                  command=popup.destroy).pack(side=tk.LEFT)

        e_token.focus()

    def _on_token_enter(self, e=None):
        token = self.token_box.get().strip()
        if token and token != g["token"]:
            g["token"] = token
            save_token(token)
            self._refresh_streamer_stats()
            self._stop_chat_irc()
            self.root.after(500, self._start_chat_irc)
            if not self._lurker_timer_id:
                self._schedule_lurker_timer()

    # ================================================================
    # ACTIONS
    # ================================================================
    def _open_auth(self):
        import webbrowser
        webbrowser.open(
            f"https://id.twitch.tv/oauth2/authorize?client_id={CLIENT_ID}"
            f"&redirect_uri=http://localhost&response_type=token"
            f"&scope=moderator:read:chatters+channel:read:subscriptions"
            f"+bits:read+moderator:read:followers+user:read:broadcast"
            f"+chat:edit+chat:read"
            f"+moderator:manage:chat_messages"    # delete individual messages
            f"+moderator:manage:banned_users"     # ban / timeout users
            f"+moderator:manage:chat_settings"    # slow mode / emote-only etc
            f"+channel:moderate"                  # legacy mod commands via IRC
        )

    def _open_profile(self):
        if g["current_user_url"]:
            import webbrowser
            webbrowser.open(g["current_user_url"])

    def _open_chat_edge(self):
        url = f"https://www.twitch.tv/popout/{AUTO_CHANNEL}/chat?darkpopout"
        try:
            g["chat_proc"] = subprocess.Popen(
                ["msedge", f"--app={url}"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except FileNotFoundError:
            import webbrowser
            webbrowser.open(url)

    def _on_quit(self):
        if not messagebox.askyesno("Quit", "Are you sure you want to quit?"):
            return
        log.info("Application shutting down...")
        # Signal all background threads to stop
        g["tw_stop_event"].set()
        g["yt_stop_event"].set()
        self._stop_chat_windows()
        self._stop_yt_chat()
        # Release pygame audio device so it does not stay locked after exit
        try:
            import pygame
            if pygame.mixer.get_init():
                pygame.mixer.quit()
        except Exception:
            pass
        log.info("Clean shutdown complete.")
        self.root.after(300, self.root.destroy)

    def _stop_chat_windows(self):
        if self._lurker_timer_id:
            self.root.after_cancel(self._lurker_timer_id)
            self._lurker_timer_id = None
        self._stop_chat_irc()
        # Kill Edge chat window if we launched it
        if g["chat_proc"]:
            try:
                g["chat_proc"].terminate()
            except Exception:
                pass
            g["chat_proc"] = None

    # ================================================================
    # OUTPUT
    # ================================================================
    def _append_output(self, label, value, color_tag):
        self.output.configure(state=tk.NORMAL)
        if label:
            self.output.insert(tk.END, label.upper() + " ", "gold")
        self.output.insert(tk.END, str(value).upper() + "\n", color_tag)
        self.output.configure(state=tk.DISABLED)
        self.output.see(tk.END)
        # Keep last 20 lines when over limit (matches PS1 behavior)
        lines = self.output.get("1.0", tk.END).splitlines()
        if len(lines) > 200:
            self.output.configure(state=tk.NORMAL)
            keep = "\n".join(lines[-20:])
            self.output.delete("1.0", tk.END)
            self.output.insert(tk.END, keep)
            self.output.configure(state=tk.DISABLED)

    # ================================================================
    # LURKER LABELS
    # ================================================================
    def _add_lurker_label(self, parent, name, time_str, is_live):
        color  = FG_LIME if is_live else FG_CYAN
        suffix = " [LIVE]" if is_live else ""
        lbl    = tk.Label(parent,
                          text=f"{name.upper()} ({time_str}){suffix}",
                          fg=color, bg="black",
                          font=("Consolas", g["lurker_font_size"], "bold"),
                          anchor="center", cursor="hand2")
        lbl.pack(fill=tk.X, pady=1, padx=2)
        lbl.bind("<Enter>",    lambda e: lbl.configure(bg="#1e1e32"))
        lbl.bind("<Leave>",    lambda e: lbl.configure(bg="black"))
        lbl.bind("<Button-1>", lambda e, n=name: self._lurker_click(n))

    def _lurker_click(self, name):
        self.user_box.delete(0, tk.END)
        self.user_box.insert(0, name)
        self.user_box.configure(fg="black")
        self._run_user_lookup()

    # ================================================================
    # PROFILE IMAGES  (canvas-based, no gear icon)
    # ================================================================
    def _load_owner_pic(self, url):
        def _t():
            # Load display photo
            photo = dl_image(url, (175, 175), crop=True)
            if photo:
                self.root.after(0, lambda: self._set_owner(photo))
            # Also store raw PIL image for soft glow generation
            try:
                r   = requests.get(url, timeout=10)
                img = Image.open(io.BytesIO(r.content)).convert("RGBA")
                bbox = img.getbbox()
                if bbox:
                    img = img.crop(bbox)
                self._owner_pil_img = img
            except Exception:
                self._owner_pil_img = None
        threading.Thread(target=_t, daemon=True).start()

    def _set_owner(self, photo):
        self._owner_photo      = photo
        self._owner_photo_orig = photo
        self.owner_canvas.itemconfig(self._owner_img, image=photo)

    def _load_search_pic(self, url):
        def _t():
            photo = dl_image(url, (80, 80), crop=True)
            if photo:
                self.root.after(0, lambda: self._set_search(photo))
            # Store raw PIL for bloom glow
            try:
                r   = requests.get(url, timeout=10)
                img = Image.open(io.BytesIO(r.content)).convert("RGBA")
                bbox = img.getbbox()
                if bbox:
                    img = img.crop(bbox)
                self._search_pil_img = img
            except Exception:
                self._search_pil_img = None
        threading.Thread(target=_t, daemon=True).start()

    def _set_search(self, photo):
        self._search_photo      = photo
        self._search_photo_orig = photo
        self.search_canvas.itemconfig(self._search_img, image=photo)

    # ================================================================
    # STREAMER STATS
    # ================================================================
    def _refresh_streamer_stats(self):
        def _fetch():
            data = invoke_twitch_api(
                f"https://api.twitch.tv/helix/users?login={AUTO_CHANNEL}")
            if not data or not data.get("data"):
                return
            d = data["data"][0]
            g["my_id"] = d["id"]
            self.root.after(0, lambda: self._load_owner_pic(d["profile_image_url"]))
            # Kick off emote load in background (runs independently of stream status)
            self.ui(self._load_emotes)

            # Account age
            try:
                created = datetime.datetime.fromisoformat(
                    d["created_at"].replace("Z", "+00:00"))
                diff    = datetime.datetime.now(datetime.timezone.utc) - created
                years   = diff.days // 365
                months  = (diff.days % 365) // 30
                age_str = f"{years}Y {months}M"
            except Exception:
                age_str = "—"

            followers = invoke_twitch_api(
                f"https://api.twitch.tv/helix/channels/followers"
                f"?broadcaster_id={d['id']}")
            stream = invoke_twitch_api(
                f"https://api.twitch.tv/helix/streams?user_id={d['id']}")
            subs = invoke_twitch_api(
                f"https://api.twitch.tv/helix/subscriptions"
                f"?broadcaster_id={d['id']}")
            channel = invoke_twitch_api(
                f"https://api.twitch.tv/helix/channels"
                f"?broadcaster_id={d['id']}")

            # ── YouTube channel stats ────────────────────────────────
            yt_subs    = None
            yt_viewers = None
            if YT_CHANNEL_ID and YT_API_KEY:
                try:
                    yt_ch = requests.get(
                        "https://www.googleapis.com/youtube/v3/channels",
                        params={"part": "statistics", "id": YT_CHANNEL_ID,
                                "key": YT_API_KEY}, timeout=8).json()
                    if yt_ch.get("items"):
                        stats_yt = yt_ch["items"][0]["statistics"]
                        yt_subs  = int(stats_yt.get("subscriberCount", 0))
                except Exception:
                    pass
                try:
                    yt_live = requests.get(
                        "https://www.googleapis.com/youtube/v3/search",
                        params={"part": "id", "channelId": YT_CHANNEL_ID,
                                "eventType": "live", "type": "video",
                                "key": YT_API_KEY}, timeout=8).json()
                    if yt_live.get("items"):
                        vid_id = yt_live["items"][0]["id"]["videoId"]
                        yt_vd  = requests.get(
                            "https://www.googleapis.com/youtube/v3/videos",
                            params={"part": "liveStreamingDetails",
                                    "id": vid_id, "key": YT_API_KEY},
                            timeout=8).json()
                        if yt_vd.get("items"):
                            lsd = yt_vd["items"][0].get("liveStreamingDetails", {})
                            yt_viewers = int(lsd.get("concurrentViewers", 0))
                except Exception:
                    pass

            def _update():
                if followers:
                    self.follow_label.configure(
                        text=_fmt(followers.get('total', 0)))
                if subs:
                    self.subs_label.configure(
                        text=_fmt(subs.get('total')) if isinstance(subs.get('total'), int) else "—")
                self.acct_age_label.configure(text=f"ACCOUNT AGE: {age_str}")
                if channel and channel.get("data"):
                    ch = channel["data"][0]
                    self.game_label.configure(
                        text=f"CATEGORY: {ch.get('game_name', '—') or '—'}")
                    title = ch.get("title", "—") or "—"
                    self.title_label.configure(text=f"TITLE: {title}")

                # ── Twitch live/offline ──────────────────────────────────────
                if stream and stream.get("data"):
                    self.live_dot.configure(text="LIVE", fg="#ff4444")
                    self.mic_icon.configure(fg="#ff4444")
                    self.viewer_count_label.configure(
                        text=_fmt(stream['data'][0]['viewer_count']))
                    # LIVE — show colored icon
                    tw_img = g.get("twitch_icon")
                    if hasattr(self, "tw_status_icon"):
                        if tw_img:
                            self.tw_status_icon.configure(image=tw_img)
                        else:
                            self.tw_status_icon.configure(text="TW", fg="#6441a5")
                else:
                    self.live_dot.configure(text="OFFLINE", fg=FG_LIME)
                    self.mic_icon.configure(fg=FG_LIME)
                    self.viewer_count_label.configure(text="0")
                    # OFFLINE — show gray icon
                    tw_gray = g.get("twitch_icon_gray")
                    if hasattr(self, "tw_status_icon"):
                        if tw_gray:
                            self.tw_status_icon.configure(image=tw_gray)
                        else:
                            self.tw_status_icon.configure(text="TW", fg="#555555")

                # Twitch icon in stat panel — replace text placeholder with favicon
                tw_large = g.get("twitch_icon_large")
                if tw_large and hasattr(self, "_tw_icon_canvas") and hasattr(self, "_tw_icon_item"):
                    twc = self._tw_icon_canvas
                    try: twc.delete(self._tw_icon_item)
                    except Exception: pass
                    h = twc.winfo_height() or 105
                    ix = 10; iy = 24 + (h - 24) // 2
                    self._tw_icon_item = twc.create_image(ix, iy, image=tw_large, anchor="w")
                    twc._tw_icon_ref = tw_large

                # ── YouTube live/offline ─────────────────────────────────────
                yt_img  = g.get("youtube_icon")
                yt_gray = g.get("youtube_icon_gray")

                if yt_subs is not None or yt_viewers is not None:
                    if hasattr(self, "yt_status_icon"):
                        if yt_viewers and yt_viewers > 0:
                            # LIVE — colored icon
                            if yt_img:
                                self.yt_status_icon.configure(image=yt_img)
                            else:
                                self.yt_status_icon.configure(text="▶", fg="#cc0000")
                            self.yt_live_label.configure(text="LIVE", fg="#ff4444")
                        else:
                            # Has data but offline — gray icon
                            if yt_gray:
                                self.yt_status_icon.configure(image=yt_gray)
                            else:
                                self.yt_status_icon.configure(text="▶", fg="#555555")
                            self.yt_live_label.configure(text="OFFLINE", fg=FG_LIME)
                    else:
                        if yt_viewers and yt_viewers > 0:
                            self.yt_live_label.configure(text="LIVE", fg="#ff4444")
                        else:
                            self.yt_live_label.configure(text="OFFLINE", fg=FG_LIME)
                else:
                    # No data yet — gray
                    if hasattr(self, "yt_status_icon"):
                        if yt_gray:
                            self.yt_status_icon.configure(image=yt_gray)
                        else:
                            self.yt_status_icon.configure(text="▶", fg="#555555")
                    self.yt_live_label.configure(text="OFFLINE", fg=FG_LIME)

                # YouTube icon in stat box is drawn on canvas at build time — no update needed

                # YouTube stat labels
                if yt_subs is not None:
                    self.yt_subs_label.configure(text=_fmt(yt_subs))
                if yt_viewers is not None:
                    self.yt_viewers_label.configure(text=_fmt(yt_viewers))
                elif yt_subs is not None:
                    self.yt_viewers_label.configure(text="0")
            self.root.after(0, _update)
        threading.Thread(target=_fetch, daemon=True).start()

    # ================================================================
    # LURKER REFRESH
    # ================================================================
    def _schedule_lurker_timer(self):
        self._lurker_timer_id = self.root.after(1000, self._run_lurker_refresh)

    def _run_lurker_refresh(self):
        if not g["my_id"] or not g["token"]:
            self._schedule_lurker_timer()
            return

        remaining = max(0, int(
            (g["next_refresh_time"] - datetime.datetime.now()).total_seconds()))
        self.refresh_label.configure(text=f"NEXT REFRESH IN: {remaining}S")

        if remaining <= 0 and not g["is_refreshing"]:
            g["is_refreshing"] = True
            self.refresh_label.configure(text="REFRESHING...")
            threading.Thread(target=self._do_lurker_refresh,
                             daemon=True).start()
        self._schedule_lurker_timer()

    def _do_lurker_refresh(self):
        try:
            self._refresh_streamer_stats()
            chatters = invoke_twitch_api(
                f"https://api.twitch.tv/helix/chat/chatters"
                f"?broadcaster_id={g['my_id']}&moderator_id={g['my_id']}")
            if not chatters or not chatters.get("data"):
                return

            all_ids = "&user_id=".join(c["user_id"] for c in chatters["data"])
            sc = invoke_twitch_api(
                f"https://api.twitch.tv/helix/streams?user_id={all_ids}")
            live_ids = {s["user_id"] for s in sc.get("data", [])} if sc else set()

            now = datetime.datetime.now()
            longest, recent = [], []

            for c in chatters["data"]:
                un, uid = c["user_name"], c["user_id"]
                is_live = uid in live_ids

                if un not in g["session_lurkers"]:
                    g["session_lurkers"][un] = {"joined": now, "is_live": is_live}
                    if un not in g["known_lurkers"]:
                        g["known_lurkers"][un] = True
                        log_lurker(un)
                        play_lurk_notify()
                else:
                    g["session_lurkers"][un]["is_live"] = is_live

                entry = {
                    "name":    un,
                    "joined":  g["session_lurkers"][un]["joined"],
                    "is_live": is_live,
                }
                if (now - entry["joined"]).total_seconds() >= 300:
                    longest.append(entry)
                else:
                    recent.append(entry)

            count = len(chatters["data"])
            ts    = now.strftime("%H:%M:%S")

            def _update_ui():
                self.lurker_title.configure(
                    text=f"--- LURKERS ({count}) - {ts} ---")
                for w in self.longest_box.winfo_children(): w.destroy()
                for w in self.recent_box.winfo_children():  w.destroy()
                for l in longest:
                    self._add_lurker_label(
                        self.longest_box, l["name"],
                        l["joined"].strftime("%H:%M"), l["is_live"])
                for r in recent:
                    self._add_lurker_label(
                        self.recent_box, r["name"],
                        r["joined"].strftime("%H:%M"), r["is_live"])

            self.ui(_update_ui)  # always execute on main thread via ui_queue
            g["next_refresh_time"] = (datetime.datetime.now() +
                                       datetime.timedelta(seconds=60))
        finally:
            g["is_refreshing"] = False

    # ================================================================
    # USER LOOKUP
    # ================================================================
    def _run_user_lookup(self):
        token = self.token_box.get().strip()
        if token:
            g["token"] = token
            save_token(token)
        search_user = self.user_box.get().strip()
        if not search_user or search_user == "SEARCH...":
            return
        self.output.configure(state=tk.NORMAL)
        self.output.delete("1.0", tk.END)
        self.output.configure(state=tk.DISABLED)

        def _fetch():
            data = invoke_twitch_api(
                f"https://api.twitch.tv/helix/users?login={search_user}")
            if not data or not data.get("data"):
                self.root.after(0,
                    lambda: self._append_output("", "USER NOT FOUND", "red"))
                return
            d = data["data"][0]
            g["current_user_url"] = f"https://www.twitch.tv/{d['login']}"
            self.root.after(0, lambda: self._load_search_pic(
                d["profile_image_url"]))
            self.root.after(0, lambda: self.search_name_label.configure(
                text=d["display_name"].upper()))
            self.root.after(0, lambda: None)  # profile opens via canvas click

            stream = invoke_twitch_api(
                f"https://api.twitch.tv/helix/streams?user_id={d['id']}")
            if stream and stream.get("data"):
                s = stream["data"][0]
                self.root.after(0, lambda: self._append_output(
                    "STATUS:", "🔴 LIVE", "lime"))
                self.root.after(0, lambda: self._append_output(
                    "VIEWERS:", str(s["viewer_count"]), "white"))
                self.root.after(0, lambda: self._append_output(
                    "GAME:", s["game_name"], "cyan"))
                self.root.after(0, lambda: self._append_output(
                    "TITLE:", s["title"], "lightgray"))
            else:
                self.root.after(0, lambda: self._append_output(
                    "STATUS:", "OFFLINE", "gray"))

            followers = invoke_twitch_api(
                f"https://api.twitch.tv/helix/channels/followers"
                f"?broadcaster_id={d['id']}")
            if followers:
                self.root.after(0, lambda: self._append_output(
                    "FOLLOWERS:", f"{followers.get('total', 0):,}", "white"))

            created = datetime.datetime.fromisoformat(
                d["created_at"].replace("Z", "+00:00"))
            diff    = datetime.datetime.now(datetime.timezone.utc) - created
            age_str = (f"{diff.days // 365} YEARS, "
                       f"{(diff.days % 365) // 30} MONTHS")
            self.root.after(0, lambda: self._append_output(
                "ACCOUNT AGE:", age_str, "silver"))

            # Viewer's own channel stats
            viewer_followers = invoke_twitch_api(
                f"https://api.twitch.tv/helix/channels/followers"
                f"?broadcaster_id={d['id']}")
            if viewer_followers:
                self.root.after(0, lambda vf=viewer_followers: self._append_output(
                    "THEIR FOLLOWERS:", f"{vf.get('total', 0):,}", "white"))

            if g["my_id"]:
                follow = invoke_twitch_api(
                    f"https://api.twitch.tv/helix/channels/followers"
                    f"?broadcaster_id={g['my_id']}&user_id={d['id']}")
                if follow and follow.get("data"):
                    raw = follow["data"][0].get("followed_at", "")
                    try:
                        fdate = datetime.datetime.fromisoformat(
                            raw.replace("Z", "+00:00")).strftime("%m/%d/%Y")
                    except Exception:
                        fdate = "UNKNOWN"
                    self.root.after(0, lambda: self._append_output(
                        "FOLLOW STATUS:",
                        f"FOLLOWING YOU SINCE {fdate}", "lime"))
                else:
                    self.root.after(0, lambda: self._append_output(
                        "FOLLOW STATUS:", "NOT FOLLOWING YOU", "red"))

                sub = invoke_twitch_api(
                    f"https://api.twitch.tv/helix/subscriptions"
                    f"?broadcaster_id={g['my_id']}&user_id={d['id']}")
                if sub and sub.get("data"):
                    tier = int(sub["data"][0].get("tier", 1000)) // 1000
                    self.root.after(0, lambda: self._append_output(
                        "SUB STATUS:",
                        f"SUBSCRIBED (TIER {tier})", "skyblue"))
                else:
                    self.root.after(0, lambda: self._append_output(
                        "SUB STATUS:", "NOT SUBSCRIBED", "red"))

        threading.Thread(target=_fetch, daemon=True).start()

    # ================================================================
    # CHAT IRC  — with exponential backoff, socket timeout, state UI
    # ================================================================
    def _set_tw_state(self, state):
        """Update Twitch connection state — always via ui_queue (thread-safe)."""
        g["tw_state"] = state
        colors = {
            "connected":    (f"● CONNECTED AS {(g['chat_username'] or '').upper()}", FG_LIME),
            "connecting":   ("● CONNECTING...",             "#ffd700"),   # yellow
            "reconnecting": ("● RECONNECTING — MINOR HICCUP...", "#ff8c00"),  # orange
            "failed":       ("● CONNECTION FAILED",         "#ff4444"),   # red
            "disconnected": ("● DISCONNECTED",              FG_GRAY),
        }
        text, color = colors.get(state, ("● UNKNOWN", FG_GRAY))
        if hasattr(self, "chat_status_label"):
            _t, _c = text, color
            self.ui(lambda t=_t, c=_c: self.chat_status_label.configure(text=t, fg=c))
        if hasattr(self, "tw_dot"):
            dot_color = {"connected": FG_LIME, "connecting": "#ffd700",
                         "reconnecting": "#ff8c00", "failed": "#ff4444",
                         "disconnected": FG_GRAY}.get(state, FG_GRAY)
            use_color = state == "connected"
            def _upd_tw(c=dot_color, col=use_color):
                self.tw_dot.configure(fg=c)
                if hasattr(self, "tw_dot_img"):
                    icon = g.get("twitch_icon") if col else g.get("twitch_icon_gray")
                    if icon:
                        self.tw_dot_img.configure(image=icon)
                        self.tw_dot_img.image = icon
            self.ui(_upd_tw)
        log.info(f"Twitch state → {state}")

    def _start_chat_irc(self):
        if g["tw_state"] in ("connected", "connecting", "reconnecting"):
            return
        g["tw_stop_event"].clear()
        g["tw_backoff"] = 2
        threading.Thread(target=self._irc_connect_loop, daemon=True).start()

    def _irc_connect_loop(self):
        """Persistent connect loop with exponential backoff."""
        token = self.token_box.get().strip()
        g["token"] = token
        if not token:
            self.root.after(0, lambda: self.chat_status_label.configure(
                text="● NO TOKEN - CLICK LOGO TO AUTHORIZE", fg="red"))
            return

        # Validate token health before connecting
        if not validate_token(token):
            self._set_tw_state("failed")
            return

        token_clean = re.sub(r"^oauth:", "", token)

        while not g["tw_stop_event"].is_set():
            self._set_tw_state("connecting" if g["tw_backoff"] == 2 else "reconnecting")
            try:
                ui = invoke_twitch_api("https://api.twitch.tv/helix/users")
                if not ui or not ui.get("data"):
                    self._set_tw_state("failed")
                    return
                g["chat_username"] = ui["data"][0]["login"]

                context  = ssl.create_default_context()
                raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                raw_sock.settimeout(10.0)   # hard timeout — no ghost sockets
                sock = context.wrap_socket(raw_sock,
                                           server_hostname="irc.chat.twitch.tv")
                sock.connect(("irc.chat.twitch.tv", 6697))  # SSL port
                sock.settimeout(30.0)   # longer read timeout once connected
                g["chat_socket"] = sock
                g["tw_backoff"]  = 2   # reset backoff on success

                def send(m):
                    sock.sendall((m + "\r\n").encode("utf-8"))

                send(f"PASS oauth:{token_clean}")
                send(f"NICK {g['chat_username']}")
                time.sleep(0.3)
                send("CAP REQ :twitch.tv/tags twitch.tv/commands twitch.tv/membership")
                send(f"JOIN #{AUTO_CHANNEL}")
                g["irc_sock"] = sock  # store ref for mod commands

                self._set_tw_state("connected")
                # buttons now in settings popup — dot updated via _set_tw_state
                log.info(f"IRC connected as {g['chat_username']}")

                # Read loop
                buf = ""
                while not g["tw_stop_event"].is_set():
                    try:
                        data = sock.recv(4096).decode("utf-8", errors="replace")
                        if not data:
                            log.warning("IRC: empty recv — connection dropped")
                            break
                        buf += data
                        while "\r\n" in buf:
                            line, buf = buf.split("\r\n", 1)
                            self.root.after(0, lambda l=line:
                                self._process_irc_line(l))
                    except socket.timeout:
                        # Send PING to keep alive
                        try:
                            sock.sendall(b"PING :tmi.twitch.tv\r\n")
                        except Exception:
                            break
                    except Exception as ex:
                        # Suppress expected socket errors on clean shutdown
                        if not g["tw_stop_event"].is_set():
                            log.warning(f"IRC read error: {ex}")
                        break

            except Exception as ex:
                log.warning(f"IRC connect error: {ex}")

            finally:
                try:
                    if g["chat_socket"]:
                        g["chat_socket"].close()
                except Exception:
                    pass
                g["chat_socket"] = None

            if g["tw_stop_event"].is_set():
                break

            # Exponential backoff — cap at 60s
            backoff = g["tw_backoff"]
            log.info(f"IRC reconnecting in {backoff}s...")
            self._set_tw_state("reconnecting")
            self.ui(lambda b=backoff: self.chat_status_label.configure(
                text=f"● RECONNECTING IN {b}s...", fg="#ffd700"))
            g["tw_stop_event"].wait(backoff)
            g["tw_backoff"] = min(backoff * 2, 60)

        self._set_tw_state("disconnected")
        # buttons now in settings popup — dot updated via _set_tw_state

    def _process_irc_line(self, line):
        if self.debug_var.get() and line:
            self.chat_status_label.configure(
                text=f"● DEBUG: {line[:60]}...", fg=FG_WHITE)

        if line.startswith("PING"):
            try:
                g["chat_socket"].sendall(b"PONG :tmi.twitch.tv\r\n")
            except Exception:
                pass
            return

        if "NOTICE" in line and (
                "authentication failed" in line or
                "Login unsuccessful" in line):
            self.chat_status_label.configure(
                text="● AUTH FAILED - RE-AUTHORIZE NEEDED", fg="red")
            return

        if ":tmi.twitch.tv 001" in line:
            self.chat_status_label.configure(
                text=f"● CONNECTED AS {(g['chat_username'] or '').upper()}",
                fg=FG_LIME)

        if f"PRIVMSG #{AUTO_CHANNEL} :" in line:
            self._handle_privmsg(line)

    def _handle_privmsg(self, line):
        now        = datetime.datetime.now()
        time_fmt   = now.strftime("%I:%M %p")
        emote_data = user = msg = ""

        if line.startswith("@"):
            parts    = line.split(" ", 2)
            tag_dict = {}
            for t in parts[0].lstrip("@").split(";"):
                if "=" in t:
                    k, v = t.split("=", 1)
                    tag_dict[k] = v
            emote_data = tag_dict.get("emotes", "")
            user       = tag_dict.get("display-name", "")
            msg_id     = tag_dict.get("id", "")
            # Store message ID per user (keep last 20)
            if msg_id and user:
                store = g["msg_id_store"].setdefault(user.lower(), [])
                store.append(msg_id)
                if len(store) > 20:
                    store.pop(0)
            if not user and len(parts) > 1:
                user = parts[1].split("!")[0].lstrip(":")
            idx = line.find(" :", len(parts[0]) + len(parts[1]))
            if idx >= 0:
                msg = line[idx + 2:]
        else:
            m = re.match(
                r":(\w+)!\w+@\w+\.tmi\.twitch\.tv PRIVMSG #\w+ :(.*)", line)
            if m:
                user, msg = m.group(1), m.group(2)

        if not user or not msg:
            return

        # Sound notification with cooldown
        if self.sound_var.get():
            elapsed = (datetime.datetime.now() -
                       g["last_sound_time"]).total_seconds()
            if (user not in MUTE_LIST and
                    elapsed >= g["sound_cooldown_seconds"]):
                play_notify()
                g["last_sound_time"] = datetime.datetime.now()

        # Parse emote positions
        emote_map = {}
        if emote_data:
            for entry in emote_data.split("/"):
                if ":" in entry:
                    eid, ranges = entry.split(":", 1)
                    rng = ranges.split(",")[0].split("-")
                    try:
                        s = int(rng[0])
                        e2 = int(rng[1]) + 1
                        if s >= 0 and e2 <= len(msg):
                            emote_map[msg[s:e2]] = eid
                    except Exception:
                        pass

        # Build log message
        log_parts = []
        for word in msg.split(" "):
            if not word:
                continue
            clean = word.strip()
            if clean in emote_map:
                log_parts.append(f"[EMOJI:{clean}]")
            else:
                log_parts.append(word)
        log_message = " ".join(log_parts)

        is_own = g["chat_username"] and user.lower() == g["chat_username"].lower()

        # Find any emotes not yet in cache
        uncached = {eid for eid in emote_map.values()
                    if eid not in g["emote_image_cache"]}

        if uncached:
            # Download ALL missing emotes first, then display the message
            def _fetch_then_show():
                for eid in uncached:
                    get_emote_image(eid)   # blocks until downloaded & cached
                # Now back on main thread with all images ready — no flicker
                self.ui(lambda: self._append_chat_message(
                    time_fmt, user, msg, emote_map, own=is_own))
                if self.log_var.get():
                    self.ui(lambda: log_chat(
                        f"[{time_fmt}] TWITCH: {user}: {log_message}"))
            threading.Thread(target=_fetch_then_show, daemon=True).start()
        else:
            # All emotes already cached — display instantly
            self._append_chat_message(time_fmt, user, msg, emote_map, own=is_own)
            if self.log_var.get():
                log_chat(f"[{time_fmt}] TWITCH: {user}: {log_message}")

    def _append_chat_message(self, time_str, user, msg, emote_map=None,
                              own=False, platform="twitch"):
        if emote_map is None:
            emote_map = {}

        self.chat_text.configure(state=tk.NORMAL)

        # Platform icon — tiny 16x16 inline before timestamp
        icon = g["twitch_icon"] if platform == "twitch" else g["youtube_icon"]
        if icon:
            icon_lbl = tk.Label(self.chat_text, image=icon,
                                bg=BG_CHAT, bd=0, cursor="arrow")
            icon_lbl.image = icon
            self.chat_text.window_create(tk.END, window=icon_lbl, padx=2, pady=2)
        else:
            # Fallback text if icon not loaded yet
            dot_color = FG_PURPLE if platform == "twitch" else "#ff0000"
            dot_tag = f"dot_{platform}_{id(time_str)}"
            self.chat_text.tag_configure(dot_tag, foreground=dot_color,
                font=("Segoe UI", g["chat_font_size"], "bold"))
            self.chat_text.insert(tk.END, "● ", dot_tag)

        self.chat_text.insert(tk.END, f"{time_str} ", "timestamp")

        # Username — clicking inserts @mention
        # Use shared tags so font size updates propagate instantly
        import hashlib
        if own:
            username_tag = "own_username"
        elif platform == "twitch":
            username_tag = "username"
        else:
            yt_colors = ["#ff6b6b", "#ff9f43", "#ffd700", "#7bed9f", "#70a1ff"]
            yt_idx = int(hashlib.md5(user.encode()).hexdigest(), 16) % len(yt_colors)
            username_tag = f"yt_username_{yt_idx}"
            self.chat_text.tag_configure(
                username_tag,
                foreground=yt_colors[yt_idx],
                font=("Segoe UI", g["chat_font_size"], "bold"))
        self.chat_text.insert(tk.END, f"{user}: ", username_tag)

        # Bind click on this tag to insert @mention
        def _click_user(e, u=user):
            current = self.chat_message_box.get()
            if not current.strip():
                self.chat_message_box.delete(0, tk.END)
                self.chat_message_box.insert(0, f"@{u} ")
            else:
                self.chat_message_box.insert(tk.END, f" @{u} ")
            self.chat_message_box.focus()

        # Thin click-target tag overlaid on top for this specific message
        click_tag = f"click_{id(time_str)}"
        self.chat_text.tag_configure(click_tag)
        # Re-tag the just-inserted username range with click_tag
        end_idx = self.chat_text.index(tk.END)
        ulen = len(user) + 2  # "user: "
        line_start = f"{end_idx} - {ulen} chars"
        self.chat_text.tag_add(click_tag, line_start, end_idx)
        self.chat_text.tag_bind(click_tag, "<Button-1>", _click_user)

        # Right-click username → full moderator context menu
        def _right_click_user(e, u=user, plat=platform):
            menu = tk.Menu(self.root, tearoff=0, bg=BG_PANEL2, fg=FG_WHITE,
                           font=("Segoe UI", 10), relief=tk.FLAT,
                           activebackground="#2a2a3e", activeforeground=FG_CYAN)

            # Header
            menu.add_command(label=f"  👤  {u}",
                             state=tk.DISABLED, font=("Segoe UI", 10, "bold"))
            menu.add_separator()

            # Always available
            menu.add_command(label="  🔍  View Profile",
                             command=lambda un=u: self._ctx_view_profile(un))
            menu.add_command(label="  @   Mention in chat",
                             command=lambda un=u: self._ctx_mention(un))
            menu.add_command(label="  📋  Copy username",
                             command=lambda un=u: self._ctx_copy(un))
            menu.add_separator()

            # Mute / Unmute
            already_muted = u in MUTE_LIST
            if already_muted:
                menu.add_command(label=f"  🔊  Unmute {u}",
                                 command=lambda un=u: self._unmute_user(un))
            else:
                menu.add_command(label=f"  🔇  Mute {u}  (hide in this app)",
                                 command=lambda un=u: self._mute_user(un))

            # Twitch-only mod actions
            if plat == "twitch":
                menu.add_separator()
                menu.add_command(label="  📣  Shoutout  !so",
                                 command=lambda un=u: self._ctx_shoutout(un))
                menu.add_separator()
                # Timeout submenu
                timeout_menu = tk.Menu(menu, tearoff=0, bg=BG_PANEL2,
                                       fg=FG_WHITE, font=("Segoe UI", 10),
                                       activebackground="#2a2a3e",
                                       activeforeground=FG_GOLD)
                for lbl, secs in [("1 minute",60),("5 minutes",300),
                                   ("10 minutes",600),("1 hour",3600),
                                   ("24 hours",86400)]:
                    timeout_menu.add_command(
                        label=f"  ⏱  {lbl}",
                        command=lambda un=u, s=secs: self._ctx_timeout(un, s))
                menu.add_cascade(label="  ⏱  Timeout", menu=timeout_menu)
                menu.add_command(label="  🔨  Ban user",
                                 foreground="#ff6666",
                                 command=lambda un=u: self._ctx_ban(un))
                menu.add_command(label="  🧹  Clear their messages",
                                 command=lambda un=u: self._ctx_clear_messages(un))
            try:
                menu.tk_popup(e.x_root, e.y_root)
            finally:
                menu.grab_release()

        self.chat_text.tag_bind(click_tag, "<Button-3>", _right_click_user)


        # Clamp incoming message length — protects against walls of text/spam
        # Twitch's own cap is 500 chars; YouTube allows up to 200
        if len(msg) > 500:
            msg = msg[:497] + "…"

        # Message body — use cached emote image if available, else show name in gold
        words = msg.split(" ")
        for i, word in enumerate(words):
            clean = word.strip()
            if clean in emote_map:
                eid   = emote_map[clean]
                photo = g["emote_image_cache"].get(eid)
                if photo:
                    lbl = tk.Label(self.chat_text, image=photo,
                                   bg=BG_CHAT, bd=0)
                    lbl.image = photo
                    self.chat_text.window_create(tk.END, window=lbl, padx=2, pady=2)
                else:
                    # Not cached yet — show emote name styled in gold
                    self.chat_text.insert(tk.END, f":{clean}: ", "emote_name")
            else:
                sep = " " if i < len(words) - 1 else ""
                self.chat_text.insert(tk.END, word + sep, "message")

        self.chat_text.insert(tk.END, "\n")
        self.chat_text.configure(state=tk.DISABLED)
        self.chat_text.see(tk.END)

        g["chat_message_count"] += 1
        if g["chat_message_count"] > g["max_chat_messages"]:
            self.chat_text.configure(state=tk.NORMAL)
            self.chat_text.delete("1.0", "101.0")
            self.chat_text.configure(state=tk.DISABLED)
            g["chat_message_count"] -= 100

    def _mute_user(self, username):
        """Add user to mute list instantly and persist to DB."""
        global MUTE_LIST
        if username not in MUTE_LIST:
            MUTE_LIST.append(username)
            db_set_mute_list(MUTE_LIST)
            self._append_chat_message(
                datetime.datetime.now().strftime("%I:%M %p"),
                "SYSTEM", f"🔇 {username} muted and added to mute list.",
                platform="twitch")
            log.info(f"Muted user: {username}")

    def _unmute_user(self, username):
        """Remove user from mute list and persist to DB."""
        global MUTE_LIST
        if username in MUTE_LIST:
            MUTE_LIST.remove(username)
            db_set_mute_list(MUTE_LIST)
            self._append_chat_message(
                datetime.datetime.now().strftime("%I:%M %p"),
                "SYSTEM", f"🔊 {username} unmuted.",
                platform="twitch")
            log.info(f"Unmuted user: {username}")

    # ================================================================
    # CONTEXT MENU ACTIONS
    # ================================================================

    def _ctx_mention(self, username):
        """Insert @mention into chat box."""
        box = self.chat_message_box
        current = box.get().strip()
        box.delete(0, tk.END)
        box.insert(0, f"{current} @{username} ".lstrip())
        box.focus()
        box.icursor(tk.END)

    def _ctx_copy(self, username):
        """Copy username to clipboard."""
        self.root.clipboard_clear()
        self.root.clipboard_append(username)
        self._append_chat_message(
            datetime.datetime.now().strftime("%I:%M %p"),
            "SYSTEM", f"📋 Copied '{username}' to clipboard.",
            platform="twitch")

    def _ctx_view_profile(self, username):
        """Open Twitch profile in browser."""
        import webbrowser
        webbrowser.open(f"https://www.twitch.tv/{username}")

    def _ctx_shoutout(self, username):
        """Send !so @username to Twitch chat."""
        self._send_irc_command(f"!so @{username}")
        self._append_chat_message(
            datetime.datetime.now().strftime("%I:%M %p"),
            "SYSTEM", f"📣 Shoutout sent: !so @{username}",
            platform="twitch")

    def _ctx_timeout(self, username, seconds):
        """Timeout a user for N seconds via IRC /timeout."""
        mins = seconds // 60
        label = f"{mins} minute(s)" if mins < 60 else f"{mins//60} hour(s)"
        if not messagebox.askyesno(
                "Confirm Timeout",
                f"Timeout {username} for {label}?",
                parent=self.root):
            return
        self._send_irc_command(f"/timeout {username} {seconds}")
        self._append_chat_message(
            datetime.datetime.now().strftime("%I:%M %p"),
            "SYSTEM", f"⏱ {username} timed out for {label}.",
            platform="twitch")

    def _ctx_ban(self, username):
        """Permanently ban a user via IRC /ban."""
        if not messagebox.askyesno(
                "Confirm Ban",
                f"⚠️ Permanently ban {username}?\n\nThis cannot be undone from this app.",
                icon="warning", parent=self.root):
            return
        self._send_irc_command(f"/ban {username}")
        self._mute_user(username)  # also hide locally
        self._append_chat_message(
            datetime.datetime.now().strftime("%I:%M %p"),
            "SYSTEM", f"🔨 {username} banned.",
            platform="twitch")

    def _ctx_clear_messages(self, username):
        """Delete user messages locally + via Twitch Helix API."""
        # ── Local wipe ───────────────────────────────────────────────
        try:
            self.chat_text.configure(state=tk.NORMAL)
            total_lines = int(self.chat_text.index(tk.END).split(".")[0])
            lines_to_delete = []
            i = 1
            while i <= total_lines:
                line_text = self.chat_text.get(f"{i}.0", f"{i}.end")
                if username.lower() in line_text.lower():
                    lines_to_delete.append(i)
                i += 1
            for ln in reversed(lines_to_delete):
                self.chat_text.delete(f"{ln}.0", f"{ln+1}.0")
            self.chat_text.configure(state=tk.DISABLED)
        except Exception as ex:
            log.warning(f"Local clear failed: {ex}")
            lines_to_delete = []

        # ── Twitch API delete — runs in background thread ────────────
        def _api_delete():
            deleted = 0
            failed  = 0
            try:
                broadcaster_id = g.get("my_id")
                token          = g.get("token", "").strip()
                msg_ids        = list(g["msg_id_store"].get(username.lower(), []))
                if not broadcaster_id or not token or not msg_ids:
                    # Fallback: 1-sec timeout wipes messages for all viewers
                    self._send_irc_command(f"/timeout {username} 1")
                    self.ui(lambda r=len(lines_to_delete): self._append_chat_message(
                        datetime.datetime.now().strftime("%I:%M %p"), "SYSTEM",
                        f"🧹 Cleared {r} local message(s) from {username} "
                        f"(used timeout fallback to wipe from Twitch).",
                        platform="twitch"))
                    return
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Client-Id":     CLIENT_ID,
                    "Content-Type":  "application/json",
                }
                for mid in msg_ids:
                    try:
                        url = (
                            f"https://api.twitch.tv/helix/moderation/chat"
                            f"?broadcaster_id={broadcaster_id}"
                            f"&moderator_id={broadcaster_id}"
                            f"&message_id={mid}"
                        )
                        r = requests.delete(url, headers=headers, timeout=5)
                        if r.status_code in (204, 200):
                            deleted += 1
                        else:
                            failed += 1
                    except Exception:
                        failed += 1
                # Clear stored IDs for this user
                g["msg_id_store"].pop(username.lower(), None)
            except Exception as ex:
                log.warning(f"API delete failed: {ex}")
                failed = 1

            local = len(lines_to_delete)
            if deleted > 0:
                status = f"🧹 Deleted {deleted} message(s) from {username} on Twitch + cleared {local} locally."
            elif failed > 0:
                # API failed — fall back to timeout wipe
                self._send_irc_command(f"/timeout {username} 1")
                status = f"🧹 Cleared {local} local message(s) from {username} (timeout used to wipe Twitch chat)."
            else:
                status = f"🧹 Cleared {local} local message(s) from {username} (no recent messages to delete on Twitch)."
            self.ui(lambda s=status: self._append_chat_message(
                datetime.datetime.now().strftime("%I:%M %p"),
                "SYSTEM", s, platform="twitch"))

        threading.Thread(target=_api_delete, daemon=True).start()

    def _send_irc_command(self, command: str):
        """Send a raw IRC message/command to the current channel."""
        try:
            sock = g.get("irc_sock")
            if sock:
                msg = f"PRIVMSG #{AUTO_CHANNEL} :{command}\r\n"
                sock.sendall(msg.encode("utf-8"))
            else:
                self._append_chat_message(
                    datetime.datetime.now().strftime("%I:%M %p"),
                    "SYSTEM", "⚠️ Not connected to Twitch chat.",
                    platform="twitch")
        except Exception as ex:
            log.warning(f"IRC command failed: {ex}")

    # ================================================================
    # SEND TARGET HELPERS
    # ================================================================
    def _send_btn_label(self):
        t = SEND_TARGET
        labels = {"twitch": "SEND", "youtube": "SEND", "all": "SEND"}
        return labels.get(t, "SEND")

    def _send_btn_icon(self):
        """Return the PhotoImage for the current send target, or None."""
        t = SEND_TARGET
        if t == "twitch":
            return g.get("twitch_icon")
        if t == "youtube":
            return g.get("youtube_icon")
        # "all" — build overlapping TW+YT badge icon on demand
        if not g.get("all_icon"):
            try:
                import tempfile
                tw_path = os.path.join(tempfile.gettempdir(), "tw_icon.ico")
                yt_path = os.path.join(tempfile.gettempdir(), "yt_icon.ico")
                # TW large on left, YT smaller overlapping bottom-right
                tw_img = Image.open(tw_path).convert("RGBA").resize((22, 22), Image.LANCZOS)
                yt_img = Image.open(yt_path).convert("RGBA").resize((16, 16), Image.LANCZOS)
                canvas = Image.new("RGBA", (28, 22), (0, 0, 0, 0))
                canvas.paste(tw_img, (0, 0), tw_img)
                canvas.paste(yt_img, (11, 6), yt_img)
                g["all_icon"] = ImageTk.PhotoImage(canvas)
            except Exception:
                pass
        return g.get("all_icon") or g.get("twitch_icon")

    def _show_send_menu(self):
        """Custom Toplevel popup with real favicons next to each option."""
        popup = tk.Toplevel(self.root)
        popup.overrideredirect(True)   # no title bar
        popup.configure(bg="#1a1a1f")
        popup.attributes("-topmost", True)

        options = [
            ("twitch",  g.get("twitch_icon"),  "TWITCH only",   "#bf94ff", "#6441a5"),
            ("youtube", g.get("youtube_icon"), "YOUTUBE only",  "#ff6666", "#cc0000"),
            ("all",     g.get("all_icon"),     "ALL platforms", "#1e90ff", "#1e90ff"),
        ]

        def _pick(target, p=popup):
            p.destroy()
            self._set_send_target(target)

        for target, icon, label, fg_col, hover_col in options:
            row = tk.Frame(popup, bg="#1a1a1f", cursor="hand2")
            row.pack(fill=tk.X, padx=1, pady=1)
            if icon:
                img_lbl = tk.Label(row, image=icon, bg="#1a1a1f",
                                   cursor="hand2")
                img_lbl.image = icon
                img_lbl.pack(side=tk.LEFT, padx=(14, 6), pady=10)
            else:
                tk.Label(row, text="●", fg=fg_col, bg="#1a1a1f",
                         font=("Segoe UI", 14, "bold"),
                         cursor="hand2").pack(side=tk.LEFT, padx=(14, 6), pady=10)
            txt_lbl = tk.Label(row, text=label, fg=fg_col, bg="#1a1a1f",
                               font=("Segoe UI", 13, "bold"),
                               cursor="hand2", padx=8)
            txt_lbl.pack(side=tk.LEFT, pady=10, padx=(0, 20))
            # Hover highlight + click for entire row and children
            for w in (row, txt_lbl) + ((img_lbl,) if icon else ()):
                w.bind("<Enter>",  lambda e, r=row, c=hover_col:
                    r.configure(bg=c) or [ch.configure(bg=c)
                    for ch in r.winfo_children()])
                w.bind("<Leave>",  lambda e, r=row:
                    r.configure(bg="#1a1a1f") or [ch.configure(bg="#1a1a1f")
                    for ch in r.winfo_children()])
                w.bind("<Button-1>", lambda e, t=target: _pick(t))
            # Separator line between items
            if target != "all":
                tk.Frame(popup, bg="#2d2d3a", height=1).pack(fill=tk.X)

        # Right-align popup with arrow button, without primary screen clamps
        popup.update_idletasks()
        pw = popup.winfo_reqwidth()
        ph = popup.winfo_reqheight()
        ax_right = (self.chat_send_arrow.winfo_rootx()
                    + self.chat_send_arrow.winfo_width())
        ay = self.chat_send_arrow.winfo_rooty()
        
        x = ax_right - pw
        y = ay - ph - 4
        popup.geometry(f"+{x}+{y}")

        # Close if focus leaves the popup entirely (not just to a child widget)
        def _maybe_close(e):
            focused = popup.focus_get()
            if focused is None or str(focused) == ".":
                try:
                    popup.destroy()
                except Exception:
                    pass
        popup.bind("<FocusOut>", _maybe_close)
        popup.focus_set()

    # Colors per send target
    _SEND_COLORS = {
        "twitch":  ("#6441a5", "#4e3080"),  # purple main, darker arrow
        "youtube": ("#cc0000", "#991000"),  # red main, darker arrow
        "all":     ("#1e90ff", "#1670cc"),  # blue main, darker arrow
    }

    def _set_send_target(self, target):
        global SEND_TARGET
        SEND_TARGET = target
        db_set("send_target", target)
        main_c, arrow_c = self._SEND_COLORS.get(target, ("#1e90ff", "#1670cc"))
        icon = self._send_btn_icon()
        self.chat_send_btn.configure(
            text=self._send_btn_label(), bg=main_c,
            image=icon if icon else "",
            compound=tk.LEFT if icon else tk.NONE)
        if icon:
            self.chat_send_btn.image = icon  # prevent GC
        self.chat_send_arrow.configure(bg=main_c)
        # Frame bg must match so there is no gap between the two halves
        self.chat_send_btn.master.configure(bg=main_c)
        # Re-bind hover so it brightens from the correct base color, not a stale one
        import colorsys
        def _lighten(hex_c, amount=0.15):
            r, g2, b = int(hex_c[1:3],16)/255, int(hex_c[3:5],16)/255, int(hex_c[5:7],16)/255
            h, s, v = colorsys.rgb_to_hsv(r, g2, b)
            v2 = min(1.0, v + amount)
            r2, g3, b2 = colorsys.hsv_to_rgb(h, s, v2)
            return "#{:02x}{:02x}{:02x}".format(int(r2*255), int(g3*255), int(b2*255))
        main_hover  = _lighten(main_c)
        arrow_hover = _lighten(main_c)
        for ev, col in (("<Enter>", main_hover), ("<Leave>", main_c)):
            self.chat_send_btn.bind(ev, lambda e, c=col: self.chat_send_btn.configure(bg=c))
        for ev, col in (("<Enter>", main_hover), ("<Leave>", main_c)):
            self.chat_send_arrow.bind(ev, lambda e, c=col: self.chat_send_arrow.configure(bg=c))
        log.info(f"Send target set to: {target}")

    def _reload_after_restore(self):
        """Hot-reload credentials after a backup restore — no restart needed."""
        global CLIENT_ID, YT_API_KEY, AUTO_CHANNEL, YT_CHANNEL_ID, CHANNEL_NAME_COLOR
        try:
            # Reload DB settings
            AUTO_CHANNEL       = db_get("auto_channel",       "")
            YT_CHANNEL_ID      = db_get("yt_channel_id",      "")
            CHANNEL_NAME_COLOR = db_get("channel_name_color", "#00e5ff")

            # Reload keyring credentials
            CLIENT_ID, YT_API_KEY = _load_credentials()
            token = load_token()
            if token:
                g["token"] = token
                if hasattr(self, "token_box"):
                    self.token_box.delete(0, tk.END)
                    self.token_box.insert(0, token)

            # Update live UI
            if hasattr(self, "channel_name_label") and AUTO_CHANNEL:
                self.channel_name_label.configure(
                    text=AUTO_CHANNEL.upper(), fg=CHANNEL_NAME_COLOR)

            # Reconnect IRC with restored token
            self._stop_chat_irc()
            self.root.after(800, self._start_chat_irc)
            self._refresh_streamer_stats()

            messagebox.showinfo("Restore Complete",
                "✅ Profile restored and reloaded!\n\n"
                "All credentials are now active.\n"
                "YouTube connection may need a reconnect.",
                parent=self.root)
        except Exception as e:
            messagebox.showerror("Reload Error",
                f"Restore succeeded but hot-reload failed:\n{e}\n\n"
                "Please restart the app.", parent=self.root)

    def _toggle_stay_on_top(self):
        global WIN_STAY_ON_TOP
        WIN_STAY_ON_TOP = self._sot_var.get()
        self.root.attributes("-topmost", WIN_STAY_ON_TOP)
        db_set("stay_on_top", "1" if WIN_STAY_ON_TOP else "0")

    def _set_opacity(self, val):
        global WIN_OPACITY
        WIN_OPACITY = int(val) / 100
        self.root.attributes("-alpha", WIN_OPACITY)
        db_set("opacity", str(WIN_OPACITY))

    def _toggle_emote_picker(self):
        """Close picker if already open, open it if not."""
        if self._emote_popup and self._emote_popup.winfo_exists():
            self._emote_popup.destroy()
            self._emote_popup = None
            return
        self._show_emote_picker()

    def _show_emote_picker(self):
        """Grid popup with MY EMOTES / GLOBAL tabs."""
        channel_emotes = g.get("channel_emotes", [])
        global_emotes  = g.get("global_emotes",  [])

        popup = tk.Toplevel(self.root)
        self._emote_popup = popup
        popup.protocol("WM_DELETE_WINDOW",
            lambda: (popup.destroy(), setattr(self, "_emote_popup", None)))
        popup.title("Emotes")
        popup.configure(bg=BG_DARK)
        popup.resizable(True, True)
        popup.attributes("-topmost", True)

        # Center picker horizontally over the chat column
        popup.update_idletasks()
        pw = 580
        ph = 660
        chat_x = self.chat_text.winfo_rootx()
        chat_w = self.chat_text.winfo_width()
        chat_y = self.chat_text.winfo_rooty()
        x = chat_x + (chat_w - pw) // 2
        y = chat_y + (self.chat_text.winfo_height() - ph) // 2
        popup.geometry(f"{pw}x{ph}+{x}+{y}")

        # Not connected yet
        if not channel_emotes and not global_emotes:
            tk.Label(popup,
                     text="😀  No emotes loaded yet\n\nConnect to Twitch first.",
                     fg=FG_GRAY, bg=BG_DARK,
                     font=("Segoe UI", 12), justify=tk.CENTER
                     ).pack(expand=True)
            tk.Button(popup, text="Close", bg=BG_BTN, fg=FG_WHITE,
                      font=("Segoe UI", 10), relief=tk.FLAT,
                      padx=12, pady=4, cursor="hand2",
                      command=popup.destroy).pack(pady=(0, 16))
            return

        # ── Tab bar ──────────────────────────────────────────────────
        tab_var = tk.StringVar(value="mine")
        tab_bar = tk.Frame(popup, bg=BG_PANEL2)
        tab_bar.pack(fill=tk.X)

        def _tab_style(btn, active):
            btn.configure(bg="#6441a5" if active else BG_PANEL2,
                          fg=FG_WHITE   if active else FG_GRAY)

        btn_mine   = tk.Button(tab_bar, text=f"🌟  MY EMOTES ({len(channel_emotes)})",
                               font=("Segoe UI", 10, "bold"), relief=tk.FLAT,
                               cursor="hand2", padx=12, pady=6)
        btn_global = tk.Button(tab_bar, text=f"🌍  GLOBAL ({len(global_emotes)})",
                               font=("Segoe UI", 10, "bold"), relief=tk.FLAT,
                               cursor="hand2", padx=12, pady=6)
        btn_mine.pack(side=tk.LEFT)
        btn_global.pack(side=tk.LEFT)
        _tab_style(btn_mine, True)
        _tab_style(btn_global, False)

        # ── Search bar ────────────────────────────────────────────────
        search_var = tk.StringVar()
        search_box = tk.Entry(popup, textvariable=search_var,
                              font=("Segoe UI", 11), bg=BG_PANEL2,
                              fg=FG_WHITE, insertbackground=FG_WHITE,
                              relief=tk.FLAT)
        search_box.pack(fill=tk.X, padx=8, pady=(6, 4), ipady=4)
        search_box.insert(0, "Search emotes...")
        search_box.bind("<FocusIn>", lambda e: (
            search_box.delete(0, tk.END)
            if search_box.get() == "Search emotes..." else None))

        # ── Scrollable canvas grid ────────────────────────────────────
        frame_outer = tk.Frame(popup, bg=BG_DARK)
        frame_outer.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)
        canvas = tk.Canvas(frame_outer, bg=BG_DARK, highlightthickness=0)
        scrollbar = tk.Scrollbar(frame_outer, orient=tk.VERTICAL,
                                  command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        grid_frame = tk.Frame(canvas, bg=BG_DARK)
        canvas_win = canvas.create_window((0, 0), window=grid_frame, anchor="nw")
        def _on_resize(e):
            canvas.itemconfig(canvas_win, width=e.width)
        canvas.bind("<Configure>", _on_resize)
        grid_frame.bind("<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<MouseWheel>",
            lambda e: canvas.yview_scroll(-1*(e.delta//120), "units"))

        COLS = 5
        _rendered = []  # keep PhotoImage refs alive

        def _insert_emote(name):
            cur = self.chat_message_box.get()
            sep = " " if cur and not cur.endswith(" ") else ""
            self.chat_message_box.insert(tk.END, f"{sep}{name} ")
            # Keep picker open so multiple emotes can be added
            # User closes it manually or by clicking the 😊 button again

        # Loading label shown while images fetch
        loading_lbl = tk.Label(grid_frame,
            text="🔄  Loading emotes...", fg=FG_GRAY, bg=BG_DARK,
            font=("Segoe UI", 11))
        loading_lbl.grid(row=0, column=0, columnspan=6, pady=20)

        CELL_W, CELL_H, IMG_S = 100, 100, 56

        def _render(emote_list):
            """Render emote grid — only called once images are ready."""
            for w in grid_frame.winfo_children():
                w.destroy()
            _rendered.clear()
            if not emote_list:
                tk.Label(grid_frame, text="No emotes match.",
                         fg=FG_GRAY, bg=BG_DARK,
                         font=("Segoe UI", 10)).grid(
                    row=0, column=0, columnspan=COLS, pady=20)
                return
            for idx, em in enumerate(emote_list[:300]):
                col  = idx % COLS
                row  = idx // COLS
                eid  = em["id"]
                name = em["name"]
                photo = g["picker_emote_cache"].get(eid) or g["emote_image_cache"].get(eid)

                cell = tk.Frame(grid_frame, bg=BG_DARK, cursor="hand2",
                                width=CELL_W, height=CELL_H)
                cell.grid(row=row, column=col, padx=3, pady=3)
                cell.grid_propagate(False)

                if photo:
                    lbl = tk.Label(cell, image=photo, bg=BG_DARK, cursor="hand2")
                    lbl.image = photo
                    _rendered.append(photo)
                else:
                    lbl = tk.Label(cell, text=name[:6], fg=FG_GOLD,
                                   bg=BG_DARK, font=("Segoe UI", 8, "bold"),
                                   cursor="hand2", wraplength=CELL_W - 4)
                # Place image centred in top portion, name label below
                lbl.place(x=CELL_W//2, y=IMG_S//2 + 4, anchor="center")
                tip = tk.Label(cell, text=name, fg=FG_GRAY, bg=BG_DARK,
                               font=("Segoe UI", 7), wraplength=CELL_W - 4,
                               cursor="hand2", justify=tk.CENTER)
                tip.place(x=CELL_W//2, y=IMG_S + 10, anchor="n")

                def _enter(e, c=cell, l=lbl, t=tip):
                    c.configure(bg=BG_PANEL2)
                    l.configure(bg=BG_PANEL2)
                    t.configure(bg=BG_PANEL2, fg=FG_WHITE)
                def _leave(e, c=cell, l=lbl, t=tip):
                    c.configure(bg=BG_DARK)
                    l.configure(bg=BG_DARK)
                    t.configure(bg=BG_DARK, fg=FG_GRAY)
                for w in (cell, lbl, tip):
                    w.bind("<Button-1>", lambda e, n=name: _insert_emote(n))
                    w.bind("<Enter>", _enter)
                    w.bind("<Leave>", _leave)

        active_list = [channel_emotes]  # mutable ref so tab switch updates it

        def _current_filtered():
            q = search_var.get().lower()
            src = active_list[0]
            return src if (not q or q == "search emotes...") \
                       else [e for e in src if q in e["name"].lower()]

        def _on_search(*_):
            _render(_current_filtered())

        def _switch_tab(tab):
            if tab == "mine":
                active_list[0] = channel_emotes
                _tab_style(btn_mine, True)
                _tab_style(btn_global, False)
            else:
                active_list[0] = global_emotes
                _tab_style(btn_mine, False)
                _tab_style(btn_global, True)
            search_var.set("")
            _prefetch_then_render(active_list[0])

        btn_mine.configure(  command=lambda: _switch_tab("mine"))
        btn_global.configure(command=lambda: _switch_tab("global"))

        def _prefetch_then_render(emote_list):
            """Fetch images for emote_list in one background thread, render once."""
            def _work():
                uncached = [e["id"] for e in emote_list[:300]
                            if e["id"] not in g["picker_emote_cache"]]
                for eid in uncached:
                    get_picker_emote_image(eid)
                    if not popup.winfo_exists():
                        return
                if popup.winfo_exists():
                    snap = list(emote_list)
                    self.ui(lambda: _render(snap))
            threading.Thread(target=_work, daemon=True).start()

        # Wire search ONCE — not inside prefetch so it never stacks up
        search_var.trace_add("write", _on_search)

        # Start on MY EMOTES tab
        _prefetch_then_render(channel_emotes)
        search_box.focus()

    def _send_chat_message(self):
        # Anti-spam / Enter-key bounce guard — ignore presses < 1.5s apart
        import time as _time
        now_time = _time.time()
        if hasattr(self, "_last_send_time") and (now_time - self._last_send_time) < 1.5:
            return
        self._last_send_time = now_time

        message = self.chat_message_box.get().strip()
        if not message:
            return

        target  = SEND_TARGET  # "twitch" | "youtube" | "all"
        now     = datetime.datetime.now()
        time_fmt = now.strftime("%I:%M %p")
        sent_any = False

        # ── Twitch IRC send ──────────────────────────────────────────
        if target in ("twitch", "all"):
            if not g["chat_socket"]:
                messagebox.showwarning("Twitch Not Connected",
                    "Not connected to Twitch chat.")
                if target == "twitch":
                    return
            else:
                try:
                    g["chat_socket"].sendall(
                        f"PRIVMSG #{AUTO_CHANNEL} :{message}\r\n".encode("utf-8"))
                    # Twitch does not echo own messages — display locally
                    # Build emote map so sent emotes render as images
                    own_emote_map = {
                        e["name"]: e["id"]
                        for e in (g.get("channel_emotes", []) +
                                  g.get("global_emotes",  []))
                        if e["name"] in message.split()
                    }
                    self._append_chat_message(
                        time_fmt, g["chat_username"] or "me",
                        message, own_emote_map, own=True)
                    sent_any = True
                except Exception as ex:
                    messagebox.showerror("Twitch Send Failed", str(ex))
                    if target == "twitch":
                        return

        # ── YouTube Live Chat send ───────────────────────────────────
        if target in ("youtube", "all"):
            if not g["yt_service"] or not g["yt_live_chat_id"]:
                messagebox.showwarning("YouTube Not Connected",
                    "Not connected to a YouTube livestream.")
                if target == "youtube":
                    return
            else:
                def _yt_send(msg=message, ts=time_fmt):
                    try:
                        g["yt_service"].liveChatMessages().insert(
                            part="snippet",
                            body={"snippet": {
                                "liveChatId": g["yt_live_chat_id"],
                                "type": "textMessageEvent",
                                "textMessageDetails": {"messageText": msg}
                            }}
                        ).execute()
                        # Instant local feedback — stops "lag illusion" re-sends
                        self.ui(lambda: self._append_chat_message(
                            ts, "me (YT)", f"{msg} ✓", own=True, platform="youtube"))
                    except Exception as ex:
                        log.warning(f"YouTube send failed: {ex}")
                        self.ui(lambda e=ex: messagebox.showerror(
                            "YouTube Send Failed", str(e)))
                threading.Thread(target=_yt_send, daemon=True).start()
                sent_any = True

        if sent_any:
            if self.log_var.get():
                log_chat(f"[{time_fmt}] SENT: {message}")
            self.chat_message_box.delete(0, tk.END)

    # ================================================================
    # YOUTUBE CHAT
    # ================================================================
    def _get_yt_creds(self, allow_browser=False):
        """Load and silently refresh YouTube credentials.
        Only opens browser if allow_browser=True (explicit user action).
        Returns creds or None."""
        from google.auth.transport.requests import Request

        SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]
        creds  = None

        # Load saved token from keyring
        raw = db_get("yt_token")
        log.info(f"YT token from DB: {'found' if raw else 'NOT FOUND'}")
        if raw:
            try:
                # Store as plain JSON — no pickle, no encoding issues
                from google.oauth2.credentials import Credentials
                data  = json.loads(raw)
                creds = Credentials(
                    token         = data.get("token"),
                    refresh_token = data.get("refresh_token"),
                    token_uri     = data.get("token_uri"),
                    client_id     = data.get("client_id"),
                    client_secret = data.get("client_secret"),
                    scopes        = data.get("scopes"),
                )
                log.info(f"YT creds loaded — valid={creds.valid}, "
                         f"expired={creds.expired}, "
                         f"has_refresh={bool(creds.refresh_token)}")
            except Exception as ex:
                log.warning(f"YT creds decode failed: {ex}")
                creds = None

        # Silently refresh if expired but refresh token exists — no browser
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                log.info("YouTube token refreshed silently")
            except Exception as ex:
                log.warning(f"YouTube token refresh failed: {ex}")
                creds = None

        # If still no valid creds and browser allowed — open login once
        if (not creds or not creds.valid) and allow_browser:
            from google_auth_oauthlib.flow import InstalledAppFlow
            if not os.path.exists(YT_CLIENT_SECRET):
                self.ui(lambda: messagebox.showerror(
                    "YouTube Auth",
                    f"client_secret.json not found in:\n{SCRIPT_DIR}"))
                return None
            log.info("YouTube opening browser for first-time login...")
            flow  = InstalledAppFlow.from_client_secrets_file(
                YT_CLIENT_SECRET, SCOPES)
            creds = flow.run_local_server(port=0)
        elif not creds or not creds.valid:
            log.info("YouTube: no valid token found, browser login required")
            return None

        # Save as clean JSON — no pickle, no base64, no encoding issues
        try:
            token_data = json.dumps({
                "token":         creds.token,
                "refresh_token": creds.refresh_token,
                "token_uri":     creds.token_uri,
                "client_id":     creds.client_id,
                "client_secret": creds.client_secret,
                "scopes":        list(creds.scopes) if creds.scopes else [],
            })
            db_set("yt_token", token_data)
            log.info(f"YT token saved to DB ({len(token_data)} chars)")
        except Exception as ex:
            log.warning(f"Failed to save YT token: {ex}")

        # Migrate old yt_token.pkl if present
        old_path = os.path.join(SCRIPT_DIR, "yt_token.pkl")
        if os.path.exists(old_path):
            try:
                os.remove(old_path)
                log.info("yt_token.pkl removed")
            except Exception:
                pass

        return creds

    def _start_yt_chat(self):
        """Authenticate with YouTube and start polling live chat."""
        if g["yt_polling"]:
            return
        g["yt_stop_event"].clear()
        self._set_yt_state("connecting")

        def _auth():
            try:
                from googleapiclient.discovery import build

                # User explicitly clicked connect — allow browser if needed
                creds = self._get_yt_creds(allow_browser=True)
                if not creds:
                    self._set_yt_state("failed")
                    return

                g["yt_service"] = build("youtube", "v3", credentials=creds)

                # Find active live broadcast
                resp = g["yt_service"].liveBroadcasts().list(
                    part="snippet", broadcastStatus="active",
                    broadcastType="all").execute()

                if not resp.get("items"):
                    self.ui(lambda: self._append_chat_message(
                        datetime.datetime.now().strftime("%I:%M %p"),
                        "SYSTEM", "No active YouTube livestream found. Watching for stream...",
                        platform="youtube"))
                    self._set_yt_state("disconnected")
                    # Start auto-detect watcher
                    self.ui(self._start_yt_watcher)
                    return

                g["yt_live_chat_id"] = resp["items"][0]["snippet"]["liveChatId"]
                g["yt_polling"]      = True
                g["yt_next_page"]    = None

                self._set_yt_state("connected")
                self.ui(lambda: self._append_chat_message(
                    datetime.datetime.now().strftime("%I:%M %p"),
                    "SYSTEM", "✅ YouTube chat connected!",
                    platform="youtube"))
                self.ui(self._poll_yt_chat)

            except Exception as ex:
                self.root.after(0, lambda: messagebox.showerror(
                    "YouTube Error", str(ex)))

        threading.Thread(target=_auth, daemon=True).start()

    def _poll_yt_chat(self):
        """Poll YouTube live chat — respects pollingIntervalMillis and rate limits."""
        if not g["yt_polling"] or not g["yt_live_chat_id"]:
            return
        if g["yt_stop_event"].is_set():
            return

        def _fetch():
            try:
                kwargs = dict(
                    liveChatId=g["yt_live_chat_id"],
                    part="snippet,authorDetails",
                    maxResults=200)
                if g["yt_next_page"]:
                    kwargs["pageToken"] = g["yt_next_page"]

                resp     = g["yt_service"].liveChatMessages().list(**kwargs).execute()
                g["yt_next_page"] = resp.get("nextPageToken")
                # Strictly follow YouTube's requested polling interval
                interval = max(5000, resp.get("pollingIntervalMillis", 5000))
                items    = resp.get("items", [])

                for item in items:
                    snippet = item["snippet"]
                    author  = item["authorDetails"]
                    msg     = snippet.get("displayMessage", "")
                    user    = author.get("displayName", "unknown")
                    ts      = datetime.datetime.now().strftime("%I:%M %p")
                    self.ui(lambda t=ts, u=user, m=msg:
                        self._append_chat_message(t, u, m, platform="youtube"))
                    if self.log_var.get():
                        log_chat(f"[{t}] YOUTUBE: {u}: {m}")

                if g["yt_polling"] and not g["yt_stop_event"].is_set():
                    g["yt_poll_job"] = self.root.after(interval, self._poll_yt_chat)

            except Exception as ex:
                err_str = str(ex)
                log.warning(f"YT poll error: {err_str}")

                # 1. STREAM ENDED — chat closed, not found, or access revoked
                if ("liveChatEnded" in err_str or "notFound" in err_str
                        or "404" in err_str
                        or ("403" in err_str
                            and "quota" not in err_str.lower()
                            and "rate" not in err_str.lower())):
                    log.info("YouTube live chat ended — disconnecting and returning to watcher.")
                    self.ui(self._stop_yt_chat)
                    self.ui(lambda: self._append_chat_message(
                        datetime.datetime.now().strftime("%I:%M %p"),
                        "SYSTEM",
                        "⚪ YouTube stream ended. Chat disconnected. Watching for next stream...",
                        platform="youtube"))
                    self.ui(self._start_yt_watcher)
                    return

                # 2. QUOTA / RATE LIMIT — back off 5 minutes
                elif ("quotaExceeded" in err_str or "rateLimitExceeded" in err_str
                        or "429" in err_str
                        or ("403" in err_str and "quota" in err_str.lower())):
                    cooldown = 300_000
                    log.warning("YouTube quota/rate limit hit — cooling down 5 minutes")
                    self.ui(lambda: self._set_yt_state("reconnecting"))
                    self.ui(lambda: self.chat_status_label.configure(
                        text="● YT RATE LIMITED — COOLING DOWN 5min", fg="#ffd700"))
                    if g["yt_polling"]:
                        g["yt_poll_job"] = self.root.after(cooldown, self._poll_yt_chat)

                # 3. TEMPORARY GLITCH — retry in 15s
                elif g["yt_polling"]:
                    g["yt_poll_job"] = self.root.after(15000, self._poll_yt_chat)

        threading.Thread(target=_fetch, daemon=True).start()

    def _start_yt_watcher(self):
        """Poll every 60s waiting for an active YouTube livestream to start."""
        # Guard: never run more than one watcher at a time
        if g.get("yt_watching", False):
            return
        if g["yt_polling"] or g["yt_stop_event"].is_set():
            return

        g["yt_watching"] = True
        log.info("YouTube watcher: checking for live stream...")

        def _check():
            try:
                if not g["yt_service"]:
                    g["yt_watching"] = False
                    return
                resp = g["yt_service"].liveBroadcasts().list(
                    part="snippet", broadcastStatus="active",
                    broadcastType="all").execute()
                if resp.get("items"):
                    # Stream found — connect automatically!
                    g["yt_watching"] = False  # unlock before connecting
                    log.info("YouTube watcher: stream detected, connecting...")
                    g["yt_live_chat_id"] = resp["items"][0]["snippet"]["liveChatId"]
                    g["yt_polling"]      = True
                    g["yt_next_page"]    = None
                    self._set_yt_state("connected")
                    self.ui(lambda: self._append_chat_message(
                        datetime.datetime.now().strftime("%I:%M %p"),
                        "SYSTEM", "🔴 YouTube stream detected — chat connected!",
                        platform="youtube"))
                    self.ui(self._poll_yt_chat)
                else:
                    # Not live yet — check again in 60s
                    g["yt_watching"] = False  # unlock before rescheduling
                    if not g["yt_stop_event"].is_set():
                        self.ui(lambda: self.root.after(60000, self._start_yt_watcher))
            except Exception as ex:
                g["yt_watching"] = False  # always unlock on error
                err_str = str(ex)
                log.warning(f"YT watcher error: {ex}")
                if "quotaExceeded" in err_str or "quota" in err_str.lower():
                    # Quota exhausted — back off 6 hours, no point hammering the API
                    log.warning("YouTube quota exceeded in watcher — backing off 6 hours")
                    self.ui(lambda: self.chat_status_label.configure(
                        text="● YT QUOTA EXCEEDED — RETRY IN 6H", fg="#ffd700")
                        if hasattr(self, "chat_status_label") else None)
                    if not g["yt_stop_event"].is_set():
                        self.ui(lambda: self.root.after(6 * 3600 * 1000, self._start_yt_watcher))
                elif not g["yt_stop_event"].is_set():
                    self.ui(lambda: self.root.after(60000, self._start_yt_watcher))

        threading.Thread(target=_check, daemon=True).start()

    def _set_yt_state(self, state):
        """Update YouTube connection state and reflect in UI."""
        g["yt_state"] = state
        dot_color = {"connected": FG_LIME, "connecting": "#ffd700",
                     "reconnecting": "#ff8c00", "failed": "#ff4444",
                     "disconnected": FG_GRAY}.get(state, FG_GRAY)
        if hasattr(self, "yt_dot"):
            use_color = state == "connected"
            def _upd_yt(c=dot_color, col=use_color):
                self.yt_dot.configure(fg=c)
                if hasattr(self, "yt_dot_img"):
                    icon = g.get("youtube_icon") if col else g.get("youtube_icon_gray")
                    if icon:
                        self.yt_dot_img.configure(image=icon)
                        self.yt_dot_img.image = icon
            self.ui(_upd_yt)
        log.info(f"YouTube state → {state}")

    def _stop_yt_chat(self):
        """Stop YouTube chat polling cleanly."""
        g["yt_stop_event"].set()
        g["yt_polling"]      = False
        g["yt_live_chat_id"] = None
        if g["yt_poll_job"]:
            self.root.after_cancel(g["yt_poll_job"])
            g["yt_poll_job"] = None
        # buttons now in settings popup — dot updated via _set_yt_state
        self._append_chat_message(
            datetime.datetime.now().strftime("%I:%M %p"),
            "SYSTEM", "YouTube chat disconnected.", platform="youtube")
        log.info("YouTube chat disconnected by user")

    def _stop_chat_irc(self):
        g["irc_sock"] = None
        g["tw_stop_event"].set()
        if g["chat_socket"]:
            try:
                g["chat_socket"].close()
            except Exception:
                pass
            g["chat_socket"]   = None
            g["chat_username"] = None
        g["tw_state"] = "disconnected"
        self.chat_status_label.configure(text="● DISCONNECTED", fg=FG_GRAY)
        # buttons now in settings popup — dot updated via _set_tw_state
        log.info("Twitch IRC disconnected by user")

    # ================================================================
    # STARTUP
    # ================================================================
    def _process_ui_queue(self):
        """16ms heartbeat — drains the UI task queue on the main thread."""
        try:
            while not self.ui_queue.empty():
                item = self.ui_queue.get_nowait()
                task, args = item
                if args:
                    task(*args)
                else:
                    task()
        except Exception as ex:
            log.warning(f"UI queue error: {ex}")
        finally:
            self.root.after(16, self._process_ui_queue)

    def ui(self, task, *args):
        """Thread-safe UI update — any background thread calls this.
        Pass either ui(callable) or ui(callable, arg1, arg2...)
        """
        self.ui_queue.put((task, args))

    def _load_emotes(self):
        """Fetch channel + global emotes — works offline/not-live.
        Only needs a valid token and AUTO_CHANNEL name.
        """
        if g.get("channel_emotes"):  # already loaded
            return
        if not g.get("token") or not AUTO_CHANNEL:
            log.info("Emotes: no token or channel name yet — skipping")
            return

        def _fetch():
            try:
                # Step 1 — resolve broadcaster_id
                broadcaster_id = g.get("my_id")
                if not broadcaster_id:
                    data = invoke_twitch_api(
                        f"https://api.twitch.tv/helix/users?login={AUTO_CHANNEL}")
                    if data and data.get("data"):
                        broadcaster_id = data["data"][0]["id"]
                        g["my_id"] = broadcaster_id
                if not broadcaster_id:
                    log.warning("Emotes: could not resolve broadcaster_id")
                    return

                # Step 2 — YOUR channel emotes only (sub badges, custom etc.)
                emote_resp = invoke_twitch_api(
                    f"https://api.twitch.tv/helix/chat/emotes"
                    f"?broadcaster_id={broadcaster_id}")
                channel = []
                if emote_resp and emote_resp.get("data"):
                    channel = [{"id": e["id"], "name": e["name"]}
                                for e in emote_resp["data"]]
                    log.info(f"Loaded {len(channel)} channel emotes")
                g["channel_emotes"] = channel

                # Pre-cache YOUR emotes immediately — small set, fast
                for em in channel:
                    get_emote_image(em["id"])        # 28x28 for chat
                    get_picker_emote_image(em["id"]) # 48x48 for picker

                # Step 3 — global emotes (lazy, stored separately)
                global_resp = invoke_twitch_api(
                    "https://api.twitch.tv/helix/chat/emotes/global")
                if global_resp and global_resp.get("data"):
                    g["global_emotes"] = [
                        {"id": e["id"], "name": e["name"]}
                        for e in global_resp["data"]
                    ]
                    log.info(f"Loaded {len(g['global_emotes'])} global emotes")

            except Exception as ex:
                log.warning(f"_load_emotes error: {ex}")

        threading.Thread(target=_fetch, daemon=True).start()

    def _on_shown(self):
        # Sync send button color to saved default target
        self._set_send_target(db_get("send_target", "twitch"))
        # pre-fetch Twitch + YouTube icons; callback refreshes send button image
        def _icons_loaded():
            self.ui(lambda: self._set_send_target(SEND_TARGET))
            # Apply grayscale status icons now that they're loaded
            def _apply_status_icons():
                # Fall back to PIL logos only if branded assets weren't found
                if g.get("_need_tw_pil_logos"):
                    g["_need_tw_pil_logos"] = False
                    if not g.get("twitch_icon"):
                        g["twitch_icon"]      = _make_tw_logo(28)
                        g["twitch_icon_gray"] = _make_tw_logo(28)
                    if not g.get("twitch_icon_large"):
                        g["twitch_icon_large"] = _make_tw_logo(68)

                tw_gray = g.get("twitch_icon_gray")
                yt_gray = g.get("youtube_icon_gray")

                tw_gray = g.get("twitch_icon_gray")
                yt_gray = g.get("youtube_icon_gray")

                # Both offline at startup — always use gray versions
                if hasattr(self, "tw_status_icon"):
                    if tw_gray:
                        self.tw_status_icon.configure(image=tw_gray)
                    else:
                        self.tw_status_icon.configure(text="TW", fg="#555555")
                if hasattr(self, "yt_status_icon"):
                    if yt_gray:
                        self.yt_status_icon.configure(image=yt_gray)
                    else:
                        self.yt_status_icon.configure(text="▶", fg="#555555")
                # Chat header dots
                if hasattr(self, "tw_dot_img") and tw_gray:
                    self.tw_dot_img.configure(image=tw_gray)
                    self.tw_dot_img.image = tw_gray
                if hasattr(self, "yt_dot_img") and yt_gray:
                    self.yt_dot_img.configure(image=yt_gray)
                    self.yt_dot_img.image = yt_gray
                # Twitch icon in stat panel — replace placeholder with large favicon
                tw_large = g.get("twitch_icon_large")
                if tw_large and hasattr(self, "_tw_icon_canvas") and hasattr(self, "_tw_icon_item"):
                    twc = self._tw_icon_canvas
                    try: twc.delete(self._tw_icon_item)
                    except Exception: pass
                    h = twc.winfo_height() or 105
                    ix = 10; iy = 24 + (h - 24) // 2
                    self._tw_icon_item = twc.create_image(ix, iy, image=tw_large, anchor="w")
                    twc._tw_icon_ref = tw_large
                # Build combined ALL icon: TW + YT side by side
                try:
                    tw_path = os.path.join(tempfile.gettempdir(), "tw_icon.ico")
                    yt_path = os.path.join(tempfile.gettempdir(), "yt_icon.ico")
                    tw_img = Image.open(tw_path).convert("RGBA").resize((22, 22), Image.LANCZOS)
                    yt_img = Image.open(yt_path).convert("RGBA").resize((16, 16), Image.LANCZOS)
                    canvas = Image.new("RGBA", (28, 22), (0, 0, 0, 0))
                    canvas.paste(tw_img, (0, 0), tw_img)
                    canvas.paste(yt_img, (11, 6), yt_img)
                    g["all_icon"] = ImageTk.PhotoImage(canvas)
                except Exception:
                    pass
                # Refresh send button if currently on ALL
                if SEND_TARGET == "all":
                    self.ui(lambda: self._set_send_target("all"))
            self.ui(_apply_status_icons)
        load_platform_icons(on_loaded=_icons_loaded)
        # Start YouTube service silently in background for auto-detect
        def _init_yt():
            try:
                from googleapiclient.discovery import build
                # Silent only — never open browser on startup
                creds = self._get_yt_creds(allow_browser=False)
                if creds:
                    g["yt_service"] = build("youtube", "v3", credentials=creds)
                    log.info("YouTube service initialized — auto-detect active")
                    self.ui(self._start_yt_watcher)
                else:
                    log.info("YouTube: no saved token — click YT CONNECT to sign in")
            except Exception as ex:
                log.warning(f"YouTube silent init failed: {ex}")
        threading.Thread(target=_init_yt, daemon=True).start()

        token = load_token()
        if token:
            self.token_box.insert(0, token)
            g["token"] = token
            self._refresh_streamer_stats()
            self._schedule_lurker_timer()
            self._load_emotes()  # load emotes independently — works offline
        self._start_chat_irc()
        # Resize triggers lurker refresh (matches PS1 Form.Add_Resize)
        self.root.bind("<Configure>",
                       lambda e: self._on_resize(e))

    def _on_resize(self, e):
        if e.widget == self.root:
            g["next_refresh_time"] = datetime.datetime.now()


# ========================================================================
# ENTRY POINT
# ========================================================================
if __name__ == "__main__":
    try:
        root = tk.Tk()
        app  = App(root)
        root.mainloop()
    except Exception as e:
        import traceback
        traceback.print_exc()
        input("Press Enter to exit...")
