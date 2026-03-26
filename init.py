#!/usr/bin/env python3
"""
Media Forge — Combined TV Renamer + MKV Track Forge
• Writes status.json for live dashboard.html updates
• All subprocess calls use CREATE_NO_WINDOW on Windows (no terminal popups)
• Fully threaded — UI never blocks
"""

import os, re, json, threading, urllib.request, urllib.parse, subprocess, shutil
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import customtkinter as ctk
from tkinter import filedialog, messagebox

# ─── Suppress console windows on Windows ───────────────────────────────────
import sys, platform
POPEN_FLAGS = {}
if platform.system() == "Windows":
    import ctypes as _ctypes
    CREATE_NO_WINDOW = 0x08000000
    POPEN_FLAGS = {"creationflags": CREATE_NO_WINDOW}

# ─── Status JSON path (same dir as script) ─────────────────────────────────
STATUS_PATH = Path(__file__).parent / "status.json"

def write_status(data: dict):
    data["updated"] = datetime.now().isoformat()
    try:
        with open(STATUS_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception:
        pass

# ─── Shared app state ───────────────────────────────────────────────────────
_state = {
    "active_tab": "tv",
    "tv": {
        "show": None, "year": None, "folder": None,
        "mode": "dry_run", "running": False,
        "log": [], "stats": {"renamed": 0, "skipped": 0, "errors": 0},
        "progress": 0,
    },
    "mkv": {
        "files": [], "running": False,
        "log": [], "stats": {"full": 0, "partial": 0, "skipped": 0, "failed": 0},
        "progress": 0,
        "audio_tracks": [], "sub_tracks": [],
    },
}

def push_log(tab: str, msg: str, level: str = "info"):
    entry = {"t": datetime.now().strftime("%H:%M:%S"), "msg": msg, "level": level}
    _state[tab]["log"].append(entry)
    _state[tab]["log"] = _state[tab]["log"][-300:]  # keep last 300 lines
    write_status(_state)

# ═══════════════════════════════════════════════════════════════════════════
#  TV RENAMER BACKEND
# ═══════════════════════════════════════════════════════════════════════════
TVMAZE_BASE = "https://api.tvmaze.com"
TRASH_WORDS = [
    r'10bit', r'x265', r'x264', r'hevc', r'dual[-. ]audio', r'multi',
    r'esubs?', r'subs?', r'\[.*?\]', r'\(.*?\)',
    r'1080p', r'720p', r'480p', r'2160p', r'4k',
    r'BluRay', r'WEB-?DL', r'HDTV', r'DVDRip', r'HDRip',
    r'AMZN', r'NF', r'DSNP', r'HMAX',
    r'AAC\d*', r'AC3', r'DTS', r'FLAC', r'MP3',
    r'H\.?264', r'H\.?265', r'AVC', r'REMUX',
    r'[A-Z0-9]{5,}\.',
]

def tvmaze_get(path):
    url = TVMAZE_BASE + path
    req = urllib.request.Request(url, headers={"User-Agent": "MediaForge/2.0"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read().decode())

def search_shows(query):
    encoded = urllib.parse.quote(query)
    data = tvmaze_get(f"/search/shows?q={encoded}")
    results = []
    for item in data:
        s = item["show"]
        year = (s.get("premiered") or "")[:4]
        network = ""
        if s.get("network"):
            network = s["network"].get("name", "")
        elif s.get("webChannel"):
            network = s["webChannel"].get("name", "")
        label = f"{s['name']}  ({year})  [{network}]  — id:{s['id']}"
        results.append((label, s["id"], s["name"], year))
    return results

def fetch_episodes(show_id):
    data = tvmaze_get(f"/shows/{show_id}/episodes")
    ep_map  = {}   # (season, ep) → (title, airdate)
    abs_map = {}   # absolute_number → (title, airdate)  — for shows like One Piece
    for ep in data:
        s       = ep.get("season", 0)
        e       = ep.get("number", 0)
        abs_num = ep.get("airstamp") and None  # placeholder; use number field below
        abs_num = ep.get("number", 0)          # TVmaze "number" within season
        # TVmaze also provides a global sequence via "id" ordering — use loop index
        name    = ep.get("name", "") or "Unknown"
        airdate = ep.get("airdate", "") or ""
        if s and e:
            ep_map[(s, e)] = (name, airdate)
    # Build absolute-number index using the order TVmaze returns episodes
    # (they come back in broadcast order, so enumerate gives a reliable abs number)
    for abs_idx, ep in enumerate(data, start=1):
        name    = ep.get("name", "") or "Unknown"
        airdate = ep.get("airdate", "") or ""
        abs_map[abs_idx] = (name, airdate)
    return ep_map, abs_map

def detect_episode(fname):
    clean = fname
    for pat in TRASH_WORDS:
        clean = re.sub(pat, ' ', clean, flags=re.IGNORECASE)
    patterns = [
        (r'[Ss](\d{1,2})[Ee](\d{1,4})', 1, 2),
        (r'[Ss]eason\s*(\d{1,2}).*?[Ee]p?(?:isode)?\s*(\d{1,4})', 1, 2),
        (r'(\d{1,2})x(\d{2,4})', 1, 2),
        (r'[Ss](\d{1,2})[-_. ](\d{2,4})', 1, 2),
    ]
    for p, sg, eg in patterns:
        m = re.search(p, f" {clean} ", re.IGNORECASE)
        if m:
            return int(m.group(sg)), int(m.group(eg))
    abs_pats = [
        r'[Ee]p?(\d{2,4})(?:[^p]|$)',
        r'[-_ ](\d{3,4})[-_ .]',
        r'^(\d{3,4})[-_ .]',
    ]
    for p in abs_pats:
        m = re.search(p, f" {clean} ", re.IGNORECASE)
        if m:
            return 1, int(m.group(1).lstrip('0') or '0')
    return None

def collect_video_files(root, recurse):
    exts = ('.mkv', '.mp4', '.avi', '.m4v', '.mov', '.wmv', '.ts', '.flv')
    out = []
    if recurse:
        for dp, dns, fns in os.walk(root):
            dns.sort()
            for fn in sorted(fns):
                if fn.lower().endswith(exts):
                    out.append((dp, fn))
    else:
        for fn in sorted(os.listdir(root)):
            full = os.path.join(root, fn)
            if os.path.isfile(full) and fn.lower().endswith(exts):
                out.append((root, fn))
    return out

def build_new_name(show_name, episode_map, abs_map, season, ep, ext, use_airdate):
    key = (season, ep)
    if key in episode_map:
        title, airdate = episode_map[key]
    elif ep in abs_map:
        # Absolute-numbered show (e.g. One Piece): ep number IS the absolute ep
        title, airdate = abs_map[ep]
    else:
        title, airdate = "Unknown Title", ""
    airdate_tag = f" - [{airdate}]" if (use_airdate and airdate) else ""
    name = f"{show_name} - S{season:02d}E{ep:03d}{airdate_tag} - {title}.{ext}"
    return re.sub(r'[\\/*?:"<>|]', "", name)

# ═══════════════════════════════════════════════════════════════════════════
#  MKV BACKEND
# ═══════════════════════════════════════════════════════════════════════════
SPECIAL_SUFFIX = "-special"

def _find_tool(name):
    p = shutil.which(name)
    if p:
        return p
    for base in [r"C:\Program Files\MKVToolNix", r"C:\Program Files (x86)\MKVToolNix"]:
        pp = os.path.join(base, name + ".exe")
        if os.path.exists(pp):
            return pp
    return None

MKVMERGE    = _find_tool("mkvmerge")
MKVPROPEDIT = _find_tool("mkvpropedit")

def _run(args):
    return subprocess.run(
        args, capture_output=True, text=True,
        encoding="utf-8", errors="replace",
        stdin=subprocess.DEVNULL, **POPEN_FLAGS
    )

# ── Scan cache: path → parsed JSON. Cleared when user clears file list. ──
SCAN_CACHE: dict = {}

def identify_mkv(path):
    if path in SCAN_CACHE:
        return SCAN_CACHE[path]
    if not MKVMERGE:
        return None
    r = _run([MKVMERGE, "--identify", "--identification-format", "json", path])
    try:
        data = json.loads(r.stdout)
        # Precompute _key on every track so callers never recompute it
        for t in data.get("tracks", []):
            t["_key"] = track_key(t)
        SCAN_CACHE[path] = data
        return data
    except Exception:
        return None

def _lang_from_moviesmod(name: str) -> str:
    m = re.match(r"(?:MoviesMod\.org\s*-\s*)(.+)", name, re.IGNORECASE)
    return m.group(1).strip() if m else name

def track_key(track):
    props = track.get("properties", {})
    lang  = props.get("language_ietf") or props.get("language") or "und"
    raw   = (props.get("track_name") or "").strip()
    name  = _lang_from_moviesmod(raw) if raw else ""
    codec = track.get("codec", "")
    ttype = track.get("type", "")
    return (ttype, lang, name, codec)

def key_to_str(k):
    return "|".join(str(x) for x in k)

def str_to_key(s):
    if not s:
        return None
    p = s.split("|")
    return tuple(p) if len(p) == 4 else None

def track_label(track):
    props   = track.get("properties", {})
    lang    = (props.get("language_ietf") or props.get("language") or "und").upper()
    raw     = (props.get("track_name") or "").strip()
    name    = _lang_from_moviesmod(raw) if raw else ""
    codec   = track.get("codec", "")
    forced  = " [FORCED]" if props.get("forced_track") else ""
    default = " ★"        if props.get("default_track") else ""
    parts   = [lang]
    if name:
        parts.append(f'"{name}"')
    parts.append(f"[{codec}]")
    return "  ".join(parts) + forced + default

def find_common_tracks(file_data_list, ttype):
    if not file_data_list:
        return [], []
    key_counts, key_labels = defaultdict(int), {}
    total = len(file_data_list)
    for fd in file_data_list:
        if not fd:
            continue
        seen = set()
        for t in fd.get("tracks", []):
            if t.get("type") != ttype:
                continue
            k = t.get("_key") or track_key(t)
            if k not in seen:
                key_counts[k] += 1
                key_labels[k]  = track_label(t)
                seen.add(k)
    common  = sorted([(k, key_labels[k], key_counts[k])
                      for k in key_counts if key_counts[k] == total], key=lambda x: -x[2])
    partial = sorted([(k, key_labels[k], key_counts[k])
                      for k in key_counts if key_counts[k] < total], key=lambda x: -x[2])
    return common, partial

def files_missing_track(file_paths, file_data, key, ttype):
    if not key:
        return set()
    missing = set()
    for p in file_paths:
        fd = file_data.get(p)
        if not fd:
            missing.add(p)
            continue
        found = any(
            (t.get("_key") or track_key(t)) == key
            for t in fd.get("tracks", [])
            if t.get("type") == ttype
        )
        if not found:
            missing.add(p)
    return missing

def set_defaults(mkv_path, audio_key, sub_key, append_special=False):
    if not MKVPROPEDIT:
        return False, "mkvpropedit not found."
    data = identify_mkv(mkv_path)
    if not data:
        return False, "Could not identify file."
    args = [MKVPROPEDIT, mkv_path]
    if append_special:
        cur = (data.get("container", {}).get("properties", {}).get("title")
               or Path(mkv_path).stem)
        new_title = cur if cur.endswith(SPECIAL_SUFFIX) else cur + SPECIAL_SUFFIX
        args += ["--edit", "info", "--set", f"title={new_title}"]
    fa = fs = False
    for t in data.get("tracks", []):
        tid   = t["id"] + 1
        ttype = t["type"]
        k     = t.get("_key") or track_key(t)
        if ttype == "audio":
            val = 1 if (audio_key and k == audio_key) else 0
            if audio_key and k == audio_key:
                fa = True
            args += ["--edit", f"track:{tid}", "--set", f"flag-default={val}"]
        elif ttype == "subtitles":
            val = 1 if (sub_key and k == sub_key) else 0
            if sub_key and k == sub_key:
                fs = True
            args += ["--edit", f"track:{tid}", "--set", f"flag-default={val}"]
    if len(args) == 2:
        return False, "No audio/subtitle tracks found."
    r = _run(args)
    if r.returncode != 0:
        return False, r.stderr.strip() or "mkvpropedit error"
    warns = []
    if audio_key and not fa:
        warns.append("audio track not in file")
    if sub_key and not fs:
        warns.append("subtitle track not in file")
    suffix_note = f"  [title+={SPECIAL_SUFFIX!r}]" if append_special else ""
    return True, ("OK" + suffix_note + (" (warn: " + ", ".join(warns) + ")" if warns else ""))

# ═══════════════════════════════════════════════════════════════════════════
#  COLOUR PALETTE
# ═══════════════════════════════════════════════════════════════════════════
C_BG      = "#0a0c10"
C_SIDEBAR = "#0f1117"
C_CARD    = "#141720"
C_ACCENT  = "#6ee7b7"
C_ACCENT2 = "#38bdf8"
C_SUCCESS = "#4ade80"
C_ERROR   = "#f87171"
C_WARN    = "#fbbf24"
C_TEXT    = "#e2e8f0"
C_SUBTEXT = "#475569"
C_BORDER  = "#1e2433"
C_INPUT   = "#1a1f2e"

ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

# ═══════════════════════════════════════════════════════════════════════════
#  MAIN APP WINDOW
# ═══════════════════════════════════════════════════════════════════════════
class MediaForge(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Media Forge")
        self.geometry("1260x820")
        self.minsize(1000, 660)
        self.configure(fg_color=C_BG)

        # TV state
        self.tv_show_id      = None
        self.tv_show_name    = ""
        self.tv_show_year    = ""
        self.tv_episode_map  = {}   # (season, ep) → (title, airdate)
        self.tv_abs_map      = {}   # absolute_ep  → (title, airdate)
        self.tv_search_res   = []
        self.tv_path         = ""

        # MKV state
        self.mkv_file_paths  = []
        self.mkv_file_data   = {}
        self.mkv_audio_common  = []
        self.mkv_audio_partial = []
        self.mkv_sub_common    = []
        self.mkv_sub_partial   = []
        self.mkv_sel_audio   = ctk.StringVar(value="")
        self.mkv_sel_sub     = ctk.StringVar(value="")
        self.mkv_move_files  = ctk.BooleanVar(value=False)
        self.mkv_out_folder  = ctk.StringVar(value="")

        self.mkv_sel_audio.trace_add("write", lambda *_: self._mkv_update_highlights())
        self.mkv_sel_sub.trace_add("write",   lambda *_: self._mkv_update_highlights())

        self._build_ui()
        write_status(_state)

    # ─────────────────────────────────────────────────────────────────────
    # TOP-LEVEL LAYOUT
    # ─────────────────────────────────────────────────────────────────────
    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        # ── Header bar ──────────────────────────────────────────────────
        hdr = ctk.CTkFrame(self, fg_color=C_SIDEBAR, height=56, corner_radius=0)
        hdr.grid(row=0, column=0, sticky="ew")
        hdr.grid_propagate(False)
        ctk.CTkLabel(hdr, text="⬡  Media Forge",
                     font=("Georgia", 22, "bold"),
                     text_color=C_ACCENT).pack(side="left", padx=20, pady=10)
        ctk.CTkLabel(hdr, text="TV Renamer  +  MKV Track Manager",
                     font=("Consolas", 11), text_color=C_SUBTEXT).pack(side="left")

        # Dashboard link hint
        ctk.CTkLabel(hdr, text="📊 Open dashboard.html for live view",
                     font=("Consolas", 10), text_color=C_SUBTEXT).pack(side="right", padx=16)

        # ── Tab view ────────────────────────────────────────────────────
        self.tabs = ctk.CTkTabview(self, fg_color=C_BG,
                                   segmented_button_fg_color=C_SIDEBAR,
                                   segmented_button_selected_color=C_ACCENT,
                                   segmented_button_selected_hover_color="#34d399",
                                   segmented_button_unselected_color=C_SIDEBAR,
                                   segmented_button_unselected_hover_color=C_BORDER,
                                   text_color=C_TEXT,
                                   text_color_disabled=C_SUBTEXT)
        self.tabs.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))
        self.tabs.add("📺  TV Renamer")
        self.tabs.add("🎬  MKV Track Forge")
        self.tabs._segmented_button.configure(font=("Segoe UI", 13, "bold"))

        self._build_tv_tab(self.tabs.tab("📺  TV Renamer"))
        self._build_mkv_tab(self.tabs.tab("🎬  MKV Track Forge"))

    # ─────────────────────────────────────────────────────────────────────
    # TV RENAMER TAB
    # ─────────────────────────────────────────────────────────────────────
    def _build_tv_tab(self, parent):
        parent.grid_columnconfigure(1, weight=1)
        parent.grid_rowconfigure(0, weight=1)

        # ── Sidebar ──────────────────────────────────────────────────────
        sb = ctk.CTkFrame(parent, width=270, fg_color=C_SIDEBAR, corner_radius=12)
        sb.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        sb.grid_propagate(False)
        sb.grid_rowconfigure(99, weight=1)

        self._tv_sec(sb, 0, "① Search Show  (TVmaze)")
        self.tv_show_entry = ctk.CTkEntry(
            sb, placeholder_text="e.g. Breaking Bad",
            fg_color=C_INPUT, border_color=C_BORDER,
            text_color=C_TEXT, placeholder_text_color=C_SUBTEXT,
            font=("Consolas", 12), height=34, corner_radius=8)
        self.tv_show_entry.grid(row=1, column=0, padx=16, pady=(4, 4), sticky="ew")
        self.tv_show_entry.bind("<Return>", lambda _: self._tv_search())

        self.tv_search_btn = ctk.CTkButton(
            sb, text="🔍  Search", fg_color=C_ACCENT2, hover_color="#0284c7",
            text_color="#0a0c10", font=("Segoe UI", 12, "bold"),
            height=34, corner_radius=8, command=self._tv_search)
        self.tv_search_btn.grid(row=2, column=0, padx=16, pady=(0, 6), sticky="ew")

        self.tv_result_var = ctk.StringVar(value="")
        self.tv_result_menu = ctk.CTkOptionMenu(
            sb, variable=self.tv_result_var, values=["— search first —"],
            fg_color=C_INPUT, button_color=C_BORDER,
            button_hover_color=C_ACCENT2, dropdown_fg_color=C_CARD,
            text_color=C_TEXT, font=("Consolas", 10),
            dropdown_font=("Consolas", 10), height=34, corner_radius=8,
            command=self._tv_show_selected)
        self.tv_result_menu.grid(row=3, column=0, padx=16, pady=(0, 4), sticky="ew")

        self.tv_show_lbl = ctk.CTkLabel(
            sb, text="No show loaded", font=("Consolas", 10),
            text_color=C_SUBTEXT, wraplength=220, justify="left")
        self.tv_show_lbl.grid(row=4, column=0, padx=16, pady=(0, 6), sticky="w")

        ctk.CTkFrame(sb, height=1, fg_color=C_BORDER).grid(
            row=5, column=0, sticky="ew", padx=12, pady=8)

        self._tv_sec(sb, 6, "② Video Folder")
        self.tv_dir_lbl = ctk.CTkLabel(
            sb, text="No folder selected", font=("Consolas", 10),
            text_color=C_SUBTEXT, wraplength=220, justify="left")
        self.tv_dir_lbl.grid(row=7, column=0, padx=16, pady=(0, 4), sticky="w")
        ctk.CTkButton(
            sb, text="Browse…", fg_color=C_ACCENT2, hover_color="#0284c7",
            text_color="#0a0c10", font=("Segoe UI", 12), height=34, corner_radius=8,
            command=self._tv_select_dir
        ).grid(row=8, column=0, padx=16, pady=(0, 6), sticky="ew")

        ctk.CTkFrame(sb, height=1, fg_color=C_BORDER).grid(
            row=9, column=0, sticky="ew", padx=12, pady=8)

        self._tv_sec(sb, 10, "Options")
        opts = ctk.CTkFrame(sb, fg_color="transparent")
        opts.grid(row=11, column=0, padx=16, pady=4, sticky="ew")
        self.tv_dry_var     = ctk.BooleanVar(value=True)
        self.tv_recurse_var = ctk.BooleanVar(value=True)
        self.tv_airdate_var = ctk.BooleanVar(value=True)
        for text, var in [("Dry Run (safe preview)", self.tv_dry_var),
                           ("Scan sub-folders",       self.tv_recurse_var),
                           ("Include air-date",        self.tv_airdate_var)]:
            ctk.CTkSwitch(opts, text=text, variable=var,
                          font=("Segoe UI", 11), text_color=C_TEXT,
                          progress_color=C_ACCENT, button_color=C_TEXT
                          ).pack(anchor="w", pady=3)
        self.tv_dry_var.trace_add("write", self._tv_update_badge)

        ctk.CTkFrame(sb, height=1, fg_color=C_BORDER).grid(
            row=12, column=0, sticky="ew", padx=12, pady=8)

        self.tv_start_btn = ctk.CTkButton(
            sb, text="▶  START RENAME",
            fg_color=C_ACCENT, hover_color="#34d399", text_color="#0a0c10",
            font=("Segoe UI", 14, "bold"), height=46, corner_radius=8,
            command=self._tv_run)
        self.tv_start_btn.grid(row=13, column=0, padx=16, pady=6, sticky="ew")
        ctk.CTkButton(
            sb, text="Clear Log", fg_color=C_CARD, hover_color=C_BORDER,
            text_color=C_SUBTEXT, font=("Segoe UI", 11), height=32, corner_radius=8,
            command=lambda: self._clear_console(self.tv_console)
        ).grid(row=14, column=0, padx=16, pady=(0, 4), sticky="ew")

        # Stats footer
        stats_card = ctk.CTkFrame(sb, fg_color=C_CARD, corner_radius=10)
        stats_card.grid(row=99, column=0, padx=12, pady=14, sticky="sew")
        ctk.CTkLabel(stats_card, text="Last Run Stats",
                     font=("Segoe UI", 11, "bold"), text_color=C_SUBTEXT).pack(pady=(10, 4))
        inner = ctk.CTkFrame(stats_card, fg_color="transparent")
        inner.pack(padx=12, pady=(0, 12))
        self.tv_stat_renamed = self._stat_pill(inner, "0", "Renamed", C_SUCCESS)
        self.tv_stat_skipped = self._stat_pill(inner, "0", "Skipped", C_WARN)
        self.tv_stat_errors  = self._stat_pill(inner, "0", "Errors",  C_ERROR)

        # ── Main log area ─────────────────────────────────────────────────
        main = ctk.CTkFrame(parent, fg_color="transparent")
        main.grid(row=0, column=1, sticky="nsew")
        main.grid_columnconfigure(0, weight=1)
        main.grid_rowconfigure(1, weight=1)

        hrow = ctk.CTkFrame(main, fg_color="transparent")
        hrow.grid(row=0, column=0, sticky="ew", pady=(4, 10))
        ctk.CTkLabel(hrow, text="Activity Log",
                     font=("Georgia", 19, "bold"), text_color=C_TEXT).pack(side="left")
        self.tv_mode_badge = ctk.CTkLabel(
            hrow, text="  DRY RUN  ", font=("Consolas", 11, "bold"),
            text_color=C_BG, fg_color=C_WARN, corner_radius=6)
        self.tv_mode_badge.pack(side="left", padx=12)

        self.tv_console = ctk.CTkTextbox(
            main, font=("Consolas", 12), fg_color=C_CARD, text_color=C_TEXT,
            border_color=C_BORDER, border_width=1, corner_radius=10,
            scrollbar_button_color=C_BORDER, wrap="none")
        self.tv_console.grid(row=1, column=0, sticky="nsew")

        self.tv_progress = ctk.CTkProgressBar(main, height=6,
                                              progress_color=C_ACCENT, fg_color=C_BORDER)
        self.tv_progress.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        self.tv_progress.set(0)

        self._tv_log("┌─────────────────────────────────────────────────────┐")
        self._tv_log("│  TV Show → Jellyfin / Plex / Emby Renamer           │")
        self._tv_log("│  1. Search show  2. Pick result  3. Select folder   │")
        self._tv_log("│  4. Dry Run to preview  5. Go Live to rename        │")
        self._tv_log("└─────────────────────────────────────────────────────┘\n")

    def _tv_sec(self, parent, row, text):
        ctk.CTkLabel(parent, text=text.upper(), font=("Segoe UI", 10, "bold"),
                     text_color=C_SUBTEXT).grid(row=row, column=0, padx=16, pady=(6, 2), sticky="w")

    def _stat_pill(self, parent, value, label, color):
        f = ctk.CTkFrame(parent, fg_color="transparent")
        f.pack(side="left", padx=6, pady=4)
        v = ctk.CTkLabel(f, text=value, font=("Segoe UI", 22, "bold"), text_color=color)
        v.pack()
        ctk.CTkLabel(f, text=label, font=("Segoe UI", 10), text_color=C_SUBTEXT).pack()
        return v

    def _tv_log(self, text):
        self.tv_console.insert("end", text + "\n")
        self.tv_console.see("end")
        push_log("tv", text)

    def _tv_update_badge(self, *_):
        if self.tv_dry_var.get():
            self.tv_mode_badge.configure(text="  DRY RUN  ", fg_color=C_WARN)
        else:
            self.tv_mode_badge.configure(text="  ⚠ LIVE  ", fg_color=C_ERROR)
        _state["tv"]["mode"] = "dry_run" if self.tv_dry_var.get() else "live"
        write_status(_state)

    def _tv_search(self):
        query = self.tv_show_entry.get().strip()
        if not query:
            return
        self.tv_search_btn.configure(state="disabled", text="⏳ Searching…")
        self._tv_log(f"🔍  Searching: {query}")
        threading.Thread(target=self._tv_search_thread, args=(query,), daemon=True).start()

    def _tv_search_thread(self, query):
        try:
            results = search_shows(query)
            self.tv_search_res = results
            if not results:
                self._tv_log("  ⚠  No results found.")
                self.tv_result_menu.configure(values=["— no results —"])
                self.tv_result_var.set("— no results —")
            else:
                labels = [r[0] for r in results]
                self.tv_result_menu.configure(values=labels)
                self.tv_result_var.set(labels[0])
                # Don't auto-load — let the user pick first, then they
                # click the dropdown which triggers _tv_show_selected
                self._tv_log(f"  ✅  {len(results)} result(s) — pick one from the dropdown.")
        except Exception as e:
            self._tv_log(f"  ❌  Search failed: {e}")
        finally:
            self.tv_search_btn.configure(state="normal", text="🔍  Search")

    def _tv_show_selected(self, label):
        for lbl, sid, sname, syear in self.tv_search_res:
            if lbl == label:
                self.tv_show_id   = sid
                self.tv_show_name = sname
                self.tv_show_year = syear
                self.tv_show_lbl.configure(text="Loading episodes…", text_color=C_WARN)
                threading.Thread(target=self._tv_load_eps, daemon=True).start()
                break

    def _tv_load_eps(self):
        try:
            self.tv_episode_map, self.tv_abs_map = fetch_episodes(self.tv_show_id)
            count = len(self.tv_episode_map)
            self.tv_show_lbl.configure(
                text=f"✅  {self.tv_show_name}  ({self.tv_show_year})\n    {count} episodes",
                text_color=C_SUCCESS)
            self._tv_log(f'  📺  Loaded {count} episodes for "{self.tv_show_name}"')
            _state["tv"]["show"] = self.tv_show_name
            _state["tv"]["year"] = self.tv_show_year
            write_status(_state)
        except Exception as e:
            self.tv_show_lbl.configure(text="❌  Failed", text_color=C_ERROR)
            self._tv_log(f"  ❌  Episode fetch failed: {e}")

    def _tv_select_dir(self):
        p = filedialog.askdirectory()
        if p:
            self.tv_path = p
            short = ("…" + p[-34:]) if len(p) > 37 else p
            self.tv_dir_lbl.configure(text=short, text_color=C_ACCENT)
            self._tv_log(f"📁  Folder: {p}")
            _state["tv"]["folder"] = p
            write_status(_state)

    def _tv_run(self):
        if not self.tv_show_id or (not self.tv_episode_map and not self.tv_abs_map):
            messagebox.showerror("No show loaded", "Search for a show first.")
            return
        if not self.tv_path:
            messagebox.showerror("No folder", "Select a folder first.")
            return
        self.tv_start_btn.configure(state="disabled", text="⏳ Running…")
        threading.Thread(target=self._tv_process, daemon=True).start()

    def _tv_process(self):
        dry     = self.tv_dry_var.get()
        recurse = self.tv_recurse_var.get()
        airdate = self.tv_airdate_var.get()
        renamed = skipped = errors = 0
        _state["tv"]["running"] = True
        _state["tv"]["progress"] = 0
        write_status(_state)

        mode_str = "DRY RUN" if dry else "⚠ LIVE"
        self._tv_log(f"\n{'─'*56}")
        self._tv_log(f"  Show  : {self.tv_show_name}  ({self.tv_show_year})")
        self._tv_log(f"  Mode  : {mode_str}")
        self._tv_log(f"{'─'*56}\n")

        all_files = collect_video_files(self.tv_path, recurse)
        total = len(all_files)
        self._tv_log(f"  Found {total} video file(s)\n")

        for idx, (dirpath, fname) in enumerate(all_files, 1):
            pct = idx / max(total, 1)
            self.tv_progress.set(pct)
            _state["tv"]["progress"] = round(pct * 100)
            write_status(_state)

            detected = detect_episode(fname)
            if detected is None:
                self._tv_log(f"  ⏭  SKIP (no episode #): {fname}")
                skipped += 1
                continue

            season, ep = detected
            ext = fname.rsplit('.', 1)[-1]
            in_season_map = (season, ep) in self.tv_episode_map
            in_abs_map    = ep in self.tv_abs_map
            if not in_season_map and not in_abs_map:
                self._tv_log(f"  ⚠  S{season:02d}E{ep:03d} not found in TVmaze data")
            elif not in_season_map and in_abs_map:
                self._tv_log(f"  ℹ  S{season:02d}E{ep:03d} matched by absolute ep# {ep}")

            new_name = build_new_name(
                self.tv_show_name, self.tv_episode_map, self.tv_abs_map,
                season, ep, ext, airdate)
            old_p = os.path.join(dirpath, fname)
            new_p = os.path.join(dirpath, new_name)

            if old_p == new_p:
                self._tv_log(f"  ✔  ALREADY OK: {fname}")
                skipped += 1
                continue

            rel_dir    = os.path.relpath(dirpath, self.tv_path)
            folder_tag = f"[{rel_dir}]  " if rel_dir != "." else ""

            if dry:
                self._tv_log(f"  →  {folder_tag}{new_name}")
                renamed += 1
            else:
                try:
                    os.rename(old_p, new_p)
                    self._tv_log(f"  ✅ {folder_tag}{new_name}")
                    renamed += 1
                except Exception as e:
                    self._tv_log(f"  ❌ ERROR: {fname} — {e}")
                    errors += 1

        self.tv_progress.set(1.0)
        self._tv_log(f"\n{'─'*56}")
        self._tv_log(f"  Done! Renamed: {renamed}  Skipped: {skipped}  Errors: {errors}")
        self._tv_log(f"{'─'*56}\n")

        self.tv_stat_renamed.configure(text=str(renamed))
        self.tv_stat_skipped.configure(text=str(skipped))
        self.tv_stat_errors.configure(text=str(errors))
        self.tv_start_btn.configure(state="normal", text="▶  START RENAME")

        _state["tv"]["running"]  = False
        _state["tv"]["progress"] = 100
        _state["tv"]["stats"]    = {"renamed": renamed, "skipped": skipped, "errors": errors}
        write_status(_state)

    # ─────────────────────────────────────────────────────────────────────
    # MKV TAB
    # ─────────────────────────────────────────────────────────────────────
    def _build_mkv_tab(self, parent):
        parent.grid_columnconfigure(0, weight=0)
        parent.grid_columnconfigure(1, weight=1)
        parent.grid_rowconfigure(0, weight=1)

        # ── Left: file list ───────────────────────────────────────────────
        left = ctk.CTkFrame(parent, width=260, fg_color=C_SIDEBAR, corner_radius=12)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        left.grid_propagate(False)
        left.grid_columnconfigure(0, weight=1)
        left.grid_rowconfigure(3, weight=1)

        ctk.CTkLabel(left, text="INPUT FILES", font=("Segoe UI", 10, "bold"),
                     text_color=C_SUBTEXT).grid(row=0, column=0, padx=14, pady=(12, 4), sticky="w")

        btn_row = ctk.CTkFrame(left, fg_color="transparent")
        btn_row.grid(row=1, column=0, padx=14, pady=(0, 6), sticky="ew")
        for text, cmd, color in [("+ Files", self._mkv_add_files, C_ACCENT2),
                                  ("📁 Folder", self._mkv_add_folder, C_CARD),
                                  ("✕ Clear", self._mkv_clear_files, C_CARD)]:
            ctk.CTkButton(
                btn_row, text=text, fg_color=color, hover_color=C_BORDER,
                text_color="#0a0c10" if color == C_ACCENT2 else C_SUBTEXT,
                font=("Segoe UI", 11), height=30, corner_radius=6, width=70,
                command=cmd
            ).pack(side="left", padx=(0, 4))

        self.mkv_listbox_frame = ctk.CTkScrollableFrame(
            left, fg_color=C_CARD, corner_radius=8)
        self.mkv_listbox_frame.grid(row=3, column=0, padx=14, pady=(0, 6), sticky="nsew")
        self.mkv_listbox_frame.grid_columnconfigure(0, weight=1)
        self.mkv_file_labels = []

        self.mkv_file_count_lbl = ctk.CTkLabel(
            left, text="No files", font=("Segoe UI", 10), text_color=C_SUBTEXT)
        self.mkv_file_count_lbl.grid(row=4, column=0, padx=14, pady=(0, 4), sticky="w")

        ctk.CTkButton(
            left, text="⟳  Scan / Refresh Tracks",
            fg_color="#1a2040", hover_color="#233060", text_color=C_ACCENT2,
            font=("Segoe UI", 12, "bold"), height=36, corner_radius=8,
            command=self._mkv_scan
        ).grid(row=5, column=0, padx=14, pady=(0, 10), sticky="ew")

        # Tools status
        tool_ok = MKVMERGE and MKVPROPEDIT
        ctk.CTkLabel(
            left,
            text=("✅ MKVToolNix found" if tool_ok else "❌ MKVToolNix missing — install it"),
            font=("Consolas", 10),
            text_color=C_SUCCESS if tool_ok else C_ERROR
        ).grid(row=6, column=0, padx=14, pady=(0, 10), sticky="w")

        # ── Right: tracks + options + log ─────────────────────────────────
        right = ctk.CTkFrame(parent, fg_color="transparent")
        right.grid(row=0, column=1, sticky="nsew")
        right.grid_columnconfigure(0, weight=1)
        right.grid_rowconfigure(2, weight=1)

        # Tracks area
        tracks = ctk.CTkFrame(right, fg_color="transparent")
        tracks.grid(row=0, column=0, sticky="ew")
        tracks.grid_columnconfigure(0, weight=1)
        tracks.grid_columnconfigure(1, weight=1)

        # Audio column
        af = ctk.CTkFrame(tracks, fg_color=C_CARD, corner_radius=10)
        af.grid(row=0, column=0, sticky="nsew", padx=(0, 6), pady=(0, 8))
        af.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(af, text="AUDIO TRACKS", font=("Segoe UI", 10, "bold"),
                     text_color=C_SUBTEXT).grid(row=0, column=0, padx=12, pady=(10, 4), sticky="w")
        self.mkv_audio_sf = ctk.CTkScrollableFrame(af, height=140, fg_color="transparent")
        self.mkv_audio_sf.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 8))
        self.mkv_audio_sf.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(self.mkv_audio_sf, text="Scan files to see tracks",
                     font=("Segoe UI", 10), text_color=C_SUBTEXT).pack(anchor="w", pady=6)

        # Sub column
        sf_col = ctk.CTkFrame(tracks, fg_color=C_CARD, corner_radius=10)
        sf_col.grid(row=0, column=1, sticky="nsew", pady=(0, 8))
        sf_col.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(sf_col, text="SUBTITLE TRACKS", font=("Segoe UI", 10, "bold"),
                     text_color=C_SUBTEXT).grid(row=0, column=0, padx=12, pady=(10, 4), sticky="w")
        self.mkv_sub_sf = ctk.CTkScrollableFrame(sf_col, height=140, fg_color="transparent")
        self.mkv_sub_sf.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 8))
        self.mkv_sub_sf.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(self.mkv_sub_sf, text="Scan files to see tracks",
                     font=("Segoe UI", 10), text_color=C_SUBTEXT).pack(anchor="w", pady=6)

        # Partial info
        self.mkv_partial_lbl = ctk.CTkLabel(
            right, text="", font=("Segoe UI", 10), text_color="#a78bfa",
            wraplength=700, justify="left")
        self.mkv_partial_lbl.grid(row=1, column=0, sticky="ew", padx=4, pady=(0, 4))

        # Options + log
        bot = ctk.CTkFrame(right, fg_color="transparent")
        bot.grid(row=2, column=0, sticky="nsew")
        bot.grid_columnconfigure(0, weight=1)
        bot.grid_rowconfigure(1, weight=1)

        opt_row = ctk.CTkFrame(bot, fg_color=C_CARD, corner_radius=10)
        opt_row.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        opt_row.grid_columnconfigure(3, weight=1)

        ctk.CTkCheckBox(
            opt_row, text="Move to output folder", variable=self.mkv_move_files,
            font=("Segoe UI", 11), text_color=C_TEXT,
            fg_color=C_ACCENT, hover_color="#34d399",
            command=self._mkv_toggle_out
        ).grid(row=0, column=0, padx=12, pady=10, sticky="w")

        ctk.CTkLabel(opt_row, text="Output:", font=("Segoe UI", 11),
                     text_color=C_SUBTEXT).grid(row=0, column=1, padx=(16, 4), pady=10)
        self.mkv_out_entry = ctk.CTkEntry(
            opt_row, textvariable=self.mkv_out_folder,
            fg_color=C_INPUT, border_color=C_BORDER, text_color=C_TEXT,
            font=("Consolas", 11), height=32, corner_radius=6, state="disabled")
        self.mkv_out_entry.grid(row=0, column=2, padx=(0, 6), pady=10, sticky="ew")
        ctk.CTkButton(
            opt_row, text="Browse", fg_color=C_SIDEBAR, hover_color=C_BORDER,
            text_color=C_SUBTEXT, font=("Segoe UI", 11), height=32, corner_radius=6, width=80,
            command=self._mkv_browse_out
        ).grid(row=0, column=3, padx=(0, 6), pady=10)

        self.mkv_start_btn = ctk.CTkButton(
            opt_row, text="▶  START", fg_color=C_ACCENT, hover_color="#34d399",
            text_color="#0a0c10", font=("Segoe UI", 13, "bold"),
            height=36, corner_radius=8, width=110,
            command=lambda: self._mkv_process(dry=False))
        self.mkv_start_btn.grid(row=0, column=4, padx=6, pady=10)
        ctk.CTkButton(
            opt_row, text="Dry Run", fg_color=C_CARD, hover_color=C_BORDER,
            text_color=C_WARN, font=("Segoe UI", 12), height=36, corner_radius=8, width=90,
            command=lambda: self._mkv_process(dry=True)
        ).grid(row=0, column=5, padx=(0, 12), pady=10)

        # Log
        log_hdr = ctk.CTkFrame(bot, fg_color="transparent")
        log_hdr.grid(row=1, column=0, sticky="new")
        ctk.CTkLabel(log_hdr, text="Activity Log",
                     font=("Georgia", 16, "bold"), text_color=C_TEXT).pack(side="left")
        ctk.CTkButton(
            log_hdr, text="Clear", fg_color=C_CARD, hover_color=C_BORDER,
            text_color=C_SUBTEXT, font=("Segoe UI", 11), height=26, corner_radius=6, width=60,
            command=lambda: self._clear_console(self.mkv_console)
        ).pack(side="right")

        self.mkv_console = ctk.CTkTextbox(
            bot, font=("Consolas", 11), fg_color=C_CARD, text_color=C_TEXT,
            border_color=C_BORDER, border_width=1, corner_radius=10,
            scrollbar_button_color=C_BORDER, wrap="none")
        self.mkv_console.grid(row=2, column=0, sticky="nsew", pady=(4, 0))

        self.mkv_progress = ctk.CTkProgressBar(
            bot, height=6, progress_color=C_ACCENT, fg_color=C_BORDER)
        self.mkv_progress.grid(row=3, column=0, sticky="ew", pady=(6, 0))
        self.mkv_progress.set(0)

        # Stats footer row
        stats_row = ctk.CTkFrame(bot, fg_color=C_CARD, corner_radius=10)
        stats_row.grid(row=4, column=0, sticky="ew", pady=(8, 0))
        ctk.CTkLabel(stats_row, text="Last Run:", font=("Segoe UI", 11, "bold"),
                     text_color=C_SUBTEXT).pack(side="left", padx=12, pady=8)
        self.mkv_stat_full    = self._mkv_stat(stats_row, "0", "Full",    C_SUCCESS)
        self.mkv_stat_partial = self._mkv_stat(stats_row, "0", "Partial", "#a78bfa")
        self.mkv_stat_skipped = self._mkv_stat(stats_row, "0", "Skipped", C_WARN)
        self.mkv_stat_failed  = self._mkv_stat(stats_row, "0", "Failed",  C_ERROR)

        self._mkv_log("MKV Track Forge ready. Add files, scan tracks, then START.")
        if not MKVMERGE or not MKVPROPEDIT:
            self._mkv_log("⚠  MKVToolNix not found. Install from https://mkvtoolnix.download")

    def _mkv_stat(self, parent, val, label, color):
        f = ctk.CTkFrame(parent, fg_color="transparent")
        f.pack(side="left", padx=10, pady=8)
        v = ctk.CTkLabel(f, text=val, font=("Segoe UI", 18, "bold"), text_color=color)
        v.pack()
        ctk.CTkLabel(f, text=label, font=("Segoe UI", 9), text_color=C_SUBTEXT).pack()
        return v

    def _mkv_log(self, text, level="info"):
        self.mkv_console.insert("end", text + "\n")
        self.mkv_console.see("end")
        push_log("mkv", text, level)

    def _mkv_add_files(self):
        paths = filedialog.askopenfilenames(
            title="Select MKV files",
            filetypes=[("MKV files", "*.mkv"), ("All files", "*.*")])
        for p in paths:
            if p not in self.mkv_file_paths:
                self.mkv_file_paths.append(p)
                self._mkv_add_label(Path(p).name)
        self._mkv_update_count()

    def _mkv_add_folder(self):
        folder = filedialog.askdirectory(title="Select folder with MKV files")
        if not folder:
            return
        added = 0
        for f in sorted(Path(folder).rglob("*.mkv")):
            p = str(f)
            if p not in self.mkv_file_paths:
                self.mkv_file_paths.append(p)
                self._mkv_add_label(f.name)
                added += 1
        self._mkv_log(f"Added {added} file(s) from {folder}")
        self._mkv_update_count()

    def _mkv_add_label(self, name):
        lbl = ctk.CTkLabel(
            self.mkv_listbox_frame, text=f"  {name}",
            font=("Consolas", 10), text_color=C_TEXT,
            anchor="w", fg_color="transparent")
        lbl.grid(row=len(self.mkv_file_labels), column=0, sticky="ew", pady=1)
        self.mkv_file_labels.append(lbl)

    def _mkv_clear_files(self):
        SCAN_CACHE.clear()
        self.mkv_file_paths.clear()
        self.mkv_file_data.clear()
        for lbl in self.mkv_file_labels:
            lbl.destroy()
        self.mkv_file_labels.clear()
        self._mkv_update_count()
        for sf in [self.mkv_audio_sf, self.mkv_sub_sf]:
            for w in sf.winfo_children():
                w.destroy()
            ctk.CTkLabel(sf, text="Scan files to see tracks",
                         font=("Segoe UI", 10), text_color=C_SUBTEXT).pack(anchor="w", pady=6)

    def _mkv_update_count(self):
        n = len(self.mkv_file_paths)
        self.mkv_file_count_lbl.configure(text=f"{n} file{'s' if n != 1 else ''} queued")
        _state["mkv"]["files"] = [Path(p).name for p in self.mkv_file_paths]
        write_status(_state)

    def _mkv_toggle_out(self):
        self.mkv_out_entry.configure(
            state="normal" if self.mkv_move_files.get() else "disabled")

    def _mkv_browse_out(self):
        folder = filedialog.askdirectory(title="Select output folder")
        if folder:
            self.mkv_out_folder.set(folder)
            self.mkv_move_files.set(True)
            self._mkv_toggle_out()

    def _mkv_scan(self):
        if not self.mkv_file_paths:
            messagebox.showinfo("No Files", "Add files first.")
            return
        if not MKVMERGE:
            messagebox.showerror("Missing Tool", "mkvmerge not found.")
            return
        self._mkv_log("Scanning tracks…")
        threading.Thread(target=self._mkv_scan_thread, daemon=True).start()

    def _mkv_scan_thread(self):
        self.mkv_file_data.clear()
        total = len(self.mkv_file_paths)
        completed = [0]  # mutable counter shared across futures

        with ThreadPoolExecutor(max_workers=min(8, max(1, total))) as ex:
            futures = {ex.submit(identify_mkv, p): p for p in self.mkv_file_paths}

            for future in as_completed(futures):
                p = futures[future]
                try:
                    self.mkv_file_data[p] = future.result()
                except Exception:
                    self.mkv_file_data[p] = None

                completed[0] += 1
                pct = completed[0] / total
                # Batch UI updates: only fire every 3rd completion or on last file
                if completed[0] % 3 == 0 or completed[0] == total:
                    _pct = pct
                    _name = Path(p).name
                    self.mkv_progress.set(_pct)
                    _state["mkv"]["progress"] = round(_pct * 100)
                    write_status(_state)

        self._mkv_populate_tracks()
        self.mkv_progress.set(0)
        self._mkv_log(f"Scan complete: {total} file(s) — cache size: {len(SCAN_CACHE)}")

    def _mkv_populate_tracks(self):
        loaded = [v for v in self.mkv_file_data.values() if v]
        a_com, a_par = find_common_tracks(loaded, "audio")
        s_com, s_par = find_common_tracks(loaded, "subtitles")
        self.mkv_audio_common  = a_com
        self.mkv_audio_partial = a_par
        self.mkv_sub_common    = s_com
        self.mkv_sub_partial   = s_par
        n_total = len(loaded)

        # Expose to dashboard
        _state["mkv"]["audio_tracks"] = [lbl for _, lbl, _ in a_com + a_par]
        _state["mkv"]["sub_tracks"]   = [lbl for _, lbl, _ in s_com + s_par]
        write_status(_state)

        def build(scroll_frame, common, partial, sel_var, kind):
            for w in scroll_frame.winfo_children():
                w.destroy()
            if not common and not partial:
                ctk.CTkLabel(scroll_frame, text=f"No {kind} tracks found",
                             font=("Segoe UI", 10), text_color=C_SUBTEXT).pack(anchor="w", pady=6)
                return
            rb_kw = dict(fg_color=C_ACCENT, hover_color="#34d399",
                         text_color=C_TEXT, font=("Consolas", 10), anchor="w")
            ctk.CTkRadioButton(scroll_frame, text="(no change)",
                               variable=sel_var, value="",
                               **rb_kw).pack(anchor="w", pady=2, padx=4)
            if common:
                ctk.CTkLabel(scroll_frame, text="● In ALL files:",
                             font=("Segoe UI", 9, "bold"),
                             text_color=C_SUCCESS).pack(anchor="w", padx=4, pady=(4, 0))
                for k, lbl, cnt in common:
                    ctk.CTkRadioButton(scroll_frame, text=f"  {lbl}",
                                       variable=sel_var, value=key_to_str(k),
                                       **rb_kw).pack(anchor="w", pady=1, padx=12)
            if partial:
                ctk.CTkLabel(scroll_frame,
                             text=f"◌ Partial (→ adds '{SPECIAL_SUFFIX}' suffix):",
                             font=("Segoe UI", 9, "bold"),
                             text_color="#a78bfa").pack(anchor="w", padx=4, pady=(6, 0))
                for k, lbl, cnt in partial:
                    ctk.CTkRadioButton(scroll_frame,
                                       text=f"  {lbl}  ({cnt}/{n_total})",
                                       variable=sel_var, value=key_to_str(k),
                                       **rb_kw).pack(anchor="w", pady=1, padx=12)

        build(self.mkv_audio_sf, a_com, a_par, self.mkv_sel_audio, "audio")
        build(self.mkv_sub_sf,   s_com, s_par, self.mkv_sel_sub,   "subtitle")
        self._mkv_update_highlights()

    def _mkv_update_highlights(self):
        audio_key = str_to_key(self.mkv_sel_audio.get())
        sub_key   = str_to_key(self.mkv_sel_sub.get())
        audio_miss = (files_missing_track(
            self.mkv_file_paths, self.mkv_file_data, audio_key, "audio")
            if audio_key else set())
        sub_miss   = (files_missing_track(
            self.mkv_file_paths, self.mkv_file_data, sub_key, "subtitles")
            if sub_key else set())
        both_miss = audio_miss & sub_miss
        only_a    = audio_miss - sub_miss
        only_s    = sub_miss   - audio_miss
        partial_n = len(only_a) + len(only_s)
        skip_n    = len(both_miss)

        # Update file label colors
        for i, p in enumerate(self.mkv_file_paths):
            am = p in audio_miss
            sm = p in sub_miss
            if i < len(self.mkv_file_labels):
                if am and sm:
                    self.mkv_file_labels[i].configure(text_color=C_ERROR)
                elif am or sm:
                    self.mkv_file_labels[i].configure(text_color=C_WARN)
                else:
                    self.mkv_file_labels[i].configure(text_color=C_TEXT)

        lines = []
        if partial_n:
            lines.append(f"⚡ {partial_n} file(s) → partial: available track(s) set + '{SPECIAL_SUFFIX}' appended")
        if skip_n:
            lines.append(f"⛔ {skip_n} file(s) → SKIPPED (missing both tracks)")
        self.mkv_partial_lbl.configure(text="\n".join(lines))

    def _mkv_process(self, dry=False):
        if not self.mkv_file_paths:
            messagebox.showinfo("No Files", "Add files first.")
            return
        audio_key = str_to_key(self.mkv_sel_audio.get())
        sub_key   = str_to_key(self.mkv_sel_sub.get())
        if not audio_key and not sub_key:
            messagebox.showinfo("Nothing Selected", "Select at least one audio or subtitle track.")
            return
        if self.mkv_move_files.get() and not self.mkv_out_folder.get():
            messagebox.showwarning("No Output Folder", "Set an output folder or uncheck 'Move files'.")
            return
        self.mkv_start_btn.configure(state="disabled", text="⏳ Processing…")
        threading.Thread(
            target=self._mkv_process_thread,
            args=(audio_key, sub_key, dry),
            daemon=True
        ).start()

    def _mkv_process_thread(self, audio_key, sub_key, dry):
        counts = {"full": 0, "partial": 0, "skipped": 0, "failed": 0}
        _state["mkv"]["running"] = True
        write_status(_state)
        total = len(self.mkv_file_paths)
        out_dir = Path(self.mkv_out_folder.get()) if self.mkv_move_files.get() else None
        if out_dir and not dry:
            out_dir.mkdir(parents=True, exist_ok=True)

        self._mkv_log(f"─── {'DRY RUN' if dry else 'Processing'} ───")

        for i, p in enumerate(self.mkv_file_paths):
            name = Path(p).name
            fd   = self.mkv_file_data.get(p)
            pct  = (i + 1) / total
            self.mkv_progress.set(pct)
            _state["mkv"]["progress"] = round(pct * 100)
            write_status(_state)

            def has_key(key, ttype, _fd=fd):
                if not key or not _fd:
                    return False
                return any(track_key(t) == key
                           for t in _fd.get("tracks", [])
                           if t.get("type") == ttype)

            has_audio = has_key(audio_key, "audio")
            has_sub   = has_key(sub_key, "subtitles")
            wants_audio   = audio_key is not None
            wants_sub     = sub_key   is not None
            missing_audio = wants_audio and not has_audio
            missing_sub   = wants_sub   and not has_sub
            missing_both  = missing_audio and missing_sub

            if missing_both:
                counts["skipped"] += 1
                self._mkv_log(f"  ⛔ SKIP  {name}  (missing both tracks)", "warn")
            else:
                is_partial  = missing_audio or missing_sub
                apply_audio = audio_key if has_audio else None
                apply_sub   = sub_key   if has_sub   else None

                if dry:
                    tag  = "PARTIAL" if is_partial else "FULL"
                    note = f"  → title+={SPECIAL_SUFFIX!r}" if is_partial else ""
                    self._mkv_log(f"  [DRY/{tag}] {name}{note}")
                    counts["partial" if is_partial else "full"] += 1
                else:
                    ok, msg = set_defaults(p, apply_audio, apply_sub,
                                           append_special=is_partial)
                    sym = "⚡" if (ok and is_partial) else "✅" if ok else "❌"
                    counts["partial" if (ok and is_partial) else
                           "full"    if ok else "failed"] += 1
                    self._mkv_log(f"  {sym} {name}  —  {msg}")
                    if ok and out_dir:
                        try:
                            shutil.move(p, out_dir / Path(p).name)
                        except Exception as e:
                            self._mkv_log(f"    ⚠ move failed: {e}", "warn")

        label = "Dry run" if dry else "Done"
        self._mkv_log(
            f"─── {label}: {counts['full']} full  "
            f"{counts['partial']} partial  "
            f"{counts['skipped']} skipped  "
            f"{counts['failed']} failed ───")

        self.mkv_stat_full.configure(text=str(counts["full"]))
        self.mkv_stat_partial.configure(text=str(counts["partial"]))
        self.mkv_stat_skipped.configure(text=str(counts["skipped"]))
        self.mkv_stat_failed.configure(text=str(counts["failed"]))
        self.mkv_start_btn.configure(state="normal", text="▶  START")
        self.mkv_progress.set(0)

        _state["mkv"]["running"] = False
        _state["mkv"]["stats"]   = counts
        write_status(_state)

    # ─────────────────────────────────────────────────────────────────────
    # SHARED HELPERS
    # ─────────────────────────────────────────────────────────────────────
    def _clear_console(self, widget):
        widget.delete("1.0", "end")


# ═══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    write_status(_state)
    app = MediaForge()
    app.mainloop()
