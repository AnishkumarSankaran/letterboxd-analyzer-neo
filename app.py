"""
╔══════════════════════════════════════════════════════════════════════╗
║  LETTERBOXD PROFILE ANALYZER — V9 NEO-BRUTALIST COMIC EDITION       ║
║                                                                      ║
║  V9 FIXES vs V8:                                                     ║
║  ✅ UI: Neo-Brutalist Comic — CMYK palette, hard shadows, Chivo      ║
║  ✅ ZERO transform:rotate, ZERO border-radius, ZERO slanting         ║
║  ✅ Artist cards: RECTANGULAR (aspect-ratio 3/4), never circles      ║
║  ✅ AVG RATING: reads from ratings.csv, not watched.csv              ║
║  ✅ GRID COUNTS: 4 newest, 4 oldest, 4 crowd favs, 4 hidden gems    ║
║  ✅ REWATCH FIX: group by (Name,Year) — Superman 1978 ≠ 2025        ║
║  ✅ PLOTLY FULLSCREEN: plot_bgcolor=white, theme=None, no white-on-  ║
║       white invisible text bug in full screen mode                  ║
║  ✅ ROULETTE PERF: pre-built pool in session_state, sample() only   ║
║  ✅ FUNNY LOADERS: custom messages instead of default spinners       ║
║  ✅ VECTORISED DATA: explode() replaces iterrows() loops             ║
║  ✅ @st.cache_data on all heavy analysis functions                   ║
║  ✅ Map tab footer: "VIBECODED WITH CLAUDE"                          ║
║  ✅ Straight badges: transform:none, never rotated                   ║
║                                                                      ║
║  Performance: handles 10,000+ films fast.                           ║
╚══════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import html as _html
import json
import os
import random as _rnd
import sys
import traceback
import zipfile
from datetime import datetime as _dt_now, timedelta as _td
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

_app_dir = os.path.dirname(os.path.abspath(__file__))
if _app_dir not in sys.path:
    sys.path.insert(0, _app_dir)

from database import get_database, _make_cache_key
from tmdb_async import fetch_movies_with_progress
from data_processing import (
    analyze_countries,
    analyze_genres,
    analyze_languages,
    analyze_movies_per_day,
    analyze_movies_per_month,
    calculate_total_hours,
    drop_unused_columns,
    extract_year_from_title,
    get_first_and_last_film,
    get_highly_rated_unseen,
    get_milestones,
    get_most_watched_films,
    get_newest_films,
    get_oldest_films,
    get_recently_watched,
    get_top_people,
    get_top_people_with_images,
    optimise_dtypes,
)
from visualization import (
    COUNTRY_NAME_MAP,
    CMYK,
    _divider,
    _section_header,
    create_world_map,
    display_brutalist_title,
    display_film_grid,
    display_film_grid_large,
    display_global_footer,
    display_map_footer,
    display_metric_card,
    display_milestone_card,
    display_people_with_images,
    display_recently_watched,
    display_rewatch_counts,
    display_tab_easter_egg,
    display_top_countries,
    display_top_list,
    display_watchlist_roulette,
    load_custom_css,
    plot_bar_chart,
    random_loader,
    reset_metric_counter,
)

# ── Optional Sentry ────────────────────────────────────────────────────────────
try:
    import sentry_sdk
    _dsn = st.secrets.get("SENTRY_DSN", "") if hasattr(st, "secrets") else ""
    if _dsn:
        sentry_sdk.init(dsn=_dsn, traces_sample_rate=0.2)
except Exception:
    sentry_sdk = None  # type: ignore


def _capture(ex: Exception) -> None:
    if sentry_sdk:
        try:
            sentry_sdk.capture_exception(ex)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Letterboxd Analyzer — V9 NEO-BRUTAL",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded",
)
load_custom_css()

# ─────────────────────────────────────────────────────────────────
# CONFIG — Centralised tunables (no more magic numbers)
# ─────────────────────────────────────────────────────────────────
CONFIG = {
    "MIN_VOTE_COUNT": 1000,       # Min TMDB votes for highest/lowest rated
    "REWATCH_TOP_N": 10,          # Max films shown in re-watched section
    "POSTER_GRID_COLS": 4,        # Default poster grid column count
    "TOP_ACTORS_N": 12,           # Number of top actors to display
    "TOP_DIRECTORS_N": 12,        # Number of top directors to display
    "RECENT_FILMS_N": 20,         # Recently watched list length
    "GRID_SMALL_N": 4,            # Small grid film count
    "GRID_LARGE_N": 10,           # Large grid film count (was 8)
    "TOP_LIST_N": 25,             # Top list entries (genres, languages)
    "TOP_COUNTRIES_N": 25,        # Countries in map leaderboard
    "RATED_DISPLAY_N": 10,        # Films in 5-star / lowest ratings grids
    "SAFE_NUM_COLS": ["vote_count", "runtime", "popularity", "vote_average", "tmdb_id"],
}

# ─────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────
db = get_database()

# ─────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        '<div style="font-family:Chivo,Impact,sans-serif;font-size:2.8rem;'
        'font-weight:900;color:#FFDE00;text-shadow:5px 5px 0 #000;'
        '-webkit-text-stroke:2px #000;margin-bottom:25px;line-height:1;">'
        'STATS<br>&amp;<br>SETTINGS</div>',
        unsafe_allow_html=True,
    )
    try:
        stats = db.get_cache_stats()
        st.metric("FILMS CACHED",  f"{stats.get('total_movies', 0):,}")
        st.metric("PEOPLE CACHED", f"{stats.get('total_people', 0):,}")
        st.metric("ADDED THIS WEEK", stats.get("recent_additions", 0))
    except Exception:
        st.metric("FILMS CACHED", "—")
        st.metric("PEOPLE CACHED", "—")
        st.metric("ADDED THIS WEEK", "—")

    # ✅ User counter + profile tracking
    if "_user_id" not in st.session_state:
        st.session_state["_user_id"] = _rnd.randint(1, 99999)

    _total_users = db.get_total_users()
    _user_jokes_anon = [
        "You could be touching grass. Yet here you are.",
        "Another soul consumed by the algorithm.",
        "A certified kino enjoyer has entered.",
        "Film Twitter's least dangerous member.",
        "Your Letterboxd feed called. It's worried.",
        "Somewhere, Criterion is weeping tears of joy.",
        "Welcome back, professional opinion-haver.",
    ]

    # Show username if profile data is loaded, otherwise show anonymous ID
    _profile = st.session_state.get("_user_profile")
    if _profile and _profile.get("username"):
        _uname = _html.escape(_profile["username"])
        _given = _html.escape(_profile.get("given_name", ""))
        _display = f"@{_uname}" if _uname else f"USER #{st.session_state['_user_id']:05d}"
        _name_jokes = [
            f"We see you, {_given or _uname}. We see everything.",
            f"{_given or _uname}'s taste is now under federal investigation.",
            f"{_given or _uname}: professional screen-starer since birth.",
            f"{_given or _uname} walked in. The vibes shifted.",
            f"Breaking: {_given or _uname} touches grass, immediately goes home.",
        ]
        _joke = _rnd.choice(_name_jokes)
    else:
        _display = f"USER #{st.session_state['_user_id']:05d}"
        _joke = _rnd.choice(_user_jokes_anon)

    _user_count_text = f'{_total_users:,} FILM NERDS HAVE USED THIS APP' if _total_users > 0 else 'YOU MIGHT BE THE FIRST'

    st.markdown(
        f'<div style="font-family:Space Grotesk,sans-serif;font-weight:800;'
        f'font-size:0.65rem;color:#FFF;background:#7B2D8E;border:3px solid #000;'
        f'padding:5px 10px;box-shadow:3px 3px 0 #000;margin-top:12px;'
        f'text-transform:uppercase;letter-spacing:.05em;">'
        f'📊 {_user_count_text}'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div style="font-family:Chivo,Impact,sans-serif;font-size:1.1rem;'
        f'font-weight:900;color:#000;background:#00E5FF;border:4px solid #000;'
        f'padding:10px 14px;box-shadow:5px 5px 0 #000;margin-top:4px;'
        f'text-transform:uppercase;">'
        f'👤 {_display}'
        f'</div>'
        f'<div style="font-family:Space Grotesk,sans-serif;font-weight:700;'
        f'font-size:0.72rem;color:#000;background:#FFDE00;border:3px solid #000;'
        f'border-top:none;padding:5px 10px;box-shadow:3px 3px 0 #000;'
        f'margin-bottom:8px;">'
        f'{_joke}'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Letterboxd export instructions ────────────────────────────
    st.markdown(
        '<div style="font-family:Space Grotesk,sans-serif;font-weight:800;'
        'font-size:0.72rem;color:#FFF;background:#FF003C;border:3px solid #000;'
        'padding:8px 12px;box-shadow:4px 4px 0 #000;margin-top:10px;'
        'line-height:1.7;text-transform:uppercase;letter-spacing:.04em;">'
        '📦 HOW TO GET YOUR DATA:<br>'
        'Log into Letterboxd → Settings → Data<br>'
        '→ EXPORT YOUR DATA → Download ZIP<br>'
        '→ Upload below'
        '</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div style="font-family:Space Grotesk,sans-serif;font-weight:700;'
        'font-size:0.75rem;color:#000;background:#FFDE00;border:3px solid #000;'
        'padding:8px 12px;box-shadow:4px 4px 0 #000;margin-top:15px;'
        'text-transform:uppercase;letter-spacing:.06em;line-height:2;">'
        'V9 NEO-BRUTAL EDITION<br>'
        'CMYK + HARD SHADOWS<br>'
        'ZERO RADIUS / ROTATE<br>'
        'RECTANGULAR ARTISTS<br>'
        'REMAKE-SAFE REWATCHES'
        '</div>',
        unsafe_allow_html=True,
    )

# ─────────────────────────────────────────────────────────────────
# TMDB KEY
# ─────────────────────────────────────────────────────────────────
try:
    TMDB_API_KEY: str = st.secrets["TMDB_API_KEY"]
except Exception:
    st.error(
        "⚠️ TMDB API key missing. "
        "Create `.streamlit/secrets.toml` and add `TMDB_API_KEY = \"your_key\"`."
    )
    st.stop()


# ─────────────────────────────────────────────────────────────────
# FILE UTILITIES
# ─────────────────────────────────────────────────────────────────
def _load_csv_bytes(raw: bytes, fname: str) -> pd.DataFrame:
    for enc in ("utf-8", "utf-8-sig", "iso-8859-1", "cp1252"):
        try:
            return pd.read_csv(StringIO(raw.decode(enc)))
        except (UnicodeDecodeError, Exception):
            continue
    st.error(f"Cannot decode '{fname}'.")
    return pd.DataFrame()


def process_zip_file(zip_file: Any) -> Dict[str, pd.DataFrame]:
    """Extract root-level CSVs from Letterboxd ZIP export.

    ✅ V9.1 FIX: Skip subdirectories like orphaned/ and deleted/.
    The ZIP contains diary.csv, orphaned/diary.csv, AND deleted/diary.csv.
    All have stem 'diary', so orphaned data would overwrite the real data.
    Only root-level CSVs (no '/' in path) are loaded.
    """
    import time
    t0 = time.time()
    dataframes: Dict[str, pd.DataFrame] = {}
    try:
        # ✅ SEC-2: Validate ZIP upload
        MAX_ZIP_FILES = 20
        MAX_CSV_BYTES = 50 * 1024 * 1024  # 50 MB per CSV
        with zipfile.ZipFile(zip_file) as zf:
            entries = [n for n in zf.namelist()
                       if n.lower().endswith(".csv") and "/" not in n]
            if len(entries) > MAX_ZIP_FILES:
                st.error(f"ZIP contains {len(entries)} root CSVs (max {MAX_ZIP_FILES}).")
                return dataframes
            for name in entries:
                info = zf.getinfo(name)
                if info.file_size > MAX_CSV_BYTES:
                    st.warning(f"Skipping {name}: {info.file_size/1e6:.1f} MB exceeds limit.")
                    continue
                with zf.open(name) as f:
                    df = _load_csv_bytes(f.read(), name)
                stem = Path(name).stem.lower()
                dataframes[stem] = df
        print(f"[TIME] process_zip_file: {time.time()-t0:.2f}s")
    except zipfile.BadZipFile:
        st.error("Invalid ZIP file.")
    except Exception as ex:
        st.error(f"ZIP error: {ex}")
        _capture(ex)
    return dataframes



# ─────────────────────────────────────────────────────────────────
# TMDB ENRICHMENT ENGINE
# ─────────────────────────────────────────────────────────────────
def enrich_with_progress(df: pd.DataFrame, label: str = "films",
                         pb=None, status=None) -> pd.DataFrame:
    """Enrich DataFrame with TMDB metadata.

    ✅ V9.3 PERFORMANCE:
      • ONE bulk SQL query (no N+1)
      • Vectorised pd.DataFrame merge (no iterrows + df.at)
      • Single-transaction batch writes
      • Negative-hash tmdb_id for failures (no UNIQUE collisions)
    """
    import time as _t
    if df.empty:
        return df
    try:
        own_progress = pb is None
        if own_progress:
            pb = st.progress(0)
            status = st.empty()
        status.text(f"🎬 {random_loader()} — {label}")

        _t0 = _t.time()
        df = df.copy()

        # Vectorised year/title extraction
        if "Year" not in df.columns:
            parsed          = df["Name"].apply(extract_year_from_title)
            df["parsed_title"] = parsed.apply(lambda x: x[0])
            df["parsed_year"]  = parsed.apply(lambda x: x[1])
        else:
            df["parsed_title"] = df["Name"]
            df["parsed_year"]  = df["Year"]

        # Build cache keys
        df["_cache_key"] = df.apply(
            lambda r: _make_cache_key(
                str(r.get("parsed_title", "")).strip(),
                r.get("parsed_year")
            ), axis=1,
        )
        print(f"  [{label}] parse+keys: {_t.time()-_t0:.2f}s")

        # ✅ ONE bulk query
        _t1 = _t.time()
        pairs = list(zip(
            df["parsed_title"].astype(str).str.strip(),
            df["parsed_year"],
        ))
        status.text(f"🔍 Checking cache for {len(pairs):,} {label}…")
        cached_movies = db.get_movies_bulk(pairs)
        print(f"  [{label}] SQL bulk lookup ({len(pairs)} pairs → {len(cached_movies)} hits): {_t.time()-_t1:.2f}s")
        pb.progress(0.15)

        # ✅ FIX 1: Separate REAL hits from FAILED hits with 7-day TTL
        # Failed entries (negative tmdb_id) younger than 7 days are silently
        # skipped — they are NOT retried and NOT merged into the display df.
        # Only failures older than 7 days are retried from TMDB.
        FAILURE_TTL_DAYS = 7
        failed_keys  = set()   # Old failures → retry from TMDB
        known_stale  = set()   # Recent failures → skip silently
        for ck, movie in list(cached_movies.items()):
            tmdb_id = movie.get("tmdb_id")
            poster  = movie.get("poster_path", "")
            try:
                tid = int(tmdb_id) if tmdb_id is not None else 0
            except (ValueError, TypeError):
                tid = 0
            if tid < 0 or (tid == 0 and not poster):
                # Check TTL — only retry failures older than threshold
                cached_at = movie.get("cached_at", "")
                try:
                    age_days = (_dt_now.now() - _dt_now.fromisoformat(str(cached_at))).days
                except Exception:
                    age_days = 999  # Unknown age → retry
                if age_days < FAILURE_TTL_DAYS:
                    known_stale.add(ck)
                else:
                    failed_keys.add(ck)
                del cached_movies[ck]  # Remove from hits so it doesn't merge

        if failed_keys:
            print(f"  [{label}] {len(failed_keys)} expired failures → retrying")
        if known_stale:
            print(f"  [{label}] {len(known_stale)} recent failures → skipping (TTL {FAILURE_TTL_DAYS}d)")

        # ✅ VECTORISED MERGE
        _t2 = _t.time()
        skip_cols = {"id", "cached_at", "cache_key"}
        cached_keys = set(cached_movies.keys()) if cached_movies else set()

        if cached_movies:
            cache_df = pd.DataFrame.from_records(list(cached_movies.values()))
            cache_df.rename(columns={"cache_key": "_cache_key"}, inplace=True)
            cache_df.drop(columns=[c for c in skip_cols if c in cache_df.columns],
                          errors="ignore", inplace=True)
            user_only = {"Name", "Date", "Letterboxd URI", "Rating", "Tags",
                         "Watched Date", "parsed_title", "parsed_year"}
            cache_df.drop(columns=[c for c in user_only if c in cache_df.columns],
                          errors="ignore", inplace=True)

            # Deduplicate cache_df on _cache_key to prevent row multiplication in merge
            cache_df = cache_df.drop_duplicates(subset=["_cache_key"], keep="first")

            merge_cols = [c for c in cache_df.columns if c != "_cache_key"]
            df = df.merge(cache_df, on="_cache_key", how="left", suffixes=("", "_cached"))
            for col in merge_cols:
                cached_col = f"{col}_cached"
                if cached_col in df.columns:
                    df[col] = df[cached_col].combine_first(df.get(col, pd.Series(dtype="object")))
                    df.drop(columns=[cached_col], inplace=True)
            hits = df["tmdb_id"].notna().sum() if "tmdb_id" in df.columns else 0
        else:
            hits = 0
        print(f"  [{label}] merge: {_t.time()-_t2:.2f}s ({hits} hits)")

        pb.progress(0.25)
        if hits:
            status.text(f"⚡ {hits:,} {label} from cache. {random_loader()}")

        # ✅ KEY-BASED uncached detection (not tmdb_id-based)
        # skip_keys = successful cache hits + recent failures (not retried)
        skip_keys = cached_keys | known_stale
        uncached_mask = ~df["_cache_key"].isin(skip_keys)
        uncached = df[uncached_mask]
        print(f"  [{label}] uncached: {len(uncached)} films to fetch from TMDB")

        _t3 = _t.time()
        if len(uncached) > 0:
            status.text(f"📡 {random_loader()} ({len(uncached):,} {label} to fetch)")
            fetch_list = list(zip(
                uncached["parsed_title"].astype(str).str.strip(),
                uncached["parsed_year"],
            ))
            api_results = fetch_movies_with_progress(
                TMDB_API_KEY, fetch_list, pb, status,
            )
            # ✅ FIX 2: Vectorised bulk write (replaces per-cell df.at[] loop)
            to_cache = []
            write_records = []
            for orig_idx, movie_data in zip(uncached.index, api_results):
                if movie_data:
                    to_cache.append(movie_data)
                    write_records.append({**movie_data, "_orig_idx": orig_idx})
                else:
                    title = str(df.at[orig_idx, "parsed_title"]).strip()
                    year  = df.at[orig_idx, "parsed_year"]
                    if title:
                        ck = _make_cache_key(title, year)
                        raw = abs(hash(ck)) % 2_000_000_000
                        neg_id = -(raw if raw != 0 else 1)
                        write_records.append({"_orig_idx": orig_idx, "tmdb_id": neg_id})
                        to_cache.append({
                            "title": title,
                            "year": int(year) if year and str(year) not in ("", "nan") else None,
                            "tmdb_id": neg_id,
                        })

            if write_records:
                new_df = pd.DataFrame(write_records).set_index("_orig_idx")
                for col in new_df.columns:
                    df.loc[new_df.index, col] = new_df[col]

            if to_cache:
                db.add_movies_batch(to_cache)
            print(f"  [{label}] TMDB fetch ({len(uncached)} films): {_t.time()-_t3:.2f}s")

        pb.progress(1.0)
        status.text(f"✅ {len(df):,} {label} ready!")

        # Cleanup temp columns
        df.drop(columns=["_cache_key"], errors="ignore", inplace=True)

        # Post-enrichment optimisations
        df = drop_unused_columns(df)
        df = optimise_dtypes(df)
        print(f"  [{label}] TOTAL: {_t.time()-_t0:.2f}s")

        return df

    except Exception as ex:
        st.error(f"Enrichment error: {ex}")
        import traceback; traceback.print_exc()
        _capture(ex)
        return df


def _safe_num(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def _enrichment_cache_check(key: str) -> Optional[pd.DataFrame]:
    """Check if enriched data exists in session_state cache."""
    return st.session_state.get(key)


def _fetch_and_enrich(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """Run TMDB enrichment if data is not already enriched."""
    if "tmdb_id" not in df.columns or df["tmdb_id"].isna().all():
        with st.spinner(f"🎬 {random_loader()}"):
            df = enrich_with_progress(df, label)
    return df


def _store_enrichment(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Store enriched DataFrame in session_state and return it."""
    st.session_state[key] = df
    return df


def _get_enriched(df: pd.DataFrame, key: str, label: str) -> pd.DataFrame:
    """Return cached enriched DataFrame or enrich, cache, and return."""
    cached = _enrichment_cache_check(key)
    if cached is not None:
        return cached
    df = _fetch_and_enrich(df, label)
    return _store_enrichment(df, key)


# ─────────────────────────────────────────────────────────────────
# WELCOME SCREEN
# ─────────────────────────────────────────────────────────────────
def _show_welcome() -> None:
    st.markdown(
        '<div class="nb-welcome">'
        '<div class="nb-wt">⬆ UPLOAD YOUR DATA TO BEGIN</div>'
        '<ul class="nb-wl">'
        '<li>9 analysis tabs — brutal &amp; informative</li>'
        '<li>Remake-safe rewatch counter (Name + Year)</li>'
        '<li>Rectangular artist portraits, never circles</li>'
        '<li>Watchlist roulette with zero lag</li>'
        '<li>World cinema choropleth map</li>'
        '<li>Funny loading messages (we have standards)</li>'
        '<li>Handles 10,000+ films without breaking a sweat</li>'
        '<li>VIBECODED WITH CLAUDE™</li>'
        '</ul>'
        '<div style="margin-top:20px;">'
        '<span class="nb-copyleft">NO RIGHTS RESERVED</span>'
        '</div>'
        '<div class="nb-wf-tagline">developed during sleepless nights and existential crisis</div>'
        '</div>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────
# UPLOAD UI
# ─────────────────────────────────────────────────────────────────
def main() -> None:
    try:
        display_brutalist_title()

        with st.sidebar:
            st.markdown(
                '<div style="font-family:Chivo,sans-serif;font-size:1.3rem;'
                'font-weight:900;color:#000;background:#FFDE00;border:5px solid #000;'
                'padding:8px 14px;box-shadow:5px 5px 0 #000;margin-bottom:16px;'
                'text-transform:uppercase;">📤 UPLOAD DATA</div>',
                unsafe_allow_html=True,
            )
            mode = st.radio("Method:", ["ZIP Export", "Individual CSVs"], label_visibility="collapsed")

        dataframes: Dict[str, pd.DataFrame] = {}

        if mode == "ZIP Export":
            with st.sidebar:
                st.markdown(
                    "<small><b>Settings → Import & Export → Export Your Data<br>"
                    "Download ZIP → upload below</b></small>",
                    unsafe_allow_html=True,
                )
                zf = st.file_uploader("Upload Letterboxd ZIP", type=["zip"])
                if zf:
                    with st.spinner("📂 Extracting…"):
                        dataframes = process_zip_file(zf)
                    if dataframes:
                        st.success(f"✅ {len(dataframes)} CSVs loaded")
                        # ✅ V9.3: Extract profile.csv and track user
                        if "_user_profile" not in st.session_state:
                            profile_df = dataframes.get("profile", pd.DataFrame())
                            if not profile_df.empty and "Username" in profile_df.columns:
                                _p = profile_df.iloc[0]
                                _uname = str(_p.get("Username", "")).strip()
                                _given = str(_p.get("Given Name", "")).strip()
                                _bio   = str(_p.get("Bio", "")).strip()
                                if _uname:
                                    watched_count = len(dataframes.get("watched", pd.DataFrame()))
                                    db.track_user(_uname, _given, _bio, watched_count)
                                    st.session_state["_user_profile"] = {
                                        "username": _uname,
                                        "given_name": _given,
                                        "bio": _bio,
                                    }
                                    st.rerun()  # ✅ Rerun so sidebar picks up the username
        else:
            with st.sidebar:
                wf  = st.file_uploader("watched.csv",   type=["csv"], key="w")
                rf  = st.file_uploader("ratings.csv",   type=["csv"], key="r")
                wlf = st.file_uploader("watchlist.csv", type=["csv"], key="wl")
                df  = st.file_uploader("diary.csv (for rewatches)", type=["csv"], key="d")

                if wf:  dataframes["watched"]   = pd.read_csv(wf)
                if rf:  dataframes["ratings"]   = pd.read_csv(rf)
                if wlf: dataframes["watchlist"] = pd.read_csv(wlf)
                if df:  dataframes["diary"]     = pd.read_csv(df)

                if dataframes: st.success(f"✅ {len(dataframes)} file(s) loaded")
                
                # Add ability to manually clear fails
                if st.button("Retry Failed TMDB Lookups"):
                    try:
                        # Query all failed cache keys, then delete them
                        cur = db.conn.cursor()
                        cur.execute(
                            "SELECT cache_key FROM movies WHERE tmdb_id < 0 OR tmdb_id IS NULL"
                        )
                        failed_cks = [r[0] for r in cur.fetchall()]
                        if failed_cks:
                            db.delete_failed_entries(failed_cks)
                        st.session_state.clear()
                        st.success(f"Cleared {len(failed_cks)} failures! Refreshing...")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to clear: {e}")

        if not dataframes:
            _show_welcome()
            return

        display_analysis(dataframes)

    except Exception as ex:
        st.error(f"App error: {ex}")
        _capture(ex)


# ─────────────────────────────────────────────────────────────────
# TAB DISPATCHER
# ─────────────────────────────────────────────────────────────────
def display_analysis(dataframes: Dict[str, pd.DataFrame]) -> None:
    import time
    import hashlib
    t0 = time.time()

    # ✅ FIX: Invalidate enrichment caches when new data is uploaded.
    # Hash the column names + row count of each dataset to detect changes.
    # If any dataset changes, ALL enrichment caches are cleared.
    _sig_parts = []
    for k in sorted(dataframes.keys()):
        _df = dataframes[k]
        _sig_parts.append(f"{k}:{len(_df)}:{','.join(_df.columns[:5])}")
    _data_sig = hashlib.md5("|".join(_sig_parts).encode()).hexdigest()

    if st.session_state.get("_data_signature") != _data_sig:
        # New data detected — clear ALL enrichment caches
        for _ek in ["watched_enriched", "ratings_enriched", "watchlist_enriched",
                    "roulette_pool", "roulette_pick", "roulette_genre"]:
            st.session_state.pop(_ek, None)
        st.session_state["_data_signature"] = _data_sig

    watched_df = dataframes.get("watched", pd.DataFrame())
    ratings_df = dataframes.get("ratings", pd.DataFrame())
    watchlist_df = dataframes.get("watchlist", pd.DataFrame())

    datasets_to_enrich = []
    
    # Watched
    if not watched_df.empty:
        if "watched_enriched" not in st.session_state:
            datasets_to_enrich.append(("watched", watched_df, "watched_enriched", "watched films"))
        else:
            dataframes["watched"] = st.session_state["watched_enriched"]
    
    # Ratings
    if not ratings_df.empty:
        if "ratings_enriched" not in st.session_state:
            datasets_to_enrich.append(("ratings", ratings_df, "ratings_enriched", "rated films"))
        else:
            dataframes["ratings"] = st.session_state["ratings_enriched"]

    # Watchlist
    if not watchlist_df.empty:
        if "watchlist_enriched" not in st.session_state:
            datasets_to_enrich.append(("watchlist", watchlist_df, "watchlist_enriched", "watchlist"))
        else:
            dataframes["watchlist"] = st.session_state["watchlist_enriched"]

    print(f"[TIME] display_analysis setup: {time.time()-t0:.2f}s")
    
    if datasets_to_enrich:
        t_enrich = time.time()
        pb = st.progress(0)
        status = st.empty()
        for i, (key, df_raw, cache_key, label) in enumerate(datasets_to_enrich):
            status.text(f"🎬 Enriching {label} ({i+1}/{len(datasets_to_enrich)})…")
            enriched = enrich_with_progress(df_raw, label, pb, status)
            enriched = _safe_num(enriched, CONFIG["SAFE_NUM_COLS"])
            st.session_state[cache_key] = enriched
            dataframes[key] = enriched
        pb.progress(1.0)
        status.text(f"✅ All data enriched!")
        time.sleep(0.5)
        pb.empty()
        status.empty()
        print(f"[TIME] display_analysis enrichment: {time.time()-t_enrich:.2f}s")

    t_tabs = time.time()
    tabs = st.tabs([
        "🎬 WATCHED",
        "📋 WATCHLIST",
        "⭐ RATINGS",
        "🕐 RECENT",
        "🎭 ARTISTS",
        "🏆 MILESTONES",
        "📊 STATS",
        "🗺️ MAP",
        "🎰 ROULETTE",
    ])
    with tabs[0]: 
        t_w = time.time()
        _tab_watched(dataframes)
        print(f"[TIME] _tab_watched: {time.time()-t_w:.2f}s")
    with tabs[1]:
        t_w = time.time()
        _tab_watchlist(dataframes)
        print(f"[TIME] _tab_watchlist: {time.time()-t_w:.2f}s")
    with tabs[2]:
        t_w = time.time()
        _tab_ratings(dataframes)
        print(f"[TIME] _tab_ratings: {time.time()-t_w:.2f}s")
    with tabs[3]:
        t_w = time.time()
        _tab_recent(dataframes)
        print(f"[TIME] _tab_recent: {time.time()-t_w:.2f}s")
    with tabs[4]:
        t_w = time.time()
        _tab_artists(dataframes)
        print(f"[TIME] _tab_artists: {time.time()-t_w:.2f}s")
    with tabs[5]:
        t_w = time.time()
        _tab_milestones(dataframes)
        print(f"[TIME] _tab_milestones: {time.time()-t_w:.2f}s")
    with tabs[6]:
        t_w = time.time()
        _tab_stats(dataframes)
        print(f"[TIME] _tab_stats: {time.time()-t_w:.2f}s")
    with tabs[7]:
        t_w = time.time()
        _tab_map(dataframes)
        print(f"[TIME] _tab_map: {time.time()-t_w:.2f}s")
    with tabs[8]:
        t_w = time.time()
        _tab_roulette(dataframes)
        print(f"[TIME] _tab_roulette: {time.time()-t_w:.2f}s")
    print(f"[TIME] display_analysis total tabs render: {time.time()-t_tabs:.2f}s")


# ─────────────────────────────────────────────────────────────────
# TAB: 🎬 WATCHED
# ─────────────────────────────────────────────────────────────────
def _tab_watched(dataframes: Dict[str, pd.DataFrame]) -> None:
    try:
        watched_df = dataframes.get("watched", pd.DataFrame())
        ratings_df = dataframes.get("ratings", pd.DataFrame())
        if watched_df.empty:
            st.warning("No watched data found. Upload watched.csv or full ZIP.")
            return

        st.markdown(
            '<div class="nb-title-cyan" style="font-size:2.5rem;margin-bottom:20px;">🎬 WATCHED FILMS</div>',
            unsafe_allow_html=True,
        )
        reset_metric_counter()

        watched_df = _get_enriched(watched_df, "watched_enriched", "watched films")
        # _safe_num already applied by display_analysis at line 674

        hours = calculate_total_hours(watched_df)
        days  = hours / 24

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            display_metric_card(f"{len(watched_df):,}", "TOTAL FILMS", "Go touch grass.")
        with col2:
            display_metric_card(f"{hours:,.0f}", "HOURS WATCHED", "Could've learned 3 languages.")
        with col3:
            display_metric_card(f"{days:,.1f}", "DAYS OF FILM", "A full cinematic existence.")
        with col4:
            # ✅ V9 FIX: avg rating from ratings.csv, not watched.csv (which has no Rating col)
            avg_r = None
            if not ratings_df.empty and "Rating" in ratings_df.columns:
                avg_r = pd.to_numeric(ratings_df["Rating"], errors="coerce").dropna().mean()
            elif "Rating" in watched_df.columns:
                avg_r = pd.to_numeric(watched_df["Rating"], errors="coerce").dropna().mean()
            if avg_r is not None and not pd.isna(avg_r):
                display_metric_card(f"{avg_r:.2f}★", "AVG RATING", "Generous much?")
            else:
                display_metric_card("—", "AVG RATING", "Upload ratings.csv for this.")

        _divider()

        _section_header("GENRES")
        plot_bar_chart(analyze_genres(watched_df), "Genre", "Count", "")

        _divider()
        _section_header("LANGUAGES")
        plot_bar_chart(analyze_languages(watched_df), "Language", "Count", "")

        _divider()
        _section_header("MONTHLY PATTERN")
        plot_bar_chart(analyze_movies_per_month(watched_df), "Month", "Count", "")

        _divider()
        _section_header("WEEKLY PATTERN")
        plot_bar_chart(analyze_movies_per_day(watched_df), "Day", "Count", "")

        _divider()
        col_act, col_dir = st.columns(2)
        with col_act:
            display_top_list(get_top_people(watched_df, "actors", 30), "TOP ACTORS", "🎭")
        with col_dir:
            display_top_list(get_top_people(watched_df, "directors", 30), "TOP DIRECTORS", "🎬")

        display_tab_easter_egg("watched")
        display_global_footer()

    except Exception as ex:
        st.error(f"Watched tab error: {ex}")
        _capture(ex)


# ─────────────────────────────────────────────────────────────────
# TAB: 📋 WATCHLIST
# ─────────────────────────────────────────────────────────────────
def _tab_watchlist(dataframes: Dict[str, pd.DataFrame]) -> None:
    try:
        watchlist_df = dataframes.get("watchlist", pd.DataFrame())
        if watchlist_df.empty:
            st.warning("No watchlist data found.")
            return
        st.markdown(
            '<div class="nb-title-cyan" style="font-size:2.5rem;margin-bottom:20px;">📋 WATCHLIST</div>',
            unsafe_allow_html=True,
        )
        reset_metric_counter()

        watchlist_df = _get_enriched(watchlist_df, "watchlist_enriched", "watchlist")
        watchlist_df = _safe_num(watchlist_df, ["vote_count","runtime","popularity","vote_average","tmdb_id"])

        hours_wl = calculate_total_hours(watchlist_df)
        col1, col2, col3 = st.columns(3)
        with col1:
            display_metric_card(f"{len(watchlist_df):,}", "FILMS TO WATCH", "You will never finish this.")
        with col2:
            display_metric_card(f"{hours_wl:,.0f}", "HOURS QUEUED", "That's your future, right there.")
        with col3:
            display_metric_card(f"{hours_wl/24:,.1f}", "DAYS QUEUED", "No pressure.")

        _divider()
        _section_header("CRITICALLY ACCLAIMED (ON YOUR LIST)")
        acclaimed = get_highly_rated_unseen(watchlist_df, min_rating=7.5, n=CONFIG["GRID_SMALL_N"] + 1)
        if not acclaimed.empty:
            display_film_grid(acclaimed, "", cols_count=5)

        _divider()
        _section_header("GENRE BREAKDOWN")
        plot_bar_chart(analyze_genres(watchlist_df), "Genre", "Count", "")

        _divider()
        # ✅ V9 FIX: 4 films each (was 2)
        col_pop, col_obs = st.columns(2)
        with col_pop:
            if "vote_count" in watchlist_df.columns:
                popular = watchlist_df[watchlist_df["vote_count"] > 0].nlargest(4, "vote_count")
                display_film_grid(popular, "CROWD FAVOURITES", cols_count=4)
        with col_obs:
            if "vote_count" in watchlist_df.columns:
                obscure = watchlist_df[watchlist_df["vote_count"] > 0].nsmallest(4, "vote_count")
                display_film_grid(obscure, "HIDDEN GEMS", cols_count=4)

        _divider()
        col_act, col_dir = st.columns(2)
        with col_act:
            display_top_list(get_top_people(watchlist_df, "actors", 20), "FEATURED ACTORS", "🎭")
        with col_dir:
            display_top_list(get_top_people(watchlist_df, "directors", 20), "FEATURED DIRECTORS", "🎬")

        display_tab_easter_egg("watchlist")
        display_global_footer()

    except Exception as ex:
        st.error(f"Watchlist tab error: {ex}")
        _capture(ex)


# ─────────────────────────────────────────────────────────────────
# TAB: ⭐ RATINGS
# ─────────────────────────────────────────────────────────────────
def _tab_ratings(dataframes: Dict[str, pd.DataFrame]) -> None:
    try:
        ratings_df = dataframes.get("ratings", pd.DataFrame())
        if ratings_df.empty:
            st.warning("No ratings data found.")
            return
        st.markdown(
            '<div class="nb-title-cyan" style="font-size:2.5rem;margin-bottom:20px;">⭐ RATINGS</div>',
            unsafe_allow_html=True,
        )
        reset_metric_counter()

        ratings_df = _get_enriched(ratings_df, "ratings_enriched", "rated films")
        # _safe_num already applied by display_analysis
        if "Rating" in ratings_df.columns:
            ratings_df["Rating"] = pd.to_numeric(ratings_df["Rating"], errors="coerce")

        col1, col2, col3 = st.columns(3)
        with col1:
            display_metric_card(f"{len(ratings_df):,}", "RATED FILMS", "You have opinions. Good.")
        with col2:
            if "Rating" in ratings_df.columns:
                avg = ratings_df["Rating"].mean()
                display_metric_card(f"{avg:.2f}★", "AVERAGE RATING", "Your personal Rotten Tomatoes.")
            else:
                display_metric_card("—", "AVERAGE RATING")
        with col3:
            if "Rating" in ratings_df.columns:
                fives = int((ratings_df["Rating"] == 5).sum())
                display_metric_card(f"{fives:,}", "5-STAR FILMS", "Absolute masterpieces.")
            else:
                display_metric_card("—", "5-STAR FILMS")

        _divider()
        if "Rating" in ratings_df.columns:
            _section_header("RATING DISTRIBUTION")
            rc = ratings_df["Rating"].value_counts().sort_index().reset_index()
            rc.columns = ["Rating", "Count"]
            rc["Rating"] = rc["Rating"].apply(lambda x: f"{x}★")
            plot_bar_chart(rc, "Rating", "Count", "")

        _divider()
        if "Rating" in ratings_df.columns:
            # Sort by Date (most recently logged first) so display is not random
            if "Date" in ratings_df.columns:
                ratings_sorted = ratings_df.copy()
                ratings_sorted["_sort_date"] = pd.to_datetime(ratings_sorted["Date"], errors="coerce")
                ratings_sorted = ratings_sorted.sort_values("_sort_date", ascending=False)
            else:
                ratings_sorted = ratings_df

            top_rated    = ratings_sorted[ratings_sorted["Rating"] == 5].head(CONFIG["RATED_DISPLAY_N"])
            bottom_rated = ratings_sorted[ratings_sorted["Rating"] <= 1].head(CONFIG["RATED_DISPLAY_N"])

            # ✅ FIX: Full-width grids (no st.columns(2) — prevents poster overlap)
            if not top_rated.empty:
                display_film_grid(top_rated, "YOUR 5-STAR FILMS ⭐⭐⭐⭐⭐", show_rating=True, cols_count=5, square=True)
            _divider()
            if not bottom_rated.empty:
                display_film_grid(bottom_rated, "YOUR LOWEST RATINGS 💀", show_rating=True, cols_count=5, square=True)
        display_tab_easter_egg("ratings")
        display_global_footer()

    except Exception as ex:
        st.error(f"Ratings tab error: {ex}")
        _capture(ex)


# ─────────────────────────────────────────────────────────────────
# TAB: 🕐 RECENT
# ─────────────────────────────────────────────────────────────────
def _tab_recent(dataframes: Dict[str, pd.DataFrame]) -> None:
    try:
        watched_df = dataframes.get("watched", pd.DataFrame())
        if watched_df.empty:
            st.warning("No watched data found.")
            return
        st.markdown(
            '<div class="nb-title-cyan" style="font-size:2.5rem;margin-bottom:20px;">🕐 RECENT ACTIVITY</div>',
            unsafe_allow_html=True,
        )
        watched_df = st.session_state.get("watched_enriched", watched_df)
        recent = get_recently_watched(watched_df, "Date", n=CONFIG["RECENT_FILMS_N"])
        if not recent.empty:
            display_recently_watched(recent)
        _divider()
        # ✅ FIX: Full-width grids (no st.columns(2) — prevents poster overlap)
        display_film_grid_large(get_newest_films(watched_df, CONFIG["GRID_LARGE_N"]), "NEWEST RELEASES", n=CONFIG["GRID_LARGE_N"], square=True)
        _divider()
        display_film_grid_large(get_oldest_films(watched_df, CONFIG["GRID_LARGE_N"]), "OLDEST RELEASES", n=CONFIG["GRID_LARGE_N"], square=True)
        display_tab_easter_egg("recent")
        display_global_footer()

    except Exception as ex:
        st.error(f"Recent tab error: {ex}")
        _capture(ex)


# ─────────────────────────────────────────────────────────────────
# TAB: 🎭 ARTISTS
# ─────────────────────────────────────────────────────────────────
def _tab_artists(dataframes: Dict[str, pd.DataFrame]) -> None:
    try:
        watched_df = dataframes.get("watched", pd.DataFrame())
        if watched_df.empty:
            st.warning("No watched data found.")
            return
        st.markdown(
            '<div class="nb-title-cyan" style="font-size:2.5rem;margin-bottom:20px;">🎭 ARTISTS</div>',
            unsafe_allow_html=True,
        )
        watched_df = st.session_state.get("watched_enriched", watched_df)

        # ✅ V9.2: Renamed to "Most Watched", limited to 12
        top_dirs = get_top_people_with_images(watched_df, "directors_with_images", CONFIG["TOP_DIRECTORS_N"])
        if top_dirs:
            display_people_with_images(top_dirs, "MOST WATCHED DIRECTORS")

        _divider()

        top_acts = get_top_people_with_images(watched_df, "actors_with_images", CONFIG["TOP_ACTORS_N"])
        if top_acts:
            display_people_with_images(top_acts, "MOST WATCHED ACTORS")

        _divider()

        # ✅ V9.2: Highest Rated Directors (by avg user rating, min 2 films)
        _section_header("HIGHEST RATED DIRECTORS")
        ratings_df = dataframes.get("ratings", pd.DataFrame())
        if not ratings_df.empty:
            ratings_df = st.session_state.get("ratings_enriched", ratings_df)
        dir_data: Dict[str, dict] = {}
        act_data: Dict[str, dict] = {}
        rated_src = ratings_df if not ratings_df.empty and "Rating" in ratings_df.columns else watched_df
        di_col = "directors_with_images"
        ac_col = "actors_with_images"
        has_di = di_col in rated_src.columns
        has_ac = ac_col in rated_src.columns
        if (has_di or has_ac) and "Rating" in rated_src.columns:
            # ✅ FIX 3: Single pass with to_dict('records') — replaces two iterrows loops
            cols_needed = {"Rating"}
            if has_di: cols_needed.add(di_col)
            if has_ac: cols_needed.add(ac_col)
            mask = rated_src["Rating"].notna()
            if has_di: mask = mask & rated_src[di_col].notna()
            if has_ac: mask = mask | (rated_src[ac_col].notna() & rated_src["Rating"].notna())
            for row in rated_src.loc[mask, list(cols_needed)].to_dict("records"):
                try:
                    rating = float(row["Rating"])
                except Exception:
                    continue
                if has_di and row.get(di_col):
                    try:
                        for d in json.loads(row[di_col]):
                            n = d["name"]
                            if n not in dir_data:
                                dir_data[n] = {"ratings": [], "profile_path": d.get("profile_path", "")}
                            dir_data[n]["ratings"].append(rating)
                    except Exception:
                        pass
                if has_ac and row.get(ac_col):
                    try:
                        for a in json.loads(row[ac_col]):
                            n = a["name"]
                            if n not in act_data:
                                act_data[n] = {"ratings": [], "profile_path": a.get("profile_path", "")}
                            act_data[n]["ratings"].append(rating)
                    except Exception:
                        pass

        hr_dirs = sorted(
            [{"name": n, "profile_path": v["profile_path"], "count": len(v["ratings"]),
              "avg_rating": sum(v["ratings"]) / len(v["ratings"])}
             for n, v in dir_data.items() if len(v["ratings"]) >= 2],
            key=lambda x: x["avg_rating"], reverse=True
        )
        if hr_dirs:
            display_people_with_images(hr_dirs[:CONFIG["TOP_DIRECTORS_N"]], "", "⭐")

        _divider()

        # ✅ V9.2: Highest Rated Actors (by avg user rating, min 2 films)
        _section_header("HIGHEST RATED ACTORS")
        hr_acts = sorted(
            [{"name": n, "profile_path": v["profile_path"], "count": len(v["ratings"]),
              "avg_rating": sum(v["ratings"]) / len(v["ratings"])}
             for n, v in act_data.items() if len(v["ratings"]) >= 2],
            key=lambda x: x["avg_rating"], reverse=True
        )
        if hr_acts:
            display_people_with_images(hr_acts[:CONFIG["TOP_ACTORS_N"]], "", "⭐")

        _divider()
        col_act, col_dir = st.columns(2)
        with col_act:
            display_top_list(get_top_people(watched_df, "actors", 30), "ALL TOP ACTORS (30)", "🎭")
        with col_dir:
            display_top_list(get_top_people(watched_df, "directors", 30), "ALL TOP DIRECTORS (30)", "🎬")

        display_tab_easter_egg("artists")
        display_global_footer()

    except Exception as ex:
        st.error(f"Artists tab error: {ex}")
        _capture(ex)


# ─────────────────────────────────────────────────────────────────
# TAB: 🏆 MILESTONES
# ─────────────────────────────────────────────────────────────────
def _tab_milestones(dataframes: Dict[str, pd.DataFrame]) -> None:
    try:
        watched_df = dataframes.get("watched", pd.DataFrame())
        if watched_df.empty:
            st.warning("No watched data found.")
            return
        st.markdown(
            '<div class="nb-title-cyan" style="font-size:2.5rem;margin-bottom:20px;">🏆 MILESTONES</div>',
            unsafe_allow_html=True,
        )
        from datetime import datetime
        watched_df = st.session_state.get("watched_enriched", watched_df)
        yr = datetime.now().year

        _section_header(f"FIRST & LAST OF {yr}")
        fl = get_first_and_last_film(watched_df, "Date", yr)
        if fl.get("first") is not None:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(
                    f'<div style="font-family:Chivo,sans-serif;font-size:1.3rem;font-weight:900;'
                    f'color:#000;background:#00D34D;border:5px solid #000;padding:6px 15px;'
                    f'display:inline-block;box-shadow:5px 5px 0 #000;margin-bottom:12px;'
                    f'width:100%;text-align:center;">'
                    f'🎬 FIRST FILM OF {yr}</div>',
                    unsafe_allow_html=True,
                )
                display_milestone_card(f"FIRST {yr}", fl["first"])
            with col2:
                st.markdown(
                    f'<div style="font-family:Chivo,sans-serif;font-size:1.3rem;font-weight:900;'
                    f'color:#FFF;background:#FF003C;border:5px solid #000;padding:6px 15px;'
                    f'display:inline-block;box-shadow:5px 5px 0 #000;margin-bottom:12px;'
                    f'width:100%;text-align:center;">'
                    f'🎬 LAST FILM OF {yr}</div>',
                    unsafe_allow_html=True,
                )
                if fl.get("last") is not None:
                    display_milestone_card(f"LAST {yr}", fl["last"])

        _divider()
        _section_header("DIARY MILESTONES")
        milestones = get_milestones(watched_df, "Date")
        for mname, mrow in milestones.items():
            st.markdown(
                f'<div style="font-family:Chivo,sans-serif;font-size:1.5rem;font-weight:900;'
                f'color:#000;background:#00E5FF;border:5px solid #000;padding:5px 14px;'
                f'display:inline-block;box-shadow:5px 5px 0 #000;margin-bottom:10px;">'
                f'🏅 FILM #{mname}</div>',
                unsafe_allow_html=True,
            )
            display_milestone_card(mname, mrow)
            _divider()

        _divider()
        _section_header("MOST RE-WATCHED FILMS")
        # ✅ V9 FIX: Use diary.csv (each viewing = one row) instead of
        # watched.csv (deduplicated, one row per film — never has duplicates).
        diary_df = dataframes.get("diary", pd.DataFrame())
        if not diary_df.empty and "Name" in diary_df.columns:
            most_watched = get_most_watched_films(diary_df, 10)
            if not most_watched.empty:
                # Merge poster/metadata from enriched watched_df
                meta_cols = ["Name", "Year", "poster_path", "tmdb_id",
                             "vote_average", "vote_count", "runtime"]
                avail = [c for c in meta_cols if c in watched_df.columns]
                if "poster_path" in watched_df.columns and "poster_path" not in most_watched.columns:
                    merge_on = []
                    if "Name" in most_watched.columns and "Name" in watched_df.columns:
                        merge_on.append("Name")
                    if "Year" in most_watched.columns and "Year" in watched_df.columns:
                        merge_on.append("Year")
                    if merge_on:
                        meta = watched_df.drop_duplicates(subset=merge_on, keep="first")[avail]
                        most_watched = most_watched.merge(meta, on=merge_on, how="left",
                                                         suffixes=("", "_meta"))
                display_film_grid_large(most_watched, "", n=10)
                display_rewatch_counts(most_watched)
            else:
                st.info("No re-watched films detected — single-viewing purist mode activated.")
        else:
            st.info("No diary data found. Upload the full Letterboxd ZIP (contains diary.csv) to see rewatches.")

        display_tab_easter_egg("milestones")
        display_global_footer()

    except Exception as ex:
        st.error(f"Milestones tab error: {ex}")
        _capture(ex)


# ─────────────────────────────────────────────────────────────────
# TAB: 📊 STATS
# ─────────────────────────────────────────────────────────────────
def _tab_stats(dataframes: Dict[str, pd.DataFrame]) -> None:
    try:
        watched_df = dataframes.get("watched", pd.DataFrame())
        if watched_df.empty:
            st.warning("No watched data found.")
            return
        st.markdown(
            '<div class="nb-title-cyan" style="font-size:2.5rem;margin-bottom:20px;">📊 STATS — HIGHS &amp; LOWS</div>',
            unsafe_allow_html=True,
        )
        watched_df = st.session_state.get("watched_enriched", watched_df)
        watched_df = _safe_num(watched_df, ["runtime","vote_average","vote_count"])

        def _sg(subset: pd.DataFrame, lbl: str) -> None:
            if not subset.empty:
                display_film_grid(subset, lbl, cols_count=1)

        # ✅ V9 FIX: Filter vote_average > 0 AND vote_count >= CONFIG threshold
        # so only films with statistically meaningful TMDB ratings are shown.
        # Without the vote_count floor, obscure films with 2-3 votes
        # (e.g. "Marco" at 10.0) would dominate the highs/lows.
        va_valid = watched_df[
            (watched_df["vote_average"] > 0) & (watched_df["vote_count"] >= CONFIG["MIN_VOTE_COUNT"])
        ].sort_values("vote_average", ascending=False)
        if "tmdb_id" in va_valid.columns:
            va_valid = va_valid.drop_duplicates(subset=["tmdb_id"])
        else:
            va_valid = va_valid.drop_duplicates(subset=["Name"])

        col1, col2 = st.columns(2)
        with col1:
            if "vote_average" in watched_df.columns and not va_valid.empty:
                _sg(va_valid.nlargest(1, "vote_average"), "HIGHEST TMDB RATED 🏆")
        with col2:
            if "vote_average" in watched_df.columns and not va_valid.empty:
                _sg(va_valid.nsmallest(1, "vote_average"), "LOWEST TMDB RATED 💀")
        _divider()
        # ✅ Also deduplicate for vote_count stats
        vc_valid = watched_df[watched_df["vote_count"] > 0]
        if "tmdb_id" in vc_valid.columns:
            vc_valid = vc_valid.drop_duplicates(subset=["tmdb_id"])
        else:
            vc_valid = vc_valid.drop_duplicates(subset=["Name"])

        col3, col4 = st.columns(2)
        with col3:
            if "vote_count" in watched_df.columns and not vc_valid.empty:
                _sg(vc_valid.nlargest(1, "vote_count"), "MOST POPULAR 📣")
        with col4:
            if "vote_count" in watched_df.columns and not vc_valid.empty:
                _sg(vc_valid.nsmallest(1, "vote_count"), "MOST OBSCURE 🔍")
        _divider()
        col5, col6 = st.columns(2)
        with col5:
            _sg(get_newest_films(watched_df, 1, min_votes=10), "NEWEST RELEASE 🚀")
        with col6:
            _sg(get_oldest_films(watched_df, 1, min_votes=10), "OLDEST RELEASE 🕰️")
        _divider()
        col7, col8 = st.columns(2)
        with col7:
            if "runtime" in watched_df.columns:
                _sg(watched_df.nlargest(1,"runtime"), "LONGEST FILM ⌛")
        with col8:
            if "runtime" in watched_df.columns:
                _sg(watched_df[watched_df["runtime"]>0].nsmallest(1,"runtime"), "SHORTEST FILM ⚡")

        display_tab_easter_egg("stats")


        display_global_footer()

    except Exception as ex:
        st.error(f"Stats tab error: {ex}")
        _capture(ex)


# ─────────────────────────────────────────────────────────────────
# TAB: 🗺️ MAP
# ─────────────────────────────────────────────────────────────────
def _tab_map(dataframes: Dict[str, pd.DataFrame]) -> None:
    try:
        watched_df = dataframes.get("watched", pd.DataFrame())
        if watched_df.empty:
            st.warning("No watched data found.")
            return
        st.markdown(
            '<div class="nb-title-cyan" style="font-size:2.5rem;margin-bottom:20px;">🗺️ WORLD CINEMA MAP</div>',
            unsafe_allow_html=True,
        )
        watched_df = st.session_state.get("watched_enriched", watched_df)

        if "production_countries" not in watched_df.columns:
            st.info("⚙️ Visit 🎬 WATCHED tab first to process your films, then the map unlocks!")
            display_map_footer()  # still show footer even with no data
            return

        country_data = analyze_countries(watched_df)
        if not country_data:
            st.info("No country data found in your watched films.")
            display_map_footer()
            return

        create_world_map(country_data)
        _divider()
        display_top_countries(country_data, top_n=CONFIG["TOP_COUNTRIES_N"])
        _divider()

        total_countries = len(country_data)
        total_mapped    = sum(country_data.values())
        col1, col2, col3 = st.columns(3)
        reset_metric_counter()
        with col1:
            display_metric_card(f"{total_countries}", "COUNTRIES EXPLORED", "A cinematic world tour.")
        with col2:
            display_metric_card(f"{total_mapped:,}", "FILMS MAPPED", "Your cultural footprint.")
        with col3:
            if country_data:
                top_code  = max(country_data, key=country_data.get)
                top_name  = COUNTRY_NAME_MAP.get(top_code, top_code)  # ✅ No [:12] truncation
                top_count = country_data[top_code]
                display_metric_card(top_name, f"#1 COUNTRY ({top_count:,} films)")

        display_tab_easter_egg("map")
        display_global_footer()

        # ✅ Mandatory Map tab footer
        display_map_footer()

    except Exception as ex:
        st.error(f"Map tab error: {ex}")
        _capture(ex)


# ─────────────────────────────────────────────────────────────────
# TAB: 🎰 ROULETTE
# ─────────────────────────────────────────────────────────────────
def _tab_roulette(dataframes: Dict[str, pd.DataFrame]) -> None:
    try:
        watchlist_df = dataframes.get("watchlist", pd.DataFrame())
        st.markdown(
            '<div class="nb-title-cyan" style="font-size:2.5rem;margin-bottom:20px;">🎰 WATCHLIST ROULETTE</div>',
            unsafe_allow_html=True,
        )
        if watchlist_df.empty:
            st.warning("No watchlist data found. Upload watchlist.csv or full ZIP.")
            display_tab_easter_egg("roulette")
            display_global_footer()
            return

        # ✅ V9 PERFORMANCE FIX: pool is built once per session — not re-enriched on spin
        watchlist_df = _get_enriched(watchlist_df, "watchlist_enriched", "watchlist")
        watchlist_df = _safe_num(watchlist_df, CONFIG["SAFE_NUM_COLS"])

        display_watchlist_roulette(watchlist_df)
        display_tab_easter_egg("roulette")
        display_global_footer()

    except Exception as ex:
        st.error(f"Roulette tab error: {ex}")
        _capture(ex)


# ─────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
