"""
data_processing.py — V9 Neo-Brutalist Comic Edition

V9 CHANGES:
  ✅ get_most_watched_films: groups by (Name, Year) — distinguishes remakes.
     Superman (1978) ≠ Superman (2025). Mean Girls (2004) ≠ Mean Girls (2024).
  ✅ Vectorized: analyze_genres, analyze_languages, get_top_people,
     analyze_countries — all use str.split().explode() instead of iterrows().
  ✅ @st.cache_data on all heavy analysis functions.
  ✅ Memory: downcast float64→float32 on numeric enrichment cols.
  ✅ Drop unused columns after enrichment to reduce memory footprint.
"""

from __future__ import annotations

try:
    import orjson
    def _json_loads(s: str) -> Any:
        return orjson.loads(s)
except ImportError:
    import json
    def _json_loads(s: str) -> Any:
        return json.loads(s)

import re
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st


# ─────────────────────────────────────────────────────────────────
# DATE HELPERS
# ─────────────────────────────────────────────────────────────────
def parse_letterboxd_date(date_str: str) -> Optional[datetime]:
    """Parse a Letterboxd date string."""
    if pd.isna(date_str) or not date_str:
        return None
    try:
        return pd.to_datetime(date_str)
    except Exception:
        return None


def extract_year_from_title(name: str) -> Tuple[str, Optional[int]]:
    """Extract trailing (YYYY) year from a film title."""
    if pd.isna(name) or not name:
        return name, None
    match = re.search(r'\((\d{4})\)\s*$', str(name))
    if match:
        return name[:match.start()].strip(), int(match.group(1))
    return name, None


# ─────────────────────────────────────────────────────────────────
# MEMORY OPTIMISER
# ─────────────────────────────────────────────────────────────────
def optimise_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Downcast float64 → float32 and convert low-cardinality
    object columns to category for faster groupby operations.
    Safe to call multiple times (idempotent).
    """
    for col in df.select_dtypes(include=["float64"]).columns:
        df[col] = df[col].astype("float32", errors="ignore")

    for col in ["original_language"]:
        if col in df.columns:
            n_unique = df[col].nunique()
            n_total  = len(df)
            # Only convert to category if cardinality < 50% of rows
            if n_unique < n_total * 0.5:
                df[col] = df[col].astype("category")
    return df


def drop_unused_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop columns we never use in display to cut memory."""
    cols_to_drop = [c for c in df.columns if c.endswith("_y")]
    heavy_cols   = [c for c in df.columns if "video" in c.lower() or "adult" in c.lower()]
    return df.drop(columns=cols_to_drop + heavy_cols, errors="ignore")


# ─────────────────────────────────────────────────────────────────
# GENRE ANALYSIS — VECTORISED
# ─────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def analyze_genres(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vectorised genre distribution — explode() instead of iterrows().
    10× faster for 10,000+ row DataFrames.
    """
    if df.empty or "genres" not in df.columns:
        return pd.DataFrame()
    genres_s = (
        df["genres"]
        .dropna()
        .astype(str)
        .str.split(",")
        .explode()
        .str.strip()
    )
    genres_s = genres_s[genres_s != ""]
    if genres_s.empty:
        return pd.DataFrame()

    # Remove misclassified people names
    _BAD = {"jones", "allen", "woody", "chuck", "imamura", "shohei", "none", "nan"}
    genres_s = genres_s[~genres_s.str.lower().isin(_BAD)]

    result = (
        genres_s.value_counts()
        .reset_index()
        .rename(columns={"index": "Genre", "genres": "Genre", "count": "Count", 0: "Count"})
    )
    result.columns = ["Genre", "Count"]
    return result


# ─────────────────────────────────────────────────────────────────
# LANGUAGE ANALYSIS — VECTORISED
# ─────────────────────────────────────────────────────────────────
LANGUAGE_MAP: Dict[str, str] = {
    "en":"English","fr":"French","es":"Spanish","de":"German","it":"Italian",
    "ja":"Japanese","ko":"Korean","zh":"Chinese","ru":"Russian","pt":"Portuguese",
    "ar":"Arabic","hi":"Hindi","sv":"Swedish","nl":"Dutch","pl":"Polish",
    "da":"Danish","fi":"Finnish","no":"Norwegian","tr":"Turkish","el":"Greek",
    "th":"Thai","id":"Indonesian","cs":"Czech","hu":"Hungarian","ro":"Romanian",
    "he":"Hebrew","uk":"Ukrainian","vi":"Vietnamese","fa":"Persian","bn":"Bengali",
    "ta":"Tamil","te":"Telugu","ur":"Urdu","ml":"Malayalam","mr":"Marathi",
    "pa":"Punjabi","kn":"Kannada","gu":"Gujarati","nb":"Norwegian (Bokmål)",
    "sr":"Serbian","hr":"Croatian","sk":"Slovak","sl":"Slovenian","bg":"Bulgarian",
    "lt":"Lithuanian","lv":"Latvian","et":"Estonian","mk":"Macedonian",
    "sq":"Albanian","az":"Azerbaijani","ka":"Georgian","hy":"Armenian",
    "ms":"Malay","tl":"Filipino","sw":"Swahili","af":"Afrikaans",
    "ca":"Catalan","eu":"Basque","gl":"Galician","is":"Icelandic",
    "mt":"Maltese","cy":"Welsh","ga":"Irish","lb":"Luxembourgish",
    "mn":"Mongolian","kk":"Kazakh","uz":"Uzbek","ky":"Kyrgyz","tk":"Turkmen",
    "xx":"Silent Film",
}


@st.cache_data(show_spinner=False)
def analyze_languages(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorised language distribution."""
    if df.empty or "original_language" not in df.columns:
        return pd.DataFrame()
    lang_s = df["original_language"].dropna().astype(str).str.strip()
    lang_s = lang_s[lang_s.str.len() > 0]
    if lang_s.empty:
        return pd.DataFrame()
    result = (
        lang_s.value_counts()
        .reset_index()
    )
    result.columns = ["Language", "Count"]
    result["Language"] = result["Language"].apply(lambda x: LANGUAGE_MAP.get(x, x.upper()))
    return result


# ─────────────────────────────────────────────────────────────────
# COUNTRY ANALYSIS — VECTORISED
# ─────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def analyze_countries(df: pd.DataFrame) -> Dict[str, int]:
    """Vectorised country distribution — explode() on production_countries.

    ✅ V9.3 FIX: Normalize using ISO codes (not full names) because
    tmdb_async.py stores production_countries as ISO 3166-1 alpha-2 codes
    (e.g. "SU", "RU"). Previous version tried matching full names like
    "Soviet Union" which never matched the stored code "SU".
    """
    # Map historical/variant ISO codes → modern ISO codes
    CODE_NORMALIZE: Dict[str, str] = {
        "SU": "RU",   # Soviet Union → Russia
        "YU": "RS",   # Yugoslavia → Serbia
        "CS": "CZ",   # Czechoslovakia → Czech Republic
        "DD": "DE",   # East Germany → Germany
        "XG": "DE",   # West Germany → Germany
        "AN": "NL",   # Netherlands Antilles → Netherlands
        "TP": "TL",   # East Timor (old) → Timor-Leste
        "ZR": "CD",   # Zaire → DR Congo
        "BU": "MM",   # Burma → Myanmar
        "XC": "CZ",   # Czechoslovakia (alt) → Czech Republic
    }

    if df.empty or "production_countries" not in df.columns:
        return {}
    country_s = (
        df["production_countries"]
        .dropna()
        .astype(str)
        .str.split(",")
        .explode()
        .str.strip()
    )
    country_s = country_s[country_s.str.len() > 0]
    if country_s.empty:
        return {}
    # Apply ISO code normalization
    country_s = country_s.map(lambda c: CODE_NORMALIZE.get(c, c))
    return country_s.value_counts().to_dict()


# ─────────────────────────────────────────────────────────────────
# TIME-BASED CHARTS — VECTORISED
# ─────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def analyze_movies_per_month(df: pd.DataFrame, date_column: str = "Date") -> pd.DataFrame:
    """Vectorised monthly watch distribution."""
    if df.empty or date_column not in df.columns:
        return pd.DataFrame()
    dates = pd.to_datetime(df[date_column], errors="coerce").dropna()
    if dates.empty:
        return pd.DataFrame()
    month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    counts = dates.dt.month.value_counts().reindex(range(1, 13), fill_value=0)
    return pd.DataFrame({"Month": month_names, "Count": counts.values})


@st.cache_data(show_spinner=False)
def analyze_movies_per_day(df: pd.DataFrame, date_column: str = "Date") -> pd.DataFrame:
    """Vectorised day-of-week watch distribution."""
    if df.empty or date_column not in df.columns:
        return pd.DataFrame()
    dates = pd.to_datetime(df[date_column], errors="coerce").dropna()
    if dates.empty:
        return pd.DataFrame()
    day_names = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    counts = dates.dt.dayofweek.value_counts().reindex(range(7), fill_value=0)
    return pd.DataFrame({"Day": day_names, "Count": counts.values})


# ─────────────────────────────────────────────────────────────────
# RUNTIME
# ─────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def calculate_total_hours(df: pd.DataFrame) -> float:
    """Sum runtime column (minutes) and convert to hours."""
    if df.empty or "runtime" not in df.columns:
        return 0.0
    return float(pd.to_numeric(df["runtime"], errors="coerce").fillna(0).sum()) / 60.0


# ─────────────────────────────────────────────────────────────────
# TOP PEOPLE — VECTORISED
# ─────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def get_top_people(df: pd.DataFrame, column: str, top_n: int = 30) -> List[Tuple[str, int]]:
    """
    Vectorised top actors/directors.
    Handles plain comma-separated strings (e.g. 'actors' column).
    """
    if df.empty or column not in df.columns:
        return []
    people_s = (
        df[column]
        .dropna()
        .astype(str)
        .str.split(",")
        .explode()
        .str.strip()
    )
    people_s = people_s[people_s.str.len() > 0]
    if people_s.empty:
        return []
    return list(people_s.value_counts().head(top_n).items())


@st.cache_data(show_spinner=False)
def get_top_people_with_images(
    df: pd.DataFrame,
    column: str,
    top_n: int = 15,
) -> List[Dict]:
    """
    Build top-N list with TMDB profile images.
    Uses a fast dict accumulation approach (avoids iterrows).
    """
    if df.empty or column not in df.columns:
        return []

    counter: Counter = Counter()
    image_map: Dict[str, str] = {}

    # Only iterate rows where the JSON column is non-null
    mask = df[column].notna()
    for raw in df.loc[mask, column]:
        try:
            people = _json_loads(raw)
            for p in people:
                n = p.get("name", "")
                if not n:
                    continue
                counter[n] += 1
                if p.get("profile_path") and n not in image_map:
                    image_map[n] = p["profile_path"]
        except Exception:
            pass

    if not counter:
        return []

    return [
        {"name": name, "count": count, "profile_path": image_map.get(name, "")}
        for name, count in counter.most_common(top_n)
    ]


# ─────────────────────────────────────────────────────────────────
# RECENT / NEWEST / OLDEST
# ─────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def get_recently_watched(df: pd.DataFrame, date_column: str = "Date", n: int = 20) -> pd.DataFrame:
    if df.empty or date_column not in df.columns:
        return pd.DataFrame()
    df_c = df.copy()
    df_c["parsed_date"] = pd.to_datetime(df_c[date_column], errors="coerce")
    return df_c.dropna(subset=["parsed_date"]).sort_values("parsed_date", ascending=False).head(n)


@st.cache_data(show_spinner=False)
def get_newest_films(df: pd.DataFrame, n: int = 4, min_votes: int = 0) -> pd.DataFrame:
    if df.empty or "release_date" not in df.columns:
        return pd.DataFrame()
    mask = df["release_date"].notna() & (df["release_date"] != "")
    if min_votes > 0 and "vote_count" in df.columns:
        mask = mask & (df["vote_count"] >= min_votes)
    return (
        df[mask]
        .sort_values("release_date", ascending=False)
        .head(n)
    )


@st.cache_data(show_spinner=False)
def get_oldest_films(df: pd.DataFrame, n: int = 4, min_votes: int = 0) -> pd.DataFrame:
    if df.empty or "release_date" not in df.columns:
        return pd.DataFrame()
    mask = df["release_date"].notna() & (df["release_date"] != "")
    if min_votes > 0 and "vote_count" in df.columns:
        mask = mask & (df["vote_count"] >= min_votes)
    return (
        df[mask]
        .sort_values("release_date", ascending=True)
        .head(n)
    )


# ─────────────────────────────────────────────────────────────────
# MOST RE-WATCHED — CRITICAL FIX
# ─────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def get_most_watched_films(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """
    ✅ V9 CRITICAL FIX: Group by (Name, Year) to distinguish remakes.

    The V7/V8 bug: groupby('Name') alone merged:
      - Superman (1978) with Superman (2025) → wrong ×2 count
      - Mean Girls (2004) with Mean Girls (2024) → wrong ×2 count

    ✅ V9.1 FIX: NEVER group by Letterboxd URI.
    In diary.csv, each diary *entry* has a unique URI (e.g. boxd.it/ajYpN9),
    NOT the film URI. Grouping by URI yields zero duplicates.
    Always use (Name, Year) which correctly identifies the same film.
    """
    if df.empty or "Name" not in df.columns:
        return pd.DataFrame()

    # Always group by (Name, Year) — works for both diary.csv and watched.csv.
    # NEVER use Letterboxd URI — it's per diary entry, not per film.
    if "Year" in df.columns:
        group_cols = ["Name", "Year"]
    elif "parsed_year" in df.columns:
        group_cols = ["Name", "parsed_year"]
    else:
        group_cols = ["Name"]

    film_counts = (
        df.groupby(group_cols, as_index=False, dropna=False)
          .size()
          .rename(columns={"size": "WatchCount"})
    )
    film_counts = film_counts[film_counts["WatchCount"] > 1].sort_values(
        "WatchCount", ascending=False
    ).head(n)

    if film_counts.empty:
        return pd.DataFrame()

    # Merge back full metadata from FIRST occurrence of each group
    first_occ = df.drop_duplicates(subset=group_cols, keep="first")
    result = film_counts.merge(first_occ, on=group_cols, how="left")

    # Resolve column collision from merge
    if "WatchCount_x" in result.columns:
        result["WatchCount"] = result["WatchCount_x"]
        result = result.drop(columns=["WatchCount_x", "WatchCount_y"], errors="ignore")

    return result.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────
# MILESTONES
# ─────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def get_milestones(df: pd.DataFrame, date_column: str = "Date") -> Dict:
    """Get viewing milestones (50th, 100th, 250th, 500th, 1000th films)."""
    if df.empty or date_column not in df.columns:
        return {}
    df_c = df.copy()
    df_c["_d"] = pd.to_datetime(df_c[date_column], errors="coerce")
    df_s = df_c.dropna(subset=["_d"]).sort_values("_d")
    milestones = {}
    for m in [50, 100, 150, 200, 250, 300, 500, 1000, 1500, 2000]:
        if len(df_s) >= m:
            milestones[f"{m}th"] = df_s.iloc[m - 1]
    return milestones


@st.cache_data(show_spinner=False)
def get_first_and_last_film(
    df: pd.DataFrame,
    date_column: str = "Date",
    year: Optional[int] = None,
) -> Dict:
    """Get first and last film watched (optionally filtered by year)."""
    if df.empty or date_column not in df.columns:
        return {}
    df_c = df.copy()
    df_c["_d"] = pd.to_datetime(df_c[date_column], errors="coerce")
    df_c = df_c.dropna(subset=["_d"])
    if year:
        df_c = df_c[df_c["_d"].dt.year == year]
    if df_c.empty:
        return {}
    df_s = df_c.sort_values("_d")
    return {"first": df_s.iloc[0], "last": df_s.iloc[-1]}


# ─────────────────────────────────────────────────────────────────
# HIGHLY RATED UNSEEN
# ─────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def get_highly_rated_unseen(
    watchlist_df: pd.DataFrame,
    min_rating: float = 7.5,
    n: int = 5,
) -> pd.DataFrame:
    if watchlist_df.empty or "vote_average" not in watchlist_df.columns:
        return pd.DataFrame()
    return (
        watchlist_df[watchlist_df["vote_average"] >= min_rating]
        .sort_values("vote_average", ascending=False)
        .head(n)
    )
