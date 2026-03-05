"""
visualization.py — V9 Neo-Brutalist Comic Edition

Design rules:
  • CMYK palette: cyan #00E5FF, magenta #FF00A0, yellow #FFDE00, ink #000, paper #FFF
  • Hard offset shadows (8px 8px 0 #000) — ZERO blur
  • Borders: 3–8px solid black
  • ZERO border-radius. ZERO transform:rotate. ZERO slanting.
  • Artist photo containers: aspect-ratio 3/4 — RECTANGULAR, never circles
  • All HTML strings: compact, ZERO blank lines inside (CommonMark rule)
  • Plotly: plot_bgcolor / paper_bgcolor = '#FFFFFF', theme=None → fullscreen fix
"""

from __future__ import annotations

import html as _html
import json
import os
import random
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


# ─────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────
TMDB_IMG  = "https://image.tmdb.org/t/p/w342"
TMDB_IMG_L = "https://image.tmdb.org/t/p/w500"

CMYK = {
    "cyan":    "#00E5FF",
    "magenta": "#FF00A0",
    "yellow":  "#FFDE00",
    "red":     "#FF003C",
    "blue":    "#0088FF",
    "green":   "#00D34D",
    "orange":  "#FF6600",
    "purple":  "#9D00FF",
    "ink":     "#000000",
    "paper":   "#FFFFFF",
    "paper_dk":"#F0F0F0",
}

# Rotating card accent colors
CARD_COLORS = [
    CMYK["cyan"], CMYK["magenta"], CMYK["yellow"],
    CMYK["green"], CMYK["red"], CMYK["blue"],
]

# Country code → full name
COUNTRY_NAME_MAP: Dict[str, str] = {
    "US":"United States","GB":"United Kingdom","FR":"France","DE":"Germany",
    "IT":"Italy","JP":"Japan","KR":"South Korea","CN":"China","IN":"India",
    "ES":"Spain","RU":"Russia","AU":"Australia","CA":"Canada","BR":"Brazil",
    "MX":"Mexico","AR":"Argentina","SE":"Sweden","DK":"Denmark","NO":"Norway",
    "FI":"Finland","NL":"Netherlands","BE":"Belgium","CH":"Switzerland",
    "AT":"Austria","PL":"Poland","CZ":"Czech Republic","HU":"Hungary",
    "RO":"Romania","PT":"Portugal","GR":"Greece","TR":"Turkey","IR":"Iran",
    "IL":"Israel","EG":"Egypt","ZA":"South Africa","NG":"Nigeria","KE":"Kenya",
    "TH":"Thailand","ID":"Indonesia","MY":"Malaysia","PH":"Philippines",
    "TW":"Taiwan","HK":"Hong Kong","NZ":"New Zealand","IE":"Ireland",
    "SG":"Singapore","PK":"Pakistan","BD":"Bangladesh","VN":"Vietnam",
    "UA":"Ukraine","HR":"Croatia","RS":"Serbia","BG":"Bulgaria","SK":"Slovakia",
    "SI":"Slovenia","LT":"Lithuania","LV":"Latvia","EE":"Estonia",
    "SU":"Soviet Union","YU":"Yugoslavia","CS":"Czechoslovakia","XK":"Kosovo",
    "DD":"East Germany","TZ":"Tanzania","UZ":"Uzbekistan","KZ":"Kazakhstan",
    "MK":"North Macedonia","BA":"Bosnia & Herzegovina","AL":"Albania",
    "CL":"Chile","CO":"Colombia","PE":"Peru","VE":"Venezuela","EC":"Ecuador",
    "UY":"Uruguay","PY":"Paraguay","BO":"Bolivia","CU":"Cuba","DO":"Dominican Republic",
    "MA":"Morocco","DZ":"Algeria","TN":"Tunisia","LY":"Libya","GH":"Ghana",
    "ET":"Ethiopia","SN":"Senegal","CI":"Ivory Coast","CM":"Cameroon","AO":"Angola",
    "SA":"Saudi Arabia","AE":"United Arab Emirates","QA":"Qatar","IQ":"Iraq","SY":"Syria",
    "LB":"Lebanon","JO":"Jordan","KW":"Kuwait","OM":"Oman","YE":"Yemen",
    "AF":"Afghanistan","NP":"Nepal","LK":"Sri Lanka","MM":"Myanmar",
    "KH":"Cambodia","LA":"Laos","MN":"Mongolia",
}

# Funny loading messages
LOADERS: List[str] = [
    "🎬 Consulting the Auteurs...",
    "📽️ Rewinding the film reels...",
    "🍿 Judging your 5-star ratings...",
    "🎭 Arguing with Letterboxd servers...",
    "🎞️ Developing the negatives...",
    "🎥 Calculating your pretentiousness index...",
    "📺 Loading movies faster than you rate them...",
    "🎬 Asking Kubrick's ghost for permission...",
    "🍿 Counting how many times you watched The Room...",
    "📽️ Cross-referencing against the Letterboxd hivemind...",
    "🎭 Syncing with the ghost of Roger Ebert...",
    "🎞️ Checking if you've actually seen Jeanne Dielman...",
]

# Tab easter eggs
TAB_EGGS: Dict[str, str] = {
    "watched":    "★ 'The only way to watch more films is to watch more films.' — You, probably",
    "watchlist":  "📋 Your watchlist is just organized procrastination.",
    "ratings":    "⭐ Remember: giving 5 stars too freely is a war crime.",
    "recent":     "🕐 That last film? Your therapist would have notes.",
    "artists":    "🎭 Auteur theory is just astrology for film bros.",
    "milestones": "🏆 Achievement unlocked: Severe touch-grass deficiency.",
    "stats":      "📊 These numbers mean nothing. And yet, you feel seen.",
    "map":        "🗺️ Cinema is the universal language. (Subtitles help.)",
    "roulette":   "🎰 Fate has better taste than your algorithm.",
}


# ─────────────────────────────────────────────────────────────────
# CSS LOADER
# ─────────────────────────────────────────────────────────────────
def load_custom_css() -> None:
    """Inject the Neo-Brutalist Comic CSS into the Streamlit page.

    Loads from split CSS files: tokens → layout → components → animations.
    Falls back to monolithic styles.css if split files don't exist.
    """
    static = Path(__file__).parent / "static"
    css_parts = []
    for name in ("tokens.css", "layout.css", "components.css", "animations.css"):
        p = static / name
        if p.exists():
            css_parts.append(p.read_text(encoding="utf-8"))

    if css_parts:
        css = "\n".join(css_parts)
    else:
        # Fallback: monolithic styles.css
        fallback = static / "styles.css"
        css = fallback.read_text(encoding="utf-8") if fallback.exists() else ""

    st.markdown(
        f'<style>{css}</style>',
        unsafe_allow_html=True,
    )


def random_loader() -> str:
    """Return a random funny loading message."""
    return random.choice(LOADERS)


# ─────────────────────────────────────────────────────────────────
# LAYOUT HELPERS
# ─────────────────────────────────────────────────────────────────
def _divider() -> None:
    st.markdown('<hr class="nb-hr">', unsafe_allow_html=True)


def _section_header(text: str) -> None:
    st.markdown(
        f'<div class="nb-sec-hdr">{text}</div>',
        unsafe_allow_html=True,
    )


def _flag(code: str) -> str:
    """ISO-2 country code → flag emoji."""
    try:
        return chr(0x1F1E6 + ord(code[0]) - 65) + chr(0x1F1E6 + ord(code[1]) - 65)
    except Exception:
        return "🌍"


# ─────────────────────────────────────────────────────────────────
# TITLE CARD
# ─────────────────────────────────────────────────────────────────
def display_brutalist_title() -> None:
    """Neo-Brutalist Comic title card."""
    st.markdown(
        '<div class="nb-badge">V9 NEO-BRUTALIST COMIC EDITION</div>'
        '<div style="display:flex;flex-direction:column;align-items:flex-start;gap:10px;margin-bottom:6px;">'
        '<span class="nb-title-cyan">LETTERBOXD</span>'
        '<span class="nb-title-yellow">ANALYZER</span>'
        '</div>'
        '<div class="nb-subtitle">VALIDATING YOUR KINO TASTE SINCE 2024</div>'
        '<br>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────
# METRIC CARD
# ─────────────────────────────────────────────────────────────────
_METRIC_COLORS = ["nb-m-cyan", "nb-m-mag", "nb-m-green", "nb-m-blue",
                  "nb-m-purple", "nb-m-yellow", "nb-m-red"]
_metric_idx: int = 0

def display_metric_card(value: str, label: str, joke: str = "") -> None:
    """Render a Neo-Brutalist metric panel."""
    global _metric_idx
    cls = _METRIC_COLORS[_metric_idx % len(_METRIC_COLORS)]
    _metric_idx += 1
    joke_html = f'<div class="nb-mjoke">{joke}</div>' if joke else ''
    st.markdown(
        f'<div class="nb-metric {cls}">'
        f'<div class="nb-mlabel">{label}</div>'
        f'<div class="nb-mval">{value}</div>'
        f'{joke_html}'
        f'</div>',
        unsafe_allow_html=True,
    )


def reset_metric_counter() -> None:
    """Reset rotating metric color index (call once per tab)."""
    global _metric_idx
    _metric_idx = 0


# ─────────────────────────────────────────────────────────────────
# PLOTLY — NEO-BRUTALIST THEME (fixes fullscreen white-on-white bug)
# ─────────────────────────────────────────────────────────────────
def _apply_brutal_plotly_theme(fig: go.Figure, title: str = "") -> go.Figure:
    """
    Apply Neo-Brutalist theme to a Plotly figure.

    ✅ V9 FIX: Sets plot_bgcolor and paper_bgcolor to white (#FFFFFF) so
    that when the chart is expanded to fullscreen, the background is white
    with black text — preventing invisible white-on-white labels.

    Use st.plotly_chart(fig, theme=None) to bypass Streamlit's default
    light theme and preserve these explicit colors.
    """
    bar_colors = [CMYK["cyan"], CMYK["magenta"], CMYK["yellow"],
                  CMYK["green"], CMYK["red"], CMYK["blue"], CMYK["orange"]]

    fig.update_layout(
        title       = title,
        font        = {"family": "Space Grotesk, Arial, sans-serif",
                       "color": "#000000", "size": 12},
        plot_bgcolor  = "#FFFFFF",  # White paper — visible in fullscreen
        paper_bgcolor = "#FFFFFF",
        title_font  = {"family": "Chivo, Impact, sans-serif",
                       "color": "#000000", "size": 16},
        xaxis = {
            "showgrid":     True,
            "gridcolor":    "#000000",
            "gridwidth":    1,
            "linecolor":    "#000000",
            "linewidth":    3,
            "tickfont":     {"color": "#000000", "size": 11},
            "title_font":   {"color": "#000000"},
            "zeroline":     True,
            "zerolinecolor":"#000000",
            "zerolinewidth": 2,
        },
        yaxis = {
            "showgrid":     True,
            "gridcolor":    "#E0E0E0",
            "gridwidth":    1,
            "linecolor":    "#000000",
            "linewidth":    3,
            "tickfont":     {"color": "#000000", "size": 11},
            "title_font":   {"color": "#000000"},
        },
        margin = {"t": 50, "b": 60, "l": 80, "r": 20},
        showlegend = False,
    )

    # Apply cycling bar colors and add black border to each bar
    if hasattr(fig, "data") and fig.data:
        for i, trace in enumerate(fig.data):
            if hasattr(trace, "marker"):
                n = max(1, len(trace.x) if hasattr(trace, "x") and trace.x is not None else 1)
                colors = [bar_colors[j % len(bar_colors)] for j in range(n)]
                fig.update_traces(
                    marker_color=colors,
                    marker_line_color="#000000",
                    marker_line_width=2,
                    selector={"type": "bar"},
                )
                break  # only update first bar trace

    return fig


def plot_bar_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str = "",
    top_n: int = 20,
) -> None:
    """Render a Neo-Brutalist bar chart. theme=None prevents Streamlit overriding colors."""
    if df.empty:
        return
    plot_df = df.head(top_n)
    fig = px.bar(plot_df, x=x_col, y=y_col, title=title)
    fig = _apply_brutal_plotly_theme(fig, title)
    st.plotly_chart(fig, theme=None, width="stretch")


# ─────────────────────────────────────────────────────────────────
# FILM GRIDS
# ─────────────────────────────────────────────────────────────────
def _poster_html(row: Any, show_rating: bool = False, badge: str = "") -> str:
    """Build a compact Neo-Brutalist film card HTML string (zero blank lines)."""
    name = _html.escape(str(row.get("Name", "Unknown"))[:40])
    year = row.get("Year", row.get("parsed_year", ""))
    year = f"{int(year)}" if pd.notna(year) and str(year).strip() else ""
    poster = row.get("poster_path", "")
    img_url = f"{TMDB_IMG}{poster}" if poster and str(poster).startswith("/") else ""
    rating_val = row.get("Rating", "")
    stars = ""
    if show_rating and pd.notna(rating_val):
        try:
            n = float(rating_val)
            stars = "★" * int(n) + ("½" if n % 1 >= 0.5 else "")
        except Exception:
            pass
    watch_count = row.get("WatchCount", "")
    badge_html = f'<div class="nb-abadge">{badge or (f"×{int(watch_count)}" if watch_count and int(watch_count) > 1 else "")}</div>' if (badge or (watch_count and str(watch_count) != "1")) else ""
    img_part = f'<img src="{img_url}" alt="{name}" loading="lazy">' if img_url else f'<div style="width:100%;height:100%;display:flex;align-items:center;justify-content:center;font-size:2.5rem;background:#FFDE00;">🎬</div>'
    year_part = f'<div class="nb-fyear">{year}</div>' if year else ""
    rating_part = f'<div class="nb-frating">{stars}</div>' if stars else ""
    return (
        f'<div class="nb-fc">'
        f'{badge_html}'
        f'<div class="nb-fp">{img_part}</div>'
        f'<div class="nb-fcap">'
        f'<div class="nb-ftitle">{name}</div>'
        f'{year_part}'
        f'{rating_part}'
        f'</div>'
        f'</div>'
    )


def display_film_grid(
    df: pd.DataFrame,
    title: str = "",
    show_rating: bool = False,
    cols_count: int = 4,
    square: bool = False,
) -> None:
    """Render a grid of film cards."""
    if df.empty:
        return
    if title:
        _section_header(title)
    cards = "".join(_poster_html(row, show_rating) for row in df.to_dict("records"))
    sq_cls = " nb-fg-sq" if square else ""
    st.markdown(
        f'<div class="nb-fg nb-fg-{cols_count}{sq_cls}">{cards}</div>',
        unsafe_allow_html=True,
    )


def display_film_grid_large(df: pd.DataFrame, title: str = "", n: int = 8, square: bool = False) -> None:
    """Render a film grid showing up to n films."""
    cols = 5 if n >= 10 else 4
    display_film_grid(df.head(n), title, show_rating=True, cols_count=cols, square=square)


# ─────────────────────────────────────────────────────────────────
# PEOPLE / ARTIST CARDS — RECTANGULAR
# ─────────────────────────────────────────────────────────────────
def _person_html(person: dict, show_count: bool = True) -> str:
    """
    Build a compact rectangular Neo-Brutalist artist card.
    ✅ V9 FIX: aspect-ratio 3/4, border-radius: 0 — RECTANGULAR, never circles.
    Zero blank lines in the HTML string.
    """
    name    = _html.escape(str(person.get("name", "Unknown")))
    profile = person.get("profile_path", "")
    count   = person.get("count", 0)
    avg_r   = person.get("avg_rating", None)
    img_url = f"{TMDB_IMG}{profile}" if profile and str(profile).startswith("/") else ""
    photo_html = (
        f'<div class="nb-aphoto"><img src="{img_url}" alt="{name}" loading="lazy"></div>'
        if img_url else
        f'<div class="nb-anophoto">🎬</div>'
    )
    count_label = f'{int(count)} FILMS'
    if avg_r is not None:
        count_label += f' · {avg_r:.1f}★'
    count_part = f'<div class="nb-acount">{count_label}</div>' if show_count else ''
    return (
        f'<div class="nb-ac">'
        f'{photo_html}'
        f'<div class="nb-ainfo">'
        f'<div class="nb-aname">{name}</div>'
        f'{count_part}'
        f'</div>'
        f'</div>'
    )


def display_people_with_images(
    people_data: List[dict],
    title: str = "",
    badge: str = "",
) -> None:
    """Render rectangular Neo-Brutalist artist cards."""
    if not people_data:
        return
    if title:
        _section_header(title)
    cards = "".join(_person_html(p) for p in people_data)
    st.markdown(
        f'<div class="nb-ag">{cards}</div>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────
# TOP LISTS (leaderboard)
# ─────────────────────────────────────────────────────────────────
def display_top_list(
    items: List[Tuple[str, int]],
    header: str,
    icon: str = "",
) -> None:
    """
    Render a ranked leaderboard list.
    ✅ ZERO blank lines inside HTML — solves CommonMark rendering bug.
    """
    if not items:
        return
    rows = "".join(
        f'<div class="nb-li">'
        f'<span class="nb-lrank">{rank}</span>'
        f'<span class="nb-lname">{name}</span>'
        f'<span class="nb-lval">{count}</span>'
        f'</div>'
        for rank, (name, count) in enumerate(items, 1)
    )
    st.markdown(
        f'<div class="nb-lw">'
        f'<div class="nb-lhdr">{icon} {header}</div>'
        f'{rows}'
        f'</div>',
        unsafe_allow_html=True,
    )


def display_top_countries(country_data: Dict[str, int], top_n: int = 25) -> None:
    """Render country leaderboard with flag emojis."""
    if not country_data:
        return
    _section_header("COUNTRIES — FULL RANKINGS")
    sorted_countries = sorted(country_data.items(), key=lambda x: x[1], reverse=True)[:top_n]
    rows = "".join(
        f'<div class="nb-li">'
        f'<span class="nb-lrank">{rank}</span>'
        f'<span class="nb-lname">{_flag(code)} {COUNTRY_NAME_MAP.get(code, code)}</span>'
        f'<span class="nb-lval">{count}</span>'
        f'</div>'
        for rank, (code, count) in enumerate(sorted_countries, 1)
    )
    st.markdown(
        f'<div class="nb-lw">'
        f'<div class="nb-lhdr">🗺️ BY COUNTRY</div>'
        f'{rows}'
        f'</div>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────
# REWATCH COUNTS
# ─────────────────────────────────────────────────────────────────
def display_rewatch_counts(df: pd.DataFrame) -> None:
    """
    Render rewatch count badges for most-watched films.
    ✅ V9 FIX: compact HTML — ZERO blank lines — always renders.
    """
    if df.empty or "WatchCount" not in df.columns:
        return
    badges = "".join(
        f'<div class="nb-rw-badge">'
        f'<div class="nb-rw-cnt">×{int(row["WatchCount"])}</div>'
        f'<div class="nb-rw-lbl">{_html.escape(str(row.get("Name",""))[:22])}</div>'
        f'</div>'
        for row in df.to_dict("records")
        if pd.notna(row.get("WatchCount")) and int(row["WatchCount"]) > 1
    )
    if badges:
        st.markdown(
            f'<div class="nb-rw-grid">{badges}</div>',
            unsafe_allow_html=True,
        )


# ─────────────────────────────────────────────────────────────────
# RECENTLY WATCHED TIMELINE
# ─────────────────────────────────────────────────────────────────
def display_recently_watched(df: pd.DataFrame) -> None:
    """Render the recent-activity timeline."""
    if df.empty:
        return
    _section_header("RECENTLY WATCHED")
    parts = []
    for row in df.head(15).to_dict("records"):
        name   = _html.escape(str(row.get("Name", "Unknown")))
        year   = row.get("Year", row.get("parsed_year", ""))
        year_s = f"{int(year)}" if pd.notna(year) and str(year).strip() else ""
        date_s = str(row.get("parsed_date", row.get("Date", "")))[:10]
        poster = row.get("poster_path", "")
        img_url = f"{TMDB_IMG}{poster}" if poster and str(poster).startswith("/") else ""
        img_html = (
            f'<div class="nb-rw-thumb"><img src="{img_url}" alt="{name}" loading="lazy"></div>'
            if img_url else
            f'<div class="nb-rw-thumb" style="background:#FFDE00;display:flex;align-items:center;justify-content:center;width:75px;height:112px;border:5px solid #000;font-size:2rem;">🎬</div>'
        )
        year_badge  = f'<span class="nb-rw-meta">{year_s}</span>' if year_s else ""
        date_badge  = f'<span class="nb-rw-meta">{date_s}</span>' if date_s else ""
        parts.append(
            f'<div class="nb-rw-item">'
            f'{img_html}'
            f'<div>'
            f'<div class="nb-rw-title">{name}</div>'
            f'{year_badge}{date_badge}'
            f'</div>'
            f'</div>'
        )
    st.markdown("".join(parts), unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────
# MILESTONE CARD
# ─────────────────────────────────────────────────────────────────
def display_milestone_card(label: str, row: Any) -> None:
    """Render a single milestone block."""
    name   = str(row.get("Name", "Unknown"))
    year   = row.get("Year", row.get("parsed_year", ""))
    year_s = f"{int(year)}" if pd.notna(year) and str(year).strip() else ""
    date_s = str(row.get("parsed_date", row.get("Date", "")))[:10]
    poster = row.get("poster_path", "")
    img_url = f"{TMDB_IMG}{poster}" if poster and str(poster).startswith("/") else ""
    img_part = (
        f'<div style="border:5px solid #000;box-shadow:6px 6px 0 #000;overflow:hidden;width:120px;flex-shrink:0;margin-right:20px;">'
        f'<img src="{img_url}" alt="{name}" loading="lazy" style="width:100%;display:block;border-radius:0 !important;">'
        f'</div>'
        if img_url else ""
    )
    st.markdown(
        f'<div class="nb-ms">'
        f'<div style="display:flex;align-items:flex-start;">'
        f'{img_part}'
        f'<div>'
        f'<div class="nb-msnum">{label}</div>'
        f'<div class="nb-mstitle">{name}</div>'
        f'<div style="margin-top:6px;">'
        f'<span style="background:#000;color:#FFDE00;font-family:Chivo,Impact,sans-serif;font-size:.85rem;padding:3px 10px;border:2px solid #000;display:inline-block;margin-right:8px;">{year_s}</span>'
        f'<span style="background:#00E5FF;color:#000;font-family:Space Grotesk,sans-serif;font-weight:900;font-size:.85rem;padding:3px 10px;border:2px solid #000;display:inline-block;">{date_s}</span>'
        f'</div>'
        f'</div>'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────
# WORLD MAP
# ─────────────────────────────────────────────────────────────────
def create_world_map(country_data: Dict[str, int]) -> None:
    """
    Render a choropleth world cinema map.
    ✅ V9 FIX: locationmode='country names' (was invalid 'ISO-3166-1-alpha-2').
    ✅ V9 FIX: plot_bgcolor and paper_bgcolor = white → fullscreen readable.
    """
    if not country_data:
        return

    codes  = list(country_data.keys())
    counts = list(country_data.values())
    names  = [COUNTRY_NAME_MAP.get(c, c) for c in codes]

    fig = go.Figure(go.Choropleth(
        locations     = names,
        z             = counts,
        text          = names,
        locationmode  = "country names",
        colorscale    = [
            [0.0, "#F0F0F0"],
            [0.2, "#00E5FF"],
            [0.5, "#FF00A0"],
            [0.8, "#FFDE00"],
            [1.0, "#000000"],
        ],
        autocolorscale = False,
        reversescale   = False,
        marker_line_color = "#000000",
        marker_line_width = 1.0,
        colorbar = {
            "title": "Films",
            "thickness": 16,
            "len": 0.7,
            "tickfont": {"color": "#000000"},
            "titlefont": {"color": "#000000", "family": "Chivo, Impact"},
            "outlinecolor": "#000000",
            "outlinewidth": 2,
        },
        hovertemplate = "<b>%{text}</b><br>Films: %{z}<extra></extra>",
    ))

    fig.update_layout(
        geo = {
            "showframe":      True,
            "framecolor":     "#000000",
            "framewidth":     2,
            "showcoastlines": True,
            "coastlinecolor": "#000000",
            "showland":       True,
            "landcolor":      "#F0F0F0",
            "showocean":      True,
            "oceancolor":     "#FFFFFF",
            "showlakes":      True,
            "lakecolor":      "#FFFFFF",
            "projection":     {"type": "natural earth"},
            "bgcolor":        "#FFFFFF",
            "showcountries":  True,
            "countrycolor":   "#CCCCCC",
        },
        plot_bgcolor  = "#FFFFFF",
        paper_bgcolor = "#FFFFFF",
        font          = {"family": "Space Grotesk, Arial", "color": "#000000"},
        height        = 500,
        margin        = {"t": 10, "b": 10, "l": 0, "r": 0},
    )

    st.plotly_chart(fig, theme=None, width="stretch")


# ─────────────────────────────────────────────────────────────────
# WATCHLIST ROULETTE
# ─────────────────────────────────────────────────────────────────
def display_watchlist_roulette(df: pd.DataFrame) -> None:
    """
    Neo-Brutalist watchlist roulette.

    ✅ V9 PERFORMANCE FIX:
    - On first render, build a pool list and store in session_state.
    - On every spin, only call df.sample(1) on the pre-filtered pool —
      no TMDB API calls, no re-enrichment.
    - Genre filter resets the pool but doesn't re-enrich.
    - SPIN AGAIN button is always green via CSS.
    """
    _section_header("WATCHLIST ROULETTE")
    st.markdown(
        '<div class="nb-rou-wrap">'
        '<div class="nb-rou-title">WATCHLIST<br>ROULETTE</div>',
        unsafe_allow_html=True,
    )

    # ── Genre filter ─────────────────────────────────────────────
    all_genres: List[str] = []
    if "genres" in df.columns:
        all_genres = sorted(set(
            g.strip()
            for gs in df["genres"].dropna()
            for g in str(gs).split(",")
            if g.strip()
        ))

    genre_filter = "All Genres"
    if all_genres:
        genre_filter = st.selectbox(
            "Filter by genre:",
            ["All Genres"] + all_genres,
            key="roulette_genre_filter",
        )

    # ── Build / invalidate pool ───────────────────────────────────
    pool_key   = "roulette_pool"
    filter_key = "roulette_genre"

    # Invalidate pool when filter changes
    if st.session_state.get(filter_key) != genre_filter:
        st.session_state[filter_key] = genre_filter
        if pool_key in st.session_state:
            del st.session_state[pool_key]

    # Build pool once (not on every spin)
    if pool_key not in st.session_state:
        pool_df = df.copy()
        if genre_filter != "All Genres" and "genres" in pool_df.columns:
            pool_df = pool_df[
                pool_df["genres"].fillna("").str.contains(genre_filter, case=False)
            ]
        if pool_df.empty:
            st.warning("🎬 No films match that genre filter. Try a different one!")
            st.markdown('</div>', unsafe_allow_html=True)
            return
        st.session_state[pool_key] = pool_df.to_dict("records")

    pool = st.session_state[pool_key]

    # ── Spin buttons ─────────────────────────────────────────────
    col_spin, col_respin = st.columns([2, 1])
    with col_spin:
        spin = st.button("🎰 SPIN THE WHEEL", key="roulette_spin", use_container_width=True)
    with col_respin:
        respin = st.button("RE-SPIN (COWARD)", key="roulette_respin", use_container_width=True)

    if spin or respin:
        st.session_state["roulette_pick"] = random.choice(pool)

    film = st.session_state.get("roulette_pick")

    # ── Display result ────────────────────────────────────────────
    if film:
        name     = _html.escape(str(film.get("Name", "Unknown Film")))
        year     = film.get("Year", film.get("parsed_year", ""))
        year_s   = f"{int(year)}" if year and pd.notna(year) and str(year).strip() else ""
        overview = _html.escape(str(film.get("overview", "No synopsis available."))[:400])
        poster   = film.get("poster_path", "")
        img_url  = f"{TMDB_IMG_L}{poster}" if poster and str(poster).startswith("/") else ""
        vote_avg = film.get("vote_average", "")
        runtime  = film.get("runtime", "")
        rt_s     = f"{int(runtime)} min" if runtime and pd.notna(runtime) and str(runtime) != "0" else ""
        va_s     = f"{float(vote_avg):.1f}/10 TMDB" if vote_avg and pd.notna(vote_avg) and str(vote_avg) != "0.0" else ""

        img_part = (
            f'<div style="border:8px solid #000;box-shadow:8px 8px 0 #000;overflow:hidden;width:200px;flex-shrink:0;margin-right:30px;background:#FFDE00;">'
            f'<img src="{img_url}" alt="{name}" style="width:100%;display:block;border-radius:0 !important;filter:contrast(1.15);">'
            f'</div>'
            if img_url else
            f'<div style="border:8px solid #000;box-shadow:8px 8px 0 #000;width:200px;height:300px;background:#FFDE00;flex-shrink:0;margin-right:30px;display:flex;align-items:center;justify-content:center;font-size:4rem;">🎬</div>'
        )
        meta_parts = ""
        if year_s:
            meta_parts += f'<span style="background:#FFDE00;color:#000;border:3px solid #000;font-family:Chivo,Impact,sans-serif;font-weight:900;font-size:.95rem;padding:4px 12px;margin-right:10px;box-shadow:3px 3px 0 #000;display:inline-block;">{year_s}</span>'
        if va_s:
            meta_parts += f'<span style="background:#00E5FF;color:#000;border:3px solid #000;font-family:Space Grotesk,sans-serif;font-weight:900;font-size:.95rem;padding:4px 12px;margin-right:10px;box-shadow:3px 3px 0 #000;display:inline-block;">{va_s}</span>'
        if rt_s:
            meta_parts += f'<span style="background:#FF003C;color:#FFF;border:3px solid #000;font-family:Space Grotesk,sans-serif;font-weight:900;font-size:.95rem;padding:4px 12px;box-shadow:3px 3px 0 #000;display:inline-block;">{rt_s}</span>'

        st.markdown(
            f'<div class="nb-target">'
            f'<div class="nb-tbadge">🎯 TARGET ACQUIRED</div>'
            f'<div style="display:flex;flex-wrap:wrap;align-items:flex-start;">'
            f'{img_part}'
            f'<div style="flex:1;min-width:220px;">'
            f'<div class="nb-tmovietitle">{name}</div>'
            f'<div style="margin-bottom:14px;">{meta_parts}</div>'
            f'<div style="font-family:Space Grotesk,sans-serif;font-size:.95rem;line-height:1.6;color:#000;background:#F0F0F0;padding:15px;border:3px solid #000;box-shadow:4px 4px 0 #000;">{overview}</div>'
            f'</div>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown('</div>', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────
# TAB EASTER EGGS
# ─────────────────────────────────────────────────────────────────
def display_tab_easter_egg(tab: str) -> None:
    """Render a straight (never rotated) easter egg badge."""
    msg = TAB_EGGS.get(tab, "🎬 Cinema is life.")
    st.markdown(
        f'<div style="margin-top:20px;text-align:right;">'
        f'<span class="nb-egg">{msg}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────
# MAP TAB FOOTER
# ─────────────────────────────────────────────────────────────────
def display_map_footer() -> None:
    """Mandatory 'VIBECODED WITH CLAUDE' footer for the Map tab."""
    st.markdown(
        '<div class="nb-mapfooter">VIBECODED WITH CLAUDE</div>',
        unsafe_allow_html=True,
    )


def display_global_footer() -> None:
    """Global footer shown on all tabs: left = MADE WITH LOVE AND POTATO PC, right = COPYLEFT.2026."""
    st.markdown(
        '<div class="nb-globalfooter">'
        '<span class="nb-gf-left">MADE WITH LOVE AND POTATO PC</span>'
        '<span class="nb-gf-right">COPYLEFT.2026</span>'
        '</div>',
        unsafe_allow_html=True,
    )
