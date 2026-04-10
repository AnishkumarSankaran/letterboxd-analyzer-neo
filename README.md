<div align="center">

# 🎬 LETTERBOXD ANALYZER
### V9 NEO-BRUTALIST COMIC EDITION

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.35+-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)](https://streamlit.io)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue?style=flat-square)](https://www.gnu.org/licenses/gpl-3.0)
[![TMDB](https://img.shields.io/badge/Powered%20by-TMDB-01D277?style=flat-square)](https://www.themoviedb.org)
[![Turso](https://img.shields.io/badge/Cache-Turso%20%2F%20SQLite-4FF8D2?style=flat-square)](https://turso.tech)
[![Sentry](https://img.shields.io/badge/Monitoring-Sentry-362D59?style=flat-square)](https://sentry.io)

**A professional-grade, blazing-fast analytics tool for your Letterboxd film diary.**  
Upload your data. Get obsessed with your own taste.

[🚀 Live Demo](#) · [📖 Documentation](#installation) · [🐛 Report Bug](https://github.com/AnishkumarSankaran/letterboxd-neo-brutalist/issues) · [✨ Request Feature](https://github.com/AnishkumarSankaran/letterboxd-neo-brutalist/issues)

</div>

---

## ✨ What Is This?

A **self-hosted Letterboxd data analyzer** that gives you deep analytics about your film-watching habits — the kind of stats Letterboxd itself doesn't show you.

Export your data from Letterboxd, upload the ZIP, and in under 2 minutes you get:

- 📊 **9 analytics tabs** — Watched, Ratings, Recent, Artists, Milestones, Stats, Map, Roulette, Watchlist
- 🎨 **Neo-Brutalist UI** — CMYK colour palette, hard box shadows, Chivo font, zero border-radius
- ⚡ **Fast async TMDB fetching** — 2,000+ films enriched in ~110s cold, ~8s warm (SQLite / Turso cache)
- 🗺️ **World map** — Choropleth of every production country in your watched history
- 🎰 **Watchlist Roulette** — Can't decide what to watch? Spin the wheel (pre-built pool, zero lag)
- 📈 **Genre, language, decade patterns** — Understand your actual taste
- 🎭 **Artist cards** — Most-watched and highest-rated directors and actors with profile photos
- 🏆 **Milestones** — First and last film of the year, every 50th diary entry, most re-watched
- ☁️ **Cloud-ready** — Turso backend persists the TMDB cache across Streamlit Cloud restarts

---

## 🏗️ Architecture

```
letterboxd-neo-brutalist/
├── app.py                  # Main Streamlit app, enrichment pipeline, tab dispatcher
├── data_processing.py      # Vectorised analytics (genres, decades, milestones, rewatches)
├── database.py             # SQLite/Turso cache engine — bulk lookups, batch writes, WAL mode
├── tmdb_async.py           # Async TMDB client (aiohttp, 40 concurrent req/s, single session)
├── visualization.py        # Neo-Brutalist UI components, Plotly chart wrappers, world map
├── utils.py                # Validators, formatters, cache key helpers
├── static/
│   ├── tokens.css          # Design tokens — CMYK palette, fonts, shadows
│   ├── layout.css          # Global layout, sidebar, tabs, buttons, inputs
│   ├── components.css      # Film grids, artist cards, metric cards, roulette
│   └── animations.css      # Keyframe animations (pop-in, blinking cursor)
├── requirements.txt
└── .streamlit/
    ├── secrets.toml        # API keys (not committed to git)
    └── config.toml
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.11 or higher
- A [TMDB API key](https://www.themoviedb.org/settings/api) (free)
- Your [Letterboxd data export](https://letterboxd.com/settings/data/) (ZIP file)

### Installation

```bash
# Clone the repository
git clone https://github.com/AnishkumarSankaran/letterboxd-neo-brutalist.git
cd letterboxd-neo-brutalist

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate        # macOS/Linux
venv\Scripts\activate           # Windows

# Install dependencies
pip install -r requirements.txt
```

### Configuration

Create `.streamlit/secrets.toml`:

```toml
TMDB_API_KEY = "your_tmdb_api_key_here"

# Optional: Turso cloud database (leave empty to use local SQLite)
# TURSO_DATABASE_URL = "libsql://your-db.turso.io"
# TURSO_AUTH_TOKEN   = "your-turso-token"

# Optional: Sentry error monitoring
# SENTRY_DSN = "https://your-sentry-dsn"
```

### Run

```bash
streamlit run app.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## 📊 Feature Overview

| Tab | What You Get |
|-----|-------------|
| 🎬 **Watched** | Total films, hours, genres, languages, monthly/weekly/decade patterns, top actors and directors |
| ⭐ **Ratings** | Rating distribution, 5-star grid, lowest-rated grid — sourced from `ratings.csv` |
| 🕐 **Recent** | Recently watched timeline, newest releases you've seen, oldest releases you've watched |
| 🎭 **Artists** | Most-watched directors and actors with profile photos; highest-rated by your own ratings |
| 🏆 **Milestones** | First & last film of the year (diary-date accurate), every 50th diary entry, most re-watched |
| 📊 **Stats** | Highest/lowest TMDB-rated, most popular, most obscure, longest, shortest |
| 🗺️ **Map** | World choropleth map of production countries with top-country leaderboard |
| 🎰 **Roulette** | Random film picker from your watchlist — pre-built pool, instant spin |
| 📋 **Watchlist** | Genre breakdown, critically acclaimed picks, crowd favourites, hidden gems |

---

## ⚡ Performance

| Scenario | Time |
|----------|------|
| Cold start (2,000 films, empty cache) | ~110 seconds |
| Warm start (same films, full cache) | ~8 seconds |
| Subsequent reruns (no file change) | ~5 seconds |
| Tab navigation after enrichment | < 1 second |

Performance is achieved through:
- **Single aiohttp session** across all TMDB batches — no repeated TLS handshakes
- **B-tree indexed SQLite** — one `IN (...)` query for all films (no N+1 loop)
- **`@st.cache_data`** on all heavy analytics functions
- **Vectorised pandas `explode()`** instead of `iterrows()` loops
- **`optimise_dtypes()`** — float64→float32, low-cardinality→Categorical
- **Session state caching** — enriched DataFrames stored per session, not recomputed on tab switch

---

## 🗄️ Database & Cache

Film metadata fetched from TMDB is cached in `movie_cache.db` (SQLite, WAL mode).

- Failed lookups are stored with a negative synthetic TMDB ID and retried after 7 days.
- Cache keys are deterministic MD5 hashes of normalised title + year — consistent across restarts.
- The `cache_key` column has a B-tree index for O(log n) lookups.

**Cloud deployment (Turso):** Set `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` in `.streamlit/secrets.toml` to switch to cloud-hosted SQLite. A pre-populated Turso database can serve thousands of users without re-fetching from TMDB on every cold start.

---

## 🔧 Configuration Reference

| Secret Key | Required | Description |
|-----------|----------|-------------|
| `TMDB_API_KEY` | ✅ Yes | From [themoviedb.org](https://www.themoviedb.org/settings/api) |
| `TURSO_DATABASE_URL` | Optional | Cloud SQLite URL (`libsql://...`) |
| `TURSO_AUTH_TOKEN` | Optional | Turso auth token |
| `SENTRY_DSN` | Optional | Sentry DSN for error monitoring |

---

## 📜 License

GNU General Public License v3.0.

You are free to use, copy, distribute, and modify this project. Derivative works must retain the same GPL v3.0 licence and credit the original creator.

See [LICENSE](LICENSE) for full terms.

---

## 👤 Creator

**Anishkumar Sankaran**

- 📽️ Letterboxd: [@antonymic](https://letterboxd.com/antonymic/)
- 💻 GitHub: [@AnishkumarSankaran](https://github.com/AnishkumarSankaran)
- 📧 [Contact via Email for Issues](letterboxdanalyzerneo@gmail.com)

*Built with love and a potato PC (AMD A4 dual-core ThinkPad). If this runs on my machine, it runs on yours.*

---

## 🙏 Acknowledgements

- [TMDB](https://www.themoviedb.org/) — Film metadata API (free for non-commercial use)
- [Letterboxd](https://letterboxd.com/) — For making it easy to export your data
- [Streamlit](https://streamlit.io/) — For making Python apps beautiful
- [Turso](https://turso.tech/) — Cloud SQLite backend
- [Sentry](https://sentry.io/) — Error monitoring
- Neo-Brutalist design movement — For making the web ugly in a beautiful way

---

## 📝 Changelog

### V9.4 (April 2026)
- ✅ Artists: Highest Rated sections are now year-filtered
- ✅ Milestones: First & Last uses selected year, not current year
- ✅ Milestones: Diary ordering uses `Watched Date`, not `Date`
- ✅ Milestones: Diary milestones (50th, 100th…) respect year filter
- ✅ Milestones: Most Re-watched uses year-filtered diary
- ✅ Milestones: Poster lookup uses full enriched cache, not year slice

### V9.3 (March 2026)
- ✅ Year dropdown reads `diary.csv` "Watched Date" (actual watch date)
- ✅ `_apply_year_filter` uses diary cross-reference strategy
- ✅ All tabs pass `diary_df` to `_apply_year_filter`

### V9 — Neo-Brutalist Comic Edition (March 2026)
- ✅ Full UI redesign: Neo-Brutalist CMYK palette (cyan / magenta / yellow / black)
- ✅ Async TMDB with single session pool (A1–A5 performance fixes)
- ✅ Vectorised pandas: `explode()` replaces all `iterrows()` loops
- ✅ Turso cloud database support
- ✅ Year-matching fix: Superman (1978) ≠ Superman (2025)
- ✅ Country data: ISO code normalisation (SU→RU, YU→RS, etc.)
- ✅ Watchlist Roulette with pre-built pool for instant sampling
- ✅ `@st.cache_data` on all heavy functions
- ✅ Neo-Brutalist artist cards: rectangular 3/4 ratio, never circles
- ✅ Sentry optional integration
- ✅ B-tree indexed `cache_key` column in SQLite
- ✅ Single bulk SQL query — no N+1 loop
- ✅ Deterministic MD5 cache keys (no Python hash seed issues)
- ✅ Failure TTL: 7-day retry window for failed TMDB lookups

*(See previous versions at [letterboxd-analyzer-pro](https://github.com/AnishkumarSankaran/letterboxd-analyzer-pro))*
