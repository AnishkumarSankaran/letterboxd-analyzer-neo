<div align="center">

# 🎬 LETTERBOXD ANALYZER
### V9 NEO-BRUTALIST COMIC EDITION

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.33+-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)](https://streamlit.io)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue?style=flat-square)](https://www.gnu.org/licenses/gpl-3.0)
[![TMDB](https://img.shields.io/badge/Powered%20by-TMDB-01D277?style=flat-square)](https://www.themoviedb.org)

**A professional-grade, blazing-fast analytics tool for your Letterboxd film diary.**  
Upload your data. Get obsessed with your own taste.

[🚀 Live Demo](#) · [📖 Documentation](#installation) · [🐛 Report Bug](https://github.com/AnishkumarSankaran/letterboxd-neo-brutalist/issues) · [✨ Request Feature](https://github.com/AnishkumarSankaran/letterboxd-neo-brutalist/issues)

</div>

---

## ✨ What Is This?

This is a **self-hosted Letterboxd data analyzer** that gives you deep analytics about your film-watching habits — the kind of stats Letterboxd itself doesn't show you.

Export your data from Letterboxd, upload the ZIP, and in under 2 minutes you get:

- 📊 **9 analytics tabs** — Watched, Ratings, Recent, Artists, Milestones, Stats, Map, Roulette, Watchlist
- 🎨 **Neo-Brutalist UI** — CMYK color palette, hard box shadows, Chivo font, zero border-radius
- ⚡ **Fast async TMDB fetching** — 2,000+ films enriched in ~110s cold, ~8s warm (SQLite cache)
- 🗺️ **World map** — See which countries' cinema you've watched
- 🎰 **Watchlist Roulette** — Can't decide what to watch? Spin the wheel
- 📈 **Genre, language, decade patterns** — Understand your actual taste

---

## 📸 Screenshots

> *(Add screenshots here after deployment)*

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

# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (Mac/Linux)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Configuration

Create `.streamlit/secrets.toml`:
```toml
TMDB_API_KEY = "your_tmdb_api_key_here"

# Optional: Turso cloud database (leave empty to use local SQLite)
# TURSO_DATABASE_URL = "libsql://your-db.turso.io"
# TURSO_AUTH_TOKEN = "your-turso-token"
```

### Run
```bash
streamlit run app.py
```

Open http://localhost:8501 in your browser.

---

## 📊 Feature Overview

| Tab | What You Get |
|-----|-------------|
| 🎬 **Watched** | Total films, hours, genres, languages, monthly/weekly/decade patterns |
| ⭐ **Ratings** | Your highest and lowest rated films with posters |
| 🕐 **Recent** | Newest releases you've seen, oldest films, recently watched timeline |
| 🎭 **Artists** | Top actors and directors by film count, with photos |
| 🏆 **Milestones** | Your 100th, 500th, 1000th films and viewing timeline |
| 📊 **Stats** | Crowd favorites, hidden gems, runtime extremes |
| 🗺️ **Map** | World choropleth map of production countries |
| 🎰 **Roulette** | Random film picker from your watchlist |
| 📋 **Watchlist** | Your watchlist enriched with ratings and runtime |

---

## ⚡ Performance

| Scenario | Time |
|----------|------|
| Cold start (2,000 films, empty cache) | ~110 seconds |
| Warm start (same films, full cache) | ~8 seconds |
| Subsequent reruns (no file change) | ~3 seconds |

Performance achieved through:
- Single async TMDB session across all 43 batches (no repeated TLS handshakes)
- B-tree indexed SQLite bulk lookup (one SQL query for all films)
- `@st.cache_data` on all computation-heavy functions
- Vectorized pandas `explode()` instead of `iterrows()` loops
- `optimise_dtypes()`: float64 → float32, low-cardinality categories

---

## 🗄️ Database & Cache

Films fetched from TMDB are cached locally in `movie_cache.db` (SQLite) with a 7-day TTL for failures.

**Cloud deployment (Turso):** Set `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` in secrets to switch to cloud-hosted SQLite. The pre-populated cloud cache can serve thousands of users without re-fetching from TMDB on every cold start.

---

## 🏗️ Architecture
letterboxd-neo-brutalist/
├── app.py                  # Main Streamlit app, enrichment pipeline
├── data_processing.py      # Vectorized analytics (genres, decades, milestones)
├── database.py             # SQLite/Turso cache engine with bulk lookups
├── tmdb_async.py           # Async TMDB client (aiohttp, 40 concurrent)
├── visualization.py        # Neo-Brutalist UI components
├── utils.py                # Validators, formatters, helpers
├── static/                 # CSS files, images
│   ├── tokens.css
│   ├── layout.css
│   ├── components.css
│   └── animations.css
├── requirements.txt
└── .streamlit/
└── config.toml
---

## 🔧 Configuration Reference

| Secret Key | Required | Description |
|-----------|----------|-------------|
| `TMDB_API_KEY` | ✅ Yes | Get from themoviedb.org |
| `TURSO_DATABASE_URL` | Optional | Cloud SQLite URL |
| `TURSO_AUTH_TOKEN` | Optional | Turso auth token |

---

## 📜 License

This project is licensed under the **GNU General Public License v3.0**.

You are free to:
- ✅ Use this for any purpose (personal or commercial)
- ✅ Copy and distribute it
- ✅ Modify it
- ✅ Distribute your modifications

You must:
- ⚠️ Keep the same GPL v3.0 license on any derivative work
- ⚠️ Credit the original creator: **Anishkumar Sankaran**
- ⚠️ Make your source code available if you distribute the app

See [LICENSE](LICENSE) for full terms.

---

## 👤 Creator

**Anishkumar Sankaran**

- 📽️ Letterboxd: [@antonymic](https://letterboxd.com/antonymic/)
- 💻 GitHub: [@AnishkumarSankaran](https://github.com/AnishkumarSankaran)
- 📧 [Contact via GitHub Issues](https://github.com/AnishkumarSankaran/letterboxd-neo-brutalist/issues)

*Built with love and a potato PC (AMD A4 dual-core ThinkPad). If this runs on my machine, it runs on yours.*

---

## 🙏 Acknowledgements

- [TMDB](https://www.themoviedb.org/) — Film metadata API (free for non-commercial use)
- [Letterboxd](https://letterboxd.com/) — For making it easy to export your data
- [Streamlit](https://streamlit.io/) — For making Python apps beautiful
- [Turso](https://turso.tech/) — Cloud SQLite backend
- Neo-Brutalist design movement — For making the web ugly in a beautiful way

---

## 📝 Changelog

### V9 — Neo-Brutalist Comic Edition (March 2026)
- ✅ Full UI redesign: Neo-Brutalist CMYK palette (cyan/magenta/yellow/black)
- ✅ Async TMDB with single session pool (A1-A5 performance fixes)
- ✅ Vectorized pandas: `explode()` replaces all `iterrows()` loops
- ✅ Turso cloud database support
- ✅ V9 year-matching fix: Superman (1978) ≠ Superman (2025)
- ✅ Country data: ISO code normalization (SU→RU, YU→RS, etc.)
- ✅ Watchlist Roulette with pre-built pool for instant sampling
- ✅ `@st.cache_data` on all heavy functions
- ✅ Neo-Brutalist artist cards: rectangular 3/4 ratio, never circles

*(See previous versions at [letterboxd-analyzer-pro](https://github.com/AnishkumarSankaran/letterboxd-analyzer-pro))*
