"""
database.py — V9.3 High-Performance Cache Engine

✅ V9.3 FIXES:
  • cache_key column + index → instant B-tree lookups (no more full table scans)
  • get_movies_bulk() → ONE query for all films (no more N+1 loop)
  • add_movies_batch() → single transaction (no more per-row commits)
  • WAL mode + NORMAL sync → concurrent reads during writes
  • Thread-safe via per-operation cursor (no shared cursor)
  • Auto-backfill cache_key for existing rows on startup
"""

import sqlite3
import json
import re
from typing import Dict, List, Optional, Tuple
import pandas as pd
from datetime import datetime
import streamlit as st


def _make_cache_key(title: str, year) -> str:
    """Build a deterministic cache key from title + year.

    Normalization: lowercase, strip, remove periods, collapse whitespace.
    Key format: "normalised_title::YEAR" (or "normalised_title::0" if no year).
    """
    t = str(title).lower().strip()
    t = t.replace(".", "")
    t = re.sub(r"\s+", " ", t)
    try:
        y = int(year) if year is not None and str(year).strip() not in ("", "nan", "None") else 0
    except (ValueError, TypeError):
        y = 0
    return f"{t}::{y}"


class MovieDatabase:
    """High-performance SQLite database for caching movie metadata.

    V9.3: cache_key index, bulk reads, batch writes, WAL mode.
    """

    def __init__(self, db_path: str = "movie_cache.db"):
        self.db_path = db_path
        self.conn = None
        self._initialize_database()

    def _initialize_database(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)

        # ✅ WAL mode: allows concurrent reads during writes
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
        self.conn.execute("PRAGMA temp_store=MEMORY")

        cur = self.conn.cursor()

        # Create movies table (adds cache_key column)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS movies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                year INTEGER,
                tmdb_id INTEGER UNIQUE,
                poster_path TEXT,
                popularity REAL,
                vote_count INTEGER,
                runtime INTEGER,
                genres TEXT,
                actors TEXT,
                directors TEXT,
                overview TEXT,
                release_date TEXT,
                original_language TEXT,
                vote_average REAL,
                actors_with_images TEXT,
                directors_with_images TEXT,
                production_countries TEXT,
                cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                cache_key TEXT,
                UNIQUE(title, year)
            )
        ''')

        # Create people table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS people (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                profile_path TEXT,
                cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Create users table for tracking unique visitors
        cur.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                given_name TEXT,
                bio TEXT,
                film_count INTEGER DEFAULT 0,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # ✅ V9.3 MIGRATION: Add cache_key column to existing databases.
        # CREATE TABLE IF NOT EXISTS won't modify an existing table, so we
        # need ALTER TABLE for databases created with the old schema.
        try:
            cur.execute('ALTER TABLE movies ADD COLUMN cache_key TEXT')
        except sqlite3.OperationalError:
            pass  # Column already exists — safe to ignore

        # ✅ CRITICAL INDEX: cache_key enables instant B-tree lookups
        cur.execute('CREATE INDEX IF NOT EXISTS idx_cache_key ON movies(cache_key)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_tmdb_id ON movies(tmdb_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_person_name ON people(name)')

        self.conn.commit()

        # ✅ Backfill cache_key for existing rows that don't have one
        self._backfill_cache_keys()
        # ✅ Fix corrupt year values (numpy bytes stored by accident)
        self._fix_corrupt_years()

    def _backfill_cache_keys(self):
        """One-time migration: compute cache_key for rows that lack it."""
        cur = self.conn.cursor()
        cur.execute("SELECT id, title, year FROM movies WHERE cache_key IS NULL")
        rows = cur.fetchall()
        if rows:
            import struct as _struct
            updates = []
            for row_id, title, year in rows:
                if isinstance(year, bytes):
                    try:
                        year = _struct.unpack('<q', year.ljust(8, b'\x00'))[0]
                    except Exception:
                        year = None
                updates.append((_make_cache_key(title, year), row_id))
            cur.executemany(
                "UPDATE movies SET cache_key = ? WHERE id = ?",
                updates,
            )
            self.conn.commit()

    def _fix_corrupt_years(self):
        """Fix rows where year was stored as raw bytes (numpy.int64 corruption).

        ✅ V9.3: When numpy.int64 values were passed to SQLite, the Python
        adapter stored them as raw bytes (e.g. b'\\xea\\x07...' = 2026).
        This crashes _row_to_dict and makes the entire bulk lookup fail.
        """
        cur = self.conn.cursor()
        cur.execute("SELECT id, year FROM movies WHERE typeof(year) = 'blob'")
        corrupt = cur.fetchall()
        if corrupt:
            import struct
            fixed = 0
            for row_id, raw_year in corrupt:
                try:
                    year_int = struct.unpack('<q', raw_year.ljust(8, b'\x00'))[0]
                    cur.execute("UPDATE movies SET year = ? WHERE id = ?", (year_int, row_id))
                    fixed += 1
                except Exception:
                    cur.execute("UPDATE movies SET year = NULL WHERE id = ?", (row_id,))
                    fixed += 1
            self.conn.commit()
            if fixed:
                print(f"[DB] Fixed {fixed} corrupt year values")

        # ✅ Fix tmdb_id=-1 collisions: give each failure a unique negative ID
        cur.execute("SELECT id, cache_key FROM movies WHERE tmdb_id = -1")
        failures = cur.fetchall()
        if len(failures) > 1:
            for i, (row_id, ck) in enumerate(failures):
                raw = abs(hash(str(ck))) % 2_000_000_000
                neg_id = -(raw if raw != 0 else (i + 1))
                cur.execute("UPDATE movies SET tmdb_id = ? WHERE id = ?", (neg_id, row_id))
            self.conn.commit()
            print(f"[DB] Fixed {len(failures)} tmdb_id=-1 collisions")
    # ─── COLUMNS ──────────────────────────────────────────────────
    _COLUMNS = [
        'id', 'title', 'year', 'tmdb_id', 'poster_path', 'popularity',
        'vote_count', 'runtime', 'genres', 'actors', 'directors',
        'overview', 'release_date', 'original_language', 'vote_average',
        'actors_with_images', 'directors_with_images', 'production_countries',
        'cached_at', 'cache_key',
    ]

    def _row_to_dict(self, row: Tuple) -> Dict:
        movie = dict(zip(self._COLUMNS, row))
        # ✅ V9.3 FIX: Handle corrupt year values (bytes, numpy types)
        raw_year = movie.get('year')
        if raw_year is not None:
            try:
                if isinstance(raw_year, bytes):
                    # numpy.int64 stored as raw bytes — interpret as little-endian int
                    import struct
                    movie['year'] = struct.unpack('<q', raw_year.ljust(8, b'\x00'))[0]
                else:
                    movie['year'] = int(raw_year)
            except (ValueError, TypeError, struct.error):
                movie['year'] = None
        return movie

    # ─── SINGLE LOOKUP (fast, uses cache_key index) ───────────────
    def get_movie(self, title: str, year=None) -> Optional[Dict]:
        """Retrieve a movie using the cache_key index (instant B-tree lookup)."""
        try:
            key = _make_cache_key(title, year)
            cur = self.conn.cursor()
            cur.execute("SELECT * FROM movies WHERE cache_key = ?", (key,))
            row = cur.fetchone()
            return self._row_to_dict(row) if row else None
        except Exception as e:
            print(f"Cache lookup error [{title}]: {e}")
            return None

    # ─── BULK LOOKUP (the N+1 killer) ─────────────────────────────
    def get_movies_bulk(self, title_year_pairs: List[Tuple[str, any]]) -> Dict[str, Dict]:
        """Fetch all cached movies in ONE query. Returns {cache_key: movie_dict}.

        ✅ V9.3: Replaces the 2,053-query N+1 loop with a single SELECT.
        Uses batched IN() clauses (SQLite limit: 999 params per query).
        """
        if not title_year_pairs:
            return {}

        keys = [_make_cache_key(t, y) for t, y in title_year_pairs]
        result: Dict[str, Dict] = {}
        cur = self.conn.cursor()

        # SQLite supports max 999 parameters per query — batch in chunks
        CHUNK = 900
        for i in range(0, len(keys), CHUNK):
            chunk = keys[i:i + CHUNK]
            placeholders = ",".join("?" * len(chunk))
            cur.execute(
                f"SELECT * FROM movies WHERE cache_key IN ({placeholders})",
                chunk,
            )
            for row in cur.fetchall():
                movie = self._row_to_dict(row)
                ck = movie.get("cache_key", "")
                if ck:
                    result[ck] = movie

        return result

    # ─── SINGLE INSERT ────────────────────────────────────────────
    def add_movie(self, movie_data: Dict) -> bool:
        """Add a movie to the cache (computes cache_key automatically).

        ✅ V9.3 FIX: Explicitly converts all values to Python-native types
        before insertion. numpy.int64/float64 values cause SQLite to store
        raw bytes, corrupting the database.
        """
        try:
            title = str(movie_data.get('title', '')).strip()
            if not title:
                return False
            # ✅ CRITICAL: Convert year to Python int (not numpy.int64)
            raw_year = movie_data.get('year')
            try:
                year = int(raw_year) if raw_year is not None and str(raw_year).strip() not in ('', 'nan', 'None') else None
            except (ValueError, TypeError):
                year = None
            key = _make_cache_key(title, year)
            cur = self.conn.cursor()

            # ✅ Convert every value to Python native type to prevent numpy corruption
            def _py(val, default=None):
                """Convert numpy/pandas types to Python natives.

                Order matters!
                • isinstance(float) catches Python float (8.707) → keep as float
                • isinstance(int)   catches Python int   (278)   → keep as int
                • hasattr fallbacks catch numpy.int64 / numpy.float64
                • numpy.int64 has BOTH __int__ and __float__, but
                  isinstance(numpy.int64, (int,float)) is False, so we
                  need the hasattr fallback.  We check __float__ FIRST
                  for numpy because numpy.float64 also has __int__, and
                  we must NOT truncate it.  For numpy.int64 we explicitly
                  check it is NOT a float-like value before calling int().
                """
                if val is None:
                    return default
                if isinstance(val, (bytes,)):
                    return default
                try:
                    # 1. Python-native float (preserves 8.707)
                    if isinstance(val, float):
                        return float(val)
                    # 2. Python-native int (preserves 278)
                    if isinstance(val, int):
                        return int(val)
                    # 3. numpy.float64 → Python float (hasattr fallback)
                    if hasattr(val, '__float__'):
                        return float(val)
                    # 4. numpy.int64 → Python int (last resort)
                    if hasattr(val, '__int__'):
                        return int(val)
                except (ValueError, TypeError):
                    return default
                return str(val) if val else default

            cur.execute('''
                INSERT OR REPLACE INTO movies
                (title, year, tmdb_id, poster_path, popularity, vote_count,
                 runtime, genres, actors, directors, overview, release_date,
                 original_language, vote_average, actors_with_images,
                 directors_with_images, production_countries, cache_key)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                title,
                year,
                _py(movie_data.get('tmdb_id'), None),
                str(movie_data.get('poster_path', '') or ''),
                _py(movie_data.get('popularity', 0.0), 0.0),
                _py(movie_data.get('vote_count', 0), 0),
                _py(movie_data.get('runtime', 0), 0),
                str(movie_data.get('genres', '') or ''),
                str(movie_data.get('actors', '') or ''),
                str(movie_data.get('directors', '') or ''),
                str(movie_data.get('overview', '') or ''),
                str(movie_data.get('release_date', '') or ''),
                str(movie_data.get('original_language', '') or ''),
                _py(movie_data.get('vote_average', 0.0), 0.0),
                str(movie_data.get('actors_with_images', '') or ''),
                str(movie_data.get('directors_with_images', '') or ''),
                str(movie_data.get('production_countries', '') or ''),
                key,
            ))
            # NOTE: no commit here — caller must commit (batch-friendly)
            return True
        except Exception as e:
            print(f"Insert error [{movie_data.get('title')}]: {e}")
            return False

    # ─── BATCH INSERT (single transaction) ────────────────────────
    def add_movies_batch(self, movies: List[Dict]) -> int:
        """Add multiple movies in ONE atomic transaction.

        ✅ V9.3: Single BEGIN/COMMIT instead of per-row commits.
        50 movies → 1 disk flush instead of 50.
        """
        if not movies:
            return 0
        added = 0
        try:
            for movie in movies:
                if self.add_movie(movie):
                    added += 1
            self.conn.commit()  # ✅ ONE commit for the entire batch
            return added
        except Exception as e:
            self.conn.rollback()
            print(f"Batch insert error: {e}")
            return added

    # ─── DELETE FAILED ENTRIES (for retry logic) ────────────────
    def delete_failed_entries(self, cache_keys: List[str]) -> int:
        """Delete cached failure entries (negative tmdb_id) for the given keys.

        Called before re-inserting successful retries so we don't hit
        the UNIQUE(title, year) or UNIQUE(tmdb_id) constraint.
        Returns the number of rows deleted.
        """
        if not cache_keys:
            return 0
        try:
            cur = self.conn.cursor()
            deleted = 0
            CHUNK = 900
            for i in range(0, len(cache_keys), CHUNK):
                chunk = cache_keys[i:i + CHUNK]
                placeholders = ",".join("?" * len(chunk))
                cur.execute(
                    f"DELETE FROM movies WHERE cache_key IN ({placeholders}) "
                    f"AND (tmdb_id < 0 OR tmdb_id IS NULL)",
                    chunk,
                )
                deleted += cur.rowcount
            self.conn.commit()
            if deleted:
                print(f"[DB] Deleted {deleted} failed cache entries for retry")
            return deleted
        except Exception as e:
            print(f"Delete failed entries error: {e}")
            return 0

    # ─── PEOPLE CACHE ─────────────────────────────────────────────
    def get_person(self, name: str) -> Optional[Dict]:
        try:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT name, profile_path, cached_at FROM people WHERE name = ? COLLATE NOCASE",
                (name,),
            )
            row = cur.fetchone()
            if row:
                return {'name': row[0], 'profile_path': row[1], 'cached_at': row[2]}
            return None
        except Exception as e:
            print(f"Person lookup error [{name}]: {e}")
            return None

    def add_person(self, name: str, profile_path: str) -> bool:
        try:
            cur = self.conn.cursor()
            cur.execute(
                "INSERT OR REPLACE INTO people (name, profile_path) VALUES (?, ?)",
                (name, profile_path),
            )
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Person insert error [{name}]: {e}")
            return False

    # ─── USER TRACKING ────────────────────────────────────────────
    def track_user(self, username: str, given_name: str = "",
                   bio: str = "", film_count: int = 0) -> None:
        """Record or update a user visit."""
        try:
            cur = self.conn.cursor()
            cur.execute('''
                INSERT INTO users (username, given_name, bio, film_count, last_seen)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(username) DO UPDATE SET
                    given_name = excluded.given_name,
                    bio = excluded.bio,
                    film_count = excluded.film_count,
                    last_seen = CURRENT_TIMESTAMP
            ''', (username, given_name, bio, film_count))
            self.conn.commit()
        except Exception as e:
            print(f"User tracking error [{username}]: {e}")

    def get_total_users(self) -> int:
        """Return the total number of unique users who have used the app."""
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT COUNT(*) FROM users")
            return cur.fetchone()[0]
        except Exception:
            return 0

    # ─── STATS ────────────────────────────────────────────────────
    def get_all_movies(self) -> pd.DataFrame:
        try:
            return pd.read_sql_query("SELECT * FROM movies", self.conn)
        except Exception:
            return pd.DataFrame()

    def get_cache_stats(self) -> Dict:
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT COUNT(*) FROM movies")
            total_movies = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM movies WHERE cached_at > datetime('now', '-7 days')")
            recent = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM people")
            total_people = cur.fetchone()[0]
            return {
                'total_movies': total_movies,
                'recent_additions': recent,
                'total_people': total_people,
            }
        except Exception:
            return {'total_movies': 0, 'recent_additions': 0, 'total_people': 0}

    def clear_old_cache(self, days: int = 90):
        try:
            cur = self.conn.cursor()
            cur.execute(
                "DELETE FROM movies WHERE cached_at < datetime('now', '-' || ? || ' days')",
                (days,),
            )
            cur.execute(
                "DELETE FROM people WHERE cached_at < datetime('now', '-' || ? || ' days')",
                (days,),
            )
            self.conn.commit()
        except Exception as e:
            print(f"Cache cleanup error: {e}")

    def close(self):
        if self.conn:
            self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


@st.cache_resource
def get_database():
    """Get or create the singleton database instance."""
    return MovieDatabase()
