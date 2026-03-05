"""
Enhanced Async TMDB API module for Letterboxd Analyzer V2
Includes person (actor/director) image fetching and production country data
"""

import aiohttp
import asyncio
from typing import Dict, List, Optional, Tuple
import streamlit as st
from datetime import datetime
import json

# TMDB Configuration
TMDB_BASE_URL = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"
TMDB_PROFILE_BASE = "https://image.tmdb.org/t/p/w185"

# Rate limiting
MAX_CONCURRENT_REQUESTS = 40   # TMDB allows ~40 req/s
REQUEST_TIMEOUT = 15           # Per-request read timeout (seconds)
CONNECT_TIMEOUT = 5            # TCP connect timeout (seconds)
MAX_RETRIES = 0                # No in-coroutine retry — failures are cached and retried next session


class TMDBAsyncClient:
    """
    Enhanced high-performance async TMDB API client.
    Fetches movie data, person images, and production countries.
    """
    
    def __init__(self, api_key: str):
        """Initialize the async client with API key."""
        self.api_key = api_key
        self.session = None
        self.semaphore = None  # Created in __aenter__ inside the running event loop
    
    async def __aenter__(self):
        """Async context manager entry - creates the aiohttp session."""
        # ✅ A3 FIX: Create semaphore inside the running event loop, not in __init__
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        # ✅ A2 FIX: Use connect + sock_read instead of total.
        # total= includes semaphore queue time, which kills valid requests.
        timeout = aiohttp.ClientTimeout(
            total=None,            # No wall-clock cap (prevents killing queued requests)
            connect=CONNECT_TIMEOUT,
            sock_connect=CONNECT_TIMEOUT,
            sock_read=REQUEST_TIMEOUT,  # Wait up to 15s for TMDB response body
        )
        # ✅ A1 FIX: DNS cache + zombie socket cleanup
        connector = aiohttp.TCPConnector(
            limit=MAX_CONCURRENT_REQUESTS,
            ttl_dns_cache=300,           # Cache TMDB DNS for 5 min (default was 10s)
            enable_cleanup_closed=True,  # Prune dead TIME_WAIT sockets
            keepalive_timeout=30,        # Reuse live connections for 30s
        )
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - closes the session."""
        if self.session:
            await self.session.close()
    
    async def search_and_get_movie_details(
        self, 
        title: str, 
        year: Optional[int] = None
    ) -> Optional[Dict]:
        """
        Search for a movie and get full details with credits AND production countries.
        Retries once on timeout with a short backoff.
        """
        for attempt in range(MAX_RETRIES + 1):
            result = await self._do_search_and_get(title, year)
            if result is not None:
                return result
            if attempt < MAX_RETRIES:
                await asyncio.sleep(2 * (attempt + 1))  # 2s backoff
        return None

    async def _do_search_and_get(
        self,
        title: str,
        year: Optional[int] = None
    ) -> Optional[Dict]:
        """Internal: single attempt to search + fetch movie details."""
        async with self.semaphore:
            try:
                # Step 1: Search for the movie
                search_url = f"{TMDB_BASE_URL}/search/movie"
                params = {
                    'api_key': self.api_key,
                    'query': title,
                    'language': 'en-US'
                }
                if year:
                    params['year'] = year
                
                async with self.session.get(search_url, params=params) as response:
                    if response.status != 200:
                        return None
                    
                    search_data = await response.json()
                    
                    if not search_data.get('results'):
                        return None
                
                # ✅ V9 FIX: Find best match by year instead of blindly
                # picking the first (most popular) result, which may be
                # a different film with the same title (e.g. Superman 1978 vs 2025)
                results = search_data['results']
                movie_id = results[0]['id']
                if year:
                    for r in results:
                        rd = r.get('release_date', '')
                        if rd and rd[:4] == str(year):
                            movie_id = r['id']
                            break
                
                # Step 2: Get detailed info WITH credits in ONE call
                details_url = f"{TMDB_BASE_URL}/movie/{movie_id}"
                params = {
                    'api_key': self.api_key,
                    'append_to_response': 'credits',
                    'language': 'en-US'
                }
                
                async with self.session.get(details_url, params=params) as response:
                    if response.status != 200:
                        return None
                    
                    full_data = await response.json()
                    
                    # Extract cast with images (top 10)
                    cast_list = full_data.get('credits', {}).get('cast', [])
                    actors_with_images = []
                    for actor in cast_list[:10]:
                        actors_with_images.append({
                            'name': actor.get('name', ''),
                            'profile_path': actor.get('profile_path', '')
                        })
                    
                    # Extract directors with images
                    crew_list = full_data.get('credits', {}).get('crew', [])
                    directors_with_images = []
                    for person in crew_list:
                        if person.get('job') == 'Director':
                            directors_with_images.append({
                                'name': person.get('name', ''),
                                'profile_path': person.get('profile_path', '')
                            })
                    
                    # Extract production countries
                    production_countries = [
                        country.get('iso_3166_1', '') 
                        for country in full_data.get('production_countries', [])
                    ]
                    
                    # Build comprehensive movie info
                    movie_info = {
                        'title': full_data.get('title', title),
                        'year': year if year else self._extract_year(full_data.get('release_date', '')),
                        'tmdb_id': movie_id,
                        'poster_path': full_data.get('poster_path', ''),
                        'popularity': full_data.get('popularity', 0),
                        'vote_count': full_data.get('vote_count', 0),
                        'runtime': full_data.get('runtime', 0),
                        'overview': full_data.get('overview', ''),
                        'release_date': full_data.get('release_date', ''),
                        'original_language': full_data.get('original_language', ''),
                        'vote_average': full_data.get('vote_average', 0),
                        'genres': self._extract_genres(full_data.get('genres', [])),
                        'actors': self._extract_actors(cast_list),
                        'directors': self._extract_directors(crew_list),
                        'actors_with_images': json.dumps(actors_with_images),
                        'directors_with_images': json.dumps(directors_with_images),
                        'production_countries': ','.join(production_countries)
                    }
                    
                    return movie_info
                    
            except asyncio.TimeoutError:
                print(f"Timeout fetching {title} ({year})")
                return None
            except Exception as e:
                print(f"Error fetching {title} ({year}): {e}")
                return None
    
    def _extract_year(self, release_date: str) -> Optional[int]:
        """Extract year from release date string."""
        if release_date and len(release_date) >= 4:
            try:
                return int(release_date[:4])
            except:
                pass
        return None
    
    def _extract_genres(self, genres_list: List[Dict]) -> str:
        """Extract genre names from TMDB genres list."""
        if not genres_list:
            return ''
        return ', '.join([g.get('name', '') for g in genres_list if g.get('name')])
    
    def _extract_actors(self, cast_list: List[Dict], max_actors: int = 10) -> str:
        """Extract top actors from cast list."""
        if not cast_list:
            return ''
        actors = [actor.get('name', '') for actor in cast_list[:max_actors] if actor.get('name')]
        return ', '.join(actors)
    
    def _extract_directors(self, crew_list: List[Dict]) -> str:
        """Extract directors from crew list."""
        if not crew_list:
            return ''
        directors = [
            person.get('name', '') 
            for person in crew_list 
            if person.get('job') == 'Director' and person.get('name')
        ]
        return ', '.join(directors)
    
    async def fetch_multiple_movies(
        self, 
        movie_list: List[Tuple[str, Optional[int]]]
    ) -> List[Optional[Dict]]:
        """
        Fetch multiple movies concurrently.
        """
        tasks = [
            self.search_and_get_movie_details(title, year)
            for title, year in movie_list
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                processed_results.append(None)
            else:
                processed_results.append(result)
        
        return processed_results


def run_async_fetch(api_key: str, movie_list: List[Tuple[str, Optional[int]]]) -> List[Optional[Dict]]:
    """
    Wrapper function to run async code from synchronous context.
    """
    async def _fetch():
        async with TMDBAsyncClient(api_key) as client:
            return await client.fetch_multiple_movies(movie_list)
    
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = loop.run_until_complete(_fetch())
        loop.close()
        return results
    except Exception as e:
        print(f"Error in async fetch: {e}")
        return [None] * len(movie_list)


def fetch_movies_with_progress(
    api_key: str,
    movie_list: List[Tuple[str, Optional[int]]],
    progress_bar=None,
    status_text=None
) -> List[Optional[Dict]]:
    """
    Fetch movies with progress tracking for Streamlit UI.

    ✅ A5 FIX: Creates ONE TMDBAsyncClient (one session, one connector,
    one DNS cache, one TLS connection pool) for the entire fetch run.
    Previously created 43 separate sessions (one per batch), each paying
    full DNS + TLS handshake overhead.
    """
    import time as _t
    BATCH_SIZE = 50
    all_results = []
    total_movies = len(movie_list)

    async def _fetch_all_batches():
        async with TMDBAsyncClient(api_key) as client:
            for i in range(0, total_movies, BATCH_SIZE):
                batch = movie_list[i:i + BATCH_SIZE]
                batch_num = i // BATCH_SIZE + 1
                total_batches = (total_movies + BATCH_SIZE - 1) // BATCH_SIZE

                if status_text:
                    status_text.text(
                        f"🎬 Fetching movies: Batch {batch_num}/{total_batches} "
                        f"({len(batch)} movies)"
                    )

                t0 = _t.time()
                batch_results = await client.fetch_multiple_movies(batch)
                elapsed = _t.time() - t0
                ok = sum(1 for r in batch_results if r is not None)
                fail = len(batch_results) - ok
                print(
                    f"    TMDB batch {batch_num}/{total_batches}: "
                    f"{len(batch)} films → {ok} ok, {fail} fail, {elapsed:.2f}s"
                )

                all_results.extend(batch_results)

                if progress_bar:
                    progress = min((i + len(batch)) / total_movies, 1.0)
                    progress_bar.progress(progress)

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_fetch_all_batches())
        loop.close()
    except Exception as e:
        print(f"Error in async fetch: {e}")
        # Pad remaining results with None if partial failure
        while len(all_results) < total_movies:
            all_results.append(None)

    return all_results
