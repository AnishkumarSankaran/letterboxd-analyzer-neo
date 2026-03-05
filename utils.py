"""
Utilities Module for Letterboxd Analyzer V3
Helper functions, validators, and utility tools
"""

import pandas as pd
import re
from typing import Optional, List, Dict, Tuple
from datetime import datetime
import hashlib
import os


def validate_letterboxd_csv(df: pd.DataFrame, csv_type: str) -> Tuple[bool, List[str]]:
    """
    Validate that a CSV file has the expected Letterboxd format.
    Returns (is_valid, list_of_errors).
    """
    errors = []
    
    required_columns = {
        'watched': ['Name', 'Year', 'Date'],
        'ratings': ['Name', 'Year', 'Rating'],
        'watchlist': ['Name', 'Year']
    }
    
    if csv_type not in required_columns:
        return False, [f"Unknown CSV type: {csv_type}"]
    
    for col in required_columns[csv_type]:
        if col not in df.columns:
            errors.append(f"Missing required column: {col}")
    
    is_valid = len(errors) == 0
    return is_valid, errors


def clean_movie_title(title: str) -> str:
    """
    Clean and normalize movie titles.
    Removes special characters, extra whitespace, etc.
    """
    if pd.isna(title):
        return ""
    
    # Convert to string
    title = str(title).strip()
    
    # Remove multiple spaces
    title = re.sub(r'\s+', ' ', title)
    
    # Remove leading/trailing punctuation
    title = title.strip('.,;:!?')
    
    return title


def parse_year_from_string(text: str) -> Optional[int]:
    """
    Extract a 4-digit year from a string.
    Returns None if no valid year found.
    """
    if pd.isna(text):
        return None
    
    # Look for 4-digit year
    match = re.search(r'\b(19|20)\d{2}\b', str(text))
    if match:
        year = int(match.group())
        # Validate year is reasonable (1888-2050)
        if 1888 <= year <= 2050:
            return year
    
    return None


def format_runtime(minutes: int) -> str:
    """
    Format runtime in minutes to human-readable string.
    Example: 142 -> "2h 22m"
    """
    if pd.isna(minutes) or minutes <= 0:
        return "Unknown"
    
    hours = minutes // 60
    mins = minutes % 60
    
    if hours > 0:
        return f"{hours}h {mins}m"
    else:
        return f"{mins}m"


def calculate_percentage(part: int, total: int) -> float:
    """
    Calculate percentage, handling division by zero.
    Returns 0.0 if total is 0.
    """
    if total == 0:
        return 0.0
    return (part / total) * 100


def generate_cache_key(title: str, year: Optional[int]) -> str:
    """
    Generate a unique cache key for a movie.
    Uses MD5 hash of normalized title and year.
    """
    normalized = clean_movie_title(title).lower()
    year_str = str(year) if year else "unknown"
    key_string = f"{normalized}_{year_str}"
    
    return hashlib.md5(key_string.encode()).hexdigest()


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero.
    """
    if denominator == 0:
        return default
    return numerator / denominator


def truncate_text(text: str, max_length: int = 50, suffix: str = "...") -> str:
    """
    Truncate text to maximum length, adding suffix if truncated.
    """
    if pd.isna(text):
        return ""
    
    text = str(text)
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix


def parse_date_flexible(date_str: str) -> Optional[datetime]:
    """
    Parse date string with multiple format attempts.
    Returns None if parsing fails.
    """
    if pd.isna(date_str) or not date_str:
        return None
    
    formats = [
        '%Y-%m-%d',
        '%d/%m/%Y',
        '%m/%d/%Y',
        '%Y/%m/%d',
        '%d-%m-%Y',
        '%m-%d-%Y',
        '%B %d, %Y',
        '%d %B %Y'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(str(date_str), fmt)
        except:
            continue
    
    # Try pandas as fallback
    try:
        return pd.to_datetime(date_str)
    except:
        return None


def get_file_size_mb(filepath: str) -> float:
    """
    Get file size in megabytes.
    Returns 0.0 if file doesn't exist.
    """
    if not os.path.exists(filepath):
        return 0.0
    
    size_bytes = os.path.getsize(filepath)
    return size_bytes / (1024 * 1024)


def merge_duplicate_entries(df: pd.DataFrame, key_col: str = 'Name') -> pd.DataFrame:
    """
    Merge duplicate movie entries, keeping most recent data.
    """
    if df.empty or key_col not in df.columns:
        return df
    
    # Sort by date if available
    if 'Date' in df.columns:
        df_sorted = df.sort_values('Date', ascending=False)
    else:
        df_sorted = df.copy()
    
    # Drop duplicates, keeping first (most recent)
    df_unique = df_sorted.drop_duplicates(subset=[key_col], keep='first')
    
    return df_unique.reset_index(drop=True)


def extract_country_name(country_code: str) -> str:
    """
    Convert country code to full country name.
    """
    country_map = {
        'US': 'United States', 'GB': 'United Kingdom', 'FR': 'France',
        'DE': 'Germany', 'IT': 'Italy', 'ES': 'Spain', 'JP': 'Japan',
        'KR': 'South Korea', 'CN': 'China', 'IN': 'India', 'CA': 'Canada',
        'AU': 'Australia', 'BR': 'Brazil', 'MX': 'Mexico', 'RU': 'Russia',
        'SE': 'Sweden', 'NO': 'Norway', 'DK': 'Denmark', 'FI': 'Finland',
        'NL': 'Netherlands', 'BE': 'Belgium', 'CH': 'Switzerland'
    }
    
    return country_map.get(country_code, country_code)


def validate_rating(rating: float) -> bool:
    """
    Validate that a rating is in valid range (0.5 to 5.0 stars).
    """
    if pd.isna(rating):
        return False
    
    try:
        rating = float(rating)
        return 0.5 <= rating <= 5.0
    except:
        return False


def create_backup_filename(original: str) -> str:
    """
    Create a backup filename with timestamp.
    Example: "data.csv" -> "data_backup_20240206_123456.csv"
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    name, ext = os.path.splitext(original)
    return f"{name}_backup_{timestamp}{ext}"


def estimate_watch_time(df: pd.DataFrame) -> Dict[str, float]:
    """
    Estimate total watch time from runtime data.
    Returns dict with hours, days, weeks.
    """
    if df.empty or 'runtime' not in df.columns:
        return {'hours': 0, 'days': 0, 'weeks': 0}
    
    total_minutes = pd.to_numeric(df['runtime'], errors='coerce').fillna(0).sum()
    total_hours = total_minutes / 60
    
    return {
        'hours': total_hours,
        'days': total_hours / 24,
        'weeks': total_hours / (24 * 7)
    }


def find_common_actors(df: pd.DataFrame, min_movies: int = 3) -> List[str]:
    """
    Find actors who appear in multiple movies.
    Returns list of actors appearing in at least min_movies films.
    """
    if df.empty or 'actors' not in df.columns:
        return []
    
    all_actors = []
    for actors_str in df['actors'].dropna():
        if actors_str:
            actors = [a.strip() for a in str(actors_str).split(',')]
            all_actors.extend(actors)
    
    from collections import Counter
    actor_counts = Counter(all_actors)
    
    common_actors = [
        actor for actor, count in actor_counts.items()
        if count >= min_movies
    ]
    
    return sorted(common_actors, key=lambda x: actor_counts[x], reverse=True)


def calculate_genre_percentages(df: pd.DataFrame) -> Dict[str, float]:
    """
    Calculate percentage of total for each genre.
    """
    if df.empty or 'genres' not in df.columns:
        return {}
    
    all_genres = []
    for genres_str in df['genres'].dropna():
        if genres_str:
            genres = [g.strip() for g in str(genres_str).split(',')]
            all_genres.extend(genres)
    
    from collections import Counter
    genre_counts = Counter(all_genres)
    total = sum(genre_counts.values())
    
    return {
        genre: (count / total) * 100
        for genre, count in genre_counts.items()
    }


def export_to_markdown(df: pd.DataFrame, filename: str = "export.md"):
    """
    Export DataFrame to Markdown format.
    """
    try:
        md_content = df.to_markdown(index=False)
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(md_content)
        return filename
    except Exception as e:
        print(f"Error exporting to markdown: {e}")
        return None


def create_simple_report(df: pd.DataFrame) -> str:
    """
    Create a simple text report of basic statistics.
    """
    total_movies = len(df)
    
    report_lines = [
        "=" * 50,
        "LETTERBOXD ANALYSIS REPORT",
        "=" * 50,
        "",
        f"Total Movies: {total_movies}",
        ""
    ]
    
    if 'Rating' in df.columns:
        avg_rating = df['Rating'].mean()
        report_lines.append(f"Average Rating: {avg_rating:.2f} stars")
    
    if 'runtime' in df.columns:
        watch_time = estimate_watch_time(df)
        report_lines.append(f"Total Watch Time: {watch_time['hours']:.1f} hours")
    
    report_lines.extend(["", "=" * 50])
    
    return "\n".join(report_lines)
