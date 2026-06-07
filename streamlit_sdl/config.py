from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "processed" / "borrowings-clean.csv"
SDL_CSV_PATH = BASE_DIR / "data" / "processed" / "SDL.csv"
GSEC_DIR_PATH = BASE_DIR / "data" / "processed" / "gsec"
TENOR_AVERAGE_CSV_PATH = BASE_DIR / "data" / "processed" / "Tenor Average (1).csv"
DB_PATH = BASE_DIR / "db" / "sdl_analytics.duckdb"
TABLE_NAME = "sdl_borrowings"
SDL_TABLE_NAME = "sdl_secondary_market"
DB_SCHEMA_VERSION = "2026-05-01-trimmed-borrowing-dates"
DEFAULT_STATE = "Assam"


def get_source_data_version() -> str:
    tracked_paths = [
        CSV_PATH,
        SDL_CSV_PATH,
        TENOR_AVERAGE_CSV_PATH,
        *sorted(GSEC_DIR_PATH.glob("*.csv")),
    ]
    parts: list[str] = []
    for path in tracked_paths:
        if path.exists():
            parts.append(f"{path.name}:{path.stat().st_mtime_ns}")
    return "|".join(parts)
