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
