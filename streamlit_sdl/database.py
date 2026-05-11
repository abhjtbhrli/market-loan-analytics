from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from streamlit_sdl.config import (
    CSV_PATH,
    DB_PATH,
    GSEC_DIR_PATH,
    SDL_CSV_PATH,
    SDL_TABLE_NAME,
    TABLE_NAME,
    TENOR_AVERAGE_CSV_PATH,
)


COLUMN_RENAMES = {
    "Index": "record_index",
    "FY": "fy",
    "Date of Auction": "auction_date",
    "Date of Issue": "issue_date",
    "Date of Maturity": "maturity_date",
    "Notified Amount (Rupees Crore)": "notified_amount_crore",
    "Residual Maturity Period": "residual_maturity_period",
    "Maturity Year": "maturity_year",
    "Number of Competitive Bid Received": "competitive_bid_count_received",
    "Value of Competitive Bid Received\n(Rupees Crore)": "competitive_bid_value_received_crore",
    "Number of Non-Competitive Bid Received": "non_competitive_bid_count_received",
    "Value of Non-Competitive Bid Received\n(Rupees Crore)": "non_competitive_bid_value_received_crore",
    "Number of Competitive Bid Accepted": "competitive_bid_count_accepted",
    "Value of Competitive Bid Accepted\n(Rupees Crore)": "competitive_bid_value_accepted_crore",
    "Number of Non-Competitive Bid Accepted": "non_competitive_bid_count_accepted",
    "Value of Non-Competitive Bid Accepted\n(Rupees Crore)": "non_competitive_bid_value_accepted_crore",
    "Devolvement on Primary Dealers\n(Rupees Crore)": "devolvement_primary_dealers_crore",
    "Devolvement/Private placement on RBI\n(Rupees Crore)": "devolvement_rbi_crore",
    "Cut off Issue/ Price": "cut_off_issue_price",
    "Cut Off Yield": "cut_off_yield",
    "Nomenclature Loan": "loan_name",
    "ISIN No.": "isin",
    "Weighted Avg. Price": "weighted_avg_price",
    "Weighted Avg. Yield": "weighted_avg_yield",
    "State": "state",
    "Amount": "amount_crore",
    "Tenor group": "tenor_group",
    "Bid cover": "bid_cover",
    "WAY": "way",
    "Notified - availed": "notified_minus_availed",
    "Yield / Price": "yield_or_price",
    "Outstanding": "outstanding",
    "Maturity FY": "maturity_fy",
}

DATE_COLUMNS = ["auction_date", "issue_date", "maturity_date"]
NUMERIC_COLUMNS = [
    "record_index",
    "notified_amount_crore",
    "residual_maturity_period",
    "maturity_year",
    "competitive_bid_count_received",
    "competitive_bid_value_received_crore",
    "non_competitive_bid_count_received",
    "non_competitive_bid_value_received_crore",
    "competitive_bid_count_accepted",
    "competitive_bid_value_accepted_crore",
    "non_competitive_bid_count_accepted",
    "non_competitive_bid_value_accepted_crore",
    "devolvement_primary_dealers_crore",
    "devolvement_rbi_crore",
    "cut_off_issue_price",
    "cut_off_yield",
    "weighted_avg_price",
    "weighted_avg_yield",
    "amount_crore",
    "bid_cover",
    "way",
    "notified_minus_availed",
]


def _clean_numeric_series(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype("string")
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip()
        .replace({"-": None, "True": "1", "False": "0", "nan": None, "<NA>": None})
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _parse_sdl_datetime(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype("string")
        .str.replace(" (India Standard Time)", "", regex=False)
        .str.strip()
    )
    return pd.to_datetime(cleaned, format="%a %b %d %Y %H:%M:%S GMT%z", errors="coerce")


def load_clean_dataframe(csv_path: str | None = None) -> pd.DataFrame:
    dataframe = pd.read_csv(csv_path or CSV_PATH)
    dataframe = dataframe.rename(columns=COLUMN_RENAMES)

    for column in DATE_COLUMNS:
        dataframe[column] = pd.to_datetime(
            dataframe[column].astype("string").str.strip(),
            format="%d-%b-%y",
            errors="coerce",
        )

    for column in NUMERIC_COLUMNS:
        dataframe[column] = _clean_numeric_series(dataframe[column])

    dataframe["state"] = dataframe["state"].astype("string").str.strip()
    dataframe["fy"] = dataframe["fy"].astype("string").str.strip()
    dataframe["tenor_group"] = dataframe["tenor_group"].astype("string").str.strip()
    dataframe["loan_name"] = dataframe["loan_name"].astype("string").str.strip()
    dataframe["yield_or_price"] = dataframe["yield_or_price"].astype("string").str.strip()
    dataframe["auction_month"] = dataframe["auction_date"].dt.to_period("M").dt.to_timestamp()

    return dataframe


def load_clean_sdl_dataframe(csv_path: str | None = None) -> pd.DataFrame:
    source_path = csv_path or SDL_CSV_PATH
    original_df = pd.read_csv(source_path)
    filedate = original_df.iloc[1, 3]

    dataframe = pd.read_csv(source_path, header=None)
    dataframe = dataframe.iloc[5:].copy().reset_index(drop=True)
    dataframe.columns = [
        "isin",
        "description",
        "coupon",
        "maturity_text",
        "price_rs",
        "ytm_semi_annual",
    ]

    dataframe["isin"] = dataframe["isin"].astype("string").str.strip()
    dataframe["description"] = dataframe["description"].astype("string").str.strip()
    dataframe["coupon"] = _clean_numeric_series(dataframe["coupon"])
    dataframe["maturity_text"] = dataframe["maturity_text"].astype("string").str.strip()
    dataframe["maturity_date"] = _parse_sdl_datetime(dataframe["maturity_text"])
    dataframe["price_rs"] = _clean_numeric_series(dataframe["price_rs"])
    dataframe["ytm_semi_annual"] = _clean_numeric_series(dataframe["ytm_semi_annual"])
    dataframe["filedate"] = filedate
    dataframe["filedate"] = _parse_sdl_datetime(dataframe["filedate"])

    return dataframe


def load_gsec_history_dataframe(folder_path: str | Path | None = None) -> pd.DataFrame:
    source_dir = Path(folder_path or GSEC_DIR_PATH)
    frames: list[pd.DataFrame] = []

    for csv_path in sorted(source_dir.glob("*.csv")):
        dataframe = pd.read_csv(csv_path)
        available_columns = [column for column in ["Date", "Price"] if column in dataframe.columns]
        if not available_columns:
            continue
        dataframe = dataframe[available_columns].copy()
        dataframe = dataframe.rename(columns={"Date": "date", "Price": "price"})
        frames.append(dataframe)

    if not frames:
        return pd.DataFrame(columns=["date", "price"])

    dataframe = pd.concat(frames, ignore_index=True)
    dataframe["date"] = pd.to_datetime(dataframe["date"], format="%d-%m-%Y", errors="coerce")
    dataframe["price"] = _clean_numeric_series(dataframe["price"])
    dataframe = (
        dataframe.dropna(subset=["date"])
        .drop_duplicates(subset=["date"], keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )
    return dataframe


def load_tenor_average_dataframe(csv_path: str | Path | None = None) -> pd.DataFrame:
    source_path = csv_path or TENOR_AVERAGE_CSV_PATH
    raw_df = pd.read_csv(source_path, header=None)
    filedate_raw = raw_df.iloc[2, 1]
    filedate = _parse_sdl_datetime(pd.Series([filedate_raw])).iloc[0]

    dataframe = raw_df.iloc[3:].reset_index(drop=True)
    dataframe.columns = dataframe.iloc[0]
    dataframe = dataframe.iloc[1:].reset_index(drop=True)

    dataframe["MATURITY BUCKET"] = _clean_numeric_series(dataframe["MATURITY BUCKET"])
    dataframe["PUBLISHED YTM"] = _clean_numeric_series(dataframe["PUBLISHED YTM"])
    current_year = pd.Timestamp.today().year

    def build_tenor_label(value: float | int | None) -> str | pd.NA:
        if value is None or pd.isna(value):
            return pd.NA
        bucket_value = int(value)
        if bucket_value in [1, 2]:
            return "Short-term tenors"
        tenor_years = bucket_value - current_year
        suffix = "year" if tenor_years == 1 else "years"
        return f"{tenor_years} {suffix}"

    dataframe["tenor"] = dataframe["MATURITY BUCKET"].apply(build_tenor_label)
    dataframe["filedate"] = filedate
    return dataframe.rename(
        columns={
            "MATURITY BUCKET": "maturity_bucket",
            "PUBLISHED YTM": "published_ytm",
        }
    )[["maturity_bucket", "published_ytm", "tenor", "filedate"]]


def _create_or_replace_tables(connection: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    dataframe = load_clean_dataframe()
    sdl_dataframe = load_clean_sdl_dataframe()
    connection.register("borrowings_df", dataframe)
    connection.register("sdl_df", sdl_dataframe)
    connection.execute(f"CREATE OR REPLACE TABLE {TABLE_NAME} AS SELECT * FROM borrowings_df")
    connection.execute(f"CREATE OR REPLACE TABLE {SDL_TABLE_NAME} AS SELECT * FROM sdl_df")
    connection.unregister("borrowings_df")
    connection.unregister("sdl_df")
    return connection


def ensure_required_tables(connection: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    existing_tables = {
        row[0]
        for row in connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
    }
    required_tables = {TABLE_NAME, SDL_TABLE_NAME}
    if not required_tables.issubset(existing_tables):
        _create_or_replace_tables(connection)
    return connection


def initialize_database() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(DB_PATH))
    return _create_or_replace_tables(connection)
