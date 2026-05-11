from __future__ import annotations

import duckdb
import pandas as pd

from streamlit_sdl.config import DEFAULT_STATE, SDL_TABLE_NAME, TABLE_NAME
from streamlit_sdl.database import load_clean_dataframe


FISCAL_MONTH_OPTIONS = [
    ("All months", 12),
    ("Apr", 1),
    ("May", 2),
    ("Jun", 3),
    ("Jul", 4),
    ("Aug", 5),
    ("Sep", 6),
    ("Oct", 7),
    ("Nov", 8),
    ("Dec", 9),
    ("Jan", 10),
    ("Feb", 11),
    ("Mar", 12),
]


def get_available_fys(connection: duckdb.DuckDBPyConnection, state: str = DEFAULT_STATE) -> list[str]:
    query = f"""
        SELECT DISTINCT fy
        FROM {TABLE_NAME}
        WHERE lower(state) = lower(?)
          AND fy IS NOT NULL
        ORDER BY fy DESC
    """
    rows = connection.execute(query, [state]).fetchall()
    return [row[0] for row in rows]


def get_assam_summary(
    connection: duckdb.DuckDBPyConnection,
    fy: str,
    state: str = DEFAULT_STATE,
) -> dict[str, object]:
    query = f"""
        SELECT
            COALESCE(SUM(amount_crore), 0) AS total_borrowing_crore,
            COUNT(*) AS auction_count,
            COALESCE(AVG(amount_crore), 0) AS average_auction_size_crore,
            MAX(auction_date) AS latest_auction_date
        FROM {TABLE_NAME}
        WHERE lower(state) = lower(?)
          AND fy = ?
    """
    row = connection.execute(query, [state, fy]).fetchone()
    return {
        "total_borrowing_crore": row[0],
        "auction_count": row[1],
        "average_auction_size_crore": row[2],
        "latest_auction_date": row[3],
    }


def get_monthly_borrowing_trend(
    connection: duckdb.DuckDBPyConnection,
    fy: str,
    state: str = DEFAULT_STATE,
) -> pd.DataFrame:
    query = f"""
        SELECT
            auction_date,
            auction_month,
            amount_crore,
            loan_name
        FROM {TABLE_NAME}
        WHERE lower(state) = lower(?)
          AND fy = ?
          AND auction_date IS NOT NULL
        ORDER BY auction_date
    """
    return connection.execute(query, [state, fy]).df()


def get_available_tenor_groups(
    connection: duckdb.DuckDBPyConnection,
    fy: str,
    state: str = DEFAULT_STATE,
) -> list[str]:
    query = f"""
        SELECT DISTINCT tenor_group
        FROM {TABLE_NAME}
        WHERE lower(state) = lower(?)
          AND fy = ?
          AND tenor_group IS NOT NULL
        ORDER BY tenor_group
    """
    rows = connection.execute(query, [state, fy]).fetchall()
    return [row[0] for row in rows]


def get_auction_level_data(
    connection: duckdb.DuckDBPyConnection,
    fy: str,
    tenor_groups: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    state: str = DEFAULT_STATE,
) -> pd.DataFrame:
    query = f"""
        SELECT
            auction_date,
            issue_date,
            maturity_date,
            amount_crore,
            notified_amount_crore,
            tenor_group,
            loan_name,
            isin,
            cut_off_yield,
            weighted_avg_yield,
            bid_cover
        FROM {TABLE_NAME}
        WHERE lower(state) = lower(?)
          AND fy = ?
    """
    parameters: list[object] = [state, fy]

    if tenor_groups:
        placeholders = ", ".join(["?"] * len(tenor_groups))
        query += f" AND tenor_group IN ({placeholders})"
        parameters.extend(tenor_groups)

    if start_date:
        query += " AND auction_date >= ?"
        parameters.append(start_date)

    if end_date:
        query += " AND auction_date <= ?"
        parameters.append(end_date)

    query += " ORDER BY auction_date DESC, amount_crore DESC"
    return connection.execute(query, parameters).df()


def get_assam_reissue_candidates(
    connection: duckdb.DuckDBPyConnection,
    state: str = DEFAULT_STATE,
) -> pd.DataFrame:
    query = f"""
        SELECT
            b.state,
            b.fy,
            b.auction_date,
            b.issue_date,
            b.maturity_date,
            b.maturity_fy,
            b.loan_name,
            b.isin,
            b.amount_crore,
            b.cut_off_yield,
            s.description,
            s.coupon,
            s.price_rs,
            s.ytm_semi_annual,
            s.filedate
        FROM {SDL_TABLE_NAME} s
        INNER JOIN {TABLE_NAME} b
            ON s.isin = b.isin
        WHERE lower(b.state) = lower(?)
        ORDER BY b.issue_date DESC, b.maturity_date DESC, b.auction_date DESC
    """
    return connection.execute(query, [state]).df()


def get_assam_outstanding_maturity_profile(
    connection: duckdb.DuckDBPyConnection,
    state: str = DEFAULT_STATE,
) -> pd.DataFrame:
    query = f"""
        SELECT
            maturity_fy,
            strftime(maturity_date, '%m') AS maturity_month_num,
            amount_crore
        FROM {TABLE_NAME}
        WHERE lower(state) = lower(?)
          AND outstanding = TRUE
          AND maturity_fy IS NOT NULL
          AND maturity_date IS NOT NULL
        ORDER BY maturity_date
    """
    return connection.execute(query, [state]).df()


def get_statewise_comparison(
    connection: duckdb.DuckDBPyConnection,
    selected_months: list[str] | None = None,
    current_fy: str | None = None,
    comparison_fy: str | None = None,
) -> pd.DataFrame:
    dataframe = load_clean_dataframe()
    fy_values = sorted(dataframe["fy"].dropna().unique(), reverse=True)
    if len(fy_values) < 2:
        return pd.DataFrame()

    selected_current_fy = current_fy or fy_values[0]
    selected_comparison_fy = comparison_fy or fy_values[1]
    if selected_current_fy == selected_comparison_fy:
        return pd.DataFrame()

    fiscal_month_order = {month: order for month, order in FISCAL_MONTH_OPTIONS if month != "All months"}
    month_labels = list(fiscal_month_order.keys())
    selected_months = selected_months or month_labels
    month_map = {4: 1, 5: 2, 6: 3, 7: 4, 8: 5, 9: 6, 10: 7, 11: 8, 12: 9, 1: 10, 2: 11, 3: 12}

    filtered_df = dataframe[dataframe["fy"].isin([selected_current_fy, selected_comparison_fy])].copy()
    filtered_df = filtered_df[
        filtered_df["state"].notna()
        & ~filtered_df["state"].isin(["Delhi", "Jammu & Kashmir", "Puducherry"])
        & filtered_df["auction_date"].notna()
    ]
    filtered_df["fiscal_month_order"] = filtered_df["auction_date"].dt.month.map(month_map)
    allowed_orders = [fiscal_month_order[month] for month in selected_months if month in fiscal_month_order]
    filtered_df = filtered_df[filtered_df["fiscal_month_order"].isin(allowed_orders)]

    comparison_df = (
        filtered_df.groupby(["state", "fy"], as_index=False)["amount_crore"]
        .sum()
        .pivot(index="state", columns="fy", values="amount_crore")
        .fillna(0)
        .reset_index()
    )
    comparison_df = comparison_df.rename(
        columns={
            selected_current_fy: "current_fy_amount",
            selected_comparison_fy: "previous_fy_amount",
        }
    )
    if "current_fy_amount" not in comparison_df.columns:
        comparison_df["current_fy_amount"] = 0.0
    if "previous_fy_amount" not in comparison_df.columns:
        comparison_df["previous_fy_amount"] = 0.0

    comparison_df = comparison_df[
        (comparison_df["current_fy_amount"] > 0) | (comparison_df["previous_fy_amount"] > 0)
    ].sort_values(["current_fy_amount", "previous_fy_amount", "state"], ascending=[False, False, True])
    comparison_df["current_fy"] = selected_current_fy
    comparison_df["previous_fy"] = selected_comparison_fy
    comparison_df["selected_months"] = ", ".join(selected_months)
    return comparison_df


def get_interest_rate_summary(gsec_df: pd.DataFrame, selected_window: str) -> dict[str, object]:
    if gsec_df.empty:
        return {
            "latest_yield": None,
            "trend_label": "Not available",
            "delta_bps": None,
            "latest_date": None,
            "selected_window": selected_window,
        }

    dataframe = gsec_df.sort_values("date").reset_index(drop=True)
    first_row = dataframe.iloc[0]
    latest_row = dataframe.iloc[-1]
    latest_date = latest_row["date"]
    latest_yield = latest_row["price"]
    delta_bps = (latest_yield - first_row["price"]) * 100

    if abs(delta_bps) < 15:
        trend_label = "Stable"
    elif delta_bps <= -15:
        trend_label = "Easing"
    elif delta_bps >= 15:
        trend_label = "Rising"
    else:
        trend_label = "Stable"

    return {
        "latest_yield": float(latest_yield),
        "trend_label": trend_label,
        "delta_bps": float(delta_bps),
        "latest_date": latest_date,
        "selected_window": selected_window,
    }


def get_gsec_chart_window(
    gsec_df: pd.DataFrame,
    window_label: str,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    if gsec_df.empty:
        return gsec_df

    dataframe = gsec_df.sort_values("date").copy()
    latest_date = dataframe["date"].max()

    if window_label == "Custom" and start_date is not None and end_date is not None:
        return dataframe[(dataframe["date"] >= start_date) & (dataframe["date"] <= end_date)].reset_index(drop=True)

    window_map = {
        "5Y": pd.DateOffset(years=5),
        "1Y": pd.DateOffset(years=1),
        "6M": pd.DateOffset(months=6),
        "3M": pd.DateOffset(months=3),
        "1M": pd.DateOffset(months=1),
    }
    cutoff = latest_date - window_map.get(window_label, pd.DateOffset(years=1))
    return dataframe[dataframe["date"] >= cutoff].reset_index(drop=True)


def get_interest_rate_commentary(trend_label: str | None) -> str:
    if trend_label is None or pd.isna(trend_label):
        return "Current rate direction is not available from the benchmark series."
    if trend_label == "Rising":
        return "Rates have firmed up over the selected period, indicating higher borrowing costs."
    if trend_label == "Easing":
        return "Rates have eased over the selected period, indicating softer borrowing costs."
    if trend_label == "Stable":
        return "Rates have remained broadly stable over the selected period."
    return "Current rate direction is not available from the benchmark series."
