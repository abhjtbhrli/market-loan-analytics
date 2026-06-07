from __future__ import annotations

from datetime import date

import pandas as pd


def _to_naive_timestamp(value: pd.Timestamp | None) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    if ts.tz is not None:
        return ts.tz_localize(None)
    return ts


def get_selected_isin_row(table_df: pd.DataFrame, selected_rows: list[int] | None) -> pd.Series | None:
    if not selected_rows:
        return None
    row_index = selected_rows[0]
    if row_index < 0 or row_index >= len(table_df):
        return None
    return table_df.iloc[row_index]


def build_reissue_candidate_universe(table_df: pd.DataFrame, state: str = "Assam") -> tuple[pd.DataFrame, list[str]]:
    warnings: list[str] = []
    filtered_df = table_df.copy()

    if "state" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["state"].astype("string").str.lower() == state.lower()]
    else:
        warnings.append("State column unavailable. Showing all states in the reissue universe.")

    if "maturity_date" in filtered_df.columns and filtered_df["maturity_date"].notna().any():
        today = pd.Timestamp(date.today())
        filtered_df = filtered_df[filtered_df["maturity_date"] >= today]
    else:
        warnings.append("Maturity date unavailable. Skipping non-matured security filter.")

    if "isin" not in filtered_df.columns:
        warnings.append("ISIN column unavailable. Securities could not be uniquely identified.")

    sort_columns = [column for column in ["issue_date", "maturity_date", "auction_date"] if column in filtered_df.columns]
    if sort_columns:
        filtered_df = filtered_df.sort_values(sort_columns, ascending=[False] * len(sort_columns))

    return filtered_df.reset_index(drop=True), warnings


def calculate_cash_proceeds(face_amount_cr: float, price: float | None) -> float | None:
    if price is None:
        return None
    return face_amount_cr * price / 100


def calculate_premium_discount(face_amount_cr: float, cash_proceeds_cr: float | None) -> float | None:
    if cash_proceeds_cr is None:
        return None
    return cash_proceeds_cr - face_amount_cr


def calculate_coupon_interest(face_amount_cr: float, coupon_rate: float | None) -> tuple[float | None, float | None]:
    if coupon_rate is None:
        return None, None
    annual_coupon_interest_cr = face_amount_cr * coupon_rate / 100
    return annual_coupon_interest_cr, annual_coupon_interest_cr / 2


def calculate_revised_maturity_obligation(existing_maturity_month_repayment_cr: float, face_amount_cr: float) -> float:
    return existing_maturity_month_repayment_cr + face_amount_cr


def calculate_interest_saving(
    face_amount_cr: float,
    market_rate: float | None,
    coupon_rate: float | None,
    valuation_date: pd.Timestamp | None,
    maturity_date: pd.Timestamp | None,
) -> float | None:
    valuation_date = _to_naive_timestamp(valuation_date)
    maturity_date = _to_naive_timestamp(maturity_date)

    if (
        market_rate is None
        or coupon_rate is None
        or valuation_date is None
        or maturity_date is None
        or maturity_date <= valuation_date
    ):
        return None

    years_to_maturity = (maturity_date - valuation_date).days / 365.25
    market_interest_cr = face_amount_cr * market_rate * years_to_maturity / 100
    coupon_interest_cr = face_amount_cr * coupon_rate * years_to_maturity / 100
    return market_interest_cr - coupon_interest_cr


def calculate_total_interest_cost(
    face_amount_cr: float,
    rate: float | None,
    valuation_date: pd.Timestamp | None,
    maturity_date: pd.Timestamp | None,
) -> float | None:
    valuation_date = _to_naive_timestamp(valuation_date)
    maturity_date = _to_naive_timestamp(maturity_date)

    if (
        rate is None
        or valuation_date is None
        or maturity_date is None
        or maturity_date <= valuation_date
    ):
        return None

    years_to_maturity = (maturity_date - valuation_date).days / 365.25
    return face_amount_cr * rate * years_to_maturity / 100


def calculate_reissue_vs_fresh_advantage(
    fresh_cash_proceeds_cr: float | None,
    reissue_cash_proceeds_cr: float | None,
    fresh_interest_cost_cr: float | None,
    reissue_interest_cost_cr: float | None,
) -> float | None:
    if (
        fresh_cash_proceeds_cr is None
        or reissue_cash_proceeds_cr is None
        or fresh_interest_cost_cr is None
        or reissue_interest_cost_cr is None
    ):
        return None

    upfront_advantage_cr = reissue_cash_proceeds_cr - fresh_cash_proceeds_cr
    interest_advantage_cr = fresh_interest_cost_cr - reissue_interest_cost_cr
    return upfront_advantage_cr + interest_advantage_cr


def rank_reissue_recommendations(
    candidate_df: pd.DataFrame,
    maturity_profile_df: pd.DataFrame,
    threshold_cr: float = 2000.0,
    top_n: int = 3,
) -> pd.DataFrame:
    if candidate_df.empty:
        return pd.DataFrame()

    profile_df = maturity_profile_df.copy()
    if not profile_df.empty:
        profile_df["maturity_month_num"] = profile_df["maturity_month_num"].astype("string").str.zfill(2)
        repayment_lookup = (
            profile_df.groupby(["maturity_fy", "maturity_month_num"], dropna=False)["amount_crore"].sum().to_dict()
        )
    else:
        repayment_lookup = {}

    ranked_df = candidate_df.copy()
    ranked_df["maturity_date"] = pd.to_datetime(ranked_df.get("maturity_date"), errors="coerce")
    ranked_df["amount_crore"] = pd.to_numeric(ranked_df.get("amount_crore"), errors="coerce")
    ranked_df["price_rs"] = pd.to_numeric(ranked_df.get("price_rs"), errors="coerce")
    ranked_df["ytm_semi_annual"] = pd.to_numeric(ranked_df.get("ytm_semi_annual"), errors="coerce")
    ranked_df["existing_maturity_repayment_cr"] = ranked_df.apply(
        lambda row: float(
            repayment_lookup.get(
                (
                    row.get("maturity_fy"),
                    row["maturity_date"].strftime("%m") if pd.notna(row.get("maturity_date")) else None,
                ),
                0.0,
            )
        ),
        axis=1,
    )
    ranked_df["within_threshold"] = ranked_df["existing_maturity_repayment_cr"] <= float(threshold_cr)
    ranked_df["maturity_gap_to_threshold_cr"] = threshold_cr - ranked_df["existing_maturity_repayment_cr"]

    ranked_df = ranked_df.sort_values(
        [
            "within_threshold",
            "price_rs",
            "maturity_gap_to_threshold_cr",
            "ytm_semi_annual",
            "issue_date",
        ],
        ascending=[False, False, False, True, False],
        na_position="last",
    )
    return ranked_df.head(top_n).reset_index(drop=True)


def classify_par_position(price: float | None) -> str | None:
    if price is None:
        return None
    if price < 100:
        return "Discount to par"
    if price > 100:
        return "Premium to par"
    return "Par"


def calculate_reissue_effective_cost(
    cash_proceeds_cr: float | None,
    face_amount_cr: float,
    coupon_rate: float | None,
    reissue_date: pd.Timestamp | None,
    maturity_date: pd.Timestamp | None,
    coupon_frequency: int = 2,
    fallback_ytm: float | None = None,
) -> float | None:
    if (
        cash_proceeds_cr is None
        or cash_proceeds_cr <= 0
        or coupon_rate is None
        or reissue_date is None
        or maturity_date is None
        or maturity_date <= reissue_date
    ):
        return fallback_ytm

    coupon_cashflow = face_amount_cr * coupon_rate / 100 / coupon_frequency
    payment_dates: list[pd.Timestamp] = []
    next_payment_date = pd.Timestamp(reissue_date) + pd.DateOffset(months=12 // coupon_frequency)
    while next_payment_date < maturity_date:
        payment_dates.append(next_payment_date)
        next_payment_date = next_payment_date + pd.DateOffset(months=12 // coupon_frequency)
    payment_dates.append(pd.Timestamp(maturity_date))

    outflows: list[float] = []
    for idx, _ in enumerate(payment_dates):
        if idx == len(payment_dates) - 1:
            outflows.append(face_amount_cr + coupon_cashflow)
        else:
            outflows.append(coupon_cashflow)

    def net_present_value(rate: float) -> float:
        return sum(outflow / ((1 + rate) ** (period + 1)) for period, outflow in enumerate(outflows)) - cash_proceeds_cr

    low, high = -0.99, 1.0
    npv_low = net_present_value(low)
    npv_high = net_present_value(high)
    iterations = 0
    while npv_low * npv_high > 0 and iterations < 25:
        high *= 2
        npv_high = net_present_value(high)
        iterations += 1

    if npv_low * npv_high > 0:
        return fallback_ytm

    for _ in range(120):
        mid = (low + high) / 2
        npv_mid = net_present_value(mid)
        if abs(npv_mid) < 1e-9:
            low = high = mid
            break
        if npv_low * npv_mid <= 0:
            high = mid
            npv_high = npv_mid
        else:
            low = mid
            npv_low = npv_mid

    semi_annual_irr = (low + high) / 2
    effective_cost_pct = ((1 + semi_annual_irr) ** coupon_frequency - 1) * 100
    return effective_cost_pct
