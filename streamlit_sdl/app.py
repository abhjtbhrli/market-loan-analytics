from __future__ import annotations

import sys
from math import ceil
from pathlib import Path

import pandas as pd
import streamlit as st


if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from streamlit_sdl.charts import (
    render_auction_table,
    render_current_sdl_yields_table,
    render_gsec_trend_chart,
    render_interest_rate_commentary,
    render_interest_rate_metrics,
    render_monthly_trend,
    render_outstanding_maturity_pivot,
    render_reissue_candidates_table,
    render_statewise_comparison_table,
    render_summary,
)
from streamlit_sdl.config import DB_SCHEMA_VERSION, DEFAULT_STATE, get_source_data_version
from streamlit_sdl.database import (
    ensure_required_tables,
    initialize_database,
    load_clean_dataframe,
    load_gsec_history_dataframe,
    load_tenor_average_dataframe,
)
from streamlit_sdl.queries import (
    FISCAL_MONTH_OPTIONS,
    get_assam_outstanding_maturity_profile,
    get_assam_reissue_candidates,
    get_assam_summary,
    get_auction_level_data,
    get_available_fys,
    get_gsec_chart_window,
    get_interest_rate_commentary,
    get_interest_rate_summary,
    get_monthly_borrowing_trend,
    get_statewise_comparison,
)
from streamlit_sdl.reissue_logic import (
    build_reissue_candidate_universe,
    calculate_cash_proceeds,
    calculate_interest_saving,
    calculate_premium_discount,
    calculate_reissue_vs_fresh_advantage,
    calculate_revised_maturity_obligation,
    calculate_total_interest_cost,
    classify_par_position,
)


st.set_page_config(page_title="Market Loan Analytics", layout="wide")

PAGE_OPTIONS = [
    "Homepage",
    "Assam",
    "State-wise comparison",
    "Reissuances",
    "Interest Rate Movements",
]

HOMEPAGE_LINK_MAP = {
    "home": "Homepage",
    "assam": "Assam",
    "statewise": "State-wise comparison",
    "reissuances": "Reissuances",
    "rates": "Interest Rate Movements",
}


@st.cache_resource(show_spinner=False)
def get_connection(_schema_version: str = DB_SCHEMA_VERSION, _source_data_version: str = ""):
    connection = initialize_database()
    return ensure_required_tables(connection)


def render_app_style() -> None:
    st.markdown(
        """
        <style>
        :root {
            --page-text: #253240;
            --sidebar-bg: #f7dedd;
            --sidebar-box: #f4cfcf;
            --sidebar-box-hover: #efc0c0;
            --sidebar-active: #ffffff;
            --sidebar-active-border: #d88989;
        }

        .stApp {
            background: #ffffff;
            color: var(--page-text);
        }

        .block-container {
            max-width: 98% !important;
            padding-top: 1.25rem;
            padding-left: 1.5rem;
            padding-right: 1.5rem;
        }

        div[data-testid="stSidebar"] {
            background: var(--sidebar-bg);
            border-right: 1px solid #efd0d0;
        }

        div[data-testid="stSidebarUserContent"] {
            padding-top: 1.2rem;
        }

        div[data-testid="stSidebarUserContent"] .nav-label {
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #8b5f5f;
            margin: 0.3rem 0 0.8rem 0.15rem;
        }

        div[data-testid="stSidebarUserContent"] .nav-active {
            background: var(--sidebar-active);
            border: 1px solid var(--sidebar-active-border);
            color: #3c2b2b;
            padding: 1rem 0.95rem;
            border-radius: 0.2rem;
            font-weight: 700;
            margin: 0.25rem 0 0.55rem 0;
            box-shadow: 0 1px 0 rgba(122, 67, 67, 0.04);
        }

        div[data-testid="stSidebarUserContent"] div.stButton {
            margin-bottom: 0.55rem;
        }

        div[data-testid="stSidebarUserContent"] div.stButton > button {
            width: 100%;
            min-height: 3.9rem;
            text-align: left;
            justify-content: flex-start;
            border: 1px solid transparent;
            background: var(--sidebar-box);
            box-shadow: none;
            padding: 1rem 0.95rem;
            border-radius: 0.2rem;
            color: #5a4343;
            font-weight: 600;
        }

        div[data-testid="stSidebarUserContent"] div.stButton > button:hover {
            background: var(--sidebar-box-hover);
            border-color: #e1a9a9;
            color: #412f2f;
        }

        div[data-testid="stSidebarUserContent"] div.stButton > button:focus {
            border-color: #cc8a8a;
            box-shadow: none;
        }

        h1, h2, h3 {
            color: #2e3b4a;
        }

        div[data-testid="stCaptionContainer"] p,
        div[data-testid="stCaptionContainer"] span,
        .stCaption,
        [data-testid="stWidgetLabel"] p,
        [data-testid="stMarkdownContainer"] p,
        [data-testid="stMarkdownContainer"] li {
            color: #253240 !important;
        }

        [data-testid="stMetric"] {
            background: #fbfcfe;
            border: 1px solid #e8edf3;
            border-radius: 0.75rem;
            padding: 0.8rem 0.9rem;
        }

        .reissue-panel-anchor + div {
            background: #eaf4ff;
            border: 1px solid #cfe4fb;
            border-radius: 0.9rem;
            padding: 0.9rem 1rem 0.8rem 1rem;
        }

        .reissue-panel-anchor + div p {
            margin-bottom: 0.35rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def format_rupee_cr(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "Not available"
    return f"{value:,.2f} Cr"


def format_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "Not available"
    return f"{value:,.2f}%"


def filter_reissue_candidates_by_tenor(
    table_df: pd.DataFrame,
    tenor_range_years: tuple[int, int],
) -> pd.DataFrame:
    if table_df.empty or "maturity_date" not in table_df.columns:
        return table_df

    filtered_df = table_df.copy()
    filtered_df["maturity_date"] = pd.to_datetime(filtered_df["maturity_date"], errors="coerce")
    today = pd.Timestamp.today().normalize()
    filtered_df["tenor_years"] = (filtered_df["maturity_date"] - today).dt.days / 365.25

    min_tenor, max_tenor = tenor_range_years
    filtered_df = filtered_df[
        filtered_df["tenor_years"].notna()
        & (filtered_df["tenor_years"] >= float(min_tenor))
        & (filtered_df["tenor_years"] <= float(max_tenor))
    ]
    return filtered_df.drop(columns=["tenor_years"])


def render_reissue_simulation_panel(
    selected_reissue_row: pd.Series | None,
    maturity_profile_df: pd.DataFrame,
    tenor_df: pd.DataFrame,
) -> None:
    st.markdown('<div class="reissue-panel-anchor"></div>', unsafe_allow_html=True)
    with st.container():
        st.markdown("### Simulation Panel")

        if selected_reissue_row is None:
            st.info("Select a security from the table to run a reissue simulation.")
            return

        maturity_date = pd.to_datetime(selected_reissue_row.get("maturity_date"), errors="coerce")
        filedate = pd.to_datetime(selected_reissue_row.get("filedate"), errors="coerce")
        selected_isin = selected_reissue_row.get("isin")
        coupon_rate = pd.to_numeric(selected_reissue_row.get("coupon"), errors="coerce")
        market_price = pd.to_numeric(selected_reissue_row.get("price_rs"), errors="coerce")
        market_rate = pd.to_numeric(selected_reissue_row.get("ytm_semi_annual"), errors="coerce")
        maturity_year = int(maturity_date.year) if pd.notna(maturity_date) else None
        default_reissue_amount_value = pd.to_numeric(selected_reissue_row.get("amount_crore"), errors="coerce")
        default_reissue_amount = float(default_reissue_amount_value) if pd.notna(default_reissue_amount_value) else 0.0

        if st.session_state.get("selected_reissue_isin") != selected_isin:
            st.session_state["selected_reissue_isin"] = selected_isin
            st.session_state["reissue_amount_cr"] = default_reissue_amount

        maturity_date_label = maturity_date.strftime("%b %Y") if pd.notna(maturity_date) else "Not available"
        maturity_month_num = maturity_date.strftime("%m") if pd.notna(maturity_date) else None
        maturity_fy = selected_reissue_row.get("maturity_fy")

        existing_repayment_due = None
        if maturity_month_num and maturity_fy is not None:
            existing_repayment_due = maturity_profile_df[
                (maturity_profile_df["maturity_fy"] == maturity_fy)
                & (maturity_profile_df["maturity_month_num"] == maturity_month_num)
            ]["amount_crore"].sum()
            existing_repayment_due = float(existing_repayment_due)

        reissue_amount = st.number_input(
            "Proposed face amount (Cr)",
            min_value=0.0,
            step=100.0,
            key="reissue_amount_cr",
        )

        st.markdown("#### Existing obligation of selected security")
        st.markdown(f"- Maturity month: {maturity_date_label}")
        st.markdown(
            f"- Existing repayment in maturity month ({maturity_date_label}): {format_rupee_cr(existing_repayment_due)}"
            if existing_repayment_due is not None
            else f"- Existing repayment in maturity month ({maturity_date_label}): Not available"
        )
        st.markdown("- [View detailed repayment schedule](#repayment-schedule)")

        cash_proceeds_cr = calculate_cash_proceeds(
            reissue_amount,
            None if pd.isna(market_price) else float(market_price),
        )
        par_position = classify_par_position(None if pd.isna(market_price) else float(market_price))

        warnings: list[str] = []
        if pd.isna(market_price):
            warnings.append("FBIL price is not available. Reference pricing context is limited.")
        if pd.isna(market_rate):
            warnings.append("FBIL reference yield is not available. Interest saving cannot be estimated.")
        if pd.isna(coupon_rate):
            warnings.append("Coupon is not available. Coupon interest estimates cannot be calculated.")
        if pd.isna(maturity_date):
            warnings.append("Maturity date is not available. Maturity impact cannot be estimated.")
        if reissue_amount <= 0:
            warnings.append("Enter a proposed face amount greater than zero to view reissue estimates.")

        for warning in warnings:
            st.warning(warning)

        if reissue_amount <= 0:
            return

        premium_discount_cr = calculate_premium_discount(reissue_amount, cash_proceeds_cr)
        interest_saving_cr = calculate_interest_saving(
            reissue_amount,
            None if pd.isna(market_rate) else float(market_rate),
            None if pd.isna(coupon_rate) else float(coupon_rate),
            filedate if pd.notna(filedate) else pd.Timestamp.today().normalize(),
            maturity_date if pd.notna(maturity_date) else None,
        )
        additional_maturity_obligation_cr = reissue_amount if pd.notna(maturity_date) else None
        revised_maturity_obligation_cr = (
            calculate_revised_maturity_obligation(existing_repayment_due or 0, reissue_amount)
            if existing_repayment_due is not None
            else None
        )

        st.markdown("#### Reissue Outcome")
        proceeds_line = f"Cash proceeds: {format_rupee_cr(cash_proceeds_cr)}"
        if par_position:
            par_phrase = {
                "Discount to par": "issued at a discount to face value",
                "Premium to par": "issued at a premium to face value",
                "Par": "issued at par",
            }.get(par_position, par_position.lower())
            proceeds_line = f"{proceeds_line} (*{par_phrase}*)"
        st.markdown(f"**Cash proceeds**: {proceeds_line.split(': ', 1)[1]}")

        st.markdown("#### Financial Impact")
        if premium_discount_cr is None or pd.isna(premium_discount_cr):
            st.markdown("Upfront impact: Not available")
        else:
            upfront_amount_text = format_rupee_cr(abs(float(premium_discount_cr)))
            if premium_discount_cr > 0:
                st.markdown(f"Upfront gain: {upfront_amount_text}")
            elif premium_discount_cr < 0:
                st.markdown(f"Upfront shortfall: {upfront_amount_text}")
            else:
                st.markdown(f"Upfront gain: {upfront_amount_text}")

        if interest_saving_cr is None or pd.isna(interest_saving_cr):
            st.markdown("Interest impact over life of the security: Not available")
        else:
            interest_amount_text = format_rupee_cr(abs(float(interest_saving_cr)))
            if interest_saving_cr > 0:
                st.markdown(f"Interest saving (over life): {interest_amount_text}")
            elif interest_saving_cr < 0:
                st.markdown(f"Additional interest (over life): {interest_amount_text}")
            else:
                st.markdown(f"Interest saving (over life): {interest_amount_text}")

        if (
            premium_discount_cr is None
            or pd.isna(premium_discount_cr)
            or interest_saving_cr is None
            or pd.isna(interest_saving_cr)
        ):
            total_present_value_line = "Total gain (at present value): Not available"
        else:
            total_present_value_cr = float(premium_discount_cr) + float(interest_saving_cr)
            total_present_value_text = format_rupee_cr(abs(total_present_value_cr))
            if total_present_value_cr >= 0:
                total_present_value_line = f"Total gain (at present value): {total_present_value_text}"
            else:
                total_present_value_line = f"Total loss (at present value): {total_present_value_text}"
        st.markdown(f"**{total_present_value_line}**")
        st.markdown("*(estimated based on current yield difference)*")

        st.markdown("#### Repayment Impact")
        st.markdown(f"Additional maturity obligation: {format_rupee_cr(additional_maturity_obligation_cr)}")
        st.markdown(
            f"Revised repayment in maturity month ({maturity_date_label}): "
            f"{format_rupee_cr(revised_maturity_obligation_cr)}"
        )

        st.markdown("#### Reissue vs Fresh Issue Comparison")
        tenor_match_df = tenor_df.copy()
        tenor_match_df["maturity_bucket"] = pd.to_numeric(tenor_match_df["maturity_bucket"], errors="coerce")
        fresh_issue_row = tenor_match_df[tenor_match_df["maturity_bucket"] == maturity_year].head(1)
        fresh_issue_yield = (
            pd.to_numeric(fresh_issue_row["published_ytm"].iloc[0], errors="coerce")
            if not fresh_issue_row.empty
            else None
        )
        comparison_valuation_date = filedate if pd.notna(filedate) else pd.Timestamp.today().normalize()
        fresh_issue_cash_proceeds_cr = reissue_amount
        reissue_cash_proceeds_cr = cash_proceeds_cr
        fresh_issue_interest_cost_cr = calculate_total_interest_cost(
            reissue_amount,
            None if pd.isna(fresh_issue_yield) else float(fresh_issue_yield),
            comparison_valuation_date,
            maturity_date if pd.notna(maturity_date) else None,
        )
        reissue_interest_cost_cr = calculate_total_interest_cost(
            reissue_amount,
            None if pd.isna(coupon_rate) else float(coupon_rate),
            comparison_valuation_date,
            maturity_date if pd.notna(maturity_date) else None,
        )
        comparison_advantage_cr = calculate_reissue_vs_fresh_advantage(
            fresh_issue_cash_proceeds_cr,
            reissue_cash_proceeds_cr,
            fresh_issue_interest_cost_cr,
            reissue_interest_cost_cr,
        )

        if par_position == "Premium to par":
            reissue_price_label = "At Premium"
        elif par_position == "Discount to par":
            reissue_price_label = "At Discount"
        elif par_position == "Par":
            reissue_price_label = "At Par"
        else:
            reissue_price_label = "Not available"

        comparison_rows = [
            ("Face Amount (Cr)", f"{reissue_amount:,.0f}", f"{reissue_amount:,.0f}"),
            (
                "Maturity Year",
                str(maturity_year) if maturity_year is not None else "Not available",
                str(maturity_year) if maturity_year is not None else "Not available",
            ),
            (
                "Yield / Coupon (%)",
                format_pct(None if pd.isna(fresh_issue_yield) else float(fresh_issue_yield)),
                format_pct(None if pd.isna(coupon_rate) else float(coupon_rate)),
            ),
            ("Issue Price", "At Par", reissue_price_label),
            (
                "Cash Proceeds (Cr)",
                f"{fresh_issue_cash_proceeds_cr:,.2f}" if fresh_issue_cash_proceeds_cr is not None else "Not available",
                f"{reissue_cash_proceeds_cr:,.2f}" if reissue_cash_proceeds_cr is not None else "Not available",
            ),
            (
                "Total Interest Cost (Cr)",
                f"{fresh_issue_interest_cost_cr:,.2f}" if fresh_issue_interest_cost_cr is not None else "Not available",
                f"{reissue_interest_cost_cr:,.2f}" if reissue_interest_cost_cr is not None else "Not available",
            ),
        ]
        rows_html = "".join(
            f"<tr><td>{metric}</td><td>{fresh_value}</td><td>{reissue_value}</td></tr>"
            for metric, fresh_value, reissue_value in comparison_rows
        )
        summary_value = "Not available"
        if comparison_advantage_cr is not None and not pd.isna(comparison_advantage_cr):
            sign = "+" if float(comparison_advantage_cr) >= 0 else "-"
            summary_value = f"{sign}{abs(float(comparison_advantage_cr)):,.2f} Cr"

        st.markdown(
            f"""
            <table style="width:100%; border-collapse:collapse; margin-top:0.35rem;">
                <thead>
                    <tr>
                        <th style="text-align:left; border-bottom:1px solid #cbd5e1; padding:0.35rem 0.25rem;">Metric</th>
                        <th style="text-align:left; border-bottom:1px solid #cbd5e1; padding:0.35rem 0.25rem;">Fresh issue</th>
                        <th style="text-align:left; border-bottom:1px solid #cbd5e1; padding:0.35rem 0.25rem;">Reissue</th>
                    </tr>
                </thead>
                <tbody>{rows_html}</tbody>
            </table>
            <p style="margin-top:0.55rem; font-weight:700;">Savings / (Additional Cost): {summary_value}</p>
            <p style="margin-top:0.1rem;">Positive value indicates cost advantage of reissue over fresh issue.</p>
            """,
            unsafe_allow_html=True,
        )


def render_sidebar_navigation() -> str:
    if "selected_page" not in st.session_state:
        st.session_state.selected_page = PAGE_OPTIONS[0]

    st.sidebar.markdown('<div class="nav-label">Navigation - Market Loan Analytics</div>', unsafe_allow_html=True)

    for page_name in PAGE_OPTIONS:
        if st.session_state.selected_page == page_name:
            st.sidebar.markdown(f'<div class="nav-active">{page_name}</div>', unsafe_allow_html=True)
        else:
            if st.sidebar.button(page_name, key=f"nav_{page_name}", use_container_width=True):
                st.session_state.selected_page = page_name
                st.rerun()

    return st.session_state.selected_page


def render_homepage() -> None:
    st.title("Market Loan Analytics Platform")
    st.markdown(
        "A decision-support tool for analysing SDL borrowings, interest rate trends, and reissuance strategies."
    )

    st.subheader("Start here")
    st.markdown(
        """
        <style>
        .homepage-start {
            margin-top: 0.35rem;
            padding-left: 0.15rem;
        }
        .homepage-start ul {
            list-style: none;
            margin: 0;
            padding: 0;
        }
        .homepage-start li {
            margin: 0.85rem 0;
            line-height: 1.45;
        }
        .homepage-start li::before {
            content: "\\2022";
            color: #5f7389;
            font-weight: 700;
            display: inline-block;
            width: 1rem;
            margin-left: -0.1rem;
        }
        .homepage-start a {
            color: #1f5da8;
            text-decoration: none;
            font-size: 1.02rem;
        }
        .homepage-start a:hover {
            text-decoration: underline;
        }
        .homepage-destination {
            font-weight: 700;
            color: #243648;
        }
        .homepage-start-note {
            margin-top: 1.25rem;
            color: #58687a;
            font-size: 0.96rem;
        }
        </style>
        <div class="homepage-start">
            <ul>
                <li><a href="?page=assam">View borrowing trends &rarr; <span class="homepage-destination">Assam Dashboard</span></a></li>
                <li><a href="?page=statewise">Compare states &rarr; <span class="homepage-destination">State-wise Comparison</span></a></li>
                <li><a href="?page=reissuances">Evaluate reissue vs fresh borrowing &rarr; <span class="homepage-destination">Reissue Simulator</span></a></li>
                <li><a href="?page=rates">Track market conditions &rarr; <span class="homepage-destination">Interest Rate Movements</span></a></li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    render_app_style()
    requested_page_key = st.query_params.get("page")
    if requested_page_key:
        requested_page = HOMEPAGE_LINK_MAP.get(str(requested_page_key).lower())
        if requested_page:
            st.session_state.selected_page = requested_page
    selected_page = render_sidebar_navigation()

    if selected_page == "Homepage":
        render_homepage()
        return

    if selected_page == "Reissuances":
        st.title("Reissue Scenario Analysis")
        connection = get_connection(DB_SCHEMA_VERSION, get_source_data_version())
        raw_reissue_df = get_assam_reissue_candidates(connection, DEFAULT_STATE)
        reissue_df, reissue_warnings = build_reissue_candidate_universe(raw_reissue_df, DEFAULT_STATE)
        maturity_profile_df = get_assam_outstanding_maturity_profile(connection, DEFAULT_STATE)
        tenor_df = load_tenor_average_dataframe()
        left_col, right_col = st.columns([1.55, 0.9])
        for warning in reissue_warnings:
            st.warning(warning)
        with left_col:
            maturity_dates = pd.to_datetime(reissue_df.get("maturity_date"), errors="coerce")
            if maturity_dates.notna().any():
                tenor_years = ((maturity_dates - pd.Timestamp.today().normalize()).dt.days / 365.25).dropna()
                slider_min = max(0, int(tenor_years.min()))
                slider_max = max(slider_min, int(ceil(tenor_years.max())))
                selected_tenor_range = st.slider(
                    "Select tenor range (years)",
                    min_value=slider_min,
                    max_value=slider_max,
                    value=(slider_min, slider_max),
                    step=1,
                )
                reissue_df = filter_reissue_candidates_by_tenor(reissue_df, selected_tenor_range)
            selected_reissue_row = render_reissue_candidates_table(reissue_df)
        with right_col:
            render_reissue_simulation_panel(selected_reissue_row, maturity_profile_df, tenor_df)
        st.divider()
        render_outstanding_maturity_pivot(maturity_profile_df)
        return

    if selected_page == "State-wise comparison":
        connection = get_connection(DB_SCHEMA_VERSION, get_source_data_version())
        fy_options = sorted(load_clean_dataframe()["fy"].dropna().unique(), reverse=True)
        filter_col1, filter_col2, filter_col3 = st.columns([1, 1, 1.4])
        selected_current_fy = filter_col1.selectbox("Primary FY", fy_options, index=0)
        comparison_candidates = [fy for fy in fy_options if fy != selected_current_fy]
        selected_comparison_fy = filter_col2.selectbox(
            "Comparison FY",
            comparison_candidates,
            index=0 if comparison_candidates else None,
        )
        month_labels = [month for month, _ in FISCAL_MONTH_OPTIONS if month != "All months"]
        selected_months = filter_col3.multiselect(
            "Compare borrowings for months",
            month_labels,
            default=month_labels,
        )
        comparison_df = get_statewise_comparison(
            connection,
            selected_months=selected_months,
            current_fy=selected_current_fy,
            comparison_fy=selected_comparison_fy,
        )
        render_statewise_comparison_table(comparison_df, selected_months)
        return

    if selected_page == "Interest Rate Movements":
        st.title("Interest Rate Movements")
        st.markdown("Track recent interest rate trends and current borrowing levels")

        gsec_df = load_gsec_history_dataframe()
        tenor_df = load_tenor_average_dataframe()
        window_choice = st.segmented_control(
            "Select window",
            options=["5Y", "1Y", "6M", "3M", "1M", "Custom"],
            default="1M",
            selection_mode="single",
        )
        selected_window = window_choice or "1M"
        custom_start = None
        custom_end = None
        if selected_window == "Custom" and not gsec_df.empty:
            min_date = gsec_df["date"].min().date()
            max_date = gsec_df["date"].max().date()
            default_start = max(min_date, (pd.Timestamp(max_date) - pd.DateOffset(years=1)).date())
            date_col1, date_col2 = st.columns(2)
            custom_start = pd.Timestamp(
                date_col1.date_input("Start date", value=default_start, min_value=min_date, max_value=max_date)
            )
            custom_end = pd.Timestamp(
                date_col2.date_input("End date", value=max_date, min_value=min_date, max_value=max_date)
            )

        gsec_chart_df = get_gsec_chart_window(gsec_df, selected_window, custom_start, custom_end)
        summary = get_interest_rate_summary(gsec_chart_df, selected_window)
        render_interest_rate_metrics(summary)

        left_col, right_col = st.columns([1.45, 0.95])
        with left_col:
            render_gsec_trend_chart(gsec_chart_df)
        with right_col:
            render_current_sdl_yields_table(tenor_df)

        render_interest_rate_commentary(get_interest_rate_commentary(summary.get("trend_label")))
        return

    connection = get_connection(DB_SCHEMA_VERSION, get_source_data_version())
    fy_options = get_available_fys(connection, DEFAULT_STATE)
    if not fy_options:
        st.error("No Assam data is available in the DuckDB table.")
        return

    st.title("Market Loan Dashboard - Assam")
    filter_col1, filter_col2 = st.columns([1, 2])
    selected_fy = filter_col1.selectbox("Financial year", fy_options, index=0)

    date_bounds_df = get_auction_level_data(
        connection,
        fy=selected_fy,
        start_date=None,
        end_date=None,
        state=DEFAULT_STATE,
    )
    min_date = date_bounds_df["auction_date"].min().date()
    max_date = date_bounds_df["auction_date"].max().date()
    selected_date_range = filter_col2.date_input(
        "Auction date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(selected_date_range, tuple) and len(selected_date_range) == 2:
        start_date, end_date = selected_date_range
    else:
        start_date, end_date = min_date, max_date

    summary = get_assam_summary(connection, selected_fy, DEFAULT_STATE)
    render_summary(summary, selected_fy)

    trend_df = get_monthly_borrowing_trend(connection, selected_fy, DEFAULT_STATE)
    render_monthly_trend(trend_df)

    auction_df = get_auction_level_data(
        connection,
        fy=selected_fy,
        start_date=str(start_date),
        end_date=str(end_date),
        state=DEFAULT_STATE,
    )
    render_auction_table(auction_df)


if __name__ == "__main__":
    main()
