from __future__ import annotations

import html
import altair as alt
import pandas as pd
import streamlit as st


def render_wrapped_dataframe(dataframe: pd.DataFrame, height: int = 420) -> None:
    st.markdown(
        """
        <style>
        div[data-testid="stDataFrame"] div[role="columnheader"] {
            white-space: normal !important;
            line-height: 1.15 !important;
            word-break: break-word !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.dataframe(dataframe, width="stretch", height=height, hide_index=True)


def format_crore(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:,.2f} Cr"


def format_crore_whole(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:,.0f} Cr"


def render_summary(summary: dict[str, object], fy: str) -> None:
    st.subheader(f"Assam borrowing summary for {fy}")
    col1, col2, col3, col4 = st.columns(4)
    latest_auction = summary["latest_auction_date"]
    latest_auction_label = (
        latest_auction.strftime("%d %b %Y") if latest_auction is not None and not pd.isna(latest_auction) else "-"
    )

    col1.metric("Total borrowing", format_crore_whole(summary["total_borrowing_crore"]))
    col2.metric("Auctions", f"{int(summary['auction_count'])}")
    col3.metric("Average auction size", format_crore(summary["average_auction_size_crore"]))
    col4.metric("Latest market loan availed on", latest_auction_label)


def render_monthly_trend(trend_df: pd.DataFrame) -> None:
    st.subheader("Monthly borrowing trend")
    if trend_df.empty:
        st.info("No Assam monthly trend data is available for the selected filters.")
        return

    chart_df = trend_df.copy()
    chart_df["auction_month"] = pd.to_datetime(chart_df["auction_month"])

    monthly_df = (
        chart_df.groupby("auction_month", as_index=False)["amount_crore"]
        .sum()
        .rename(columns={"amount_crore": "monthly_amount_crore"})
    )
    monthly_df["month_label"] = monthly_df["auction_month"].dt.strftime("%b %Y")

    bars = (
        alt.Chart(monthly_df)
        .mark_bar(color="#2f6db3", size=48)
        .encode(
            x=alt.X(
                "month_label:N",
                title="Month",
                sort=monthly_df["month_label"].tolist(),
                axis=alt.Axis(labelAngle=0),
            ),
            y=alt.Y("monthly_amount_crore:Q", title="Amount (Cr)"),
            tooltip=[
                alt.Tooltip("auction_month:T", title="Month"),
                alt.Tooltip("monthly_amount_crore:Q", title="Monthly total (Cr)", format=",.2f"),
            ],
        )
    )

    st.altair_chart(
        bars.properties(height=360),
        width="stretch",
    )

    quarterly_df = monthly_df.copy()
    quarterly_df["quarter"] = quarterly_df["auction_month"].dt.month.map(
        {
            4: "Q1",
            5: "Q1",
            6: "Q1",
            7: "Q2",
            8: "Q2",
            9: "Q2",
            10: "Q3",
            11: "Q3",
            12: "Q3",
            1: "Q4",
            2: "Q4",
            3: "Q4",
        }
    )
    quarterly_df = (
        quarterly_df.groupby("quarter", as_index=False)["monthly_amount_crore"]
        .sum()
        .rename(columns={"monthly_amount_crore": "Amount"})
    )
    quarter_order = ["Q1", "Q2", "Q3", "Q4"]
    quarterly_df["quarter"] = pd.Categorical(quarterly_df["quarter"], categories=quarter_order, ordered=True)
    quarterly_df = quarterly_df.sort_values("quarter")
    quarterly_df["Amount"] = quarterly_df["Amount"].map(lambda value: f"{value:,.0f}")
    quarterly_df = quarterly_df.rename(columns={"quarter": "Quarter"})

    st.subheader("Quarterly borrowing figures")
    st.dataframe(quarterly_df, width="stretch", hide_index=True)


def render_auction_table(table_df: pd.DataFrame) -> None:
    st.subheader("Auction-level table")
    if table_df.empty:
        st.info("No auctions match the selected filters.")
        return

    display_df = table_df.copy()
    for date_column in ["auction_date", "issue_date", "maturity_date"]:
        display_df[date_column] = pd.to_datetime(display_df[date_column]).dt.strftime("%d-%b-%Y")

    display_df = display_df.rename(
        columns={
            "auction_date": "Auction date",
            "issue_date": "Issue date",
            "maturity_date": "Maturity date",
            "amount_crore": "Amount (Cr)",
            "notified_amount_crore": "Notified amount (Cr)",
            "tenor_group": "Tenor group",
            "loan_name": "Loan name",
            "isin": "ISIN",
            "cut_off_yield": "Cut-off yield",
            "weighted_avg_yield": "Weighted avg yield",
            "bid_cover": "Bid cover",
        }
    )
    st.dataframe(display_df, width="stretch", hide_index=True)


def render_reissue_candidates_table(table_df: pd.DataFrame) -> None:
    st.subheader("Securities eligible for reissue")
    if table_df.empty:
        st.markdown(
            '<p style="color:#253240; margin-bottom:0.4rem;">'
            "Eligible Assam securities currently trading in the secondary market, based on FBIL SDL/SGS price data"
            "</p>",
            unsafe_allow_html=True,
        )
        st.info("No Assam ISINs matched the secondary-market SDL file.")
        return None
    st.markdown(
        '<p style="color:#253240; margin-bottom:0.4rem;">'
        "Eligible Assam securities currently trading in the secondary market, based on FBIL SDL/SGS price data"
        "</p>",
        unsafe_allow_html=True,
    )

    display_df = table_df.copy()
    for date_column in ["issue_date", "maturity_date", "filedate"]:
        display_df[date_column] = pd.to_datetime(display_df[date_column]).dt.strftime("%d-%b-%Y")

    display_df["amount_crore"] = display_df["amount_crore"].map(lambda value: f"{value:,.0f}" if pd.notna(value) else "-")
    display_df["price_rs"] = display_df["price_rs"].map(lambda value: f"{value:,.4f}" if pd.notna(value) else "-")
    display_df["ytm_semi_annual"] = display_df["ytm_semi_annual"].map(
        lambda value: f"{value:,.4f}" if pd.notna(value) else "-"
    )
    filedate_series = display_df["filedate"].dropna()
    filedate_label = filedate_series.iloc[0] if not filedate_series.empty else "-"

    display_df = display_df[
        [
            "loan_name",
            "issue_date",
            "maturity_date",
            "amount_crore",
            "ytm_semi_annual",
            "price_rs",
        ]
    ].rename(
        columns={
            "issue_date": "Issue date",
            "maturity_date": "Maturity date",
            "loan_name": "SDL",
            "amount_crore": "Amount\n(Cr)",
            "price_rs": "Market\nprice",
            "ytm_semi_annual": "Market\nyield",
        }
    )
    st.markdown(
        """
        <style>
        div[data-testid="stDataFrame"] table {
            width: 100% !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    selection = st.dataframe(
        display_df,
        width="stretch",
        height=280,
        hide_index=True,
        key="reissue_candidates_table",
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "SDL": st.column_config.TextColumn("SDL", width="small"),
            "Issue date": st.column_config.TextColumn("Issue date", width="small"),
            "Maturity date": st.column_config.TextColumn("Maturity date", width="small"),
            "Amount\n(Cr)": st.column_config.TextColumn("Amount\n(Cr)", width="small"),
            "Market\nyield": st.column_config.TextColumn(
                "Market\nyield",
                width="small",
            ),
            "Market\nprice": st.column_config.TextColumn(
                "Market\nprice",
                width="small",
            ),
        },
    )
    formatted_filedate = (
        pd.to_datetime(filedate_label, errors="coerce").strftime("%d %b %Y")
        if filedate_label != "-"
        else "-"
    )
    st.markdown(
        '<p style="color:#253240; margin-top:0.4rem; font-style:italic;">'
        "Source: Financial Benchmarks India Pvt. Ltd. (FBIL), "
        f"FBIL snapshot date: {formatted_filedate}"
        "</p>",
        unsafe_allow_html=True,
    )
    selected_rows = selection.selection.rows if selection is not None else []
    if not selected_rows:
        return None
    return table_df.iloc[selected_rows[0]]


def render_reissue_recommendations(table_df: pd.DataFrame, threshold_cr: float = 2000.0) -> None:
    st.markdown("#### Suggested reissue options")
    if table_df.empty:
        st.info("No suggestions are available for the selected filters.")
        return

    st.markdown(
        f"Ranked by preferring securities where existing maturity-month liability is within {threshold_cr:,.0f} Cr, then by highest market price."
    )

    for idx, (_, row) in enumerate(table_df.iterrows(), start=1):
        sdl_name = row.get("loan_name") or row.get("description") or "Security"
        maturity_date = pd.to_datetime(row.get("maturity_date"), errors="coerce")
        maturity_label = maturity_date.strftime("%b %Y") if pd.notna(maturity_date) else "Not available"
        price = pd.to_numeric(row.get("price_rs"), errors="coerce")
        existing_repayment = pd.to_numeric(row.get("existing_maturity_repayment_cr"), errors="coerce")
        price_text = f"{price:,.4f}" if pd.notna(price) else "Not available"

        st.markdown(
            f"""
            <div style="border:1px solid #d9e2ec; border-radius:0.8rem; padding:0.75rem 0.9rem; margin:0.55rem 0; background:#fbfcfe;">
                <p style="margin:0 0 0.25rem 0; font-weight:700;">{idx}. {html.escape(str(sdl_name))}</p>
                <p style="margin:0 0 0.2rem 0;">Maturity: {html.escape(maturity_label)}</p>
                <p style="margin:0 0 0.2rem 0;">Market price: {price_text}</p>
                <p style="margin:0 0 0.2rem 0;">Existing repayment in maturity month: {format_crore(existing_repayment)}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_outstanding_maturity_pivot(table_df: pd.DataFrame) -> None:
    st.markdown('<div id="repayment-schedule"></div>', unsafe_allow_html=True)
    st.subheader("Principal repayment schedule")
    if table_df.empty:
        st.info("No outstanding Assam borrowings are available for the maturity profile.")
        return

    table_df = table_df[["maturity_fy", "maturity_month_num", "amount_crore"]].copy()

    month_order = ["04", "05", "06", "07", "08", "09", "10", "11", "12", "01", "02", "03"]
    month_labels = {
        "04": "Apr",
        "05": "May",
        "06": "Jun",
        "07": "Jul",
        "08": "Aug",
        "09": "Sep",
        "10": "Oct",
        "11": "Nov",
        "12": "Dec",
        "01": "Jan",
        "02": "Feb",
        "03": "Mar",
    }

    pivot_df = pd.pivot_table(
        table_df,
        index="maturity_fy",
        columns="maturity_month_num",
        values="amount_crore",
        aggfunc="sum",
        fill_value=0,
    )
    pivot_df.columns.name = None
    pivot_df = pivot_df.reindex(columns=month_order, fill_value=0)
    pivot_df = pivot_df.rename(columns=month_labels)
    pivot_df["Total"] = pivot_df.sum(axis=1)

    total_row = pd.DataFrame([pivot_df.sum(axis=0)], index=["Total"])
    pivot_df = pd.concat([pivot_df, total_row])
    pivot_df.index.name = "Maturity FY"
    pivot_df = pivot_df.reset_index()

    month_columns = ["Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar", "Total"]
    saturation_columns = ["Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar"]
    display_df = pivot_df.copy()

    def highlight_saturated(value: object) -> str:
        if pd.isna(value) or value == "":
            return ""
        try:
            numeric_value = float(str(value).replace(",", ""))
        except ValueError:
            return ""
        if numeric_value >= 2000:
            return "background-color: #f8b4b4; color: #7f1d1d; font-weight: 600;"
        return ""

    for column in month_columns:
        display_df[column] = display_df[column].map(
            lambda value: "" if pd.isna(value) or float(value) == 0 else f"{float(value):,.0f}"
        )

    styled_df = display_df.style.map(
        highlight_saturated,
        subset=pd.IndexSlice[display_df["Maturity FY"] != "Total", saturation_columns],
    )
    styled_df = styled_df.hide(axis="index")
    styled_df = styled_df.set_properties(subset=["Maturity FY"], **{"font-weight": "700"})
    styled_df = styled_df.set_properties(subset=["Total"], **{"font-weight": "700"})
    grand_total_mask = display_df["Maturity FY"] == "Total"
    styled_df = styled_df.set_properties(
        subset=pd.IndexSlice[grand_total_mask, :],
        **{"font-weight": "700"},
    )
    styled_df = styled_df.set_table_styles(
        [
            {"selector": "table", "props": [("width", "100%"), ("border-collapse", "collapse"), ("font-size", "0.92rem")]},
            {"selector": "th", "props": [("white-space", "normal"), ("padding", "6px 8px"), ("text-align", "center")]},
            {"selector": "td", "props": [("padding", "6px 8px"), ("text-align", "right")]},
        ]
    )
    st.markdown(styled_df.to_html(), unsafe_allow_html=True)


def _render_state_metric_table(
    title: str,
    table_df: pd.DataFrame,
    current_fy_label: str,
    previous_fy_label: str,
    formatter,
    caption_text: str | None = None,
) -> None:
    if title:
        st.markdown(f"#### {title}")
    if caption_text:
        st.caption(caption_text)
    if table_df.empty:
        st.info(f"No {title.lower()} data is available for the selected filters.")
        return

    display_df = table_df[["state", "current_fy_amount", "previous_fy_amount"]].copy().rename(
        columns={
            "state": "State",
            "current_fy_amount": current_fy_label,
            "previous_fy_amount": previous_fy_label,
        }
    )
    display_df.insert(0, "S. No.", range(1, len(display_df) + 1))
    display_df[current_fy_label] = display_df[current_fy_label].map(formatter)
    display_df[previous_fy_label] = display_df[previous_fy_label].map(formatter)
    st.dataframe(display_df, width="stretch", hide_index=True, height=820)


def _render_state_distribution_table(distribution_df: pd.DataFrame, distribution_fy_label: str) -> None:
    st.markdown("#### Borrowing distribution analysis")
    st.caption(f"Share of total SDL availed across states in {distribution_fy_label}")
    if distribution_df.empty:
        st.info("No borrowing distribution data is available for the selected filters.")
        return

    display_df = distribution_df.copy().rename(
        columns={
            "state": "State",
            "amount_crore": f"{distribution_fy_label} amount",
            "share_pct": "Share of total",
        }
    )
    display_df.insert(0, "S. No.", range(1, len(display_df) + 1))
    display_df[f"{distribution_fy_label} amount"] = display_df[f"{distribution_fy_label} amount"].map(
        lambda value: f"{value:,.0f}"
    )
    display_df["Share of total"] = display_df["Share of total"].map(lambda value: f"{value:,.1f}%")
    total_amount = distribution_df["amount_crore"].sum()
    total_row = pd.DataFrame(
        [
            {
                "S. No.": "",
                "State": "All states",
                f"{distribution_fy_label} amount": f"{total_amount:,.0f}",
                "Share of total": "100.0%",
            }
        ]
    )
    display_df = pd.concat([display_df, total_row], ignore_index=True)
    table_height = min(1200, 44 + 35 * max(len(display_df), 1))
    st.dataframe(display_df, width="stretch", hide_index=True, height=table_height)


def _render_state_tenor_distribution_table(distribution_df: pd.DataFrame, distribution_fy_label: str) -> None:
    st.markdown("#### Borrowing distribution analysis")
    st.caption(f"Tenor-bucket mix as a percentage of each state's total borrowing in {distribution_fy_label}")
    if distribution_df.empty:
        st.info("No tenor distribution data is available for the selected filters.")
        return

    display_df = distribution_df.copy()
    tenor_order = ["T1-5", "T6-10", "T11-15", "T16-20", "T20+"]
    ordered_columns = ["state"] + [column for column in tenor_order if column in display_df.columns]
    ordered_columns += [column for column in display_df.columns if column not in ordered_columns]
    display_df = display_df[ordered_columns]
    display_df.insert(0, "S. No.", range(1, len(display_df) + 1))
    value_columns = [column for column in display_df.columns if column not in ["state", "State", "S. No."]]
    for column in value_columns:
        display_df[column] = display_df[column].map(lambda value: f"{value:,.1f}%")
    display_df = display_df.rename(columns={"state": "State"})
    all_states_mask = display_df["State"] == "All states"
    if all_states_mask.any():
        all_states_df = display_df[all_states_mask].copy()
        other_states_df = display_df[~all_states_mask].copy()
        other_states_df["S. No."] = range(1, len(other_states_df) + 1)
        all_states_df["S. No."] = ""
        display_df = pd.concat([other_states_df, all_states_df], ignore_index=True)
    table_height = min(1200, 44 + 35 * max(len(display_df), 1))
    st.dataframe(display_df, width="stretch", hide_index=True, height=table_height)


def render_statewise_comparison_table(
    table_df: pd.DataFrame,
    selected_months: list[str],
    analytics: dict[str, object] | None = None,
) -> None:
    st.title("State-wise comparison")
    if table_df.empty:
        st.info("No state-wise comparison data is available.")
        return

    analytics = analytics or {}
    summary = analytics.get("summary", {}) if isinstance(analytics, dict) else {}
    current_fy = table_df["current_fy"].iloc[0]
    previous_fy = table_df["previous_fy"].iloc[0]
    current_fy_label = f"FY {current_fy[-5:]}"
    previous_fy_label = f"FY {previous_fy[-5:]}"
    distribution_fy = summary.get("distribution_fy", current_fy) if isinstance(summary, dict) else current_fy
    distribution_fy_label = f"FY {distribution_fy[-5:]}" if distribution_fy else current_fy_label
    full_month_set = {"Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar"}
    if set(selected_months) == full_month_set:
        st.caption("SDL availed (in Cr.) by all states for all months available in the selected financial years")
    else:
        st.caption(f"SDL availed (in Cr.) by all states for selected months: {', '.join(selected_months)}")

    comparison_left, comparison_right = st.columns(2)
    with comparison_left:
        _render_state_metric_table(
            "Borrowing amount comparison",
            analytics.get("amount_df", table_df) if isinstance(analytics, dict) else table_df,
            current_fy_label,
            previous_fy_label,
            formatter=lambda value: f"{value:,.0f}",
            caption_text=None,
        )
    with comparison_right:
        st.markdown("#### Select another comparison metric")
        toggle_col, note_col = st.columns([0.48, 0.52])
        metric_choice = toggle_col.segmented_control(
            "Select another comparison metric",
            options=["Yield", "Bid cover"],
            default="Yield",
            key="statewise_metric_choice",
            label_visibility="collapsed",
        )
        if metric_choice == "Bid cover":
            note_col.markdown(
                "<p style='margin:0.25rem 0 0 0; font-size:0.8rem; line-height:1.25; white-space:normal; word-break:break-word;'><em>Value of competitive bids received / value of competitive bids accepted</em></p>",
                unsafe_allow_html=True,
            )
        else:
            note_col.markdown(
                "<p style='margin:0.25rem 0 0 0; font-size:0.8rem; line-height:1.25; white-space:normal; word-break:break-word;'><em>Average cut-off yield across auctions for the selected months</em></p>",
                unsafe_allow_html=True,
            )
        if metric_choice == "Bid cover":
            _render_state_metric_table(
                "",
                analytics.get("bid_cover_df", pd.DataFrame()) if isinstance(analytics, dict) else pd.DataFrame(),
                current_fy_label,
                previous_fy_label,
                formatter=lambda value: f"{value:,.2f}",
                caption_text=None,
            )
        else:
            _render_state_metric_table(
                "",
                analytics.get("yield_df", pd.DataFrame()) if isinstance(analytics, dict) else pd.DataFrame(),
                current_fy_label,
                previous_fy_label,
                formatter=lambda value: f"{value:,.2f}",
                caption_text=None,
            )

    st.markdown("#### Distribution view")
    distribution_choice = st.segmented_control(
        "Distribution view",
        options=["by Amount", "by Tenor"],
        default="by Amount",
        key="statewise_distribution_choice",
        label_visibility="collapsed",
    )
    distribution_amount_by_fy = (
        analytics.get("distribution_amount_by_fy", {}) if isinstance(analytics, dict) else {}
    )
    distribution_tenor_by_fy = (
        analytics.get("distribution_tenor_by_fy", {}) if isinstance(analytics, dict) else {}
    )
    distribution_fy_options = sorted(
        set(distribution_amount_by_fy.keys()) | set(distribution_tenor_by_fy.keys()),
        reverse=True,
    )
    default_distribution_fy = "2026-27" if "2026-27" in distribution_fy_options else (
        distribution_fy_options[0] if distribution_fy_options else None
    )
    distribution_filter_col, _ = st.columns([0.28, 0.72])
    selected_distribution_fy = distribution_filter_col.selectbox(
        "Distribution FY",
        distribution_fy_options,
        index=distribution_fy_options.index(default_distribution_fy) if default_distribution_fy else None,
        key="statewise_distribution_fy",
    ) if distribution_fy_options else None
    selected_distribution_fy_label = (
        f"FY {selected_distribution_fy[-5:]}" if selected_distribution_fy else distribution_fy_label
    )

    if distribution_choice == "by Tenor":
        _render_state_tenor_distribution_table(
            distribution_tenor_by_fy.get(selected_distribution_fy, pd.DataFrame()),
            selected_distribution_fy_label,
        )
    else:
        _render_state_distribution_table(
            distribution_amount_by_fy.get(selected_distribution_fy, pd.DataFrame()),
            selected_distribution_fy_label,
        )


def render_interest_rate_metrics(summary: dict[str, object]) -> None:
    col1, col2, col3 = st.columns(3)
    latest_yield = summary.get("latest_yield")
    trend_symbol = summary.get("trend_symbol", "")
    trend_label = summary.get("trend_label", "Not available")
    delta_bps = summary.get("one_month_delta_bps")

    latest_yield_label = f"{latest_yield:,.2f}%" if latest_yield is not None and not pd.isna(latest_yield) else "-"
    delta_label = f"{delta_bps:+.0f} bps" if delta_bps is not None and not pd.isna(delta_bps) else "-"

    col1.metric("10Y G-Sec", latest_yield_label)
    col2.metric("Trend", f"{trend_symbol} {trend_label}".strip())
    col3.metric("Δ (1 Month)", delta_label)


def render_interest_rate_metrics(summary: dict[str, object]) -> None:
    col1, col2, col3 = st.columns(3)
    latest_yield = summary.get("latest_yield")
    trend_label = summary.get("trend_label", "Not available")
    delta_bps = summary.get("delta_bps")
    selected_window = summary.get("selected_window", "1M")

    latest_yield_label = f"{latest_yield:,.2f}%" if latest_yield is not None and not pd.isna(latest_yield) else "-"
    delta_label = f"{delta_bps:+.0f} bps" if delta_bps is not None and not pd.isna(delta_bps) else "-"
    trend_value = f"{trend_label} ({delta_label}, {selected_window})" if delta_label != "-" else trend_label

    col1.metric("10Y G-Sec", latest_yield_label)
    col2.metric("Trend", trend_value)
    col3.metric(f"Delta ({selected_window})", delta_label)


def render_gsec_trend_chart(chart_df: pd.DataFrame) -> None:
    st.subheader("10Y G-Sec Trend Chart")
    if chart_df.empty:
        st.info("No G-Sec history is available for the selected window.")
        return

    dataframe = chart_df.copy()
    dataframe["date"] = pd.to_datetime(dataframe["date"])
    dataframe["date_label"] = dataframe["date"].dt.strftime("%d %b %Y")
    dataframe["yield_label"] = dataframe["price"].map(lambda value: f"{value:,.3f}%")
    last_point_df = dataframe.tail(1)

    hover = alt.selection_point(fields=["date"], nearest=True, on="mouseover", empty=False)

    base = alt.Chart(dataframe).encode(
        x=alt.X(
            "date:T",
            title="Date",
            axis=alt.Axis(
                format="%d %b",
                labelAngle=0,
                tickCount=8,
                labelOverlap="greedy",
            ),
        ),
        y=alt.Y("price:Q", title="Yield (%)"),
    )
    line = base.mark_line(color="#1f5da8", strokeWidth=2.5)
    selectors = base.mark_point(opacity=0).add_params(hover)
    points = base.mark_point(color="#1f5da8", size=45).encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0))
    )
    tooltips = base.mark_rule(color="#94a3b8").encode(
        opacity=alt.condition(hover, alt.value(0.5), alt.value(0)),
        tooltip=[
            alt.Tooltip("date_label:N", title="Date"),
            alt.Tooltip("yield_label:N", title="Yield"),
        ],
    ).transform_filter(hover)
    last_point = alt.Chart(last_point_df).mark_point(color="#d94841", size=90, filled=True).encode(
        x="date:T",
        y="price:Q",
        tooltip=[
            alt.Tooltip("date_label:N", title="Date"),
            alt.Tooltip("yield_label:N", title="Yield"),
        ],
    )
    last_label = alt.Chart(last_point_df).mark_text(
        align="left",
        dx=8,
        dy=-8,
        color="#d94841",
        fontWeight="bold",
    ).encode(
        x="date:T",
        y="price:Q",
        text=alt.Text("price:Q", format=",.2f"),
    )

    chart = (line + selectors + points + tooltips + last_point + last_label).properties(height=360)
    st.altair_chart(chart, width="stretch")
    st.markdown(
        '<p style="color:#253240; margin-top:0.4rem; font-style:italic;">Source: Investing.com</p>',
        unsafe_allow_html=True,
    )


def render_current_sdl_yields_table(table_df: pd.DataFrame) -> None:
    if table_df.empty:
        st.subheader("Current SDL yields")
        st.markdown("**Indicative tenor-wise yields**")
        st.info("No SDL tenor yields are available.")
        return

    display_df = table_df.copy()
    display_df = display_df[display_df["tenor"] != "Short-term tenors"].copy()
    if display_df.empty:
        st.subheader("Current SDL yields")
        st.markdown("**Indicative tenor-wise yields**")
        st.info("No non-short-term SDL tenor yields are available.")
        return

    display_df["maturity_bucket"] = display_df["maturity_bucket"].map(
        lambda value: f"{int(value)}" if pd.notna(value) else "-"
    )
    display_df["published_ytm"] = display_df["published_ytm"].map(
        lambda value: f"{value:,.4f}" if pd.notna(value) else "-"
    )
    display_df = display_df.rename(
        columns={
            "maturity_bucket": "Maturity year",
            "tenor": "Tenor",
            "published_ytm": "Market yield",
        }
    )[["Maturity year", "Tenor", "Market yield"]]
    st.subheader("Current SDL yields")
    st.markdown("**Indicative tenor-wise yields**")
    st.dataframe(display_df, width="stretch", hide_index=True, height=360)
    filedate_series = pd.to_datetime(table_df["filedate"], errors="coerce").dropna()
    formatted_filedate = filedate_series.iloc[0].strftime("%d %b %Y") if not filedate_series.empty else "-"
    st.markdown(
        '<p style="color:#253240; margin-top:0.4rem; font-style:italic;">'
        "Source: Financial Benchmarks India Pvt. Ltd. (FBIL), "
        f"FBIL snapshot date: {formatted_filedate}"
        "</p>",
        unsafe_allow_html=True,
    )


def render_interest_rate_commentary(commentary_text: str) -> None:
    st.markdown(
        f"""
        <div style="margin-top:0.85rem; padding:0.85rem 1rem; border:1px solid #d9e2ec; border-radius:0.75rem; background:#f7fafc;">
            {html.escape(commentary_text)}
        </div>
        """,
        unsafe_allow_html=True,
    )
