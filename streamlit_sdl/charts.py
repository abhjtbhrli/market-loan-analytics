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
    st.markdown(
        '<p style="color:#253240; margin-bottom:0.4rem;">'
        "Eligible Assam securities currently trading in the secondary market, based on FBIL SDL/SGS price data"
        "</p>",
        unsafe_allow_html=True,
    )
    if table_df.empty:
        st.info("No Assam ISINs matched the secondary-market SDL file.")
        return None

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


def render_statewise_comparison_table(table_df: pd.DataFrame, selected_months: list[str]) -> None:
    st.title("State-wise comparison")
    if table_df.empty:
        st.info("No state-wise comparison data is available.")
        return

    current_fy = table_df["current_fy"].iloc[0]
    previous_fy = table_df["previous_fy"].iloc[0]
    current_fy_label = f"FY {current_fy[-5:]}"
    previous_fy_label = f"FY {previous_fy[-5:]}"
    full_month_set = {"Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar"}
    if set(selected_months) == full_month_set:
        st.caption("SDL availed (in Cr.) by all states for all months available in the selected financial years")
    else:
        st.caption(f"SDL availed (in Cr.) by all states for selected months: {', '.join(selected_months)}")

    display_df = table_df[["state", "current_fy_amount", "previous_fy_amount"]].copy()
    display_df.insert(0, "Sl.", range(1, len(display_df) + 1))

    total_current = display_df["current_fy_amount"].sum()
    total_previous = display_df["previous_fy_amount"].sum()

    def fmt(value: float) -> str:
        return f"{value:,.0f}"

    display_df["current_fy_amount"] = display_df["current_fy_amount"].map(fmt)
    display_df["previous_fy_amount"] = display_df["previous_fy_amount"].map(fmt)

    rows_html: list[str] = []
    for _, row in display_df.iterrows():
        is_assam = row["state"] == "Assam"
        row_style = ' style="background:#fff200;font-weight:700;"' if is_assam else ""
        rows_html.append(
            "<tr{row_style}>"
            f"<td>{row['Sl.']}</td>"
            f"<td style='text-align:left'>{html.escape(str(row['state']))}</td>"
            f"<td>{row['current_fy_amount']}</td>"
            f"<td>{row['previous_fy_amount']}</td>"
            "</tr>".format(row_style=row_style)
        )

    rows_html.append(
        "<tr style='font-weight:700;background:#f3f4f6;'>"
        "<td></td>"
        "<td style='text-align:left'>Total</td>"
        f"<td>{fmt(total_current)}</td>"
        f"<td>{fmt(total_previous)}</td>"
        "</tr>"
    )

    table_html = f"""
    <style>
    .state-comparison-wrap {{
        margin-top: 0.5rem;
    }}
    .state-comparison-table {{
        width: 100%;
        border-collapse: collapse;
        table-layout: fixed;
        font-size: 0.98rem;
    }}
    .state-comparison-table th,
    .state-comparison-table td {{
        border: 1px solid #222;
        padding: 0.34rem 0.45rem;
        text-align: center;
        vertical-align: middle;
        line-height: 1.1;
    }}
    .state-comparison-table th {{
        background: #eef2f6;
        font-weight: 800;
    }}
    .state-comparison-table .sl-col {{
        width: 8%;
    }}
    .state-comparison-table .state-col {{
        width: 44%;
        text-align: left;
    }}
    .state-comparison-table .fy-col {{
        width: 24%;
    }}
    </style>
    <div class="state-comparison-wrap">
        <table class="state-comparison-table">
            <thead>
                <tr>
                    <th class="sl-col">Sl.</th>
                    <th class="state-col">State</th>
                    <th class="fy-col">{current_fy_label}</th>
                    <th class="fy-col">{previous_fy_label}</th>
                </tr>
            </thead>
            <tbody>
                {''.join(rows_html)}
            </tbody>
        </table>
    </div>
    """
    st.markdown(table_html, unsafe_allow_html=True)


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
    st.subheader("Current SDL yields")
    st.markdown("**Indicative tenor-wise yields**")
    if table_df.empty:
        st.info("No SDL tenor yields are available.")
        return

    display_df = table_df.copy()
    display_df = display_df[display_df["tenor"] != "Short-term tenors"].copy()
    if display_df.empty:
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
