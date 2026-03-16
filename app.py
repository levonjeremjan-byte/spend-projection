"""
Spend Projection – Interactive Tool
Run:  streamlit run app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import openpyxl
import calendar
import re
import io
import requests
from pathlib import Path
from datetime import date
from contextlib import redirect_stdout

from spend_projection import (
    read_historical_csv,
    read_daily_gmv,
    read_am_files,
    read_provider_gmv_from_data_drop,
    build_historical_benchmarks,
    build_fallback_benchmarks,
    compute_weekly_gmv,
    match_and_project,
    compute_wow_changes,
    enrich_with_liquidity,
    iso_week_to_dates,
)

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"

# ─── Country Configurations ──────────────────────────────────────────────────
# To add a new country: add an entry here + place its CSVs in data/<CC>/

COUNTRIES = {
    "CZ": {
        "name": "Czechia",
        "historical_csv": str(DATA_DIR / "CZ" / "daily_campaign_spend.csv"),
        "gmv_csv": str(DATA_DIR / "CZ" / "daily_gmv.csv"),
        "exclude_ams": ["Klára Bradová"],
        "exclude_spend_objectives": ["provider_campaign_locations"],
        "google_sheets": {
            "Jan Matějka - CZ campaigns 2026": "1jYFaJKX4uuKvKhcM3zLOCcdvftwRrCMYIoqINYGFF5w",
            "Erika Šimková - CZ campaigns 2026": "1KFbT6IDdY48ElQ6UMyYSItSi1g__iGOtEj4fLiD1QR0",
            "Anežka Rücklová - CZ campaigns 2026": "1-SZv-p-52ypwmIj57VfhP2BNzJxFHpz1Ggy2pOzkS0Q",
            "Berta Šimonová - CZ campaigns 2026": "1j0hfBgbGG-DrfYIV1O0Zs7p8WJ5lQapcFuZBorRkMEw",
            "Laura Ernestová - CZ campaigns 2026": "1mVkEOcYKMl5iuchL_1ZOmAXsH4qPjiR0Ecr-o1IiPZQ",
            "Peter Ciuprik - CZ campaigns 2026": "1J8ExwH8U5ae_PwaMgN92KKPmwhRY4dStF4FVgeW3Rd0",
            "Lukáš Bílý - CZ campaigns 2026": "1t6eIku63Apz74tZUWBji6J1xUPbHffLJdsu56Iewboo",
            "Marketing forms - CZ campaigns 2026": "1QcHGAi40E2WGJfOm6UnimFas0itzvZV79z6T5uhR23I",
        },
    },
}

MONTH_NAMES = {i: calendar.month_name[i] for i in range(1, 13)}


# ─── Helpers ─────────────────────────────────────────────────────────────────

def weeks_in_month(year, month):
    weeks = set()
    for day in range(1, calendar.monthrange(year, month)[1] + 1):
        weeks.add(date(year, month, day).isocalendar()[1])
    return sorted(weeks)


def scan_uploaded_files(file_buffers, target_weeks):
    """Scan in-memory uploaded files for week tabs."""
    rows = []
    for name, buf in file_buffers.items():
        am_name = name.rsplit(".xlsx", 1)[0].split(" - ")[0].strip()
        buf.seek(0)
        wb = openpyxl.load_workbook(buf, read_only=True)
        target_tabs, other_tabs, skipped = [], [], []
        for sn in wb.sheetnames:
            sn_lower = sn.lower().strip()
            if "copy" in sn_lower or "template" in sn_lower:
                skipped.append(sn)
                continue
            m = re.search(r"(?:Week|W)\s*(\d+)", sn, re.IGNORECASE)
            if m:
                wk = int(m.group(1))
                if wk in target_weeks:
                    target_tabs.append(f"W{wk}")
                else:
                    other_tabs.append(f"W{wk}")
        wb.close()
        rows.append({
            "File": name,
            "AM": am_name,
            "Matched Weeks": ", ".join(target_tabs) or "—",
            "Other Weeks": ", ".join(other_tabs) or "—",
            "Skipped Tabs": ", ".join(skipped) or "—",
        })
    return pd.DataFrame(rows)


def fetch_google_sheets(sheets_config):
    """Download Google Sheets as .xlsx files into memory.

    Args:
        sheets_config: dict mapping "AM Name - Country campaigns Year" -> sheet_id

    Returns:
        (files_dict, errors_list) where files_dict maps "name.xlsx" -> BytesIO
    """
    files = {}
    errors = []
    for label, sheet_id in sheets_config.items():
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
        try:
            resp = requests.get(url, timeout=60)
            if resp.status_code == 200 and resp.headers.get(
                "content-type", ""
            ).startswith("application/vnd.openxmlformats"):
                fname = f"{label}.xlsx"
                files[fname] = io.BytesIO(resp.content)
            else:
                errors.append(
                    f"{label}: HTTP {resp.status_code} "
                    f"(may require sign-in or is not shared publicly)"
                )
        except requests.RequestException as e:
            errors.append(f"{label}: {e}")
    return files, errors


@st.cache_data(show_spinner=False)
def _hist_meta(path, _mtime):
    df = pd.read_csv(path, usecols=["Date"], parse_dates=["Date"])
    return len(df), str(df["Date"].min().date()), str(df["Date"].max().date())


@st.cache_data(show_spinner=False)
def _gmv_meta(path, _mtime):
    df = pd.read_csv(path, usecols=["date", "daily_gmv_eur"], parse_dates=["date"])
    return len(df), str(df["date"].min().date()), str(df["date"].max().date()), df["daily_gmv_eur"].mean()


def build_config(country_key, year, target_weeks, error_margin, am_buffers):
    cc = COUNTRIES[country_key]
    week_dates = {w: iso_week_to_dates(year, w) for w in target_weeks}
    first_monday = iso_week_to_dates(year, target_weeks[0])[0]
    month_num = date(*[int(x) for x in first_monday.split("-")]).month
    return {
        "country_code": country_key,
        "country_name": cc["name"],
        "projection_label": f"{MONTH_NAMES.get(month_num, '')} {year}",
        "year": year,
        "target_weeks": target_weeks,
        "error_margin": error_margin,
        "am_file_buffers": am_buffers,
        "am_files_dir": "",
        "historical_csv": cc["historical_csv"],
        "gmv_csv": cc["gmv_csv"],
        "exclude_ams": cc["exclude_ams"],
        "exclude_spend_objectives": cc["exclude_spend_objectives"],
        "_week_dates": week_dates,
    }


def run_pipeline(cfg):
    log = io.StringIO()
    with redirect_stdout(log):
        hist_df = read_historical_csv(cfg)
        gmv_df = read_daily_gmv(cfg)
        planned_df = read_am_files(cfg)
        provider_gmv = read_provider_gmv_from_data_drop(cfg)
        benchmarks = build_historical_benchmarks(hist_df, gmv_df)
        fallback = build_fallback_benchmarks(hist_df, provider_gmv, gmv_df)
        weekly_gmv = compute_weekly_gmv(gmv_df, cfg["_week_dates"])
        projected_df = match_and_project(
            planned_df, benchmarks, fallback, provider_gmv, weekly_gmv
        )
        projected_df = enrich_with_liquidity(projected_df)
        wow_df = compute_wow_changes(projected_df, cfg["target_weeks"])
    return {
        "projected_df": projected_df,
        "weekly_gmv": weekly_gmv,
        "wow_df": wow_df,
        "log": log.getvalue(),
        "cfg": cfg,
    }


# ─── Page Config ─────────────────────────────────────────────────────────────

st.set_page_config(page_title="Spend Projection", page_icon="📊", layout="wide")
st.title("Spend Projection Tool")

if "am_files" not in st.session_state:
    st.session_state["am_files"] = {}

# ─── Sidebar ─────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Configuration")

    country_key = st.selectbox(
        "Country", list(COUNTRIES.keys()),
        format_func=lambda k: f"{k} – {COUNTRIES[k]['name']}",
    )
    cc = COUNTRIES[country_key]

    col_m, col_y = st.columns(2)
    with col_m:
        month = st.selectbox("Month", range(1, 13), index=2,
                             format_func=lambda m: MONTH_NAMES[m])
    with col_y:
        year = st.number_input("Year", value=2026, min_value=2024, max_value=2030)

    auto_weeks = weeks_in_month(year, month)
    target_weeks = st.multiselect("Target Weeks (ISO)", options=range(1, 54),
                                  default=auto_weeks)
    target_weeks = sorted(target_weeks)

    error_margin = st.slider("Error Margin %", 0, 50, 25) / 100

    st.divider()

    n_am = len(st.session_state["am_files"])
    hist_ok = Path(cc["historical_csv"]).exists()
    gmv_ok = Path(cc["gmv_csv"]).exists()
    has_gsheets = bool(cc.get("google_sheets"))

    st.caption(f"{'✅' if n_am > 0 else '❌'} {n_am} AM files loaded")
    if n_am == 0 and has_gsheets:
        st.caption("   ↳ Click **Fetch Sheets** in the AM tab")
    st.caption(f"{'✅' if hist_ok else '❌'} Historical spend CSV")
    st.caption(f"{'✅' if gmv_ok else '❌'} Daily GMV CSV")

    st.divider()

    can_run = n_am > 0 and hist_ok and gmv_ok and len(target_weeks) > 0
    run_btn = st.button("Run Projection", type="primary", disabled=not can_run,
                        use_container_width=True)

# ─── Run pipeline ────────────────────────────────────────────────────────────

if run_btn:
    cfg = build_config(country_key, year, target_weeks, error_margin,
                       st.session_state["am_files"])
    with st.spinner("Running projection pipeline (~60s)..."):
        result = run_pipeline(cfg)
    st.session_state["result"] = result
    st.session_state["run_ts"] = pd.Timestamp.now()

# ─── Tabs ────────────────────────────────────────────────────────────────────

tab_am, tab_proj, tab_data = st.tabs([
    "AM Campaign Files", "Projection Results", "Data Sources",
])

# ═══ TAB 1: AM FILES ═════════════════════════════════════════════════════════

with tab_am:
    st.subheader("AM Campaign Files")

    gsheets = cc.get("google_sheets", {})

    if gsheets:
        st.markdown("**Fetch from Google Sheets**")
        st.caption(
            f"{len(gsheets)} sheets configured for {cc['name']}. "
            "Click the button to download the latest data directly."
        )
        fetch_col1, fetch_col2 = st.columns([1, 3])
        with fetch_col1:
            fetch_btn = st.button("Fetch Sheets", type="primary",
                                  use_container_width=True)
        if fetch_btn:
            with st.spinner(f"Downloading {len(gsheets)} sheets from Google..."):
                fetched, fetch_errors = fetch_google_sheets(gsheets)
            if fetched:
                st.session_state["am_files"].update(fetched)
                st.success(f"Loaded {len(fetched)} sheets from Google.")
            if fetch_errors:
                for err in fetch_errors:
                    st.warning(err)
            if fetched:
                st.rerun()

        st.divider()

    with st.expander("Manual upload (optional)"):
        st.caption("Upload .xlsx files for AMs not in Google Sheets, "
                   "or to override a fetched file.")
        uploaded = st.file_uploader("Drop .xlsx files here", type=["xlsx"],
                                    accept_multiple_files=True, key="am_uploader")
        if uploaded:
            for f in uploaded:
                st.session_state["am_files"][f.name] = io.BytesIO(f.getvalue())

    if st.session_state["am_files"]:
        if st.button("Clear all files"):
            st.session_state["am_files"] = {}
            st.rerun()

        if len(target_weeks) > 0:
            st.divider()
            st.markdown("**Loaded files & detected week tabs**")
            scan_df = scan_uploaded_files(st.session_state["am_files"], target_weeks)
            st.dataframe(scan_df, hide_index=True)
    else:
        st.info("No files loaded yet. Click **Fetch Sheets** above to get started.")

    st.divider()
    with st.expander("Tab naming rules"):
        st.markdown("""
The engine recognises these tab name patterns:

| Pattern | Example | Handled? |
|---|---|---|
| `Week 10 (2026)` | Standard | Yes |
| `WEEK 10 (2026)` | All caps | Yes |
| `W10 (2026)` | Shorthand | Yes |
| `W 10` | Shorthand with space | Yes |
| `Copy of Week 10` | Duplicate tab | Skipped |
| `TEMPLATE (COPY)` | Template | Skipped |

**Column order must match the standard AM campaign file template** (Week, Provider ID, City, Provider Name, Commitment, Campaign, Discount %, Cost Sharing %, Users, ..., Investments reason at column L).
Data is read from row 4 onwards (row 3 = header).
        """)

# ═══ TAB 2: PROJECTION RESULTS ═══════════════════════════════════════════════

with tab_proj:
    if "result" not in st.session_state:
        st.info("Upload AM files, configure settings in the sidebar, and click **Run Projection**.")
    else:
        res = st.session_state["result"]
        proj = res["projected_df"]
        wgmv = res["weekly_gmv"]
        wow = res["wow_df"]
        cfg = res["cfg"]
        tw = cfg["target_weeks"]
        margin = cfg["error_margin"]
        total_gmv = sum(wgmv.get(w, 0) for w in tw)
        total_spend = proj["weekly_projected_bolt_eur"].sum()

        total_liq = proj["liquidity_spend_eur"].sum() if "liquidity_spend_eur" in proj.columns else 0
        total_am_only = proj["am_spend_eur"].sum() if "am_spend_eur" in proj.columns else total_spend
        has_liq = total_liq > 0

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Projected Spend", f"€{total_spend:,.0f}")
        m2.metric("% of GMV", f"{total_spend / total_gmv * 100:.3f}%" if total_gmv else "—")
        if has_liq:
            m3.metric("AM Spend (excl. liquidity)", f"€{total_am_only:,.0f}",
                      delta=f"-€{total_liq:,.0f} liquidity separated",
                      delta_color="off")
        else:
            m3.metric(f"With {int(margin*100)}% Margin", f"€{total_spend * (1 + margin):,.0f}")
        m4.metric("Campaigns Processed", f"{len(proj):,}")

        if has_liq:
            m5, m6, m7, m8 = st.columns(4)
            m5.metric(f"AM Spend + {int(margin*100)}% Margin", f"€{total_am_only * (1 + margin):,.0f}")
            m6.metric("AM Spend % of GMV",
                      f"{total_am_only / total_gmv * 100:.3f}%" if total_gmv else "—")
            m7.metric("Liquidity to Separate", f"€{total_liq:,.0f}")
            n_liq = proj["is_liquidity"].sum() if "is_liquidity" in proj.columns else 0
            m8.metric("Liquidity Campaigns", f"{n_liq:,}")

        st.caption(
            f"Run at {st.session_state['run_ts'].strftime('%H:%M:%S')} · "
            f"{cfg['country_code']} · {cfg['projection_label']} · "
            f"Weeks {tw[0]}–{tw[-1]}"
        )

        tab_names = [
            "By Spend Objective", "By Account Manager",
            "Liquidity Split",
            "Week-over-Week Changes", "Flagged Campaigns", "Campaign Detail",
        ]
        sub_reason, sub_am, sub_liq, sub_wow, sub_flags, sub_detail = st.tabs(tab_names)

        # ── By Spend Objective ────────────────────────────────────────
        with sub_reason:
            pivot = proj.pivot_table(
                index="reason_display", columns="week",
                values="weekly_projected_bolt_eur", aggfunc="sum", fill_value=0,
            )
            for w in tw:
                if w not in pivot.columns:
                    pivot[w] = 0
            pivot = pivot[tw]
            pivot["Total"] = pivot.sum(axis=1)
            pivot[f"+ {int(margin*100)}% margin"] = pivot["Total"] * (1 + margin)

            totals = pivot.sum().to_frame().T
            totals.index = ["TOTAL"]
            eur_display = pd.concat([pivot, totals])

            st.markdown("**Projected Bolt Spend (EUR)**")
            st.dataframe(
                eur_display.style.format({c: "€{:,.0f}" for c in eur_display.columns}),
                hide_index=False,
            )
            st.download_button("Download as CSV", eur_display.to_csv(),
                               "projection_by_objective.csv", "text/csv")

            st.markdown("**Projected Bolt Spend (% of GMV)**")
            pct = pivot[tw].copy()
            for w in tw:
                wg = wgmv.get(w, 1)
                pct[w] = pivot[w] / wg if wg > 0 else 0
            pct["Total"] = pivot["Total"] / total_gmv if total_gmv > 0 else 0
            pct[f"+ {int(margin*100)}% margin"] = pct["Total"] * (1 + margin)
            pct_totals = pct.sum().to_frame().T
            pct_totals.index = ["TOTAL"]
            st.dataframe(
                pd.concat([pct, pct_totals]).style.format("{:.3%}"),
                hide_index=False,
            )

        # ── By Account Manager ────────────────────────────────────────
        with sub_am:
            am_pivot = proj.pivot_table(
                index="am_name", columns="week",
                values="weekly_projected_bolt_eur", aggfunc="sum", fill_value=0,
            )
            for w in tw:
                if w not in am_pivot.columns:
                    am_pivot[w] = 0
            am_pivot = am_pivot[tw]
            am_pivot["Total"] = am_pivot.sum(axis=1)
            am_pivot[f"+ {int(margin*100)}% margin"] = am_pivot["Total"] * (1 + margin)
            am_pivot = am_pivot.sort_values("Total", ascending=False)

            totals_am = am_pivot.sum().to_frame().T
            totals_am.index = ["GRAND TOTAL"]
            am_display = pd.concat([am_pivot, totals_am])

            st.markdown("**Summary per Account Manager (EUR)**")
            st.dataframe(
                am_display.style.format({c: "€{:,.0f}" for c in am_display.columns}),
                hide_index=False,
            )
            st.download_button("Download as CSV", am_display.to_csv(),
                               "projection_by_am.csv", "text/csv")

            st.markdown("**Summary per Account Manager (% of GMV)**")
            am_pct = am_pivot[tw].copy()
            for w in tw:
                wg = wgmv.get(w, 1)
                am_pct[w] = am_pivot[w] / wg if wg > 0 else 0
            am_pct["Total"] = am_pivot["Total"] / total_gmv if total_gmv > 0 else 0
            am_pct[f"+ {int(margin*100)}% margin"] = am_pct["Total"] * (1 + margin)
            am_pct_totals = am_pct.sum().to_frame().T
            am_pct_totals.index = ["GRAND TOTAL"]
            st.dataframe(
                pd.concat([am_pct, am_pct_totals]).style.format("{:.3%}"),
                hide_index=False,
            )

            st.divider()
            st.markdown("**Top providers per AM (≥80% of spend)**")
            threshold = st.slider(
                "Cumulative spend threshold %", 50, 100, 80,
                key="top_prov_thresh", format="%d%%",
            ) / 100

            for am in am_pivot.index:
                am_total = am_pivot.loc[am, "Total"]
                if am_total <= 0:
                    continue
                prov_spend = (
                    proj[proj["am_name"] == am]
                    .groupby(["provider_id", "provider_name"])["weekly_projected_bolt_eur"]
                    .sum()
                    .reset_index()
                    .rename(columns={"weekly_projected_bolt_eur": "Total Spend (EUR)"})
                    .sort_values("Total Spend (EUR)", ascending=False)
                )
                prov_spend["% of AM Spend"] = prov_spend["Total Spend (EUR)"] / am_total
                prov_spend["Cumulative %"] = prov_spend["% of AM Spend"].cumsum()
                top = prov_spend[
                    prov_spend["Cumulative %"].shift(1, fill_value=0) < threshold
                ].copy()

                with st.expander(
                    f"{am} — {len(top)} providers cover "
                    f"{top['% of AM Spend'].sum():.0%} of €{am_total:,.0f}"
                ):
                    st.dataframe(
                        top.style.format({
                            "Total Spend (EUR)": "€{:,.0f}",
                            "% of AM Spend": "{:.1%}",
                            "Cumulative %": "{:.1%}",
                            "provider_id": "{:.0f}",
                        }),
                        hide_index=True,
                    )

            st.divider()
            st.markdown("**Breakdown by AM + Spend Objective**")
            selected_am = st.selectbox("Select AM", ["All"] + list(am_pivot.index))
            src = proj if selected_am == "All" else proj[proj["am_name"] == selected_am]

            bd = src.pivot_table(
                index=["am_name", "reason_display"], columns="week",
                values="weekly_projected_bolt_eur", aggfunc="sum", fill_value=0,
            )
            for w in tw:
                if w not in bd.columns:
                    bd[w] = 0
            bd = bd[tw]
            bd["Total"] = bd.sum(axis=1)
            bd = bd.sort_values(["am_name", "Total"], ascending=[True, False])
            st.dataframe(
                bd.style.format({c: "€{:,.0f}" for c in bd.columns}),
                hide_index=False,
            )

        # ── Liquidity Split ─────────────────────────────────────────────
        with sub_liq:
            if not has_liq:
                st.info("No liquidity campaigns detected. Campaigns are tagged as "
                        "liquidity when their AM COMMENTS column (col X) mentions "
                        "'liquidity'.")
            else:
                liq_df = proj[proj["is_liquidity"]].copy()
                non_liq_df = proj[~proj["is_liquidity"]].copy()

                st.markdown("**Spend split: AM top-up vs general liquidity base**")
                st.caption(
                    "Liquidity campaigns are detected from the AM COMMENTS column. "
                    "The projected spend is split proportionally using the "
                    "discount split (e.g. 30+10 → 75% liquidity / 25% AM)."
                )

                # ── Per-AM breakdown with liquidity split ──
                st.markdown("---")
                st.markdown("**Breakdown per Account Manager**")

                am_liq_data = []
                am_order_liq = (
                    proj.groupby("am_name")["weekly_projected_bolt_eur"]
                    .sum().sort_values(ascending=False).index
                )
                for am in am_order_liq:
                    am_d = proj[proj["am_name"] == am]
                    am_liq_data.append({
                        "Account Manager": am,
                        **{f"W{w} AM": am_d[am_d["week"] == w]["am_spend_eur"].sum() for w in tw},
                        **{f"W{w} Liq": am_d[am_d["week"] == w]["liquidity_spend_eur"].sum() for w in tw},
                        "Total AM Spend": am_d["am_spend_eur"].sum(),
                        "Total Liquidity": am_d["liquidity_spend_eur"].sum(),
                        "Total Combined": am_d["weekly_projected_bolt_eur"].sum(),
                    })
                am_liq_df = pd.DataFrame(am_liq_data).set_index("Account Manager")

                totals_row = am_liq_df.sum().to_frame().T
                totals_row.index = ["GRAND TOTAL"]
                am_liq_display = pd.concat([am_liq_df, totals_row])

                fmt_cols = {c: "€{:,.0f}" for c in am_liq_display.columns}
                st.dataframe(
                    am_liq_display.style.format(fmt_cols),
                    hide_index=False,
                )
                st.download_button("Download AM liquidity breakdown as CSV",
                                   am_liq_display.to_csv(),
                                   "am_liquidity_breakdown.csv", "text/csv",
                                   key="dl_am_liq")

                # ── Per-AM per-Reason with split ──
                st.markdown("---")
                st.markdown("**Breakdown per Person / Week / Reason**")

                sel_am_liq = st.selectbox(
                    "Filter by AM", ["All"] + list(am_order_liq),
                    key="liq_am_filter",
                )
                src_liq = proj if sel_am_liq == "All" else proj[proj["am_name"] == sel_am_liq]

                rows_pwr = []
                for (am, reason), grp in src_liq.groupby(["am_name", "reason_display"]):
                    row_data = {"Account Manager": am, "Reason": reason}
                    for w in tw:
                        wk_grp = grp[grp["week"] == w]
                        row_data[f"W{w} AM"] = wk_grp["am_spend_eur"].sum()
                        row_data[f"W{w} Liq"] = wk_grp["liquidity_spend_eur"].sum()
                    row_data["Total AM"] = grp["am_spend_eur"].sum()
                    row_data["Total Liq"] = grp["liquidity_spend_eur"].sum()
                    row_data["Total"] = grp["weekly_projected_bolt_eur"].sum()
                    rows_pwr.append(row_data)

                pwr_df = pd.DataFrame(rows_pwr).sort_values(
                    ["Account Manager", "Total"], ascending=[True, False]
                )
                st.dataframe(
                    pwr_df.style.format(
                        {c: "€{:,.0f}" for c in pwr_df.columns if c not in ("Account Manager", "Reason")}
                    ),
                    hide_index=True,
                    height=500,
                )
                st.download_button("Download person/week/reason as CSV",
                                   pwr_df.to_csv(index=False),
                                   "person_week_reason.csv", "text/csv",
                                   key="dl_pwr")

                # ── Liquidity campaign detail ──
                st.markdown("---")
                st.markdown("**Liquidity Campaign Detail**")

                liq_ams = sorted(liq_df["am_name"].unique())
                sel_liq_am = st.selectbox(
                    "Filter by AM", ["All"] + liq_ams,
                    key="liq_detail_am",
                )
                liq_show = liq_df if sel_liq_am == "All" else liq_df[liq_df["am_name"] == sel_liq_am]

                liq_detail = liq_show[[
                    "am_name", "week", "provider_id", "provider_name", "city",
                    "campaign_type", "discount_pct", "liquidity_base_pct",
                    "am_topup_pct", "cost_share_pct", "reason_display",
                    "weekly_projected_bolt_eur", "am_spend_eur",
                    "liquidity_spend_eur", "match_method", "am_comment",
                ]].rename(columns={
                    "am_name": "AM", "week": "Week", "provider_id": "Provider ID",
                    "provider_name": "Provider", "city": "City",
                    "campaign_type": "Campaign", "discount_pct": "Total Disc %",
                    "liquidity_base_pct": "Liq Base %", "am_topup_pct": "AM Top-up %",
                    "cost_share_pct": "Provider Share %",
                    "reason_display": "Reason",
                    "weekly_projected_bolt_eur": "Total Projected",
                    "am_spend_eur": "AM Spend (EUR)",
                    "liquidity_spend_eur": "Liquidity (EUR)",
                    "match_method": "Match Method",
                    "am_comment": "AM Comment",
                }).sort_values(["AM", "Week", "Total Projected"], ascending=[True, True, False])

                st.dataframe(
                    liq_detail.style.format({
                        "Total Projected": "€{:,.0f}",
                        "AM Spend (EUR)": "€{:,.0f}",
                        "Liquidity (EUR)": "€{:,.0f}",
                        "Provider ID": "{:.0f}",
                        "Total Disc %": "{:.0f}",
                        "Liq Base %": "{:.0f}",
                        "AM Top-up %": "{:.0f}",
                        "Provider Share %": "{:.0%}",
                    }),
                    hide_index=True,
                    height=500,
                )
                st.caption(f"{len(liq_detail):,} liquidity campaigns")
                st.download_button("Download liquidity detail as CSV",
                                   liq_detail.to_csv(index=False),
                                   "liquidity_detail.csv", "text/csv",
                                   key="dl_liq_detail")

        # ── Week-over-Week Changes ────────────────────────────────────
        with sub_wow:
            if wow is None or (isinstance(wow, pd.DataFrame) and wow.empty):
                st.info("No week-over-week changes detected (same providers across all weeks, or only one target week).")
            else:
                transitions = wow["Transition"].unique().tolist()
                sel_trans = st.selectbox("Transition", transitions)
                tw_data = wow[wow["Transition"] == sel_trans]

                new_df = tw_data[tw_data["Change"] == "New"].sort_values(
                    "Spend Impact (EUR)", ascending=False
                )
                drop_df = tw_data[tw_data["Change"] == "Dropped"].sort_values(
                    "Spend Impact (EUR)", ascending=True
                )

                c1, c2, c3 = st.columns(3)
                c1.metric("New Providers", len(new_df),
                          f"+€{new_df['Spend Impact (EUR)'].sum():,.0f}")
                c2.metric("Dropped Providers", len(drop_df),
                          f"€{drop_df['Spend Impact (EUR)'].sum():,.0f}")
                net = tw_data["Spend Impact (EUR)"].sum()
                c3.metric("Net Spend Impact", f"€{net:,.0f}",
                          f"{'↑' if net > 0 else '↓'} {abs(net):,.0f}")

                show_cols = ["Provider ID", "Provider", "AM", "Campaigns",
                             "Reasons", "Spend Impact (EUR)"]
                fmt_dict = {"Spend Impact (EUR)": "€{:,.0f}", "Provider ID": "{:.0f}"}

                if not new_df.empty:
                    st.markdown("**New Providers**")
                    st.dataframe(new_df[show_cols].style.format(fmt_dict),
                                 hide_index=True)

                if not drop_df.empty:
                    st.markdown("**Dropped Providers**")
                    st.dataframe(drop_df[show_cols].style.format(fmt_dict),
                                 hide_index=True)

                st.download_button("Download all WoW changes as CSV",
                                   wow.to_csv(index=False),
                                   "wow_changes.csv", "text/csv")

        # ── Flagged Campaigns ─────────────────────────────────────────
        with sub_flags:
            st.markdown("Campaigns that deserve a closer look before approving the budget.")

            bolt_share_thresh = st.slider(
                "Flag: Bolt cost share ≥", 50, 100, 75,
                key="bolt_share_thresh", format="%d%%",
            ) / 100

            flags = proj.copy()
            bolt_share = 1.0 - flags["cost_share_pct"]
            flags["bolt_share_pct"] = bolt_share

            am_medians = flags.groupby("am_name")["weekly_projected_bolt_eur"].transform("median")
            spend_p90 = flags["weekly_projected_bolt_eur"].quantile(0.90)

            flag_labels = []
            for _, r in flags.iterrows():
                f = []
                if r["bolt_share_pct"] >= bolt_share_thresh:
                    f.append(f"High Bolt share ({r['bolt_share_pct']:.0%})")
                if r["cost_share_pct"] == 0:
                    f.append("No cost share (100% Bolt)")
                if r["weekly_projected_bolt_eur"] >= spend_p90:
                    f.append("Top 10% spender")
                if am_medians.loc[_] > 0 and r["weekly_projected_bolt_eur"] > 2 * am_medians.loc[_]:
                    f.append("Outlier (>2× AM median)")
                if "fallback" in str(r["match_method"]).lower():
                    f.append("New provider (no history)")
                flag_labels.append(", ".join(f))

            flags["Flags"] = flag_labels
            flagged = flags[flags["Flags"] != ""].copy()

            if flagged.empty:
                st.success("No flagged campaigns.")
            else:
                flag_counts = {}
                for fl in flagged["Flags"]:
                    for f in fl.split(", "):
                        flag_counts[f] = flag_counts.get(f, 0) + 1

                st.markdown(f"**{len(flagged)} campaigns flagged** out of {len(proj)}")
                for label, cnt in sorted(flag_counts.items(), key=lambda x: -x[1]):
                    st.caption(f"  {label}: {cnt}")

                show = flagged[[
                    "am_name", "week", "provider_id", "provider_name",
                    "campaign_type", "discount_pct", "cost_share_pct",
                    "bolt_share_pct", "reason_display",
                    "weekly_projected_bolt_eur", "match_method", "Flags",
                ]].rename(columns={
                    "am_name": "AM", "week": "Week", "provider_id": "Provider ID",
                    "provider_name": "Provider", "campaign_type": "Campaign",
                    "discount_pct": "Discount %",
                    "cost_share_pct": "Provider Share %",
                    "bolt_share_pct": "Bolt Share %",
                    "reason_display": "Objective",
                    "weekly_projected_bolt_eur": "Weekly (EUR)",
                    "match_method": "Match Method",
                }).sort_values("Weekly (EUR)", ascending=False)

                st.dataframe(
                    show.style.format({
                        "Weekly (EUR)": "€{:,.0f}",
                        "Provider ID": "{:.0f}",
                        "Provider Share %": "{:.0%}",
                        "Bolt Share %": "{:.0%}",
                    }),
                    hide_index=True,
                    height=600,
                )
                st.download_button("Download flagged campaigns as CSV",
                                   show.to_csv(index=False),
                                   "flagged_campaigns.csv", "text/csv")

        # ── Campaign Detail ───────────────────────────────────────────
        with sub_detail:
            st.markdown("**All projected campaigns**")

            fc1, fc2, fc3 = st.columns(3)
            with fc1:
                f_am = st.multiselect("Filter AM", sorted(proj["am_name"].unique()))
            with fc2:
                f_reason = st.multiselect("Filter Objective",
                                          sorted(proj["reason_display"].unique()))
            with fc3:
                f_match = st.multiselect("Filter Match Method",
                                         sorted(proj["match_method"].unique()))

            filtered = proj.copy()
            if f_am:
                filtered = filtered[filtered["am_name"].isin(f_am)]
            if f_reason:
                filtered = filtered[filtered["reason_display"].isin(f_reason)]
            if f_match:
                filtered = filtered[filtered["match_method"].isin(f_match)]

            col_map = {
                "am_name": "AM", "week": "Week", "provider_id": "Provider ID",
                "provider_name": "Provider", "city": "City",
                "campaign_type": "Campaign", "discount_pct": "Discount %",
                "cost_share_pct": "Cost Share %", "reason_display": "Objective",
                "match_method": "Match Method",
                "avg_daily_bolt_eur": "Avg Daily (EUR)",
                "weekly_projected_bolt_eur": "Weekly (EUR)",
            }
            if has_liq:
                col_map["am_spend_eur"] = "AM Spend (EUR)"
                col_map["liquidity_spend_eur"] = "Liquidity (EUR)"
                col_map["is_liquidity"] = "Liquidity?"

            available_cols = [c for c in col_map.keys() if c in filtered.columns]
            detail = (
                filtered[available_cols]
                .rename(columns=col_map)
                .sort_values(["AM", "Week", "Weekly (EUR)"],
                             ascending=[True, True, False])
            )
            if "Liquidity?" in detail.columns:
                detail["Liquidity?"] = detail["Liquidity?"].map({True: "Yes", False: ""})

            fmt_detail = {
                "Avg Daily (EUR)": "€{:,.2f}",
                "Weekly (EUR)": "€{:,.2f}",
                "Provider ID": "{:.0f}",
            }
            if has_liq:
                fmt_detail["AM Spend (EUR)"] = "€{:,.0f}"
                fmt_detail["Liquidity (EUR)"] = "€{:,.0f}"

            st.dataframe(
                detail.style.format(fmt_detail),
                hide_index=True,
                height=600,
            )
            st.caption(f"Showing {len(detail):,} of {len(proj):,} campaigns")
            st.download_button("Download filtered detail as CSV",
                               detail.to_csv(index=False),
                               "campaign_detail.csv", "text/csv")

        with st.expander("Execution log"):
            st.code(res["log"], language="text")

# ═══ TAB 3: DATA SOURCES ═════════════════════════════════════════════════════

with tab_data:
    st.subheader("Data Sources")
    st.caption("Historical spend and GMV CSVs are bundled in the repo. "
               "Upload replacements here when you need to refresh them.")

    col_hist, col_gmv = st.columns(2)

    with col_hist:
        st.markdown("**Historical Campaign Spend**")
        hist_path = Path(cc["historical_csv"])
        if hist_path.exists():
            nrows, dmin, dmax = _hist_meta(str(hist_path), hist_path.stat().st_mtime_ns)
            st.caption(f"File: `{hist_path.name}`")
            st.caption(f"Rows: {nrows:,}")
            st.caption(f"Date range: {dmin} to {dmax}")
        else:
            st.warning("File not found — upload one below.")

        st.markdown("**Upload new CSV**")
        hist_upload = st.file_uploader("Historical spend CSV", type=["csv"],
                                       key="hist_upload")
        if hist_upload:
            hist_path.parent.mkdir(parents=True, exist_ok=True)
            hist_path.write_bytes(hist_upload.getvalue())
            _hist_meta.clear()
            st.success(f"Saved to {hist_path.name}")
            st.rerun()

    with col_gmv:
        st.markdown("**Daily GMV**")
        gmv_path = Path(cc["gmv_csv"])
        if gmv_path.exists():
            ndays, gmin, gmax, avg_gmv = _gmv_meta(
                str(gmv_path), gmv_path.stat().st_mtime_ns
            )
            st.caption(f"File: `{gmv_path.name}`")
            st.caption(f"Days: {ndays:,}")
            st.caption(f"Date range: {gmin} to {gmax}")
            st.caption(f"Avg daily GMV: €{avg_gmv:,.0f}")
        else:
            st.warning("File not found — upload one below.")

        st.markdown("**Upload new CSV**")
        gmv_upload = st.file_uploader("Daily GMV CSV", type=["csv"],
                                      key="gmv_upload")
        if gmv_upload:
            gmv_path.parent.mkdir(parents=True, exist_ok=True)
            gmv_path.write_bytes(gmv_upload.getvalue())
            _gmv_meta.clear()
            st.success(f"Saved to {gmv_path.name}")
            st.rerun()
