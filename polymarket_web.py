"""
Streamlit 版 Musk 推文清洗 + 交互式分析工作台。
运行：streamlit run 工作室脚本/项目/Polymarket/polymarket_web.py
"""

import tempfile
import requests
from datetime import datetime, timedelta, date, time
from pathlib import Path
from contextlib import contextmanager
from typing import Dict, Tuple, List
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import streamlit as st
import re
from pathlib import Path
import streamlit.components.v1 as components

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

WEEKDAY_MAP = {
    "周一": "Mon",
    "星期一": "Mon",
    "Mon": "Mon",
    "Monday": "Mon",
    "周二": "Tue",
    "星期二": "Tue",
    "Tue": "Tue",
    "Tuesday": "Tue",
    "周三": "Wed",
    "星期三": "Wed",
    "Wed": "Wed",
    "Wednesday": "Wed",
    "周四": "Thu",
    "星期四": "Thu",
    "Thu": "Thu",
    "Thursday": "Thu",
    "周五": "Fri",
    "星期五": "Fri",
    "Fri": "Fri",
    "Friday": "Fri",
    "周六": "Sat",
    "星期六": "Sat",
    "Sat": "Sat",
    "Saturday": "Sat",
    "周日": "Sun",
    "星期日": "Sun",
    "周天": "Sun",
    "Sun": "Sun",
    "Sunday": "Sun",
}

import polymarket

WEEKDAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
WEEKDAY_CN = {
    "Mon": "周一",
    "Tue": "周二",
    "Wed": "周三",
    "Thu": "周四",
    "Fri": "周五",
    "Sat": "周六",
    "Sun": "周日",
}
ANCHOR_DAYS = [4, 7, 11, 14, 18, 21, 25, 28]


@contextmanager
def glass_block(class_name: str = "glass-card"):
    st.markdown(f"<div class='{class_name}'>", unsafe_allow_html=True)
    yield
    st.markdown("</div>", unsafe_allow_html=True)


def midday_dt(day: date) -> datetime:
    """Convert a date to EST 12:00 PM (naive) datetime."""
    return datetime.combine(day, time()) + timedelta(hours=12)
SECTION_OPTIONS: Dict[str, str] = {
    "overview": "🔎 概览（全部）",
    "daily": "📆 日趋势（自然日）",
    "weekly_cycle_total": "📊 历史 7 日周期总量",
    "weekly_compare_day": "📈 历史 7 日周期对比（日级）",
    "weekly_compare_hour": "🕒 历史 7 日周期对比（小时级）",
    "hourly": "🕒 小时趋势",
    "weekday": "📅 Weekday 分布",
    "heatmap": "🧊 Weekday × Hour",
    "insight": "🤖 行为洞察",
    "detail": "📄 清洗明细",
}


def load_clean_outputs(clean_path: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """读取清洗明细 + 统计结果并补充必要字段。"""
    detail_df = pd.read_csv(clean_path)
    detail_df["Beijing_dt"] = pd.to_datetime(
        detail_df["Beijing_time"].astype(str).str.replace(" CST", "", regex=False), errors="coerce"
    )
    detail_df["EST_dt"] = pd.to_datetime(
        detail_df["EDT_time"]
        .astype(str)
        .str.replace(" EDT", "", regex=False)
        .str.replace(" EST", "", regex=False),
        errors="coerce",
    )

    stats_path = clean_path.with_name(clean_path.name.replace(".csv", "_stats.xlsx"))
    day_df = pd.read_excel(stats_path, sheet_name="day_summary_12PM-12PM_EST")
    day_natural_df = pd.read_excel(stats_path, sheet_name="day_summary_natural_EST")
    hour_df = pd.read_excel(stats_path, sheet_name="hour_summary")

    day_df["date"] = pd.to_datetime(day_df["date"], format="%m/%d/%Y")
    day_natural_df["date"] = pd.to_datetime(day_natural_df["date"], format="%m/%d/%Y")
    if "week_day" not in day_natural_df.columns:
        day_natural_df["week_day"] = day_natural_df["date"].dt.strftime("%a")
    hour_df["date"] = pd.to_datetime(hour_df["date"], format="%m/%d/%Y")
    return detail_df, day_df, day_natural_df, hour_df


def ensure_file(uploaded, fetch_latest=False):
    """处理上传/在线拉取的原始数据并运行清洗；若无输入则使用最新 clean 文件。"""
    temp_path = None
    source_info = None
    if fetch_latest:
        try:
            with st.spinner("正在从 XTracker 拉取最新数据…"):
                resp = requests.post(
                    "https://www.xtracker.io/api/download",
                    json={"handle": "elonmusk", "platform": "X"},
                    timeout=30,
                )
                resp.raise_for_status()
                with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                    tmp.write(resp.content)
                    temp_path = Path(tmp.name)
            source_info = {"mode": "在线读取", "name": "XTracker"}
            st.success("已从 XTracker 拉取最新数据，开始清洗…")
        except Exception as exc:
            st.error(f"在线拉取失败：{exc}")
    elif uploaded:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(uploaded.getbuffer())
            temp_path = Path(tmp.name)
        source_info = {"mode": "本地文件", "name": uploaded.name}
        st.success(f"已上传 {uploaded.name}，开始清洗…")

    if temp_path:
        original_input = polymarket.INPUT_FILE
        try:
            polymarket.INPUT_FILE = str(temp_path)
            polymarket.run_cleaning()
        finally:
            polymarket.INPUT_FILE = original_input

    files = polymarket.list_clean_files()
    if not files:
        st.info("👆 先上传 XTracker 导出的 CSV，完成清洗后即可展示。")
        return None, {"mode": "未加载", "name": "—"}
    latest = Path(files[0])
    st.success(f"当前使用：{latest.name}")
    if source_info is None:
        source_info = {"mode": "本地缓存", "name": latest.name}
    return latest, source_info


def metrics_overview(day_bucket_df: pd.DataFrame, detail_df: pd.DataFrame):
    total = int(day_bucket_df["day_tweet_count"].sum()) if not day_bucket_df.empty else 0
    avg_day = float(day_bucket_df["day_tweet_count"].mean()) if not day_bucket_df.empty else 0.0
    busiest = (
        day_bucket_df.loc[day_bucket_df["day_tweet_count"].idxmax()]
        if not day_bucket_df.empty
        else None
    )
    cols = st.columns(3)
    cols[0].metric("总推文数", f"{total:,}")
    cols[1].metric("日均推文", f"{avg_day:.1f}")
    if busiest is not None:
        cols[2].metric("最高峰", f"{busiest['day_tweet_count']} 条", busiest["date"].strftime("%Y-%m-%d"))
    else:
        cols[2].metric("最高峰", "—")


def render_cst_clock():
    beijing_now = datetime.now(ZoneInfo("Asia/Shanghai"))
    now_est = datetime.now(ZoneInfo("America/New_York"))
    server_str = now_est.strftime("%Y/%m/%d %H:%M:%S")
    weekday = now_est.strftime("%A")
    beijing_str = beijing_now.strftime("%Y/%m/%d %H:%M:%S")
    est_by_delta = beijing_now - timedelta(hours=13)
    est_delta_str = est_by_delta.strftime("%Y/%m/%d %H:%M:%S")
    html_content = f"""
    <style>
      @keyframes pulseClock {{
        0% {{ opacity: 0.3; transform: scale(0.9); }}
        50% {{ opacity: 1; transform: scale(1); }}
        100% {{ opacity: 0.3; transform: scale(0.9); }}
      }}
      .clock-chip {{
        display: inline-flex;
        align-items: center;
        gap: 10px;
        padding: 8px 16px;
        border-radius: 16px;
        background: linear-gradient(120deg, #2563eb, #22d3ee);
        color: #fff;
        font-family: 'SF Pro Display', 'Segoe UI', sans-serif;
        font-size: 17px;
        font-weight: 600;
        box-shadow: 0 8px 20px rgba(37, 99, 235, 0.35);
      }}
      .clock-indicator {{
        width: 10px;
        height: 10px;
        border-radius: 50%;
        background: #fbbf24;
        box-shadow: 0 0 14px rgba(251, 191, 36, 0.85);
        animation: pulseClock 1.2s infinite;
      }}
      .clock-time {{ font-variant-numeric: tabular-nums; letter-spacing: 1px; }}
      .clock-body {{ display:flex; flex-direction:column; gap:2px; font-size:17px; }}
      .clock-rows {{ display:flex; flex-direction:column; gap:0px; }}
    </style>
    <div class='clock-chip'>
      <div class='clock-indicator'></div>
      <div class='clock-body'>
        <div>🇺🇸 美国东部时间（EST）：<span id="cst-clock-text" class='clock-time'>{server_str}</span>
        (<span id="cst-clock-weekday">{weekday}</span>) ｜ 🇨🇳 北京时间：<span id="bj-clock-text">{beijing_str}</span></div>
      </div>
    </div>
    <script>
      const estFormatter = new Intl.DateTimeFormat('zh-CN', {{
        timeZone: 'America/New_York',
        year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit', second: '2-digit',
        hour12: false
      }});
      const weekdayFmt = new Intl.DateTimeFormat('en-US', {{ timeZone: 'America/New_York', weekday: 'long' }});
      const bjFormatter = new Intl.DateTimeFormat('zh-CN', {{
        timeZone: 'Asia/Shanghai',
        year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit', second: '2-digit',
        hour12: false
      }});
      function updateClock() {{
        const now = new Date();
        const estParts = estFormatter.formatToParts(now).reduce((acc, part) => {{
          if (part.type !== 'literal') acc[part.type] = part.value;
          return acc;
        }}, {{}});
        const estFormatted = `${{estParts.year}}/${{estParts.month}}/${{estParts.day}} ${{estParts.hour}}:${{estParts.minute}}:${{estParts.second}}`;
        const clockEl = document.getElementById('cst-clock-text');
        const weekdayEl = document.getElementById('cst-clock-weekday');
        if (clockEl) clockEl.textContent = estFormatted;
        if (weekdayEl) weekdayEl.textContent = weekdayFmt.format(now);

        const bjParts = bjFormatter.formatToParts(now).reduce((acc, part) => {{
          if (part.type !== 'literal') acc[part.type] = part.value;
          return acc;
        }}, {{}});
        const bjFormatted = `${{bjParts.year}}/${{bjParts.month}}/${{bjParts.day}} ${{bjParts.hour}}:${{bjParts.minute}}:${{bjParts.second}}`;
        const bjEl = document.getElementById('bj-clock-text');
        if (bjEl) bjEl.textContent = bjFormatted;
      }}
      updateClock();
      setInterval(updateClock, 1000);
    </script>
    """
    components.html(html_content, height=58)


def render_left_table(df: pd.DataFrame):
    if df.empty:
        st.info("暂无可展示的数据")
        return
    table_html = df.to_html(index=False, classes="hist-table", border=0, justify="left")
    html = f"""
    <style>
      table.hist-table {{ width: 100%; border-collapse: collapse; }}
      table.hist-table th, table.hist-table td {{
        text-align: left !important;
        padding: 6px 10px;
        border-bottom: 1px solid rgba(0,0,0,0.05);
        font-size: 13px;
        white-space: nowrap;
      }}
      .hist-wrapper {{
        max-height: 320px;
        overflow-y: auto;
        border: 1px solid rgba(0,0,0,0.05);
        border-radius: 10px;
        padding: 4px;
        background: rgba(255,255,255,0.6);
      }}
    </style>
    <div class='hist-wrapper'>{table_html}</div>
    """
    height = min(420, 80 + 26 * len(df))
    components.html(html, height=height)


def render_hour_matrix(cycles_meta: List[Dict]):
    if not cycles_meta:
        return
    rows = []
    hour_labels = [f"{h:02d}:00" for h in range(24)]
    for wd in WEEKDAY_ORDER:
        row = {"Weekday": wd}
        for hour in range(24):
            chunks = []
            for meta in cycles_meta:
                count = meta["data"].get((hour, wd), 0)
                if count > 0:
                    chunks.append(
                        f"<div class='cell-line'><span class='legend-dot' style='background:{meta['color']}'></span>{count}</div>"
                    )
            row[f"{hour:02d}:00"] = "".join(chunks)
        rows.append(row)
    df = pd.DataFrame(rows)

    legend_html = "".join(
        f"<span class='legend-item'><span class='legend-dot' style='background:{meta['color']}'></span>{meta['label']}</span>"
        for meta in cycles_meta
        if meta["data"]
    )

    table_html = df.to_html(index=False, escape=False, classes="hist-table matrix-table", justify="left", border=0)
    html = f"""
    <style>
      .matrix-container {{
        width: 100%;
        max-height: 480px;
        overflow: auto;
        border: 1px solid rgba(0,0,0,0.08);
        border-radius: 14px;
        padding: 6px;
        background: rgba(255,255,255,0.75);
      }}
      table.matrix-table {{ min-width: 1500px; border-collapse: collapse; }}
      table.matrix-table th, table.matrix-table td {{
        min-width: 110px;
        text-align: left !important;
        vertical-align: top;
        padding: 10px;
      }}
      table.matrix-table td {{ min-height: 56px; }}
      .cell-line {{ display: flex; align-items: center; gap: 6px; font-size: 13px; margin-bottom: 4px; }}
      .legend-dot {{ width: 10px; height: 10px; border-radius: 50%; display:inline-block; }}
      .matrix-legend {{ margin-bottom: 6px; font-size: 13px; display:flex; flex-wrap:wrap; gap:12px; }}
      .legend-item {{ display:flex; align-items:center; gap:6px; }}
    </style>
    <div class='matrix-legend'>{legend_html}</div>
    <div class='matrix-container'>{table_html}</div>
    """
    components.html(html, height=520)


def render_historical_today_table(
    current_df: pd.DataFrame,
    history_df: pd.DataFrame,
    history_cycles: int,
    hour_scope_df: pd.DataFrame,
    focus_date,
):
    if current_df is None or current_df.empty:
        st.info("暂无当前周期数据")
        return
    if isinstance(focus_date, datetime):
        focus_date = focus_date.date()
    current = current_df.copy()
    current["date"] = pd.to_datetime(current["date"]).dt.date
    available_dates = sorted(current["date"].unique())
    if available_dates:
        focus_date = max(d for d in available_dates if d <= focus_date) if any(
            d <= focus_date for d in available_dates
        ) else available_dates[-1]
    history = history_df.copy() if history_df is not None else pd.DataFrame(columns=["date", "day_tweet_count"])
    if not history.empty:
        history["date"] = pd.to_datetime(history["date"]).dt.date

    weekday = current.loc[current["date"] == focus_date, "week_day"]
    weekday = weekday.iloc[0] if not weekday.empty else focus_date.strftime("%a")
    weekday_cn = WEEKDAY_CN.get(weekday, weekday)
    st.markdown(f"#### 📜 历史上的今天（0:00-24:00 EST，{weekday_cn}）")

    rows: List[Dict[str, str]] = []
    current_count = int(current.loc[current["date"] == focus_date, "day_tweet_count"].sum())
    rows.append({"周期": f"本周期（{focus_date.strftime('%m/%d')}）", "推文数": current_count})

    for idx in range(1, history_cycles + 1):
        target_date = focus_date - timedelta(days=7 * idx)
        count = int(history.loc[history["date"] == target_date, "day_tweet_count"].sum())
        rows.append({"周期": f"历史周期 {idx}（{target_date.strftime('%m/%d')}）", "推文数": count})

    render_left_table(pd.DataFrame(rows))

    if hour_scope_df is None or hour_scope_df.empty:
        return
    hour_scope = hour_scope_df.copy()
    hour_scope["date"] = pd.to_datetime(hour_scope["date"]).dt.date

    def build_hour_map(target_date):
        subset = hour_scope.loc[hour_scope["date"] == target_date]
        if subset.empty:
            return {}
        temp = subset.copy()
        temp_dates = pd.to_datetime(temp["date"])
        temp["week_day"] = temp_dates.dt.strftime("%a")
        temp["hour_us"] = temp["hour_us"].astype(int)
        grouped = temp.groupby(["hour_us", "week_day"])["hour_tweet_count"].sum()
        return grouped.to_dict()

    colors = ["#ef4444", "#0ea5e9", "#f97316", "#a855f7", "#10b981", "#facc15", "#ec4899"]
    cycles_meta = []
    cycles_meta.append(
        {
            "label": f"本周期（{focus_date.strftime('%m/%d')}）",
            "color": colors[0],
            "data": build_hour_map(focus_date),
        }
    )
    for idx in range(1, history_cycles + 1):
        target_date = focus_date - timedelta(days=7 * idx)
        cycles_meta.append(
            {
                "label": f"历史周期 {idx}（{target_date.strftime('%m/%d')}）",
                "color": colors[idx % len(colors)],
                "data": build_hour_map(target_date),
            }
        )

    if any(meta["data"] for meta in cycles_meta):
        st.markdown("##### ⏱ 按小时分布（周×小时矩阵）")
        render_hour_matrix(cycles_meta)


def render_day_section(
    day_df: pd.DataFrame,
    show_values: bool,
    title_suffix: str = "自然日EST 0:00-23:59",
    history_df: pd.DataFrame | None = None,
    history_cycles: int = 0,
    base_range: Tuple[datetime.date, datetime.date] | None = None,
):
    st.subheader(f"📆 日趋势（{title_suffix}）")
    if day_df.empty:
        st.info("当前筛选区间内没有数据")
        return
    ordered_labels = day_df.sort_values("date")["date"].dt.strftime("%m/%d (%a)").tolist()
    day_df = day_df.assign(label=day_df["date"].dt.strftime("%m/%d (%a)"))
    hover = alt.selection_point(fields=["label"], nearest=True, on="mouseover", empty="none")
    base_line = (
        alt.Chart(day_df)
        .mark_line(interpolate="monotone")
        .encode(
            x=alt.X(
                "label:N",
                title="",
                sort=ordered_labels,
                axis=alt.Axis(labelAngle=0),
            ),
            y=alt.Y("day_tweet_count:Q", title="Tweets per Day"),
        )
    )
    base_points = (
        alt.Chart(day_df)
        .mark_point(size=70)
        .encode(
            x="label:N",
            y="day_tweet_count:Q",
            tooltip=["label:N", "day_tweet_count:Q"],
        )
        .add_params(hover)
    )
    rule = (
        alt.Chart(day_df)
        .mark_rule(color="#888", strokeDash=[4, 4])
        .encode(x="label:N")
        .transform_filter(hover)
    )
    base = (base_line + base_points + rule)

    if (
        history_df is not None
        and base_range is not None
        and history_cycles > 0
        and not history_df.empty
        and ordered_labels
    ):
        hist_copy = history_df.copy()
        hist_copy["date"] = pd.to_datetime(hist_copy["date"]).dt.date
        history_lookup = hist_copy.set_index("date")["day_tweet_count"].to_dict()
        base_start = base_range[0]
        base_start = base_start.date() if isinstance(base_start, datetime) else base_start
        days_in_cycle = min(len(ordered_labels), 7)
        overlay_rows: List[Dict] = []
        for idx in range(1, history_cycles + 1):
            cycle_start = base_start - timedelta(days=7 * idx)
            cycle_end = cycle_start + timedelta(days=days_in_cycle - 1)
            cycle_label = f"{cycle_start.strftime('%m/%d')}–{cycle_end.strftime('%m/%d')}"
            for offset in range(days_in_cycle):
                label = ordered_labels[offset]
                target_date = cycle_start + timedelta(days=offset)
                count = int(history_lookup.get(target_date, 0))
                overlay_rows.append(
                    {
                        "label": label,
                        "count": count,
                        "cycle": cycle_label,
                        "actual_date": target_date.strftime("%Y-%m-%d"),
                    }
                )
        history_overlay_df = pd.DataFrame(overlay_rows)
        if not history_overlay_df.empty:
            history_overlay_df["label"] = pd.Categorical(
                history_overlay_df["label"], categories=ordered_labels, ordered=True
            )
            history_line = (
                alt.Chart(history_overlay_df)
                .mark_line(strokeDash=[4, 3], opacity=0.65)
                .encode(
                    x=alt.X("label:N", sort=ordered_labels, title="", axis=alt.Axis(labelAngle=0)),
                    y=alt.Y("count:Q", title="Tweets per Day"),
                    color=alt.Color(
                        "cycle:N",
                        title="历史周期",
                        legend=alt.Legend(orient="right"),
                        scale=alt.Scale(scheme="tableau10"),
                    ),
                )
            )
            history_points = (
                alt.Chart(history_overlay_df)
                .mark_point(size=50, opacity=0.7)
                .encode(
                    x=alt.X("label:N", sort=ordered_labels),
                    y="count:Q",
                    color=alt.Color("cycle:N", legend=None, scale=alt.Scale(scheme="tableau10")),
                    tooltip=["cycle:N", "label:N", "actual_date:N", "count:Q"],
                )
            )
            base = base + history_line + history_points

    base = base.properties(height=320).interactive()
    if show_values:
        text = (
            alt.Chart(day_df)
            .mark_text(dy=-10, fontSize=11)
            .encode(
                x=alt.X("label:N", sort=ordered_labels, axis=alt.Axis(labelAngle=0)),
                y="day_tweet_count:Q",
                text="day_tweet_count:Q",
            )
        )
        chart = base + text
    else:
        chart = base
    st.altair_chart(chart, width="stretch")

    summary_html = build_daytrend_ai_summary(day_df, history_df, history_cycles, base_range)
    if summary_html:
        st.markdown(summary_html, unsafe_allow_html=True)


def render_hour_section(hour_df: pd.DataFrame, show_values: bool):
    st.subheader("🕒 小时级趋势（美东小时）")
    if hour_df.empty:
        st.info("当前筛选区间内没有数据")
        return
    hover = alt.selection_point(fields=["date"], nearest=True, on="mouseover", empty="none")
    base_line = (
        alt.Chart(hour_df)
        .mark_line(interpolate="monotone")
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("hour_tweet_count:Q", title="Tweets per Hour"),
            color=alt.Color("hour_us:N", title="Hour (US)"),
        )
    )
    base_points = (
        alt.Chart(hour_df)
        .mark_point(size=50)
        .encode(
            x="date:T",
            y="hour_tweet_count:Q",
            color=alt.Color("hour_us:N", legend=None),
            tooltip=["date:T", "week_day:N", "hour_us:N", "hour_cn:N", "hour_tweet_count:Q"],
        )
        .add_params(hover)
    )
    rule = (
        alt.Chart(hour_df)
        .mark_rule(color="#888", strokeDash=[4, 4])
        .encode(x="date:T")
        .transform_filter(hover)
    )
    base = (base_line + base_points + rule).properties(height=320).interactive()
    if show_values:
        text = (
            alt.Chart(hour_df)
            .mark_text(fontSize=10, dy=-8)
            .encode(
                x="date:T",
                y="hour_tweet_count:Q",
                color=alt.Color("hour_us:N", legend=None),
                text="hour_tweet_count:Q",
            )
        )
        chart = base + text
    else:
        chart = base
    st.altair_chart(chart, width="stretch")


def render_weekday_section(day_df: pd.DataFrame, show_values: bool):
    st.subheader("📅 Weekday 分布（按日汇总）")
    if day_df.empty:
        st.info("当前筛选区间内没有数据")
        return
    weekday_stats = (
        day_df.groupby("week_day")["day_tweet_count"].sum().reindex(WEEKDAY_ORDER).reset_index().dropna()
    )
    base = (
        alt.Chart(weekday_stats)
        .mark_bar()
        .encode(x=alt.X("week_day:N", title="Weekday"), y=alt.Y("day_tweet_count:Q", title="Total Tweets"))
    )
    if show_values:
        labels = (
            alt.Chart(weekday_stats)
            .mark_text(dy=-8, fontSize=12)
            .encode(x="week_day:N", y="day_tweet_count:Q", text="day_tweet_count:Q")
        )
        chart = base + labels
    else:
        chart = base
    st.altair_chart(chart, width="stretch")


def render_heatmap(hour_df: pd.DataFrame, show_values: bool):
    st.subheader("🧊 Weekday × Hour 热力")
    if hour_df.empty:
        st.info("当前筛选区间内没有数据")
        return
    hours = sorted(hour_df["hour_us"].unique())
    if not hours:
        st.info("无有效小时数据")
        return
    idx = pd.MultiIndex.from_product([WEEKDAY_ORDER, hours], names=["week_day", "hour_us"])
    grid = (
        hour_df.groupby(["week_day", "hour_us"])["hour_tweet_count"]
        .sum()
        .reindex(idx, fill_value=0)
        .reset_index()
    )
    grid = grid[grid["hour_us"].notna()]
    grid["week_day"] = pd.Categorical(grid["week_day"], categories=WEEKDAY_ORDER, ordered=True)
    heat = (
        alt.Chart(grid)
        .mark_rect()
        .encode(
            x=alt.X("hour_us:O", title="Hour (US)"),
            y=alt.Y("week_day:O", title="Weekday", sort=WEEKDAY_ORDER),
            color=alt.Color("hour_tweet_count:Q", title="Tweets"),
            tooltip=["week_day", "hour_us", "hour_tweet_count"],
        )
    )
    if show_values:
        text = (
            alt.Chart(grid)
            .mark_text(fontSize=10)
            .encode(x="hour_us:O", y="week_day:O", text="hour_tweet_count:Q")
        )
        heat = heat + text
    st.altair_chart(heat, width="stretch")


def behavior_insights(day_df: pd.DataFrame, hour_df: pd.DataFrame):
    st.subheader("🤖 行为洞察（基于当前筛选）")
    if day_df.empty:
        st.info("暂无数据可分析")
        return
    top_weekday = day_df.groupby("week_day")["day_tweet_count"].mean().idxmax()
    low_weekday = day_df.groupby("week_day")["day_tweet_count"].mean().idxmin()
    max_day = day_df.loc[day_df["day_tweet_count"].idxmax()]
    min_day = day_df.loc[day_df["day_tweet_count"].idxmin()]

    top_hour = low_hour = None
    if not hour_df.empty:
        agg = hour_df.groupby("hour_us")["hour_tweet_count"].sum()
        top_hour = agg.idxmax()
        low_hour = agg.idxmin()

    dom_stats = (
        day_df.assign(day_of_month=day_df["date"].dt.day)
        .groupby("day_of_month")["day_tweet_count"]
        .mean()
    )
    dom_high = dom_stats.idxmax() if not dom_stats.empty else None
    dom_low = dom_stats.idxmin() if not dom_stats.empty else None

    st.markdown(
        f"""
        - **高频周几**：{top_weekday}，**低频周几**：{low_weekday}
        - **高峰小时（美东）**：{top_hour if top_hour is not None else '—'}，低谷小时：{low_hour if low_hour is not None else '—'}
        - **最繁忙日期**：{max_day['date'].strftime('%Y-%m-%d')}（{int(max_day['day_tweet_count'])} 条）
        - **最清淡日期**：{min_day['date'].strftime('%Y-%m-%d')}（{int(min_day['day_tweet_count'])} 条）
        - **按月日（Day-of-Month）平均**：高峰在 {dom_high if dom_high is not None else '—'} 日，低谷在 {dom_low if dom_low is not None else '—'} 日
        """
    )


def render_weekly_compare(
    day_natural_df: pd.DataFrame,
    base_start_date,
    show_values: bool,
    cycles: int = 3,
    weekday_order: List[str] = WEEKDAY_ORDER,
):
    st.subheader("📈 历史 7 日周期对比（日级）")
    if day_natural_df.empty:
        st.info("暂无历史数据用于对比")
        return
    if isinstance(base_start_date, tuple):
        base_date = base_start_date[0]
    else:
        base_date = base_start_date
    base_date = base_date.date() if isinstance(base_date, datetime) else base_date
    lookup = (
        day_natural_df.groupby(day_natural_df["date"].dt.date)["day_tweet_count"].sum()
    )
    data: List[Dict] = []
    for idx in range(1, cycles + 1):
        cycle_start = base_date - timedelta(days=7 * idx)
        cycle_end = cycle_start + timedelta(days=7)
        cycle_label = f"{cycle_start.strftime('%m/%d')}–{cycle_end.strftime('%m/%d')}"
        for offset in range(7):
            current = cycle_start + timedelta(days=offset)
            weekday_label = current.strftime("%a")
            count = int(lookup.get(current, 0))
            data.append(
                {
                    "weekday": weekday_label,
                    "weekday_order": weekday_order.index(weekday_label),
                    "count": count,
                    "cycle": cycle_label,
                }
            )
    if not data:
        st.info("历史周期数据不足")
        return
    plot_df = pd.DataFrame(data)
    plot_df["weekday"] = pd.Categorical(plot_df["weekday"], categories=weekday_order, ordered=True)
    hover = alt.selection_point(fields=["weekday"], nearest=True, on="mouseover", empty="none")
    line = (
        alt.Chart(plot_df)
        .mark_line(point=True, interpolate="monotone")
        .encode(
            x=alt.X("weekday:O", sort=alt.Sort(weekday_order), title="", axis=alt.Axis(labelAngle=0)),
            y=alt.Y("count:Q", title="Posts per Day"),
            color=alt.Color("cycle:N", title="历史周期", legend=alt.Legend(orient="right")),
        )
    )
    points = (
        alt.Chart(plot_df)
        .mark_point(size=60)
        .encode(
            x=alt.X("weekday:O", sort=alt.Sort(weekday_order)),
            y="count:Q",
            color=alt.Color("cycle:N", legend=alt.Legend(title="历史周期", orient="right")),
            tooltip=["cycle:N", "weekday:N", "count:Q"],
        )
        .add_params(hover)
    )
    rule = (
        alt.Chart(plot_df)
        .mark_rule(color="#888", strokeDash=[4, 4])
        .encode(x=alt.X("weekday:O", sort=alt.Sort(weekday_order)))
        .transform_filter(hover)
    )
    chart = (line + points + rule)
    summary = plot_df.groupby("weekday")["count"].mean()
    dispersion = plot_df.groupby("weekday")["count"].std().fillna(0)
    top_day = summary.idxmax()
    low_day = summary.idxmin()
    stable_day = dispersion.idxmin()

    if show_values:
        text = (
            alt.Chart(plot_df)
            .mark_text(fontSize=11, dy=-8)
            .encode(x="weekday:O", y="count:Q", color=alt.Color("cycle:N", legend=None), text="count:Q")
        )
        chart = chart + text
    st.altair_chart(chart, width="stretch")

    narrative = (
        f"<div style='font-family:SF Pro Display,Helvetica,sans-serif;background:rgba(255,255,255,0.18);"
        f"padding:12px 16px;border-radius:14px;margin-top:12px;'>"
        f"<b>AI 观察：</b>在当前 {cycles} 个历史周期中，"
        f"<b>{top_day}</b> 平均最活跃（≈{summary[top_day]:.1f} 条/日），"
        f"<b>{low_day}</b> 最清淡（≈{summary[low_day]:.1f} 条/日）。"
        f"波动最小的是 <b>{stable_day}</b>（σ≈{dispersion[stable_day]:.1f}），代表稳定基线。"
        f"整体日均分布的离差约 ±{(summary.max() - summary.mean()):.1f} 条，可据此判断高低峰时间段。"
        "</div>"
    )
    st.markdown(narrative, unsafe_allow_html=True)


def render_cycle_totals(
    bucket_df: pd.DataFrame,
    base_range,
    cycles: int,
    show_values: bool,
):
    st.subheader("📊 历史 7 日周期总量（12PM→12PM）")
    if bucket_df.empty:
        st.info("暂无符合筛选条件的 12PM 周期数据")
        return

    base_end = base_range[1]
    base_end = base_end.date() if isinstance(base_end, datetime) else base_end
    bucket_df = bucket_df.copy()
    bucket_df["date"] = pd.to_datetime(bucket_df["date"]).dt.date

    records = []
    for idx in range(1, cycles + 1):
        cycle_start = base_end - timedelta(days=7 * idx)
        cycle_end = cycle_start + timedelta(days=7)
        mask = (bucket_df["date"] >= cycle_start) & (bucket_df["date"] < cycle_end)
        total = int(bucket_df.loc[mask, "day_tweet_count"].sum())
        label = f"{cycle_start.strftime('%m/%d')} 12PM–{cycle_end.strftime('%m/%d')} 12PM"
        records.append({"cycle": label, "order": idx, "total": total})

    if not records:
        st.info("历史周期数据不足")
        return

    plot_df = pd.DataFrame(records)
    plot_df = plot_df.sort_values("order", ascending=False)
    hover = alt.selection_point(fields=["cycle"], nearest=True, on="mouseover", empty="none")
    line = (
        alt.Chart(plot_df)
        .mark_line(point=True, interpolate="monotone")
        .encode(
            x=alt.X("cycle:N", sort=list(plot_df["cycle"]), title="", axis=alt.Axis(labelAngle=0)),
            y=alt.Y("total:Q", title="过去 7 天总发推"),
        )
    )
    points = (
        alt.Chart(plot_df)
        .mark_point(size=70)
        .encode(
            x=alt.X("cycle:N", sort=list(plot_df["cycle"])),
            y="total:Q",
            color=alt.Color("cycle:N", title="时间区间", legend=alt.Legend(orient="right")),
            tooltip=["cycle:N", "total:Q"],
        )
        .add_params(hover)
    )
    rule = (
        alt.Chart(plot_df)
        .mark_rule(color="#888", strokeDash=[4, 4])
        .encode(x="cycle:N")
        .transform_filter(hover)
    )
    chart = line + points + rule
    if show_values:
        text = (
            alt.Chart(plot_df)
            .mark_text(fontSize=11, dy=-8)
            .encode(x="cycle:N", y="total:Q", text="total:Q")
        )
        chart = chart + text
    st.altair_chart(chart, use_container_width=True)

    if not plot_df.empty:
        max_row = plot_df.loc[plot_df["total"].idxmax()]
        min_row = plot_df.loc[plot_df["total"].idxmin()]
        trend = "上升" if plot_df.iloc[0]["total"] >= plot_df.iloc[-1]["total"] else "回落"
        st.markdown(
            f"<div style='font-family:SF Pro Display,Helvetica,sans-serif;background:rgba(255,255,255,0.18);"
            f"padding:12px 16px;border-radius:14px;margin-top:12px;'>"
            f"<b>AI 观察：</b>最高周期出现在 <b>{max_row['cycle']}</b>（{max_row['total']} 条），"
            f"最低周期为 <b>{min_row['cycle']}</b>（{min_row['total']} 条），"
            f"整体走势呈现 <b>{trend}</b>，可作为盘口高低位参考。"
            "</div>",
            unsafe_allow_html=True,
        )


def summarize_weekday_profile(frame: pd.DataFrame | None):
    if frame is None or frame.empty:
        return None
    temp = frame.copy()
    if "week_day" not in temp.columns and "date" in temp.columns:
        temp["week_day"] = pd.to_datetime(temp["date"]).dt.strftime("%a")
    avg = temp["day_tweet_count"].mean()
    weekday_means = (
        temp.groupby("week_day")["day_tweet_count"].mean().reindex(WEEKDAY_ORDER)
    )
    parts = []
    for wd in WEEKDAY_ORDER:
        val = weekday_means.get(wd)
        if pd.isna(val):
            continue
        parts.append(f"{WEEKDAY_CN.get(wd, wd)} {val:.1f}")
    breakdown = "｜".join(parts)
    return avg, breakdown


def build_daytrend_ai_summary(
    current_df: pd.DataFrame,
    history_df: pd.DataFrame | None,
    history_cycles: int,
    base_range,
):
    sections = []
    all_frames: List[pd.DataFrame] = []
    current_label = ""
    if base_range is not None:
        start_val, end_val = base_range
        if not isinstance(start_val, datetime):
            start_val = datetime.combine(start_val, datetime.min.time())
        if not isinstance(end_val, datetime):
            end_val = datetime.combine(end_val, datetime.min.time())
        current_label = f"（{start_val.strftime('%m/%d')}–{end_val.strftime('%m/%d')}）"

    if current_df is not None and not current_df.empty:
        all_frames.append(current_df)
        current_stats = summarize_weekday_profile(current_df)
        if current_stats:
            avg, breakdown = current_stats
            sections.append(f"<b>本周期{current_label}</b>：日均 {avg:.1f} 条；{breakdown}")

    if (
        history_df is not None
        and not history_df.empty
        and base_range is not None
        and history_cycles > 0
    ):
        hist = history_df.copy()
        hist["date"] = pd.to_datetime(hist["date"])
        base_start_raw = base_range[0]
        if isinstance(base_start_raw, datetime):
            base_start_date = base_start_raw.date()
        else:
            base_start_date = base_start_raw
        history_frames = []
        for idx in range(1, history_cycles + 1):
            cycle_start = base_start_date - timedelta(days=7 * idx)
            cycle_end_inclusive = base_start_date - timedelta(days=7 * (idx - 1))
            mask = (
                (hist["date"].dt.date >= cycle_start)
                & (hist["date"].dt.date <= cycle_end_inclusive)
            )
            subset = hist.loc[mask]
            stats = summarize_weekday_profile(subset)
            if not stats:
                continue
            avg, breakdown = stats
            label = (
                f"历史周期 {idx}（{cycle_start.strftime('%m/%d')}–"
                f"{cycle_end_inclusive.strftime('%m/%d')}）"
            )
            sections.append(f"<b>{label}</b>：日均 {avg:.1f} 条；{breakdown}")
            history_frames.append(subset)

        if history_frames:
            all_frames.extend(history_frames)

    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        combined_stats = summarize_weekday_profile(combined)
        if combined_stats:
            avg, breakdown = combined_stats
            sections.append(f"<b>全部周期</b>：日均 {avg:.1f} 条；{breakdown}")

    if not sections:
        return ""
    return (
        "<div style='font-family:SF Pro Display,Helvetica,sans-serif;background:rgba(255,255,255,0.18);"
        "padding:12px 16px;border-radius:14px;margin-top:8px;'>"
        + "<b>AI 观察：</b><br>"
        + "<br>".join(sections)
        + "</div>"
    )


def build_cycle_shortcuts(min_day: date, max_day: date, today: date):
    today = min(today, max_day)
    shortcuts = []
    month_cursor = date(min_day.year, min_day.month, 1)
    while month_cursor <= max_day:
        for anchor in ANCHOR_DAYS:
            try:
                start = date(month_cursor.year, month_cursor.month, anchor)
            except ValueError:
                continue
            if start < min_day or start > max_day:
                continue
            display_end = start + timedelta(days=7)
            actual_end = min(display_end, max_day)
            icon = "⭕️" if start <= today else "⚪️"
            label = f"{icon} {start:%m/%d} → {display_end:%m/%d}"
            shortcuts.append({"label": label, "start": start, "end": actual_end, "display_end": display_end})
        month_cursor = (month_cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
    shortcuts.sort(key=lambda x: x["start"], reverse=True)
    return shortcuts


def render_cycle_forecast(
    day_bucket_full: pd.DataFrame,
    day_bucket_current: pd.DataFrame,
    history_bucket_scope: pd.DataFrame,
    history_cycles: int,
    cycle_start: date,
    cycle_actual_end: date,
    cycle_display_end: date,
):
    st.subheader("🔮 周期预测")
    total_days = 7
    est_now = datetime.now(ZoneInfo("America/New_York")).replace(tzinfo=None)
    cycle_start_dt = midday_dt(cycle_start)
    full_end_dt = midday_dt(cycle_display_end)
    clamped_now = max(min(est_now, full_end_dt), cycle_start_dt)
    elapsed = clamped_now - cycle_start_dt
    remaining_time = max(full_end_dt - clamped_now, timedelta(0))
    elapsed_days_float = elapsed.total_seconds() / 86400
    remaining_days_float = remaining_time.total_seconds() / 86400
    progress_pct = min(100.0, (elapsed_days_float / total_days) * 100)
    elapsed_days_int = int(elapsed.total_seconds() // 86400)
    elapsed_hours_int = int((elapsed.total_seconds() % 86400) // 3600)
    remaining_days_int = int(remaining_time.total_seconds() // 86400)
    remaining_hours_int = int((remaining_time.total_seconds() % 86400) // 3600)
    st.caption(
        f"当前周期：{cycle_start:%m/%d} 12:00 → {cycle_display_end:%m/%d} 12:00（EST）｜ "
        f"已过去 {elapsed_days_int} 天 {elapsed_hours_int} 小时 ({progress_pct:.1f}%)，剩余约 "
        f"{remaining_days_int} 天 {remaining_hours_int} 小时"
    )

    day_bucket_current = day_bucket_current.copy()
    day_bucket_current["date"] = pd.to_datetime(day_bucket_current["date"]).dt.date
    actual_dates = sorted(day_bucket_current["date"].unique().tolist())
    actual_total = day_bucket_current["day_tweet_count"].sum()
    completed_days = len(actual_dates)
    candidate_dates = [cycle_start + timedelta(days=i) for i in range(total_days + 1)]
    remaining_dates = [
        d
        for d in candidate_dates
        if d not in actual_dates and d < cycle_display_end
    ]
    remaining_count = len(remaining_dates)
    remaining_offsets = [(d - cycle_start).days for d in remaining_dates]
    if completed_days > 0:
        avg_day = day_bucket_current["day_tweet_count"].mean()
        max_day = day_bucket_current["day_tweet_count"].max()
        min_day_val = day_bucket_current["day_tweet_count"].min()
    else:
        avg_day = max_day = min_day_val = 0
    forecast_avg = actual_total + avg_day * remaining_days_float
    forecast_max = actual_total + max_day * remaining_days_float
    forecast_min = actual_total + min_day_val * remaining_days_float

    def badge(text, color):
        return f"<span style='color:{color};font-weight:600;'>{text}</span>"

    avg_formula = f"{actual_total:.0f} + {avg_day:.1f} × {remaining_days_float:.1f} = {forecast_avg:.1f}"
    max_formula = f"{actual_total:.0f} + {max_day:.1f} × {remaining_days_float:.1f} = {forecast_max:.1f}"
    min_formula = f"{actual_total:.0f} + {min_day_val:.1f} × {remaining_days_float:.1f} = {forecast_min:.1f}"

    summary_html = f"""
    <div style='padding:10px 14px;border-radius:14px;background:rgba(255,255,255,0.15);'>
    <div style='font-weight:600;margin-bottom:6px;'>基于当前周期均值 / 峰值 / 谷值</div>
    <ul style='padding-left:18px;margin:0;'>
      <li>已统计 {badge(completed_days, '#0ea5e9')} 天，累计 {badge(f"{actual_total:.0f}", '#1d4ed8')} 条；剩余 {badge(f"{remaining_days_float:.1f}", '#0ea5e9')} 天。</li>
      <li>均值 {badge(f"{avg_day:.1f}", '#22c55e')} 条/日 ⇒ {badge(avg_formula, '#22c55e')}。</li>
      <li>峰值 {badge(f"{max_day:.1f}", '#f97316')} 条/日 ⇒ {badge(max_formula, '#f97316')}。</li>
      <li>谷值 {badge(f"{min_day_val:.1f}", '#ec4899')} 条/日 ⇒ {badge(min_formula, '#ec4899')}。</li>
    </ul>
    </div>
    """
    st.markdown(summary_html, unsafe_allow_html=True)

    if remaining_dates:
        desc_items = []
        for d in remaining_dates:
            wd = d.strftime("%a")
            wd_cn = WEEKDAY_CN.get(wd, wd)
            desc_items.append(f"{wd_cn}（{d:%m/%d}）")
        end_label = f"{WEEKDAY_CN.get(cycle_display_end.strftime('%a'), cycle_display_end.strftime('%a'))}（{cycle_display_end:%m/%d}） · 截至 12:00 PM"
        st.markdown(
            f"<div style='padding:10px 14px;border-radius:12px;background:rgba(255,255,255,0.12);margin-top:10px;'>"
            f"距离 {cycle_display_end:%m/%d} 12:00 PM 结束还有 <b>{remaining_count}</b> 天："
            f"{'，'.join(desc_items)}；终点 {end_label}。</div>",
            unsafe_allow_html=True,
        )

    # 历史周期维度
    if history_cycles > 0:
        hist_cycles: List[Dict] = []
        history_bucket_scope = history_bucket_scope.copy()
        history_bucket_scope["date"] = pd.to_datetime(history_bucket_scope["date"]).dt.date
        for idx in range(1, history_cycles + 1):
            hist_start = cycle_start - timedelta(days=7 * idx)
            hist_end = hist_start + timedelta(days=7)
            hist_df = history_bucket_scope[
                (history_bucket_scope["date"] >= hist_start) & (history_bucket_scope["date"] < hist_end)
            ].copy()
            if hist_df.empty:
                continue
            label = f"历史周期 {idx}"
            hist_df["cycle"] = label
            hist_cycles.append({"label": label, "start": hist_start, "df": hist_df})
        if hist_cycles:
            hist_all = pd.concat([c["df"] for c in hist_cycles], ignore_index=True)
            remaining_weekdays = [
                {
                    "weekday": d.strftime("%a"),
                    "weekday_cn": WEEKDAY_CN.get(d.strftime("%a"), d.strftime("%a")),
                    "label": d.strftime("%m/%d"),
                }
                for d in remaining_dates
            ]
            if remaining_weekdays:
                rows = []
                add_avg = add_max = add_min = 0.0
                for meta in remaining_weekdays:
                    wd_values = hist_all.loc[hist_all["week_day"] == meta["weekday"], "day_tweet_count"]
                    if wd_values.empty:
                        continue
                    avg = wd_values.mean()
                    mx = wd_values.max()
                    mn = wd_values.min()
                    add_avg += avg
                    add_max += mx
                    add_min += mn
                    rows.append(
                        f"{meta['weekday_cn']}（{meta['label']}）：均值 {badge(f'{avg:.1f}', '#22c55e')} ｜峰 "
                        f"{badge(f'{mx:.1f}', '#f97316')} ｜谷 {badge(f'{mn:.1f}', '#ec4899')}"
                    )
                if rows:
                    hist_avg_formula = f"{actual_total:.0f} + {add_avg:.1f} = {actual_total + add_avg:.1f}"
                    hist_max_formula = f"{actual_total:.0f} + {add_max:.1f} = {actual_total + add_max:.1f}"
                    hist_min_formula = f"{actual_total:.0f} + {add_min:.1f} = {actual_total + add_min:.1f}"
                    st.markdown(
                        "<div style='padding:10px 14px;border-radius:14px;background:rgba(255,255,255,0.15);margin-top:12px;'>"
                        "<div style='font-weight:600;margin-bottom:6px;'>历史周期参考（按剩余周几）</div>"
                        + "<br>".join(rows)
                        + "<ul style='padding-left:18px;margin-top:10px;'>"
                        + f"<li>历史均值：{badge(hist_avg_formula, '#22c55e')}。</li>"
                        + f"<li>历史峰值：{badge(hist_max_formula, '#f97316')}。</li>"
                        + f"<li>历史谷值：{badge(hist_min_formula, '#ec4899')}。</li>"
                        + "</ul></div>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.info("历史周期中暂未找到对应周几的数据")

                # 档位速率与概率推演
                elapsed_days = max(elapsed.total_seconds() / 86400, 1e-6)
                elapsed_hours = max(elapsed.total_seconds() / 3600, 1e-6)
                current_rate_day = actual_total / elapsed_days
                current_rate_hour = actual_total / elapsed_hours
                st.markdown(
                    "<div style='padding:10px 14px;border-radius:12px;background:rgba(255,255,255,0.12);"
                    "margin-top:12px;'>当前运行速率 ≈ "
                    f"{badge(f'{current_rate_day:.1f}', '#0ea5e9')} 条/日 ｜ "
                    f"{badge(f'{current_rate_hour:.2f}', '#0ea5e9')} 条/小时。</div>",
                    unsafe_allow_html=True,
                )

                bucket_samples = []
                if remaining_offsets:
                    for cycle in hist_cycles:
                        df_cycle = cycle["df"].set_index("date")
                        addition = 0.0
                        valid = True
                        for offset in remaining_offsets:
                            target_date = cycle["start"] + timedelta(days=offset)
                            if target_date not in df_cycle.index:
                                valid = False
                                break
                            vals = df_cycle.loc[[target_date], "day_tweet_count"]
                            if vals.empty:
                                valid = False
                                break
                            addition += float(vals.iloc[0])
                        if valid:
                            bucket_samples.append(actual_total + addition)

                if bucket_samples:
                    bucket_ranges = [(start, start + 19) for start in range(100, 500, 20)]
                    total_samples = len(bucket_samples)
                    rows_html = []
                    for start, end in bucket_ranges:
                        count = sum(1 for val in bucket_samples if start <= val <= end)
                        prob = count / total_samples
                        rows_html.append(
                            f"<tr><td>{start}–{end}</td><td>{prob*100:.1f}%</td></tr>"
                        )
                    over_count = sum(1 for val in bucket_samples if val >= 500)
                    over_prob = over_count / total_samples
                    rows_html.append(f"<tr><td>≥500</td><td>{over_prob*100:.1f}%</td></tr>")
                    st.markdown(
                        "<div style='margin-top:12px;'>"
                        "<div style='font-weight:600;margin-bottom:6px;'>档位概率预测（20 条/档）</div>"
                        "<table style='width:100%;border-collapse:collapse;font-size:13px;'>"
                        "<tr style='text-align:left;border-bottom:1px solid rgba(255,255,255,0.2);'>"
                        "<th style='padding:4px;'>区间</th><th style='padding:4px;'>概率</th></tr>"
                        + "".join(rows_html)
                        + "</table>"
                        "<div style='font-size:12px;color:rgba(255,255,255,0.7);margin-top:6px;'>"
                        "依据历史周期在相同剩余天数上的真实产出估算。</div>"
                        "</div>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.info("历史周期样本不足，暂无法估计档位概率。")
            else:
                st.info("当前周期已完成，历史预测无需再估计。")
        else:
            st.info("所选历史周期暂无可用数据。")
    else:
        st.info("未选择历史周期，跳过历史预测。")


def render_weekly_hour_compare(
    hour_df: pd.DataFrame,
    base_range,
    show_values: bool,
    cycles: int = 3,
    weekday_order: List[str] = WEEKDAY_ORDER,
):
    st.subheader("🕒 历史 7 日周期对比（小时级）")
    if hour_df.empty:
        st.info("暂无小时级数据")
        return
    base_start = base_range[0]
    base_date = base_start.date() if isinstance(base_start, datetime) else base_start
    hour_lookup = (
        hour_df.groupby([hour_df["date"].dt.date, "hour_us"])["hour_tweet_count"].sum()
    )
    data = []
    hour_detail_rows = []
    for idx in range(1, cycles + 1):
        cycle_start = base_date - timedelta(days=7 * idx)
        cycle_end = cycle_start + timedelta(days=7)
        cycle_label = f"{cycle_start.strftime('%m/%d')}–{cycle_end.strftime('%m/%d')}"
        for offset in range(7):
            day = cycle_start + timedelta(days=offset)
            weekday_label = day.strftime("%a")
            counts = []
            for hour in range(24):
                val = hour_lookup.get((day, hour), 0)
                counts.append(val)
                hour_detail_rows.append(
                    {"cycle": cycle_label, "weekday": weekday_label, "hour": hour, "count": val}
                )
            total = sum(counts)
            if total == 0:
                continue
            top_hour = max(range(24), key=lambda h: counts[h])
            data.append(
                {
                    "weekday": weekday_label,
                    "top_hour": top_hour,
                    "count_at_hour": counts[top_hour],
                    "cycle": cycle_label,
                }
            )
    if not data:
        st.info("历史小时数据不足")
        return
    plot_df = pd.DataFrame(data)
    plot_df["weekday"] = pd.Categorical(plot_df["weekday"], categories=weekday_order, ordered=True)
    hover = alt.selection_point(fields=["weekday"], nearest=True, on="mouseover", empty="none")
    line = (
        alt.Chart(plot_df)
        .mark_line(point=True, interpolate="monotone")
        .encode(
            x=alt.X("weekday:O", sort=alt.Sort(weekday_order), title="", axis=alt.Axis(labelAngle=0)),
            y=alt.Y("top_hour:Q", title="Peak Hour (US)", scale=alt.Scale(domain=[0, 23])),
            color=alt.Color("cycle:N", title="周期"),
        )
    )
    points = (
        alt.Chart(plot_df)
        .mark_point(size=60)
        .encode(
            x=alt.X("weekday:O", sort=alt.Sort(weekday_order)),
            y="top_hour:Q",
            color=alt.Color("cycle:N", legend=alt.Legend(title="历史周期", orient="right")),
            tooltip=[
                "cycle:N",
                "weekday:N",
                alt.Tooltip("top_hour:Q", title="Hour (US)"),
                alt.Tooltip("count_at_hour:Q", title="Tweets at Hour"),
            ],
        )
        .add_params(hover)
    )
    rule = (
        alt.Chart(plot_df)
        .mark_rule(color="#888", strokeDash=[4, 4])
        .encode(x=alt.X("weekday:O", sort=alt.Sort(weekday_order)))
        .transform_filter(hover)
    )
    chart = line + points + rule
    if show_values:
        text = (
            alt.Chart(plot_df)
            .mark_text(fontSize=11, dy=-8)
            .encode(
                x=alt.X("weekday:O", sort=alt.Sort(weekday_order)),
                y="top_hour:Q",
                color=alt.Color("cycle:N", legend=None),
                text=alt.Text("top_hour:Q", format=".0f"),
            )
        )
        chart = chart + text
    st.altair_chart(chart, width="stretch")

    detail_df = pd.DataFrame(hour_detail_rows)
    detail_df["weekday"] = pd.Categorical(detail_df["weekday"], categories=weekday_order, ordered=True)
    heatmap = (
        alt.Chart(detail_df)
        .mark_rect()
        .encode(
            x=alt.X("weekday:O", sort=alt.Sort(weekday_order), title="", axis=alt.Axis(labelAngle=0)),
            y=alt.Y("hour:O", title="Hour (US)", sort=list(range(24))),
            color=alt.Color("count:Q", title="Tweets"),
            tooltip=["cycle:N", "weekday:N", "hour:Q", "count:Q"],
        )
    )
    st.altair_chart(heatmap, width="stretch")

    weekday_hour_avg = detail_df.groupby(["weekday", "hour"])["count"].mean().reset_index()
    top_pairs = weekday_hour_avg.sort_values("count", ascending=False).head(3)
    low_pairs = weekday_hour_avg.sort_values("count", ascending=True).head(3)
    summary_text = (
        "<div style='font-family:SF Pro Display,Helvetica,sans-serif;background:rgba(255,255,255,0.18);"
        "padding:12px 16px;border-radius:14px;margin-top:12px;'>"
        "<b>AI 观察：</b>热点集中在 "
        + ", ".join(f"{row['weekday']} {int(row['hour']):02d}:00 (≈{row['count']:.1f})" for _, row in top_pairs.iterrows())
        + "；冷点位于 "
        + ", ".join(f"{row['weekday']} {int(row['hour']):02d}:00 (≈{row['count']:.1f})" for _, row in low_pairs.iterrows())
        + "。可优先在热点时段加仓、冷点时段低频监控，以提升赔率把握。"
        "</div>"
    )
    st.markdown(summary_text, unsafe_allow_html=True)


def render_detail(detail_df: pd.DataFrame):
    st.subheader("📄 清洗明细（限定区间）")
    st.info("明细请通过上方下载最新清洗 CSV 查看，以避免冗长表格。")


def parse_weekday_from_text(text: str):
    for key, value in WEEKDAY_MAP.items():
        if key in text:
            return value
    return None


def parse_hour_from_text(text: str):
    match = re.search(r"(\d{1,2})\s*(?:点|时|hour|小时|:|点钟)", text)
    if match:
        hour = int(match.group(1))
        if 0 <= hour <= 23:
            return hour
    match = re.search(r"(\d{1,2})\s*h", text)
    if match:
        hour = int(match.group(1))
        if 0 <= hour <= 23:
            return hour
    return None


def parse_hour_window(text: str):
    range_match = re.search(r"(\d{1,2})\s*[-–~～至到]\s*(\d{1,2})", text)
    if range_match:
        start = int(range_match.group(1))
        end = int(range_match.group(2))
        if 0 <= start <= 23 and 0 <= end <= 23:
            if end < start:
                start, end = end, start
            return list(range(start, end + 1))
    single = parse_hour_from_text(text)
    if single is not None:
        return [single]
    return []


def aggregate_day_stats(frame: pd.DataFrame, start_dt: datetime, end_dt: datetime):
    subset = frame[["content", "EDT_time", "Beijing_time", "year", "Month", "WeekDay", "Hour"]].copy()
    bucket = polymarket.build_day_bucket_stats(subset)
    bucket["date"] = pd.to_datetime(bucket["date"], format="%m/%d/%Y")
    bucket = bucket[(bucket["date"] >= start_dt) & (bucket["date"] <= end_dt)]

    natural = polymarket.build_natural_day_stats(subset)
    natural["date"] = pd.to_datetime(natural["date"], format="%m/%d/%Y")
    natural = natural[(natural["date"] >= start_dt) & (natural["date"] <= end_dt)]
    return bucket, natural


def build_history_day_scope(detail_scope: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    empty_bucket = pd.DataFrame(columns=["date", "day_tweet_count", "week_day"])
    empty_natural = empty_bucket.copy()
    if detail_scope.empty:
        return empty_bucket, empty_natural

    scope_start = detail_scope["EST_dt"].min().floor("D")
    scope_end = detail_scope["EST_dt"].max().floor("D") + pd.Timedelta(hours=23, minutes=59, seconds=59)
    bucket, natural = aggregate_day_stats(detail_scope, scope_start.to_pydatetime(), scope_end.to_pydatetime())
    if "week_day" not in natural.columns:
        natural["week_day"] = natural["date"].dt.strftime("%a")
    if "week_day" not in bucket.columns:
        bucket["week_day"] = bucket["date"].dt.strftime("%a")
    return bucket, natural


def ai_cycle_analysis(query: str, detail_full: pd.DataFrame, base_start: datetime, cycles_default: int = 1):
    cycles_match = re.search(r"历史\s*(\d+)", query)
    cycles = int(cycles_match.group(1)) if cycles_match else cycles_default
    weekday = parse_weekday_from_text(query)
    hours = parse_hour_window(query)
    if weekday is None or not hours:
        return "请同时包含目标周几（如周三）以及小时或小时区间（如9~12点）。"

    results = []
    base_date = datetime.combine(base_start.date(), datetime.min.time())
    for idx in range(1, cycles + 1):
        start = base_date - timedelta(days=7 * idx)
        end = start + timedelta(days=7)
        mask = (detail_full["EST_dt"] >= start) & (detail_full["EST_dt"] < end)
        subset = detail_full.loc[mask]
        subset = subset[(subset["WeekDay"] == weekday) & (subset["Hour"].isin(hours))]
        results.append((start.strftime("%Y-%m-%d"), (end - timedelta(seconds=1)).strftime("%Y-%m-%d"), len(subset)))

    summary_lines = [
        f"周期 {i+1}: {start} → {end}，发推 {count} 条"
        for i, (start, end, count) in enumerate(results)
    ]
    if len(hours) == 1:
        hour_label = f"{hours[0]:02d}:00"
    else:
        hour_label = f"{hours[0]:02d}:00–{hours[-1]:02d}:59"
    head = f"AI 分析：针对历史 {cycles} 个周期的 {weekday} {hour_label} 发推量"
    return head + "\n" + "\n".join(summary_lines)


def filter_data(
    day_bucket_df: pd.DataFrame,
    day_natural_df: pd.DataFrame,
    hour_df: pd.DataFrame,
    detail_df: pd.DataFrame,
    date_range: Tuple[datetime.date, datetime.date],
    weekday_filter,
    hour_filter,
):
    start_date, end_date = date_range
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())

    hour_mask = (
        (hour_df["date"] >= start_dt)
        & (hour_df["date"] <= end_dt)
        & (hour_df["week_day"].isin(weekday_filter))
        & (hour_df["hour_us"].isin(hour_filter))
    )
    hour_filtered = hour_df.loc[hour_mask].copy()

    detail_mask = (
        (detail_df["EST_dt"] >= start_dt)
        & (detail_df["EST_dt"] <= end_dt)
        & (detail_df["WeekDay"].isin(weekday_filter))
        & (detail_df["Hour"].isin(hour_filter))
    )
    detail_filtered = detail_df.loc[detail_mask].copy()

    if not detail_filtered.empty:
        day_bucket_filtered, day_natural_filtered = aggregate_day_stats(detail_filtered, start_dt, end_dt)
    else:
        day_bucket_filtered = day_bucket_df.iloc[0:0].copy()
        day_natural_filtered = day_natural_df.iloc[0:0].copy()

    return day_bucket_filtered, day_natural_filtered, hour_filtered, detail_filtered, start_dt, end_dt


def main():
    st.set_page_config(page_title="Musk Tweet Analyzer", layout="wide")
    st.markdown(
        """
        <style>
        .stApp {
            background: linear-gradient(135deg, #f4f7fb, #e0e7ff 60%, #fdf2f8);
            color: #0f172a;
        }
        div.block-container {
            padding-top: 1rem;
            padding-bottom: 2rem;
        }
        .glass-panel, .glass-card {
            background: rgba(255,255,255,0.9);
            border: 1px solid rgba(148,163,184,0.4);
            box-shadow: 0 18px 35px rgba(15,23,42,0.12);
            border-radius: 22px;
            padding: 18px;
            margin-bottom: 18px;
            backdrop-filter: blur(10px);
            color: #0f172a;
        }
        .glass-panel h4, .glass-card h4, .glass-card h3, .glass-panel h3 {
            margin-top: 0;
            color: #0f172a;
        }
        .glass-panel label, .glass-card label, .glass-panel p, .glass-card p {
            color: #0f172a;
        }
        .glass-panel .stButton>button, .glass-card .stButton>button {
            background: linear-gradient(135deg,#2563eb,#22d3ee);
            color: #fff;
            border: none;
            border-radius: 999px;
            padding: 6px 14px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.title("Musk 推文清洗 + 分析工作台")

    if "ai_response" not in st.session_state:
        st.session_state["ai_response"] = ""
    if "ai_query_text" not in st.session_state:
        st.session_state["ai_query_text"] = ""
    if "ai_should_run" not in st.session_state:
        st.session_state["ai_should_run"] = False

    navigation_col, content_col = st.columns([1, 4])
    uploaded = None
    fetch_latest = None

    with navigation_col:
        with glass_block("glass-panel"):
            st.subheader("数据源")
            uploaded = st.file_uploader("上传 XTracker 原始 CSV（可选）", type=["csv"])
            fetch_latest = st.button("在线获取 XTracker 最新数据", use_container_width=True)

    clean_path, source_info = ensure_file(uploaded, fetch_latest=fetch_latest)
    if clean_path is None:
        return

    detail_df, day_bucket_df, day_natural_df, hour_df = load_clean_outputs(clean_path)
    min_day, max_day = day_natural_df["date"].min().date(), day_natural_df["date"].max().date()
    default_start = max(min_day, (max_day - timedelta(days=7)))
    today_local = datetime.now(ZoneInfo("Asia/Shanghai")).date()

    pending_range = st.session_state.pop("pending_cycle_range", None)
    pending_display_end = st.session_state.pop("pending_cycle_display_end", None)
    last_sidebar_range = st.session_state.get("last_sidebar_range", (default_start, max_day))
    last_main_range = st.session_state.get("last_main_range", (default_start, max_day))
    current_display_end = st.session_state.get("current_cycle_display_end", last_sidebar_range[0] + timedelta(days=7))
    if pending_range:
        last_sidebar_range = pending_range
        last_main_range = pending_range
        if pending_display_end:
            current_display_end = pending_display_end

    with navigation_col:
        with glass_block("glass-panel"):
            st.subheader("导航与筛选")
            section_label = st.radio("快速跳转", list(SECTION_OPTIONS.values()), index=0)
            section_key = [k for k, v in SECTION_OPTIONS.items() if v == section_label][0]

            date_range_sidebar = st.date_input(
                "日期范围（EST 12PM 边界）",
                value=last_sidebar_range,
                min_value=min_day,
                max_value=max_day,
            )
            last_sidebar_range = date_range_sidebar

            weekday_filter = st.multiselect("选择 Weekday", WEEKDAY_ORDER, default=WEEKDAY_ORDER)
            hour_options = sorted(hour_df["hour_us"].dropna().unique().tolist())
            hour_filter = st.multiselect("美东小时", hour_options, default=hour_options if hour_options else [])
            history_cycles = st.slider("历史周期条数（每条=向前 7 天）", min_value=1, max_value=12, value=4)
            show_values = st.checkbox("显示图表数值标签", value=False)

            cycle_shortcuts = build_cycle_shortcuts(min_day, max_day, today_local)
            if cycle_shortcuts:
                st.markdown("#### 🗓 盘口快捷周期（选择即应用）")
                labels = [opt["label"] for opt in cycle_shortcuts]
                if "cycle_select_last" not in st.session_state:
                    st.session_state["cycle_select_last"] = labels[0]
                    st.session_state["current_cycle_display_end"] = cycle_shortcuts[0]["display_end"]
                prev_label = st.session_state.get("cycle_select_last", labels[0])
                selected_label = st.selectbox(
                    "选择起始日期",
                    labels,
                    index=labels.index(prev_label) if prev_label in labels else 0,
                    key="cycle_select",
                )
                if selected_label != prev_label:
                    chosen = next(opt for opt in cycle_shortcuts if opt["label"] == selected_label)
                    st.session_state["pending_cycle_range"] = (chosen["start"], chosen["end"])
                    st.session_state["pending_cycle_display_end"] = chosen.get("display_end", chosen["end"])
                    st.session_state["cycle_select_last"] = selected_label
                    st.rerun()

            st.divider()
            st.markdown("### 🤖 AI 周期问答")
            ai_query = st.text_area(
                "输入想分析的规则",
                value=st.session_state.get("ai_query_text", ""),
                placeholder="例：历史2个周期，周二 10 点发推多少？",
                height=110,
                key="ai_query_input",
            )
            if st.button("生成 AI 分析", key="ai_query_button"):
                st.session_state["ai_query_text"] = ai_query.strip()
                st.session_state["ai_should_run"] = True
            ai_output_placeholder = st.empty()
            ai_response = st.session_state.get("ai_response", "")
            if ai_response:
                formatted = ai_response.replace("\n", "<br>")
                ai_output_placeholder.markdown(
                    f"<div style='font-size:12px;background:rgba(255,255,255,0.08);padding:10px;border-radius:10px;'>{formatted}</div>",
                    unsafe_allow_html=True,
                )

    if isinstance(date_range_sidebar, tuple):
        date_range_sidebar = (date_range_sidebar[0], date_range_sidebar[1])
    else:
        date_range_sidebar = (date_range_sidebar, date_range_sidebar)

    weekday_filter = weekday_filter or WEEKDAY_ORDER
    hour_defaults = hour_options if hour_options else list(range(24))
    hour_filter = hour_filter or hour_defaults

    st.session_state["last_sidebar_range"] = last_sidebar_range
    st.session_state["last_main_range"] = last_main_range
    st.session_state["current_cycle_display_end"] = current_display_end

    with content_col:
        date_range_active = last_main_range
        st.session_state["last_main_range"] = last_main_range

        history_span_days = max(history_cycles, 1) * 7
        hist_start_date = date_range_active[0] - timedelta(days=history_span_days)
        hist_start_dt = datetime.combine(hist_start_date, datetime.min.time())
        hist_end_dt = datetime.combine(date_range_active[1], datetime.max.time())

        detail_scope_mask = (
            detail_df["WeekDay"].isin(weekday_filter)
            & detail_df["Hour"].isin(hour_filter)
            & (detail_df["EST_dt"] >= hist_start_dt)
            & (detail_df["EST_dt"] <= hist_end_dt)
        )
        detail_scope_df = detail_df.loc[detail_scope_mask].copy()

        hour_scope_mask = (
            hour_df["week_day"].isin(weekday_filter)
            & hour_df["hour_us"].isin(hour_filter)
            & (hour_df["date"] >= hist_start_dt)
            & (hour_df["date"] <= hist_end_dt)
        )
        hour_scope_df = hour_df.loc[hour_scope_mask].copy()

        history_bucket_scope, history_day_scope = build_history_day_scope(detail_scope_df)

        if st.session_state.get("ai_should_run"):
            query_text = st.session_state.get("ai_query_text", "").strip()
            if query_text:
                ai_source = detail_scope_df if not detail_scope_df.empty else detail_df
                anchor_dt = datetime.combine(date_range_active[1], datetime.min.time())
                st.session_state["ai_response"] = ai_cycle_analysis(
                    query_text, ai_source, anchor_dt, cycles_default=history_cycles
                )
            else:
                st.session_state["ai_response"] = "请输入需要分析的周几与小时描述。"
            st.session_state["ai_should_run"] = False

        (
            day_bucket_filtered,
            day_natural_filtered,
            hour_filtered,
            detail_filtered,
            start_dt,
            end_dt,
        ) = filter_data(
            day_bucket_df, day_natural_df, hour_df, detail_df, date_range_active, weekday_filter, hour_filter
        )

        if not day_natural_filtered.empty:
            first_weekday = (
                day_natural_filtered.sort_values("date")["week_day"].iloc[0]
            )
            start_idx = WEEKDAY_ORDER.index(first_weekday)
            dynamic_weekday_order = WEEKDAY_ORDER[start_idx:] + WEEKDAY_ORDER[:start_idx]
        else:
            dynamic_weekday_order = WEEKDAY_ORDER

        with glass_block():
            latest_data_dt = day_natural_df["date"].max()
            latest_time_str = latest_data_dt.strftime("%Y-%m-%d") if not pd.isna(latest_data_dt) else "未知"
            st.caption(
                f"数据来源：{source_info['mode']}（{source_info['name']}），最新数据时间：{latest_time_str}"
            )
            st.caption(f"筛选范围：{start_dt:%Y-%m-%d} → {end_dt:%Y-%m-%d}")
            st.download_button("下载最新清洗 CSV", data=clean_path.read_bytes(), file_name=clean_path.name)

            cycle_start = date_range_active[0]
            cycle_actual_end = date_range_active[1]
            cycle_display_end = st.session_state.get("current_cycle_display_end", cycle_actual_end)

            render_cst_clock()
            render_cycle_forecast(
                day_bucket_df,
                day_bucket_filtered,
                history_bucket_scope,
                history_cycles,
                cycle_start,
                cycle_actual_end,
                cycle_display_end,
            )
            metrics_overview(day_bucket_filtered, detail_filtered)

        with glass_block():
            render_historical_today_table(
                day_natural_filtered,
                history_day_scope,
                history_cycles,
                hour_scope_df,
                date_range_active[1],
            )

        if section_key == "overview":
            with glass_block():
                render_day_section(
                    day_natural_filtered,
                    show_values,
                    history_df=history_day_scope,
                    history_cycles=history_cycles,
                    base_range=date_range_active,
                )
            with glass_block():
                render_cycle_totals(history_bucket_scope, date_range_active, history_cycles, show_values)
            with glass_block():
                render_weekly_hour_compare(
                    hour_scope_df, date_range_active, show_values, history_cycles, dynamic_weekday_order
                )
            with glass_block():
                render_hour_section(hour_filtered, show_values)
            with glass_block():
                render_weekday_section(day_natural_filtered, show_values)
            with glass_block():
                render_heatmap(hour_filtered, show_values)
            with glass_block():
                behavior_insights(day_natural_filtered, hour_filtered)
            with glass_block():
                render_detail(detail_filtered)
        elif section_key == "daily":
            with glass_block():
                render_day_section(
                    day_natural_filtered,
                    show_values,
                    history_df=history_day_scope,
                    history_cycles=history_cycles,
                    base_range=date_range_active,
                )
        elif section_key == "weekly_compare_day":
            with glass_block():
                render_weekly_compare(
                    history_day_scope, date_range_active, show_values, history_cycles, dynamic_weekday_order
                )
        elif section_key == "weekly_cycle_total":
            with glass_block():
                render_cycle_totals(history_bucket_scope, date_range_active, history_cycles, show_values)
        elif section_key == "weekly_compare_hour":
            with glass_block():
                render_weekly_hour_compare(
                    hour_scope_df, date_range_active, show_values, history_cycles, dynamic_weekday_order
                )
        elif section_key == "hourly":
            with glass_block():
                render_hour_section(hour_filtered, show_values)
        elif section_key == "weekday":
            with glass_block():
                render_weekday_section(day_natural_filtered, show_values)
        elif section_key == "heatmap":
            with glass_block():
                render_heatmap(hour_filtered, show_values)
        elif section_key == "insight":
            with glass_block():
                behavior_insights(day_natural_filtered, hour_filtered)
        elif section_key == "detail":
            with glass_block():
                render_detail(detail_filtered)


if __name__ == "__main__":
    main()
