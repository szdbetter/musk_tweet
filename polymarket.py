# -*- coding: utf-8 -*-
"""
Elon Musk 推文数据清洗 + 分析 工具（交互菜单版）
- 1 清洗：将混乱的 XTracker 导出 CSV 合并碎行、修复引号、推断年份、生成北京时间列
- 2 分析：基础概览（月份/星期/小时）
- 3 高级分析：按你的维度需求（周维度、小时维度、周+小时维度），支持输入“过去 N 个月”
"""

import os
import re
import glob
import calendar
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Set

from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt, IntPrompt
from rich.panel import Panel
from rich.progress import track

console = Console()
BASE_DIR = Path(__file__).resolve().parent

# ====================== 配置 ======================
INPUT_FILE = "elonmusk.csv"            # 原始导出文件（未清洗）
OUTPUT_PREFIX = str(BASE_DIR / "elonmusk_clean")  # 清洗文件前缀（绝对路径）
ENCODING = "utf-8"                     # 读取原始文件时的编码
START_YEAR = 2024                      # 第一段月份所属年份（后续遇到月份回卷则 +1 年）
MONTH_ORDER_ENG = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
WEEK_ORDER = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
# ==================================================


# ------------------------ 工具函数 ------------------------
def next_output_name(prefix: str) -> str:
    """扫描现有 clean 文件并生成下一个三位编号的输出文件名。"""
    exists = sorted(glob.glob(f"{prefix}_*.csv"))
    if not exists:
        return f"{prefix}_001.csv"
    nums = []
    for f in exists:
        m = re.search(r'_(\d{3})\.csv$', f)
        if m:
            nums.append(int(m.group(1)))
    return f"{prefix}_{(max(nums)+1 if nums else 1):03d}.csv"


def parse_est_datetime(series: pd.Series) -> pd.Series:
    """
    将 “YYYY-MM-DD HH:MM:SS EDT/EST” 解析为带时区的 America/New_York 时间。
    """
    if series.empty:
        return pd.Series(dtype="datetime64[ns, America/New_York]")
    normalized = (
        series.astype(str)
        .str.strip()
        .str.replace(" EDT", " -0400", regex=False)
        .str.replace(" EST", " -0500", regex=False)
    )
    dt = pd.to_datetime(normalized, format="%Y-%m-%d %H:%M:%S %z", errors="coerce", utc=True)
    return dt.dt.tz_convert("America/New_York")


def parse_bj_datetime(series: pd.Series) -> pd.Series:
    """
    将 “YYYY-MM-DD HH:MM:SS CST” 解析为 Asia/Shanghai 时间。
    """
    normalized = series.astype(str).str.strip().str.replace(" CST", " +0800", regex=False)
    dt = pd.to_datetime(normalized, format="%Y-%m-%d %H:%M:%S %z", errors="coerce", utc=True)
    return dt.dt.tz_convert("Asia/Shanghai")


def coalesce_records(lines: List[str]) -> List[str]:
    """把非以推文ID开头的行并回上一条，保证一条推文只占一行（粗合并）"""
    recs, buf = [], []
    id_head_re = re.compile(r'^\s*"?(\d{18,19})"?,')
    for raw in lines:
        line = raw.rstrip("\n\r")
        if id_head_re.match(line):
            if buf:
                recs.append(" ".join(buf).strip())
                buf = []
            buf.append(line.strip())
        else:
            buf.append(line.strip())
    if buf:
        recs.append(" ".join(buf).strip())
    return recs


def clean_content_text(s: str) -> str:
    """清洗内容字段里的多余引号/逗号断裂"""
    # 先把 '","' 的 CSV 分隔情况温和处理为逗号+空格（不破坏最外层）
    s = s.replace('","', ', ')
    # 去掉最外围引号
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1]
    # 内部双引号转义还原
    s = s.replace('""', '"')
    # 压缩多空白
    s = re.sub(r'\s+', ' ', s)
    return s.strip()


def parse_record(rec: str):
    """
    从合并后的单行记录里抽取：
    - ID
    - 内容
    - 日期碎片（Mon Day, hh:mm:ss AM/PM TZ）
    """
    # 抓最右侧的日期时间片段
    tail_re = re.compile(
        r',\s*("?(?P<mon>[A-Za-z]{3})\s+(?P<day>\d{1,2}),\s+(?P<time>\d{1,2}:\d{2}:\d{2})\s+(?P<ampm>AM|PM)\s+(?P<tz>[A-Z]{2,4})"?)\s*$'
    )
    m_date = tail_re.search(rec)
    if not m_date:
        return None

    m_id = re.match(r'^\s*"?(\d{18,19})"?,', rec)
    if not m_id:
        return None
    tw_id = m_id.group(1)

    first_comma_idx = rec.find(",", m_id.end() - 1)
    if first_comma_idx == -1:
        return None

    date_span_start = m_date.start()
    content_chunk = rec[first_comma_idx + 1 : date_span_start]
    content_clean = clean_content_text(content_chunk)

    return tw_id, content_clean, m_date.groupdict()


def assign_years(parsed_records: List[Tuple[str, str, Dict[str, str]]]) -> Tuple[List[Tuple[str,str,str,str,int,int,str,int]], Set[int]]:
    """
    根据月份回卷规则为每条记录分配年份，并生成“EDT时间字符串 + 北京时间字符串”
    返回：
      rows: [(id, content, EDT时间字符串, 北京时间字符串, 年份, 月份, 周几简称, 小时)]
      all_years: 涉及到的年份集合
    """
    year = START_YEAR
    month_idx_prev = None
    results = []
    all_years = set()

    for rid, content, date_info in parsed_records:
        mon = date_info["mon"]
        if mon not in MONTH_ORDER_ENG:
            continue
        idx = MONTH_ORDER_ENG.index(mon)  # 0-11

        if month_idx_prev is not None and idx < month_idx_prev:
            year += 1
            console.log(f"📆 检测到跨年：{MONTH_ORDER_ENG[month_idx_prev]}→{mon}，年份切换到 {year}")
        month_idx_prev = idx
        all_years.add(year)

        day = int(date_info["day"])
        time_str = date_info["time"]
        ampm = date_info["ampm"]
        tz = date_info["tz"]

        # 解析 12 小时制
        t = datetime.strptime(f"{time_str} {ampm}", "%I:%M:%S %p")
        edt_dt = datetime(year, idx + 1, day, t.hour, t.minute, t.second)
        # 根据原始标识判断时差：EDT(+12h)、EST(+13h)，默认按 12 小时处理
        tz_upper = tz.upper()
        offset_hours = 13 if tz_upper == "EST" else 12
        bj_dt = edt_dt + timedelta(hours=offset_hours)

        edt_fmt = f"{year:04d}-{idx+1:02d}-{day:02d} {t.strftime('%H:%M:%S')} {tz}"
        bj_fmt  = bj_dt.strftime("%Y-%m-%d %H:%M:%S CST")

        extra_year = bj_dt.year
        extra_month = bj_dt.month
        extra_weekday = bj_dt.strftime("%a")
        extra_hour = bj_dt.hour

        results.append((rid, content, edt_fmt, bj_fmt, extra_year, extra_month, extra_weekday, extra_hour))

    return results, all_years


def robust_parse_bj(series: pd.Series) -> pd.Series:
    """
    更鲁棒地解析 “北京时间” 列为 pandas datetime：
    - 去掉末尾时区字样（如 CST）
    - 先尝试固定格式，再 fallback 到 dateutil
    """
    s = series.astype(str).str.replace(r'\s+[A-Z]{2,4}$', '', regex=True)
    dt = pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    if dt.isna().all():
        dt = pd.to_datetime(s, errors="coerce")
    return dt


def filter_last_n_months(df: pd.DataFrame, n_months: int) -> pd.DataFrame:
    """
    以数据中“北京时间”的最大月份为锚点，回溯 N 个月（包含锚点月），返回该区间数据
    """
    dt = robust_parse_bj(df["北京时间"])
    df = df.copy()
    df["__dt"] = dt
    max_dt = df["__dt"].max()
    if pd.isna(max_dt):
        return df.iloc[0:0]
    # 计算起点（滚动 n-1 个月）
    year, month = max_dt.year, max_dt.month
    for _ in range(n_months - 1):
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    start_dt = datetime(year, month, 1)
    # 终点为锚点月最后一天 23:59:59
    end_day = calendar.monthrange(max_dt.year, max_dt.month)[1]
    end_dt = datetime(max_dt.year, max_dt.month, end_day, 23, 59, 59)

    mask = (df["__dt"] >= start_dt) & (df["__dt"] <= end_dt)
    return df.loc[mask].drop(columns=["__dt"])


def build_day_bucket_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    生成“12:00 → 次日 12:00”的按天统计。通过把时间整体减去 12 小时，
    再按日期分组即可得到以中午为界的窗口。
    """
    est_series = parse_est_datetime(df["EDT_time"])
    shifted = (est_series - pd.to_timedelta(12, unit="h")).dt.tz_localize(None).dropna()
    day_counts = shifted.dt.floor("D").value_counts().sort_index()
    result = day_counts.reset_index()
    result.columns = ["date", "day_tweet_count"]
    result["week_day"] = pd.to_datetime(result["date"]).dt.strftime("%a")
    result["date"] = pd.to_datetime(result["date"]).dt.strftime("%m/%d/%Y")
    return result[["date", "week_day", "day_tweet_count"]]


def build_natural_day_stats(df: pd.DataFrame) -> pd.DataFrame:
    """按自然日（EST 0:00-23:59）统计推文数量，用于日线展示。"""
    est_series = parse_est_datetime(df["EDT_time"])
    natural = (
        pd.DataFrame({"date": est_series.dt.tz_localize(None).dt.date, "week_day": est_series.dt.strftime("%a")})
        .dropna()
    )
    counts = (
        natural.groupby(["date", "week_day"]).size().reset_index(name="day_tweet_count")
    )
    counts["date"] = pd.to_datetime(counts["date"]).dt.strftime("%m/%d/%Y")
    return counts[["date", "week_day", "day_tweet_count"]]


def build_hourly_stats(df: pd.DataFrame) -> pd.DataFrame:
    """生成逐小时推文统计，同时记录美东与北京时间的小时。"""
    est_series = parse_est_datetime(df["EDT_time"])
    bj_series = parse_bj_datetime(df["Beijing_time"])
    valid_mask = est_series.notna() & bj_series.notna()
    est_series = est_series[valid_mask]
    bj_series = bj_series[valid_mask]
    temp = pd.DataFrame(
        {
            "date": est_series.dt.tz_convert(None).dt.strftime("%m/%d/%Y"),
            "week_day": est_series.dt.strftime("%a"),
            "hour_us": est_series.dt.hour,
            "hour_cn": bj_series.dt.hour,
        }
    )
    counts = (
        temp.value_counts()
        .reset_index(name="hour_tweet_count")
        .sort_values(["date", "hour_us", "hour_cn"])
    )
    return counts


# ------------------------ 1) 清洗 ------------------------
def run_cleaning():
    """
    读取原始 XTracker CSV，重建破碎行并补全时间信息，产出标准化的 clean 文件。
    步骤：
      1. 合并碎行并解析 tweet_id / 内容 / 日期片段；
      2. 根据月份回卷推断年份并生成北京时间列；
      3. 写入新的 CSV 并打印清洗摘要。
    """
    if not os.path.exists(INPUT_FILE):
        console.print(f"[red]❌ 未找到源文件：{INPUT_FILE}[/red]")
        return

    out_name = next_output_name(OUTPUT_PREFIX)

    console.print(f"\n[bold cyan]🚀 开始清洗文件：[/bold cyan]{INPUT_FILE}\n")
    with open(INPUT_FILE, "r", encoding=ENCODING, errors="ignore") as f:
        raw_lines = f.readlines()
    total_lines = len(raw_lines)
    if total_lines == 0:
        console.print("[red]❌ 文件为空[/red]")
        return

    header = raw_lines[0].strip()
    coalesced = coalesce_records(raw_lines[1:])
    # 统计原始有效记录（粗估：以 id 开头的行数）
    raw_valid = sum(1 for line in raw_lines[1:] if re.match(r'^\s*"?\d{18,19}"?,', line))

    parsed_records = []
    for rec in coalesced:
        pr = parse_record(rec)
        if pr:
            parsed_records.append(pr)

    rows, all_years = assign_years(parsed_records)
    df_clean = pd.DataFrame(
        [
            (content, edt_str, bj_time, year_val, month_val, weekday_val, hour_val)
            for _tw_id, content, edt_str, bj_time, year_val, month_val, weekday_val, hour_val in rows
        ],
        columns=["content", "EDT_time", "Beijing_time", "year", "Month", "WeekDay", "Hour"],
    )

    # Excel 中避免中文表头乱码，统一改为英文标题
    header = '"content","EDT_time","Beijing_time","year","Month","WeekDay","Hour"'

    with open(out_name, "w", encoding="utf-8", newline="") as out:
        out.write(header + "\n")
        for _tw_id, content, edt_str, bj_time, year_val, month_val, weekday_val, hour_val in rows:
            esc = lambda x: '"' + x.replace('"', '""') + '"'
            out.write(
                f'{esc(content)},{esc(edt_str)},{esc(bj_time)},'
                f'{year_val},{month_val},"{weekday_val}",{hour_val}\n'
            )

    # 生成统计用 Excel（单文件含 detail + 汇总）
    day_stats = build_day_bucket_stats(df_clean)
    day_natural_stats = build_natural_day_stats(df_clean)
    hour_stats = build_hourly_stats(df_clean)
    excel_path = out_name.replace(".csv", "_stats.xlsx")
    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        df_clean.to_excel(writer, sheet_name="detail", index=False)
        day_stats.to_excel(writer, sheet_name="day_summary_12PM-12PM_EST", index=False)
        day_natural_stats.to_excel(writer, sheet_name="day_summary_natural_EST", index=False)
        hour_stats.to_excel(writer, sheet_name="hour_summary", index=False)

    cleaned = len(rows)
    removed = max(raw_valid - cleaned, 0)

    console.print(Panel.fit(f"""
📦 原始文件：{os.path.basename(INPUT_FILE)}（总行数 {total_lines}，疑似有效记录 {raw_valid}）
🧹 清洗后：{os.path.basename(out_name)}（有效推文 {cleaned} 条）
🗑️ 估算清理掉碎行/无效：{removed} 条
📅 共检测到 {len(all_years)} 年：{(min(all_years) if all_years else '—')} – {(max(all_years) if all_years else '—')}
✅ [bold green]清洗完成（已含北京时间 + 年份推断）[/bold green]
""", title="清洗报告", border_style="green"))


# ------------------------ 2) 基础分析 ------------------------
def basic_overview(selected_file: str):
    """
    展示单个 clean CSV 的核心概览，包括月份/星期/小时分布以及原创 vs 转推占比。
    解析逻辑：
      - 将“北京时间”列转换为 pandas 时间戳（带时区）；
      - 逐个维度统计频次并用 rich 表格渲染；
      - 输出补充指标（如转推占比）。
    """
    console.print(f"\n[bold]📊 正在分析：[/bold]{selected_file}\n")
    df = pd.read_csv(selected_file)

    # —— 统一把“CST”当作中国标准时间 —— #
    BJ_TZ = "Asia/Shanghai"

    def parse_bj_time(s: str):
        """
        期望输入：'YYYY-MM-DD HH:MM:SS CST' 或 'YYYY-MM-DD HH:MM:SS'
        无论有没有 'CST'，都按北京时间本地化
        """
        if pd.isna(s):
            return pd.NaT
        txt = str(s).strip()
        if not txt:
            return pd.NaT

        # 去掉可能出现的 'CST'（防止被误判为美中 CST）
        txt = txt.replace("CST", "").strip()

        # 明确用固定格式解析，避免 pandas 猜测
        # 你的清洗代码里北京时间就是 'YYYY-MM-DD HH:MM:SS'
        try:
            dt = datetime.strptime(txt, "%Y-%m-%d %H:%M:%S")
            # 本地化到北京时区
            return pd.Timestamp(dt, tz=BJ_TZ)
        except Exception:
            # 兜底：最后再让 pandas 尝试解析，但立刻本地化/转换到北京时区
            ts = pd.to_datetime(txt, errors="coerce", utc=True)
            if pd.isna(ts):
                return pd.NaT
            return ts.tz_convert(BJ_TZ)

    # ✅ 使用稳健解析
    df["datetime_bj"] = df["北京时间"].apply(parse_bj_time)

    # 如果后续需要无时区的“普通时间列”，可以再加：
    # df["datetime_bj_naive"] = df["datetime_bj"].dt.tz_convert(BJ_TZ).dt.tz_localize(None)

    # 内容列（自动取第 2 列名）
    content_col = df.columns[1]

    # 月份统计
    console.print(Panel.fit("📆 按月份统计发推数量", border_style="cyan"))
    if df["datetime_bj"].notna().any():
        df["month_num"] = df["datetime_bj"].dt.month
        month_counts = df["month_num"].value_counts().sort_index()
        if month_counts.empty:
            console.print("[yellow]⚠️ 没有检测到月份数据[/yellow]")
        else:
            t = Table(show_header=True, header_style="bold blue")
            t.add_column("月份")
            t.add_column("发推数量", justify="right")
            for m, c in month_counts.items():
                t.add_row(calendar.month_abbr[m], str(int(c)))
            console.print(t)
    else:
        console.print("[yellow]⚠️ 无法解析北京时间，月份表为空[/yellow]")

    # 星期统计（按月份汇总）
    console.print(Panel.fit("📅 周一至周日发推数量（按月份汇总）", border_style="magenta"))
    if df["datetime_bj"].notna().any():
        df["weekday"] = df["datetime_bj"].dt.day_name()
        df["month_num"] = df["datetime_bj"].dt.month
        pivot = pd.pivot_table(df, index="month_num", columns="weekday", values=content_col, aggfunc="count", fill_value=0)
        if not pivot.empty:
            pivot = pivot.reindex(columns=WEEK_ORDER).fillna(0)
            t = Table(show_header=True, header_style="bold magenta")
            t.add_column("月份")
            for w in WEEK_ORDER:
                t.add_column(w, justify="right")
            for m in pivot.index:
                row = [calendar.month_abbr[m]] + [str(int(v)) for v in pivot.loc[m, WEEK_ORDER]]
                t.add_row(*row)
            console.print(t)
        else:
            console.print("[yellow]⚠️ 无法生成星期分布表[/yellow]")
    else:
        console.print("[yellow]⚠️ 无法解析北京时间，星期表为空[/yellow]")

    # 小时分布（北京时间）
    console.print(Panel.fit("🕒 每小时（北京时间）发推数量", border_style="green"))
    if df["datetime_bj"].notna().any():
        df["hour"] = df["datetime_bj"].dt.hour
        hour_counts = df["hour"].value_counts().sort_index()
        if hour_counts.empty:
            console.print("[yellow]⚠️ 无小时分布数据[/yellow]")
        else:
            t = Table(show_header=True, header_style="bold yellow")
            t.add_column("北京时间")
            t.add_column("发推数量", justify="right")
            for h, c in hour_counts.items():
                t.add_row(f"{h:02d}:00–{(h+1)%24:02d}:00", str(int(c)))
            console.print(t)
    else:
        console.print("[yellow]⚠️ 无法解析北京时间，小时表为空[/yellow]")

    # 原创/转推占比
    df["is_rt"] = df[content_col].astype(str).str.startswith("RT @")
    rt_ratio = float(df["is_rt"].mean()) if len(df) else 0.0
    console.print(f"\n💬 转推占比：{rt_ratio:.2%}，原创占比：{1-rt_ratio:.2%}")


# ------------------------ 3) 高级自定义分析 ------------------------
from rich.style import Style
from rich.box import SIMPLE_HEAD

def advanced_analysis(selected_file: str):
    """
    根据用户选择执行 N 个月滚动窗口的周维度 / 小时维度 / 周×小时热力分析。
    重点：先筛选指定范围的数据，再构建数据透视表并以颜色编码展示密度。
    """
    console.print(Panel.fit("🧭 高级自定义分析\n1) 过去 N 个月：周一至周日分布\n2) 过去 N 个月：小时分布\n3) 过去 N 个月：周×小时二维分布（彩色热度）\n4) 返回上一级",
                            border_style="cyan"))
    choice = Prompt.ask("请选择功能", choices=["1","2","3","4"])
    if choice == "4":
        return

    df = pd.read_csv(selected_file)
    df["datetime_bj"] = pd.to_datetime(df["北京时间"], errors="coerce")
    if not df["datetime_bj"].notna().any():
        console.print("[red]❌ 无法解析北京时间，无法进行高级分析[/red]")
        return

    months_back = IntPrompt.ask("请输入过去 N 个月（≥1）", default=3, show_default=True)
    if months_back < 1:
        months_back = 1

    dfw = filter_last_n_months(df, months_back)
    if dfw.empty:
        console.print("[yellow]⚠️ 指定范围内无数据[/yellow]")
        return

    dfw["weekday"] = dfw["datetime_bj"].dt.day_name()
    dfw["hour"] = dfw["datetime_bj"].dt.hour
    content_col = dfw.columns[1]

    # ===== 周 + 小时二维表 =====
    if choice == "3":
        console.print(Panel.fit(f"🧩 过去 {months_back} 个月：周×小时二维分布（北京时间）", border_style="cyan"))
        pivot = pd.pivot_table(dfw, index="weekday", columns="hour", values=content_col, aggfunc="count", fill_value=0)
        pivot = pivot.reindex(index=WEEK_ORDER, columns=sorted(pivot.columns))

        # 渲染分两页显示 (0–11, 12–23)
        for block_start in [0, 12]:
            cols = list(range(block_start, block_start + 12))
            sub = pivot[cols].copy()

            t = Table(show_header=True, box=SIMPLE_HEAD, header_style="bold cyan", title=f"{block_start:02d}:00–{(block_start+11)%24:02d}:59 区间")
            t.add_column("星期", justify="center", style="bold")
            for h in cols:
                t.add_column(f"{h:02d}", justify="right", width=4)

            max_val = int(sub.values.max()) if sub.values.size > 0 else 0
            for w in WEEK_ORDER:
                row = [w]
                for h in cols:
                    val = int(sub.loc[w, h]) if (w in sub.index and h in sub.columns) else 0
                    if max_val == 0:
                        color = "grey39"
                    else:
                        ratio = val / max_val
                        if ratio > 0.75:
                            color = "bold red"
                        elif ratio > 0.4:
                            color = "yellow"
                        elif ratio > 0.1:
                            color = "cyan"
                        else:
                            color = "grey39"
                    row.append(f"[{color}]{val}[/]")
                t.add_row(*row)

            console.print(t)
        return

    # ===== 周维度 =====
    if choice == "1":
        console.print(Panel.fit(f"📅 过去 {months_back} 个月：周一至周日发推数量", border_style="magenta"))
        wk_counts = dfw["weekday"].value_counts()
        t = Table(show_header=True, box=SIMPLE_HEAD, header_style="bold magenta")
        t.add_column("星期", justify="center")
        t.add_column("发推数量", justify="right")
        for w in WEEK_ORDER:
            t.add_row(w, str(int(wk_counts.get(w, 0))))
        console.print(t)
        return

    # ===== 小时维度 =====
    if choice == "2":
        console.print(Panel.fit(f"🕒 过去 {months_back} 个月：每小时（北京时间）发推数量", border_style="green"))
        hr_counts = dfw["hour"].value_counts().sort_index()
        t = Table(show_header=True, box=SIMPLE_HEAD, header_style="bold yellow")
        t.add_column("北京时间")
        t.add_column("发推数量", justify="right")
        max_v = int(hr_counts.max()) if not hr_counts.empty else 0
        for h in range(24):
            val = int(hr_counts.get(h, 0))
            ratio = val / max_v if max_v else 0
            if ratio > 0.75:
                color = "bold red"
            elif ratio > 0.4:
                color = "yellow"
            elif ratio > 0.1:
                color = "cyan"
            else:
                color = "grey39"
            t.add_row(f"{h:02d}:00–{(h+1)%24:02d}:00", f"[{color}]{val}[/]")
        console.print(t)



# ------------------------ 主菜单 ------------------------
def list_clean_files() -> List[str]:
    """列出所有清洗输出文件（按时间倒序使用方便的最新优先）。"""
    return sorted(glob.glob(f"{OUTPUT_PREFIX}_*.csv"), reverse=True)


def run_analysis_menu():
    """
    展示 clean 文件列表并进入分析子菜单，允许用户选择基础概览或高级分析。
    使用循环以便在完成一次分析后继续操作，直到用户返回主菜单。
    """
    files = list_clean_files()
    if not files:
        console.print("[red]未找到清洗后的文件，请先执行【1 数据清洗】！[/red]")
        return

    console.print("\n📂 可用清洗文件（新→旧）：")
    tb = Table(show_header=True, header_style="bold magenta")
    tb.add_column("编号", justify="center", width=6)
    tb.add_column("文件名", overflow="fold")
    tb.add_column("最后修改时间", justify="right")
    for i, f in enumerate(files, 1):
        mtime = datetime.fromtimestamp(os.path.getmtime(f)).strftime("%Y-%m-%d %H:%M:%S")
        tb.add_row(str(i), os.path.basename(f), mtime)
    console.print(tb)

    idx = Prompt.ask("请输入要分析的文件编号", choices=[str(i) for i in range(1, len(files)+1)])
    selected_file = files[int(idx)-1]

    while True:
        console.print(Panel.fit("📊 分析功能\n1) 基础概览\n2) 高级自定义分析（N 个月：周/小时/周×小时）\n3) 返回主菜单", border_style="blue"))
        c = Prompt.ask("请选择", choices=["1","2","3"])
        if c == "1":
            for _ in track(range(2), description="🔍 生成概览…"):
                pass
            basic_overview(selected_file)
        elif c == "2":
            advanced_analysis(selected_file)
        else:
            break


def main_menu():
    """
    顶层入口：提供“清洗/分析/退出”三个选项，并调用对应流程。
    通过进度条提示关键步骤的执行感，提升 CLI 体验。
    """
    while True:
        console.print(Panel.fit("[bold cyan]Elon Musk 推文数据工具[/bold cyan]\n1️⃣ 数据清洗\n2️⃣ 数据分析\n3️⃣ 退出", border_style="cyan"))
        choice = Prompt.ask("请选择操作", choices=["1","2","3"])
        if choice == "1":
            for _ in track(range(3), description="🧹 正在清洗…"):
                pass
            run_cleaning()
        elif choice == "2":
            run_analysis_menu()
        else:
            console.print("[yellow]👋 再见！[/yellow]")
            break


if __name__ == "__main__":
    main_menu()
