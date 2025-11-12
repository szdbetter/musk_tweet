import os
import re
import glob
import pandas as pd
from datetime import datetime, timedelta
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt
from rich.progress import track
from rich.panel import Panel

console = Console()

# ==================== 配置 ====================
INPUT_FILE = "elonmusk.csv"
OUTPUT_PREFIX = "elonmusk_clean"
ENCODING = "utf-8"
START_YEAR = 2024
MONTH_ORDER = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
# =================================================


def next_output_name(prefix: str) -> str:
    exists = sorted(glob.glob(f"{prefix}_*.csv"))
    if not exists:
        return f"{prefix}_001.csv"
    nums = [int(re.search(r'_(\d{3})\.csv$', f).group(1)) for f in exists if re.search(r'_(\d{3})\.csv$', f)]
    return f"{prefix}_{max(nums)+1:03d}.csv" if nums else f"{prefix}_001.csv"


def coalesce_records(lines):
    recs, buf = [], []
    for raw in lines:
        line = raw.rstrip("\n\r")
        if re.match(r'^\s*"?(\d{18,19})"?,', line):
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
    s = s.replace('","', ', ')
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1]
    s = s.replace('""', '"')
    return re.sub(r'\s+', ' ', s).strip()


def parse_record(rec: str):
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


def assign_years(parsed_records):
    """根据月份顺序推断年份 + 自动计算北京时间"""
    year = START_YEAR
    month_idx_prev = None
    results = []
    all_years = set()

    for rid, content, date_info in parsed_records:
        mon = date_info["mon"]
        if mon not in MONTH_ORDER:
            continue
        idx = MONTH_ORDER.index(mon)
        if month_idx_prev is not None and idx < month_idx_prev:
            year += 1
            console.log(f"📆 检测到跨年：{MONTH_ORDER[month_idx_prev]}→{mon}，切换到 {year}")
        month_idx_prev = idx
        all_years.add(year)

        day = int(date_info["day"])
        time_str = date_info["time"]
        ampm = date_info["ampm"]
        tz = date_info["tz"]

        t = datetime.strptime(time_str + " " + ampm, "%I:%M:%S %p")
        edt_dt = datetime(year, idx + 1, day, t.hour, t.minute, t.second)
        bj_dt = edt_dt + timedelta(hours=12)
        date_fmt = f"{year:04d}-{idx+1:02d}-{day:02d} {t.strftime('%H:%M:%S')} {tz}"
        bj_time = bj_dt.strftime("%Y-%m-%d %H:%M:%S CST")

        results.append((rid, content, date_fmt, bj_time))
    return results, all_years


# ========== 清洗 ==========
def run_cleaning():
    if not os.path.exists(INPUT_FILE):
        console.print(f"[red]❌ 未找到源文件：{INPUT_FILE}[/red]")
        return

    out_name = next_output_name(OUTPUT_PREFIX)
    console.print(f"\n🚀 开始清洗文件：{INPUT_FILE}\n")
    with open(INPUT_FILE, "r", encoding=ENCODING, errors="ignore") as f:
        raw_lines = f.readlines()
    total_lines = len(raw_lines)
    if total_lines == 0:
        console.print("[red]❌ 文件为空[/red]")
        return

    header = raw_lines[0].strip()
    coalesced = coalesce_records(raw_lines[1:])
    parsed_records = [parse_record(rec) for rec in coalesced if parse_record(rec)]
    rows, all_years = assign_years(parsed_records)

    header_parts = header.split(",")
    if len(header_parts) >= 3:
        header_parts[2] = '"EDT时间"'
    header = ",".join(header_parts) + ',"北京时间"'

    with open(out_name, "w", encoding="utf-8", newline="") as out:
        out.write(header + "\n")
        for tw_id, content, dt, bj_time in rows:
            esc = lambda x: '"' + x.replace('"', '""') + '"'
            out.write(f'{esc(tw_id)},{esc(content)},{esc(dt)},{esc(bj_time)}\n')

    cleaned = len(rows)
    removed = max(total_lines - 1 - cleaned, 0)
    console.print(Panel.fit(f"""
📦 原始文件：{os.path.basename(INPUT_FILE)}（共 {total_lines} 行）
🧹 清洗后：{os.path.basename(out_name)}（共 {cleaned} 条推文）
⚙️ 清理掉 {removed} 条无效/碎行
📅 共检测到 {len(all_years)} 年数据：{min(all_years)}–{max(all_years)}
✅ [bold green]清洗完成！（含北京时间 + 年份推断）[/bold green]
""", title="清洗报告", border_style="green"))


# ========== 分析 ==========
def run_analysis():
    files = sorted(glob.glob(f"{OUTPUT_PREFIX}_*.csv"), reverse=True)
    if not files:
        console.print("[red]未找到清洗后的文件，请先执行清洗模式！[/red]")
        return

    console.print("\n📂 可用清洗文件：")
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("编号", justify="center")
    table.add_column("文件名")
    table.add_column("最后修改时间", justify="right")

    for i, f in enumerate(files, 1):
        mtime = datetime.fromtimestamp(os.path.getmtime(f)).strftime("%Y-%m-%d %H:%M:%S")
        table.add_row(str(i), os.path.basename(f), mtime)
    console.print(table)

    idx = Prompt.ask("\n请输入要分析的文件编号", choices=[str(i) for i in range(1, len(files)+1)])
    selected_file = files[int(idx)-1]
    console.print(f"\n📊 正在分析：{selected_file}\n")

    df = pd.read_csv(selected_file)
    df["datetime_bj"] = pd.to_datetime(df["北京时间"], format="%Y-%m-%d %H:%M:%S", errors="coerce")

    # 自动检测推文内容列（通常是第二列）
    content_col = df.columns[1]
    df["hour"] = df["datetime_bj"].dt.hour
    df["weekday"] = df["datetime_bj"].dt.day_name()
    df["month"] = df["datetime_bj"].dt.month_name()

    # === 月份维度 ===
    console.print(Panel.fit("📆 按月份统计发推数量", border_style="cyan"))
    month_table = df["month"].value_counts().reindex(MONTH_ORDER).dropna()
    if month_table.empty:
        console.print("[yellow]⚠️ 没有检测到月份数据[/yellow]")
    else:
        t_month = Table(show_header=True, header_style="bold blue")
        t_month.add_column("月份")
        t_month.add_column("发推数量", justify="right")
        for m, c in month_table.items():
            t_month.add_row(m, str(int(c)))
        console.print(t_month)

    # === 周几维度 ===
    console.print(Panel.fit("📅 周一至周日发推数量（按月份汇总）", border_style="magenta"))
    df["month_num"] = df["datetime_bj"].dt.month
    pivot = pd.pivot_table(df, index="month_num", columns="weekday", values="hour", aggfunc="count", fill_value=0)
    if not pivot.empty:
        pivot = pivot.reindex(columns=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]).fillna(0)
        t_week = Table(show_header=True, header_style="bold cyan")
        t_week.add_column("月份")
        for c in pivot.columns:
            t_week.add_column(c, justify="right")
        for m in pivot.index:
            row = [MONTH_ORDER[m-1]] + [str(int(v)) for v in pivot.loc[m]]
            t_week.add_row(*row)
        console.print(t_week)
    else:
        console.print("[yellow]⚠️ 无法生成星期分布表[/yellow]")

    # === 小时维度 ===
    console.print(Panel.fit("🕒 每小时（北京时间）发推数量", border_style="green"))
    hour_table = df["hour"].value_counts().sort_index()
    if hour_table.empty:
        console.print("[yellow]⚠️ 无小时分布数据[/yellow]")
    else:
        t_hour = Table(show_header=True, header_style="bold yellow")
        t_hour.add_column("北京时间")
        t_hour.add_column("发推数量", justify="right")
        for h, c in hour_table.items():
            t_hour.add_row(f"{h:02d}:00–{(h+1)%24:02d}:00", str(int(c)))
        console.print(t_hour)

    # === 原创/转推占比 ===
    df["is_rt"] = df[content_col].astype(str).str.startswith("RT @")
    rt_ratio = df["is_rt"].mean() if len(df) > 0 else 0
    console.print(f"\n💬 转推占比：{rt_ratio:.2%}，原创占比：{1-rt_ratio:.2%}")
    console.print(Panel.fit("[green]✅ 分析完成，可以根据时间规律制定下注策略！[/green]", title="分析报告", border_style="green"))


# ========== 主菜单 ==========
def main_menu():
    while True:
        console.print(Panel.fit("[bold cyan]Elon Musk 推文数据工具[/bold cyan]\n1️⃣ 数据清洗\n2️⃣ 数据分析\n3️⃣ 退出\n", border_style="cyan"))
        choice = Prompt.ask("请选择操作", choices=["1", "2", "3"])
        if choice == "1":
            for _ in track(range(3), description="🚀 正在清洗数据..."):
                pass
            run_cleaning()
        elif choice == "2":
            for _ in track(range(2), description="📊 正在载入分析模块..."):
                pass
            run_analysis()
        elif choice == "3":
            console.print("[yellow]👋 程序已退出。再见！[/yellow]")
            break


if __name__ == "__main__":
    main_menu()
