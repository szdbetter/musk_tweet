// ==UserScript==
// @name         Polymarket Market & K-Line Exporter
// @namespace    https://tapmonkey.app/
// @version      1.1.0
// @description  Button-driven export of current event's sub-markets + K-line CSVs with progress panel
// @match        https://polymarket.com/*
// @grant        none
// ==/UserScript==

(function () {
  "use strict";

  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  const toBeijing = (utcString) => {
    const d = new Date(utcString);
    return new Date(d.getTime() + 8 * 3600 * 1000)
      .toISOString()
      .replace("T", " ")
      .replace("Z", "");
  };

  const downloadCSV = (filename, rows) => {
    const csv = rows.map((r) => r.join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    Object.assign(document.createElement("a"), { href: url, download: filename }).click();
    setTimeout(() => URL.revokeObjectURL(url), 5000);
  };

  const fetchMarkets = async (slug) => {
    const url = `https://gamma-api.polymarket.com/events/slug/${slug}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`获取 markets 失败: ${res.status}`);
    const json = await res.json();
    return json.markets ?? [];
  };

  const fetchKline = async (token, startTs, fidelity) => {
    const url = `https://clob.polymarket.com/prices-history?startTs=${startTs}&market=${token}&fidelity=${fidelity}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`获取 K 线失败: ${res.status}`);
    const json = await res.json();
    return json.history ?? [];
  };

  const createPanel = () => {
    const wrap = document.createElement("div");
    Object.assign(wrap.style, {
      position: "fixed",
      top: "12px",
      right: "12px",
      width: "320px",
      padding: "12px",
      borderRadius: "12px",
      background: "rgba(15,15,20,0.85)",
      color: "#fff",
      zIndex: 999999,
      boxShadow: "0 8px 20px rgba(0,0,0,0.4)",
      fontFamily: "SF Pro Display,Roboto,Arial,sans-serif",
      fontSize: "13px",
      lineHeight: "1.4",
      resize: "horizontal",
      overflow: "hidden",
    });
    wrap.dataset.collapsed = "false";

    let btn;
    let status;
    let summary;
    let tableWrap;

    const headerRow = document.createElement("div");
    Object.assign(headerRow.style, {
      display: "flex",
      justifyContent: "space-between",
      alignItems: "center",
      marginBottom: "8px",
    });
    const title = document.createElement("span");
    title.textContent = "Polymarket Exporter";
    title.style.fontWeight = "600";
    headerRow.appendChild(title);

    const toggle = document.createElement("button");
    toggle.textContent = "—";
    Object.assign(toggle.style, {
      border: "none",
      background: "transparent",
      color: "#fff",
      fontSize: "16px",
      cursor: "pointer",
      padding: "0 4px",
    });
    toggle.addEventListener("click", () => {
      const collapsed = wrap.dataset.collapsed === "true";
      wrap.dataset.collapsed = (!collapsed).toString();
      const hide = wrap.dataset.collapsed === "true";
      toggle.textContent = hide ? "+" : "—";
      [btn, status, summary, tableWrap].forEach((el) => {
        if (el) el.style.display = hide ? "none" : "";
      });
    });
    headerRow.appendChild(toggle);
    wrap.appendChild(headerRow);

    btn = document.createElement("button");
    btn.textContent = "获取当前事件数据";
    Object.assign(btn.style, {
      width: "100%",
      padding: "6px 12px",
      borderRadius: "8px",
      border: "none",
      cursor: "pointer",
      fontWeight: "600",
      background: "#3b82f6",
      color: "#fff",
      marginBottom: "10px",
    });
    wrap.appendChild(btn);

    status = document.createElement("div");
    status.style.minHeight = "60px";
    status.style.whiteSpace = "pre-wrap";
    status.textContent = "点击上方按钮开始抓取。";
    wrap.appendChild(status);

    summary = document.createElement("div");
    summary.style.fontSize = "12px";
    summary.style.color = "rgba(255,255,255,0.8)";
    summary.style.marginBottom = "6px";
    wrap.appendChild(summary);

    tableWrap = document.createElement("div");
    Object.assign(tableWrap.style, {
      maxHeight: "140px",
      overflow: "auto",
      marginTop: "8px",
      borderTop: "1px solid rgba(255,255,255,0.2)",
      paddingTop: "6px",
      fontSize: "12px",
    });
    wrap.appendChild(tableWrap);

    document.body.appendChild(wrap);
    return { btn, status, tableWrap, summary };
  };

  const panel = createPanel();

  const renderPreview = (tableWrap, marketRows) => {
    tableWrap.innerHTML = "";
    const tbl = document.createElement("table");
    tbl.style.width = "100%";
    tbl.style.borderCollapse = "collapse";

    const header = document.createElement("tr");
    ["Name", "Yes Token", "No Token", "Yes Price"].forEach((text) => {
      const th = document.createElement("th");
      th.textContent = text;
      Object.assign(th.style, {
        textAlign: "left",
        padding: "2px 4px",
        borderBottom: "1px solid rgba(255,255,255,0.2)",
        position: "sticky",
        top: "0",
        background: "rgba(15,15,20,0.85)",
      });
      header.appendChild(th);
    });
    tbl.appendChild(header);

    marketRows.slice(0, 8).forEach((row) => {
      const tr = document.createElement("tr");
      const cells = [
        row.name,
        row.yes_token ? `${row.yes_token.slice(0, 8)}…` : "—",
        row.no_token ? `${row.no_token.slice(0, 8)}…` : "—",
        row.yes_price,
      ];
      cells.forEach((text) => {
        const td = document.createElement("td");
        td.textContent = text;
        td.style.padding = "2px 4px";
        td.style.borderBottom = "1px solid rgba(255,255,255,0.1)";
        tr.appendChild(td);
      });
      tbl.appendChild(tr);
    });

    tableWrap.appendChild(tbl);
  };

  const exportData = async () => {
    const { btn, status, tableWrap, summary } = panel;
    try {
      btn.disabled = true;
      btn.textContent = "抓取中…";
      status.textContent = "准备获取事件 slug…";

      const slug = location.pathname.split("/").pop();
      if (!slug) throw new Error("无法识别事件 slug");
      status.textContent = `当前事件：${slug}\n请求 markets…`;

      const markets = await fetchMarkets(slug);
      if (!markets.length) throw new Error("未找到子市场");

      const marketRows = [
        [
          "name",
          "condition_id",
          "yes_token",
          "no_token",
          "yes_price",
          "no_price",
          "yes_volume",
          "no_volume",
          "total_volume",
          "updated",
        ],
      ];

      const marketInfo = markets.map((m) => {
        const [yes_token = "", no_token = ""] = JSON.parse(m.clobTokenIds || "[]");
        const yes_price = Number(m.bestAsk || m.lastTradePrice || 0);
        const no_price = Number(m.bestBid ? 1 - m.bestBid : 1 - yes_price);
        const volume = Number(m.volumeNum || 0);
        const name = m.groupItemTitle || m.question.match(/\d+-\d+/)?.[0] || "Unknown";

        marketRows.push([
          name,
          m.conditionId,
          yes_token,
          no_token,
          yes_price.toFixed(3),
          no_price.toFixed(3),
          volume,
          volume,
          volume,
          toBeijing(m.updatedAt),
        ]);

        return { name, condition_id: m.conditionId, yes_token, no_token, yes_price: yes_price.toFixed(3) };
      });

      downloadCSV(`markets_${slug}_${Date.now()}.csv`, marketRows);
      status.textContent = `✅ 市场导出完成：${marketInfo.length} 个\n开始抓取 K 线…`;

      renderPreview(tableWrap, marketInfo);

      const startTs = Math.floor(Date.now() / 1000) - 7 * 24 * 3600;
      const fidelity = 60;
      const klineRows = [["market_name", "market_id", "token_type", "token_id", "price", "timestamp"]];
      let totalKline = 0;

      const appendHistory = async (token, typeLabel, name, marketId) => {
        if (!token) return 0;
        status.textContent = `📈 ${name} (${typeLabel.toUpperCase()})…`;
        await sleep(150);
        const history = await fetchKline(token, startTs, fidelity);
        history.forEach(({ t, p }) =>
          klineRows.push([name, marketId, typeLabel, token, p, t])
        );
        return history.length;
      };

      for (const { name, condition_id, yes_token, no_token } of marketInfo) {
        const yesCount = await appendHistory(yes_token, "yes", name, condition_id);
        const noCount = await appendHistory(no_token, "no", name, condition_id);
        status.textContent = `📈 ${name}: YES ${yesCount || 0} 条 / NO ${noCount || 0} 条`;
        totalKline += (yesCount || 0) + (noCount || 0);
      }

      downloadCSV(`kline_${slug}_${Date.now()}.csv`, klineRows);
      status.textContent += `\n✅ K 线导出完成：${klineRows.length - 1} 条`;
      summary.textContent = `总市场：${marketInfo.length} 个 ｜ K 线记录：${totalKline}`;
    } catch (err) {
      console.error("导出失败:", err);
      status.textContent = `❌ 导出失败：${err.message}`;
      alert(`导出失败：${err.message}`);
    } finally {
      btn.disabled = false;
      btn.textContent = "获取当前事件数据";
    }
  };

  panel.btn.addEventListener("click", exportData);
})();
