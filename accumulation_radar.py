#!/usr/bin/env python3
"""
莊家收籌雷達 v2 — 發現莊家橫盤吸籌 + OI多重時間框架分析 + 非同步加速

核心邏輯：
1. 莊家拉盤前必須先收籌 → 長期橫盤+低量 = 收籌中
2. OI暴漲 = 大資金進場建倉 = 即將拉盤
3. 多時間框架驗證 = 過濾短線噪音，抓主升浪

最佳化 (v2):
- 使用 asyncio + aiohttp 進行高併發全市場掃描 (提速10x)
- 引入 1h / 4h / 24h OI 趨勢驗證
- 新增 state_history 歷史狀態變更追蹤
"""

import asyncio
import aiohttp
import contextlib
import json
import logging
import os
import sys
import time
import sqlite3
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

# === 日誌 ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("accumulation_radar_v2")

# === 載入 .env ===
env_file = Path(__file__).parent / ".env.oi"
if env_file.exists():
    with open(env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

# === 常量 ===
CST = timezone(timedelta(hours=8))
EXCLUDE_COINS = {"USDC", "USDP", "TUSD", "FDUSD", "BTCDOM", "DEFI", "USDM"}

# === 配置 ===
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
FAPI = "https://fapi.binance.com"
DB_PATH = Path(__file__).parent / "accumulation.db"

import yaml

# === 載入 config.yaml ===
config_file = Path(__file__).parent / "config.yaml"
try:
    with open(config_file, "r", encoding="utf-8") as f:
        CONF = yaml.safe_load(f)
except Exception as e:
    log.warning(f"無法讀取 config.yaml，使用預設參數: {e}")
    CONF = {}

STRAT_CONF = CONF.get("strategy", {})
SYS_CONF = CONF.get("system", {})
HEALTH_CONF = CONF.get("health", {})

# 策略參數
MIN_SIDEWAYS_DAYS = STRAT_CONF.get("min_sideways_days", 45)
MAX_RANGE_PCT = STRAT_CONF.get("max_range_pct", 80)
MAX_AVG_VOL_USD = STRAT_CONF.get("max_avg_vol_usd", 20_000_000)
MIN_DATA_DAYS = STRAT_CONF.get("min_data_days", 50)
VOL_BREAKOUT_MULT = STRAT_CONF.get("vol_breakout_mult", 3.0)

# 全域性令牌桶限速器 (Binance 權重限制)
_weight_used = 0
_weight_reset_at = 0
_weight_lock = None
_sem = None  # 併發數控制

# 健康監控狀態
api_fail_count = 0
MAX_API_FAILS = HEALTH_CONF.get("max_api_fails", 10)
ENABLE_ALERTS = HEALTH_CONF.get("enable_alerts", True)
BINANCE_WEIGHT_LIMIT = SYS_CONF.get("binance_weight_limit", 1100)
MAX_CONCURRENT_TASKS = SYS_CONF.get("max_concurrent_tasks", 50)


async def api_get(session, endpoint, params=None, weight=2):
    """非同步API請求（嚴格遵循 Binance 權重限制）"""
    global _weight_used, _weight_reset_at, api_fail_count
    
    while True:
        async with _weight_lock:
            now = time.time()
            if now >= _weight_reset_at:
                _weight_used = 0
                _weight_reset_at = now + 60
            
            if _weight_used + weight > BINANCE_WEIGHT_LIMIT:
                sleep_time = _weight_reset_at - now
            else:
                _weight_used += weight
                sleep_time = 0
                
        if sleep_time > 0:
            log.warning(f"接近限速({_weight_used}/{BINANCE_WEIGHT_LIMIT}), 等待 {sleep_time:.1f}s")
            await asyncio.sleep(sleep_time + 0.1)
            continue
        break
        
    url = endpoint if endpoint.startswith("http") else f"{FAPI}{endpoint}"
    
    for attempt in range(3):
        try:
            async with session.get(url, params=params, timeout=10) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 3 * (attempt + 1)))
                    log.warning(f"429限速! {endpoint} 等待 {retry_after}s")
                    await asyncio.sleep(retry_after)
                elif resp.status == 418:
                    log.error(f"418 IP被封禁! {endpoint} 冷卻60s")
                    await asyncio.sleep(60)
                else:
                    log.warning(f"API HTTP {resp.status}: {endpoint}")
                    return None
        except asyncio.TimeoutError:
            log.warning(f"API 超時 {endpoint} (attempt {attempt+1})")
            await asyncio.sleep(1)
        except Exception as e:
            log.warning(f"API 失敗 {endpoint}: {e} (attempt {attempt+1})")
            await asyncio.sleep(1)
            
    log.error(f"API 3次重試失敗: {endpoint}")
    api_fail_count += 1
    return None


def init_db():
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS watchlist (
        symbol TEXT PRIMARY KEY,
        coin TEXT,
        added_date TEXT,
        sideways_days INT,
        range_pct REAL,
        avg_vol REAL,
        low_price REAL,
        high_price REAL,
        current_price REAL,
        score REAL,
        status TEXT DEFAULT 'watching',
        last_oi_alert TEXT,
        notes TEXT
    )""")
    # 歷史狀態追溯表
    c.execute("""CREATE TABLE IF NOT EXISTS state_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        coin TEXT,
        old_status TEXT,
        new_status TEXT,
        change_time TEXT,
        price REAL,
        score REAL
    )""")
    conn.commit()
    return conn


async def get_all_perp_symbols(session):
    info = await api_get(session, "/fapi/v1/exchangeInfo", weight=1)
    if not info: return []
    return [s["symbol"] for s in info["symbols"]
            if s["quoteAsset"] == "USDT" 
            and s["contractType"] == "PERPETUAL"
            and s["status"] == "TRADING"]


def analyze_accumulation(symbol, klines):
    if len(klines) < MIN_DATA_DAYS: return None
    data = []
    for k in klines:
        data.append({
            "ts": k[0], "open": float(k[1]), "high": float(k[2]),
            "low": float(k[3]), "close": float(k[4]), "vol": float(k[7])
        })
    
    coin = symbol.removesuffix("USDT")
    if coin in EXCLUDE_COINS: return None
    
    recent_7d = data[-7:]
    prior = data[:-7]
    if not prior: return None
    
    recent_avg_px = sum(d["close"] for d in recent_7d) / len(recent_7d)
    prior_avg_px = sum(d["close"] for d in prior) / len(prior)
    if prior_avg_px > 0 and ((recent_avg_px - prior_avg_px) / prior_avg_px) > 3.0:
        return None
    
    best_sideways = 0
    best_range = 0
    best_low = 0
    best_high = 0
    best_avg_vol = 0
    best_slope_pct = 0
    
    for window in range(MIN_SIDEWAYS_DAYS, len(prior) + 1):
        window_data = prior[-window:]
        lows = [d["low"] for d in window_data]
        highs = [d["high"] for d in window_data]
        w_low = min(lows)
        w_high = max(highs)
        
        if w_low <= 0: continue
        range_pct = ((w_high - w_low) / w_low) * 100
        
        if range_pct <= MAX_RANGE_PCT:
            avg_vol = sum(d["vol"] for d in window_data) / len(window_data)
            if avg_vol <= MAX_AVG_VOL_USD:
                closes = [d["close"] for d in window_data]
                n = len(closes)
                x_mean = (n - 1) / 2.0
                y_mean = sum(closes) / n
                den = sum((i - x_mean) ** 2 for i in range(n))
                slope = sum((i - x_mean) * (c - y_mean) for i, c in enumerate(closes)) / den if den > 0 else 0
                slope_pct = (slope * n / closes[0] * 100) if closes[0] > 0 else 0
                
                if abs(slope_pct) > 20: continue
                
                if window > best_sideways:
                    best_sideways = window
                    best_range = range_pct
                    best_low = w_low
                    best_high = w_high
                    best_avg_vol = avg_vol
                    best_slope_pct = slope_pct
                    
    if best_sideways < MIN_SIDEWAYS_DAYS: return None
    
    days_score = min(best_sideways / 90, 1.0) * 25
    range_score = max(0, (1 - best_range / MAX_RANGE_PCT)) * 20
    vol_score = max(0, (1 - best_avg_vol / MAX_AVG_VOL_USD)) * 20
    
    recent_vol = sum(d["vol"] for d in recent_7d) / len(recent_7d)
    vol_breakout = recent_vol / best_avg_vol if best_avg_vol > 0 else 0
    breakout_score = min(vol_breakout / VOL_BREAKOUT_MULT, 1.0) * 15
    
    est_mcap = data[-1]["close"] * best_avg_vol * 30
    if est_mcap > 0 and est_mcap < 50_000_000: mcap_score = 20
    elif est_mcap < 100_000_000: mcap_score = 15
    elif est_mcap < 200_000_000: mcap_score = 10
    elif est_mcap < 500_000_000: mcap_score = 5
    else: mcap_score = 0
    
    total_score = days_score + range_score + vol_score + breakout_score + mcap_score
    flatness_bonus = max(0, (1 - abs(best_slope_pct) / 20)) * 5
    total_score += flatness_bonus
    
    if vol_breakout >= VOL_BREAKOUT_MULT: status = "🔥放量啟動"
    elif vol_breakout >= 1.5: status = "⚡開始放量"
    else: status = "💤收籌中"
    
    return {
        "symbol": symbol, "coin": coin, "sideways_days": best_sideways,
        "range_pct": best_range, "slope_pct": best_slope_pct,
        "low_price": best_low, "high_price": best_high,
        "avg_vol": best_avg_vol, "current_price": data[-1]["close"],
        "recent_vol": recent_vol, "vol_breakout": vol_breakout,
        "score": total_score, "status": status, "data_days": len(data),
    }


async def fetch_and_analyze(session, sym):
    """併發抓取K線並分析收籌"""
    async with _sem:
        # klines limit=180 weight 約等於 2
        klines = await api_get(session, "/fapi/v1/klines", {"symbol": sym, "interval": "1d", "limit": 180}, weight=2)
        if klines and isinstance(klines, list):
            return analyze_accumulation(sym, klines)
        return None


async def scan_accumulation_pool(session):
    log.info("📊 併發掃描全市場收籌標的...")
    start_time = time.time()
    
    symbols = await get_all_perp_symbols(session)
    log.info(f"  共 {len(symbols)} 個合約，開始分配併發任務...")
    
    tasks = [fetch_and_analyze(session, sym) for sym in symbols]
    
    results = []
    completed = 0
    for coro in asyncio.as_completed(tasks):
        r = await coro
        if r:
            results.append(r)
        completed += 1
        if completed % 100 == 0:
            log.info(f"  進度: {completed}/{len(symbols)}... 已發現{len(results)}個")
            
    results.sort(key=lambda x: x["score"], reverse=True)
    cost = time.time() - start_time
    log.info(f"  ✅ 發現 {len(results)} 個標的 (耗時 {cost:.1f}s)")
    return results


def format_usd(v):
    if v >= 1e9: return f"${v/1e9:.1f}B"
    if v >= 1e6: return f"${v/1e6:.1f}M"
    if v >= 1e3: return f"${v/1e3:.0f}K"
    return f"${v:.0f}"


def build_pool_report(results, state_changes):
    if not results: return ""
    now = datetime.now(CST)
    lines = [
        f"🏦 **莊家收籌雷達 v2** — 標的池更新",
        f"⏰ {now.strftime('%Y-%m-%d %H:%M')} CST",
        f"━━━━━━━━━━━━━━━━━━",
        f"耗時極速掃描發現 {len(results)} 個潛伏標的：",
        "",
    ]
    
    # 狀態變更提醒 (非常重要)
    if state_changes:
        lines.append(f"🔔 **狀態升級提醒 (State Changes)**")
        for sc in state_changes:
            sym, coin, old_s, new_s, dt, px, score = sc
            lines.append(f"  🚨 **{coin}** 狀態升級: {old_s} ➡️ {new_s} (當前價: ${px:.5f})")
        lines.append("")
    
    firing = [r for r in results if "放量啟動" in r["status"]]
    warming = [r for r in results if "開始放量" in r["status"]]
    sleeping = [r for r in results if "收籌中" in r["status"]]
    
    if firing:
        lines.append(f"🔥 **放量啟動** ({len(firing)}個) — 最高優先順序！")
        for r in firing[:10]:
            lines.append(
                f"  🔥 **{r['coin']}** | 分:{r['score']:.0f} | "
                f"橫盤{r['sideways_days']}天 | 波動{r['range_pct']:.0f}% | "
                f"Vol放大{r['vol_breakout']:.1f}x"
            )
            lines.append(
                f"     ${r['current_price']:.6f} | "
                f"區間: ${r['low_price']:.6f}~${r['high_price']:.6f} | "
                f"均量: {format_usd(r['avg_vol'])}"
            )
        lines.append("")
    
    if warming:
        lines.append(f"⚡ **開始放量** ({len(warming)}個)")
        for r in warming[:8]:
            lines.append(f"  ⚡ {r['coin']} | 分:{r['score']:.0f} | 橫盤{r['sideways_days']}天 | Vol{r['vol_breakout']:.1f}x")
        lines.append("")
    
    if sleeping:
        lines.append(f"💤 **收籌中** ({len(sleeping)}個)")
        for r in sleeping[:12]:
            lines.append(f"  💤 {r['coin']} | 分:{r['score']:.0f} | 橫盤{r['sideways_days']}天 | Vol {format_usd(r['avg_vol'])}")
            
    return "\n".join(lines)


async def send_telegram(session, text):
    if not TG_BOT_TOKEN:
        log.info(f"[TG] No token, stdout:\n{text}")
        return
    
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    
    html_text = text.replace("**", "<b>", 1)
    while "**" in html_text:
        html_text = html_text.replace("**", "</b>", 1)
        if "**" in html_text:
            html_text = html_text.replace("**", "<b>", 1)
            
    chunks = []
    current = ""
    for line in html_text.split("\n"):
        if len(current) + len(line) + 1 > 3800:
            chunks.append(current)
            current = line
        else:
            current += "\n" + line if current else line
    if current:
        chunks.append(current)
        
    for chunk in chunks:
        try:
            async with session.post(url, json={
                "chat_id": TG_CHAT_ID,
                "text": chunk,
                "parse_mode": "HTML"
            }, timeout=10) as resp:
                if resp.status == 200:
                    log.info(f"[TG] Sent ✓ ({len(chunk)} chars)")
                else:
                    plain = re.sub(r'<[^>]+>', '', chunk)
                    await session.post(url, json={"chat_id": TG_CHAT_ID, "text": plain}, timeout=10)
                    log.info("[TG] Sent plain fallback")
        except Exception as e:
            log.error(f"[TG] Error: {e}")
        await asyncio.sleep(0.5)


async def send_discord(session, text):
    """傳送訊息至 Discord Webhook"""
    if not DISCORD_WEBHOOK_URL:
        return
        
    # Discord 的 Markdown 中不能隨便亂用 html 標籤，我們把原先為了 TG 加的 <b> 標籤等過濾掉
    discord_text = text.replace("<b>", "**").replace("</b>", "**").replace("<i>", "*").replace("</i>", "*")
    
    # Discord 每則訊息上限 2000 字元
    chunks = []
    current = ""
    for line in discord_text.split("\n"):
        if len(current) + len(line) + 1 > 1900:
            chunks.append(current)
            current = line
        else:
            current += "\n" + line if current else line
    if current:
        chunks.append(current)
        
    for chunk in chunks:
        try:
            async with session.post(DISCORD_WEBHOOK_URL, json={"content": chunk}, timeout=10) as resp:
                if resp.status in (200, 204):
                    log.info(f"[Discord] Sent ✓ ({len(chunk)} chars)")
                else:
                    log.warning(f"[Discord] Error HTTP {resp.status}")
        except Exception as e:
            log.error(f"[Discord] Error: {e}")
        await asyncio.sleep(0.5)


def save_watchlist(conn, results):
    c = conn.cursor()
    now = datetime.now(CST).strftime("%Y-%m-%d %H:%M")
    
    c.execute("SELECT symbol, status FROM watchlist")
    old_map = {row[0]: row[1] for row in c.fetchall()}
    
    state_changes = []
    
    for r in results:
        sym = r["symbol"]
        new_status = r["status"]
        old_status = old_map.get(sym)
        
        # 記錄狀態升級 (例如：收籌中 -> 開始放量)
        if old_status and old_status != new_status:
            # 只有從低階升到高階才特別提醒 (包含'放量'關鍵字)
            if "放量" in new_status and new_status != old_status:
                state_changes.append((sym, r["coin"], old_status, new_status, now, r["current_price"], r["score"]))
                
        c.execute("""INSERT OR REPLACE INTO watchlist 
            (symbol, coin, added_date, sideways_days, range_pct, avg_vol, 
             low_price, high_price, current_price, score, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (sym, r["coin"], now, r["sideways_days"], r["range_pct"],
             r["avg_vol"], r["low_price"], r["high_price"], r["current_price"],
             r["score"], new_status))
             
    if state_changes:
        c.executemany("""INSERT INTO state_history 
            (symbol, coin, old_status, new_status, change_time, price, score)
            VALUES (?, ?, ?, ?, ?, ?, ?)""", state_changes)
            
    conn.commit()
    log.info(f"  💾 儲存 {len(results)} 個標的, 記錄 {len(state_changes)} 條狀態變更")
    return state_changes


async def fetch_oi_multi_timeframe(session, sym):
    """抓取單個幣種的多時間框架 OI"""
    async with _sem:
        # limit=26 取最近 25 小時的資料 (1+25=26)
        oi_hist = await api_get(session, "/futures/data/openInterestHist", {"symbol": sym, "period": "1h", "limit": 26}, weight=1)
        if oi_hist and len(oi_hist) >= 2:
            curr = float(oi_hist[-1]["sumOpenInterestValue"])
            prev_1h = float(oi_hist[-2]["sumOpenInterestValue"])
            
            # 4h (取索引 -5, 如果資料不夠就取最前面的)
            idx_4h = -5 if len(oi_hist) >= 5 else 0
            prev_4h = float(oi_hist[idx_4h]["sumOpenInterestValue"])
            
            # 24h
            idx_24h = -25 if len(oi_hist) >= 25 else 0
            prev_24h = float(oi_hist[idx_24h]["sumOpenInterestValue"])
            
            d1h = ((curr - prev_1h) / prev_1h * 100) if prev_1h > 0 else 0
            d4h = ((curr - prev_4h) / prev_4h * 100) if prev_4h > 0 else 0
            d24h = ((curr - prev_24h) / prev_24h * 100) if prev_24h > 0 else 0
            
            circ = float(oi_hist[-1].get("CMCCirculatingSupply", 0))
            return sym, {"oi_usd": curr, "d1h": d1h, "d4h": d4h, "d24h": d24h, "circ_supply": circ}
        return sym, None


async def main_async():
    global _weight_lock, _sem, api_fail_count
    _weight_lock = asyncio.Lock()
    _sem = asyncio.Semaphore(MAX_CONCURRENT_TASKS)  # 最大併發連線數
    api_fail_count = 0
    
    mode = sys.argv[1] if len(sys.argv) > 1 else "full"
    log.info(f"🏦 莊家收籌雷達 v2 (Async) — {datetime.now(CST).strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"   模式: {mode}")
    
    with contextlib.closing(init_db()) as conn:
        async with aiohttp.ClientSession() as session:
            
            if mode in ("full", "pool"):
                results = await scan_accumulation_pool(session)
                if results:
                    state_changes = save_watchlist(conn, results)
                    report = build_pool_report(results, state_changes)
                    if report:
                        await asyncio.gather(
                            send_telegram(session, report),
                            send_discord(session, report)
                        )
                        
            if mode in ("full", "oi"):
                c = conn.cursor()
                c.execute("SELECT symbol FROM watchlist WHERE status != 'removed'")
                watchlist = [row[0] for row in c.fetchall()]
                
                if not watchlist:
                    log.warning("⚠️ 標的池為空")
                    return
                    
                log.info("📡 併發獲取全市場費率與行情...")
                start_oi_time = time.time()
                
                # 併發請求高權重全市場介面
                t_task = api_get(session, "/fapi/v1/ticker/24hr", weight=40)
                p_task = api_get(session, "/fapi/v1/premiumIndex", weight=1)
                cg_task = api_get(session, "https://api.coingecko.com/api/v3/search/trending", weight=1)
                mcap_task = api_get(session, "https://www.binance.com/bapi/composite/v1/public/marketing/symbol/list", weight=1)
                
                tickers_raw, premiums_raw, cg_raw, mcap_raw = await asyncio.gather(t_task, p_task, cg_task, mcap_task)
                
                if not tickers_raw or not premiums_raw:
                    log.error("❌ 行情或費率 API 失敗")
                    return
                    
                ticker_map = {t["symbol"]: {"px_chg": float(t["priceChangePercent"]), "vol": float(t["quoteVolume"]), "price": float(t["lastPrice"])} 
                              for t in tickers_raw if t["symbol"].endswith("USDT")}
                funding_map = {p["symbol"]: float(p["lastFundingRate"]) 
                               for p in premiums_raw if p["symbol"].endswith("USDT")}
                               
                mcap_map = {}
                if mcap_raw and "data" in mcap_raw:
                    for item in mcap_raw["data"]:
                        if item.get("name") and item.get("marketCap"):
                            mcap_map[item["name"]] = float(item["marketCap"])
                
                cg_trending = set()
                heat_map = {}
                if cg_raw and "coins" in cg_raw:
                    for item in cg_raw["coins"]:
                        sym = item["item"]["symbol"].upper()
                        rank = item["item"].get("score", 99)
                        cg_trending.add(sym)
                        heat_map[sym] = max(50 - rank * 3, 10)
                        
                log.info(f"✅ 拉取完成: CG({len(cg_trending)}), Mcap({len(mcap_map)})")
                
                # 成交量暴增快速判定 (5d 均值)
                vol_surge_coins = set()
                # 我們只對交易量大於 $20M 的幣去併發查 klines 來判定放量
                surge_check_tasks = []
                
                async def check_vol_surge(sym, tk_vol):
                    async with _sem:
                        kl = await api_get(session, "/fapi/v1/klines", {"symbol": sym, "interval": "1d", "limit": 6}, weight=1)
                        if kl and len(kl) >= 5:
                            avg_5d = sum(float(k[7]) for k in kl[:-1]) / (len(kl)-1)
                            if avg_5d > 0 and tk_vol / avg_5d >= 2.5:
                                return sym.removesuffix("USDT"), (tk_vol / avg_5d)
                    return None, 0
                    
                for sym, tk in ticker_map.items():
                    if tk["vol"] > 20_000_000:
                        surge_check_tasks.append(check_vol_surge(sym, tk["vol"]))
                        
                log.info(f"🔍 檢查 {len(surge_check_tasks)} 個高交易量幣種是否放量...")
                surge_results = await asyncio.gather(*surge_check_tasks)
                for coin, ratio in surge_results:
                    if coin:
                        vol_surge_coins.add(coin)
                        heat_map[coin] = heat_map.get(coin, 0) + min(ratio * 10, 50)
                        
                dual_heat = cg_trending & vol_surge_coins
                for coin in dual_heat:
                    heat_map[coin] = heat_map.get(coin, 0) + 20
                
                c.execute("SELECT symbol, score, sideways_days, range_pct, avg_vol, status FROM watchlist")
                pool_map = {row[0]: {"pool_score": row[1], "sideways_days": row[2], "status": row[5]} for row in c.fetchall()}
                
                scan_syms = set()
                for sym, pd in pool_map.items():
                    if "放量" in pd.get("status", "") or "開始" in pd.get("status", ""):
                        scan_syms.add(sym)
                for sym, _ in sorted(ticker_map.items(), key=lambda x: x[1]["vol"], reverse=True)[:100]:
                    scan_syms.add(sym)
                    
                log.info(f"🔥 併發掃描 {len(scan_syms)} 個重點合約的多時間框架 OI...")
                oi_tasks = [fetch_oi_multi_timeframe(session, sym) for sym in scan_syms]
                oi_results = await asyncio.gather(*oi_tasks)
                oi_map = {sym: data for sym, data in oi_results if data}
                
                all_syms = set(list(pool_map.keys()) + list(oi_map.keys()))
                coin_data = {}
                
                for sym in all_syms:
                    tk = ticker_map.get(sym, {})
                    if not tk: continue
                    pool = pool_map.get(sym)
                    oi = oi_map.get(sym, {})
                    fr = funding_map.get(sym, 0)
                    coin = sym.removesuffix("USDT")
                    
                    est_mcap = mcap_map.get(coin, max(tk["vol"] * 0.3, oi.get("oi_usd", 0) * 2))
                    
                    coin_data[sym] = {
                        "coin": coin, "sym": sym, "px_chg": tk["px_chg"], "vol": tk["vol"],
                        "fr_pct": fr * 100, "oi_usd": oi.get("oi_usd", 0),
                        "d1h": oi.get("d1h", 0), "d4h": oi.get("d4h", 0), "d24h": oi.get("d24h", 0),
                        "est_mcap": est_mcap, "sw_days": pool["sideways_days"] if pool else 0,
                        "in_pool": bool(pool), "heat": heat_map.get(coin, 0),
                        "in_cg": coin in cg_trending, "vol_surge": coin in vol_surge_coins
                    }
                    
                # 策略: 綜合多時間框架
                ambush = []
                for sym, d in coin_data.items():
                    if not d["in_pool"] or d["px_chg"] > 50: continue
                    
                    mc = d["est_mcap"]
                    m_sc = 35 if mc < 50e6 else 32 if mc < 100e6 else 28 if mc < 150e6 else 25 if mc < 200e6 else 20 if mc < 300e6 else 0
                    
                    # OI多時間框架聯合判斷 (24h在漲 + 1h/4h加速)
                    d24, d4, d1 = d["d24h"], d["d4h"], d["d1h"]
                    o_sc = 0
                    if d24 > 10 and d1 > 3: o_sc = 30     # 長期向上且短期加速
                    elif d24 > 5 and d1 > 2: o_sc = 25
                    elif d4 > 5 and d1 > 2: o_sc = 20
                    elif d1 > 3: o_sc = 15
                    elif d24 > 5: o_sc = 10
                    
                    if d1 > 2 and abs(d["px_chg"]) < 5: o_sc = min(o_sc + 5, 30) # 暗流
                    
                    sw = d["sw_days"]
                    s_sc = 20 if sw >= 120 else 17 if sw >= 90 else 14 if sw >= 75 else 10 if sw >= 60 else 6 if sw >= 45 else 0
                    
                    fr = d["fr_pct"]
                    f_sc = 15 if fr < -0.1 else 12 if fr < -0.05 else 9 if fr < -0.03 else 6 if fr < -0.01 else 0
                    
                    total = m_sc + o_sc + s_sc + f_sc
                    if total >= 20:
                        ambush.append({**d, "total": total, "m_sc": m_sc, "o_sc": o_sc, "s_sc": s_sc, "f_sc": f_sc})
                        
                ambush.sort(key=lambda x: x["total"], reverse=True)
                
                # Report generation
                def mcap_str(v):
                    return f"${v/1e6:.0f}M" if v >= 1e6 else f"${v/1e3:.0f}K" if v >= 1e3 else f"${v:.0f}"
                
                now = datetime.now(CST)
                lines = [f"🏦 **莊家雷達** 多時間框架 OI", f"⏰ {now.strftime('%Y-%m-%d %H:%M')} CST"]
                
                hot_coins = sorted([d for d in coin_data.values() if d["heat"] > 0], key=lambda x: x["heat"], reverse=True)
                if hot_coins:
                    lines.append(f"\n🔥 **熱度榜**")
                    for s in hot_coins[:8]:
                        tags = []
                        if s["in_cg"]: tags.append("🌐CG")
                        if s["vol_surge"]: tags.append("📈放量")
                        if s["d24h"] > 5 or s["d1h"] > 2: tags.append(f"⚡OI[1h{s['d1h']:+.0f}%|24h{s['d24h']:+.0f}%]")
                        if s["in_pool"]: tags.append(f"💤池{s['sw_days']}天")
                        lines.append(f"  {s['coin']:<8} ~{mcap_str(s['est_mcap'])} 漲{s['px_chg']:+.0f}% | {' '.join(tags)}")
                
                lines.append(f"\n🎯 **埋伏策略** (綜合)")
                for s in ambush[:10]:
                    tags = [f"~{mcap_str(s['est_mcap'])}"]
                    if s["d1h"] > 2 or s["d24h"] > 5: tags.append(f"OI(1h:{s['d1h']:+.0f}% 24h:{s['d24h']:+.0f}%)")
                    if s["d1h"] > 2 and abs(s["px_chg"]) < 5: tags.append("🎯暗流")
                    if s["sw_days"] >= 45: tags.append(f"橫盤{s['sw_days']}天")
                    lines.append(f"  {s['coin']:<7} {s['total']}分 | {' '.join(tags)}")
                
                await asyncio.gather(
                    send_telegram(session, "\n".join(lines)),
                    send_discord(session, "\n".join(lines))
                )
                
                oi_cost = time.time() - start_oi_time
                log.info(f"✅ OI 監控掃描完成 (耗時 {oi_cost:.1f}s)")

            # 健康狀態檢查
            if ENABLE_ALERTS and api_fail_count > MAX_API_FAILS:
                alert_msg = f"⚠️ **系統健康警報** ⚠️\n本次掃描發生了 {api_fail_count} 次 API 失敗，超過容忍值 ({MAX_API_FAILS})！請檢查網路狀態或幣安 API 限制。"
                await asyncio.gather(
                    send_telegram(session, alert_msg),
                    send_discord(session, alert_msg)
                )
                log.error(f"健康狀態異常: {api_fail_count} 次失敗")
    
    log.info("✅ 指令碼執行結束")

if __name__ == "__main__":
    asyncio.run(main_async())
