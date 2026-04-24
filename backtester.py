import asyncio
import aiohttp
import sqlite3
import time
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("backtester")

CST = timezone(timedelta(hours=8))
DB_PATH = Path(__file__).parent / "accumulation.db"
FAPI = "https://fapi.binance.com"

def get_signals():
    """從資料庫獲取歷史發出的放量訊號"""
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    # 建立回測結果表
    c.execute("""CREATE TABLE IF NOT EXISTS backtest_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id INTEGER,
        symbol TEXT,
        signal_time TEXT,
        entry_price REAL,
        max_price_7d REAL,
        max_profit_7d REAL,
        UNIQUE(signal_id)
    )""")
    conn.commit()
    
    # 抓取未回測過的訊號
    c.execute("""
        SELECT h.id, h.symbol, h.change_time, h.price 
        FROM state_history h
        LEFT JOIN backtest_results b ON h.id = b.signal_id
        WHERE h.new_status LIKE '%放量%' AND b.id IS NULL
    """)
    signals = c.fetchall()
    conn.close()
    return signals

async def fetch_future_klines(session, symbol, start_ts_ms):
    """獲取訊號發出後的未來 K 線 (1小時K，最多取 168 根 = 7天)"""
    url = f"{FAPI}/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": "1h",
        "startTime": start_ts_ms,
        "limit": 168
    }
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            if resp.status == 200:
                return await resp.json()
    except Exception as e:
        log.error(f"API 錯誤 {symbol}: {e}")
    return None

async def run_backtest():
    signals = get_signals()
    if not signals:
        log.info("🎯 目前沒有需要回測的新訊號（可能資料庫還沒累積歷史訊號）。")
        return

    log.info(f"🔍 發現 {len(signals)} 個未回測的放量訊號，開始計算後續 7 天最大潛在利潤...")
    
    results = []
    async with aiohttp.ClientSession() as session:
        for sid, symbol, change_time, entry_price in signals:
            try:
                # 解析時間 (假設格式為 'YYYY-MM-DD HH:MM')
                dt = datetime.strptime(change_time, "%Y-%m-%d %H:%M").replace(tzinfo=CST)
                start_ts_ms = int(dt.timestamp() * 1000)
            except ValueError:
                continue
                
            klines = await fetch_future_klines(session, symbol, start_ts_ms)
            if klines and len(klines) > 0:
                max_high = max(float(k[2]) for k in klines)
                max_profit_pct = ((max_high - entry_price) / entry_price) * 100 if entry_price > 0 else 0
                results.append((sid, symbol, change_time, entry_price, max_high, max_profit_pct))
            
            await asyncio.sleep(0.1) # 避免觸發限速
            
    if results:
        conn = sqlite3.connect(str(DB_PATH))
        c = conn.cursor()
        c.executemany("""
            INSERT OR REPLACE INTO backtest_results 
            (signal_id, symbol, signal_time, entry_price, max_price_7d, max_profit_7d)
            VALUES (?, ?, ?, ?, ?, ?)
        """, results)
        conn.commit()
        conn.close()
        
        log.info(f"✅ 回測完成，已將 {len(results)} 筆結果寫入 backtest_results 資料表。")
        for r in results:
            log.info(f"  - {r[1]} 在 {r[2]} 訊號價 ${r[3]:.4f} -> 7日內最高價 ${r[4]:.4f} (最高利潤: +{r[5]:.1f}%)")

if __name__ == "__main__":
    asyncio.run(run_backtest())
