import streamlit as st
import sqlite3
import pandas as pd
from pathlib import Path

# === 設定 ===
st.set_page_config(page_title="莊家收籌雷達", page_icon="🏦", layout="wide")
DB_PATH = Path(__file__).parent / "accumulation.db"

# === 資料載入 ===
@st.cache_data(ttl=60)
def load_data():
    try:
        conn = sqlite3.connect(str(DB_PATH))
        df_watchlist = pd.read_sql("SELECT * FROM watchlist", conn)
        df_history = pd.read_sql("SELECT * FROM state_history ORDER BY id DESC LIMIT 100", conn)
        
        # Check if backtest_results exists
        try:
            df_backtest = pd.read_sql("SELECT * FROM backtest_results ORDER BY id DESC", conn)
        except:
            df_backtest = pd.DataFrame()
            
        conn.close()
        return df_watchlist, df_history, df_backtest
    except Exception as e:
        st.error(f"無法讀取資料庫: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

df_watchlist, df_history, df_backtest = load_data()

# === 頁首 ===
st.title("🏦 莊家收籌雷達即時看板")
st.markdown("本看板每 60 秒自動快取最新掃描結果，呈現全市場正在被主力吸籌的標的。")

# === 核心數據 ===
st.markdown("### 📊 全市場掃描概況")
col1, col2, col3, col4 = st.columns(4)

firing = len(df_watchlist[df_watchlist["status"] == "🔥放量啟動"]) if not df_watchlist.empty else 0
warming = len(df_watchlist[df_watchlist["status"] == "⚡開始放量"]) if not df_watchlist.empty else 0
sleeping = len(df_watchlist[df_watchlist["status"] == "💤收籌中"]) if not df_watchlist.empty else 0
total = len(df_watchlist) if not df_watchlist.empty else 0

col1.metric("🔥 放量啟動標的", firing)
col2.metric("⚡ 開始放量標的", warming)
col3.metric("💤 收籌中標的", sleeping)
col4.metric("總監控標的數", total)

st.divider()

# === 頁籤導覽 ===
tab1, tab2 = st.tabs(["📊 即時雷達與標的池", "📈 策略回測報告"])

with tab1:
    # === 排行榜與標的池 ===
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.markdown("### 🎯 目前潛伏標的池")
        if not df_watchlist.empty:
            # 過濾顯示欄位
            df_disp = df_watchlist[["coin", "status", "score", "sideways_days", "range_pct", "avg_vol", "current_price"]].copy()
            
            # 資料格式化
            df_disp["avg_vol"] = df_disp["avg_vol"].apply(lambda x: f"${x/1e6:.1f}M" if x >= 1e6 else f"${x/1e3:.0f}K")
            df_disp["score"] = df_disp["score"].round(1)
            df_disp["range_pct"] = df_disp["range_pct"].round(1).astype(str) + "%"
            
            df_disp.sort_values("score", ascending=False, inplace=True)
            df_disp.columns = ["幣種", "狀態", "綜合評分", "橫盤天數", "價格波動", "日均成交量", "當前價格"]
            
            st.dataframe(
                df_disp,
                use_container_width=True,
                height=500
            )
        else:
            st.info("標的池目前為空。")
    
    with col_right:
        st.markdown("### 🔔 最近狀態升級紀錄")
        if not df_history.empty:
            df_hist_disp = df_history[["change_time", "coin", "old_status", "new_status", "price"]].copy()
            df_hist_disp.columns = ["變更時間", "幣種", "原狀態", "新狀態", "當時價格"]
            st.dataframe(df_hist_disp, use_container_width=True, height=500)
        else:
            st.info("目前尚無狀態變更紀錄。")

with tab2:
    st.markdown("### 📈 放量訊號 7日回測勝率")
    st.markdown("追蹤過去被雷達判定為「放量啟動」或「開始放量」的標的，計算其後續 7 天內的最高潛在利潤（Max Favorable Excursion）。")
    
    if not df_backtest.empty:
        df_bt_disp = df_backtest[["signal_time", "symbol", "entry_price", "max_price_7d", "max_profit_7d"]].copy()
        
        # 簡單計算勝率 (最大潛在利潤 > 5% 視為成功)
        success_rate = len(df_bt_disp[df_bt_disp["max_profit_7d"] > 5]) / len(df_bt_disp) * 100
        avg_profit = df_bt_disp["max_profit_7d"].mean()
        
        st.info(f"**回測統計**：共掃描了 {len(df_bt_disp)} 次訊號 | 訊號後 7日內最高漲幅 > 5% 的比例為 **{success_rate:.1f}%** | 平均最大潛在利潤：**+{avg_profit:.1f}%**")
        
        df_bt_disp["max_profit_7d"] = df_bt_disp["max_profit_7d"].apply(lambda x: f"+{x:.1f}%")
        df_bt_disp.columns = ["訊號時間", "幣種", "進場價格", "7日內最高價", "最高潛在利潤"]
        st.dataframe(df_bt_disp, use_container_width=True, height=500)
    else:
        st.warning("目前資料庫中尚未累積足夠的歷史訊號，或尚未執行回測引擎 (`python backtester.py`)。")
