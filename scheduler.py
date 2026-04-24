import schedule
import time
import subprocess
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("scheduler")

CWD = Path(__file__).parent

def run_pool():
    log.info("⏰ 開始執行排程任務: accumulation_radar.py pool")
    try:
        subprocess.run(["python", "accumulation_radar.py", "pool"], cwd=str(CWD), check=True)
        log.info("✅ pool 任務執行完成")
    except Exception as e:
        log.error(f"❌ pool 任務執行失敗: {e}")

def run_oi():
    log.info("⏰ 開始執行排程任務: accumulation_radar.py oi")
    try:
        subprocess.run(["python", "accumulation_radar.py", "oi"], cwd=str(CWD), check=True)
        log.info("✅ oi 任務執行完成")
    except Exception as e:
        log.error(f"❌ oi 任務執行失敗: {e}")

def run_backtester():
    log.info("⏰ 開始執行排程任務: backtester.py")
    try:
        subprocess.run(["python", "backtester.py"], cwd=str(CWD), check=True)
        log.info("✅ backtester 任務執行完成")
    except Exception as e:
        log.error(f"❌ backtester 任務執行失敗: {e}")

# 設定排程
# pool 任務每天跑一次 (設定在 UTC+8 凌晨 00:00)
schedule.every().day.at("00:00").do(run_pool)

# oi 任務每小時跑一次 (設定在每小時的 05 分)
schedule.every().hour.at(":05").do(run_oi)

# 回測引擎每天跑一次 (設定在凌晨 00:30)
schedule.every().day.at("00:30").do(run_backtester)

if __name__ == "__main__":
    log.info("🚀 莊家收籌雷達自動排程器已啟動！")
    log.info("  - Pool 模式 (市場全量掃描) 將於每天 00:00 執行")
    log.info("  - OI 模式 (重點標的異動掃描) 將於每小時的 05 分執行")
    log.info("  - Backtester (回測引擎) 將於每天 00:30 執行")
    log.info("💡 提示: 請保持此視窗/進程開啟以維持排程運作。")
    
    # 初次啟動時，可以根據需求決定要不要先跑一次
    # run_pool()
    # run_oi()
    
    while True:
        schedule.run_pending()
        time.sleep(1)
