import pandas as pd
import os
import glob
import datetime

# --- 配置区 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HISTORY_FILE = os.path.join(BASE_DIR, 'data', 'holdings_time_series.csv')

def get_latest_snapshot():
    """🌟 智能搜索：自动在 data 目录下寻找日期最晚的那份快照"""
    search_pattern = os.path.join(BASE_DIR, 'data', 'master_holdings_20*.csv')
    all_snapshots = glob.glob(search_pattern)
    
    if not all_snapshots:
        # 如果找不到带日期的，最后尝试找一下 latest 副本
        latest_fallback = os.path.join(BASE_DIR, 'data', 'master_holdings_latest.csv')
        if os.path.exists(latest_fallback):
            return latest_fallback
        return None
    
    # 按文件名排序，取最后一个（日期最新）
    all_snapshots.sort(reverse=True)
    return all_snapshots[0]

def update_history():
    print("📈 正在更新历史趋势台账...")
    
    # 🌟 动态获取今天生成的最新文件路径
    input_file = get_latest_snapshot()
    
    if not input_file:
        print("❌ 错误：找不到任何 master_holdings 开头的分析文件，请先运行 step3。")
        return

    print(f"📂 正在读取数据源: {os.path.basename(input_file)}")
    
    # 1. 读取数据
    df_today = pd.read_csv(input_file)
    
    # 2. 提取 Record_Date (我们以文件名的日期或今天作为记录日)
    # 尝试从文件名提取日期，如果提取不到则用今天
    try:
        record_date = os.path.basename(input_file).split('_')[-1].replace('.csv', '')
        # 简单校验格式是否为 YYYY-MM-DD
        datetime.datetime.strptime(record_date, '%Y-%m-%d')
    except:
        record_date = datetime.datetime.now().strftime('%Y-%m-%d')
    
    # 3. 提取核心列并去重
    today_snapshot = df_today[['Holding_Ticker', 'Total_Market_Shares']].drop_duplicates().copy()
    today_snapshot['Record_Date'] = record_date
    today_snapshot = today_snapshot[['Record_Date', 'Holding_Ticker', 'Total_Market_Shares']]

    # 4. 读写追加逻辑
    if os.path.exists(HISTORY_FILE):
        df_history = pd.read_csv(HISTORY_FILE)
        
        # 🛡️ 防重检查
        if record_date in df_history['Record_Date'].astype(str).values:
            print(f"⚠️ 日期 {record_date} 的快照已在历史台账中，跳过追加。")
            return
            
        df_final = pd.concat([df_history, today_snapshot], ignore_index=True)
    else:
        df_final = today_snapshot

    # 5. 落地保存
    df_final.to_csv(HISTORY_FILE, index=False, encoding='utf-8-sig')
    print(f"✅ 历史台账已更新！日期：{record_date}，当前库内共 {len(df_final)} 条记录。")

if __name__ == "__main__":
    update_history()