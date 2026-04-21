import pandas as pd
import os
import glob
import datetime

# --- 配置区 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 历史台账文件名 [cite: 1]
HISTORY_FILE = os.path.join(BASE_DIR, 'data', 'holdings_time_series.csv')

def get_latest_snapshot():
    """🌟 智能搜索：自动在 data 目录下寻找日期最晚的那份快照 [cite: 1, 2]"""
    search_pattern = os.path.join(BASE_DIR, 'data', 'master_holdings_20*.csv')
    all_snapshots = glob.glob(search_pattern)
    if not all_snapshots: return None
    all_snapshots.sort(reverse=True)
    return all_snapshots[0]

def update_history():
    print("📈 正在更新历史趋势总账 (全市场聚合版)...")
    
    input_file = get_latest_snapshot()
    if not input_file:
        print("❌ 错误：找不到任何数据源，请先运行前面的步骤。")
        return

    print(f"📂 正在处理数据源: {os.path.basename(input_file)}")
    
    # 1. 读取底表 (包含了所有 ETF 的持仓明细和价格)
    df_today = pd.read_csv(input_file)
    
    # 2. 提取 Record_Date [cite: 3]
    try:
        record_date = os.path.basename(input_file).split('_')[-1].replace('.csv', '')
        datetime.datetime.strptime(record_date, '%Y-%m-%d')
    except:
        record_date = datetime.datetime.now().strftime('%Y-%m-%d')

    # ================= 🌟 核心修改：聚合与字段保留 =================
    # 根据你的截图要求，我们需要：
    # - 聚合：按 Holding_Ticker 分组，Shares(持股数) 求和
    # - 保留：Price, Open, High, Low, Volume, VWAP
    
    # 定义聚合规则
    # 注意：持股数求和得到 Total_Market_Shares，行情数据对同一股票是一样的，取第一个即可
    agg_rules = {
        'Shares': 'sum',
        'Price': 'first',
        'Open': 'first',
        'High': 'first',
        'Low': 'first',
        'Volume': 'first',
        'VWAP': 'first'
    }
    
    # 过滤掉底表中可能不存在的列
    actual_agg = {k: v for k, v in agg_rules.items() if k in df_today.columns}
    
    # 执行聚合
    df_agg = df_today.groupby('Holding_Ticker').agg(actual_agg).reset_index()
    
    # 重命名列名以匹配你的截图 [cite: 3]
    df_agg = df_agg.rename(columns={
        'Holding_Ticker': 'Holding_T',
        'Shares': 'Total_Market_Shares'
    })
    
    # 注入日期
    df_agg['Record_Date'] = record_date
    
    # 整理最终列顺序 (严格匹配你的截图顺序)
    final_cols = [
        'Record_Date', 'Holding_T', 'Total_Market_Shares', 
        'Price', 'Open', 'High', 'Low', 'Volume', 'VWAP'
    ]
    
    # 只提取存在的列
    df_final_today = df_agg[[col for col in final_cols if col in df_agg.columns]].copy()
    # =========================================================

    # 3. 读写追加与“去重覆盖”逻辑 [cite: 4, 5]
    if os.path.exists(HISTORY_FILE):
        df_history = pd.read_csv(HISTORY_FILE)
        
        # 将历史表和今天的新聚合表拼接
        df_combined = pd.concat([df_history, df_final_today], ignore_index=True)
        
        # 🌟 关键：按 [日期 + 股票代码] 去重，保留最后一次的数据 (keep='last')
        # 这样即使一天运行多次，也只会保留最后一次的最新聚合结果
        df_combined = df_combined.drop_duplicates(
            subset=['Record_Date', 'Holding_T'], 
            keep='last'
        )
    else:
        df_combined = df_final_today

    # 4. 落地保存
    df_combined.to_csv(HISTORY_FILE, index=False, encoding='utf-8-sig')
    print(f"✅ 全市场历史台账已更新！日期：{record_date}，共记录 {len(df_final_today)} 只标的总量。")

if __name__ == "__main__":
    update_history()