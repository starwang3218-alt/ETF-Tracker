import pandas as pd
import os
import glob
import numpy as np

# --- 配置区 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, 'data', 'cleaned')
REPORT_FILE = os.path.join(BASE_DIR, 'data', 'data_freshness_report.csv')
OUTPUT_FILE = os.path.join(BASE_DIR, 'data', 'master_holdings_analyzed.csv')

def merge_and_analyze():
    print("🚀 启动大表合并与数据贡献度分析引擎...")
    
    all_files = glob.glob(os.path.join(INPUT_DIR, '*_cleaned.csv'))
    if not all_files: return

    df_list = []
    for f in all_files:
        try:
            df_list.append(pd.read_csv(f))
        except Exception:
            pass
    df = pd.concat(df_list, ignore_index=True)

    # 🔗 映射日期
    try:
        report_df = pd.read_csv(REPORT_FILE)
        date_map = dict(zip(report_df['ETF_Ticker'], report_df['File_Date']))
        df['As_Of_Date'] = df['ETF_Ticker'].map(date_map)
    except FileNotFoundError:
        pass

    # 🧮 核心数据标准化
    df['Holding_Ticker'] = df['Holding_Ticker'].astype(str).str.strip().str.upper()
    df['Shares'] = pd.to_numeric(df['Shares'], errors='coerce').fillna(0)

    # ================= 🌟 终极防线升级：同时斩断 0 股与无限大 (inf) =================
    df = df[(df['Shares'] > 0) & (df['Shares'] != np.inf)].copy()
    # =======================================================================

    df['Total_Market_Shares'] = df.groupby('Holding_Ticker')['Shares'].transform('sum')
    df['Contribution_Ratio'] = df['Shares'] / df['Total_Market_Shares']

    # 📅 计算交易日滞后天数
    today = pd.Timestamp('today').normalize()
    def calculate_bus_days(start_date):
        if pd.isna(start_date): return 999
        try:
            return np.busday_count(start_date.date(), today.date())
        except:
            return 999

    df['As_Of_Date_Parsed'] = pd.to_datetime(df['As_Of_Date'], errors='coerce')
    df['Days_Lag'] = df['As_Of_Date_Parsed'].apply(calculate_bus_days)
    
    # 🚨 风险阈值调整：从 >=2 放宽到 >=3，完美消除周二早上的周五数据误报
    df['Data_Risk'] = (df['Contribution_Ratio'] >= 0.05) & (df['Days_Lag'] >= 3)

    df.drop(columns=['As_Of_Date_Parsed'], inplace=True)
    cols = ['As_Of_Date', 'ETF_Ticker', 'Holding_Ticker'] + [c for c in df.columns if c not in ['As_Of_Date', 'ETF_Ticker', 'Holding_Ticker']]
    df = df[cols]

    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    
    risk_count = df['Data_Risk'].sum()
    print(f"🚨 质检扫描完毕：在 {len(df)} 行数据中，发现了 {risk_count} 条高风险数据！")
    print(f"✨ 分析大表生成完毕: {OUTPUT_FILE}")

if __name__ == '__main__':
    merge_and_analyze()
    
    print("\n🔍 启动数据逻辑自检 (Sanity Check)...")
    df_check = pd.read_csv(OUTPUT_FILE)
    check_series = df_check.groupby('Holding_Ticker')['Contribution_Ratio'].sum()
    
    errors = check_series[abs(check_series - 1) > 0.0001]
    
    if errors.empty:
        print(f"✅ 逻辑完美！{len(check_series)} 个有效标的的持仓占比之和均等于 1 (100%)。无懈可击！")
    else:
        print(f"⚠️ 警告：发现 {len(errors)} 个标的的占比之和不等于 1！")
        print(errors.head())