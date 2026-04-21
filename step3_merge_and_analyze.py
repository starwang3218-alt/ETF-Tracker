import pandas as pd
import os
import glob
import numpy as np
import datetime  # 🌟 必须引入，用于生成日期文件名

# --- 配置区 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, 'data', 'cleaned')
REPORT_FILE = os.path.join(BASE_DIR, 'data', 'data_freshness_report.csv')

def merge_and_analyze():
    print("🚀 启动大表合并与数据贡献度分析引擎...")
    
    # 1. 获取所有清洗后的文件
    all_files = glob.glob(os.path.join(INPUT_DIR, '*_cleaned.csv'))
    if not all_files:
        print("❌ 错误：未找到清洗后的数据文件，请先运行 step2。")
        return

    # 2. 合并所有碎表
    df_list = []
    for f in all_files:
        try:
            temp_df = pd.read_csv(f)
            if not temp_df.empty:
                df_list.append(temp_df)
        except Exception:
            pass
    
    if not df_list:
        print("❌ 错误：所有 CSV 文件读取后均为空。")
        return
        
    # 🌟 在这里定义了 df！
    df = pd.concat(df_list, ignore_index=True)

    # 3. 🔗 映射日期
    try:
        if os.path.exists(REPORT_FILE):
            report_df = pd.read_csv(REPORT_FILE)
            date_map = dict(zip(report_df['ETF_Ticker'], report_df['File_Date']))
            df['As_Of_Date'] = df['ETF_Ticker'].map(date_map)
    except Exception as e:
        print(f"⚠️ 映射日期失败: {e}")

    # 4. 🧮 核心数据标准化
    df['Holding_Ticker'] = df['Holding_Ticker'].astype(str).str.strip().str.upper()
    df['Shares'] = pd.to_numeric(df['Shares'], errors='coerce').fillna(0)

    # 5. 🌟 终极防线：彻底斩断 0 股与无限大 (inf)
    df = df[(df['Shares'] > 0) & (df['Shares'] != np.inf)].copy()

    # 6. 计算贡献占比
    df['Total_Market_Shares'] = df.groupby('Holding_Ticker')['Shares'].transform('sum')
    df['Contribution_Ratio'] = df['Shares'] / df['Total_Market_Shares']

    # 7. 📅 计算交易日滞后天数
    today = pd.Timestamp('today').normalize()
    def calculate_bus_days(start_date):
        if pd.isna(start_date): return 999
        try:
            return np.busday_count(pd.to_datetime(start_date).date(), today.date())
        except:
            return 999

    df['Days_Lag'] = df['As_Of_Date'].apply(calculate_bus_days)
    
    # 8. 🚨 风险打标 (Data_Risk)
    df['Data_Risk'] = (df['Contribution_Ratio'] >= 0.05) & (df['Days_Lag'] >= 3)

    # 9. 整理列顺序
    cols = ['As_Of_Date', 'ETF_Ticker', 'Holding_Ticker'] + [c for c in df.columns if c not in ['As_Of_Date', 'ETF_Ticker', 'Holding_Ticker']]
    df = df[cols]

    # ================= 🌟 核心升级：双轨制保存逻辑 =================
    # 获取日期字符串 (如 2026-04-20)
    today_str = datetime.datetime.now().strftime('%Y-%m-%d')
    
    # 路径 A：带日期的历史快照（用于 Step 6 对比）
    date_file = os.path.join(BASE_DIR, 'data', f'master_holdings_{today_str}.csv')
    # 路径 B：固定名称的分析表（保持与旧代码兼容）
    latest_file = os.path.join(BASE_DIR, 'data', 'master_holdings_analyzed.csv')
    
    # 保存两个副本
    df.to_csv(date_file, index=False, encoding='utf-8-sig')
    df.to_csv(latest_file, index=False, encoding='utf-8-sig')
    
    risk_count = df['Data_Risk'].sum()
    print(f"🚨 质检扫描完毕：在 {len(df)} 行数据中，发现了 {risk_count} 条高风险数据！")
    print(f"✨ 历史快照已存: {date_file}")
    print(f"✨ 分析结果已更新: {latest_file}")

    # 10. 🔍 启动数据逻辑自检 (Sanity Check)
    check_series = df.groupby('Holding_Ticker')['Contribution_Ratio'].sum()
    errors = check_series[abs(check_series - 1) > 0.0001]
    
    if errors.empty:
        print(f"✅ 逻辑完美！{len(check_series)} 个标的的持仓占比之和均为 1 (100%)。")
    else:
        print(f"⚠️ 警告：发现 {len(errors)} 个标的的占比之和不等于 1。")

if __name__ == '__main__':
    merge_and_analyze()