import pandas as pd
import os
import numpy as np
import glob

# --- 配置区 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MERGED_DIR = os.path.join(BASE_DIR, 'data', 'merged')
REPORT_DIR = os.path.join(BASE_DIR, 'reports')

os.makedirs(REPORT_DIR, exist_ok=True)

def get_latest_two_files():
    """自动扫描 merged 文件夹，找到日期最近的两个总表"""
    # 查找所有形如 master_holdings_YYYYMMDD.csv 的文件
    files = glob.glob(os.path.join(MERGED_DIR, 'master_holdings_*.csv'))
    
    # 按照文件名（即日期）从小到大排序
    files.sort() 
    
    if len(files) < 2:
        return None, None
    
    # 倒数第二个是昨天(T-1)，最后一个是今天(T)
    return files[-2], files[-1] 

def calculate_implied_price(df):
    """计算隐含持仓价，规避除以 0 的情况"""
    # 确保列是数值型
    df['Market_Value'] = pd.to_numeric(df['Market_Value'], errors='coerce')
    df['Shares'] = pd.to_numeric(df['Shares'], errors='coerce')
    
    df['Implied_Price'] = np.where(df['Shares'] > 0, df['Market_Value'] / df['Shares'], 0)
    return df

def analyze_price_volume():
    # 1. 自动获取 T-1 和 T 日的文件路径
    file_t1, file_t = get_latest_two_files()
    
    if not file_t1 or not file_t:
        print("⚠️ 警告：在 data/merged/ 目录下找到的总表不足 2 份！")
        print("💡 请确保你至少有两天的 master_holdings_YYYYMMDD.csv 文件才能进行量价对比。")
        return

    print("⚖️ 启动 T vs T+1 机构量价追踪器...")
    print(f"📅 正在对比底稿：")
    print(f"   [T-1 昨收]: {os.path.basename(file_t1)}")
    print(f"   [T   今日]: {os.path.basename(file_t)}")

    # 2. 读取并计算隐含价格
    df_t1 = calculate_implied_price(pd.read_csv(file_t1, encoding='utf-8-sig'))
    df_t = calculate_implied_price(pd.read_csv(file_t, encoding='utf-8-sig'))

    # 只提取需要的核心列进行对比
    cols = ['ETF_Ticker', 'Holding_Ticker', 'Shares', 'Implied_Price']
    merged = pd.merge(df_t1[cols], df_t[cols], on=['ETF_Ticker', 'Holding_Ticker'], suffixes=('_T1', '_T'))

    # 3. 计算量的变化 (份额增减) 和 价的变化 (隐含价格变动)
    merged['Delta_Shares'] = merged['Shares_T'] - merged['Shares_T1']
    merged['Delta_Price_Pct'] = np.where(merged['Implied_Price_T1'] > 0, 
                                        (merged['Implied_Price_T'] - merged['Implied_Price_T1']) / merged['Implied_Price_T1'], 0)

    # 4. 归类量价关系
    conditions = [
        (merged['Delta_Shares'] > 0) & (merged['Delta_Price_Pct'] > 0), # 量增价涨：机构抢筹推升
        (merged['Delta_Shares'] > 0) & (merged['Delta_Price_Pct'] < 0), # 量增价跌：机构越跌越买 (左侧建仓)
        (merged['Delta_Shares'] < 0) & (merged['Delta_Price_Pct'] > 0), # 量缩价涨：机构逢高派发 (落袋为安)
        (merged['Delta_Shares'] < 0) & (merged['Delta_Price_Pct'] < 0)  # 量缩价跌：机构恐慌砸盘
    ]
    choices = ['1_机构抢筹推升', '2_机构左侧建仓(越跌越买)', '3_机构逢高派发', '4_机构恐慌砸盘']
    merged['Flow_Signal'] = np.select(conditions, choices, default='5_无显著变化')

    # 5. 过滤出真正有动作的记录 (份额变化不为0)并按净申赎份额降序排列
    action_df = merged[merged['Delta_Shares'] != 0].sort_values(by='Delta_Shares', ascending=False)
    
    # 提取 T 日的日期后缀，用于命名生成的报告
    t_date = os.path.basename(file_t).split('_')[-1].replace('.csv', '')
    output_path = os.path.join(REPORT_DIR, f'Report_量价异动追踪_{t_date}.csv')
    
    action_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n✨ 量价分析完成！共发现 {len(action_df)} 条机构异动记录。")
    print(f"📁 报告已保存至: {output_path}")
    print("💡 建议：重点查看【2_机构左侧建仓】的标的，这通常暗示着机构在逆势吸收廉价筹码。")

if __name__ == "__main__":
    analyze_price_volume()

# --- 在 step6 的末尾增加个股汇总功能 ---

def aggregate_stock_flows(action_df, t_date):
    print("汇总全市场个股合力动向...")
    
    # 按 Holding_Ticker 聚合，计算所有 ETF 对该股的持仓变动总和
    stock_summary = action_df.groupby('Holding_Ticker').agg(
        Total_Net_Shares=('Delta_Shares', 'sum'),           # 全市场净增减股数
        ETF_Count_Changed=('ETF_Ticker', 'nunique'),        # 有多少只 ETF 动了它
        Avg_Price_Change_Pct=('Delta_Price_Pct', 'mean')    # 平均隐含价变动
    ).reset_index()

    # 排序：看到底哪只股票被机构集体“疯抢”或“抛弃”
    stock_summary = stock_summary.sort_values(by='Total_Net_Shares', ascending=False)
    
    output_path = os.path.join(REPORT_DIR, f'Report_全市场个股合力榜_{t_date}.csv')
    stock_summary.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"✅ 个股合力榜已生成：{output_path}")