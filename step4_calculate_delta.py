import pandas as pd
import os
import glob
from datetime import datetime

DATA_DIR = 'data'

def calculate_delta():
    print("🔍 启动增量追踪器：正在寻找最近两天的机构持仓快照...")
    
    # 自动获取 data 文件夹下所有的汇总表，并按时间排序
    files = sorted(glob.glob(os.path.join(DATA_DIR, 'Master_Holdings_*.csv')))
    
    if len(files) < 2:
        print("⚠️ 警告：需要至少两天的 Master_Holdings 汇总表才能计算增量！")
        print("💡 提示：请保留今天的汇总表作为基准，等下个交易日再运行前三步生成新表后，即可使用本功能。")
        return

    # 选取最近的两天（最新的是 T 日，次新的是 T-1 日）
    file_old = files[-2]
    file_new = files[-1]
    
    date_old = file_old.split('_')[-1].replace('.csv', '')
    date_new = file_new.split('_')[-1].replace('.csv', '')
    
    print(f"📅 对比基准: [{date_old}] vs [{date_new}]")
    
    # 读取这两天的数据
    df_old = pd.read_csv(file_old)
    df_new = pd.read_csv(file_new)
    
    # 将 Ticker 设为索引，方便做减法
    df_old = df_old.set_index('ticker')
    df_new = df_new.set_index('ticker')
    
    # 找出所有的股票代码（旧表和新表的并集）
    all_tickers = df_old.index.union(df_new.index)
    
    report_data = []
    
    print("🧮 正在计算每只股票的机构加减仓动作...")
    for ticker in all_tickers:
        # 获取旧数据（如果旧表没有，说明是全新建仓，旧数据为0）
        shares_old = df_old.at[ticker, 'Total_Shares'] if ticker in df_old.index else 0
        etfs_old = df_old.at[ticker, 'ETF_Count'] if ticker in df_old.index else 0
        
        # 获取新数据（如果新表没有，说明被全部清仓，新数据为0）
        shares_new = df_new.at[ticker, 'Total_Shares'] if ticker in df_new.index else 0
        etfs_new = df_new.at[ticker, 'ETF_Count'] if ticker in df_new.index else 0
        
        # 核心逻辑：算差值
        delta_shares = shares_new - shares_old
        delta_etfs = etfs_new - etfs_old
        
        # 计算变化百分比 (排除基数为0的除息错误)
        if shares_old > 0:
            pct_change = (delta_shares / shares_old) * 100
        else:
            pct_change = 100.0 # 全新建仓视为 100% 增长
            
        report_data.append({
            'Ticker': ticker,
            'Old_Shares': shares_old,
            'New_Shares': shares_new,
            'Delta_Shares': delta_shares,
            'Delta_Pct(%)': round(pct_change, 2),
            'Old_ETF_Count': etfs_old,
            'New_ETF_Count': etfs_new,
            'Delta_ETFs': delta_etfs
        })
        
    delta_df = pd.DataFrame(report_data)
    
    # 剔除那些持股没有任何变化（Delta为0）的股票，只看有动作的
    delta_df = delta_df[delta_df['Delta_Shares'] != 0]
    
    if delta_df.empty:
        print("🤷‍♂️ 两天的数据完全一致，机构没有任何调仓动作。")
        return

    # 按机构净买入量从大到小排序
    delta_df = delta_df.sort_values(by='Delta_Shares', ascending=False)
    
    # 保存结果表
    output_filename = f"Delta_Report_{date_old}_to_{date_new}.csv"
    output_path = os.path.join(DATA_DIR, output_filename)
    delta_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print("-" * 50)
    print(f"🎉 增量计算完成！已剔除无变化股票，发现 {len(delta_df)} 只股票发生异动。")
    print(f"📊 调仓报告已生成: {output_path}")
    print("-" * 50)

    # 打印前 10 大净买入 和 前 10 大净卖出
    print(f"\n🔥 【机构一致看多：净买入榜 Top 10】")
    top_buys = delta_df.head(10).copy()
    top_buys['Delta_Shares'] = top_buys['Delta_Shares'].apply(lambda x: f"+{int(x):,}")
    print(top_buys[['Ticker', 'Delta_Shares', 'Delta_Pct(%)', 'Delta_ETFs']].to_string(index=False))
    
    print(f"\n❄️ 【机构形成分歧/出逃：净卖出榜 Top 10】")
    top_sells = delta_df.tail(10).sort_values(by='Delta_Shares', ascending=True).copy()
    top_sells['Delta_Shares'] = top_sells['Delta_Shares'].apply(lambda x: f"{int(x):,}")
    print(top_sells[['Ticker', 'Delta_Shares', 'Delta_Pct(%)', 'Delta_ETFs']].to_string(index=False))

if __name__ == "__main__":
    calculate_delta()