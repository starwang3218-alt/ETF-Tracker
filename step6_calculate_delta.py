import pandas as pd
import os
import glob
import re

# --- 统一路径管理 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
REPORT_DIR = os.path.join(BASE_DIR, 'reports') 

# 确保文件夹存在
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

def run_delta_calculator():
    print("🚀 启动机构调仓对比引擎 (Delta Calculator)...")
    
    # 1. 自动寻找 data 文件夹下所有符合命名的快照
    file_pattern = os.path.join(DATA_DIR, 'master_holdings_*.csv')
    all_files = glob.glob(file_pattern)
    
    # 排除掉 analyzed 表，只留带日期的
    snapshot_files = [f for f in all_files if re.search(r'\d{4}-\d{2}-\d{2}', f)]
    
    # 按文件名（日期）进行降序排列
    snapshot_files.sort(reverse=True)

    if len(snapshot_files) < 2:
        print(f"❌ 错误：持仓历史记录不足（当前只有 {len(snapshot_files)} 个快照）。")
        print("提示：需要至少两个日期的 master_holdings_YYYY-MM-DD.csv 文件才能计算增量。")
        return

    # 锁定最近的两个日期
    today_file = snapshot_files[0]
    prev_file = snapshot_files[1]

    print(f"📈 正在对比：\n   最新表：{today_file}\n   上期表：{prev_file}")

    # 2. 读取数据并【聚合去重】（核心防爆逻辑）
    df_today = pd.read_csv(today_file)
    df_prev = pd.read_csv(prev_file)

    # 🚨 修复：只用真实存在的列作为主键
    group_cols = ['ETF_Ticker', 'Holding_Ticker']
    
    # 将同一只 ETF 里，可能分拆成好几行的同一只股票股数加总
    df_today_grouped = df_today.groupby(group_cols, dropna=False)['Shares'].sum().reset_index()
    df_prev_grouped = df_prev.groupby(group_cols, dropna=False)['Shares'].sum().reset_index()

    # 3. 执行对撞分析 (绝对的一对一)
    df_delta = pd.merge(
        df_today_grouped, 
        df_prev_grouped, 
        on=group_cols, 
        how='outer', 
        suffixes=('_Today', '_Prev')
    )

    # 4. 数据清洗与计算
    df_delta['Shares_Today'] = df_delta['Shares_Today'].fillna(0)
    df_delta['Shares_Prev'] = df_delta['Shares_Prev'].fillna(0)
    
    # 计算变动股数，并使用 round(4) 抹除计算机底层的浮点数微小误差
    df_delta['Delta_Shares'] = (df_delta['Shares_Today'] - df_delta['Shares_Prev']).round(4)
    
    # 只保留真正发生实质变动的记录
    df_delta = df_delta[df_delta['Delta_Shares'] != 0].copy()

    # ================= 补充最新股价计算金额 =================
    if 'Price' in df_today.columns:
        # 取每个股票的最新价格字典
        price_dict = df_today.drop_duplicates('Holding_Ticker').set_index('Holding_Ticker')['Price']
        # 映射回对撞报告中
        df_delta['Price'] = df_delta['Holding_Ticker'].map(price_dict)
        
        # 计算资金变动（如果有缺失价格，按0算）
        df_delta['Market_Value_Delta'] = df_delta['Delta_Shares'] * df_delta['Price'].fillna(0)
        # 按绝对金额（无论买卖，动作最大的放前面）排序
        df_delta = df_delta.reindex(df_delta['Market_Value_Delta'].abs().sort_values(ascending=False).index)

    # 5. 保存结果 (统一存到 reports 文件夹)
    output_path = os.path.join(REPORT_DIR, 'holdings_delta_report.csv')
    df_delta.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"✨ 成功！调仓报告已生成：{output_path}")
    print(f"📊 本次共发现 {len(df_delta)} 条持仓变动记录。")

if __name__ == "__main__":
    run_delta_calculator()
