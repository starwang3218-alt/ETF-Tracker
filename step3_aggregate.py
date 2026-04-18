import pandas as pd
import os
from datetime import datetime

CLEANED_DIR = 'data/cleaned'
OUTPUT_DIR = 'data'

# ================= 核心黑名单 =================
# 剔除常见的法定货币、现金等价物和常见票据代码
BLACKLIST = {
    'USD', 'JPY', 'KRW', 'EUR', 'GBP', 'AUD', 'CAD', 'CHF', 'HKD', 'TWD', 
    'CASH', 'ZAR', 'SGD', 'INR', 'BRL', 'MXN', 'SEK', 'NOK', 'DKK', 'NZD',
    'JPFFT', 'XTSLA' # 把刚才发现的干扰项也加进来
}
# ==============================================

def generate_master_report():
    print("🚀 启动终极汇总：正在聚合 147 家机构的持仓底牌...")
    
    files = [f for f in os.listdir(CLEANED_DIR) if f.endswith('.csv')]
    if not files:
        print("❌ 找不到清洗后的文件，请确保 data/cleaned 文件夹有数据。")
        return

    all_data = []
    
    for filename in files:
        file_path = os.path.join(CLEANED_DIR, filename)
        etf_name = filename.replace('.csv', '')
        
        try:
            df = pd.read_csv(file_path)
            if 'ticker' in df.columns and 'shares' in df.columns:
                temp_df = df[['ticker', 'shares']].copy()
                temp_df['etf_name'] = etf_name
                all_data.append(temp_df)
        except Exception as e:
            pass

    if not all_data:
        print("❌ 没有有效数据可供合并。")
        return

    # 1. 合并超级大表
    master_df = pd.concat(all_data, ignore_index=True)
    master_df['ticker'] = master_df['ticker'].astype(str).str.upper().str.strip()

    # 2. 核心过滤：杀掉黑名单里的外汇和现金，以及代码长度大于 5 的奇怪票据
    master_df = master_df[~master_df['ticker'].isin(BLACKLIST)]
    master_df = master_df[~master_df['ticker'].str.contains('CASH', na=False)]
    master_df = master_df[master_df['ticker'].str.len() <= 5]
    master_df = master_df[master_df['ticker'].str.match(r'^[A-Z]+$')] # 必须全是纯字母

    # 3. 核心运算：分组统计
    print("🧮 正在过滤外汇与现金，计算纯血股票仓位...")
    summary_df = master_df.groupby('ticker').agg(
        Total_Shares=('shares', 'sum'),
        ETF_Count=('etf_name', 'count'),
        Holding_ETFs=('etf_name', lambda x: ', '.join(x))
    ).reset_index()

    # 4. 按总股数排序
    summary_df = summary_df.sort_values(by='Total_Shares', ascending=False)
    
    # 5. 保存
    today_str = datetime.now().strftime("%Y%m%d")
    output_filename = f"Master_Holdings_{today_str}.csv"
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    
    summary_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print("-" * 40)
    print(f"🎉 汇总完成！全市场共提取出 {len(summary_df)} 只纯正股票。")
    print(f"📊 报告已生成: {output_path}")
    print("-" * 40)
    
    print("🏆 【全市场机构最重仓纯正股票 Top 10】:")
    # 为了显示美观，把总股数转成整数，并加上千分位逗号
    display_df = summary_df[['ticker', 'Total_Shares', 'ETF_Count']].head(10).copy()
    display_df['Total_Shares'] = display_df['Total_Shares'].apply(lambda x: f"{int(x):,}")
    print(display_df.to_string(index=False))

if __name__ == "__main__":
    generate_master_report()