import pandas as pd
from pathlib import Path

def generate_collision_report():
    # 1. 配置路径
    time_series_path = Path("data/holdings_time_series.csv")
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)

    # 🌟 2. 定义货币与非股票黑名单 (剔除 INR, USD 等干扰项)
    CURRENCY_BLACKLIST = {
        'USD', 'INR', 'EUR', 'GBP', 'JPY', 'HKD', 'CAD', 'AUD', 'CHF', 'CNY', 
        'SGD', 'MXN', 'NZD', 'KRW', 'SEK', 'NOK', 'DKK', 'TRY', 'ZAR', 'BRL', 
        'TWD', 'IDR', 'MYR', 'PHP', 'THB', 'VND', 'ILS', 'CASH', 'CURRENCY', 'MONEY'
    }

    if not time_series_path.exists():
        print("❌ 找不到历史总账 holdings_time_series.csv")
        return

    print("📊 正在读取历史总账进行量价对撞分析...")
    df = pd.read_csv(time_series_path, low_memory=False)
    
    # 3. 锁定核心列名
    date_col = 'Record_Date' if 'Record_Date' in df.columns else 'Date'
    ticker_col = 'Holding_T' if 'Holding_T' in df.columns else 'Holding_Ticker'
    shares_col = 'Total_Market_Shares' if 'Total_Market_Shares' in df.columns else 'Total_Mar'
    
    # 锁定价格与成交量列
    target_metrics = ['Price', 'VWAP', 'Volume']
    available_metrics = [c for c in target_metrics if c in df.columns]

    df[date_col] = df[date_col].astype(str)
    available_dates = sorted(df[date_col].unique(), reverse=True)
    if len(available_dates) < 2:
        print("⚠️ 总账中数据不足 2 天，无法对撞。")
        return

    t0_date = available_dates[0]
    t1_date = available_dates[1]
    print(f"⚔️ 正在对撞数据: {t0_date} vs {t1_date}")

    # 4. 提取 T0 和 T1
    cols_t0 = [ticker_col, shares_col] + available_metrics
    df_t0 = df[df[date_col] == t0_date][cols_t0].rename(columns={shares_col: 'Shares_T0'})
    for m in available_metrics: df_t0 = df_t0.rename(columns={m: f"{m}_T0"})

    df_t1 = df[df[date_col] == t1_date][[ticker_col, shares_col] + available_metrics].rename(columns={shares_col: 'Shares_T1'})
    for m in available_metrics: df_t1 = df_t1.rename(columns={m: f"{m}_T1"})

    # 5. 合并对撞
    collision_df = pd.merge(df_t0, df_t1, on=ticker_col, how='outer').fillna(0)

    # 🌟 6. 执行过滤：剔除货币代码
    # 转换大写并过滤
    collision_df[ticker_col] = collision_df[ticker_col].astype(str).str.strip().str.upper()
    collision_df = collision_df[~collision_df[ticker_col].isin(CURRENCY_BLACKLIST)]
    # 过滤掉任何包含 "CASH" 字样的标的
    collision_df = collision_df[~collision_df[ticker_col].str.contains('CASH', na=False)]

    # 7. 核心计算
    collision_df['Shares_Change'] = collision_df['Shares_T0'] - collision_df['Shares_T1']
    collision_df['Change_Pct'] = collision_df.apply(
        lambda row: (row['Shares_Change'] / row['Shares_T1']) if row['Shares_T1'] > 0 else 1.0, axis=1
    )

    if 'Price_T0' in collision_df.columns and 'Price_T1' in collision_df.columns:
        collision_df['Price_Change_Pct'] = collision_df.apply(
            lambda row: ((row['Price_T0'] - row['Price_T1']) / row['Price_T1']) if row['Price_T1'] > 0 else 0.0, axis=1
        )
    
    if 'Volume_T0' in collision_df.columns and 'Volume_T1' in collision_df.columns:
        collision_df['Volume_Change_Pct'] = collision_df.apply(
            lambda row: ((row['Volume_T0'] - row['Volume_T1']) / row['Volume_T1']) if row['Volume_T1'] > 0 else 0.0, axis=1
        )

    # 8. 排序与清理
    collision_df = collision_df[collision_df['Shares_Change'] != 0]
    collision_df = collision_df.sort_values(by='Shares_Change', ascending=False)
    
    # 格式化百分比
    collision_df['Change_Pct'] = collision_df['Change_Pct'].apply(lambda x: f"{x:.4%}")
    if 'Price_Change_Pct' in collision_df.columns:
        collision_df['Price_Change_Pct'] = collision_df['Price_Change_Pct'].apply(lambda x: f"{x:.2%}")
    if 'Volume_Change_Pct' in collision_df.columns:
        collision_df['Volume_Change_Pct'] = collision_df['Volume_Change_Pct'].apply(lambda x: f"{x:.2%}")
    
    collision_df = collision_df.rename(columns={ticker_col: 'Holding_Ticker', 'Price_T0': 'Price', 'VWAP_T0': 'VWAP', 'Volume_T0': 'Volume'})

    # 9. 精简输出列
    final_cols = ['Holding_Ticker', 'Shares_Change', 'Change_Pct']
    if 'Price' in collision_df.columns: final_cols.extend(['Price', 'Price_Change_Pct'])
    if 'VWAP' in collision_df.columns: final_cols.append('VWAP')
    if 'Volume' in collision_df.columns: final_cols.extend(['Volume', 'Volume_Change_Pct'])
    final_cols.extend(['Shares_T0', 'Shares_T1'])
    
    collision_df = collision_df[[c for c in final_cols if c in collision_df.columns]]

    # 10. 保存结果
    safe_t0 = t0_date.replace('/', '').replace('-', '')
    safe_t1 = t1_date.replace('/', '').replace('-', '')
    report_filename = reports_dir / f"机构异动对撞报告_{safe_t0}_vs_{safe_t1}.csv"
    collision_df.to_csv(report_filename, index=False, encoding='utf-8-sig')
    
    print(f"\n🎉 报告已生成（已剔除货币噪音）：{report_filename}")
    print("\n🔥 【机构净买入 Top 5】")
    print(collision_df.head(5).to_string(index=False))

if __name__ == "__main__":
    generate_collision_report()