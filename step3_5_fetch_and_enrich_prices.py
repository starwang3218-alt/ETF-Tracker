import requests
import pandas as pd
import os
import glob
import re
from datetime import datetime, timedelta  # 增加 timedelta 用于日期加减

# 你的专属 API Key
API_KEY = 't2d63kZI_Sq9z_YTb654qQX8lNaRHQVI'

def fetch_and_enrich():
    print("🚀 启动 [Step 3.5] 全市场股价抓取与注魂引擎...")
    
    # 1. 动态获取今天的最新底表
    file_pattern = os.path.join('data', 'master_holdings_*.csv')
    all_files = glob.glob(file_pattern)
    snapshot_files = [f for f in all_files if re.search(r'\d{4}-\d{2}-\d{2}', f)]
    snapshot_files.sort(reverse=True)
    
    if not snapshot_files:
        print("❌ 错误：找不到今天的持仓快照，无法注入股价！")
        return
        
    latest_daily_file = snapshot_files[0]
    analyzed_file = os.path.join('data', 'master_holdings_analyzed.csv')
    
    # 从文件名提取日期 (例如 2026-04-21)
    target_date_str = re.search(r'\d{4}-\d{2}-\d{2}', latest_daily_file).group()
    print(f"📅 锁定目标持仓快照日期: {target_date_str}")

    # ================= 🌟 智能回退机制开始 =================
    max_retries = 5
    current_check_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    df_price = None

    for i in range(max_retries):
        check_str = current_check_date.strftime('%Y-%m-%d')
        print(f"📡 正在尝试获取美股行情: {check_str}")
        url = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{check_str}?adjusted=true&apiKey={API_KEY}'
        
        try:
            response = requests.get(url)
            data = response.json()
            
            # 如果请求成功，并且 results 里有数据
            if response.status_code == 200 and data.get('results'):
                df_price = pd.DataFrame(data['results'])
                print(f"🎯 成功获取到 {check_str} 的市场快照！")
                break # 拿到数据了，直接跳出循环
            else:
                print(f"⚠️ {check_str} 无数据 (可能未开盘或周末)，往前推一天...")
                current_check_date -= timedelta(days=1) # 日期减一天，继续下一次循环
        except Exception as e:
            print(f"💥 请求报错: {e}，往前推一天...")
            current_check_date -= timedelta(days=1)
            
    # 如果连续循环 5 次都没拿到数据
    if df_price is None or df_price.empty:
        print("❌ 连续 5 天未获取到有效行情，股价注魂中止。")
        return
    # ================= 🌟 智能回退机制结束 =================

    # 2. 清洗与整理提取到的行情数据
    for col in ['o', 'h', 'l', 'vw']:
        if col not in df_price.columns:
            df_price[col] = None
            
    df_price = df_price[['T', 'c', 'o', 'h', 'l', 'v', 'vw']].rename(columns={
        'T': 'Ticker', 'c': 'Price', 'o': 'Open', 'h': 'High', 'l': 'Low', 'v': 'Volume', 'vw': 'VWAP'
    })
    
    # 保存提取到的行情快照 (以实际抓取到数据的日期命名)
    actual_date_str = current_check_date.strftime('%Y%m%d')
    price_output = os.path.join('data', f'market_price_{actual_date_str}.csv')
    df_price.to_csv(price_output, index=False)
    print(f"✅ 满血行情保存至: {price_output}")
    
    # 3. 开始对撞注魂
    print("⏳ 正在将股价注入两大核心底表...")
    df_daily = pd.read_csv(latest_daily_file)
    
    # 左连接合并，主键为 Holding_Ticker
    df_enriched = pd.merge(df_daily, df_price, left_on='Holding_Ticker', right_on='Ticker', how='left')
    
    # 清理多余的 Ticker 列
    if 'Ticker' in df_enriched.columns:
        df_enriched.drop(columns=['Ticker'], inplace=True)
        
    # 计算市值 (安全检查：确保这两列都在)
    if 'Shares' in df_enriched.columns and 'Price' in df_enriched.columns:
        df_enriched['Market_Value'] = df_enriched['Shares'] * df_enriched['Price']

    # 4. 覆盖保存，完成升级
    df_enriched.to_csv(latest_daily_file, index=False)
    df_enriched.to_csv(analyzed_file, index=False)
    print(f"✨ 完美注魂！{latest_daily_file} 现已自带股价、成交量及市值基因。")

if __name__ == "__main__":
    # 确保脚本在正确的根目录下运行
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    os.chdir(BASE_DIR)
    fetch_and_enrich()
