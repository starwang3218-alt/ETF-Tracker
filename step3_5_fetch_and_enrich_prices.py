import requests
import pandas as pd
import os
import glob
import re
from datetime import datetime

# 你的专属 API Key
API_KEY = 't2d63kZI_Sq9z_YTb654qQX8lNaRHQVI'

def fetch_and_enrich():
    print("🚀 启动 [Step 3.5] 全市场股价抓取与注魂引擎...")
    
    # 1. 动态获取今天的最新底表
    # 为了防止跨时区导致日期差一天，我们直接去 data 文件夹找最新的表
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
    target_date = re.search(r'\d{4}-\d{2}-\d{2}', latest_daily_file).group()
    print(f"📅 锁定目标日期: {target_date}")

    # 2. 抓取 Polygon 行情数据
    url = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{target_date}?adjusted=true&apiKey={API_KEY}'
    
    try:
        response = requests.get(url)
        data = response.json()
        
        if response.status_code == 200 and data.get('results'):
            df_price = pd.DataFrame(data['results'])
            
            # 补全可能缺失的字段
            for col in ['o', 'h', 'l', 'vw']:
                if col not in df_price.columns:
                    df_price[col] = None
                    
            df_price = df_price[['T', 'c', 'o', 'h', 'l', 'v', 'vw']].rename(columns={
                'T': 'Ticker', 'c': 'Price', 'o': 'Open', 'h': 'High', 'l': 'Low', 'v': 'Volume', 'vw': 'VWAP'
            })
            
            # 保存今日行情快照
            date_str = target_date.replace('-', '')
            price_output = os.path.join('data', f'market_price_{date_str}.csv')
            df_price.to_csv(price_output, index=False)
            print(f"✅ 今日满血行情抓取成功！保存至: {price_output}")
            
            # 3. 开始对撞注魂
            print("⏳ 正在将股价注入两大核心底表...")
            df_daily = pd.read_csv(latest_daily_file)
            
            # 左连接合并，主键为 Holding_Ticker
            df_enriched = pd.merge(df_daily, df_price, left_on='Holding_Ticker', right_on='Ticker', how='left')
            
            # 清理多余的 Ticker 列
            if 'Ticker' in df_enriched.columns:
                df_enriched.drop(columns=['Ticker'], inplace=True)
                
            # 计算市值 (如果之前的表里有 Shares 列)
            if 'Shares' in df_enriched.columns and 'Price' in df_enriched.columns:
                df_enriched['Market_Value'] = df_enriched['Shares'] * df_enriched['Price']

            # 4. 覆盖保存，完成升级
            df_enriched.to_csv(latest_daily_file, index=False)
            df_enriched.to_csv(analyzed_file, index=False)
            print("✨ 完美注魂！大表现在已自带股价、成交量及 VWAP 基因。")
            
        else:
            print(f"⚠️ 抓取失败或今日无数据（可能未开盘）。API 返回: {data.get('status')}")
            
    except Exception as e:
        print(f"💥 行情抓取报错: {e}")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    os.chdir(BASE_DIR)
    fetch_and_enrich()
