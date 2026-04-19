#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import os
import glob
from datetime import datetime

INPUT_DIR = 'downloads'
OUTPUT_DIR = 'data/cleaned'
os.makedirs(OUTPUT_DIR, exist_ok=True)

def count_expected_etfs():
    """读取 txt 文件，统计我们期望下载的 ETF 总数"""
    expected_count = 0
    for txt_file in ['景顺每日ETF下载地址.txt', '其他每日ETF下载地址.txt']:
        if os.path.exists(txt_file):
            with open(txt_file, 'r', encoding='utf-8') as f:
                # 过滤空行和注释行
                lines = [l for l in f if l.strip() and not l.startswith('#')]
                expected_count += len(lines)
    return expected_count

def clean_all_etfs():
    all_files = glob.glob(os.path.join(INPUT_DIR, '*.csv'))
    
    # ---------------- 核心防御 1：查岗机制 ----------------
    expected_total = count_expected_etfs()
    actual_total = len(all_files)
    print(f"📊 数据质检: 目标清单共有 {expected_total} 只 ETF，当前目录找到 {actual_total} 份原始文件。")
    
    if actual_total < expected_total:
        print("⚠️ 警告：检测到文件缺失！系统将自动沿用 Git 仓库中遗留的历史文件（前向填充）进行数据计算，防止持仓断崖式下跌。")

    for file_path in all_files:
        filename = os.path.basename(file_path)
        # 提取文件名作为 ETF 代码 (比如 OMFL.csv -> OMFL)
        etf_symbol = filename.replace('.csv', '').replace('_cleaned', '').upper()
        
        try:
            df = pd.read_csv(file_path)
            df.columns = df.columns.str.strip()
            
            col_mapping = {
                'Ticker': 'Ticker', 'Company': 'Name', 'Name': 'Name',
                'Share/ Par': 'Shares', 'Shares': 'Shares',
                '% TNA': 'Weight_Pct', 'Weight': 'Weight_Pct',
                'Market value': 'Market_Value', 'Market Value': 'Market_Value'
            }
            df.rename(columns=lambda x: col_mapping.get(x, x), inplace=True)
            
            core_cols = ['Ticker', 'Name', 'Shares', 'Weight_Pct', 'Market_Value']
            existing_cols = [c for c in core_cols if c in df.columns]
            clean_df = df[existing_cols].copy()
            
            # 数据内容格式化
            if 'Weight_Pct' in clean_df.columns:
                clean_df['Weight_Pct'] = clean_df['Weight_Pct'].astype(str).str.replace('%', '', regex=False).astype(float)
            if 'Shares' in clean_df.columns:
                clean_df['Shares'] = clean_df['Shares'].astype(str).str.replace(',', '', regex=False).str.replace('--', '0', regex=False).astype(float)
            if 'Market_Value' in clean_df.columns:
                clean_df['Market_Value'] = clean_df['Market_Value'].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.replace('--', '0', regex=False).astype(float)
            if 'Ticker' in clean_df.columns:
                clean_df['Ticker'] = clean_df['Ticker'].astype(str).str.replace('_', '', regex=False).replace('--', 'CASH').str.strip()
            
            # ---------------- 核心防御 2：身份与时间戳盖章 ----------------
            clean_df['Source_ETF'] = etf_symbol
            
            # 获取文件的最后修改时间，判断数据的新鲜度
            mtime = os.path.getmtime(file_path)
            clean_df['Data_Date'] = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d')
            
            if 'Weight_Pct' in clean_df.columns:
                clean_df = clean_df.sort_values(by='Weight_Pct', ascending=False)
            
            out_filename = filename.replace('.csv', '_cleaned.csv')
            out_path = os.path.join(OUTPUT_DIR, out_filename)
            clean_df.to_csv(out_path, index=False)
            
            print(f"✅ 清洗完毕: {out_filename} (数据日期: {clean_df['Data_Date'].iloc[0]})")
            
        except Exception as e:
            print(f"❌ 清洗异常 [{filename}]: {e}")

if __name__ == "__main__":
    clean_all_etfs()
