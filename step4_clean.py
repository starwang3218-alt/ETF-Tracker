#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import os
import glob

# 目录配置
INPUT_DIR = 'downloads'
OUTPUT_DIR = 'data/cleaned'
os.makedirs(OUTPUT_DIR, exist_ok=True)

def clean_all_etfs():
    # 找到所有的 CSV 文件
    all_files = glob.glob(os.path.join(INPUT_DIR, '*.csv'))
    
    if not all_files:
        print("⚠️ 在 downloads 文件夹中没有找到任何 CSV 文件，跳过清洗。")
        return

    print(f"🔍 找到 {len(all_files)} 个原始持仓文件，开始清洗...")

    for file_path in all_files:
        filename = os.path.basename(file_path)
        try:
            # 1. 读取原始文件
            df = pd.read_csv(file_path)
            
            # 2. 清理表头 (去掉列名前后多余的空格，应对 '  Share/ Par' 的情况)
            df.columns = df.columns.str.strip()
            
            # 3. 建立列名映射字典 (兼容 Invesco 和其他家的常见表头)
            col_mapping = {
                'Ticker': 'Ticker',
                'Company': 'Name',
                'Name': 'Name',
                'Share/ Par': 'Shares', # 景顺特有
                'Shares': 'Shares',
                '% TNA': 'Weight_Pct',  # 景顺特有
                'Weight': 'Weight_Pct',
                'Market value': 'Market_Value', # 景顺特有
                'Market Value': 'Market_Value'
            }
            
            # 把现有的列名替换为标准名称
            df.rename(columns=lambda x: col_mapping.get(x, x), inplace=True)
            
            # 提取我们真正关心的核心列 (只保留存在的列，防止报错)
            core_cols = ['Ticker', 'Name', 'Shares', 'Weight_Pct', 'Market_Value']
            existing_cols = [c for c in core_cols if c in df.columns]
            clean_df = df[existing_cols].copy()
            
            # 4. 深度清洗数据内容
            if 'Weight_Pct' in clean_df.columns:
                # 剔除 % 并转换为浮点数
                clean_df['Weight_Pct'] = clean_df['Weight_Pct'].astype(str).str.replace('%', '', regex=False).astype(float)
            
            if 'Shares' in clean_df.columns:
                # 剔除千分位逗号，把 '--' 转为 0
                clean_df['Shares'] = clean_df['Shares'].astype(str).str.replace(',', '', regex=False).str.replace('--', '0', regex=False).astype(float)
                
            if 'Market_Value' in clean_df.columns:
                # 剔除美元符号和逗号
                clean_df['Market_Value'] = clean_df['Market_Value'].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.replace('--', '0', regex=False).astype(float)
                
            if 'Ticker' in clean_df.columns:
                # 修复景顺特有的代码尾部下划线 (如 XMM6_)，并将无代码的标记为 CASH
                clean_df['Ticker'] = clean_df['Ticker'].astype(str).str.replace('_', '', regex=False).replace('--', 'CASH').str.strip()
            
            # 5. 按权重从大到小排序
            if 'Weight_Pct' in clean_df.columns:
                clean_df = clean_df.sort_values(by='Weight_Pct', ascending=False)
            
            # 6. 保存为标准格式
            out_filename = filename.replace('.csv', '_cleaned.csv')
            out_path = os.path.join(OUTPUT_DIR, out_filename)
            clean_df.to_csv(out_path, index=False)
            
            print(f"✅ 成功清洗: {filename} -> {out_filename}")
            
        except Exception as e:
            print(f"❌ 清洗失败 [{filename}]: {e}")

if __name__ == "__main__":
    clean_all_etfs()
