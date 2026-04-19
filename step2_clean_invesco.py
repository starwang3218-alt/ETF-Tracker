import pandas as pd
import os
import glob
import json

INPUT_DIR = 'downloads'
OUTPUT_DIR = 'data/cleaned_invesco'
os.makedirs(OUTPUT_DIR, exist_ok=True)

def clean_invesco():
    # 抓取第一步下载的所有 csv 和 bin 文件
    all_files = glob.glob(os.path.join(INPUT_DIR, '*.csv')) + glob.glob(os.path.join(INPUT_DIR, '*.bin'))
    print(f"📊 找到 {len(all_files)} 个景顺原始文件，准备清洗...")

    for file_path in all_files:
        filename = os.path.basename(file_path)
        etf_symbol = filename.replace('.csv', '').replace('.bin', '').upper()
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                first_char = f.read(1).strip()
            
            if first_char == '{':
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if 'holdings' not in data or not data['holdings']: continue
                df = pd.DataFrame(data['holdings'])
                df.rename(columns={'ticker': 'Ticker', 'issuerName': 'Name', 'units': 'Shares', 
                                   'percentageOfTotalNetAssets': 'Weight_Pct', 'marketValueBase': 'Market_Value'}, inplace=True)
            else:
                df = pd.read_csv(file_path)
                df.columns = df.columns.str.strip()
                col_mapping = {'Ticker': 'Ticker', 'Company': 'Name', 'Name': 'Name', 
                               'Share/ Par': 'Shares', 'Shares': 'Shares', 
                               '% TNA': 'Weight_Pct', 'Weight': 'Weight_Pct', 
                               'Market value': 'Market_Value', 'Market Value': 'Market_Value'}
                df.rename(columns=lambda x: col_mapping.get(x, x), inplace=True)
            
            core_cols = ['Ticker', 'Name', 'Shares', 'Weight_Pct', 'Market_Value']
            df = df[[c for c in core_cols if c in df.columns]].copy()
            
            if 'Weight_Pct' in df.columns:
                df['Weight_Pct'] = df['Weight_Pct'].astype(str).str.replace('%', '', regex=False).astype(float)
            if 'Shares' in df.columns:
                df['Shares'] = df['Shares'].astype(str).str.replace(',', '', regex=False).str.replace('--', '0', regex=False).astype(float)
            if 'Market_Value' in df.columns:
                df['Market_Value'] = df['Market_Value'].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.replace('--', '0', regex=False).astype(float)
            if 'Ticker' in df.columns:
                df['Ticker'] = df['Ticker'].astype(str).str.replace('_', '', regex=False).replace('--', 'CASH').str.strip()
            
            df['Source_ETF'] = etf_symbol
            df['Provider'] = 'Invesco'
            
            out_path = os.path.join(OUTPUT_DIR, f"{etf_symbol}_cleaned.csv")
            df.to_csv(out_path, index=False)
            print(f"    ✅ 清洗完毕: {etf_symbol}")
            
        except Exception as e:
            print(f"    ❌ 清洗失败 [{filename}]: {e}")

if __name__ == "__main__":
    clean_invesco()
