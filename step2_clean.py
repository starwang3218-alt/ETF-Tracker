import pandas as pd
import os
import glob
import json

# 修改为你实际的输入输出目录
INPUT_DIR = 'downloads'
OUTPUT_DIR = 'data/cleaned'
os.makedirs(OUTPUT_DIR, exist_ok=True)

def unified_clean():
    # 【修复1】使用 **/*.csv 递归扫描，确保能抓到 downloads/invesco/ 和 ishares/ 下的所有文件
    all_files = glob.glob(os.path.join(INPUT_DIR, '**/*.csv'), recursive=True) + \
                glob.glob(os.path.join(INPUT_DIR, '**/*.bin'), recursive=True)
    
    print(f"📊 扫描到 {len(all_files)} 个原始文件，开始深度清洗...")

    for file_path in all_files:
        filename = os.path.basename(file_path)
        # 自动识别 Provider
        folder_name = os.path.basename(os.path.dirname(file_path)).lower()
        provider = 'Invesco' if 'invesco' in folder_name else 'iShares'
        etf_symbol = filename.replace('.csv', '').replace('.bin', '').split('_')[0].upper()
        
        try:
            # 1. 预读判断格式
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                first_char = f.read(1).strip()
            
            if first_char == '{': # Invesco 的 JSON 格式
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if 'holdings' not in data: continue
                df = pd.DataFrame(data['holdings'])
                df.rename(columns={'ticker': 'Ticker', 'units': 'Shares', 'marketValueBase': 'Market_Value'}, inplace=True)
            else:
                # 【修复2】动态扫描表头行号，专门对付 iShares 第 25 行的问题
                header_row = 0
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    for i, line in enumerate(f):
                        if 'Ticker' in line or 'Quantity' in line:
                            header_row = i
                            break
                
                # 【修复3】thousands=',' 自动移除 iShares 截图中的数字逗号
                df = pd.read_csv(file_path, skiprows=header_row, thousands=',')
                df.columns = df.columns.str.strip()
                
                # 【修复4】字典映射：把 Quantity 强制对齐为 Shares
                col_mapping = {
                    'Quantity': 'Shares', 'Share/ Par': 'Shares',
                    'Market Value': 'Market_Value', 'MarketValue': 'Market_Value',
                    'Weight': 'Weight_Pct', '% TNA': 'Weight_Pct'
                }
                df.rename(columns=lambda x: col_mapping.get(x, x), inplace=True)
            
            # 2. 字段标准化处理
            core_cols = ['Ticker', 'Name', 'Shares', 'Market_Value']
            df = df[[c for c in core_cols if c in df.columns]].copy()
            df = df.dropna(subset=['Ticker']) # 过滤页脚杂质
            
            # 强制清理数字中的符号 ($ 等) 并转为浮点数
            for col in ['Shares', 'Market_Value']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].astype(str).replace(r'[$,]', '', regex=True), errors='coerce').fillna(0)

            df['Source_ETF'] = etf_symbol
            df['Provider'] = provider
            
            out_path = os.path.join(OUTPUT_DIR, f"{etf_symbol}_cleaned.csv")
            df.to_csv(out_path, index=False)
            print(f"    ✅ 已处理: {etf_symbol} ({provider})")
            
        except Exception as e:
            print(f"    ❌ 失败 [{filename}]: {e}")

if __name__ == "__main__":
    unified_clean()
