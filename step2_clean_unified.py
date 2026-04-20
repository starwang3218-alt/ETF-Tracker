import pandas as pd
import os
import glob
import re
import warnings

# 屏蔽 openpyxl 烦人的 Excel 样式警告
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# --- 配置区 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, 'downloads')
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'cleaned')

os.makedirs(OUTPUT_DIR, exist_ok=True)

COLUMN_MAPPING = {
    'Ticker': 'Holding_Ticker',
    'Symbol': 'Holding_Ticker',
    'Holding Ticker': 'Holding_Ticker',
    'Security Identifier': 'Holding_Ticker', 
    'Identifier': 'Holding_Ticker',
    
    'Name': 'Holding_Name',
    'Description': 'Holding_Name',
    'Holding Name': 'Holding_Name',
    'Security Name': 'Holding_Name',
    'Company Name': 'Holding_Name',
    
    'Weight (%)': 'Weight_Percent',
    'Weight': 'Weight_Percent',
    'Fund Weight': 'Weight_Percent',
    'Weighting': 'Weight_Percent',
    '% of Net Assets': 'Weight_Percent',
    
    'Shares': 'Shares',
    'Shares Held': 'Shares',
    'Quantity': 'Shares',
    'Shares/Par Value': 'Shares',
    'Nominal': 'Shares',
    'Amount': 'Shares',
    
    'Market Value': 'Market_Value',
    'MarketValue': 'Market_Value',
    'Value': 'Market_Value',
    'Notional Value': 'Market_Value'
}

def extract_etf_ticker(filename):
    base = os.path.basename(filename)
    match = re.search(r'[A-Z]{2,6}', base)
    if match:
        return match.group(0)
    return base.split('.')[0]

def get_header_score(text):
    """【核心科技】给表头打分：严格匹配独立单词，防止 ishares 被当作 shares"""
    text = str(text).lower()
    # 使用 \b 规定必须是独立的单词 (word boundary)
    keywords = [
        r'\bticker\b', r'\bsymbol\b', r'\bidentifier\b',
        r'\bweight\b', r'\bshares\b', r'\bquantity\b',
        r'\bname\b', r'\bsector\b', r'\bprice\b', r'\bcurrency\b',
        r'\bmarket value\b', r'\bnotional value\b'
    ]
    score = 0
    for kw in keywords:
        if re.search(kw, text):
            score += 1
    return score

def find_header_and_load(file_path):
    """【核心科技】全文件扫描评分，找出真正的表头之王"""
    ext = file_path.lower().split('.')[-1]
    
    try:
        # CSV / BIN 逻辑
        if ext in ['csv', 'bin']:
            best_score = 0
            best_idx = 0
            
            with open(file_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
                for i, line in enumerate(f):
                    if i > 50: break # 只扫前50行
                    score = get_header_score(line)
                    # 谁的分数最高，谁就是真表头
                    if score > best_score:
                        best_score = score
                        best_idx = i
            
            # sep=None 自动识别 Tab 还是逗号分隔
            if best_score >= 2:
                return pd.read_csv(file_path, skiprows=best_idx, encoding='utf-8-sig', sep=None, engine='python', on_bad_lines='skip')
            else:
                return pd.read_csv(file_path, encoding='utf-8-sig', sep=None, engine='python', on_bad_lines='skip')

        # Excel 逻辑
        elif ext in ['xlsx', 'xls']:
            df_test = pd.read_excel(file_path, nrows=50, header=None) 
            best_score = 0
            best_idx = 0
            for i in range(len(df_test)):
                row_str = " ".join([str(val) for val in df_test.iloc[i].values])
                score = get_header_score(row_str)
                if score > best_score:
                    best_score = score
                    best_idx = i
                        
            if best_score >= 2:
                return pd.read_excel(file_path, header=best_idx) 
            return pd.read_excel(file_path)

    except pd.errors.EmptyDataError:
        print(f"    ⏩ 跳过空文件: {os.path.basename(file_path)}")
        return pd.DataFrame()
    except Exception as e:
        print(f"    ⚠️ 读取异常 {os.path.basename(file_path)}: {e}")
        return pd.DataFrame()
        
    return pd.DataFrame()

def clean_data():
    print(f"🔄 启动全能数据清洗模块...\n📂 扫描目录: {INPUT_DIR}")
    
    search_path = os.path.join(INPUT_DIR, '**', '*.*')
    all_files = glob.glob(search_path, recursive=True)
    target_files = [f for f in all_files if f.lower().endswith(('.csv', '.xlsx', '.xls', '.bin'))]
    
    if not target_files:
        print(f"❌ 错误：在 {INPUT_DIR} 没有找到任何数据文件！")
        return

    print(f"🚀 发现 {len(target_files)} 个原始文件，开始深度清洗...")
    success_count = 0
    
    for f in target_files:
        if 'download_log' in f.lower():
            continue
            
        etf_ticker = extract_etf_ticker(f)
        df = find_header_and_load(f)
        
        if df.empty:
            continue
            
        df.columns = [str(col).strip().replace('\n', '').replace('\r', '') for col in df.columns]
        df.rename(columns=COLUMN_MAPPING, inplace=True)
        
        # 暴力去重：消灭多个 Shares 同名列导致的 'str' 报错
        df = df.loc[:, ~df.columns.duplicated()]
        
        if 'Holding_Ticker' not in df.columns:
            print(f"    ⚠️ {etf_ticker} 缺少持仓代码列，目前提取到的列名有: {list(df.columns)}")
            continue
            
        df = df.dropna(subset=['Holding_Ticker'])
        df = df[~df['Holding_Ticker'].astype(str).str.contains('Total|Cash|--|NaN', case=False, na=False)]
        
        for num_col in ['Shares', 'Market_Value', 'Weight_Percent']:
            if num_col in df.columns:
                df[num_col] = df[num_col].astype(str).str.replace(',', '', regex=False)
                df[num_col] = df[num_col].str.replace('$', '', regex=False)
                df[num_col] = df[num_col].str.replace('%', '', regex=False)
                df[num_col] = pd.to_numeric(df[num_col], errors='coerce').fillna(0)
                
        df['ETF_Ticker'] = etf_ticker
        
        final_cols = ['ETF_Ticker', 'Holding_Ticker']
        for col in ['Holding_Name', 'Shares', 'Market_Value', 'Weight_Percent']:
            if col in df.columns:
                final_cols.append(col)
                
        df_clean = df[final_cols]
        
        output_file = os.path.join(OUTPUT_DIR, f"{etf_ticker}_cleaned.csv")
        df_clean.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"    ✅ 洗净: {etf_ticker} (包含 {len(df_clean)} 条持仓)")
        success_count += 1

    print(f"\n✨ 清洗完毕！成功处理 {success_count} 个文件，全部存入 {OUTPUT_DIR}")

if __name__ == "__main__":
    clean_data()