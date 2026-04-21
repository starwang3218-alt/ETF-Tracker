import pandas as pd
import os
import glob
import re
import warnings
from datetime import datetime, timedelta

# 屏蔽 openpyxl 烦人的 Excel 样式警告
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# --- 配置区 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, 'downloads')
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'cleaned')
REPORT_FILE = os.path.join(BASE_DIR, 'data', 'data_freshness_report.csv') 

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
    'Company': 'Holding_Name',         # 👈 新增：认领 Company 列
    
    'Weight (%)': 'Weight_Percent',
    'Weight': 'Weight_Percent',
    'Fund Weight': 'Weight_Percent',
    'Weighting': 'Weight_Percent',
    '% of Net Assets': 'Weight_Percent',
    
    'Shares': 'Shares',
    'Shares Held': 'Shares',
    'Quantity': 'Shares',
    'Shares/Par Value': 'Shares',
    'Share/ Par': 'Shares',            # 👈 新增：认领 Share/ Par 列
    'Share/Par': 'Shares',             # 👈 新增：防御性添加，防止有些表没有空格
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
    text = str(text).lower()
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
    ext = file_path.lower().split('.')[-1]
    try:
        if ext in ['csv', 'bin']:
            best_score = 0
            best_idx = 0
            with open(file_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
                for i, line in enumerate(f):
                    if i > 300: break 
                    score = get_header_score(line)
                    if score > best_score:
                        best_score = score
                        best_idx = i
            
            if best_score >= 2:
                return pd.read_csv(file_path, skiprows=best_idx, encoding='utf-8-sig', sep=None, engine='python', on_bad_lines='skip')
            else:
                return pd.read_csv(file_path, encoding='utf-8-sig', sep=None, engine='python', on_bad_lines='skip')

        elif ext in ['xlsx', 'xls']:
            df_test = pd.read_excel(file_path, nrows=300, header=None) 
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

    except Exception as e:
        return pd.DataFrame()

def get_expected_date():
    today = datetime.now()
    if today.weekday() == 0: 
        return (today - timedelta(days=3)).date()
    elif today.weekday() == 6:
        return (today - timedelta(days=2)).date()
    else:
        return (today - timedelta(days=1)).date()

def extract_as_of_date(file_path):
    ext = file_path.lower().split('.')[-1]
    date_pattern = re.compile(r'(\d{1,2}-[A-Za-z]{3}-\d{2,4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{2,4}|\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{2,4})')
    
    def parse_date(d_str):
        try:
            return pd.to_datetime(d_str).date()
        except:
            return None
            
    def check_text_for_date(text):
        text_str = str(text)
        text_lower = text_str.lower()
        
        if 'maturity' in text_lower:
            return None
            
        if any(kw in text_lower for kw in ['as of', 'asof', 'date', 'holdings']):
            matches = date_pattern.findall(text_str)
            if matches:
                return parse_date(matches[0])
        return None

    try:
        if ext in ['csv', 'bin']:
            with open(file_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
                for line in f:
                    parsed = check_text_for_date(line)
                    if parsed: return parsed
                            
        elif ext in ['xlsx', 'xls']:
            df_test = pd.read_excel(file_path, nrows=200, header=None)
            for _, row in df_test.iterrows():
                row_str = " ".join([str(val) for val in row.values if pd.notna(val)])
                parsed = check_text_for_date(row_str)
                if parsed: return parsed
    except Exception:
        pass
    return None

def clean_data():
    print(f"🔄 启动数据清洗引擎...\n📂 扫描目录: {INPUT_DIR}")
    
    search_path = os.path.join(INPUT_DIR, '**', '*.*')
    all_files = glob.glob(search_path, recursive=True)
    target_files = [f for f in all_files if f.lower().endswith(('.csv', '.xlsx', '.xls', '.bin'))]
    
    if not target_files:
        print(f"❌ 错误：未找到数据文件！")
        return

    success_count = 0
    expected_date = get_expected_date()
    freshness_log = [] 
    
    for f in target_files:
        if 'download_log' in f.lower(): continue
            
        etf_ticker = extract_etf_ticker(f)
        file_date = extract_as_of_date(f)
        
        status_flag = "未知"
        date_str = "未找到内置日期"
        print_status = ""

        if file_date:
            date_str = str(file_date)
            if file_date == expected_date:
                status_flag = "🟢 最新"
                print_status = f"[🟢 最新: {file_date}]"
            elif file_date < expected_date:
                status_flag = "🔴 滞后"
                print_status = f"[🔴 滞后: {file_date} (预期 {expected_date})]"
            else:
                status_flag = "🟡 异动"
                print_status = f"[🟡 异动: {file_date}]"
        else:
            status_flag = "⚪ 未知"
            print_status = "[⚪ 未找到 As Of 日期]"

        df = find_header_and_load(f)
        if df.empty:
            freshness_log.append({'ETF_Ticker': etf_ticker, 'File_Date': date_str, 'Status': '❌ 读取失败', 'Row_Count': 0})
            continue
            
        df.columns = [str(col).strip().replace('\n', '').replace('\r', '') for col in df.columns]
        df.rename(columns=COLUMN_MAPPING, inplace=True)
        df = df.loc[:, ~df.columns.duplicated()]
        
        if 'Holding_Ticker' not in df.columns:
            freshness_log.append({'ETF_Ticker': etf_ticker, 'File_Date': date_str, 'Status': '❌ 缺持仓列', 'Row_Count': 0})
            continue
            
        df = df.dropna(subset=['Holding_Ticker'])
        
        # --- 🌟 新增过滤逻辑 1：不仅过滤 Total，还要把含 As of 和 Date 的伪代码也毙掉 ---
        df = df[~df['Holding_Ticker'].astype(str).str.contains('Total|Cash|--|NaN|As of|AsOf|Date|#', case=False, na=False)]
        
        for num_col in ['Shares', 'Market_Value', 'Weight_Percent']:
            if num_col in df.columns:
                df[num_col] = df[num_col].astype(str).str.replace(',', '', regex=False)
                df[num_col] = df[num_col].str.replace('$', '', regex=False)
                df[num_col] = df[num_col].str.replace('%', '', regex=False)
                df[num_col] = pd.to_numeric(df[num_col], errors='coerce').fillna(0)
                
        # --- 🌟 新增过滤逻辑 2：剔除由于格式化等原因导致 Shares 为 0 的废弃行 ---
        if 'Shares' in df.columns:
            df = df[df['Shares'] > 0]
                
        df['ETF_Ticker'] = etf_ticker
        
        final_cols = ['ETF_Ticker', 'Holding_Ticker']
        for col in ['Holding_Name', 'Shares', 'Market_Value', 'Weight_Percent']:
            if col in df.columns:
                final_cols.append(col)
                
        df_clean = df[final_cols]
        output_file = os.path.join(OUTPUT_DIR, f"{etf_ticker}_cleaned.csv")
        df_clean.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"    ✅ 洗净: {etf_ticker} {print_status} (包含 {len(df_clean)} 条)")
        success_count += 1
        
        freshness_log.append({
            'ETF_Ticker': etf_ticker,
            'File_Date': date_str,
            'Expected_Date': str(expected_date),
            'Status': status_flag,
            'Row_Count': len(df_clean)
        })

    if freshness_log:
        df_report = pd.DataFrame(freshness_log)
        df_report.sort_values(by=['Status', 'ETF_Ticker'], inplace=True)
        df_report.to_csv(REPORT_FILE, index=False, encoding='utf-8-sig')
        print(f"\n📊 质检报告已生成: {REPORT_FILE}")

if __name__ == "__main__":
    clean_data()