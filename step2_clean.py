import pandas as pd
import os
import io

RAW_DIR = 'data/raw'
CLEANED_DIR = 'data/cleaned'
os.makedirs(CLEANED_DIR, exist_ok=True)

TICKER_ALIASES = ['ticker', 'symbol', 'holdings ticker', 'identifier']
SHARES_ALIASES = ['shares', 'share_hold', 'shares held', 'shares_held', 'qty', 'quantity']

def is_excel_file(file_path):
    """
    【核心透视逻辑】：不看后缀名，直接读取文件底层的二进制指纹
    真正的 Excel 文件是以 'PK' (ZIP结构) 开头的。
    """
    try:
        with open(file_path, 'rb') as f:
            return f.read(4) == b'PK\x03\x04'
    except:
        return False

def clean_data():
    print("🧹 启动终极靶向清洗：底层文件类型探测 + 智能解析...")
    
    files = [f for f in os.listdir(RAW_DIR) if f.endswith(('.csv', '.xlsx'))]
    if not files:
        print("❌ data/raw 文件夹为空。")
        return

    success, fail = 0, 0
    
    for filename in files:
        file_path = os.path.join(RAW_DIR, filename)
        base_name = os.path.splitext(filename)[0]
        
        try:
            df = None
            
            # --- 1. 真实身份鉴定 ---
            # 如果后缀是xlsx，或者虽然是csv但底层其实是Excel...
            if filename.endswith('.xlsx') or is_excel_file(file_path):
                # 强制用 openpyxl (Excel引擎) 解析这个伪装者
                for skip in range(15):
                    try:
                        temp_df = pd.read_excel(file_path, skiprows=skip, nrows=0, engine='openpyxl')
                        cols = [str(c).lower().strip() for c in temp_df.columns]
                        if any(t in cols for t in TICKER_ALIASES) and any(s in cols for s in SHARES_ALIASES):
                            df = pd.read_excel(file_path, skiprows=skip, engine='openpyxl')
                            break
                    except:
                        continue
            
            # --- 2. 如果确实是文本文件 (CSV / TSV) ---
            else:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                lines = content.split('\n')
                header_idx = -1
                
                for i, line in enumerate(lines[:30]):
                    low_line = line.lower()
                    if any(t in low_line for t in TICKER_ALIASES) and any(s in low_line for s in SHARES_ALIASES):
                        header_idx = i
                        break
                
                if header_idx != -1:
                    # 智能识别分隔符：应对把 Tab 伪装成逗号的情况
                    header_line = lines[header_idx]
                    delimiter = '\t' if '\t' in header_line else (';' if ';' in header_line else ',')
                    
                    df = pd.read_csv(
                        io.StringIO(content), 
                        skiprows=header_idx, 
                        sep=delimiter, 
                        on_bad_lines='skip', 
                        engine='python'
                    )

            # --- 3. 错误拦截 ---
            if df is None or df.empty:
                print(f"   ⚠️ 跳过 {filename}: 找不到符合条件的数据区。")
                fail += 1
                continue

            # --- 4. 提取与标准化 ---
            df.columns = [str(c).lower().strip() for c in df.columns]
            
            actual_ticker_col = next((c for c in df.columns if c in TICKER_ALIASES), None)
            actual_shares_col = next((c for c in df.columns if c in SHARES_ALIASES), None)
            
            if not actual_ticker_col or not actual_shares_col:
                print(f"   ⚠️ 跳过 {filename}: 匹配列名失败 -> 当前列名为 {list(df.columns)[:5]}")
                fail += 1
                continue
                
            keep_cols = {actual_ticker_col: 'ticker', actual_shares_col: 'shares'}
            clean_df = df[list(keep_cols.keys())].rename(columns=keep_cols)

            # 强效清洗
            clean_df = clean_df.dropna(subset=['ticker'])
            clean_df['shares'] = clean_df['shares'].astype(str).str.replace(',', '').str.replace('"', '')
            clean_df['shares'] = pd.to_numeric(clean_df['shares'], errors='coerce')
            clean_df = clean_df.dropna(subset=['shares'])
            
            # --- 5. 存储 ---
            save_path = os.path.join(CLEANED_DIR, f"{base_name}.csv")
            clean_df.to_csv(save_path, index=False, encoding='utf-8-sig')
            print(f"✅ {base_name}: 洗出 {len(clean_df)} 只持仓股！")
            success += 1
            
        except Exception as e:
            print(f"   ❌ {filename} 解析异常: {e}")
            fail += 1

    print("-" * 40)
    print(f"🎉 提取结束！成功战胜各种伪装，洗出 {success} 个标准文件。")

if __name__ == "__main__":
    clean_data()