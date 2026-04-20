import os
import glob
import re

# --- 适应 GitHub 和本地的路径配置 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
URL_FILE = os.path.join(BASE_DIR, '每日ETF下载地址.txt')
CLEANED_DIR = os.path.join(BASE_DIR, 'data', 'cleaned')
MISSING_FILE = os.path.join(BASE_DIR, '需要手动下载的ETF.txt')

def get_clean_ticker(text):
    """最强 Ticker 提取器：无视中文、无视 _cleaned、无视空格"""
    if not text: return ""
    
    # 1. 如果描述以 "ETF" 结尾，先把它删了，因为它是干扰项
    text = re.sub(r'\s+ETF$', '', str(text), flags=re.IGNORECASE)
    text = text.replace('_cleaned', '').replace('_Cleaned', '')
    
    # 2. 尝试从字符串中提取连续的 3-5 位大写字母
    tickers = re.findall(r'[A-Z]{3,5}', text)
    if tickers:
        res = tickers[-1]
    else:
        # 退而求其次，提取纯字母
        res = re.sub(r'[^a-zA-Z]', '', str(text)).upper()
        
    # 3. 排除“影子目标”黑名单
    blacklist = {'ETF', 'CLEANED', 'HOLDINGS', 'INDEX'}
    if res in blacklist or len(res) < 2:
        return ""
        
    # 4. 特殊处理：如果开头有小写 t（比如 t铜矿商），去掉它
    if res.startswith('T') and len(res) > 3:
        return res[1:]
        
    return res

def main():
    print("🔍 启动智能漏网之鱼排查器...")
    
    # 1. 扫描名单
    target_map = {}
    if os.path.exists(URL_FILE):
        with open(URL_FILE, 'r', encoding='utf-8-sig') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'): continue
                parts = line.split()
                raw_name = parts[-1] if len(parts) > 1 else line.split('/')[-1]
                
                key = get_clean_ticker(raw_name)
                if key:
                    target_map[key] = raw_name
    else:
        print(f"❌ 找不到名单文件: {URL_FILE}")
        return

    # 2. 扫描库房
    cleaned_keys = set()
    if os.path.exists(CLEANED_DIR):
        for f in glob.glob(os.path.join(CLEANED_DIR, '*.csv')):
            filename = os.path.splitext(os.path.basename(f))[0]
            cleaned_keys.add(get_clean_ticker(filename))
            
    # 3. 智能对账
    missing = []
    for k, original in target_map.items():
        if k not in cleaned_keys:
            missing.append(original)

    # 4. 战报输出
    print(f"📊 名单解析出的有效目标: {len(target_map)} 个")
    print(f"📊 库房实际存货: {len(cleaned_keys)} 个")
    print("-" * 40)

    if missing:
        print(f"🚨 抓到 {len(missing)} 个真正的漏网之鱼！正在生成通缉令...")
        with open(MISSING_FILE, 'w', encoding='utf-8') as f:
            for m in missing:
                f.write(f"{m}\n")
        print(f"📄 通缉令已生成: {MISSING_FILE}")
    else:
        print("✅ 完美对账！没有任何缺失！")
        # 如果全齐了，把之前的通缉令删掉，免得打包报错
        if os.path.exists(MISSING_FILE):
            os.remove(MISSING_FILE)

if __name__ == "__main__":
    main()
