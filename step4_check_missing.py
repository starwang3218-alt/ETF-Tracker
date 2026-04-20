import os
import re
import glob

# --- 配置区 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
URL_FILE = os.path.join(BASE_DIR, '每日ETF下载地址.txt')
CLEANED_DIR = os.path.join(BASE_DIR, 'data', 'cleaned')
REPORT_FILE = os.path.join(BASE_DIR, '需要手动下载的ETF.txt')

def safe_name(text):
    """和 step1 保持完全一致的命名规则，确保能对上暗号"""
    text = re.sub(r'[<>:"/|?*\x00-\x1f]+', '_', text)
    text = re.sub(r"\s+", " ", text).strip().rstrip(".")
    base = text[:120].strip() or "holdings"
    reserved = {"CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"}
    if base.upper() in reserved:
        base = f"{base}_ETF"
    return base

def main():
    print("🔍 启动漏网之鱼排查器 (基于最原始 380 个 URL 清单)...")
    
    # 1. 解析源头目标清单
    target_names = []
    if not os.path.exists(URL_FILE):
        print(f"❌ 找不到源文件: {URL_FILE}")
        return

    with open(URL_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'): 
                continue
            m = re.match(r"^(https?://\S+)(?:\s+(.+))?$", line)
            if m:
                raw_name = m.group(2).strip() if m.group(2) else line.split("/")[-1]
                target_names.append(safe_name(raw_name))
                
    # 2. 获取实际清洗成功的清单 (去后缀)
    cleaned_files = [os.path.splitext(os.path.basename(f))[0] for f in glob.glob(os.path.join(CLEANED_DIR, '*.csv'))]
    
    # 3. 极其严苛的对比找茬
    missing_etfs = []
    for target in target_names:
        if target not in cleaned_files:
            missing_etfs.append(target)
            
    # 4. 生成终极通缉令报告
    if missing_etfs:
        print(f"🚨 警报：原始清单中有 {len(target_names)} 个，但只清洗出 {len(cleaned_files)} 个！")
        print(f"🚨 发现 {len(missing_etfs)} 个顽固的漏网之鱼！正在生成通缉令...")
        with open(REPORT_FILE, 'w', encoding='utf-8') as f:
            for etf in missing_etfs:
                f.write(f"{etf}\n")
        print(f"📄 通缉令已生成: {REPORT_FILE}")
    else:
        print("🎉 完美！所有原始链接都已成功下载并清洗，没有任何缺失！")
        if os.path.exists(REPORT_FILE):
            os.remove(REPORT_FILE)  # 清理旧的通缉令

if __name__ == "__main__":
    main()
