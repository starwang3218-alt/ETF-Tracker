import os
import re
import glob

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
URL_FILE = os.path.join(BASE_DIR, '每日ETF下载地址.txt')
CLEANED_DIR = os.path.join(BASE_DIR, 'data', 'cleaned')

def check_missing_etfs():
    print("🔍 启动漏网之鱼排查器...")
    
    # 1. 获取所有已经洗干净的 ETF 代码
    cleaned_files = glob.glob(os.path.join(CLEANED_DIR, '*_cleaned.csv'))
    cleaned_tickers = set([os.path.basename(f).replace('_cleaned.csv', '') for f in cleaned_files])
    
    # 2. 从下载列表里提取应有的 ETF 代码
    expected_tickers = set()
    with open(URL_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            match = re.search(r'[A-Z]{2,6}', line)
            if match:
                expected_tickers.add(match.group(0))
                
    # 3. 找出缺失项
    missing = expected_tickers - cleaned_tickers
    
    if not missing:
        print("🎉 完美！所有链接都已成功下载并清洗，没有缺失！")
    else:
        print(f"⚠️ 发现 {len(missing)} 个未成功解析的 ETF，请手动下载覆盖：")
        for ticker in sorted(missing):
            print(f"    - {ticker}")
        
        # 写入补考清单
        with open('需要手动下载的ETF.txt', 'w', encoding='utf-8') as f:
            f.write("\n".join(sorted(missing)))
        print("📝 已生成《需要手动下载的ETF.txt》")

if __name__ == "__main__":
    check_missing_etfs()