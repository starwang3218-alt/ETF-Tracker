import os
import glob
import re

URL_FILE = '每日ETF下载地址.txt'
CLEANED_DIR = 'data/cleaned'

def get_expected_name(raw_name):
    """严格执行你总结的命名规则"""
    raw_name = raw_name.strip()
    
    has_space = ' ' in raw_name
    has_upper = bool(re.search(r'[A-Z]', raw_name))
    has_lower = bool(re.search(r'[a-z]', raw_name))
    has_chinese = bool(re.search(r'[\u4e00-\u9fa5]', raw_name))
    
    # 规则 2: 大写字母+中文 或者 中文+大写字母 或者 包含空格 -> 取大写字母
    if has_space or (has_upper and has_chinese):
        uppers = re.findall(r'[A-Z]+', raw_name)
        if uppers:
            # 取出最长的一串大写字母作为 Ticker (例如: "MDY 标普中盘400指数" -> "MDY")
            return max(uppers, key=len)
            
    # 规则 1: 小写字母+中文 或者 中文+小写字母 -> 直接原样保留
    if has_lower and has_chinese and not has_space:
        return raw_name
        
    # 补充兜底: 纯英文无中文无空格 (例如 IVV) -> 保持原样
    if not has_chinese and not has_space:
        return raw_name
        
    # 如果极度异常，提取全大写
    uppers = re.findall(r'[A-Z]+', raw_name)
    if uppers: return max(uppers, key=len)
    return raw_name

def main():
    print("🎯 启动 [终极真理版] 对账器...")
    
    # 1. 拿到库房 402 个文件的真实基准名
    actual_names = set()
    for f in glob.glob(os.path.join(CLEANED_DIR, '*_cleaned.csv')):
        base_name = os.path.basename(f).replace('_cleaned.csv', '')
        actual_names.add(base_name)

    # 2. 读取 403 行清单，严格按你的规则推演它应该叫什么名字
    missing = []
    total_lines = 0
    
    with open(URL_FILE, 'r', encoding='utf-8-sig') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'): continue
            total_lines += 1
            
            # 以第一个空格切分，把网址和后面的描述分开
            parts = line.split(None, 1) 
            if len(parts) > 1:
                raw_name = parts[1].strip()
            else:
                raw_name = line.split('/')[-1]
                
            # 套用你的法则算名字
            target_name = get_expected_name(raw_name)
            
            # 去库房点名
            if target_name not in actual_names:
                missing.append(f"【缺失】预期名字: {target_name:15} | 原行: {line}")

    # 3. 宣读审判结果
    print("-" * 60)
    if missing:
        print(f"🚨 终于对齐了！清单 {total_lines} 行，库房 {len(actual_names)} 个，这 {len(missing)} 个是真没下到：")
        for m in missing:
            print(m)
    else:
        print(f"✅ 完美！清单 {total_lines} 行和库房 {len(actual_names)} 个文件严丝合缝，逻辑彻底闭环！")
    print("-" * 60)

if __name__ == "__main__":
    main()