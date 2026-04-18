import requests
import os
import re
import time
import random
from datetime import datetime, timedelta

# 强化版请求头：虽然去掉了代理，但必须保留 Cookie 伪装
# 否则 iShares (贝莱德) 依然会只给你下发免责声明，而不给真实数据
BASE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cookie': 'cp=1; ishares_investor_type=individual; iShares_Global_User="type=individual"', 
    'Connection': 'keep-alive'
}

os.makedirs('data/raw', exist_ok=True)

def get_latest_url(original_url):
    """针对 Global X 等动态日期链接进行探测"""
    if 'globalxetfs.com' not in original_url:
        return original_url
    for i in range(5):
        check_date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
        new_url = re.sub(r'\d{8}(?=\.csv)', check_date, original_url)
        try:
            # 探测链接，已移除 proxies 参数
            r = requests.head(new_url, headers=BASE_HEADERS, timeout=10)
            if r.status_code == 200: return new_url
        except: pass
    return None

print("📥 阶段一：开始下载原始数据 (直连模式)...")

# 读取地址列表
try:
    with open('每日ETF下载地址.txt', 'r', encoding='utf-8') as f:
        lines = f.readlines()
except FileNotFoundError:
    print("❌ 错误：未找到 '每日ETF下载地址.txt' 文件。")
    exit()

for line in lines:
    line = line.strip()
    if not line: continue
    
    parts = line.split()
    url = parts[0]
    # 把 URL 后面的所有部分用空格拼接起来，作为完整的文件名
    name = " ".join(parts[1:])
    
    # 如果名字还是空的，给个默认编号防止报错
    if not name.strip():
        name = "未命名_ETF_" + str(random.randint(1000, 9999))
    
    print(f"\n正在处理: {name}")
    
    # 区分后缀名
    ext = '.xlsx' if '.xlsx' in url else '.csv'
    safe_name = re.sub(r'[\\/*?:"<>|]', '_', name)
    save_path = f"data/raw/{safe_name}{ext}"

    # 获取真实链接
    real_url = get_latest_url(url)
    if not real_url:
        print(f"   ❌ 无法定位下载链接")
        continue

    # 发起下载请求
    for attempt in range(3):
        try:
            # 随机休息，模拟真人点击
            time.sleep(random.uniform(1.5, 3))
            # 移除了 proxies 参数
            response = requests.get(real_url, headers=BASE_HEADERS, timeout=25)
            
            if response.status_code == 200:
                # 检查文件大小：如果只有几KB，很可能又是下到了免责声明
                if len(response.content) < 3000:
                    print(f"   ⚠️ 警告：下载成功但文件过小 ({len(response.content)} 字节)，可能未获取到真实持仓")
                
                with open(save_path, 'wb') as file:
                    file.write(response.content)
                print(f"   ✅ 原始文件已保存至 data/raw")
                break
            else:
                print(f"   - 尝试 {attempt+1} 失败，状态码: {response.status_code}")
        except Exception as e:
            print(f"   - 尝试 {attempt+1} 发生连接错误: {e}")
            time.sleep(3)

print("\n🎉 下载阶段结束。请进入 data/raw 文件夹手动确认原始文件内容。")