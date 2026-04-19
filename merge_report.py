import pandas as pd
import os
import glob
import time

# --- 配置区 ---
CLEANED_DIR = 'data/cleaned'      # step2 清洗后的文件存放处
OUTPUT_REPORT = '最终合并持仓报告.csv'
DAYS_TO_KEEP = 3                 # 原始数据保留天数
DOWNLOADS_DIR = 'downloads'      # 需要清理的原始下载目录

def merge_all_holdings():
    """
    将所有清洗后的单只 ETF 持仓文件合并成一个大宽表
    """
    print("🔄 启动流水线合并模块 (merge_report.py)...")
    
    # 扫描所有清洗后的 CSV
    all_cleaned_files = glob.glob(os.path.join(CLEANED_DIR, '*.csv'))
    
    if not all_cleaned_files:
        print("❌ 致命错误：没有找到任何清洗好的 CSV 文件！请检查 Step 2 是否跑通。")
        return False

    print(f"🚀 正在聚合 {len(all_cleaned_files)} 个 ETF 的持仓底牌...")
    
    combined_list = []
    for f in all_cleaned_files:
        try:
            df = pd.read_csv(f)
            combined_list.append(df)
        except Exception as e:
            print(f"⚠️ 跳过破损文件 {f}: {e}")

    # 合并数据
    master_df = pd.concat(combined_list, ignore_index=True)
    
    # 按照市值排序，方便观察权重股
    if 'Market_Value' in master_df.columns:
        master_df = master_df.sort_values(by='Market_Value', ascending=False)

    # 保存最终大表
    master_df.to_csv(OUTPUT_REPORT, index=False, encoding='utf-8-sig')
    print(f"✅ 全量合并完成！总行数: {len(master_df)}，已保存至: {OUTPUT_REPORT}")
    return True

def auto_cleanup(days=DAYS_TO_KEEP):
    """
    清理 3 天前的旧数据，确保 GitHub 仓库不会爆仓
    """
    print(f"🧹 启动自动清理程序：正在检查超过 {days} 天的旧文件...")
    now = time.time()
    cutoff = now - (days * 86400)
    
    count = 0
    # 我们需要清理两个地方：原始下载区 和 中间清洗区
    target_folders = [DOWNLOADS_DIR, CLEANED_DIR]
    
    for folder in target_folders:
        if not os.path.exists(folder):
            continue
            
        for root, dirs, files in os.walk(folder):
            for file in files:
                file_path = os.path.join(root, file)
                # 检查文件最后修改时间
                if os.path.getmtime(file_path) < cutoff:
                    try:
                        os.remove(file_path)
                        count += 1
                    except Exception as e:
                        print(f"⚠️ 无法删除 {file}: {e}")
                        
    if count > 0:
        print(f"✨ 清理完毕：共移除了 {count} 个过期原始 CSV 文件。")
    else:
        print("📁 未发现过期文件，仓库保持现状。")

# --- 执行入口 ---
if __name__ == "__main__":
    # 1. 执行合并
    success = merge_all_holdings()
    
    # 2. 只有合并成功后，才执行清理动作
    if success:
        auto_cleanup(DAYS_TO_KEEP)
    else:
        print("🚫 由于合并失败，已跳过自动清理，以便人工排查原始数据。")
