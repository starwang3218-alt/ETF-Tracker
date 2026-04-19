import os
import glob
import shutil
import pandas as pd

def merge_pipelines():
    print("🔄 启动流水线合并模块 (merge_report.py)...")
    
    # 1. 定义源文件夹 (第一阶段和第二阶段的产物)
    dir_invesco = "data/cleaned_invesco"
    dir_others = "data/cleaned_others" # 假设你第二步洗出来的数据在这里
    
    # 2. 定义目标文件夹 (昨天的 step3 默认读取的文件夹，通常是 data/cleaned)
    target_dir = "data/cleaned"
    os.makedirs(target_dir, exist_ok=True)
    
    # 每次合并前，先清空目标池，防止旧数据污染
    for old_file in glob.glob(os.path.join(target_dir, "*.csv")):
        os.remove(old_file)
        
    # 收集两边的文件
    files_invesco = glob.glob(os.path.join(dir_invesco, "*.csv")) if os.path.exists(dir_invesco) else []
    files_others = glob.glob(os.path.join(dir_others, "*.csv")) if os.path.exists(dir_others) else []
    
    all_files = files_invesco + files_others
    
    if not all_files:
        print("❌ 致命错误：没有找到任何清洗好的 CSV 文件！请检查 Step 1 和 Step 2 是否跑通。")
        return

    print(f"📊 发现 {len(files_invesco)} 个景顺文件，{len(files_others)} 个其他机构文件。")
    print(f"🚚 正在将文件运输至统一处理池: {target_dir} ...")
    
    dfs = []
    success_count = 0
    
    for f in all_files:
        try:
            # 【操作A：物理复制】把文件搬运到 step3 需要的文件夹
            filename = os.path.basename(f)
            shutil.copy(f, os.path.join(target_dir, filename))
            
            # 【操作B：读取数据】准备拼接总表
            df = pd.read_csv(f)
            # 确保即使空文件也不报错
            if not df.empty:
                dfs.append(df)
            success_count += 1
        except Exception as e:
            print(f"    ⚠️ 读取或复制 {f} 时出错: {e}")

    # 3. 生成最终的查账大宽表
    if dfs:
        master_df = pd.concat(dfs, ignore_index=True)
        # 顺手干掉完全为空的“幽灵行”
        master_df.dropna(how='all', inplace=True)
        
        output_file = "全市场_ETF_基础持仓宽表.csv"
        master_df.to_csv(output_file, index=False)
        
        print("-" * 40)
        print(f"🎉 合并大功告成！")
        print(f"✅ 成功聚合 {success_count} 个 ETF 标准文件至 '{target_dir}'。")
        print(f"✅ 生成总查账表 '{output_file}'，共包含 {len(master_df)} 条底层持仓明细。")
        print("➡️  数据已就绪，请立即执行: python step3_aggregate.py")
        print("-" * 40)

if __name__ == "__main__":
    merge_pipelines()
