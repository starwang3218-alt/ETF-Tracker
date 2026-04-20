import pandas as pd
import os
import glob
from datetime import datetime

# --- 配置区 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, 'data', 'cleaned')
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'merged')

# 确保输出目录存在
os.makedirs(OUTPUT_DIR, exist_ok=True)

def merge_all_holdings():
    print(f"🔄 启动数据大融合模块...\n📂 扫描目录: {INPUT_DIR}")
    
    # 找到所有洗干净的 csv
    all_files = glob.glob(os.path.join(INPUT_DIR, '*_cleaned.csv'))
    
    if not all_files:
        print("❌ 错误：在 cleaned 目录下没有找到任何清洗后的数据！")
        return
        
    print(f"🚀 发现 {len(all_files)} 个标准数据文件，开始合并（万剑归宗）...")
    
    df_list = []
    for f in all_files:
        try:
            # 读取时统一指定 utf-8-sig 防止乱码
            df = pd.read_csv(f, encoding='utf-8-sig')
            df_list.append(df)
        except Exception as e:
            print(f"    ⚠️ 读取失败 {os.path.basename(f)}: {e}")
            
    # 纵向合并所有数据表
    print("⏳ 正在进行全量数据拼接...")
    master_df = pd.concat(df_list, ignore_index=True)
    
    # 【核心升级】：获取今天日期，生成形如 master_holdings_20260419.csv 的文件名
    # ✨ 修改后：使用固定名称 master_holdings.csv，确保下游脚本能精准找到它
    output_file = os.path.join(OUTPUT_DIR, 'master_holdings.csv')
    master_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"\n✨ 合并大功告成！")
    print(f"📊 你的金融数据库现在总计包含 {len(master_df)} 条持仓记录！")
    print(f"💾 最终上帝视角总表已保存至: {output_file}")

if __name__ == "__main__":
    merge_all_holdings()
