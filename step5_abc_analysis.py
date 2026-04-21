import pandas as pd
import os

# --- 核心路径修复 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 锁定你刚刚在 Step 3 中生成的“看板表”
MASTER_FILE = os.path.join(BASE_DIR, 'data', 'master_holdings_analyzed.csv')

def run_abc_analysis():
    print(f"🚀 正在从 {MASTER_FILE} 读取数据进行权重排名...")
    
    if not os.path.exists(MASTER_FILE):
        print(f"❌ 找不到底表: {MASTER_FILE}")
        return

    # 注意：你说过这里用 utf-8-sig 以防乱码
    df = pd.read_csv(MASTER_FILE, encoding='utf-8-sig')
    
    # ... 后面的分析逻辑保持不变 ...
    
    # 记得输出时也保持在 data 目录下
    output_path = os.path.join(BASE_DIR, 'data', 'abc_analysis_results.csv')
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"✨ ABC 分析完成，结果已存入: {output_path}")

if __name__ == "__main__":
    run_abc_analysis()
    print("🚀 启动 ABC 核心分析仪...")
    df = pd.read_csv(MASTER_FILE, encoding='utf-8-sig')
    
    # ==========================================
    # 🎯 功能 A：全市场重仓股穿透 (找阵眼)
    # ==========================================
    print("⏳ 正在计算全市场重仓股...")
    top_holdings = df.groupby(['Holding_Ticker', 'Holding_Name']).agg(
        Total_Market_Value=('Market_Value', 'sum'),
        Held_By_ETF_Count=('ETF_Ticker', 'nunique')
    ).reset_index().sort_values(by='Total_Market_Value', ascending=False)
    
    top_holdings.head(50).to_csv(os.path.join(REPORT_DIR, 'Report_A_全市场重仓Top50.csv'), index=False, encoding='utf-8-sig')
    print("    ✅ 报告 A 已生成：揭示机构抱团核心。")

    # ==========================================
    # 🕵️‍♂️ 功能 B：个股反向追踪 (查后台)
    # ==========================================
    # 这里预设了几个不同风格的标的作为测试：防守型(VZ, PFE)，前沿科技(BBAI, ONDS)
    target_stocks = ['VZ', 'PFE', 'BBAI', 'ONDS'] 
    
    print(f"⏳ 正在追踪特定个股的 ETF 庄家: {target_stocks}")
    reverse_track = df[df['Holding_Ticker'].isin(target_stocks)].sort_values(by=['Holding_Ticker', 'Weight_Percent'], ascending=[True, False])
    reverse_track.to_csv(os.path.join(REPORT_DIR, 'Report_B_个股反向追踪.csv'), index=False, encoding='utf-8-sig')
    print("    ✅ 报告 B 已生成：看清是谁在暗中托底或建仓。")

    # ==========================================
    # ✂️ 功能 C：主题资产挖掘 (定制化池子)
    # ==========================================
    # 寻找包含特定关键词的资产，例如核能/SMR、量子计算等
    theme_keywords = 'Nuclear|Uranium|Quantum|Solid State'
    
    print(f"⏳ 正在挖掘主题资产: {theme_keywords}")
    theme_assets = df[df['Holding_Name'].astype(str).str.contains(theme_keywords, case=False, na=False)]
    theme_summary = theme_assets.groupby(['Holding_Ticker', 'Holding_Name']).agg(
        Total_Value=('Market_Value', 'sum'),
        Held_By=('ETF_Ticker', lambda x: ', '.join(x.unique()))
    ).reset_index().sort_values(by='Total_Value', ascending=False)
    
    theme_summary.to_csv(os.path.join(REPORT_DIR, 'Report_C_主题资产挖掘.csv'), index=False, encoding='utf-8-sig')
    print("    ✅ 报告 C 已生成：前沿赛道资产池已锁定。")

if __name__ == "__main__":
    run_abc_analysis()
