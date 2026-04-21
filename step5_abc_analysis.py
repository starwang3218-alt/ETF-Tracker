import pandas as pd
import os

# 1. 基础路径定义
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
# --- 重点修复：定义报告存放目录 ---
REPORT_DIR = os.path.join(BASE_DIR, 'reports') 

# 2. 确保文件夹存在（不存在就建一个，防止报错）
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

# 3. 输入文件路径
MASTER_FILE = os.path.join(DATA_DIR, 'master_holdings_analyzed.csv')

def run_abc_analysis():
    print(f"🚀 正在从 {MASTER_FILE} 读取数据进行权重排名...")
    
    if not os.path.exists(MASTER_FILE):
        print(f"❌ 找不到底表: {MASTER_FILE}")
        return

    # 读取并进行分析
    df = pd.read_csv(MASTER_FILE, encoding='utf-8-sig')
    
    # ... (这里是你中间的计算逻辑) ...
    
    print("⏳ 正在计算全市场重仓股...")
    # 假设你的计算结果叫 top_holdings
    # 这里的保存逻辑就不会报错了，因为 REPORT_DIR 已经定义好了
    output_top50 = os.path.join(REPORT_DIR, 'Report_A_全市场重仓Top50.csv')
    # top_holdings.head(50).to_csv(output_top50, index=False, encoding='utf-8-sig')
    
    print(f"✨ ABC 分析完成！报告已存入: {REPORT_DIR}")

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
