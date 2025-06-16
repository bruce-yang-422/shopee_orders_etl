import pandas as pd

input_csv = r"C:\Users\user\Documents\shopee_orders_etl\output\GTD_master_orders_cleaned.csv"
output_csv = r"C:\Users\user\Documents\shopee_orders_etl\output\GTD_master_orders_order_date_invalid.csv"

# 讀取資料
df = pd.read_csv(input_csv, dtype=str)

# 嘗試轉換 order_date，errors='coerce' 會將無法轉換的設成 NaT
df['order_date_converted'] = pd.to_datetime(df['order_date'], errors='coerce')

# 篩選出 order_date 轉換失敗的列
df_invalid = df[df['order_date_converted'].isna()]

if not df_invalid.empty:
    print(f"發現 {len(df_invalid)} 筆 order_date 無效資料，輸出至: {output_csv}")
    # 輸出異常列，保留原始 order_date 與訂單編號方便追蹤
    df_invalid[['order_sn', 'order_date']].to_csv(output_csv, index=False, encoding='utf-8-sig')
else:
    print("未發現 order_date 異常資料。")
