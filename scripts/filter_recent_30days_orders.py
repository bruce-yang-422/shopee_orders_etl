import os
import pandas as pd
from datetime import datetime, timedelta

# === 資料夾設定 ===
INPUT_DIR = "output"
OUTPUT_DIR = os.path.join(INPUT_DIR, "filtered_last_30_days")
FILES_TO_FILTER = [
    "A01_master_orders_cleaned_for_bigquery.csv",
    "B01_orders_concat.csv",
    "B02_order_details.csv",
    "B03_order_simple_details.csv",
    "B04_order_shipping_info.csv"
]

# === 日期範圍（近 30 天含今天）===
today = datetime.today().date()
start_date = today - timedelta(days=29)
today_str = today.strftime('%Y-%m-%d')
start_date_str = start_date.strftime('%Y-%m-%d')

# === 建立輸出資料夾 ===
os.makedirs(OUTPUT_DIR, exist_ok=True)

def filter_recent_orders(file_path: str, output_path: str):
    try:
        df = pd.read_csv(file_path, dtype=str)
        if "order_date" not in df.columns:
            print(f"[跳過] {file_path} 沒有 order_date 欄位")
            return

        # 嘗試轉換成標準 DATE 格式
        df["order_date"] = pd.to_datetime(df["order_date"], errors='coerce').dt.strftime('%Y-%m-%d')

        # 篩選出最近 30 天的訂單
        filtered_df = df[(df["order_date"] >= start_date_str) & (df["order_date"] <= today_str)]

        # 輸出為純文字 CSV（符合 BigQuery / PostgreSQL 匯入格式）
        filtered_df.to_csv(output_path, index=False)
        print(f"[完成] {os.path.basename(output_path)} - 共 {len(filtered_df)} 筆")

    except Exception as e:
        print(f"[錯誤] 處理 {file_path} 發生錯誤：{e}")

# === 主程式 ===
if __name__ == "__main__":
    print(f"\n📆 篩選日期範圍：{start_date_str} ～ {today_str}\n")
    for filename in FILES_TO_FILTER:
        input_path = os.path.join(INPUT_DIR, filename)
        output_path = os.path.join(OUTPUT_DIR, f"last30days_{filename}")
        filter_recent_orders(input_path, output_path)
    print("\n✅ 所有檔案已完成篩選。\n")
