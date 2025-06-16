import os
import pandas as pd

# 腳本名稱：split_b2b_orders.py
# 用途：將指定的特殊平台訂單（B2B平台）從主訂單檔案中分離出來，
#       另存為獨立檔案，並在原檔案中刪除這些B2B訂單資料。
# 執行邏輯：
#   1. 讀取主訂單檔案 A01_master_orders_cleaned.csv
#   2. 根據 shop_name 欄位判斷是否屬於 B2B 平台（MOMO購物中心、PC購物中心、Yahoo購物中心、東森購物）
#   3. 將 B2B 平台的訂單資料另存為 A01_master_orders_cleaned_B2B.csv
#   4. 將非 B2B 的訂單資料覆寫回原本的 A01_master_orders_cleaned.csv

# 設定檔案路徑
input_path = r"C:\Users\user\Documents\shopee_orders_etl\output\A01_master_orders_cleaned.csv"
output_path_b2b = r"C:\Users\user\Documents\shopee_orders_etl\output\A01_master_orders_cleaned_B2B.csv"

# 定義特殊平台清單（B2B平台）
SPECIAL_PLATFORMS = ['MOMO購物中心', 'PC購物中心', 'Yahoo購物中心', '東森購物']

# 讀取主訂單CSV檔案，所有欄位以字串格式讀取
df = pd.read_csv(input_path, dtype=str)

# 篩選出屬於B2B平台的訂單資料
df_b2b = df[df['shop_name'].isin(SPECIAL_PLATFORMS)].copy()

# 篩選出非B2B平台的訂單資料
df_normal = df[~df['shop_name'].isin(SPECIAL_PLATFORMS)].copy()

# 將B2B平台的訂單資料另存新檔
df_b2b.to_csv(output_path_b2b, index=False, encoding='utf-8-sig')
print(f"已輸出 B2B 檔案：{output_path_b2b}，共 {len(df_b2b)} 筆資料")

# 將非B2B訂單資料覆寫回原主訂單檔案
df_normal.to_csv(input_path, index=False, encoding='utf-8-sig')
print(f"原檔已更新，剩餘非 B2B 資料共 {len(df_normal)} 筆")
