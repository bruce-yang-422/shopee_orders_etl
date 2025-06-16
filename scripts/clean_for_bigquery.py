import pandas as pd
import numpy as np
import os

# 1. 路徑設定
input_csv = r'C:\Users\user\Documents\shopee_orders_etl\output\A01_master_orders_cleaned.csv'
output_csv = r'C:\Users\user\Documents\shopee_orders_etl\output\A01_master_orders_cleaned_for_bigquery.csv'

# 2. 讀取資料
df = pd.read_csv(input_csv, dtype=str)  # 全部欄位用字串讀，保留原始格式

# 3. 把常見的 nan, NaN, None, <空字串> 等轉為空字串
df = df.replace(['nan', 'NaN', 'None', np.nan], '')

# 4. 或者，如果你要 BigQuery 欄位呈現 NULL，可以用下列方式
# df = df.where(pd.notnull(df), None)

# 5. 輸出乾淨檔案
df.to_csv(output_csv, index=False, encoding='utf-8-sig')

print(f"清理完成！檔案已儲存至：{output_csv}")
