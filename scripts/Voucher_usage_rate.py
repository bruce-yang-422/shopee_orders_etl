import pandas as pd
import numpy as np
from datetime import datetime

# ==== 設定參數 (可自行調整) ====
file_path = r'C:\Users\user\Documents\shopee_orders_etl\output\A01_master_orders_cleaned_for_bigquery.csv'

# 日期區間設定
start_date = '2025-05-01'  # 開始日期
end_date = '2025-06-30'    # 結束日期

# 選擇要分析的欄位 (可選擇其中一個或兩個都分析)
analyze_columns = [
    'seller_voucher',  # 賣家優惠券
    'voucher'          # 蝦皮優惠券
]

print(f"=== 分析設定 ===")
print(f"檔案路徑: {file_path}")
print(f"日期區間: {start_date} 到 {end_date}")
print(f"分析欄位: {', '.join(analyze_columns)}")
print("=" * 50)

# ==== 讀取資料 ====
print("正在讀取資料...")
df = pd.read_csv(file_path, dtype=str)
print(f"原始資料筆數: {len(df):,}")

# ==== 步驟 1: 按 order_sn 去重，保留第一筆 ====
print("\n步驟 1: 按 order_sn 去重...")
df_dedup = df.drop_duplicates(subset=['order_sn'], keep='first')
print(f"去重後筆數: {len(df_dedup):,}")
print(f"去除了 {len(df) - len(df_dedup):,} 筆重複資料")

# ==== 步驟 2: 篩選日期區間 ====
print(f"\n步驟 2: 篩選日期區間 ({start_date} ~ {end_date})...")
# 將 order_date 轉換為日期格式
df_dedup['order_date'] = pd.to_datetime(df_dedup['order_date'], errors='coerce')
# 篩選指定日期區間的資料
start_dt = pd.to_datetime(start_date)
end_dt = pd.to_datetime(end_date)
df_filtered = df_dedup[(df_dedup['order_date'] >= start_dt) & 
                       (df_dedup['order_date'] <= end_dt)].copy()

print(f"篩選後筆數: {len(df_filtered):,}")
print(f"過濾掉 {len(df_dedup) - len(df_filtered):,} 筆區間外的資料")

# 顯示實際日期範圍
if len(df_filtered) > 0:
    actual_min = df_filtered['order_date'].min()
    actual_max = df_filtered['order_date'].max()
    print(f"實際資料日期範圍: {actual_min.strftime('%Y-%m-%d')} 到 {actual_max.strftime('%Y-%m-%d')}")

# ==== 步驟 3: 分析各個 Voucher 欄位 ====
def analyze_voucher_column(df, column_name, column_desc):
    """分析單一 voucher 欄位"""
    print(f"\n{'='*60}")
    print(f"=== {column_desc} ({column_name}) 分析 ===")
    print(f"{'='*60}")
    
    if column_name not in df.columns:
        print(f"錯誤: 找不到 '{column_name}' 欄位！")
        print("可用欄位:", [col for col in df.columns if 'voucher' in col.lower()])
        return
    
    # 將欄位轉換為數字
    df[f'{column_name}_numeric'] = pd.to_numeric(df[column_name], errors='coerce')
    
    # 分類統計
    voucher_gt_zero = (df[f'{column_name}_numeric'] > 0).sum()  # 大於 0
    voucher_eq_zero = (df[f'{column_name}_numeric'] == 0).sum()  # 等於 0
    voucher_null = df[f'{column_name}_numeric'].isna().sum()     # 空值或無法轉換
    
    total_orders = len(df)
    
    print(f"總訂單數: {total_orders:,}")
    print(f"")
    print(f"有使用 {column_desc} (>0):     {voucher_gt_zero:,} 筆 ({voucher_gt_zero/total_orders*100:.1f}%)")
    print(f"未使用 {column_desc} (=0):     {voucher_eq_zero:,} 筆 ({voucher_eq_zero/total_orders*100:.1f}%)")
    print(f"空值或異常資料:                {voucher_null:,} 筆 ({voucher_null/total_orders*100:.1f}%)")
    
    # 詳細的金額分析（僅針對有使用的）
    if voucher_gt_zero > 0:
        voucher_used = df[df[f'{column_name}_numeric'] > 0][f'{column_name}_numeric']
        print(f"\n--- 有使用 {column_desc} 的詳細分析 ---")
        print(f"最小金額: ${voucher_used.min():.2f}")
        print(f"最大金額: ${voucher_used.max():.2f}")
        print(f"平均金額: ${voucher_used.mean():.2f}")
        print(f"中位數:   ${voucher_used.median():.2f}")
        print(f"總折扣金額: ${voucher_used.sum():.2f}")
        
        # 金額分佈
        print(f"\n--- {column_desc} 金額分佈 ---")
        bins = [0, 10, 25, 50, 100, 200, 500, float('inf')]
        labels = ['1-10元', '11-25元', '26-50元', '51-100元', '101-200元', '201-500元', '500元以上']
        voucher_ranges = pd.cut(voucher_used, bins=bins, labels=labels, right=False)
        range_counts = voucher_ranges.value_counts().sort_index()
        for range_label, count in range_counts.items():
            if count > 0:
                pct = count / len(voucher_used) * 100
                print(f"  {range_label}: {count:,} 筆 ({pct:.1f}%)")
    
    # 檢查原始資料中的常見值
    print(f"\n--- 原始 {column_name} 值檢查 ---")
    voucher_values = df[column_name].value_counts().head(10)
    print(f"最常見的 {column_name} 值:")
    for value, count in voucher_values.items():
        print(f"  '{value}': {count:,} 筆")
    
    # 找出異常值
    invalid_vouchers = df[df[f'{column_name}_numeric'].isna() & df[column_name].notna()]
    if len(invalid_vouchers) > 0:
        print(f"\n無法轉換為數字的異常 {column_name} 值:")
        unique_invalid = invalid_vouchers[column_name].unique()[:5]
        for value in unique_invalid:
            count = (invalid_vouchers[column_name] == value).sum()
            print(f"  '{value}': {count} 筆")

# 執行分析
if len(df_filtered) > 0:
    for column in analyze_columns:
        if column == 'seller_voucher':
            analyze_voucher_column(df_filtered, column, '賣家優惠券')
        elif column == 'voucher':
            analyze_voucher_column(df_filtered, column, '蝦皮優惠券')
        else:
            analyze_voucher_column(df_filtered, column, column)
    
    # ==== 綜合比較分析 ====
    if len(analyze_columns) > 1:
        print(f"\n{'='*60}")
        print(f"=== 綜合比較分析 ===")
        print(f"{'='*60}")
        
        summary_data = []
        for column in analyze_columns:
            if column in df_filtered.columns:
                numeric_col = f'{column}_numeric'
                if numeric_col in df_filtered.columns:
                    used_count = (df_filtered[numeric_col] > 0).sum()
                    used_pct = used_count / len(df_filtered) * 100
                    avg_amount = df_filtered[df_filtered[numeric_col] > 0][numeric_col].mean()
                    
                    desc = '賣家優惠券' if column == 'seller_voucher' else '蝦皮優惠券' if column == 'voucher' else column
                    summary_data.append({
                        '優惠券類型': desc,
                        '使用筆數': f"{used_count:,}",
                        '使用率': f"{used_pct:.1f}%",
                        '平均金額': f"${avg_amount:.2f}" if not pd.isna(avg_amount) else "N/A"
                    })
        
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            print(summary_df.to_string(index=False))

else:
    print("警告: 篩選後沒有資料！請檢查日期區間設定。")

# ==== 輸出摘要 ====
print(f"\n{'='*60}")
print(f"=== 處理摘要 ===")
print(f"原始資料: {len(df):,} 筆")
print(f"去重後: {len(df_dedup):,} 筆")
print(f"日期篩選後: {len(df_filtered):,} 筆")
print(f"分析完成！")
print(f"{'='*60}")