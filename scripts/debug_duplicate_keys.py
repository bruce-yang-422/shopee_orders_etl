# debug_duplicate_keys.py
# 除錯重複主鍵問題
# ===================

import pandas as pd
import os

try:
    from config import OUTPUT_CSV_PATH, INPUT_DIR, COLUMN_MAPPING
except ImportError:
    print("❌ 錯誤：無法從 config.py 導入設定。")
    exit()

def clean_column_names(df):
    """清理欄位名稱"""
    cleaned_columns = {}
    for col in df.columns:
        cleaned_col = col.replace('\n', '').replace('\r', '').strip()
        
        if '若您是自行配送請使用後方蝦皮專線和包裹查詢碼聯繫買家' in cleaned_col:
            cleaned_col = '收件者電話'
        elif '請複製下方完整編號提供給您配合的物流商當做聯絡電話' in cleaned_col:
            cleaned_col = '蝦皮專線和包裹查詢碼'
        
        cleaned_columns[col] = cleaned_col
    
    df.rename(columns=cleaned_columns, inplace=True)
    return df

def create_robust_composite_key(df):
    """建立複合主鍵"""
    key_components = [
        df['order_sn'].fillna('').astype(str).str.strip(),
        df['product_sku_variation'].fillna('NULL_SKU').astype(str).str.strip().replace('', 'NULL_SKU'),
        df['product_name'].fillna('').astype(str).str.strip()
    ]
    df['composite_key'] = pd.Series(zip(*key_components)).str.join('|||')
    return df

def debug_key_generation():
    """除錯主鍵生成問題"""
    print("🔍 除錯複合主鍵生成問題")
    print("=" * 60)
    
    # 1. 讀取現有主檔
    if not os.path.exists(OUTPUT_CSV_PATH):
        print("❌ 主檔不存在")
        return
    
    df_old = pd.read_csv(OUTPUT_CSV_PATH, keep_default_na=False, dtype=str)
    print(f"📄 主檔載入: {len(df_old)} 筆資料")
    
    # 2. 讀取新的 Excel 檔案
    excel_files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.xlsx')]
    if not excel_files:
        print("❌ 沒有找到 Excel 檔案")
        return
    
    excel_path = os.path.join(INPUT_DIR, excel_files[0])
    df_new = pd.read_excel(excel_path, dtype=str)
    df_new = clean_column_names(df_new)
    df_new.rename(columns=COLUMN_MAPPING, inplace=True)
    
    print(f"📄 新檔載入: {len(df_new)} 筆資料")
    
    # 3. 篩選相同日期範圍的資料
    # 處理日期
    df_new['order_date'] = pd.to_datetime(
        df_new['order_sn'].str.slice(0, 6),
        format='%y%m%d',
        errors='coerce'
    ).dt.date
    
    df_old['order_date_parsed'] = pd.to_datetime(df_old['order_date'], errors='coerce').dt.date
    
    # 確定新資料的日期範圍
    new_date_min = df_new['order_date'].min()
    new_date_max = df_new['order_date'].max()
    print(f"📅 新資料日期範圍: {new_date_min} 到 {new_date_max}")
    
    # 篩選舊資料中相同日期範圍的部分
    mask_same_range = (
        (df_old['order_date_parsed'] >= new_date_min) & 
        (df_old['order_date_parsed'] <= new_date_max)
    )
    df_old_same_range = df_old[mask_same_range].copy()
    print(f"📅 主檔中相同日期範圍的資料: {len(df_old_same_range)} 筆")
    
    # 4. 生成複合主鍵
    df_old_same_range = create_robust_composite_key(df_old_same_range)
    df_new = create_robust_composite_key(df_new)
    
    print(f"\n🔑 複合主鍵生成結果:")
    print(f"   舊資料 (相同日期範圍): {df_old_same_range['composite_key'].nunique()} 個唯一主鍵")
    print(f"   新資料: {df_new['composite_key'].nunique()} 個唯一主鍵")
    
    # 5. 比較主鍵
    old_keys = set(df_old_same_range['composite_key'])
    new_keys = set(df_new['composite_key'])
    
    print(f"\n🔄 主鍵比較:")
    print(f"   舊資料主鍵數: {len(old_keys)}")
    print(f"   新資料主鍵數: {len(new_keys)}")
    print(f"   共同主鍵數: {len(old_keys & new_keys)}")
    print(f"   舊資料獨有: {len(old_keys - new_keys)}")
    print(f"   新資料獨有: {len(new_keys - old_keys)}")
    
    # 6. 分析消失的主鍵
    disappeared_keys = old_keys - new_keys
    if disappeared_keys:
        print(f"\n❌ 消失的主鍵 ({len(disappeared_keys)} 個):")
        disappeared_records = df_old_same_range[df_old_same_range['composite_key'].isin(disappeared_keys)]
        
        for i, (idx, record) in enumerate(disappeared_records.iterrows()):
            if i >= 5:  # 只顯示前5個
                print(f"   ... 還有 {len(disappeared_records) - 5} 筆")
                break
            
            print(f"\n   {i+1}. 訂單: {record.get('order_sn', 'N/A')}")
            print(f"      商品: {record.get('product_name', 'N/A')[:50]}...")
            print(f"      SKU: {record.get('product_sku_variation', 'N/A')}")
            print(f"      複合主鍵: {record['composite_key']}")
            
            # 檢查是否在新資料中有相似的記錄
            similar_new = df_new[
                (df_new['order_sn'] == record.get('order_sn', '')) &
                (df_new['product_name'] == record.get('product_name', ''))
            ]
            
            if len(similar_new) > 0:
                print(f"      🔍 新資料中找到相似記錄:")
                for _, new_record in similar_new.iterrows():
                    print(f"         新SKU: {new_record.get('product_sku_variation', 'N/A')}")
                    print(f"         新主鍵: {new_record['composite_key']}")
                    
                    # 比較差異
                    if record.get('product_sku_variation', '') != new_record.get('product_sku_variation', ''):
                        print(f"         ❗ SKU 不同: '{record.get('product_sku_variation', '')}' vs '{new_record.get('product_sku_variation', '')}'")
    
    # 7. 檢查資料類型問題
    print(f"\n🔬 資料類型分析:")
    
    # 檢查 SKU 欄位的資料類型
    old_sku_types = df_old_same_range['product_sku_variation'].apply(type).value_counts()
    new_sku_types = df_new['product_sku_variation'].apply(type).value_counts()
    
    print(f"   舊資料 SKU 類型: {dict(old_sku_types)}")
    print(f"   新資料 SKU 類型: {dict(new_sku_types)}")
    
    # 檢查是否有浮點數精度問題
    old_sku_sample = df_old_same_range['product_sku_variation'].dropna().head(5).tolist()
    new_sku_sample = df_new['product_sku_variation'].dropna().head(5).tolist()
    
    print(f"\n   舊資料 SKU 範例: {old_sku_sample}")
    print(f"   新資料 SKU 範例: {new_sku_sample}")

if __name__ == "__main__":
    debug_key_generation()