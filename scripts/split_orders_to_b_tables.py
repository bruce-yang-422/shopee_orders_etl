import pandas as pd

# ==== 1. 設定檔案路徑與表單名稱 ====
input_path = r'C:\Users\user\Documents\shopee_orders_etl\output\A01_master_orders_cleaned_for_bigquery.csv'
output_folder = r'C:\Users\user\Documents\shopee_orders_etl\output'
b01_name = 'B01_orders_concat.csv'
b02_name = 'B02_order_details.csv'
b03_name = 'B03_order_simple_details.csv'
b04_name = 'B04_order_shipping_info.csv'

# ==== 2. 讀取資料 ====
df = pd.read_csv(input_path, dtype=str).fillna('')

# ==== 3. B01 聚合表（訂單主體聚合） ====
b01_cols = [
    'order_sn','shop_name','shop_account','processing_date','order_date','order_status','cancellation_reason',
    'return_refund_status','buyer_username','order_creation_timestamp','total_amount_paid_by_buyer','voucher',
    'product_name','product_sku_main','quantity','return_quantity','buyer_note','seller_note'
]
b01 = df[b01_cols].copy()

# 修正：按 order_sn 分組聚合，其他欄位取第一個值
b01_grouped = b01.groupby('order_sn', as_index=False).agg({
    'shop_name': 'first',
    'shop_account': 'first', 
    'processing_date': 'first',
    'order_date': 'first',
    'order_status': 'first',
    'cancellation_reason': 'first',
    'return_refund_status': 'first',
    'buyer_username': 'first',
    'order_creation_timestamp': 'first',
    'total_amount_paid_by_buyer': 'first',  # 避免重複計算，取第一個值即可
    'voucher': 'first',
    'buyer_note': 'first',
    'seller_note': 'first',
    'product_name': lambda x: ';'.join([i for i in x if i]),
    'product_sku_main': lambda x: ';'.join([i for i in x if i]),
    'quantity': lambda x: str(sum(pd.to_numeric(x, errors='coerce').fillna(0))),
    'return_quantity': lambda x: str(sum(pd.to_numeric(x, errors='coerce').fillna(0))),
})

b01_grouped = b01_grouped.rename(columns={
    'product_name': 'product_name_list',
    'product_sku_main': 'product_sku_main_list',
    'quantity': 'total_quantity',
    'return_quantity': 'total_return_quantity'
})
b01_grouped.to_csv(f'{output_folder}\\{b01_name}', index=False, encoding='utf-8-sig')
print(f'B01 聚合表完成：{output_folder}\\{b01_name} ({len(b01_grouped)} 筆訂單)')

# ==== 4. B02 明細表（含 SKU 規格）- 保持原樣，不去重 ====
b02_cols = [
    'order_sn','shop_name','shop_account','processing_date','order_date','order_status','cancellation_reason','return_refund_status',
    'buyer_username','order_creation_timestamp','product_total_price','total_amount_paid_by_buyer','product_name','product_variation',
    'product_original_price','product_campaign_price','product_sku_main','product_sku_variation','quantity','return_quantity',
    'promo_bundle_indicator','promo_bundle_discount_label','buyer_note','seller_note'
]
b02 = df[b02_cols].copy()
b02.to_csv(f'{output_folder}\\{b02_name}', index=False, encoding='utf-8-sig')
print(f'B02 明細表完成：{output_folder}\\{b02_name} ({len(b02)} 筆明細)')

# ==== 5. B03 簡化明細表（按 order_sn 去重） ====
b03_cols = [
    'order_sn','shop_name','shop_account','processing_date','order_date','order_status','cancellation_reason','return_refund_status',
    'buyer_username','order_creation_timestamp','product_total_price','total_amount_paid_by_buyer','product_name','product_variation',
    'product_original_price','product_campaign_price','product_sku_main','quantity','return_quantity','promo_bundle_indicator',
    'promo_bundle_discount_label','buyer_note','seller_note'
]
b03 = df[b03_cols].copy()

# 修正：按 order_sn 去重，取第一筆記錄
b03_dedup = b03.drop_duplicates(subset=['order_sn'], keep='first')
b03_dedup.to_csv(f'{output_folder}\\{b03_name}', index=False, encoding='utf-8-sig')
print(f'B03 簡化明細表完成：{output_folder}\\{b03_name} ({len(b03_dedup)} 筆訂單，原始 {len(b03)} 筆)')

# ==== 6. B04 收件/物流資訊表（按 order_sn 去重） ====
b04_cols = [
    'order_sn','shop_name','shop_account','processing_date','order_date','order_status','cancellation_reason','return_refund_status',
    'buyer_username','order_creation_timestamp','recipient_address','recipient_phone','shopee_hotline_and_tracking_code','pickup_store_id',
    'recipient_city','recipient_district','recipient_postal_code','recipient_name','shipping_method','shipping_provider','days_to_ship',
    'payment_method','ship_by_date','tracking_number','buyer_payment_timestamp','actual_shipping_timestamp','order_completion_timestamp',
    'buyer_note','seller_note'
]
b04 = df[b04_cols].copy()

# 台灣全縣市對應 Looker 指標
city_mapping = {
    '臺北市': 'Taipei City', '台北市': 'Taipei City',
    '新北市': 'New Taipei City',
    '桃園市': 'Taoyuan City',
    '基隆市': 'Keelung City',
    '新竹市': 'Hsinchu City',
    '新竹縣': 'Hsinchu County',
    '苗栗縣': 'Miaoli County',
    '臺中市': 'Taichung City', '台中市': 'Taichung City',
    '彰化縣': 'Changhua County',
    '南投縣': 'Nantou County',
    '雲林縣': 'Yunlin County',
    '嘉義市': 'Chiayi City',
    '嘉義縣': 'Chiayi County',
    '臺南市': 'Tainan City', '台南市': 'Tainan City',
    '高雄市': 'Kaohsiung City',
    '屏東縣': 'Pingtung County',
    '宜蘭縣': 'Yilan County',
    '花蓮縣': 'Hualien County',
    '臺東縣': 'Taitung County', '台東縣': 'Taitung County',
    '澎湖縣': 'Penghu County',
    '金門縣': 'Kinmen County',
    '連江縣': 'Lienchiang County',
}

# 直接將 recipient_city 欄位內容轉換成 Looker 格式
b04['recipient_city'] = b04['recipient_city'].map(city_mapping).fillna(b04['recipient_city'])

# 修正：按 order_sn 去重，取第一筆記錄
b04_dedup = b04.drop_duplicates(subset=['order_sn'], keep='first')
b04_dedup.to_csv(f'{output_folder}\\{b04_name}', index=False, encoding='utf-8-sig')
print(f'B04 收件/物流表完成：{output_folder}\\{b04_name} ({len(b04_dedup)} 筆訂單，原始 {len(b04)} 筆)')

print('\n所有表格處理完成！')
print(f'- B01 聚合表：{len(b01_grouped)} 筆')
print(f'- B02 明細表：{len(b02)} 筆') 
print(f'- B03 簡化表：{len(b03_dedup)} 筆')
print(f'- B04 物流表：{len(b04_dedup)} 筆')

# 驗證金額是否正確（避免重複計算）
print(f'\n金額驗證：')
print(f'- 原始資料總金額（可能重複）: {df["total_amount_paid_by_buyer"].astype(float).sum():,.0f}')
print(f'- B01聚合後正確總金額: {b01_grouped["total_amount_paid_by_buyer"].astype(float).sum():,.0f}')