# config.py
# 共用設定檔，用於 ETL (order_processing_script.py) 與 BigQuery 上傳 (upload_to_bq.py)
# ========================================================================
import traceback
from google.cloud.bigquery import SchemaField

# ===== 1. ETL 主流程設定 (order_processing_script.py) =====

# 資料夾路徑設定，請修改為本機實際路徑
INPUT_DIR       = r"C:\Users\user\Documents\shopee_orders_etl\input"
OUTPUT_DIR      = r"C:\Users\user\Documents\shopee_orders_etl\output"
ARCHIVE_DIR     = r"C:\Users\user\Documents\shopee_orders_etl\archive"

# 主檔與孤兒檔 CSV 路徑
OUTPUT_CSV_PATH = r"C:\Users\user\Documents\shopee_orders_etl\output\A01_master_orders_cleaned.csv"
ORPHAN_CSV_PATH = r"C:\Users\user\Documents\shopee_orders_etl\output\A01_orphaned_orders.csv"

# Excel 原始欄位名稱 → DataFrame 欄位對應（根據實際 Excel 欄位修正）
COLUMN_MAPPING = {
    "訂單編號":                              "order_sn",
    "訂單狀態":                             "order_status",
    "不成立原因":                           "cancellation_reason",
    "退貨 / 退款狀態":                      "return_refund_status",
    "買家帳號":                             "buyer_username",
    "訂單成立日期":                         "order_creation_timestamp",
    "商品總價":                             "product_total_price",
    "買家支付運費":                         "buyer_paid_shipping_fee",
    "蝦皮補助運費":                         "shopee_shipping_subsidy",
    "退貨運費":                             "return_shipping_fee",
    "買家總支付金額":                       "total_amount_paid_by_buyer",
    "蝦皮補貼金額":                         "shopee_subsidy_amount",
    "蝦幣折抵":                             "shopee_coin_offset",
    "銀行信用卡活動折抵":                   "credit_card_promotion_discount",
    "優惠代碼":                             "voucher_code",
    "賣場優惠券":                           "seller_voucher",
    "賣家蝦幣回饋券":                       "seller_coin_cashback_voucher",
    "優惠券":                               "voucher",
    "成交手續費":                           "transaction_fee",
    "其他服務費":                           "other_service_fee",
    "金流與系統處理費":                     "payment_processing_fee",
    "分期付款期數":                         "installment_plan_periods",
    "金流處理費率":                         "payment_processing_fee_rate",  # 原來的欄位名稱
    "金流與系統處理費率":                   "payment_processing_fee_rate",  # 新的欄位名稱
    "成交手續費規則名稱":                   "transaction_fee_rule_name",
    "商品名稱":                             "product_name",
    "商品選項名稱":                         "product_variation",
    "商品原價":                             "product_original_price",
    "商品活動價格":                         "product_campaign_price",
    "主商品貨號":                           "product_sku_main",
    "商品選項貨號":                         "product_sku_variation",
    "數量":                                 "quantity",
    "退貨數量":                             "return_quantity",
    "促銷組合指標":                         "promo_bundle_indicator",
    "蝦皮促銷組合折扣:促銷組合標籤":        "promo_bundle_discount_label",
    "收件地址":                             "recipient_address",
    # 處理包含換行符的欄位名稱
    "收件者電話":                           "recipient_phone",
    "蝦皮專線和包裹查詢碼":                 "shopee_hotline_and_tracking_code",
    "取件門市店號":                         "pickup_store_id",
    "城市":                                 "recipient_city",
    "行政區":                               "recipient_district",
    "郵遞區號":                             "recipient_postal_code",
    "收件者姓名":                           "recipient_name",
    "寄送方式":                             "shipping_method",
    "出貨方式":                             "shipping_provider",
    "備貨時間":                             "days_to_ship",
    "付款方式":                             "payment_method",
    "最晚出貨日期":                         "ship_by_date",
    "包裹查詢號碼":                         "tracking_number",
    "買家付款時間":                         "buyer_payment_timestamp",
    "實際出貨時間":                         "actual_shipping_timestamp",
    "訂單完成時間":                         "order_completion_timestamp",
    "買家備註":                             "buyer_note",
    "備註":                                 "seller_note",
}

# 最終輸出的欄位順序
FINAL_COLUMN_ORDER = [
    "shop_name",
    "shop_account",
    "processing_date",
    "order_date",
    "order_sn",
    "order_status",
    "cancellation_reason",
    "return_refund_status",
    "buyer_username",
    "order_creation_timestamp",
    "product_total_price",
    "buyer_paid_shipping_fee",
    "shopee_shipping_subsidy",
    "return_shipping_fee",
    "total_amount_paid_by_buyer",
    "shopee_subsidy_amount",
    "shopee_coin_offset",
    "credit_card_promotion_discount",
    "voucher_code",
    "seller_voucher",
    "seller_coin_cashback_voucher",
    "voucher",
    "transaction_fee",
    "other_service_fee",
    "payment_processing_fee",
    "installment_plan_periods",
    "payment_processing_fee_rate",
    "transaction_fee_rule_name",
    "product_name",
    "product_variation",
    "product_original_price",
    "product_campaign_price",
    "product_sku_main",
    "product_sku_variation",
    "quantity",
    "return_quantity",
    "promo_bundle_indicator",
    "promo_bundle_discount_label",
    "recipient_address",
    "recipient_phone",
    "shopee_hotline_and_tracking_code",
    "pickup_store_id",
    "recipient_city",
    "recipient_district",
    "recipient_postal_code",
    "recipient_name",
    "shipping_method",
    "shipping_provider",
    "days_to_ship",
    "payment_method",
    "ship_by_date",
    "tracking_number",
    "buyer_payment_timestamp",
    "actual_shipping_timestamp",
    "order_completion_timestamp",
    "buyer_note",
    "seller_note",
]

# ===== 2. BigQuery 上傳設定 (upload_to_bq.py) =====
UPLOAD_CSV_PATH = r"C:\Users\user\Documents\shopee_orders_etl\upload_ready\A01_master_orders_cleaned.csv"

# GCP Service Account 金鑰 JSON 檔路徑
BQ_KEY_PATH     = r"C:\Users\user\Documents\shopee_orders_etl\scripts\shopee-etl-reporting-9531f3a7678a.json"

# BigQuery 目標資訊
PROJECT_ID      = "shopee-etl-reporting"
DATASET_ID      = "shopee_data"
TABLE_ID        = "a01_master_orders"

# BigQuery Schema 定義（保留欄位說明）
BQ_SCHEMA = [
    SchemaField("shop_name", "STRING", description="店鋪名稱"),
    SchemaField("shop_account", "STRING", description="店鋪帳號"),
    SchemaField("processing_date", "DATE", description="腳本處理日期"),
    SchemaField("order_date", "DATE", description="訂單日期 (從訂單編號解析)"),
    SchemaField("order_sn", "STRING", description="訂單編號"),
    SchemaField("order_status", "STRING", description="訂單狀態"),
    SchemaField("cancellation_reason", "STRING", description="不成立原因"),
    SchemaField("return_refund_status", "STRING", description="退貨 / 退款狀態"),
    SchemaField("buyer_username", "STRING", description="買家帳號"),
    SchemaField("order_creation_timestamp", "TIMESTAMP", description="訂單成立日期"),
    SchemaField("product_total_price", "FLOAT64", description="商品總價"),
    SchemaField("buyer_paid_shipping_fee", "FLOAT64", description="買家支付運費"),
    SchemaField("shopee_shipping_subsidy", "FLOAT64", description="蝦皮補助運費"),
    SchemaField("return_shipping_fee", "FLOAT64", description="退貨運費"),
    SchemaField("total_amount_paid_by_buyer", "FLOAT64", description="買家總支付金額"),
    SchemaField("shopee_subsidy_amount", "FLOAT64", description="蝦皮補貼金額"),
    SchemaField("shopee_coin_offset", "FLOAT64", description="蝦幣折抵"),
    SchemaField("credit_card_promotion_discount", "FLOAT64", description="銀行信用卡活動折抵"),
    SchemaField("voucher_code", "STRING", description="優惠代碼"),
    SchemaField("seller_voucher", "STRING", description="賣場優惠券"),
    SchemaField("seller_coin_cashback_voucher", "STRING", description="賣家蝦幣回饋券"),
    SchemaField("voucher", "STRING", description="優惠券"),
    SchemaField("transaction_fee", "FLOAT64", description="成交手續費"),
    SchemaField("other_service_fee", "FLOAT64", description="其他服務費"),
    SchemaField("payment_processing_fee", "FLOAT64", description="金流與系統處理費"),
    SchemaField("installment_plan_periods", "INT64", description="分期付款期數"),
    SchemaField("payment_processing_fee_rate", "FLOAT64", description="金流處理費率"),
    SchemaField("transaction_fee_rule_name", "STRING", description="成交手續費規則名稱"),
    SchemaField("product_name", "STRING", description="商品名稱"),
    SchemaField("product_variation", "STRING", description="商品選項名稱"),
    SchemaField("product_original_price", "FLOAT64", description="商品原價"),
    SchemaField("product_campaign_price", "FLOAT64", description="商品活動價格"),
    SchemaField("product_sku_main", "STRING", description="主商品貨號"),
    SchemaField("product_sku_variation", "STRING", description="商品選項貨號"),
    SchemaField("quantity", "INT64", description="數量"),
    SchemaField("return_quantity", "INT64", description="退貨數量"),
    SchemaField("promo_bundle_indicator", "STRING", description="促銷組合指標"),
    SchemaField("promo_bundle_discount_label", "STRING", description="蝦皮促銷組合折扣:促銷組合標籤"),
    SchemaField("recipient_address", "STRING", description="收件地址"),
    SchemaField("recipient_phone", "STRING", description="收件者電話"),
    SchemaField("shopee_hotline_and_tracking_code", "STRING", description="蝦皮專線和包裹查詢碼"),
    SchemaField("pickup_store_id", "STRING", description="取件門市店號"),
    SchemaField("recipient_city", "STRING", description="城市"),
    SchemaField("recipient_district", "STRING", description="行政區"),
    SchemaField("recipient_postal_code", "STRING", description="郵遞區號"),
    SchemaField("recipient_name", "STRING", description="收件者姓名"),
    SchemaField("shipping_method", "STRING", description="寄送方式"),
    SchemaField("shipping_provider", "STRING", description="出貨方式"),
    SchemaField("days_to_ship", "INT64", description="備貨時間"),
    SchemaField("payment_method", "STRING", description="付款方式"),
    SchemaField("ship_by_date", "DATE", description="最晚出貨日期"),
    SchemaField("tracking_number", "STRING", description="包裹查詢號碼"),
    SchemaField("buyer_payment_timestamp", "TIMESTAMP", description="買家付款時間"),
    SchemaField("actual_shipping_timestamp", "TIMESTAMP", description="實際出貨時間"),
    SchemaField("order_completion_timestamp", "TIMESTAMP", description="訂單完成時間"),
    SchemaField("buyer_note", "STRING", description="買家備註"),
    SchemaField("seller_note", "STRING", description="賣家備註"),
]