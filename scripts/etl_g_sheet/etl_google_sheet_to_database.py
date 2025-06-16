import os
import sys
import pandas as pd
from datetime import datetime
from config import INPUT_DIR, OUTPUT_DIR, COLUMN_MAPPING, FINAL_COLUMN_ORDER, OUTPUT_CSV_PATH, SHOP_ACCOUNT_MAP_PATH

SPECIAL_PLATFORMS = ['MOMO購物中心', 'PC購物中心', 'Yahoo購物中心', '東森購物']

def clean_column_names(df):
    cleaned_columns = {}
    for col in df.columns:
        cleaned_col = col.replace('\n', '').replace('\r', '').strip()
        if cleaned_col == '訂單編號(從這邊貼上)':
            cleaned_col = '訂單編號'
        cleaned_columns[col] = cleaned_col
    df.rename(columns=cleaned_columns, inplace=True)
    for drop_col in ['訂單編號2', '排行', '買家總支付金額2']:
        if drop_col in df.columns:
            df.drop(columns=[drop_col], inplace=True)
    return df

def load_shop_account_map_strict():
    if not os.path.exists(SHOP_ACCOUNT_MAP_PATH):
        print(f"警告：找不到店鋪帳號對照檔 {SHOP_ACCOUNT_MAP_PATH}，shop_account 將補空字串")
        return {}, {}
    df_map = pd.read_csv(SHOP_ACCOUNT_MAP_PATH, dtype=str)
    strict_map = {}
    reverse_map = {}
    for _, row in df_map.iterrows():
        shop_name = row['shop_name']
        account = row['shop_account']
        keywords = []
        if 'keywords' in df_map.columns and pd.notna(row.get('keywords')):
            keywords = [k.strip() for k in row['keywords'].split(',')]
        else:
            keywords = []
        strict_map[shop_name] = account
        reverse_map[shop_name] = shop_name
        for kw in keywords:
            strict_map[kw] = account
            reverse_map[kw] = shop_name
    return strict_map, reverse_map

def fill_shop_account_and_name(df, strict_map, reverse_map):
    if 'shop_account' not in df.columns:
        df['shop_account'] = ''

    def find_account_and_name(shop_name):
        if pd.isna(shop_name):
            return '', ''
        account = strict_map.get(shop_name, '')
        real_name = reverse_map.get(shop_name, '')
        return account, real_name

    results = df.apply(
        lambda row: find_account_and_name(row['shop_name']) if not row.get('shop_account') else (row['shop_account'], row['shop_name']),
        axis=1,
        result_type='expand'
    )
    df['shop_account'] = results[0]
    df['shop_name'] = results[1].where(results[1] != '', df['shop_name'])
    return df

def load_and_clean_new_csv(file_path, strict_map, reverse_map):
    print(f"讀取檔案: {file_path}")
    df = pd.read_csv(file_path, dtype=str)
    print("原始欄位名稱：", df.columns.tolist())
    df = clean_column_names(df)
    print("清理後欄位名稱：", df.columns.tolist())
    df.rename(columns=COLUMN_MAPPING, inplace=True)
    print("映射後欄位名稱：", df.columns.tolist())
    df = df.loc[:, ~df.columns.duplicated()]
    if 'order_sn' not in df.columns:
        raise KeyError("欄位 'order_sn' 不存在，請確認欄位映射與欄位名稱")
    df = df[df['order_sn'].notna()]
    df = fill_shop_account_and_name(df, strict_map, reverse_map)
    df['processing_date'] = datetime.now().strftime('%Y-%m-%d')
    date_cols = [
        'order_creation_timestamp', 'buyer_payment_timestamp',
        'actual_shipping_timestamp', 'order_completion_timestamp', 'ship_by_date'
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    float_cols = [
        'product_total_price', 'buyer_paid_shipping_fee', 'shopee_shipping_subsidy',
        'return_shipping_fee', 'total_amount_paid_by_buyer'
    ]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def main():
    if len(sys.argv) > 1:
        input_filename = sys.argv[1]
    else:
        input_filename = "八里電商業績統計 - 6月銷售明細表(原始資料).csv"
    input_csv_path = os.path.join(INPUT_DIR, input_filename)
    if not os.path.exists(input_csv_path):
        print(f"錯誤：找不到輸入檔案 {input_csv_path}")
        return

    strict_map, reverse_map = load_shop_account_map_strict()
    df_new = load_and_clean_new_csv(input_csv_path, strict_map, reverse_map)

    df_special = df_new[df_new['shop_name'].isin(SPECIAL_PLATFORMS)].copy()
    df_normal = df_new[~df_new['shop_name'].isin(SPECIAL_PLATFORMS)].copy()

    # 一般平台合併
    if os.path.exists(OUTPUT_CSV_PATH):
        df_old = pd.read_csv(OUTPUT_CSV_PATH, dtype=str)
        df_old_normal = df_old[~df_old['shop_name'].isin(SPECIAL_PLATFORMS)].copy()
        df_old_normal['composite_key'] = (
            df_old_normal['order_sn'].fillna('') + '|||' +
            df_old_normal['buyer_username'].fillna('') + '|||' +
            df_old_normal['order_creation_timestamp'].fillna('')
        )
        df_normal['composite_key'] = (
            df_normal['order_sn'].fillna('') + '|||' +
            df_normal['buyer_username'].fillna('') + '|||' +
            df_normal['order_creation_timestamp'].astype(str).fillna('')
        )
        keys_to_keep = set(df_old_normal['composite_key']) - set(df_normal['composite_key'])
        df_old_filtered = df_old_normal[df_old_normal['composite_key'].isin(keys_to_keep)]
        df_merged_normal = pd.concat([df_old_filtered, df_normal], ignore_index=True)
    else:
        print("無現有主檔，直接使用新資料（一般平台）")
        df_merged_normal = df_normal

    df_merged_normal = df_merged_normal.reindex(columns=FINAL_COLUMN_ORDER, fill_value='')
    if 'composite_key' in df_merged_normal.columns:
        df_merged_normal.drop(columns=['composite_key'], inplace=True)

    # 特殊平台合併
    output_special_path = OUTPUT_CSV_PATH.replace('.csv', '_B2B_special.csv')
    if os.path.exists(output_special_path):
        df_old_special = pd.read_csv(output_special_path, dtype=str)
        df_old_special['composite_key'] = (
            df_old_special['order_sn'].fillna('') + '|||' +
            df_old_special['buyer_username'].fillna('') + '|||' +
            df_old_special['order_creation_timestamp'].fillna('')
        )
        df_special['composite_key'] = (
            df_special['order_sn'].fillna('') + '|||' +
            df_special['buyer_username'].fillna('') + '|||' +
            df_special['order_creation_timestamp'].astype(str).fillna('')
        )
        keys_to_keep_special = set(df_old_special['composite_key']) - set(df_special['composite_key'])
        df_old_filtered_special = df_old_special[df_old_special['composite_key'].isin(keys_to_keep_special)]
        df_merged_special = pd.concat([df_old_filtered_special, df_special], ignore_index=True)
    else:
        print("無現有特殊平台主檔，直接使用新資料（特殊平台）")
        df_merged_special = df_special

    df_merged_special = df_merged_special.reindex(columns=FINAL_COLUMN_ORDER, fill_value='')
    if 'composite_key' in df_merged_special.columns:
        df_merged_special.drop(columns=['composite_key'], inplace=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df_merged_normal.to_csv(OUTPUT_CSV_PATH, index=False, encoding='utf-8-sig')
    df_merged_special.to_csv(output_special_path, index=False, encoding='utf-8-sig')

    print(f"ETL 完成，輸出一般平台檔案: {OUTPUT_CSV_PATH}")
    print(f"ETL 完成，輸出特殊平台檔案: {output_special_path}")

if __name__ == "__main__":
    main()
