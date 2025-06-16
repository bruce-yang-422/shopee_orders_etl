import pandas as pd
import os
import glob
import shutil
from datetime import datetime
try:
    from config import (
        INPUT_DIR, OUTPUT_DIR, ARCHIVE_DIR, OUTPUT_CSV_PATH, ORPHAN_CSV_PATH,
        COLUMN_MAPPING, FINAL_COLUMN_ORDER
    )
except ImportError:
    print("❌ 錯誤：無法從 config.py 導入設定。")
    exit()
import logging
import traceback

# --- 日誌設定 ---
log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'python_script_log.txt'))
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8',
    filemode='w'
)

# --- 核心處理函式 ---

def create_robust_composite_key(df):
    """建立基於訂單日期 + 訂單編號 + 買家帳號的穩健複合主鍵。
    以整筆訂單為單位進行比對，新資料會完全覆蓋舊資料。"""
    
    # 確保欄位存在
    required_cols = ['order_date', 'order_sn', 'buyer_username']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logging.error(f"缺少必要欄位: {missing_cols}")
        return df
    
    # 清理訂單日期
    order_date = df['order_date'].astype(str).str.strip()
    order_date = order_date.replace(['nan', 'NaN', '<NA>', 'None', ''], 'NO_DATE')
    
    # 清理訂單編號
    order_sn = df['order_sn'].fillna('').astype(str).str.strip()
    order_sn = order_sn.replace(['nan', 'NaN', '<NA>', 'None'], 'NO_ORDER')
    
    # 清理買家帳號
    buyer_username = df['buyer_username'].fillna('').astype(str).str.strip()
    buyer_username = buyer_username.replace(['nan', 'NaN', '<NA>', 'None'], 'NO_BUYER')
    
    # 建立複合主鍵：order_date + order_sn + buyer_username
    df['composite_key'] = order_date + '|||' + order_sn + '|||' + buyer_username
    
    # 除錯：記錄主鍵生成統計
    unique_keys = df['composite_key'].nunique()
    total_keys = len(df)
    duplicate_count = total_keys - unique_keys
    
    logging.info(f"複合主鍵生成統計: 總數={total_keys}, 唯一訂單={unique_keys}, 同訂單多商品={duplicate_count}")
    
    if duplicate_count > 0:
        print(f"📊 訂單統計: {total_keys} 筆記錄對應 {unique_keys} 個唯一訂單")
        print(f"    平均每訂單 {total_keys/unique_keys:.1f} 個商品項目")
        
        # 顯示一些範例（同一訂單的多個商品）
        duplicates = df[df.duplicated('composite_key', keep=False)]
        if len(duplicates) > 0:
            sample_order = duplicates['composite_key'].iloc[0]
            sample_items = df[df['composite_key'] == sample_order]
            print(f"    範例訂單 {sample_order.split('|||')[1]} 包含 {len(sample_items)} 個商品項目")
    
    return df


def update_logic_with_order_level_replacement(df_old, df_new):
    """以訂單為單位進行覆蓋更新的邏輯"""
    
    if df_old.empty:
        print("📝 首次建立主檔")
        return df_new, pd.DataFrame()
    
    print("🔄 進行以訂單為單位的資料比對與更新...")
    
    # 確定新資料的日期範圍
    new_date_range = None
    if 'order_date' in df_new.columns:
        valid_new_dates = pd.to_datetime(df_new['order_date'], errors='coerce').dropna()
        if len(valid_new_dates) > 0:
            new_date_min = valid_new_dates.min().date()
            new_date_max = valid_new_dates.max().date()
            new_date_range = (new_date_min, new_date_max)
            print(f"   -> 新資料日期範圍: {new_date_min} 到 {new_date_max}")
            logging.info(f"新資料日期範圍: {new_date_min} 到 {new_date_max}")
    
    # 建立複合主鍵
    df_old = create_robust_composite_key(df_old)
    df_new = create_robust_composite_key(df_new)
    
    # 取得所有唯一的訂單主鍵
    old_order_keys = set(df_old['composite_key'].unique())
    new_order_keys = set(df_new['composite_key'].unique())
    
    # 找出在新資料日期範圍內的舊訂單
    if new_date_range and 'order_date' in df_old.columns:
        # 確保 order_date 是日期格式
        df_old['order_date_parsed'] = pd.to_datetime(df_old['order_date'], errors='coerce').dt.date
        
        # 找出在新資料日期範圍內的舊訂單
        mask_in_range = (
            (df_old['order_date_parsed'] >= new_date_range[0]) & 
            (df_old['order_date_parsed'] <= new_date_range[1])
        )
        old_orders_in_range = set(df_old.loc[mask_in_range, 'composite_key'].unique())
        old_orders_outside_range = set(df_old.loc[~mask_in_range, 'composite_key'].unique())
        
        print(f"   -> 日期範圍內的舊訂單: {len(old_orders_in_range)} 個")
        print(f"   -> 日期範圍外的舊訂單: {len(old_orders_outside_range)} 個")
        
        # 找出被新資料覆蓋的訂單（在範圍內且有新版本）
        orders_to_replace = old_orders_in_range & new_order_keys
        
        # 找出在範圍內但新資料中消失的訂單（真正的孤兒訂單）
        orphaned_orders = old_orders_in_range - new_order_keys
        
        # 保留的舊資料：日期範圍外的 + 範圍內但未被覆蓋的
        orders_to_keep = old_orders_outside_range | (old_orders_in_range - new_order_keys - orders_to_replace)
        
        print(f"   -> 被新資料覆蓋的訂單: {len(orders_to_replace)} 個")
        print(f"   -> 消失的訂單（孤兒）: {len(orphaned_orders)} 個")
        print(f"   -> 保留的舊訂單: {len(orders_to_keep)} 個")
        
    else:
        # 如果無法確定日期範圍，使用全域比對模式
        logging.warning("無法確定新資料日期範圍，使用全域比對模式")
        print("   -> ⚠️ 無法確定日期範圍，使用全域比對模式")
        
        orders_to_replace = old_order_keys & new_order_keys
        orphaned_orders = old_order_keys - new_order_keys
        orders_to_keep = old_order_keys - new_order_keys - orders_to_replace
    
    # 篩選資料
    df_old_kept = df_old[df_old['composite_key'].isin(orders_to_keep)]
    orphaned_records = df_old[df_old['composite_key'].isin(orphaned_orders)]
    df_new_kept = df_new  # 新資料全部保留
    
    # 統計訂單層級的變化
    old_order_count = len(df_old_kept['composite_key'].unique()) if not df_old_kept.empty else 0
    new_order_count = len(df_new_kept['composite_key'].unique()) if not df_new_kept.empty else 0
    orphan_order_count = len(orphaned_records['composite_key'].unique()) if not orphaned_records.empty else 0
    
    print(f"   -> 最終結果:")
    print(f"      保留舊訂單: {old_order_count} 個 ({len(df_old_kept)} 筆記錄)")
    print(f"      新增/更新訂單: {new_order_count} 個 ({len(df_new_kept)} 筆記錄)")
    print(f"      孤兒訂單: {orphan_order_count} 個 ({len(orphaned_records)} 筆記錄)")
    
    # 收集所有欄位，保證欄位完整性
    all_cols = sorted(set(df_old_kept.columns) | set(df_new_kept.columns))
    df_old_aligned = df_old_kept.reindex(columns=all_cols)
    df_new_aligned = df_new_kept.reindex(columns=all_cols)
    
    # 建立 dtype 映射
    dtype_map = {}
    for col in all_cols:
        if col in df_old_kept.columns and not df_old_kept[col].isna().all():
            dtype_map[col] = df_old_kept[col].dtype
        elif col in df_new_kept.columns and not df_new_kept[col].isna().all():
            dtype_map[col] = df_new_kept[col].dtype
        else:
            dtype_map[col] = object
    
    # 套用 dtype
    try:
        df_old_aligned = df_old_aligned.astype(dtype_map, errors='ignore')
        df_new_aligned = df_new_aligned.astype(dtype_map, errors='ignore')
    except Exception as e:
        logging.warning(f"資料類型轉換時發生警告: {e}")
    
    # 合併
    final_master_df = pd.concat([df_old_aligned, df_new_aligned], ignore_index=True)
    
    return final_master_df, orphaned_records


def clean_column_names(df):
    """清理欄位名稱，移除特殊字符和多餘空白"""
    # 清理欄位名稱
    cleaned_columns = {}
    for col in df.columns:
        # 移除換行符、多餘空白，並保留主要內容
        cleaned_col = col.replace('\n', '').replace('\r', '').strip()
        
        # 處理特殊的長欄位名稱
        if '若您是自行配送請使用後方蝦皮專線和包裹查詢碼聯繫買家' in cleaned_col:
            cleaned_col = '收件者電話'
        elif '請複製下方完整編號提供給您配合的物流商當做聯絡電話' in cleaned_col:
            cleaned_col = '蝦皮專線和包裹查詢碼'
        
        cleaned_columns[col] = cleaned_col
    
    df.rename(columns=cleaned_columns, inplace=True)
    
    # 記錄清理結果
    logging.info(f"欄位名稱清理完成，清理了 {len([k for k, v in cleaned_columns.items() if k != v])} 個欄位")
    
    return df


def parse_order_date_from_sn(order_sn):
    """從訂單編號解析訂單日期"""
    try:
        if pd.isna(order_sn) or order_sn == '':
            return None
        
        order_sn_str = str(order_sn).strip()
        
        # 蝦皮訂單編號通常格式為: YYMMDDXXXXXXXX (前6位是日期)
        if len(order_sn_str) >= 6:
            date_part = order_sn_str[:6]
            # 嘗試解析為 YYMMDD 格式
            try:
                parsed_date = pd.to_datetime(date_part, format='%y%m%d')
                return parsed_date.date()
            except:
                # 如果失敗，嘗試其他可能的格式
                try:
                    # 有些可能是 YYYYMMDD 格式
                    if len(order_sn_str) >= 8:
                        date_part_8 = order_sn_str[:8]
                        parsed_date = pd.to_datetime(date_part_8, format='%Y%m%d')
                        return parsed_date.date()
                except:
                    pass
        
        return None
    except Exception as e:
        logging.warning(f"解析訂單編號日期時發生錯誤: {order_sn}, 錯誤: {e}")
        return None


def load_and_clean_new_data():
    """從 Excel 檔案讀取、解析、清理並轉換所有新訂單資料。"""
    logging.info("Starting to load and clean new data from Excel files.")
    files_to_process = glob.glob(os.path.join(INPUT_DIR, '*.xlsx'))
    if not files_to_process:
        logging.warning("No new Excel files found in input directory.")
        return None, []

    all_dataframes = []
    processed_files_paths = []
    logging.info(f"Found {len(files_to_process)} file(s) to process.")
    print(f"🔍 發現 {len(files_to_process)} 個新檔案，開始解析...")

    for filepath in files_to_process:
        filename = os.path.basename(filepath)
        try:
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            os.makedirs(ARCHIVE_DIR, exist_ok=True)
            
            # 解析店鋪資訊
            anchor_pattern = '_Order.all.'
            first_underscore_pos = filename.find('_')
            anchor_pos = filename.find(anchor_pattern)
            if first_underscore_pos == -1 or anchor_pos == -1 or anchor_pos < first_underscore_pos:
                raise ValueError(f"檔名格式不符，缺少店鋪資訊或 '{anchor_pattern}' 標記")
            
            shop_name = filename[:first_underscore_pos]
            shop_account = filename[first_underscore_pos + 1:anchor_pos]
            if not shop_name or not shop_account:
                raise ValueError("店鋪名稱或帳號為空")

            # 讀取 Excel 檔案
            print(f"   -> 📖 讀取檔案: {filename}")
            df = pd.read_excel(filepath, dtype=str)
            
            # 清理欄位名稱
            df = clean_column_names(df)
            
            # 顯示實際讀取到的欄位（用於除錯）
            logging.info(f"檔案 {filename} 清理後的欄位: {list(df.columns)}")
            
            # 重命名欄位前，檢查映射
            unmapped_columns = [col for col in df.columns if col not in COLUMN_MAPPING]
            if unmapped_columns:
                logging.warning(f"檔案 {filename} 有未映射的欄位: {unmapped_columns}")
                print(f"   -> ⚠️ 未映射欄位: {unmapped_columns}")
            
            # 重命名欄位
            df.rename(columns=COLUMN_MAPPING, inplace=True)
            
            # 統計成功映射的欄位數量
            mapped_count = len([col for col in df.columns if col in COLUMN_MAPPING.values()])
            print(f"   -> 📋 成功映射 {len(COLUMN_MAPPING)} 個欄位，資料包含 {len(df.columns)} 個欄位")
            
            # 檢查必要欄位是否存在
            required_fields = ['order_sn', 'buyer_username']
            missing_fields = [field for field in required_fields if field not in df.columns]
            if missing_fields:
                logging.warning(f"檔案 {filename} 缺少必要欄位: {missing_fields}")
                print(f"   -> ⚠️ 缺少必要欄位: {missing_fields}")
            else:
                print(f"   -> ✅ 所有必要欄位都存在")
            
            # 清理文字欄位中的換行符
            for col in df.select_dtypes(include=['object']).columns:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.replace('\n', ' ', regex=False).str.replace('\r', '', regex=False)

            # 新增店鋪資訊和處理日期
            df['shop_name'] = shop_name
            df['shop_account'] = shop_account
            df['processing_date'] = datetime.now().date()

            # 轉換數值欄位
            float_columns = [
                'product_total_price', 'buyer_paid_shipping_fee', 'shopee_shipping_subsidy',
                'return_shipping_fee', 'total_amount_paid_by_buyer', 'shopee_subsidy_amount',
                'shopee_coin_offset', 'credit_card_promotion_discount', 'transaction_fee',
                'other_service_fee', 'payment_processing_fee', 'product_original_price',
                'product_campaign_price'
            ]
            for col in float_columns:
                if col in df.columns:
                    # 移除可能的貨幣符號和逗號
                    df[col] = df[col].astype(str).str.replace(r'[^\d.-]', '', regex=True)
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # 處理費率欄位（百分比轉小數）
            if 'payment_processing_fee_rate' in df.columns:
                df['payment_processing_fee_rate'] = (
                    pd.to_numeric(
                        df['payment_processing_fee_rate'].astype(str)
                        .str.replace('%', '', regex=False)
                        .str.replace(r'[^\d.-]', '', regex=True),
                        errors='coerce'
                    ).fillna(0) / 100
                )

            # 解析訂單日期
            if 'order_sn' in df.columns:
                print(f"   -> 📅 解析訂單日期...")
                df['order_date'] = df['order_sn'].apply(parse_order_date_from_sn)
                # 統計解析成功的數量
                valid_dates = df['order_date'].notna().sum()
                print(f"      成功解析 {valid_dates}/{len(df)} 筆訂單日期")
                logging.info(f"訂單日期解析: {valid_dates}/{len(df)} 成功")

            # 處理日期欄位
            if 'ship_by_date' in df.columns:
                df['ship_by_date'] = pd.to_datetime(df['ship_by_date'], errors='coerce').dt.date

            # 處理時間戳欄位
            timestamp_cols = [
                'order_creation_timestamp', 'buyer_payment_timestamp',
                'actual_shipping_timestamp', 'order_completion_timestamp'
            ]
            for col in timestamp_cols:
                if col in df.columns:
                    # 處理空值和 '-' 符號
                    df[col] = df[col].replace(['-', '', 'nan', 'NaN'], pd.NaT)
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            # 處理整數欄位
            int_columns = ['quantity', 'return_quantity', 'installment_plan_periods', 'days_to_ship']
            for col in int_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

            all_dataframes.append(df)
            processed_files_paths.append(filepath)
            logging.info(f"Successfully parsed and cleaned: {filename}")
            print(f"   -> ✅ 已解析: {filename} ({len(df)} 筆資料)")
            
        except Exception as e:
            logging.error(f"Failed to parse {filename}: {e}\n{traceback.format_exc()}")
            print(f"   -> ❌ 解析失敗: {filename}，錯誤: {e}")

    if not all_dataframes:
        logging.warning("No dataframes were created from Excel files.")
        return None, []

    final_df = pd.concat(all_dataframes, ignore_index=True)
    logging.info(f"Concatenated all dataframes. Total new rows: {len(final_df)}")
    print(f"📊 合併完成，共 {len(final_df)} 筆新資料")
    return final_df, processed_files_paths


def run_update_logic():
    """主流程：執行讀取、比對、更新、歸檔的完整邏輯"""
    logging.info("Starting main update logic.")
    df_new, processed_files = load_and_clean_new_data()
    if df_new is None:
        print("🟡 在 'input' 資料夾中沒有找到任何新檔案可處理。")
        logging.info("No new data to process. Exiting.")
        return

    if os.path.exists(OUTPUT_CSV_PATH):
        logging.info(f"Loading existing master file from {OUTPUT_CSV_PATH}")
        print(f"\n📑 正在讀取現有主檔: {os.path.basename(OUTPUT_CSV_PATH)}")
        try:
            df_old = pd.read_csv(OUTPUT_CSV_PATH, dtype=str)
            print(f"   -> 載入 {len(df_old)} 筆現有資料")
        except Exception as e:
            logging.error(f"讀取現有主檔失敗: {e}")
            print(f"   -> ❌ 讀取主檔失敗: {e}")
            df_old = pd.DataFrame()
    else:
        logging.info("No existing master file found.")
        print("\n📑 未發現現有主檔，將直接建立新檔案。")
        df_old = pd.DataFrame()

    # 轉換舊資料的資料類型
    if not df_old.empty:
        date_cols = ['processing_date', 'order_date', 'ship_by_date']
        for col in date_cols:
            if col in df_old.columns:
                df_old[col] = pd.to_datetime(df_old[col], errors='coerce').dt.date

        timestamp_cols = [
            'order_creation_timestamp', 'buyer_payment_timestamp',
            'actual_shipping_timestamp', 'order_completion_timestamp'
        ]
        for col in timestamp_cols:
            if col in df_old.columns:
                df_old[col] = pd.to_datetime(df_old[col], errors='coerce')

    # ===== 使用新的訂單層級覆蓋邏輯 =====
    final_master_df, orphaned_records = update_logic_with_order_level_replacement(df_old, df_new)
    
    # 移除臨時欄位
    final_master_df = final_master_df.drop(columns=['composite_key', 'order_date_parsed'], errors='ignore')
    orphaned_records = orphaned_records.drop(columns=['composite_key', 'order_date_parsed'], errors='ignore')
    
    # 確保欄位順序正確
    available_columns = [col for col in FINAL_COLUMN_ORDER if col in final_master_df.columns]
    final_master_df = final_master_df.reindex(columns=available_columns)

    # 儲存與歸檔流程
    logging.info(f"Saving final master dataframe with {len(final_master_df)} rows to CSV.")
    print("\n💾 正在儲存更新後的主檔...")
    os.makedirs(os.path.dirname(OUTPUT_CSV_PATH), exist_ok=True)
    final_master_df.to_csv(OUTPUT_CSV_PATH, index=False, encoding='utf-8-sig')
    print(f"   -> ✅ 主檔已成功更新並儲存至: {os.path.basename(OUTPUT_CSV_PATH)} ({len(final_master_df)} 筆紀錄)")

    if not orphaned_records.empty:
        logging.info(f"Found {len(orphaned_records)} orphaned records. Saving to orphan file.")
        print(f"\n🟡 發現 {len(orphaned_records)} 筆已消失的訂單，正在存檔...")
        orphaned_records['orphaned_timestamp'] = datetime.now()
        is_first_write = not os.path.exists(ORPHAN_CSV_PATH)
        os.makedirs(os.path.dirname(ORPHAN_CSV_PATH), exist_ok=True)
        orphaned_records.to_csv(ORPHAN_CSV_PATH, mode='a', index=False, header=is_first_write, encoding='utf-8-sig')
        print(f"   -> ✅ 已附加至: {os.path.basename(ORPHAN_CSV_PATH)}")
    else:
        logging.info("No orphaned records found this run.")
        print("\n🟢 本次更新範圍內無任何已消失的訂單。")

    logging.info("Archiving processed source files.")
    print("\n🗄️  正在歸檔已處理的原始檔案...")
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    for filepath in processed_files:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = os.path.basename(filepath)
        archive_filename = f"{os.path.splitext(base_filename)[0]}_{timestamp}{os.path.splitext(base_filename)[1]}"
        archive_path = os.path.join(ARCHIVE_DIR, archive_filename)
        shutil.move(filepath, archive_path)
        print(f"   -> 📦 {base_filename} → {archive_filename}")
    logging.info(f"Archived {len(processed_files)} files.")
    print(f"   -> ✅ 已成功歸檔 {len(processed_files)} 個檔案。")


if __name__ == "__main__":
    try:
        logging.info("================ SCRIPT START ================")
        print("🚀 開始執行蝦皮訂單 ETL 處理流程...")
        run_update_logic()
        logging.info("================ SCRIPT SUCCESS ================")
        print("\n🎉 所有本地端 CSV 清洗流程執行完畢！")
    except Exception as e:
        error_message = traceback.format_exc()
        logging.error(f"An unhandled exception occurred:\n{error_message}")
        print(f"\n❌ 發生未預期的嚴重錯誤：{e}")
        print(f"詳細錯誤資訊請檢查 'python_script_log.txt' 檔案。")
        raise