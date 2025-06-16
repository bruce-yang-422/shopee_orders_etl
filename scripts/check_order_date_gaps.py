import pandas as pd
import os

def analyze_gaps_for_single_store(df_store_data, store_identifier, date_column_name, max_gap_days=3):
    df_store_data_copy = df_store_data.copy()
    df_store_data_copy['parsed_date'] = pd.to_datetime(df_store_data_copy[date_column_name], errors='coerce')
    df_valid_dates = df_store_data_copy.dropna(subset=['parsed_date'])

    if df_valid_dates.empty:
        return False

    unique_sorted_dates = df_valid_dates['parsed_date'].dt.normalize().sort_values().unique()

    if len(unique_sorted_dates) < 2:
        return False

    store_has_gaps = False
    first_gap_for_store = True
    previous_date = pd.Timestamp(unique_sorted_dates[0])

    for i in range(1, len(unique_sorted_dates)):
        current_date = pd.Timestamp(unique_sorted_dates[i])
        delta = current_date - previous_date

        if delta.days > max_gap_days:
            if first_gap_for_store:
                print(f"\n--- 分析結果：店家 {store_identifier} ---")
                first_gap_for_store = False

            store_has_gaps = True
            print(f"  發現訂單日期之間存在較大間隔：")
            print(f"    前一訂單日期: {previous_date.strftime('%Y-%m-%d')}")
            print(f"    本次訂單日期: {current_date.strftime('%Y-%m-%d')}")
            print(f"    中間連續 {delta.days - 1} 天沒有訂單")
            print("  ------------------------------------")

        previous_date = current_date

    return store_has_gaps

def main_analysis_by_store(file_path, max_gap_days=3):
    try:
        df = pd.read_csv(file_path)
        store_column_name = "shop_name"
        date_column_name = "order_date"

        if store_column_name not in df.columns:
            print(f"錯誤：在檔案中找不到欄位 'shop_name'。實際欄位有: {df.columns.tolist()}")
            return

        if date_column_name not in df.columns:
            print(f"錯誤：在檔案中找不到欄位 'order_date'。實際欄位有: {df.columns.tolist()}")
            return

        unique_stores = df[store_column_name].unique()

        if len(unique_stores) == 0:
            print(f"找不到任何店家代碼。")
            return

        print(f"在 'shop_name' 欄位中找到 {len(unique_stores)} 個獨立店家。")
        print(f"分析標準：尋找連續超過 {max_gap_days} 天沒有訂單的情況...\n")

        overall_any_gap_found = False

        for store_id in sorted(list(unique_stores)):
            df_current_store = df[df[store_column_name] == store_id]

            if analyze_gaps_for_single_store(df_current_store, store_id, date_column_name, max_gap_days):
                overall_any_gap_found = True

        if not overall_any_gap_found:
            print(f"\n檢查完畢。所有店家訂單日期間隔均未超過 {max_gap_days} 天。")
        else:
            print(f"\n所有店家分析完畢。")

    except FileNotFoundError:
        print(f"錯誤：找不到檔案 '{file_path}'。請確認路徑和檔案名稱是否正確。")
    except pd.errors.EmptyDataError:
        print(f"錯誤：檔案 '{file_path}' 是空的，或者不是有效的CSV格式。")
    except Exception as e:
        print(f"處理過程中發生未預期的錯誤：{e}")
        print("請檢查CSV檔案格式是否正確，以及指定的欄位是否包含預期資料。")

if __name__ == "__main__":
    csv_file_path = os.path.join("output", "A01_master_orders_cleaned.csv")
    gap_threshold = 3

    print(f"準備分析檔案: {csv_file_path}")
    print(f"以 'shop_name' 分組，每組依 'order_date' 檢查間隔（超過 {gap_threshold} 天列出警告）\n")

    main_analysis_by_store(csv_file_path, gap_threshold)
