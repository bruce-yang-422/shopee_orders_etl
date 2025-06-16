# -*- coding: utf-8 -*-
"""
腳本名稱：store_cleaned_data_status.py

用途說明：
    本腳本用於**自動產生多店家訂單資料更新狀況綜合報表**，以便定期稽核各蝦皮店家主檔（orders_analyze_A_master_featured.csv）之資料完整性、更新時效及缺漏情形。
    - 可偵測每家店最近一次訂單日期，並標記出未達預設「更新門檻日期」的店家。
    - 自動辨識主檔中未出現或名稱不一致（如手誤、簡寫等）的店家，並透過模糊比對協助管理端檢查。
    - 產生帶時間戳記的 Excel 報表，利於定期追蹤與稽核歸檔。

主要功能：
    1. 讀取主檔 CSV（預設路徑：master/orders_analyze_A_master_featured.csv），自動判斷店家名稱及訂單成立日期欄位。
    2. 根據內建店家簡稱清單（可自行維護），檢查哪些店家有資料、哪些為缺漏或名稱異常。
    3. 檢核每家店最新訂單是否已更新至門檻日期（如未達標自動標註）。
    4. 支援模糊比對，協助發現名稱接近但拼寫略異之店家，降低人為疏漏。
    5. 報表包含：店家簡稱、實際名稱、是否存在於資料庫、最新訂單日期、備註（如未達門檻/缺漏）。

檔案放置與使用說明：
    - 腳本建議放於 Shopee_order_project 專案資料夾（或任一有權限之主機）。
    - 主檔 orders_analyze_A_master_featured.csv 須放於 master 子目錄下。
    - 店家清單可於腳本 all_stores 變數維護。
    - 輸出 Excel 報表將自動存於 master 子資料夾，檔名格式如「店家更新狀況報表_整合版_20240528_103055.xlsx」（含時間戳）。
    - 執行需安裝 pandas、openpyxl（pip install pandas openpyxl），Python 3.7 以上。
    - 報表同時於命令列輸出摘要，方便快速判讀。

適用場景：
    - 跨多店家電商訂單定期彙整、缺漏稽核、業務週會進度追蹤。
    - 主管/營運/資料分析團隊定期檢查主檔資料品質，快速盤點需補件或異常門市。

開發維運建議：
    - 店家名稱務必維護正確簡稱清單，可定期調整（如增新店或更名）。
    - 請配合主檔資料命名與結構，必要時可擴充腳本自動欄位判斷/例外處理。
    - 若需支援多語系或自動通知可於後續版本擴充。
    - 報表有助於長期專案進度管理與稽核留存。

作者：楊翔志
版本：v1.0
日期：2025-05-28

# ---- 以下為主程式 ----
"""

import os
import pandas as pd
from datetime import datetime # 確保 datetime 被正確引入
from difflib import get_close_matches

def main():
    # 1. 基本路徑與檔案設定
    master_dir = r"C:\Users\user\Documents\shopee_orders_etl\output"
    csv_file = "A01_master_orders_cleaned.csv"
    file_path = os.path.join(master_dir, csv_file)

    # 2. 你的店家簡稱清單（可維護）
    all_stores = [
        "萌寵要當家",
        "汪喵日總匯",
        "毛寵星人",
        "大尾巴寵物店",
        "有才寵物商店",
        "火箭貓狗",
        "驕傲貓狗",
        "毛一堆寵物超市",
        "貓狗好時光",
        "貓狗超有事",
        "貓狗得來速",
        "Winnie維尼寵物",
        "熊好命"
    ]

    # 3. 設定資料更新門檻日期
    threshold_date = datetime.strptime("2025/06/13", "%Y/%m/%d")

    # 4. 讀取CSV，抓取欄位名稱
    df = pd.read_csv(file_path, low_memory=False)
    store_col = df.columns[0]    # 店家名稱欄（第一欄）
    order_date_col = df.columns[9]  # 訂單成立日期欄（第四欄）

    # 5. 將訂單成立日期轉成 datetime 格式
    df[order_date_col] = pd.to_datetime(df[order_date_col], errors='coerce')
    df_clean = df.dropna(subset=[store_col, order_date_col])

    # 6. 分組計算每家店最新訂單日期
    latest_dates = df_clean.groupby(store_col)[order_date_col].max()
    present_stores = set(latest_dates.index)

    # 7. 找出完全不在資料中的店家（初步判斷）
    missing_stores = [s for s in all_stores if s not in present_stores]

    # 8. 定義模糊比對函式（找相似店名）
    def fuzzy_match(store_name, candidates, cutoff=0.7):
        matches = get_close_matches(store_name, candidates, n=1, cutoff=cutoff)
        return matches[0] if matches else None

    # 9. 用模糊比對嘗試找出可能相同但名稱略有差異的店家
    fuzzy_matched = {}
    for store in missing_stores[:]: # 使用切片進行迭代，以便安全地從原始列表中刪除
        match = fuzzy_match(store, present_stores)
        if match:
            fuzzy_matched[store] = match
            missing_stores.remove(store)

    # 10. 檢查哪些店家未更新到門檻日期
    outdated_stores = []
    for store, last_date in latest_dates.items():
        if last_date < threshold_date:
            outdated_stores.append((store, last_date.strftime("%Y/%m/%d %H:%M")))

    # 11. 準備產生報表的資料列
    report_rows = []

    # 12. 先加入模糊匹配的店家（視為已存在）
    for nick, full_name in fuzzy_matched.items():
        last_date = latest_dates[full_name]
        note = "未達更新門檻" if last_date < threshold_date else ""
        report_rows.append({
            "簡稱": nick,
            "正式店名(模糊匹配)": full_name,
            "是否已在資料庫": "是(模糊匹配)",
            "最新訂單日期": last_date.strftime("%Y/%m/%d %H:%M"),
            "備註": note
        })

    # 13. 加入正式存在資料庫的店家（排除模糊匹配過的）
    for store in present_stores:
        if store not in fuzzy_matched.values(): # 確保不重複加入模糊匹配過的店家
            last_date = latest_dates[store]
            note = "未達更新門檻" if last_date < threshold_date else ""
            report_rows.append({
                "簡稱": "", # 如果是直接匹配到的，簡稱可以留空或填入該店名
                "正式店名(模糊匹配)": store,
                "是否已在資料庫": "是",
                "最新訂單日期": last_date.strftime("%Y/%m/%d %H:%M"),
                "備註": note
            })

    # 14. 加入缺漏的店家（完全沒資料）
    for store in missing_stores:
        report_rows.append({
            "簡稱": store,
            "正式店名(模糊匹配)": "",
            "是否已在資料庫": "否",
            "最新訂單日期": "",
            "備註": "缺漏店家，需確認資料來源"
        })

    # 15. 轉成DataFrame並輸出成Excel檔（需安裝openpyxl）
    report_df = pd.DataFrame(report_rows)
    
    # --- 修改開始 ---
    # 獲取當前日期和時間
    now = datetime.now()
    # 格式化日期時間字串，例如：20230521_103055 (年年月月日日_時時分分秒秒)
    timestamp_str = now.strftime("%Y%m%d_%H%M%S")
    
    # 產生新的檔案名稱，將時間戳加在基礎名稱和副檔名之間
    base_filename = "店家更新狀況報表_整合版"
    file_extension = ".xlsx"
    output_filename_with_timestamp = f"{base_filename}_{timestamp_str}{file_extension}"
    
    output_file = os.path.join(master_dir, output_filename_with_timestamp)
    # --- 修改結束 ---
    
    report_df.to_excel(output_file, index=False, engine='openpyxl')

    # 16. 印出結果摘要
    print(f"總店家數（已知）：{len(all_stores)}")
    print(f"資料中店家數（含模糊匹配）：{len(present_stores)}") # present_stores 是 CSV 中實際存在的店，fuzzy_matched 是從 all_stores 映射到 present_stores
    
    # 計算實際在 all_stores 中，且透過直接或模糊匹配找到的店家數
    found_in_all_stores_count = len(present_stores.intersection(set(all_stores))) + len(fuzzy_matched)
    
    print(f"已比對到的店家總數 (來自 all_stores)：{found_in_all_stores_count}")
    print(f"最終缺漏的店家 ({len(missing_stores)}): {missing_stores}\n")


    if fuzzy_matched:
        print("模糊匹配到的店家名稱對應：")
        for k, v in fuzzy_matched.items():
            print(f" - 你給的 '{k}' 疑似對應資料中 '{v}'")
        print()

    if outdated_stores:
        print(f"未更新到門檻日期（{threshold_date.strftime('%Y/%m/%d')}）店家與最新訂單日：")
        for s, d in outdated_stores:
            print(f" - {s}: {d}")
    else:
        # 這裡要小心，outdated_stores 只包含那些在 latest_dates 中但早於 threshold_date 的店家
        # 它不包含 fuzzy_matched 中可能過期的店家，但報表邏輯已處理
        # 也不包含 missing_stores
        # 可以考慮一個更全面的檢查，但目前印出是基於 outdated_stores 的
        print(f"所有已存在資料庫中且有訂單日期的店家，其最新訂單皆在門檻日期 ({threshold_date.strftime('%Y/%m/%d')}) 或之後。")


    print(f"\n報表已產生，檔案位置：{output_file}")

if __name__ == "__main__":
    main()