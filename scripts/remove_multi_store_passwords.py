# -*- coding: utf-8 -*-
"""
腳本名稱：remove_excel_passwords_manual.py

用途說明：
    本腳本用於**批次處理多個蝦皮電商店家的 Excel 訂單報表（.xlsx），並自動移除開啟密碼保護**。
    - 支援根據檔名自動判斷店家帳號，並依照內建密碼字典匹配密碼，協助人工輸入密碼後完成解鎖。
    - 適用於因批量下載訂單檔案後，需進行後續資料整合、分析、去密碼存檔等自動化需求。
    - 移除密碼後的新檔案，將儲存於指定的 Decrypt 目錄，確保原始資料完整保留。

主要功能：
    1. 自動比對來源資料夾中全部 .xlsx 檔案，依命名規則解析出對應之店家帳號。
    2. 依據帳號從密碼字典自動取出密碼（密碼需人工輸入於 Excel 彈窗，程式提供提示）。
    3. 完成去密碼存檔，檔名與原始檔相同，另存至目標資料夾。
    4. 處理過程自動統計成功、失敗、跳過等情形，輸出詳細執行 log 方便追蹤。

檔案放置與運作方式：
    - 腳本建議放置於 `Shopee_order_project` 專案資料夾主目錄（或自行調整）。
    - 源檔（原始密碼 Excel 檔）請放於：`input/` 子資料夾。
    - 輸出（去密碼檔案）將自動儲存於：`input/Decrypt/` 子資料夾。
    - 店家密碼字典請於腳本開頭區塊直接維護與管理（格式：{'帳號':'密碼'}）。
    - 執行前需安裝 Python、`pywin32` 套件，並確保 Windows 環境已安裝 Office Excel。
    - 執行後，依指示於 Excel 彈窗輸入密碼（如遇密碼錯誤，請依錯誤訊息檢查密碼字典是否正確）。

命名規則說明：
    - 檔案必須符合「中文店名_店家帳號_Order.all.日期.xlsx」格式（例：旺旺寵物_petboss556_Order.all.20240525.xlsx）。
    - 程式會自動擷取「_」後至「_Order.all.」前的帳號做比對。

適用場景：
    - 批次下載各分店 Shopee 訂單 Excel 報表後需清除密碼再進行資料彙整、分析。
    - 定期產生、整併跨店家訂單數據時之密碼解鎖自動化流程。

開發與維運建議：
    - 使用者建議熟悉 Windows 作業環境及 Excel 應用程式自動化操作。
    - 如目標資料夾已存在，可選擇自動清空（程式已預留對應註解，可按需開啟）。
    - 若需支援自動密碼輸入（完全無人工），請另尋破解或特殊自動化工具（本腳本強調合法、人工配合）。

作者：楊翔志
版本：v1.0
日期：2025-05-28

# ---- 以下為主程式 ----
"""
import os
import win32com.client
import time
import shutil # 用於刪除資料夾 (如果需要在運行前清空目標資料夾)

def extract_store_key_from_filename(filename):
    # 從檔名中提取用於查找密碼的店家關鍵字 (帳號)。
    # 檔名格式範例: "中文店名_店家帳號_Order.all.日期.xlsx"
    try:
        order_marker = "_Order.all."
        if order_marker in filename:
            prefix = filename.split(order_marker, 1)[0]
            if '_' in prefix:
                store_name_part, store_account_key = prefix.split('_', 1)
                return store_account_key.strip()
            else:
                # print(f"警告: 檔名 '{filename}' 的前缀 '{prefix}' 中没有底線来区分店名和帳號。")
                return None
        else:
            # print(f"警告: 檔名 '{filename}' 不符合預期的 '{order_marker}' 格式。")
            return None
    except Exception as e:
        print(f"  從檔名 '{filename}' 提取店家帳號時發生錯誤: {e}")
        return None

def remove_excel_passwords_original_logic(folder_path, store_passwords_map, output_folder_path):
    # 執行前選擇性清空目標資料夾
    if os.path.exists(output_folder_path):
        print(f"目標資料夾 '{output_folder_path}' 已存在。")
        # 如果您希望每次運行前都清空它，可以取消以下註解：
        # print(f"正在清空舊的目標資料夾: {output_folder_path}...")
        # try:
        #     for item_name in os.listdir(output_folder_path):
        #         item_path = os.path.join(output_folder_path, item_name)
        #         if os.path.isfile(item_path) or os.path.islink(item_path):
        #             os.unlink(item_path)
        #         elif os.path.isdir(item_path):
        #             shutil.rmtree(item_path)
        #     print(f"目標資料夾 '{output_folder_path}' 已清空。")
        # except Exception as e_del_folder:
        #     print(f"清空目標資料夾 '{output_folder_path}' 失敗: {e_del_folder}。")
        #     # 可以選擇在這裡終止，或者讓它繼續（可能會覆蓋檔案）
    else:
        os.makedirs(output_folder_path)
        print(f"已建立輸出資料夾: {output_folder_path}")

    excel_app = None
    xlOpenXMLWorkbook = 51 # .xlsx 檔案格式的代碼

    try:
        print("正在啟動 Excel 應用程式...")
        excel_app = win32com.client.Dispatch("Excel.Application")
        # V2 版本可能沒有像 V2.1 那樣明確設定 Interactive=False 或 AutomationSecurity=1
        # 這些設定主要是為了嘗試阻止彈窗，如果您接受彈窗，則可以保持預設
        excel_app.Visible = False      # 通常建議在背景執行
        excel_app.DisplayAlerts = False # 避免 Excel 的一些警告

        files_processed_count = 0
        files_failed_count = 0
        files_skipped_count = 0

        for filename in os.listdir(folder_path):
            if filename.lower().endswith((".xlsx")): # 專注處理 .xlsx
                file_path = os.path.join(folder_path, filename)
                output_file_path = os.path.join(output_folder_path, filename)

                print(f"\n正在處理檔案: {filename}...")
                store_account_key = extract_store_key_from_filename(filename)

                if not store_account_key:
                    print(f"  無法從檔名提取有效的店家帳號，跳過此檔案。")
                    files_skipped_count += 1
                    continue

                if store_account_key in store_passwords_map:
                    current_password_from_dict = store_passwords_map[store_account_key]
                    workbook = None
                    try:
                        # 這裡 Excel 會彈出視窗，您需要手動輸入 current_password_from_dict
                        print(f"  準備開啟檔案 (店家帳號: {store_account_key})。請在 Excel 彈窗中輸入對應密碼。")
                        print(f"  提示密碼 (來自字典): '{current_password_from_dict}'") # 打印提示，方便您查找

                        # 由於您會手動輸入，這裡傳遞的密碼參數可能被 Excel 的彈窗覆蓋
                        # 但為了完整性，我們仍然可以傳遞它
                        workbook = excel_app.Workbooks.Open(Filename=file_path,
                                                            Password=current_password_from_dict, # 腳本提供的密碼
                                                            UpdateLinks=0,
                                                            ReadOnly=False)
                        
                        print(f"  檔案開啟操作已執行。嘗試將檔案另存為無密碼版本至: {output_file_path}")
                        workbook.SaveAs(Filename=output_file_path,
                                        FileFormat=xlOpenXMLWorkbook,
                                        Password="", # 清除開啟密碼
                                        WriteResPassword="",
                                        ReadOnlyRecommended=False,
                                        CreateBackup=False)
                        
                        print(f"  成功! 店家帳號 '{store_account_key}' 的檔案應已處理並儲存。")
                        files_processed_count += 1
                    except Exception as e:
                        print(f"  處理檔案 {filename} (店家帳號: {store_account_key}) 時發生錯誤: {e}")
                        if hasattr(e, 'excepinfo') and e.excepinfo:
                            error_code = e.excepinfo[5]
                            error_message = e.excepinfo[2] if len(e.excepinfo) > 2 else "N/A"
                            print(f"    COM Error. Code: {error_code}, Message: {error_message}")
                        files_failed_count += 1
                    finally:
                        if workbook:
                            workbook.Close(SaveChanges=False)
                else:
                    print(f"  警告: 在密碼字典中找不到店家帳號 '{store_account_key}' (來自檔案 {filename}) 的密碼，跳過此檔案。")
                    files_skipped_count += 1
                        
        print(f"\n--- 處理完畢 ---")
        print(f"成功處理檔案數: {files_processed_count}")
        print(f"失敗處理檔案數: {files_failed_count}")
        print(f"跳過處理檔案數: {files_skipped_count}")

    except Exception as e:
        print(f"操作 Excel 應用程式時發生嚴重錯誤: {e}")
    finally:
        if excel_app:
            print("正在關閉 Excel 應用程式...")
            excel_app.Quit()
        excel_app = None

# --- 設定 ---
STORE_PASSWORDS = {
    "petboss5566": "725389",
    "dogcatclub5566": "692389",
    "petstar5566": "693289",
    "curly_tail_petshop": "808577",
    "yutsai_petmarket": "728677",
    "space_pet_shop": "252169",
    "proudcatdog": "706239",
    "spiked_fur_store": "253169",
    "pets_moment_store": "251759",
    "dramatic_pet_shop": "322571",
    "pet_drive_thru": "709311",
    "luckybear_pet": "316293",
    "winnie_pet_shop": "316291",
    "petdada666": "553710",

}
SOURCE_FOLDER = r"C:\Users\user\Documents\shopee_orders_etl\input"       # 您的來源路徑
DESTINATION_FOLDER = r"C:\Users\user\Documents\shopee_orders_etl\input\Decrypt" # <<<<---- 目標資料夾改回 "Decrypt"

if __name__ == "__main__":
    print(f"--- 開始批次移除多店家 Excel 檔案密碼 (手動輸入密碼輔助版) ---")
    print(f"輸出資料夾將是: {DESTINATION_FOLDER}")
    if not os.path.isdir(SOURCE_FOLDER):
        print(f"錯誤：來源資料夾 '{SOURCE_FOLDER}' 不存在。")
    elif not STORE_PASSWORDS:
        print("錯誤：店家密碼字典 'STORE_PASSWORDS' 是空的。")
    else:
        remove_excel_passwords_original_logic(SOURCE_FOLDER, STORE_PASSWORDS, DESTINATION_FOLDER)