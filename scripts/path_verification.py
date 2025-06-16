# path_verification.py
# 驗證所有路徑設定是否正確
# ================================

import os
import sys

def check_paths():
    """檢查所有重要路徑是否存在"""
    
    try:
        from config import (
            INPUT_DIR, OUTPUT_DIR, ARCHIVE_DIR, 
            OUTPUT_CSV_PATH, ORPHAN_CSV_PATH, UPLOAD_CSV_PATH, BQ_KEY_PATH
        )
    except ImportError as e:
        print(f"❌ 無法導入 config.py: {e}")
        return False
    
    print("🔍 開始驗證路徑設定...")
    
    # 應該存在的目錄
    directories_to_check = {
        "輸入資料夾": INPUT_DIR,
        "BigQuery 金鑰檔案目錄": os.path.dirname(BQ_KEY_PATH)
    }
    
    # 應該可以建立的目錄
    directories_to_create = {
        "輸出資料夾": OUTPUT_DIR,
        "歸檔資料夾": ARCHIVE_DIR,
        "主檔目錄": os.path.dirname(OUTPUT_CSV_PATH),
        "孤兒檔目錄": os.path.dirname(ORPHAN_CSV_PATH),
        "上傳檔目錄": os.path.dirname(UPLOAD_CSV_PATH)
    }
    
    # 應該存在的檔案
    files_to_check = {
        "BigQuery 金鑰檔": BQ_KEY_PATH
    }
    
    all_good = True
    
    # 檢查必須存在的目錄
    print("\n📁 檢查必須存在的目錄:")
    for name, path in directories_to_check.items():
        if os.path.exists(path):
            print(f"   ✅ {name}: {path}")
        else:
            print(f"   ❌ {name}: {path} (不存在)")
            all_good = False
    
    # 檢查可建立的目錄
    print("\n📁 檢查可建立的目錄:")
    for name, path in directories_to_create.items():
        try:
            os.makedirs(path, exist_ok=True)
            print(f"   ✅ {name}: {path}")
        except Exception as e:
            print(f"   ❌ {name}: {path} (無法建立: {e})")
            all_good = False
    
    # 檢查必須存在的檔案
    print("\n📄 檢查必須存在的檔案:")
    for name, path in files_to_check.items():
        if os.path.exists(path):
            print(f"   ✅ {name}: {path}")
        else:
            print(f"   ❌ {name}: {path} (不存在)")
            all_good = False
    
    # 檢查路徑一致性
    print("\n🔗 檢查路徑一致性:")
    if OUTPUT_CSV_PATH != UPLOAD_CSV_PATH:
        print(f"   ⚠️ 注意: OUTPUT_CSV_PATH 和 UPLOAD_CSV_PATH 不同")
        print(f"      OUTPUT_CSV_PATH: {OUTPUT_CSV_PATH}")
        print(f"      UPLOAD_CSV_PATH: {UPLOAD_CSV_PATH}")
        print(f"      您可能需要在上傳前複製檔案，或統一路徑設定")
    else:
        print(f"   ✅ OUTPUT_CSV_PATH 和 UPLOAD_CSV_PATH 一致")
    
    return all_good

if __name__ == "__main__":
    print("=" * 50)
    print("路徑設定驗證工具")
    print("=" * 50)
    
    if check_paths():
        print("\n🎉 所有路徑設定檢查通過！")
    else:
        print("\n❌ 發現路徑設定問題，請修正後再執行。")
        sys.exit(1)