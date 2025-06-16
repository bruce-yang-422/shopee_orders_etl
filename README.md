# 蝦皮訂單 ETL 自動化專案 (Shopee Orders ETL Automation Project)

**最後更新日期：** 2025年6月7日

## 專案簡介

本專案旨在提供一個全自動化的ETL (Extract, Transform, Load) 流程，用於處理從蝦皮賣家中心下載的訂單Excel報表。它會自動清洗、轉換、標準化資料，並最終將其上傳至 Google BigQuery，以利後續的數據分析與商業智慧報告。

## 核心功能

- **自動化清洗流程**：一鍵執行批次檔 (`.bat`) 即可完成所有資料清洗與轉換。
- **檔名資訊擷取**：自動從輸入的檔案名稱中擷取「店鋪名稱」與「店鋪帳號」，並新增為資料欄位。
- **欄位標準化**：將所有欄位名稱轉換為 BigQuery 最佳實踐的英文蛇形命名法 (`snake_case`)。
- **智慧歸檔機制**：成功處理的原始 Excel 檔案會自動加上時間戳並移至 `archive/` 資料夾備份，避免重複處理。
- **設定檔管理**：所有路徑、欄位對照等設定皆由 `config.py` 統一管理，方便維護。
- **一鍵上傳雲端**：提供批次檔，可一鍵將清洗後的 CSV 主檔上傳至指定的 BigQuery 資料表。

## 📂 專案結構

```
C:/Users/user/Documents/shopee_orders_etl/
│
├─ .gitignore          # (建議) Git忽略清單
├─ README.md           # 本說明檔案
├─ requirements.txt    # Python 套件依賴列表
│
├─ venv/               # Python 虛擬環境
├─ input/              # 放置手動改好檔名、待清洗的 Excel 檔案
├─ output/             # 存放清洗後輸出的主檔 A01_master_orders_cleaned.csv
├─ archive/            # 存放已處理過的原始檔案備份（自動歸檔）
├─ upload_ready/       # (手動管理) 準備上傳至 BigQuery 的 CSV 檔案暫存區
│
└─ scripts/
   ├─ shopee-etl-reporting-9531f3a7678a.json # 上傳至 BigQuery 的 Google API 金鑰
   ├─ config.py                             # 欄位設定、常數與路徑配置
   ├─ order_processing_script.py            # 主要ETL處理流程腳本
   │
   └─ bat_scripts/
      ├─ run_cleaning.bat                   # 執行洗單流程的批次檔
      └─ upload_to_bigquery.bat             # 上傳至 BigQuery 的批次檔
```

## ⚙️ 事前準備與安裝 (只需執行一次)

在開始使用前，請確保您的開發環境已滿足以下條件：

1.  **安裝 Python**: 確認已安裝 Python 3.8 或更高版本。
2.  **安裝 Google Cloud SDK**:
    - 前往 [Google Cloud SDK 安裝頁面](https://cloud.google.com/sdk/docs/install) 下載並安裝。
    - 安裝過程中請確保勾選 `bq` 和 `gcloud` command-line tools。
3.  **建立 Python 虛擬環境**:
    - 在專案根目錄打開終端機 (Command Prompt 或 PowerShell)。
    - 執行 `python -m venv venv` 來建立虛擬環境。
4.  **安裝 Python 套件**:
    - 建立一個名為 `requirements.txt` 的檔案，內容如下：
      ```
      pandas
      openpyxl
      ```
    - 在終端機中，先啟用虛擬環境：`venv\Scripts\activate`
    - 接著安裝所需套件：`pip install -r requirements.txt`
5.  **放置 GCP 金鑰**: 將您的 `shopee-etl-reporting-9531f3a7678a.json` 金鑰檔案，放置到 `scripts/` 資料夾下。

## 🚀 日常操作流程

1.  **下載與命名**: 從蝦皮賣家中心下載訂單報表 (`.xlsx` 格式)。並將其手動重新命名為以下格式：
    > **`店鋪名稱_店鋪帳號_原始檔名.xlsx`**
    >
    > (範例: `有才寵物商店_yutsai_petmarket_Order.all.20250504_20250603.xlsx`)

2.  **放置檔案**: 將重新命名好的 Excel 檔案放入 `input/` 資料夾。您可以一次放入多個檔案。

3.  **執行清洗**: 進入 `scripts/bat_scripts/` 資料夾，直接 **雙擊** `run_cleaning.bat`。
    - 腳本會自動處理 `input/` 中的