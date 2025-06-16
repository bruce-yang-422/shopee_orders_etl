#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BigQuery批量CSV檔案上傳腳本
自動上傳多個Shopee訂單CSV檔案至對應的BigQuery表格

檔案位置: C:/Users/user/Documents/shopee_orders_etl/scripts/batch_upload_to_bq.py
"""

import os
import sys
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Conflict
from google.oauth2 import service_account
import logging
from datetime import datetime
from typing import Optional, Dict, List
from pathlib import Path

# 設定日誌記錄
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bigquery_batch_upload.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class BigQueryBatchUploader:
    """BigQuery批量CSV上傳工具類"""
    
    def __init__(self, 
                 credentials_path: str,
                 project_id: str,
                 dataset_id: str = "shopee_data"):
        """
        初始化BigQuery批量上傳器
        
        Args:
            credentials_path: 服務帳戶憑證JSON檔案路徑
            project_id: Google Cloud專案ID
            dataset_id: BigQuery資料集ID
        """
        self.credentials_path = credentials_path
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = None
        self.upload_results = []
        
    def initialize_client(self) -> bool:
        """初始化BigQuery客戶端"""
        try:
            # 檢查憑證檔案是否存在
            if not os.path.exists(self.credentials_path):
                logger.error(f"憑證檔案不存在: {self.credentials_path}")
                return False
            
            # 建立憑證
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            
            # 初始化BigQuery客戶端
            self.client = bigquery.Client(
                credentials=credentials,
                project=self.project_id
            )
            
            logger.info("BigQuery客戶端初始化成功")
            return True
            
        except Exception as e:
            logger.error(f"初始化BigQuery客戶端失敗: {str(e)}")
            return False
    
    def create_dataset_if_not_exists(self) -> bool:
        """建立資料集（如果不存在）"""
        try:
            dataset_ref = self.client.dataset(self.dataset_id)
            
            try:
                self.client.get_dataset(dataset_ref)
                logger.info(f"資料集 {self.dataset_id} 已存在")
                return True
            except NotFound:
                # 資料集不存在，建立新的
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "asia-east1"  # 台灣地區
                dataset.description = "Shopee訂單資料"
                
                dataset = self.client.create_dataset(dataset)
                logger.info(f"已建立資料集: {self.dataset_id}")
                return True
                
        except Exception as e:
            logger.error(f"建立資料集失敗: {str(e)}")
            return False
    
    def read_csv_file(self, csv_path: str) -> Optional[pd.DataFrame]:
        """讀取CSV檔案"""
        try:
            if not os.path.exists(csv_path):
                logger.warning(f"CSV檔案不存在，跳過: {csv_path}")
                return None
            
            # 讀取CSV檔案
            df = pd.read_csv(csv_path, encoding='utf-8')
            logger.info(f"成功讀取 {Path(csv_path).name}，共 {len(df)} 筆資料，{len(df.columns)} 個欄位")
            
            return df
            
        except Exception as e:
            logger.error(f"讀取CSV檔案失敗 {csv_path}: {str(e)}")
            return None
    
    def upload_dataframe_to_table(self, 
                                 df: pd.DataFrame, 
                                 table_id: str, 
                                 csv_filename: str,
                                 write_disposition: str = "WRITE_TRUNCATE") -> bool:
        """
        上傳DataFrame到指定的BigQuery表格
        
        Args:
            df: 要上傳的DataFrame
            table_id: BigQuery表格ID
            csv_filename: CSV檔案名稱（用於日誌）
            write_disposition: 寫入模式
        """
        try:
            # 設定表格參考
            table_ref = self.client.dataset(self.dataset_id).table(table_id)
            
            # 設定上傳設定
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.autodetect = True  # 自動偵測schema
            job_config.write_disposition = write_disposition
            
            logger.info(f"開始上傳 {csv_filename} ({len(df)} 筆資料) 到 {self.project_id}.{self.dataset_id}.{table_id}")
            
            # 開始上傳
            job = self.client.load_table_from_dataframe(
                df, 
                table_ref, 
                job_config=job_config
            )
            
            # 等待作業完成
            job.result()
            
            # 檢查結果
            table = self.client.get_table(table_ref)
            logger.info(f"✅ {csv_filename} 上傳完成！表格 {table_id} 現在有 {table.num_rows} 筆資料")
            
            # 記錄上傳結果
            self.upload_results.append({
                "csv_file": csv_filename,
                "table_id": table_id,
                "status": "成功",
                "rows_uploaded": len(df),
                "final_rows": table.num_rows,
                "table_size_bytes": table.num_bytes
            })
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 上傳 {csv_filename} 到 {table_id} 失敗: {str(e)}")
            
            # 記錄失敗結果
            self.upload_results.append({
                "csv_file": csv_filename,
                "table_id": table_id,
                "status": "失敗",
                "error": str(e),
                "rows_uploaded": 0,
                "final_rows": 0,
                "table_size_bytes": 0
            })
            
            return False
    
    def batch_upload(self, file_table_mapping: Dict[str, str], csv_directory: str) -> Dict:
        """
        批量上傳多個CSV檔案
        
        Args:
            file_table_mapping: CSV檔案名稱對應BigQuery表格名稱的字典
            csv_directory: CSV檔案所在目錄
            
        Returns:
            上傳結果摘要
        """
        logger.info(f"=== 開始批量上傳 {len(file_table_mapping)} 個檔案 ===")
        
        success_count = 0
        skip_count = 0
        fail_count = 0
        
        for csv_filename, table_id in file_table_mapping.items():
            csv_path = os.path.join(csv_directory, csv_filename)
            
            logger.info(f"\n--- 處理檔案: {csv_filename} → {table_id} ---")
            
            # 讀取CSV檔案
            df = self.read_csv_file(csv_path)
            
            if df is None:
                skip_count += 1
                self.upload_results.append({
                    "csv_file": csv_filename,
                    "table_id": table_id,
                    "status": "跳過",
                    "error": "檔案不存在",
                    "rows_uploaded": 0,
                    "final_rows": 0,
                    "table_size_bytes": 0
                })
                continue
            
            # 上傳到BigQuery
            if self.upload_dataframe_to_table(df, table_id, csv_filename):
                success_count += 1
            else:
                fail_count += 1
        
        # 返回摘要
        summary = {
            "total_files": len(file_table_mapping),
            "success": success_count,
            "skipped": skip_count,
            "failed": fail_count,
            "details": self.upload_results
        }
        
        return summary
    
    def print_summary(self, summary: Dict):
        """列印上傳結果摘要"""
        logger.info("\n" + "="*60)
        logger.info("📊 批量上傳結果摘要")
        logger.info("="*60)
        logger.info(f"總檔案數: {summary['total_files']}")
        logger.info(f"✅ 成功: {summary['success']}")
        logger.info(f"⏭️  跳過: {summary['skipped']}")
        logger.info(f"❌ 失敗: {summary['failed']}")
        
        logger.info("\n📋 詳細結果:")
        logger.info("-" * 60)
        
        for result in summary['details']:
            status_icon = {"成功": "✅", "跳過": "⏭️", "失敗": "❌"}.get(result['status'], "❓")
            
            if result['status'] == "成功":
                size_mb = result['table_size_bytes'] / (1024 * 1024)
                logger.info(f"{status_icon} {result['csv_file']} → {result['table_id']}")
                logger.info(f"    資料筆數: {result['final_rows']:,} | 大小: {size_mb:.1f} MB")
            else:
                logger.info(f"{status_icon} {result['csv_file']} → {result['table_id']}")
                if 'error' in result:
                    logger.info(f"    原因: {result['error']}")

def main():
    """主要執行函數"""
    
    # 設定檔案和表格對應關係
    FILE_TABLE_MAPPING = {
        "B01_orders_concat.csv": "b01_orders_concat",
        "B02_order_details.csv": "b02_order_details", 
        "B03_order_simple_details.csv": "b03_order_simple_details",
        "B04_order_shipping_info.csv": "b04_order_shipping_info"
    }
    
    # 設定路徑
    csv_directory = r"C:\Users\user\Documents\shopee_orders_etl\upload_ready"
    credentials_path = r"C:\Users\user\Documents\shopee_orders_etl\scripts\shopee-etl-reporting-9531f3a7678a.json"
    
    # BigQuery設定
    PROJECT_ID = "shopee-etl-reporting"
    DATASET_ID = "shopee_data"
    
    logger.info("🚀 BigQuery批量CSV上傳開始")
    logger.info(f"⏰ 時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"📁 來源目錄: {csv_directory}")
    logger.info(f"🎯 目標專案: {PROJECT_ID}.{DATASET_ID}")
    
    # 建立批量上傳器
    uploader = BigQueryBatchUploader(
        credentials_path=credentials_path,
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID
    )
    
    # 初始化客戶端
    if not uploader.initialize_client():
        logger.error("無法初始化BigQuery客戶端，程式結束")
        sys.exit(1)
    
    # 建立資料集
    if not uploader.create_dataset_if_not_exists():
        logger.error("無法建立或存取資料集，程式結束")
        sys.exit(1)
    
    # 執行批量上傳
    summary = uploader.batch_upload(FILE_TABLE_MAPPING, csv_directory)
    
    # 列印結果摘要
    uploader.print_summary(summary)
    
    # 根據結果決定程式結束狀態
    if summary['failed'] > 0:
        logger.warning("⚠️  部分檔案上傳失敗")
        sys.exit(1)
    elif summary['success'] == 0:
        logger.warning("⚠️  沒有任何檔案成功上傳")
        sys.exit(1)
    else:
        logger.info("🎉 所有檔案處理完成！")

if __name__ == "__main__":
    main()