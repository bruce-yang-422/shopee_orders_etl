#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
訂單取消檢查腳本
比對 A01_master_orders_cleaned.csv 和 A01_orphaned_orders.csv 
根據 order_date + order_sn + buyer_username 找出相同的訂單，
然後比較 order_sn 在兩個檔案中的出現次數來判斷是否有部分商品被取消
"""

import pandas as pd
import os
from pathlib import Path

def check_order_cancellation():
    """檢查訂單是否有部分商品被取消"""
    
    # 定義檔案路徑
    output_dir = Path("C:/Users/user/Documents/shopee_orders_etl/output")
    file1 = output_dir / "A01_master_orders_cleaned.csv"
    file2 = output_dir / "A01_orphaned_orders.csv"
    
    # 關鍵比對欄位
    key_columns = ['order_date', 'order_sn', 'buyer_username']
    
    print("=== 訂單商品取消分析 ===")
    print(f"檢查目錄: {output_dir}")
    print(f"檔案1: {file1.name} (主要訂單)")
    print(f"檔案2: {file2.name} (孤立訂單)")
    print(f"🎯 比對關鍵欄位: {key_columns}")
    print("💡 分析邏輯: 比較同一訂單在兩個檔案中的商品項目數量")
    print()
    
    # 檢查檔案是否存在
    if not file1.exists():
        print(f"❌ 錯誤: 找不到檔案 {file1}")
        return False
        
    if not file2.exists():
        print(f"❌ 錯誤: 找不到檔案 {file2}")
        return False
    
    try:
        # 讀取CSV檔案
        print("📖 讀取檔案中...")
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(file2)
        
        print(f"📊 {file1.name}: {len(df1)} 列資料")
        print(f"📊 {file2.name}: {len(df2)} 列資料")
        print()
        
        # 檢查關鍵欄位是否存在
        check_columns = key_columns + ['product_name']  # 也需要商品名稱來更好地分析
        missing_cols_file1 = [col for col in check_columns if col not in df1.columns]
        missing_cols_file2 = [col for col in check_columns if col not in df2.columns]
        
        if missing_cols_file1:
            print(f"❌ 錯誤: {file1.name} 缺少欄位: {missing_cols_file1}")
            return False
            
        if missing_cols_file2:
            print(f"❌ 錯誤: {file2.name} 缺少欄位: {missing_cols_file2}")
            return False
        
        # 處理空值並準備資料
        print("🔧 準備資料...")
        
        # 處理空值
        for col in key_columns:
            df1[col] = df1[col].fillna('')
            df2[col] = df2[col].fillna('')
        
        # 過濾掉關鍵欄位為空的記錄
        df1_clean = df1.copy()
        df2_clean = df2.copy()
        
        for col in key_columns:
            df1_clean = df1_clean[df1_clean[col] != '']
            df2_clean = df2_clean[df2_clean[col] != '']
        
        print(f"🔧 過濾後有效資料: {file1.name} {len(df1_clean)} 列, {file2.name} {len(df2_clean)} 列")
        print()
        
        # 創建組合鍵 (order_date + order_sn + buyer_username)
        df1_clean['order_key'] = df1_clean[key_columns].astype(str).agg('||'.join, axis=1)
        df2_clean['order_key'] = df2_clean[key_columns].astype(str).agg('||'.join, axis=1)
        
        # 找出兩個檔案中都存在的訂單
        common_order_keys = set(df1_clean['order_key']).intersection(set(df2_clean['order_key']))
        
        print(f"🔍 發現 {len(common_order_keys)} 個相同的訂單 (order_date + order_sn + buyer_username)")
        print()
        
        if len(common_order_keys) == 0:
            print("✅ 沒有發現相同的訂單，無需檢查商品取消情況")
            return True
        
        # 分析每個相同訂單的商品項目數量
        print("=== 訂單商品項目數量分析 ===")
        print()
        
        cancellation_cases = []
        identical_cases = []
        
        for i, order_key in enumerate(list(common_order_keys)[:20]):  # 分析前20個
            # 獲取該訂單在兩個檔案中的所有記錄
            df1_order = df1_clean[df1_clean['order_key'] == order_key]
            df2_order = df2_clean[df2_clean['order_key'] == order_key]
            
            count1 = len(df1_order)
            count2 = len(df2_order)
            
            # 解析訂單資訊
            sample_row = df1_order.iloc[0]
            order_date = sample_row['order_date']
            order_sn = sample_row['order_sn']
            buyer_username = sample_row['buyer_username']
            
            print(f"{i+1}. 訂單: {order_sn}")
            print(f"   日期: {order_date}")
            print(f"   買家: {buyer_username}")
            print(f"   商品項目數量: {file1.name} = {count1}, {file2.name} = {count2}")
            
            if count1 != count2:
                # 商品項目數量不同，可能有取消
                print(f"   ⚠️  商品項目數量不同！差異: {abs(count1 - count2)}")
                
                # 顯示商品詳情
                products1 = df1_order['product_name'].fillna('未知商品').tolist()
                products2 = df2_order['product_name'].fillna('未知商品').tolist()
                
                print(f"   📦 {file1.name} 商品:")
                for j, product in enumerate(products1[:5]):  # 最多顯示5個
                    print(f"      {j+1}. {product}")
                if len(products1) > 5:
                    print(f"      ... 還有 {len(products1) - 5} 個商品")
                
                print(f"   📦 {file2.name} 商品:")
                for j, product in enumerate(products2[:5]):
                    print(f"      {j+1}. {product}")
                if len(products2) > 5:
                    print(f"      ... 還有 {len(products2) - 5} 個商品")
                
                # 找出差異的商品
                set1 = set(products1)
                set2 = set(products2)
                only_in_file1 = set1 - set2
                only_in_file2 = set2 - set1
                
                if only_in_file1:
                    print(f"   ➖ 只在 {file1.name} 中的商品: {list(only_in_file1)[:3]}")
                if only_in_file2:
                    print(f"   ➕ 只在 {file2.name} 中的商品: {list(only_in_file2)[:3]}")
                
                cancellation_cases.append({
                    'order_sn': order_sn,
                    'order_date': order_date,
                    'buyer_username': buyer_username,
                    'count1': count1,
                    'count2': count2,
                    'difference': abs(count1 - count2)
                })
                
            else:
                # 商品項目數量相同
                print(f"   ✅ 商品項目數量相同")
                identical_cases.append({
                    'order_sn': order_sn,
                    'order_date': order_date,
                    'buyer_username': buyer_username,
                    'count': count1
                })
            
            print()
        
        if len(common_order_keys) > 20:
            print(f"... 還有 {len(common_order_keys) - 20} 個訂單未顯示")
            print()
        
        # === 統計分析 ===
        print("=" * 60)
        print("📊 統計分析:")
        print()
        
        # 按 order_sn 統計出現次數
        print("=== Order SN 出現次數統計 ===")
        
        # 統計每個 order_sn 在兩個檔案中的出現次數
        sn_counts1 = df1_clean['order_sn'].value_counts()
        sn_counts2 = df2_clean['order_sn'].value_counts()
        
        # 找出在兩個檔案中都出現的 order_sn
        common_sns = set(sn_counts1.index).intersection(set(sn_counts2.index))
        
        print(f"共同出現的 order_sn 數量: {len(common_sns)}")
        
        different_count_sns = []
        for sn in common_sns:
            count1 = sn_counts1[sn]
            count2 = sn_counts2[sn]
            if count1 != count2:
                different_count_sns.append({
                    'order_sn': sn,
                    'count1': count1,
                    'count2': count2,
                    'difference': abs(count1 - count2)
                })
        
        if different_count_sns:
            print(f"\n⚠️  出現次數不同的 order_sn: {len(different_count_sns)}")
            print("前10個差異最大的:")
            
            # 按差異排序
            different_count_sns.sort(key=lambda x: x['difference'], reverse=True)
            
            for i, item in enumerate(different_count_sns[:10]):
                print(f"  {i+1}. {item['order_sn']}")
                print(f"     出現次數: {file1.name} = {item['count1']}, {file2.name} = {item['count2']}")
                print(f"     差異: {item['difference']}")
                print()
        else:
            print("✅ 所有共同的 order_sn 出現次數都相同")
        
        # === 總結 ===
        print("=" * 60)
        print("🎯 總結:")
        print(f"  相同訂單總數: {len(common_order_keys)}")
        print(f"  商品項目數量不同的訂單: {len(cancellation_cases)}")
        print(f"  商品項目數量相同的訂單: {len(identical_cases)}")
        print(f"  出現次數不同的 order_sn: {len(different_count_sns)}")
        
        if len(cancellation_cases) > 0:
            print(f"\n💡 建議:")
            print(f"  - {len(cancellation_cases)} 個訂單可能有部分商品被取消")
            print(f"  - 建議檢查這些訂單的 order_status 和 cancellation_reason")
        
        return True
        
    except Exception as e:
        print(f"❌ 處理檔案時發生錯誤: {str(e)}")
        return False

def main():
    """主函數"""
    print("開始檢查 Shopee Orders 商品取消分析...")
    print()
    
    success = check_order_cancellation()
    
    print()
    if success:
        print("🎉 分析完成")
    else:
        print("⚠️  分析過程中出現問題")
    
    # 暫停讓使用者查看結果
    input("\n按 Enter 鍵結束...")

if __name__ == "__main__":
    main()