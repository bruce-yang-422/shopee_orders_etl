import pandas as pd
import os

# 設定檔案路徑
csv_path = os.path.join('output', 'A01_master_orders_cleaned.csv')
txt_path = os.path.join('output', 'A01_master_orders_cleaned_columns.txt')

def list_columns(csv_file, output_txt):
    # 只讀欄位
    df = pd.read_csv(csv_file, nrows=0)
    columns = df.columns.tolist()
    # 存成 txt
    with open(output_txt, 'w', encoding='utf-8') as f:
        for col in columns:
            f.write(col + '\n')
    print(f'已輸出欄位清單至：{output_txt}')

if __name__ == '__main__':
    list_columns(csv_path, txt_path)
