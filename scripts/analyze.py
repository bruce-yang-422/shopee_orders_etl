import pandas as pd

def analyze_csv(input_csv, output_txt):
    df = pd.read_csv(input_csv)

    with open(output_txt, 'w', encoding='utf-8') as f:
        f.write("欄位名稱:\n")
        for col in df.columns:
            f.write(f"- {col}\n")

        f.write("\n前5筆資料範例:\n")
        f.write(df.head().to_string())

        f.write("\n\n隨機抽5筆資料範例:\n")
        sample_df = df.sample(n=5) if len(df) >= 5 else df
        f.write(sample_df.to_string())

        f.write("\n\n欄位資料型態:\n")
        f.write(df.dtypes.to_string())

        f.write("\n\n空值統計:\n")
        f.write(df.isnull().sum().to_string())

if __name__ == "__main__":
    input_csv = r"C:\Users\user\Documents\shopee_orders_etl\output\A01_master_orders_cleaned.csv"
    output_txt = r"C:\Users\user\Documents\shopee_orders_etl\input\csv_structure_report2.txt"

    analyze_csv(input_csv, output_txt)
    print(f"分析報告已輸出到 {output_txt}")
