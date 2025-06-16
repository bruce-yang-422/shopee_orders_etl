import os
import datetime

def export_directory_tree_with_exclude(
    startpath, 
    output_filename="directory_tree_simple.txt", 
    exclude_dirs=None
):
    """
    掃描指定路徑的檔案結構，並將樹狀圖匯出到文字檔案中。
    可以排除特定的資料夾。

    :param startpath: 要掃描的資料夾路徑。
    :param output_filename: 匯出的檔案名稱。
    :param exclude_dirs: 要排除的資料夾名稱列表 (例如 ['.git', '.venv', '__pycache__'])。
    """
    # 預設排除的資料夾
    if exclude_dirs is None:
        exclude_dirs = ['.git', '.venv', '__pycache__']
    
    if not os.path.exists(startpath):
        print(f"錯誤：找不到路徑 '{startpath}'")
        return

    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            # 寫入檔案標頭資訊
            f.write(f"資料夾結構掃描 (精簡版)\n")
            f.write("="*40 + "\n")
            f.write(f"掃描路徑: {startpath}\n")
            f.write(f"報告產製時間: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"已排除資料夾: {', '.join(exclude_dirs)}\n")
            f.write("="*40 + "\n\n")

            for root, dirs, files in os.walk(startpath):
                # *** 核心修改：在此處排除指定的資料夾 ***
                # `dirs[:] = [...]` 的寫法是為了直接修改 os.walk 將會走訪的目錄列表
                dirs[:] = [d for d in dirs if d not in exclude_dirs]
                
                level = root.replace(startpath, '').count(os.sep)
                indent = ' ' * 4 * level
                
                # 取得當前目錄的基本名稱
                dir_name = os.path.basename(root)
                # 如果是根目錄，則使用 startpath 的基本名稱
                if root == startpath:
                    dir_name = os.path.basename(startpath)

                f.write(f"{indent}📂 {dir_name}/\n")
                
                sub_indent = ' ' * 4 * (level + 1)
                for filename in files:
                    f.write(f"{sub_indent}📄 {filename}\n")
        
        output_abs_path = os.path.abspath(output_filename)
        print(f"✅ 成功！精簡後的目錄樹狀圖已匯出至：\n{output_abs_path}")

    except IOError as e:
        print(f"錯誤：無法寫入檔案 '{output_filename}'。錯誤訊息: {e}")


# --- 使用方式 ---

# 1. 指定要掃描的資料夾路徑
folder_to_scan = r"C:\Users\user\Documents\shopee_orders_etl"

# 2. 指定要匯出的檔案名稱
output_file = "shopee專案結構_精簡版.txt"

# 3. (可選) 自訂要排除的資料夾列表
#    預設會排除 ['.git', '.venv', '__pycache__']
#    如果還有其他想排除的，可以這樣寫：
#    exclude_list = ['.git', '.venv', '__pycache__', 'archive', 'output']
#    export_directory_tree_with_exclude(folder_to_scan, output_file, exclude_dirs=exclude_list)


# 執行函式 (使用預設排除列表)
export_directory_tree_with_exclude(folder_to_scan, output_file)