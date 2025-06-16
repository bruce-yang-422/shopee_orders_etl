@echo off
REM --------------------------------------------------------
REM 1. 啟動位於專案的 venv 虛擬環境
REM --------------------------------------------------------
call "C:\Users\user\Documents\shopee_orders_etl\venv\Scripts\activate.bat"

REM --------------------------------------------------------
REM 2. 使用剛才啟動的虛擬環境 Python 執行腳本
REM --------------------------------------------------------
python "C:\Users\user\Documents\shopee_orders_etl\scripts\order_processing_script.py"

REM --------------------------------------------------------
REM 3. 執行完畢後暫停等待，以便查看輸出或錯誤
REM --------------------------------------------------------
pause
