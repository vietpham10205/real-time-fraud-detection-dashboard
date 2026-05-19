@echo off
setlocal enabledelayedexpansion

echo ==========================================================
echo   KHOI DONG NYC TAXI: MULTI-SOURCE REAL-TIME PIPELINE
echo ==========================================================

echo.
echo [1/6] Kiem tra cac thu vien can thiet...
pip install -q pandas pyarrow kafka-python pyspark psycopg2-binary streamlit plotly scikit-learn numpy

echo.
echo [2/6] Dang khoi dong Docker (Kafka, Zookeeper, Postgres)...
docker compose up -d

echo.
echo === DOI CAC SERVICE TRO NEN HEALTHY ===
:WAIT_LOOP
docker compose ps --format "{{.Health}}" | findstr /i /v "healthy" | findstr "." >nul
if errorlevel 1 goto SERVICES_READY
echo Dang cho Docker services san sang...
timeout /t 5 /nobreak >nul
goto WAIT_LOOP

:SERVICES_READY
echo [OK] Tat ca Docker services da san sang!

echo.
echo [3/6] Reset Database Schema (Dam bao tuong thich đa nguon)...
python reset_db.py

echo.
echo [4/6] Khoi dong Spark Structured Streaming (Cua so moi)...
start cmd /k "title SPARK PROCESSOR && echo Dang chay Spark... && python spark_taxi_processor.py"

echo.
echo [5/6] Khoi dong Web Dashboard (Cua so moi)...
start cmd /k "title WEB DASHBOARD && echo Dang chay Streamlit... && streamlit run dashboard.py"

echo.
echo === DOI 15 GIAY DE SPARK VA DASHBOARD ON DINH ===
timeout /t 15 /nobreak

echo.
echo [6/6] Kich hoat Luong du lieu song song (Yellow + Green)...
start cmd /k "title YELLOW PRODUCER && echo Dang day du lieu Yellow Taxi... && python yellow_producer.py"
start cmd /k "title GREEN PRODUCER && echo Dang day du lieu Green Taxi... && python green_producer.py"

echo.
echo ==========================================================
echo TOAN BO HE THONG DA DUOC KICH HOAT THANH CONG!
echo ----------------------------------------------------------
echo - Dashboard: http://localhost:8501
echo - 2 Nguon du lieu dang do vao Kafka song song.
echo ==========================================================
pause
