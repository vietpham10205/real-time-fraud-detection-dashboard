@echo off
setlocal enabledelayedexpansion

echo ==========================================================
echo   KHOI DONG NYC TAXI: REAL-TIME FLEET INTELLIGENCE PIPELINE
echo ==========================================================

echo.
echo [1/5] Kiem tra va cai dat cac thu vien can thiet...
pip install -q pandas pyarrow kafka-python pyspark psycopg2-binary streamlit plotly scikit-learn numpy

echo.
echo [2/5] Dang khoi dong Docker (Kafka, Zookeeper, Postgres)...
docker compose up -d

echo.
echo === DOI CAC SERVICE TRO NEN HEALTHY ===
:WAIT_LOOP
:: Kiem tra trang thai (case-insensitive /i)
docker compose ps --format "{{.Health}}" | findstr /i /v "healthy" | findstr "." >nul
if errorlevel 1 goto SERVICES_READY

echo Dang cho Docker services (Zookeeper, Kafka, Postgres)...
timeout /t 5 /nobreak >nul
goto WAIT_LOOP

:SERVICES_READY
echo [OK] Tat ca Docker services da san sang!

echo.
echo [3/5] Khoi dong Kafka Producer TRUOC...
start cmd /k "title KAFKA PRODUCER && echo Dang day du lieu vao Kafka... && python taxi_producer.py"

echo.
echo === DOI 10 GIAY DE PRODUCER KHOI TAO TOPIC ===
timeout /t 10 /nobreak

echo.
echo [4/5] Khoi dong Spark Structured Streaming va ML (Cua so moi)...
start cmd /k "title SPARK PROCESSOR && echo Dang chay Spark... && python spark_taxi_processor.py"

echo.
echo [5/5] Khoi dong Web Dashboard (Cua so moi)...
start cmd /k "title WEB DASHBOARD && echo Dang chay Streamlit... && streamlit run dashboard.py"

echo.
echo ==========================================================
echo TOAN BO HE THONG DA DUOC KICH HOAT!
echo Vui long mo trinh duyet va truy cap: http://localhost:8501
echo ==========================================================
pause
