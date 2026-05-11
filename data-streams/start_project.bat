@echo off
echo ===================================================
echo   KHOI DONG HE THONG PHAT HIEN GIAN LAN REAL-TIME
echo ===================================================

echo.
echo [1/3] Dang khoi dong Docker (Kafka, Zookeeper)...
docker-compose up -d --build
echo [OK] Docker da chay nen thanh cong!
echo.
echo === VUI LONG DOI 15 GIAY DE KAFKA KHOI DONG XONG ===
timeout /t 15 /nobreak

echo.
echo [2/3] Dang khoi dong Loi phan tich Spark (Mo o cua so moi)...
start cmd /k "title SPARK ANOMALY DETECTION && echo Dang chay Spark... && python spark_anomaly_detection.py"

echo.
echo [3/3] Dang khoi dong Giao dien Web Dashboard (Mo o cua so moi)...
start cmd /k "title WEB DASHBOARD && echo Dang chay Streamlit... && streamlit run dashboard.py"

echo.
echo ===================================================
echo TOAN BO HE THONG DA DUOC KICH HOAT!
echo Vui long mo trinh duyet va truy cap: http://localhost:8501
echo Ban co the thu nho cua so nay lai.
echo ===================================================
pause
