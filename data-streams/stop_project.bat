@echo off
echo ===================================================
echo   DUNG HE THONG NYC TAXI PIPELINE
echo ===================================================

echo.
echo Dang tat cac container Docker (Kafka, Zookeeper, Postgres)...
docker-compose down

echo.
echo Tat cac tien trinh Python (Spark, Producer, Streamlit)...
taskkill /F /IM python.exe /T >nul 2>&1

echo.
echo ===================================================
echo HE THONG DA DUOC DUNG VA DON DEP SACH SE!
echo ===================================================
pause
