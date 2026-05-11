@echo off
echo ===================================================
echo   DUNG HE THONG NYC TAXI PIPELINE
echo ===================================================

echo.
echo [1/2] Dang tat cac ung dung Python...
taskkill /F /FI "WINDOWTITLE eq KAFKA PRODUCER*" /T >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq SPARK PROCESSOR*" /T >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq WEB DASHBOARD*" /T >nul 2>&1
echo [OK] Cac cua so Python da duoc tat.

echo.
echo [2/2] Dang tat Docker (Kafka, Zookeeper, Postgres)...
docker compose down

echo.
echo === DON DEP FILE TAM (Spark Checkpoints) ===
if exist "spark-warehouse" rd /s /q "spark-warehouse"
if exist "checkpoint" rd /s /q "checkpoint"

echo.
echo ===================================================
echo HE THONG DA DUOC DUNG VA DON DEP SACH SE!
echo ===================================================
pause
