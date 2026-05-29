@echo off
echo ===================================================
echo   DUNG HE THONG NYC TAXI PIPELINE (MULTI-SOURCE)
echo ===================================================

echo.
echo [1/2] Dang tat cac ung dung Python...
taskkill /F /FI "WINDOWTITLE eq YELLOW PRODUCER*"    /T >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq GREEN PRODUCER*"     /T >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq SPARK PROCESSOR*"    /T >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq WEB DASHBOARD*"      /T >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq BENCHMARK EVALUATOR*" /T >nul 2>&1
echo [OK] Tat ca 5 cua so (bao gom Benchmark Evaluator) da duoc dong.

echo.
echo [2/2] Dang tat Docker (Kafka, Zookeeper, Postgres)...
docker compose down

echo.
echo === DON DEP FILE TAM (Spark Checkpoints) ===
:: Xoa cac thu muc tam neu ton tai
if exist "spark-warehouse" rd /s /q "spark-warehouse"
if exist "checkpoint" rd /s /q "checkpoint"

echo.
echo ===================================================
echo HE THONG DA DUOC DUNG VA DON DEP SACH SE!
echo ===================================================
pause
