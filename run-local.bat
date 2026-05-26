@echo off
REM ============================================================================
REM  PROJECT HEDGE — Local Run (no Docker build needed for Rust services)
REM ============================================================================
REM
REM  Runs infrastructure in Docker (pre-built images) and Hot_Path services
REM  as native Windows binaries from target\release\.
REM
REM  Usage:
REM    run-local.bat          — Start everything
REM    run-local.bat stop     — Stop all services
REM
REM ============================================================================

setlocal enabledelayedexpansion
set "PROJECT_DIR=%~dp0"
cd /d "%PROJECT_DIR%"

set "CMD=%~1"
if "%CMD%"=="" set "CMD=start"
if /i "%CMD%"=="stop" goto :stop

echo.
echo  ============================================================
echo   PROJECT HEDGE — Starting (Local Mode)
echo  ============================================================
echo.

REM Check Docker
docker info >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Docker is not running. Start Docker Desktop for infrastructure.
    pause
    exit /b 1
)

REM Check binaries exist
if not exist "target\release\hedge-session.exe" (
    echo  [ERROR] Release binaries not found. Run: cargo build --release --workspace
    pause
    exit /b 1
)

echo  [1/4] Starting infrastructure (NATS, Redis, Postgres, Qdrant, Prometheus, Loki, Jaeger, Grafana)...
docker compose --profile infra up -d
timeout /t 10 /nobreak >nul

echo.
echo  [2/4] Starting Hot_Path services (native Windows binaries)...

REM Load .env file
if exist .env (
    for /f "usebackq tokens=1,* delims==" %%a in (".env") do (
        set "%%a=%%b"
    )
)

REM Set defaults for local run
if not defined HEDGE_NATS_URL set "HEDGE_NATS_URL=nats://127.0.0.1:4222"
if not defined HEDGE_REDIS_URL set "HEDGE_REDIS_URL=redis://127.0.0.1:6379"
if not defined RUST_LOG set "RUST_LOG=info"

REM Start each Hot_Path service in its own window
start "HEDGE-session" cmd /c "target\release\hedge-session.exe"
start "HEDGE-supervisor" cmd /c "target\release\hedge-supervisor.exe"
start "HEDGE-market-data" cmd /c "target\release\hedge-market-data.exe"
start "HEDGE-orderflow" cmd /c "target\release\hedge-orderflow.exe"
start "HEDGE-features" cmd /c "target\release\hedge-features.exe"
start "HEDGE-signals" cmd /c "target\release\hedge-signals.exe"
start "HEDGE-risk" cmd /c "target\release\hedge-risk.exe"
start "HEDGE-exec" cmd /c "target\release\hedge-exec.exe"
start "HEDGE-position" cmd /c "target\release\hedge-position.exe"
start "HEDGE-replay" cmd /c "target\release\hedge-replay.exe"
start "HEDGE-ui-gateway" cmd /c "target\release\hedge-ui-gateway.exe"

echo.
echo  [3/4] Starting Warm_AI_Pipeline services (Docker — pre-built images)...
docker compose --profile warm_ai up -d 2>nul

echo.
echo  [4/4] Starting React UI dev server...
cd ui
if not exist node_modules call npm install
start "HEDGE-UI" cmd /c "npm run dev"
cd ..

echo.
echo  ============================================================
echo   PROJECT HEDGE is running! (Local Mode)
echo  ============================================================
echo.
echo   Cockpit UI:    http://localhost:5173
echo   UI Gateway:    ws://localhost:8088
echo   Grafana:       http://localhost:3000  (admin / hedge)
echo   Jaeger:        http://localhost:16686
echo   Prometheus:    http://localhost:9090
echo   NATS Monitor:  http://localhost:8222
echo.
echo   Press any key to stop all services and exit...
echo.
pause >nul
goto :stop

REM ============================================================================
:stop
REM ============================================================================
echo.
echo  Stopping all services...

REM Kill Hot_Path windows
taskkill /fi "WINDOWTITLE eq HEDGE-session*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-supervisor*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-market-data*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-orderflow*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-features*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-signals*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-risk*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-exec*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-position*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-replay*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-ui-gateway*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-UI*" /f >nul 2>&1

REM Stop Docker infrastructure
docker compose --profile infra down >nul 2>&1
docker compose --profile warm_ai down >nul 2>&1

echo  All services stopped.
echo.
