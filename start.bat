@echo off
REM ============================================================================
REM  PROJECT HEDGE - Ordered Startup
REM ============================================================================
REM
REM  Starts all services in the correct dependency order:
REM    1. Infrastructure (NATS only - everything else is optional)
REM    2. Wait for NATS to be ready
REM    3. Hot_Path services in pipeline order
REM    4. UI Gateway
REM    5. React Cockpit UI
REM
REM  Service windows are kept OPEN on exit so you can see error messages.
REM  Press any key in this window to stop everything.
REM ============================================================================

setlocal enabledelayedexpansion
set "PROJECT_DIR=%~dp0"
cd /d "%PROJECT_DIR%"

echo.
echo  ============================================================
echo   PROJECT HEDGE - Ordered Startup
echo  ============================================================
echo.

REM --- Pre-flight checks ---
docker info >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Docker is not running. Please start Docker Desktop.
    pause
    exit /b 1
)

if not exist "target\release\hedge-session.exe" (
    echo  [ERROR] Binaries not found. Building now...
    cargo build --release --workspace
    if errorlevel 1 (
        echo  [ERROR] Build failed.
        pause
        exit /b 1
    )
)

if not exist "target\release\upstox-feed.exe" (
    echo  [ERROR] upstox-feed.exe missing. Building...
    cargo build --release -p hedge-market-data --bin upstox-feed
    if errorlevel 1 (
        echo  [ERROR] Build of upstox-feed failed.
        pause
        exit /b 1
    )
)

REM --- Load .env (key=value lines, skip comments) ---
if exist .env (
    for /f "usebackq eol=# tokens=1,* delims==" %%a in (".env") do (
        if not "%%a"=="" if not "%%b"=="" set "%%a=%%b"
    )
)

REM --- Check Upstox token presence (required for live data) ---
if not defined HEDGE_UPSTOX_ACCESS_TOKEN (
    echo  [WARNING] HEDGE_UPSTOX_ACCESS_TOKEN is not set.
    echo            The upstox-feed window will exit immediately.
    echo            Refresh your token in .env and re-run start.bat.
    echo.
)

REM --- Set defaults ---
if not defined HEDGE_REDIS_URL set "HEDGE_REDIS_URL=redis://127.0.0.1:6379"
if not defined HEDGE_POSTGRES_URL set "HEDGE_POSTGRES_URL=postgresql://hedge:hedge@127.0.0.1:5432/hedge"
if not defined HEDGE_QDRANT_URL set "HEDGE_QDRANT_URL=http://127.0.0.1:6333"
if not defined HEDGE_OLLAMA_URL set "HEDGE_OLLAMA_URL=http://127.0.0.1:11434"
if not defined HEDGE_UI_GATEWAY_BIND set "HEDGE_UI_GATEWAY_BIND=0.0.0.0:8088"
if not defined RUST_LOG set "RUST_LOG=info"
if not defined TZ set "TZ=Asia/Kolkata"
if not defined HEDGE_UPSTOX_INSTRUMENTS set "HEDGE_UPSTOX_INSTRUMENTS=NSE_EQ|INE002A01018,NSE_EQ|INE009A01021,NSE_EQ|INE062A01020,NSE_EQ|INE040A01034,NSE_EQ|INE090A01021"

REM NATS URL (no auth in dev)
set "HEDGE_NATS_URL=nats://127.0.0.1:4222"

REM ============================================================
REM  STEP 1: Infrastructure
REM ============================================================
echo  [1/5] Starting infrastructure...
docker compose --profile infra up -d
echo        Waiting for NATS to be ready...

:wait_nats
timeout /t 2 /nobreak >nul
curl -s http://127.0.0.1:8222/varz >nul 2>&1
if errorlevel 1 (
    echo        ... still waiting for NATS
    goto :wait_nats
)
echo        NATS is ready.
echo        Infrastructure ready.
echo.

REM ============================================================
REM  STEP 2: Session + Supervisor (no market dependency)
REM ============================================================
echo  [2/5] Starting session controller + supervisor...
start "HEDGE-session" cmd /k target\release\hedge-session.exe
start "HEDGE-supervisor" cmd /k target\release\hedge-supervisor.exe
timeout /t 2 /nobreak >nul
echo        Session + Supervisor started.
echo.

REM ============================================================
REM  STEP 3: Hot_Path pipeline (in order)
REM ============================================================
echo  [3/5] Starting Hot_Path pipeline (in dependency order)...

echo        [a] Market Data Engine (Upstox REST poller)...
start "HEDGE-market-data" cmd /k target\release\upstox-feed.exe
timeout /t 3 /nobreak >nul

echo        [b] Orderflow Engine...
start "HEDGE-orderflow" cmd /k target\release\hedge-orderflow.exe
timeout /t 1 /nobreak >nul

echo        [c] Feature Extraction Engine...
start "HEDGE-features" cmd /k target\release\hedge-features.exe
timeout /t 1 /nobreak >nul

echo        [d] Signal Engine...
start "HEDGE-signals" cmd /k target\release\hedge-signals.exe
timeout /t 1 /nobreak >nul

echo        [e] Risk Engine...
start "HEDGE-risk" cmd /k target\release\hedge-risk.exe
timeout /t 1 /nobreak >nul

echo        [f] Execution Engine...
start "HEDGE-exec" cmd /k target\release\hedge-exec.exe
timeout /t 1 /nobreak >nul

echo        [g] Position Engine...
start "HEDGE-position" cmd /k target\release\hedge-position.exe
timeout /t 1 /nobreak >nul

echo        [h] Replay Engine...
start "HEDGE-replay" cmd /k target\release\hedge-replay.exe
timeout /t 1 /nobreak >nul

echo        Hot_Path pipeline started.
echo.

REM ============================================================
REM  STEP 4: UI Gateway (needs NATS + Hot_Path publishing)
REM ============================================================
echo  [4/5] Starting UI Gateway...
start "HEDGE-ui-gateway" cmd /k target\release\hedge-ui-gateway.exe
timeout /t 2 /nobreak >nul
echo        UI Gateway started on ws://localhost:8088
echo.

REM ============================================================
REM  STEP 5: React Cockpit UI
REM ============================================================
echo  [5/5] Starting React Cockpit UI...
pushd ui
if not exist node_modules (
    echo        Installing UI dependencies...
    call npm install >nul 2>&1
)
set "VITE_HEDGE_GATEWAY_URL=ws://127.0.0.1:8088/ws"
start "HEDGE-UI" cmd /k npm run dev
popd
timeout /t 3 /nobreak >nul

echo.
echo  ============================================================
echo   PROJECT HEDGE is running!
echo  ============================================================
echo.
echo   Hot_Path Pipeline:
echo     Market Data  -^> Orderflow -^> Features -^> Signals
echo     -^> Risk -^> Execution -^> Position
echo.
echo   Services:
echo     Session Controller   : running (09:15-15:30 IST gate)
echo     Supervisor           : running (self-healing)
echo     Replay Engine        : running (recording)
echo     UI Gateway           : ws://localhost:8088
echo     Upstox Feed          : REST polling, 500ms LTP / 2s book
echo.
echo   Dashboards:
echo     Cockpit UI           : http://localhost:5173
echo     Grafana              : http://localhost:3000  (admin / hedge)
echo     NATS Monitor         : http://localhost:8222
echo     Jaeger Traces        : http://localhost:16686
echo     Prometheus           : http://localhost:9090
echo.
echo   Brokers configured:
echo     Primary: Upstox      (access token from .env)
echo     Backup:  Angel One   (credentials in .env)
echo.
echo  ============================================================
echo   Press any key to STOP all services and exit...
echo  ============================================================
pause >nul

REM ============================================================
REM  SHUTDOWN (reverse order)
REM ============================================================
echo.
echo  Stopping all services...

taskkill /fi "WINDOWTITLE eq HEDGE-UI*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-ui-gateway*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-replay*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-position*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-exec*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-risk*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-signals*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-features*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-orderflow*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-market-data*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-supervisor*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-session*" /f >nul 2>&1

echo.
echo  All services stopped (Docker infra left running).
echo.
