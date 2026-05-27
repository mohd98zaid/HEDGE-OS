@echo off
REM ============================================================================
REM  PROJECT HEDGE — Ordered Startup
REM ============================================================================
REM
REM  Starts all services in the correct dependency order:
REM    1. Infrastructure (NATS, Redis, Postgres, Qdrant, Observability)
REM    2. Wait for NATS to be ready
REM    3. Hot_Path services in pipeline order
REM    4. UI Gateway
REM    5. React Cockpit UI
REM
REM  Double-click to start. Press any key to stop everything.
REM ============================================================================

setlocal enabledelayedexpansion
set "PROJECT_DIR=%~dp0"
cd /d "%PROJECT_DIR%"

echo.
echo  ============================================================
echo   PROJECT HEDGE — Ordered Startup
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

REM --- Load .env ---
if exist .env (
    for /f "usebackq eol=# tokens=1,* delims==" %%a in (".env") do (
        if not "%%a"=="" if not "%%b"=="" set "%%a=%%b"
    )
)

REM --- Set defaults ---
if not defined HEDGE_REDIS_URL set "HEDGE_REDIS_URL=redis://127.0.0.1:6379"
if not defined HEDGE_POSTGRES_URL set "HEDGE_POSTGRES_URL=postgresql://hedge:hedge@127.0.0.1:5432/hedge"
if not defined HEDGE_QDRANT_URL set "HEDGE_QDRANT_URL=http://127.0.0.1:6333"
if not defined HEDGE_OLLAMA_URL set "HEDGE_OLLAMA_URL=http://127.0.0.1:11434"
if not defined HEDGE_UI_GATEWAY_BIND set "HEDGE_UI_GATEWAY_BIND=0.0.0.0:8088"
if not defined RUST_LOG set "RUST_LOG=info"
if not defined TZ set "TZ=Asia/Kolkata"

REM NATS credentials per account (dev defaults from docker-compose.yml)
set "NATS_HOT_PATH=nats://127.0.0.1:4222"
set "NATS_UI_GATEWAY=nats://127.0.0.1:4222"
set "NATS_SUPERVISOR=nats://127.0.0.1:4222"

REM ============================================================
REM  STEP 1: Infrastructure
REM ============================================================
echo  [1/5] Starting infrastructure...
docker compose --profile infra down nats >nul 2>&1
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
echo        Waiting for Redis...
timeout /t 3 /nobreak >nul
echo        Infrastructure ready.
echo.

REM ============================================================
REM  STEP 2: Session + Supervisor (no market dependency)
REM ============================================================
echo  [2/5] Starting session controller + supervisor...
start /min "HEDGE-session" cmd /c "set HEDGE_NATS_URL=%NATS_HOT_PATH% && target\release\hedge-session.exe"
start /min "HEDGE-supervisor" cmd /c "set HEDGE_NATS_URL=%NATS_SUPERVISOR% && target\release\hedge-supervisor.exe"
timeout /t 2 /nobreak >nul
echo        Session + Supervisor started.
echo.

REM ============================================================
REM  STEP 3: Hot_Path pipeline (in order)
REM ============================================================
echo  [3/5] Starting Hot_Path pipeline (in dependency order)...

echo        [a] Market Data Engine (Upstox Feed)...
start /min "HEDGE-market-data" cmd /c "set HEDGE_NATS_URL=%NATS_HOT_PATH% && target\release\upstox-feed.exe"
timeout /t 2 /nobreak >nul

echo        [b] Orderflow Engine...
start /min "HEDGE-orderflow" cmd /c "set HEDGE_NATS_URL=%NATS_HOT_PATH% && target\release\hedge-orderflow.exe"
timeout /t 1 /nobreak >nul

echo        [c] Feature Extraction Engine...
start /min "HEDGE-features" cmd /c "set HEDGE_NATS_URL=%NATS_HOT_PATH% && target\release\hedge-features.exe"
timeout /t 1 /nobreak >nul

echo        [d] Signal Engine...
start /min "HEDGE-signals" cmd /c "set HEDGE_NATS_URL=%NATS_HOT_PATH% && target\release\hedge-signals.exe"
timeout /t 1 /nobreak >nul

echo        [e] Risk Engine...
start /min "HEDGE-risk" cmd /c "set HEDGE_NATS_URL=%NATS_HOT_PATH% && target\release\hedge-risk.exe"
timeout /t 1 /nobreak >nul

echo        [f] Execution Engine...
start /min "HEDGE-exec" cmd /c "set HEDGE_NATS_URL=%NATS_HOT_PATH% && target\release\hedge-exec.exe"
timeout /t 1 /nobreak >nul

echo        [g] Position Engine...
start /min "HEDGE-position" cmd /c "set HEDGE_NATS_URL=%NATS_HOT_PATH% && target\release\hedge-position.exe"
timeout /t 1 /nobreak >nul

echo        [h] Replay Engine...
start /min "HEDGE-replay" cmd /c "set HEDGE_NATS_URL=%NATS_HOT_PATH% && target\release\hedge-replay.exe"
timeout /t 1 /nobreak >nul

echo        Hot_Path pipeline started.
echo.

REM ============================================================
REM  STEP 4: UI Gateway (needs NATS + Hot_Path publishing)
REM ============================================================
echo  [4/5] Starting UI Gateway...
set "HEDGE_NATS_URL=%NATS_UI_GATEWAY%"
start /min "HEDGE-ui-gateway" cmd /c "set HEDGE_NATS_URL=%NATS_UI_GATEWAY% & target\release\hedge-ui-gateway.exe"
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
start "HEDGE-UI" cmd /c "set VITE_HEDGE_GATEWAY_URL=ws://127.0.0.1:8088/ws && npm run dev"
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
echo.
echo   Dashboards:
echo     Cockpit UI           : http://localhost:5173
echo     Grafana              : http://localhost:3000  (admin / hedge)
echo     NATS Monitor         : http://localhost:8222
echo     Jaeger Traces        : http://localhost:16686
echo     Prometheus           : http://localhost:9090
echo.
echo   Brokers configured:
echo     Primary: Upstox      (access token in .env)
echo     Backup:  Angel One   (credentials pending)
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

echo  Stopping Docker infrastructure...
docker compose --profile infra down >nul 2>&1

echo.
echo  All services stopped. Goodbye.
echo.
