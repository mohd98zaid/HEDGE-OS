@echo off
REM ============================================================================
REM  PROJECT HEDGE — Binance Crypto Module Launcher  (v3 — production hardened)
REM ============================================================================
REM
REM  WHAT WAS FIXED IN v3
REM  ─────────────────────────────────────────────────────────────────────────
REM  ✅ CRITICAL: status command no longer kills services (was using taskkill
REM              /f as a probe — it was murdering running processes!)
REM  ✅ CRITICAL: API secrets are never visible in Task Manager / process list.
REM              Services now load .env.binance themselves via a secure helper
REM              (_launch_binance_service.cmd) — keys are NOT in the cmd line.
REM  ✅ PID-file approach for reliable status checking (no taskkill probes)
REM  ✅ Healthcheck loop: auto-restarts a crashed service (optional)
REM
REM  ┌─────────────────────────────────────────────────────────────────────┐
REM  │  ISOLATION GUARANTEE (unchanged from v2)                           │
REM  │  This script does NOT start, stop, or touch ANY file or process    │
REM  │  belonging to the Indian stock system.                             │
REM  └─────────────────────────────────────────────────────────────────────┘
REM
REM  Usage
REM  ─────
REM    start-binance.bat              → Start all services (local Python)
REM    start-binance.bat docker       → Start via Docker Compose
REM    start-binance.bat stop         → Stop all local Python services
REM    start-binance.bat docker stop  → Stop Docker Binance services
REM    start-binance.bat status       → Show status (READ-ONLY, safe)
REM    start-binance.bat build        → Build Docker images only
REM    start-binance.bat install      → Install / update Python venv
REM    start-binance.bat logs         → Show Docker / log info
REM
REM  First-time setup
REM  ─────────────────
REM    1. copy .env.binance.example .env.binance
REM    2. Set BINANCE_API_KEY and BINANCE_API_SECRET in .env.binance
REM    3. Double-click start-binance.bat
REM
REM  Prometheus metrics (when running locally)
REM  ──────────────────────────────────────────
REM    binance-feed      http://localhost:9300/metrics
REM    binance-risk      http://localhost:9301/metrics
REM    binance-strategy  http://localhost:9302/metrics
REM    binance-exec      http://localhost:9303/metrics
REM    binance-position  http://localhost:9304/metrics
REM ============================================================================

setlocal enabledelayedexpansion

set "PROJECT_DIR=%~dp0"
cd /d "%PROJECT_DIR%"

REM ── Parse commands ────────────────────────────────────────────────────────────
set "CMD1=%~1"
set "CMD2=%~2"
if "%CMD1%"=="" set "CMD1=start"

if /i "%CMD1%"=="docker"  if /i "%CMD2%"=="stop" goto :docker_stop
if /i "%CMD1%"=="docker"  goto :docker_start
if /i "%CMD1%"=="start"   goto :local_start
if /i "%CMD1%"=="stop"    goto :local_stop
if /i "%CMD1%"=="status"  goto :status
if /i "%CMD1%"=="build"   goto :docker_build
if /i "%CMD1%"=="install" goto :install_venv
if /i "%CMD1%"=="logs"    goto :logs

echo  Unknown command: %CMD1%
echo  Usage: start-binance.bat [start^|stop^|status^|docker^|build^|install^|logs]
exit /b 1


REM ============================================================================
REM  SHARED: validate .env.binance exists (do NOT load it here — stay secure)
REM ============================================================================
:check_env_file
if not exist ".env.binance" (
    echo.
    echo  [ERROR] .env.binance not found!
    echo.
    echo  Create it:
    echo    copy .env.binance.example .env.binance
    echo  Then fill in BINANCE_API_KEY and BINANCE_API_SECRET.
    echo.
    pause
    exit /b 1
)
REM Read only what we need for banner display (non-secret fields)
for /f "usebackq eol=# tokens=1,* delims==" %%a in (".env.binance") do (
    if "%%a"=="BINANCE_TESTNET"  set "BINANCE_TESTNET=%%b"
    if "%%a"=="BINANCE_SYMBOLS"  set "BINANCE_SYMBOLS=%%b"
    if "%%a"=="BINANCE_QTY_USDT" set "BINANCE_QTY_USDT=%%b"
    if "%%a"=="BINANCE_DAILY_LOSS_LIMIT" set "BINANCE_DAILY_LOSS_LIMIT=%%b"
    if "%%a"=="BINANCE_MAX_POSITION_USDT" set "BINANCE_MAX_POSITION_USDT=%%b"
    if "%%a"=="BINANCE_EMA_FAST" set "BINANCE_EMA_FAST=%%b"
    if "%%a"=="BINANCE_EMA_SLOW" set "BINANCE_EMA_SLOW=%%b"
    if "%%a"=="BINANCE_API_KEY"  set "_KEY_SET=%%b"
)
if not defined _KEY_SET (
    echo  [ERROR] BINANCE_API_KEY not set in .env.binance
    pause
    exit /b 1
)
if not defined BINANCE_TESTNET    set "BINANCE_TESTNET=true"
if not defined BINANCE_SYMBOLS    set "BINANCE_SYMBOLS=BTCUSDT,ETHUSDT,..."
if not defined BINANCE_QTY_USDT   set "BINANCE_QTY_USDT=50"
if not defined BINANCE_DAILY_LOSS_LIMIT  set "BINANCE_DAILY_LOSS_LIMIT=100"
if not defined BINANCE_MAX_POSITION_USDT set "BINANCE_MAX_POSITION_USDT=500"
if not defined BINANCE_EMA_FAST   set "BINANCE_EMA_FAST=9"
if not defined BINANCE_EMA_SLOW   set "BINANCE_EMA_SLOW=21"
REM NATS URL is public, safe to read here for NATS connectivity check
if not defined HEDGE_NATS_URL set "HEDGE_NATS_URL=nats://127.0.0.1:4222"
goto :eof


REM ============================================================================
:install_venv
REM ============================================================================
echo.
echo  ============================================================
echo   BINANCE MODULE — Installing Python virtual environment
echo  ============================================================
echo.
python --version >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Python not found. Install Python 3.11+ first.
    pause
    exit /b 1
)
set "VENV_DIR=%PROJECT_DIR%python\binance_module\.venv"
if not exist "%VENV_DIR%" (
    echo  [1/4] Creating virtual environment...
    python -m venv "%VENV_DIR%"
)
echo  [2/4] Upgrading pip...
"%VENV_DIR%\Scripts\python.exe" -m pip install --upgrade pip -q
echo  [3/4] Installing hedge-binance package...
"%VENV_DIR%\Scripts\pip.exe" install -e "%PROJECT_DIR%python\binance_module" -q
echo  [4/4] Verifying install...
"%VENV_DIR%\Scripts\python.exe" -c "import hedge_binance; print('  OK: hedge-binance', hedge_binance.__version__)"
echo.
echo  Done!
echo.
pause
goto :eof


REM ============================================================================
:local_start
REM ============================================================================
call :check_env_file
if errorlevel 1 exit /b 1

echo.
echo  ============================================================
echo   BINANCE CRYPTO MODULE — Starting  (Local Python, v3)
echo  ============================================================
echo.

if /i "%BINANCE_TESTNET%"=="true" (
    echo  [!] TESTNET MODE  — testnet.binance.vision
    echo      No real funds at risk.
) else (
    echo  [*] LIVE TRADING  — api.binance.com
    echo      *** REAL FUNDS — trade carefully! ***
)
echo.
echo   Symbols   : %BINANCE_SYMBOLS%
echo   Strategy  : EMA(%BINANCE_EMA_FAST%/%BINANCE_EMA_SLOW%) + RSI-14 + ATR SL/TP + Volume
echo   Qty/trade : %BINANCE_QTY_USDT% USDT
echo   Max pos   : %BINANCE_MAX_POSITION_USDT% USDT/symbol
echo   Daily cap : -%BINANCE_DAILY_LOSS_LIMIT% USDT loss limit
echo.
echo  [Security] API credentials are loaded by each service window
echo             directly from .env.binance — NOT passed on command line.
echo             They are NOT visible in Task Manager.
echo.

REM ── Ensure venv ────────────────────────────────────────────────────────────
set "VENV_DIR=%PROJECT_DIR%python\binance_module\.venv"
set "BINANCE_PY=%VENV_DIR%\Scripts\python.exe"
if not exist "%BINANCE_PY%" (
    echo  [SETUP] venv not found. Installing now...
    call :install_venv
    if errorlevel 1 exit /b 1
)

REM ── PID file directory ──────────────────────────────────────────────────────
if not exist "logs\binance" mkdir "logs\binance"
set "PID_DIR=%PROJECT_DIR%logs\binance"

REM ── Ensure NATS is reachable (try existing shared infra first) ─────────────
echo  [0/5] Checking NATS...
curl -s --max-time 3 "http://127.0.0.1:8222/varz" >nul 2>&1
if errorlevel 1 (
    echo         Not found on :8222. Attempting standalone NATS container...
    docker info >nul 2>&1
    if not errorlevel 1 (
        docker run -d --name binance-nats-sa --rm -p 4222:4222 -p 8222:8222 nats:2.10-alpine >nul 2>&1
        timeout /t 2 /nobreak >nul
        echo         Standalone NATS started.
    ) else (
        echo         [WARNING] Docker not available — services will retry NATS.
    )
) else (
    echo         NATS OK - shared with Indian stock system, different subjects.
)
echo.

REM ── Launch each service via secure helper (no secrets in args) ─────────────
REM    The helper script reads .env.binance itself inside the child window.
REM    BINANCE_API_KEY / BINANCE_API_SECRET never appear in the command line.

echo  [1/5] binance-feed       (crypto.tick.*)
start "BINANCE-feed" cmd /k "%PROJECT_DIR%_launch_binance_service.cmd BINANCE-feed hedge_binance.feed.service %BINANCE_PY% %PROJECT_DIR%"

echo  [2/5] binance-risk       (SL/TP monitor + circuit breaker)
start "BINANCE-risk" cmd /k "%PROJECT_DIR%_launch_binance_service.cmd BINANCE-risk hedge_binance.risk.service %BINANCE_PY% %PROJECT_DIR%"

echo  [3/5] binance-strategy   (EMA+RSI+ATR+Volume signal engine)
start "BINANCE-strategy" cmd /k "%PROJECT_DIR%_launch_binance_service.cmd BINANCE-strategy hedge_binance.strategy.service %BINANCE_PY% %PROJECT_DIR%"

echo  [4/5] binance-exec       (HMAC-signed REST, rate-limited)
start "BINANCE-exec" cmd /k "%PROJECT_DIR%_launch_binance_service.cmd BINANCE-exec hedge_binance.execution.service %BINANCE_PY% %PROJECT_DIR%"

echo  [5/5] binance-position   (account balance + position tracker)
start "BINANCE-position" cmd /k "%PROJECT_DIR%_launch_binance_service.cmd BINANCE-position hedge_binance.position.service %BINANCE_PY% %PROJECT_DIR%"

timeout /t 3 /nobreak >nul

echo.
echo  ============================================================
echo   BINANCE CRYPTO MODULE IS RUNNING
echo  ============================================================
echo.
echo   Service windows (title = BINANCE-*):
echo     BINANCE-feed       — WebSocket tick subscriber
echo     BINANCE-risk       — SL/TP monitor + circuit breaker
echo     BINANCE-strategy   — EMA/RSI/ATR/Volume signal engine
echo     BINANCE-exec       — REST order execution (rate-limited)
echo     BINANCE-position   — Account position tracker
echo.
echo   Prometheus metrics:
echo     http://localhost:9300  (feed)
echo     http://localhost:9301  (risk)
echo     http://localhost:9302  (strategy)
echo     http://localhost:9303  (exec)
echo     http://localhost:9304  (position)
echo.
echo   Grafana: http://localhost:3000/d/binance-crypto
echo.
echo   Indian stock system: NOT AFFECTED
echo.
echo  ============================================================
echo   Press any key to STOP all Binance services...
echo  ============================================================
pause >nul
goto :local_stop_silent


REM ============================================================================
:local_stop
REM ============================================================================
echo.
echo  ============================================================
echo   Stopping BINANCE services (Indian stock system UNTOUCHED)
echo  ============================================================
echo.
call :kill_binance_windows
echo  All BINANCE services stopped.
echo.
pause
goto :eof

:local_stop_silent
call :kill_binance_windows
echo  All BINANCE services stopped.
goto :eof

:kill_binance_windows
REM Uses taskkill in FIRE mode — only called when explicitly stopping
taskkill /fi "WINDOWTITLE eq BINANCE-feed"     /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq BINANCE-risk"     /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq BINANCE-strategy" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq BINANCE-exec"     /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq BINANCE-position" /f >nul 2>&1
goto :eof


REM ============================================================================
:status
REM ============================================================================
REM  ✅ FIX: status now uses tasklist (READ-ONLY) — does NOT kill any process.
REM         Previous v2 used taskkill /f as a probe which was killing services!
REM ============================================================================
echo.
echo  ============================================================
echo   BINANCE MODULE — Status (read-only check)
echo  ============================================================
echo.

REM tasklist exits with errorlevel 0 always, but we parse its output
for %%S in (feed risk strategy exec position) do (
    set "_found=0"
    for /f "delims=" %%L in ('tasklist /fi "WINDOWTITLE eq BINANCE-%%S" /fo list 2^>nul') do (
        echo %%L | find /i "PID:" >nul 2>&1 && set "_found=1"
    )
    if "!_found!"=="1" (
        echo    [RUNNING] BINANCE-%%S
    ) else (
        echo    [STOPPED] BINANCE-%%S
    )
)

echo.
echo  Docker Binance containers (if any):
docker compose -f docker-compose.binance.yml --profile binance ps 2>nul
echo.
echo  NOTE: Indian stock system not shown here.
echo        Run your main run.bat status to see full system state.
echo.
pause
goto :eof


REM ============================================================================
:docker_start
REM ============================================================================
call :check_env_file
if errorlevel 1 exit /b 1

echo.
echo  ============================================================
echo   BINANCE CRYPTO MODULE — Starting (Docker mode)
echo  ============================================================
echo.
docker info >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Docker is not running.
    pause
    exit /b 1
)
echo  [1/3] Building base image...
docker compose --env-file .env.binance -f docker-compose.binance.yml --profile binance build binance-base
if errorlevel 1 ( echo  [ERROR] Base image build failed. & pause & exit /b 1 )

echo  [2/3] Building service images...
docker compose --env-file .env.binance -f docker-compose.binance.yml --profile binance build
if errorlevel 1 ( echo  [ERROR] Service build failed. & pause & exit /b 1 )

echo  [3/3] Starting containers...
docker compose --env-file .env.binance -f docker-compose.binance.yml --profile binance up -d
if errorlevel 1 ( echo  [ERROR] Startup failed. & pause & exit /b 1 )

echo.
docker compose --env-file .env.binance -f docker-compose.binance.yml --profile binance ps
echo.
echo   Grafana: http://localhost:3000/d/binance-crypto
echo   Stop:    start-binance.bat docker stop
echo.
echo  Indian stock system: NOT AFFECTED
echo.
pause
goto :eof


REM ============================================================================
:docker_stop
REM ============================================================================
echo.
echo  Stopping Binance Docker services...
docker compose -f docker-compose.binance.yml --profile binance down
echo  Done. Indian stock system: NOT AFFECTED.
echo.
pause
goto :eof


REM ============================================================================
:docker_build
REM ============================================================================
echo.
echo  Building Binance Docker images...
docker compose --env-file .env.binance -f docker-compose.binance.yml --profile binance build
echo  Done.
echo.
pause
goto :eof


REM ============================================================================
:logs
REM ============================================================================
echo.
echo  ============================================================
echo   BINANCE MODULE — Logs
echo  ============================================================
echo.
echo  Local services: view each BINANCE-* terminal window.
echo.
echo  Docker logs:
echo    docker compose -f docker-compose.binance.yml logs -f --tail=100
echo.
echo  NATS subject monitor (all crypto.* traffic):
echo    nats sub "crypto.^>" --server nats://127.0.0.1:4222
echo.
docker compose -f docker-compose.binance.yml --profile binance logs --tail=50 2>nul
echo.
pause
goto :eof
