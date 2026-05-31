@echo off
REM ============================================================================
REM  PROJECT HEDGE - Ordered Startup (mode-aware)
REM ============================================================================
REM
REM  This is the shared engine launched by:
REM    * start-real.bat       -> HEDGE_MODE=real      (live / real data)
REM    * start-synthetic.bat  -> HEDGE_MODE=synthetic (demo / synthetic data)
REM
REM  You can also run it directly:
REM    start.bat              -> defaults to HEDGE_MODE=real
REM    set HEDGE_MODE=synthetic && start.bat
REM
REM  MODE = real
REM    Full Hot_Path pipeline (Upstox feed -> orderflow -> features ->
REM    signals -> risk -> exec -> position) + Warm_AI services. Demo synth
REM    is OFF, so every panel shows REAL data. Needs HEDGE_UPSTOX_ACCESS_TOKEN
REM    and (for live ticks) market hours; panels with no live producer show
REM    "Awaiting...".
REM
REM  MODE = synthetic
REM    Only NATS + Demo Synth + UI Gateway + Cockpit UI. The deterministic
REM    synthetic publisher fills EVERY panel within ~10s. No broker token and
REM    no market hours required. The Hot_Path engines and Warm_AI services are
REM    NOT started, so nothing real can leak in and synth owns every subject.
REM
REM  Service windows are kept OPEN on exit so you can see error messages.
REM  Press any key in this window to stop everything.
REM ============================================================================

setlocal enabledelayedexpansion
set "PROJECT_DIR=%~dp0"
cd /d "%PROJECT_DIR%"

REM --- Resolve mode (default real). Launchers set HEDGE_MODE before calling. ---
if not defined HEDGE_MODE set "HEDGE_MODE=real"
if /i "%HEDGE_MODE%"=="synthetic" (
    set "HEDGE_MODE=synthetic"
) else if /i "%HEDGE_MODE%"=="synth" (
    set "HEDGE_MODE=synthetic"
) else if /i "%HEDGE_MODE%"=="demo" (
    set "HEDGE_MODE=synthetic"
) else if /i "%HEDGE_MODE%"=="replay" (
    set "HEDGE_MODE=replay"
) else (
    set "HEDGE_MODE=real"
)

if /i "%HEDGE_MODE%"=="synthetic" (
    set "RUN_REAL_PIPELINE=0"
) else if /i "%HEDGE_MODE%"=="replay" (
    set "RUN_REAL_PIPELINE=1"
) else (
    set "RUN_REAL_PIPELINE=1"
)

echo.
echo  ============================================================
echo   PROJECT HEDGE - Ordered Startup
echo   MODE: %HEDGE_MODE%
if /i "%HEDGE_MODE%"=="synthetic" (
    echo   ^> Dashboard will be filled with SYNTHETIC data.
) else if /i "%HEDGE_MODE%"=="replay" (
    echo   ^> Dashboard will be filled with HISTORICAL REPLAY data.
) else (
    echo   ^> Dashboard will show REAL data ^(needs token / market hours^).
)
echo  ============================================================
echo.

REM --- Pre-flight checks ---
docker info >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Docker is not running. Please start Docker Desktop.
    pause
    exit /b 1
)

REM --- Build checks (mode-specific) ---
if "%RUN_REAL_PIPELINE%"=="1" (
    if not exist "target\release\hedge-session.exe" (
        echo  [ERROR] Binaries not found. Building workspace now...
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
)

REM Both modes need the UI gateway; synthetic mode also needs demo-synth.
if not exist "target\release\hedge-ui-gateway.exe" (
    echo  [INFO] Building hedge-ui-gateway...
    cargo build --release -p hedge-ui-gateway --bin hedge-ui-gateway
    if errorlevel 1 (
        echo  [ERROR] Build of hedge-ui-gateway failed.
        pause
        exit /b 1
    )
)
if not exist "target\release\hedge-demo-synth.exe" (
    echo  [INFO] Building hedge-demo-synth...
    cargo build --release -p hedge-demo-synth --bin hedge-demo-synth
    if errorlevel 1 (
        echo  [ERROR] Build of hedge-demo-synth failed.
        pause
        exit /b 1
    )
)
if not exist "target\release\hedge-replay.exe" (
    echo  [INFO] Building hedge-replay...
    cargo build --release -p hedge-replay --bin hedge-replay
    if errorlevel 1 (
        echo  [ERROR] Build of hedge-replay failed.
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

REM --- Derive feature flags from HEDGE_MODE (AUTHORITATIVE: overrides .env) ---
REM  This MUST run after the .env load so the chosen mode always wins over any
REM  HEDGE_DEMO_SYNTH / HEDGE_WARM_AI value that happens to be in .env.
if /i "%HEDGE_MODE%"=="synthetic" (
    set "HEDGE_DEMO_SYNTH=on"
    set "HEDGE_WARM_AI=off"
) else if /i "%HEDGE_MODE%"=="replay" (
    set "HEDGE_DEMO_SYNTH=off"
    set "HEDGE_WARM_AI=off"
) else (
    set "HEDGE_DEMO_SYNTH=off"
    set "HEDGE_WARM_AI=on"
)

REM --- Check Upstox token presence (real mode only) ---
if "%RUN_REAL_PIPELINE%"=="1" (
    if not defined HEDGE_UPSTOX_ACCESS_TOKEN (
        echo  [WARNING] HEDGE_UPSTOX_ACCESS_TOKEN is not set.
        echo            The upstox-feed window will exit immediately and the
        echo            market panels will stay empty. Refresh your token in
        echo            .env, or run start-synthetic.bat for a demo dashboard.
        echo.
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
if not defined HEDGE_UPSTOX_INSTRUMENTS set "HEDGE_UPSTOX_INSTRUMENTS=NSE_EQ|INE002A01018,NSE_EQ|INE009A01021,NSE_EQ|INE062A01020,NSE_EQ|INE040A01034,NSE_EQ|INE090A01021"

REM NATS URL (no auth in dev)
set "HEDGE_NATS_URL=nats://127.0.0.1:4222"

REM ============================================================
REM  STEP 1: Infrastructure (both modes need NATS)
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
REM  STEP 2 + 3 + 3.6: Real pipeline (real mode only)
REM ============================================================
if "%RUN_REAL_PIPELINE%"=="0" goto :skip_real_pipeline

echo  [2/5] Starting session controller + supervisor...
start "HEDGE-session" cmd /k target\release\hedge-session.exe
start "HEDGE-supervisor" cmd /k target\release\hedge-supervisor.exe
timeout /t 2 /nobreak >nul
echo        Session + Supervisor started.
echo.

echo  [3/5] Starting Hot_Path pipeline (in dependency order)...

echo        [a] Market Data Engine (Upstox REST poller)...
if /i "%HEDGE_MODE%" NEQ "replay" (
    start "HEDGE-market-data" cmd /k target\release\upstox-feed.exe
) else (
    echo            [Skipping in replay mode - data comes from hedge-replay]
)
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

REM hedge-replay is an inspector CLI, not a daemon.
REM   Usage: target\release\hedge-replay.exe list | info <id> | dump <id>
REM Do NOT launch it from start.bat.

echo        Hot_Path pipeline started.
echo.

REM --- Warm_AI_Pipeline (Python microservices) ---
if /i "%HEDGE_WARM_AI%"=="off" goto :skip_warm_ai
if /i "%HEDGE_WARM_AI%"=="false" goto :skip_warm_ai
if /i "%HEDGE_WARM_AI%"=="0" goto :skip_warm_ai

set "HEDGE_PY=python"
if exist "python\hedge_warm_ai\.venv\Scripts\python.exe" set "HEDGE_PY=python\hedge_warm_ai\.venv\Scripts\python.exe"
set "PYTHONPATH=%PROJECT_DIR%python\hedge_warm_ai\src;%PYTHONPATH%"

echo  [3.6] Starting Warm_AI_Pipeline (Python)...
echo        [j] AI Trade Ranking Engine (sig.emitted -^> ai.rank)...
start "HEDGE-rank" cmd /k "%HEDGE_PY%" -m hedge_warm_ai.ranking.service
timeout /t 1 /nobreak >nul

echo        [k] News Intelligence Engine (-^> ai.news.impact)...
start "HEDGE-news" cmd /k "%HEDGE_PY%" -m hedge_warm_ai.news.service
timeout /t 1 /nobreak >nul

echo        [l] Market Regime Engine (-^> ai.regime.changed + md.breadth)...
start "HEDGE-regime" cmd /k "%HEDGE_PY%" -m hedge_warm_ai.regime.service
timeout /t 1 /nobreak >nul

echo        [m] Trader Psychology Engine (-^> ai.psych.stability)...
start "HEDGE-psych" cmd /k "%HEDGE_PY%" -m hedge_warm_ai.psychology.service
timeout /t 1 /nobreak >nul
goto :warm_ai_done

:skip_warm_ai
echo  [3.6] Warm_AI_Pipeline skipped (HEDGE_WARM_AI=%HEDGE_WARM_AI%).

:warm_ai_done
echo.
goto :pipeline_done

:skip_real_pipeline
echo  [2/5] Real Hot_Path pipeline skipped (synthetic mode).
echo  [3/5] Upstox feed + engines skipped (synthetic mode).
echo.

:pipeline_done

REM ============================================================
REM  STEP 3.5: Demo Synth (synthetic dashboard filler)
REM ============================================================
REM  HEDGE_DEMO_SYNTH is derived from HEDGE_MODE above:
REM    synthetic -> on   (synth fills every panel)
REM    real      -> off  (real engines own every panel)
if /i "%HEDGE_DEMO_SYNTH%"=="off" goto :skip_demo_synth
if /i "%HEDGE_DEMO_SYNTH%"=="false" goto :skip_demo_synth
if /i "%HEDGE_DEMO_SYNTH%"=="0" goto :skip_demo_synth

echo  [3.5] Demo Synth (HEDGE_DEMO_SYNTH=on)...
start "HEDGE-demo-synth" cmd /k target\release\hedge-demo-synth.exe
timeout /t 1 /nobreak >nul
goto :demo_synth_done

:skip_demo_synth
echo  [3.5] Demo Synth skipped (HEDGE_DEMO_SYNTH=%HEDGE_DEMO_SYNTH%).

:demo_synth_done
echo.

REM ============================================================
REM  STEP 3.8: Replay (historical data) skipped here, moved to end.
REM ============================================================
:skip_replay
echo.


REM ============================================================
REM  STEP 4: UI Gateway (needs NATS + publishers)
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
echo   PROJECT HEDGE is running!  [MODE: %HEDGE_MODE%]
echo  ============================================================
echo.
if /i "%HEDGE_MODE%"=="synthetic" (
    echo   Synthetic mode: every panel is filled by the deterministic
    echo   Demo Synth publisher. No broker token or market hours needed.
    echo   Hot_Path engines and Warm_AI are intentionally NOT running.
) else if /i "%HEDGE_MODE%"=="replay" (
    echo   Replay mode: historical session %HEDGE_REPLAY_SESSION% is being
    echo   replayed. No live broker token or market hours needed.
    echo   Hot_Path engines and Warm_AI are intentionally NOT running.
) else (
    echo   Real mode: data flows through the live Hot_Path pipeline:
    echo     Market Data -^> Orderflow -^> Features -^> Signals
    echo     -^> Risk -^> Execution -^> Position   ^(+ Warm_AI services^)
    echo   Demo Synth is OFF. Panels with no live producer show "Awaiting...".
)
echo.
echo   Services:
echo     UI Gateway           : ws://localhost:8088
echo     Demo Synth           : %HEDGE_DEMO_SYNTH%
echo     Warm_AI Pipeline     : %HEDGE_WARM_AI%
if "%RUN_REAL_PIPELINE%"=="1" (
    echo     Session Controller   : running (09:15-15:30 IST gate)
    echo     Supervisor           : running (self-healing)
    echo     Upstox Feed          : REST polling, 500ms LTP / 2s book
)
echo     (Replay is an inspector CLI: hedge-replay.exe list ^| info ^| dump)
echo.
echo   Dashboards:
echo     Cockpit UI           : http://localhost:5173
echo     Grafana              : http://localhost:3000  (admin / hedge)
echo     NATS Monitor         : http://localhost:8222
echo     Jaeger Traces        : http://localhost:16686
echo     Prometheus           : http://localhost:9090
echo.
echo  ============================================================

if /i "%HEDGE_MODE%"=="replay" (
    if not "%HEDGE_REPLAY_SESSION%"=="" (
        echo.
        echo ======================================================================
        echo  READY FOR REPLAY
        echo  1. Open your dashboard at http://localhost:5173/
        echo  2. Wait until it has loaded and connected successfully
        echo  3. Press any key below to blast the historical data into the system
        echo ======================================================================
        pause
        echo  Starting Historical Replay for session %HEDGE_REPLAY_SESSION% at x10 speed...
        start "HEDGE-replay" cmd /k target\release\hedge-replay.exe play %HEDGE_REPLAY_SESSION% x10
    )
)

echo   Press any key to STOP all services and exit...
echo     UI Gateway           : ws://localhost:8088
echo     Demo Synth           : %HEDGE_DEMO_SYNTH%
echo     Warm_AI Pipeline     : %HEDGE_WARM_AI%
if "%RUN_REAL_PIPELINE%"=="1" (
    echo     Session Controller   : running (09:15-15:30 IST gate)
    echo     Supervisor           : running (self-healing)
    echo     Upstox Feed          : REST polling, 500ms LTP / 2s book
)
echo     (Replay is an inspector CLI: hedge-replay.exe list ^| info ^| dump)
echo.
echo   Dashboards:
echo     Cockpit UI           : http://localhost:5173
echo     Grafana              : http://localhost:3000  (admin / hedge)
echo     NATS Monitor         : http://localhost:8222
echo     Jaeger Traces        : http://localhost:16686
echo     Prometheus           : http://localhost:9090
echo.
echo  ============================================================
echo   Press any key to STOP all services and exit...
echo  ============================================================
pause >nul

REM ============================================================
REM  SHUTDOWN (reverse order). taskkill on absent windows is harmless.
REM ============================================================
echo.
echo  Stopping all services...

taskkill /fi "WINDOWTITLE eq HEDGE-UI*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-ui-gateway*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-psych*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-regime*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-news*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-rank*" /f >nul 2>&1
taskkill /fi "WINDOWTITLE eq HEDGE-demo-synth*" /f >nul 2>&1
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
