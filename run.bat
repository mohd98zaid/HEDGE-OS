@echo off
REM ============================================================================
REM  PROJECT HEDGE — One-Click Full System Launcher
REM ============================================================================
REM
REM  This script brings up the entire PROJECT HEDGE trading system:
REM    1. Infrastructure (NATS, Redis, Postgres+Timescale, Qdrant, Prometheus,
REM       Loki, Jaeger, Grafana)
REM    2. Hot_Path Rust services (market-data, orderflow, features, signals,
REM       risk, exec, position, supervisor, session, replay, ui-gateway)
REM    3. Warm_AI_Pipeline Python services (news, regime, priority, prevday,
REM       psych, rank, journal, governance, shadow, rag)
REM    4. React Human_Control_UI (dev server on http://localhost:5173)
REM
REM  Prerequisites:
REM    - Docker Desktop running with Docker Compose v2
REM    - Node.js 18+ and npm (for the UI dev server)
REM    - Ollama running locally on port 11434 (for AI inference)
REM
REM  Usage:
REM    run.bat              — Start everything (default)
REM    run.bat infra        — Start only infrastructure
REM    run.bat hot          — Start infrastructure + Hot_Path
REM    run.bat warm         — Start infrastructure + Warm_AI_Pipeline
REM    run.bat stop         — Stop all services
REM    run.bat logs         — Tail logs from all services
REM    run.bat status       — Show running containers
REM    run.bat build        — Build all Docker images without starting
REM    run.bat ui           — Start only the React UI dev server
REM
REM  Dashboards:
REM    Grafana:     http://localhost:3000  (admin / hedge)
REM    Jaeger:      http://localhost:16686
REM    Prometheus:  http://localhost:9090
REM    NATS:        http://localhost:8222
REM    UI Gateway:  ws://localhost:8080
REM    Cockpit UI:  http://localhost:5173
REM
REM ============================================================================

setlocal enabledelayedexpansion

set "COMPOSE_FILE=docker-compose.yml"
set "PROJECT_DIR=%~dp0"
cd /d "%PROJECT_DIR%"

REM --- Parse command ---
set "CMD=%~1"
if "%CMD%"=="" set "CMD=full"

REM --- Route to handler ---
if /i "%CMD%"=="full"   goto :start_full
if /i "%CMD%"=="infra"  goto :start_infra
if /i "%CMD%"=="hot"    goto :start_hot
if /i "%CMD%"=="warm"   goto :start_warm
if /i "%CMD%"=="stop"   goto :stop
if /i "%CMD%"=="logs"   goto :logs
if /i "%CMD%"=="status" goto :status
if /i "%CMD%"=="build"  goto :build
if /i "%CMD%"=="ui"     goto :start_ui

echo.
echo  Unknown command: %CMD%
echo  Usage: run.bat [full^|infra^|hot^|warm^|stop^|logs^|status^|build^|ui]
echo.
exit /b 1

REM ============================================================================
:start_full
REM ============================================================================
echo.
echo  ============================================================
echo   PROJECT HEDGE - Starting Full System
echo  ============================================================
echo.

REM Check Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Docker is not running. Please start Docker Desktop.
    pause
    exit /b 1
)

echo  [1/5] Checking for base image updates...
docker compose --profile full pull --ignore-pull-failures nats redis postgres qdrant prometheus loki jaeger grafana 2>nul

echo.
echo  [2/5] Building service images (this may take a while on first run)...
echo  [   ] Building warm-ai-base image first (required by all Warm_AI services)...
docker compose --profile full build warm-ai-base
echo  [   ] Building all remaining service images...
docker compose --profile full build

echo.
echo  [3/5] Starting infrastructure (NATS, Redis, Postgres, Qdrant, Prometheus, Loki, Jaeger, Grafana)...
docker compose --profile full up -d nats redis postgres qdrant prometheus loki jaeger grafana

REM Wait for infra to be healthy
echo  [   ] Waiting for infrastructure to initialize (15s)...
timeout /t 15 /nobreak >nul

echo.
echo  [4/5] Starting Hot_Path + Warm_AI_Pipeline services...
docker compose --profile full up -d

echo.
echo  [5/5] Starting React UI dev server...
cd ui
if not exist node_modules (
    echo  [   ] Installing UI dependencies...
    call npm install
)
start "HEDGE-UI" cmd /c "npm run dev"
cd ..

echo.
echo  ============================================================
echo   PROJECT HEDGE is running!
echo  ============================================================
echo.
echo   Cockpit UI:    http://localhost:5173
echo   UI Gateway:    ws://localhost:8080
echo   Grafana:       http://localhost:3000  (admin / hedge)
echo   Jaeger:        http://localhost:16686
echo   Prometheus:    http://localhost:9090
echo   NATS Monitor:  http://localhost:8222
echo.
echo   To view logs:  run.bat logs
echo   To stop:       run.bat stop
echo.
echo   Press any key to stop all services and exit...
echo.
pause >nul
call :stop_silent
goto :eof

REM ============================================================================
:start_infra
REM ============================================================================
echo.
echo  Starting infrastructure only...
docker compose --profile infra up -d
echo.
echo  Infrastructure running:
echo    NATS:        nats://localhost:4222 (monitor: http://localhost:8222)
echo    Redis:       redis://localhost:6379
echo    Postgres:    postgresql://localhost:5432/hedge
echo    Qdrant:      http://localhost:6333
echo    Prometheus:  http://localhost:9090
echo    Loki:        http://localhost:3100
echo    Jaeger:      http://localhost:16686
echo    Grafana:     http://localhost:3000
echo.
echo  Press any key to stop and exit...
pause >nul
docker compose --profile infra down
goto :eof

REM ============================================================================
:start_hot
REM ============================================================================
echo.
echo  Starting infrastructure + Hot_Path services...
docker compose --profile infra up -d
timeout /t 10 /nobreak >nul
docker compose --profile hot_path up -d
echo.
echo  Hot_Path services running. UI Gateway at ws://localhost:8080
echo.
echo  Press any key to stop and exit...
pause >nul
docker compose --profile hot_path down
docker compose --profile infra down
goto :eof

REM ============================================================================
:start_warm
REM ============================================================================
echo.
echo  Starting infrastructure + Warm_AI_Pipeline services...
docker compose --profile infra up -d
timeout /t 10 /nobreak >nul
docker compose --profile warm_ai up -d
echo.
echo  Warm_AI_Pipeline services running.
echo  Note: Ollama must be running locally on port 11434.
echo.
echo  Press any key to stop and exit...
pause >nul
docker compose --profile warm_ai down
docker compose --profile infra down
goto :eof

REM ============================================================================
:stop
REM ============================================================================
echo.
echo  Stopping all PROJECT HEDGE services...
docker compose --profile full down
echo.
echo  Stopping UI dev server (if running)...
taskkill /fi "WINDOWTITLE eq HEDGE-UI*" /f >nul 2>&1
echo.
echo  All services stopped.
echo.
pause
goto :eof

REM ============================================================================
:stop_silent
REM ============================================================================
echo.
echo  Stopping all PROJECT HEDGE services...
docker compose --profile full down
echo.
echo  Stopping UI dev server (if running)...
taskkill /fi "WINDOWTITLE eq HEDGE-UI*" /f >nul 2>&1
echo.
echo  All services stopped.
echo.
goto :eof

REM ============================================================================
:logs
REM ============================================================================
echo.
echo  Tailing logs (Ctrl+C to stop)...
echo.
docker compose --profile full logs -f --tail=100
goto :eof

REM ============================================================================
:status
REM ============================================================================
echo.
echo  PROJECT HEDGE — Container Status
echo  =================================
echo.
docker compose --profile full ps
echo.
pause
goto :eof

REM ============================================================================
:build
REM ============================================================================
echo.
echo  Building all Docker images...
echo  [1/2] Building warm-ai-base (dependency for all Warm_AI services)...
docker compose --profile full build warm-ai-base
echo  [2/2] Building all remaining images...
docker compose --profile full build
echo.
echo  Build complete.
echo.
pause
goto :eof

REM ============================================================================
:start_ui
REM ============================================================================
echo.
echo  Starting React UI dev server only...
cd ui
if not exist node_modules (
    echo  Installing UI dependencies...
    call npm install
)
echo.
echo  UI running at http://localhost:5173
echo  Press Ctrl+C to stop.
echo.
call npm run dev
cd ..
goto :eof
