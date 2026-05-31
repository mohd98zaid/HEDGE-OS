@echo off
REM ============================================================================
REM  PROJECT HEDGE - REPLAY data launcher
REM ============================================================================
REM
REM  Double-click this to run the cockpit on HISTORICAL (replay) data.
REM
REM  Starts NATS + UI Gateway + Cockpit UI + hedge-replay player.
REM  The Hot_Path engines and Warm_AI services are intentionally NOT started,
REM  so nothing real can leak in and replay owns every subject. Ideal for
REM  checking/verifying the system with old data.
REM ============================================================================

set "HEDGE_MODE=replay"
set "HEDGE_REPLAY_SESSION=%1"

if "%HEDGE_REPLAY_SESSION%"=="" (
    echo.
    echo -------------------------------------------------------------
    echo No SESSION_ID provided!
    echo Usage: start-replay.bat ^<SESSION_ID^>
    echo.
    echo If you don't have any recorded sessions, you can import
    echo historical data from Upstox by running:
    echo cargo run -p hedge-demo-synth --bin hedge-upstox-import -- ^<YYYY-MM-DD^> ^<SYMBOL^>
    echo Example: cargo run -p hedge-demo-synth --bin hedge-upstox-import -- 2026-04-25 "NSE_EQ|INE002A01018"
    echo -------------------------------------------------------------
    echo.
    echo Available sessions:
    cargo run --release -p hedge-replay --bin hedge-replay -- list
    echo.
    echo Please run this script from the command line with a session ID.
    pause
    exit /b 1
)

call "%~dp0start.bat" %*
