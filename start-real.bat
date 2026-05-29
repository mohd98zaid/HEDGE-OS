@echo off
REM ============================================================================
REM  PROJECT HEDGE - REAL data launcher
REM ============================================================================
REM
REM  Double-click this to run the cockpit on REAL data.
REM
REM  Starts the full Hot_Path pipeline (Upstox feed -> orderflow -> features ->
REM  signals -> risk -> exec -> position) plus the Warm_AI services. The Demo
REM  Synth is OFF, so every panel shows real data.
REM
REM  Requirements for live data:
REM    * HEDGE_UPSTOX_ACCESS_TOKEN set in .env (Upstox tokens expire daily).
REM    * Market open (09:15-15:30 IST) for live ticks; outside those hours the
REM      market panels show "Awaiting..." until the next session.
REM  Panels with no live producer (e.g. before any signal fires) stay empty.
REM ============================================================================

set "HEDGE_MODE=real"
call "%~dp0start.bat" %*
