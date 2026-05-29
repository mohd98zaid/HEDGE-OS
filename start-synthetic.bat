@echo off
REM ============================================================================
REM  PROJECT HEDGE - SYNTHETIC data launcher
REM ============================================================================
REM
REM  Double-click this to run the cockpit on SYNTHETIC (demo) data.
REM
REM  Starts only NATS + the Demo Synth publisher + UI Gateway + Cockpit UI.
REM  The deterministic synthetic publisher fills EVERY panel within ~10s.
REM
REM  No broker token and no market hours required. The Hot_Path engines and
REM  Warm_AI services are intentionally NOT started, so nothing real can leak
REM  in and synth owns every subject. Ideal for demos, UI work, and testing
REM  outside trading hours.
REM ============================================================================

set "HEDGE_MODE=synthetic"
call "%~dp0start.bat" %*
