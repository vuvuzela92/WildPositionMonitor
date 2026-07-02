@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0"

set "MODE=%~1"
if /I "%MODE%"=="" set "MODE=prod"

set "ARTICLES=979947681,964018818,918342224"
set "BUNDLES=01 02 03 04 05 06 07"
set "SMOKE_DIR=logs"
set "MIN_LIVE_BUNDLES=3"
set "LIVE_BUNDLES="
set "LIVE_BUNDLES_COUNT=0"

echo [1/3] Smoke check for proxy bundles
for %%I in (%BUNDLES%) do (
    echo ===== proxy_%%I =====
    for /f "delims=" %%L in ('python -c "from dotenv import dotenv_values; d=dotenv_values('.env'); i='%%I'; print('set WB_PROXY_URL=' + str(d.get(f'WB_PROXY_{i}_URL',''))); print('set WB_COOKIE=' + str(d.get(f'WB_PROXY_{i}_COOKIE',''))); print('set WB_DEVICE_ID=' + str(d.get(f'WB_PROXY_{i}_DEVICE_ID','')))"') do %%L
    set "WB_AUTHORIZATION="
    python scripts\poc_wb_internal_detail.py --articles %ARTICLES% --limit 3 --profiles K --endpoints u_card_v4 --verbose > "%SMOKE_DIR%\poc_bundle_%%I_u_card_smoke_fresh.txt"
)

echo.
echo [2/4] Smoke summaries
for %%I in (%BUNDLES%) do (
    echo ===== proxy_%%I =====
    type "%SMOKE_DIR%\poc_bundle_%%I_u_card_smoke_fresh.txt"
)

echo.
echo [3/4] Selecting live bundles
for %%I in (%BUNDLES%) do (
    findstr /C:"| K       | u_card_v4/detail_params | 3        | 3         | 0         | 0         |" "%SMOKE_DIR%\poc_bundle_%%I_u_card_smoke_fresh.txt" >nul
    if not errorlevel 1 (
        if defined LIVE_BUNDLES (
            set "LIVE_BUNDLES=!LIVE_BUNDLES!,%%I"
        ) else (
            set "LIVE_BUNDLES=%%I"
        )
        set /a LIVE_BUNDLES_COUNT+=1
        echo proxy_%%I = LIVE
    ) else (
        echo proxy_%%I = DEAD
    )
)

if not defined LIVE_BUNDLES (
    echo.
    echo Smoke failed: no live bundles found. Production wave cancelled.
    exit /b 1
)

echo Live bundles: !LIVE_BUNDLES!
echo Live bundle count: !LIVE_BUNDLES_COUNT!

if !LIVE_BUNDLES_COUNT! LSS %MIN_LIVE_BUNDLES% (
    echo.
    echo Smoke failed: live bundle count is below minimum %MIN_LIVE_BUNDLES%. Production wave cancelled.
    exit /b 1
)

if /I "%MODE%"=="smoke" (
    echo.
    echo Smoke finished successfully. Production wave skipped by mode=smoke.
    exit /b 0
)

echo.
echo [4/4] Starting production wave
set "WB_AUTHORIZATION="
set "WB_PROXY_URL="
set "WB_COOKIE="
set "WB_DEVICE_ID="
set "WB_DETAIL_ENDPOINT_MODE=u_card_v4"
set "WB_SKIP_SIMILAR_STAGE=True"
set "WB_ALLOW_MISSING_PRICE=True"
set "WB_ALLOW_MISSING_PRODUCT=True"
set "WB_COOKIE_ENABLED=True"
set "WB_PROXY_BUNDLES_ENABLED=True"
set "WB_SESSION_ROTATION_ENABLED=True"
set "WB_SESSION_ROTATE_EVERY=50"
set "WB_SESSION_ROTATION_SCOPE=detail"
set "CONCURRENT_REQUESTS_LIMIT=2"
set "WB_DETAIL_SUBMIT_DELAY=0.5"
set "CLICKHOUSE_WRITE_ENABLED=True"
set "WB_PROXY_BUNDLE_POOL=!LIVE_BUNDLES!"
set "WB_BATCH_FORBIDDEN_STOP_LOSS_ENABLED=True"
set "WB_BATCH_FORBIDDEN_STOP_LOSS_RATIO=0.35"
set "WB_BATCH_FORBIDDEN_STOP_LOSS_MIN_BATCH_SIZE=20"
set "WB_ALL_BUNDLES_498_COOLDOWN_ENABLED=True"
set "WB_ALL_BUNDLES_498_COOLDOWN_SECONDS=300"
set "CHECKPOINT_FILE=processing_checkpoint_full_fresh_7proxy.json"
set "WB_ROLLOUT_ARTICLES_LIMIT="

python -m src.main
