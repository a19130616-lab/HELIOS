@echo off
REM Helios Trading System - One-Click Setup Script (Windows)
REM This script installs all dependencies and sets up the environment

setlocal enabledelayedexpansion

echo 🚀 Helios Trading System - One-Click Setup (Windows)
echo ====================================================

REM Check if Python is installed
echo [INFO] Checking Python installation...
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python is not installed or not in PATH
    echo Please install Python 3.9+ from https://www.python.org/downloads/
    echo Make sure to check "Add Python to PATH"
    pause
    exit /b 1
)

for /f "tokens=2" %%i in ('python --version') do set PYTHON_VERSION=%%i
echo [SUCCESS] Python found: !PYTHON_VERSION!

REM Check if pip is installed
echo [INFO] Checking pip installation...
pip --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] pip is not installed
    echo Installing pip...
    python -m ensurepip --upgrade
    if %errorlevel% neq 0 (
        echo [ERROR] Failed to install pip
        pause
        exit /b 1
    )
)
echo [SUCCESS] pip found

REM Create virtual environment
echo [INFO] Creating virtual environment...
if not exist "helios-env" (
    python -m venv helios-env
    echo [SUCCESS] Virtual environment created: helios-env
) else (
    echo [WARNING] Virtual environment already exists
)

REM Activate virtual environment
echo [INFO] Activating virtual environment...
call helios-env\Scripts\activate.bat
if %errorlevel% neq 0 (
    echo [ERROR] Failed to activate virtual environment
    pause
    exit /b 1
)
echo [SUCCESS] Virtual environment activated

REM Upgrade pip
echo [INFO] Upgrading pip...
python -m pip install --upgrade pip

REM Install dependencies
echo [INFO] Installing Python dependencies from requirements.txt...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo [ERROR] Failed to install dependencies
    pause
    exit /b 1
)
echo [SUCCESS] All Python dependencies installed

REM Check Redis installation (Windows)
echo [INFO] Checking Redis installation...
redis-server --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [WARNING] Redis server not found
    echo [INFO] For Windows, you have several options:
    echo 1. Install Redis using WSL (Windows Subsystem for Linux)
    echo 2. Use Docker: docker run -d -p 6379:6379 redis:alpine
    echo 3. Download Redis for Windows from: https://github.com/microsoftarchive/redis/releases
    echo.
    echo [INFO] Starting with Docker Redis (if Docker is available)...
    docker --version >nul 2>&1
    if %errorlevel% equ 0 (
        echo [INFO] Docker found, starting Redis container...
        docker run -d -p 6379:6379 --name helios-redis redis:alpine >nul 2>&1
        if %errorlevel% equ 0 (
            echo [SUCCESS] Redis started in Docker container
        ) else (
            echo [WARNING] Could not start Redis in Docker (container may already exist)
            docker start helios-redis >nul 2>&1
            if %errorlevel% equ 0 (
                echo [SUCCESS] Existing Redis container started
            ) else (
                echo [WARNING] Please install Redis manually or use WSL
            fi
        )
    ) else (
        echo [WARNING] Docker not found. Please install Redis manually.
        echo See README.md for Redis installation instructions.
    )
) else (
    echo [SUCCESS] Redis server found
)

REM Create necessary directories
echo [INFO] Creating necessary directories...
if not exist "logs" mkdir logs
if not exist "models" mkdir models
echo [SUCCESS] Directories created

REM Run basic test
echo [INFO] Running basic functionality test...
python -c "
import sys
sys.path.insert(0, '.')
try:
    from app.config_manager import ConfigManager
    from app.models import TradingSignal, SignalDirection
    from app.utils import calculate_nobi, kelly_criterion
    print('✅ Core imports successful')
    
    # Test NOBI calculation
    bids = [(100.0, 1.0), (99.9, 2.0), (99.8, 1.5)]
    asks = [(100.1, 1.2), (100.2, 2.5), (100.3, 1.0)]
    nobi = calculate_nobi(bids, asks, 3)
    print(f'✅ NOBI calculation works: {nobi:.4f}')
    
    # Test Kelly criterion
    kelly = kelly_criterion(0.6, 0.02, 0.01)
    print(f'✅ Kelly criterion works: {kelly:.4f}')
    
    print('✅ Basic functionality test passed!')
    
except Exception as e:
    print(f'❌ Test error: {e}')
    exit(1)
"

if %errorlevel% neq 0 (
    echo [ERROR] Basic functionality test failed
    pause
    exit /b 1
)

echo [SUCCESS] Basic functionality test passed

REM Display setup summary
echo.
echo 🎉 Setup Complete!
echo ==================
echo.
echo [SUCCESS] Helios Trading System is ready to use!
echo.
echo 📋 Next Steps:
echo 1. Configure your API keys in config\config.ini
echo 2. Set testnet = True for safe testing
echo 3. Run the system: python main.py
echo.
echo 📁 Important Files:
echo    • config\config.ini - Configuration settings
echo    • main.py - Main application
echo    • logs\ - Log files will be stored here
echo    • models\ - Place ML models here
echo.
echo 🔧 Virtual Environment:
echo    • Activate: helios-env\Scripts\activate.bat
echo    • Deactivate: deactivate
echo.
echo [WARNING] IMPORTANT: Always test with testnet before live trading!
echo.
echo Press any key to exit...
pause >nul