#!/bin/bash

# Helios Trading System - One-Click Setup Script
# This script installs all dependencies and sets up the environment

set -e  # Exit on any error

echo "🚀 Helios Trading System - One-Click Setup"
echo "==========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Python 3 is installed
print_status "Checking Python installation..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
    print_success "Python 3 found: $PYTHON_VERSION"
    PYTHON_CMD="python3"
elif command -v python &> /dev/null; then
    PYTHON_VERSION=$(python --version | cut -d' ' -f2)
    if [[ $PYTHON_VERSION == 3.* ]]; then
        print_success "Python 3 found: $PYTHON_VERSION"
        PYTHON_CMD="python"
    else
        print_error "Python 3 is required but Python $PYTHON_VERSION found"
        exit 1
    fi
else
    print_error "Python 3 is not installed. Please install Python 3.9+ first."
    echo "Visit: https://www.python.org/downloads/"
    exit 1
fi

# Check if pip is installed
print_status "Checking pip installation..."
if command -v pip3 &> /dev/null; then
    PIP_CMD="pip3"
elif command -v pip &> /dev/null; then
    PIP_CMD="pip"
else
    print_error "pip is not installed. Installing pip..."
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    $PYTHON_CMD get-pip.py
    rm get-pip.py
    PIP_CMD="pip"
fi

print_success "pip found"

# Create virtual environment
print_status "Creating virtual environment..."
if [ ! -d "helios-env" ]; then
    $PYTHON_CMD -m venv helios-env
    print_success "Virtual environment created: helios-env"
else
    print_warning "Virtual environment already exists"
fi

# Activate virtual environment
print_status "Activating virtual environment..."
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    # Windows (Git Bash/MSYS)
    source helios-env/Scripts/activate
else
    # Linux/macOS
    source helios-env/bin/activate
fi

print_success "Virtual environment activated"

# Upgrade pip
print_status "Upgrading pip..."
$PIP_CMD install --upgrade pip

# Install dependencies
print_status "Installing Python dependencies from requirements.txt..."
$PIP_CMD install -r requirements.txt

print_success "All Python dependencies installed"

# Check Redis installation
print_status "Checking Redis installation..."
if command -v redis-server &> /dev/null; then
    print_success "Redis server found"
    
    # Check if Redis is running
    if pgrep redis-server > /dev/null; then
        print_success "Redis server is already running"
    else
        print_warning "Redis server is installed but not running"
        echo "Starting Redis server..."
        
        # Try to start Redis
        if command -v brew &> /dev/null && [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS with Homebrew
            brew services start redis
        elif command -v systemctl &> /dev/null; then
            # Linux with systemd
            sudo systemctl start redis
        else
            # Manual start
            redis-server --daemonize yes
        fi
        
        if pgrep redis-server > /dev/null; then
            print_success "Redis server started"
        else
            print_warning "Could not start Redis automatically. Please start it manually."
        fi
    fi
else
    print_warning "Redis server not found. Installing Redis..."
    
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v brew &> /dev/null; then
            brew install redis
            brew services start redis
            print_success "Redis installed and started via Homebrew"
        else
            print_error "Homebrew not found. Please install Redis manually:"
            echo "Visit: https://redis.io/download"
        fi
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux
        if command -v apt-get &> /dev/null; then
            # Ubuntu/Debian
            sudo apt-get update
            sudo apt-get install -y redis-server
            sudo systemctl start redis-server
            sudo systemctl enable redis-server
            print_success "Redis installed and started via apt"
        elif command -v yum &> /dev/null; then
            # CentOS/RHEL
            sudo yum install -y redis
            sudo systemctl start redis
            sudo systemctl enable redis
            print_success "Redis installed and started via yum"
        else
            print_warning "Please install Redis manually for your Linux distribution"
            echo "Visit: https://redis.io/download"
        fi
    else
        print_warning "Please install Redis manually for your operating system"
        echo "Visit: https://redis.io/download"
    fi
fi

# Test Redis connection
print_status "Testing Redis connection..."
if command -v redis-cli &> /dev/null; then
    if redis-cli ping | grep -q "PONG"; then
        print_success "Redis connection test passed"
    else
        print_warning "Redis connection test failed. Please check Redis installation."
    fi
else
    print_warning "Redis CLI not found. Please verify Redis installation."
fi

# Create necessary directories
print_status "Creating necessary directories..."
mkdir -p logs models
print_success "Directories created"

# Set permissions
print_status "Setting file permissions..."
chmod +x main.py
print_success "File permissions set"

# Run basic test
print_status "Running basic functionality test..."
if $PYTHON_CMD -c "
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
"; then
    print_success "Basic functionality test passed"
else
    print_error "Basic functionality test failed"
    exit 1
fi

# Display setup summary
echo ""
echo "🎉 Setup Complete!"
echo "=================="
echo ""
print_success "Helios Trading System is ready to use!"
echo ""
echo "📋 Next Steps:"
echo "1. Configure your API keys in config/config.ini"
echo "2. Set testnet = True for safe testing"
echo "3. Run the system: $PYTHON_CMD main.py"
echo ""
echo "📁 Important Files:"
echo "   • config/config.ini - Configuration settings"
echo "   • main.py - Main application"
echo "   • logs/ - Log files will be stored here"
echo "   • models/ - Place ML models here"
echo ""
echo "🔧 Virtual Environment:"
echo "   • Activate: source helios-env/bin/activate"
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    echo "   • (Windows: source helios-env/Scripts/activate)"
fi
echo "   • Deactivate: deactivate"
echo ""
print_warning "IMPORTANT: Always test with testnet before live trading!"
echo ""