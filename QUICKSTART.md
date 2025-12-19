# 🚀 Helios Trading System - Quick Start Guide

Get up and running with the Helios High-Frequency Trading System in just a few commands!

## One-Click Installation

### For Linux/macOS:
```bash
./setup.sh
```

### For Windows:
```batch
setup.bat
```

## What the Setup Script Does

✅ **Checks Python 3.9+ installation**  
✅ **Creates isolated virtual environment**  
✅ **Installs all required dependencies**  
✅ **Sets up Redis server**  
✅ **Creates necessary directories**  
✅ **Runs functionality tests**  
✅ **Provides next steps**  

## After Installation

### 1. Configure API Keys
Edit `config/config.ini` and add your Binance credentials:
```ini
[binance]
api_key = your_actual_api_key_here
api_secret = your_actual_api_secret_here
testnet = True  # IMPORTANT: Start with testnet!
```

### 2. Start Trading
```bash
# Activate virtual environment (if not already active)
source helios-env/bin/activate  # Linux/macOS
# or
helios-env\Scripts\activate.bat  # Windows

# Run the system
python main.py
```

## Quick Verification

Test if everything is working:
```bash
python -c "from app.utils import calculate_nobi; print('✅ Helios is ready!')"
```

## Safety First! 🛡️

- **Always start with `testnet = True`**
- **Never risk more than you can afford to lose**
- **Monitor the system closely during initial runs**
- **Check logs in the `logs/` directory**

## Need Help?

- Check the full `README.md` for detailed documentation
- Review configuration options in `config/config.ini`
- Monitor system logs in `logs/helios_*.log`
- Ensure Redis is running: `redis-cli ping` should return "PONG"

---

**Ready to trade? Run `python main.py` and watch the markets! 📈**