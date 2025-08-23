# Helios Trading System - Deployment Guide

## Overview

The Helios Trading System is now a comprehensive containerized cryptocurrency trading platform with the following enhanced capabilities:

### Key Features
- **Docker Containerization**: Production-ready multi-stage builds with health checks
- **Sentiment Analysis**: Reddit news ingestion with VADER sentiment analysis and crypto-specific lexicon
- **Multi-timeframe Analysis**: Technical indicators across multiple timeframes (1m, 5m, 15m, 1h, 4h, 1d)
- **Enhanced Decision Logic**: ML-powered signal generation with sentiment integration
- **Performance Tracking**: Real-time profitability and trade analysis
- **Health Monitoring**: Web dashboard for system status and metrics
- **Advanced Risk Management**: Dynamic position sizing with sentiment-adjusted Kelly criterion

## Operating Modes

Helios supports multiple operating modes with explicit environment gating:

| Mode | Trigger | Data Source | Trading | Component Path |
|------|--------|-------------|---------|----------------|
| live (testnet/mainnet) | Valid Binance API key & secret | WebSocket (authenticated futures streams) | Enabled (ExecutionManager) | DataIngestor + ExecutionManager |
| public | Missing/placeholder credentials AND ALLOW_PUBLIC_MODE=true | REST polling (Binance public + CoinGecko fallback) | Disabled | PublicPriceIngestor (read-only) |
| synthetic (legacy) | ALLOW_SYNTHETIC=true (explicit) when no valid credentials | Internal simulated generators | Disabled | DataIngestor synthetic threads (gated) |

Environment Flags:
- ALLOW_PUBLIC_MODE=true (default) enables graceful downgrade to read-only public mode without trading.
- ALLOW_SYNTHETIC=false (default). Turn on ONLY for local testing when you want simulated market flow.

Mode Resolution Rules (priority):
1. Valid credentials ⇒ live
2. No credentials & ALLOW_PUBLIC_MODE=true ⇒ public
3. Else if ALLOW_SYNTHETIC=true ⇒ synthetic
4. Else startup aborts (require credentials or enable public/synthetic)

Dashboard Mode Badge:
- Displays LIVE / PUBLIC / SYNTHETIC and price data row staleness (age > 60s marked with * and dimmed).

Decision Logging & Simulated PnL:
- SignalEngine publishes structured signals to Redis channel trading_signals.
- Signal directions include EXIT_LONG and EXIT_SHORT enabling explicit closure of simulated positions when trading is disabled.
- DecisionLogger subscribes and persists normalized JSON entries into Redis list decision_logs (TTL 24h, bounded length) including optional fields realized_pnl and position_size.
- Realized PnL is computed only on EXIT_* events using a fixed simulation position size (see [simulation] config) and aggregated into Redis key simulated_pnl_summary.
- /api/decisions endpoint returns recent entries (default limit=50).
- /api/performance automatically falls back to simulated_pnl_summary when no live execution metrics are present (public / synthetic modes).

Redis Keys (Schema Additions):
- trade:{SYMBOL} (latest trade price, 30s TTL)
- orderbook:{SYMBOL} (mid-price + depth snapshot, 60s TTL)
- kline:{SYMBOL} (latest aggregated candle, 300s TTL)
- klines:{SYMBOL} (list of recent candles, size 200, 1h TTL)
- decision_logs (LPUSH list of DecisionLog objects, length ≤500, 24h TTL; entries may include realized_pnl, position_size if EXIT_* processed)
- simulated_pnl_summary (hash: total_pnl, total_trades, wins, losses, win_rate, max_drawdown_pct, peak_equity, updated_ts) – maintained only when trading disabled

## Quick Start

### 1. Environment Setup

```bash
# Clone and configure
git clone <repository-url>
cd Helios

# Copy environment template
cp .env.example .env

# Edit configuration
nano .env
```

### 2. Required API Keys

Add these to your `.env` file:

```bash
# Binance API (Required)
BINANCE_API_KEY=your_binance_api_key
BINANCE_API_SECRET=your_binance_api_secret
# Public read-only mode (no trading) allowed when credentials invalid/missing
ALLOW_PUBLIC_MODE=true
# Enable legacy synthetic (simulated) data generation (off by default, gated)
ALLOW_SYNTHETIC=false

# Reddit API (Required for sentiment analysis)
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret
REDDIT_USER_AGENT=helios_trading_bot_v1.0

# Optional: Telegram notifications
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

### 3. Docker Deployment

#### Production Mode
```bash
# Build and start services
docker-compose up -d

# View logs
docker-compose logs -f helios
docker-compose logs -f redis

# Check health status
curl http://localhost:8080/health
```

#### Development Mode
```bash
# Use development configuration
docker-compose -f docker-compose.dev.yml up -d

# Access development features
docker-compose -f docker-compose.dev.yml exec helios bash
```

## Configuration

### Trading Configuration (`config/config.ini`)

```ini
[trading]
symbol = BTCUSDT
max_position_size = 1000.0
leverage = 10
stop_loss_pct = 2.0
take_profit_pct = 3.0
enable_sentiment_filtering = true
sentiment_threshold = 0.3

[sentiment]
subreddits = cryptocurrency,Bitcoin,CryptoCurrency,CryptoMarkets
max_posts_per_subreddit = 50
sentiment_decay_hours = 2.0
min_post_score = 10
enable_crypto_filtering = true

[trend_analysis]
timeframes = 1m,5m,15m,1h,4h,1d
rsi_period = 14
ema_fast = 12
ema_slow = 26
trend_strength_threshold = 0.6

[dashboard]
enable = true
port = 8080
refresh_interval = 30

[simulation]
position_size = 1.0   # Fixed notional/contract size for simulated PnL when trading disabled
```

Simulation Notes:
- Simulated realized PnL is only computed upon EXIT_LONG / EXIT_SHORT signals.
- Unmatched EXIT_* (no open position) are ignored safely.
- /api/performance merges into live metrics automatically when trading becomes enabled (simulated_pnl_summary is ignored in live mode).

## Architecture

### Service Components

1. **Main Trading Engine** (`main.py`)
   - Orchestrates all components
   - Coordinates mode selection (live/public/synthetic)
   - Lifecycle management & graceful shutdown

2. **Data Ingestion (Live)** (`app/data_ingestor.py`)
   - Authenticated Binance futures websocket streams
   - Order book, trades, klines
   - Active only in live mode

3. **Public Price Ingestion (Read-Only)** (`app/public_price_ingestor.py`)
   - Binance REST polling + CoinGecko fallback
   - Normalizes to same Redis schema as live
   - Active only in public mode

4. **Synthetic Ingestion (Legacy / Optional)**
   - Simulated data generation threads
   - Gated by ALLOW_SYNTHETIC=true
   - Disabled by default

5. **Signal Engine** (`app/signal_engine.py`)
   - Multi-timeframe technical / sentiment fusion
   - Publishes structured signals → Redis channel trading_signals
   - Throttling & confidence scoring

6. **Decision Logger** (`app/decision_logger.py`)
   - Subscribes trading_signals
   - Persists bounded list decision_logs with TTL
   - Powers /api/decisions dashboard table

7. **Risk Manager** (`app/risk_manager.py`)
   - Position sizing & exposure constraints
   - Still initialized in public mode (dry-run logic)

8. **Execution Manager** (`app/execution_manager.py`)
   - Order placement / management (live mode only)

9. **Performance Tracker** (`app/performance_tracker.py`)
   - Real-time P&L and trade analytics
   - Aggregated performance_metrics key

10. **Reddit Ingestion & Sentiment** (`app/reddit_ingestor.py`, `app/sentiment_utils.py`)
    - Streaming subreddit posts
    - Domain-specific sentiment scoring

11. **Monitoring Agent** (`app/monitoring_agent.py`)
    - Periodic system health snapshots
    - Alerting hooks (extendable)

12. **Health Dashboard** (HTTP endpoints)
    - /api/prices, /api/decisions, /api/health, /api/performance, /api/metrics
    - Live HTML dashboard with mode badge & staleness indicators

### Data Flow

```
Reddit API → Sentiment Analysis → Signal Engine → Risk Management → Execution
     ↓              ↓                    ↓              ↓             ↓
   Redis         Redis Cache        Trading Logic   Position Size   Binance API
     ↓              ↓                    ↓              ↓             ↓
Performance Tracker ← Trade Results ← Order Status ← Execution Confirmation
```

## Monitoring

### Health Checks

The system provides comprehensive health monitoring:

```bash
# System health
curl http://localhost:8080/api/health

# Live prices (mode + staleness + per-symbol latest price)
curl http://localhost:8080/api/prices

# Recent decisions (trading signals normalized & persisted)
curl http://localhost:8080/api/decisions

# Performance metrics (PnL + recent trades)
curl http://localhost:8080/api/performance

# Raw metrics summary
curl http://localhost:8080/api/metrics
```

### Log Monitoring

```bash
# Real-time logs
docker-compose logs -f helios

# Filter by component
docker-compose logs helios | grep "SentimentAnalyzer"
docker-compose logs helios | grep "SignalEngine"

# Error tracking
docker-compose logs helios | grep "ERROR"
```

### Performance Dashboard

Access the web dashboard at `http://localhost:8080` for:
- Real-time trading metrics
- Sentiment analysis results
- System component status
- Historical performance charts

## Troubleshooting

### Common Issues

1. **NLTK Permission Errors**
   - **Fixed**: NLTK data is pre-downloaded during Docker build
   - Verify: `docker run --rm helios-trading python -c "import nltk; nltk.data.find('tokenizers/punkt')"`

2. **Redis Connection Issues**
   ```bash
   # Check Redis container
   docker-compose ps redis
   
   # Test connection
   docker-compose exec redis redis-cli ping
   ```

3. **API Rate Limiting**
   - Binance: Implement request throttling
   - Reddit: Monitor API quota usage
   - Check logs for rate limit warnings

4. **Memory Usage**
   ```bash
   # Monitor container resources
   docker stats helios-trading
   
   # Adjust limits in docker-compose.yml if needed
   ```

### Debugging

#### Enable Debug Mode
```bash
# Set in .env
LOG_LEVEL=DEBUG

# Rebuild and restart
docker-compose down && docker-compose up -d
```

#### Component Testing
```bash
# Test sentiment analysis
docker-compose exec helios python -c "
from app.sentiment_utils import create_sentiment_analyzer
analyzer = create_sentiment_analyzer()
result = analyzer.analyze_sentiment('Bitcoin pump incoming!')
print(result)
"

# Test Reddit connection
docker-compose exec helios python -c "
from app.reddit_ingestor import RedditIngestor
from app.config_manager import ConfigManager
config = ConfigManager()
reddit = RedditIngestor(config.get_reddit_config())
print('Reddit connection:', reddit.test_connection())
"
```

## Production Considerations

### Security
- Store API keys in secure environment variables
- Use Docker secrets for sensitive data
- Enable firewall for exposed ports
- Regular security updates for base images

### Scaling
- Redis cluster for high availability
- Load balancing for multiple trading instances
- Horizontal scaling with Docker Swarm/Kubernetes

### Backup & Recovery
- Regular Redis data backups
- Configuration file versioning
- Trading history archival
- Disaster recovery procedures

### Performance Optimization
- Monitor memory usage and optimize data structures
- Implement connection pooling for external APIs
- Cache frequently accessed market data
- Optimize database queries and indexing

## API Integration

### Binance API Requirements
- Valid API key with futures trading permissions
- Sufficient account balance for margin requirements
- IP whitelisting (recommended for production)

### Reddit API Setup
1. Create Reddit application at https://www.reddit.com/prefs/apps
2. Get client ID and client secret
3. Set appropriate user agent string
4. Respect Reddit API rate limits (60 requests/minute)

## Support

For issues and questions:
1. Check logs for error messages
2. Verify API credentials and permissions
3. Monitor system resources and performance
4. Review configuration settings
5. Test individual components in isolation

The enhanced Helios Trading System provides a robust, containerized platform for automated cryptocurrency trading with advanced sentiment analysis and comprehensive monitoring capabilities.