# Helios Testing Guide

Comprehensive guidance for validating operating modes, decision logging (including EXIT signals), simulated PnL aggregation, and API/Redis contracts.

## 1. Scope

This guide covers tests for:
- Mode resolution (live / public / synthetic)
- Public price ingestion fallback chain
- Synthetic mode gating
- Decision logging durability & schema evolution
- EXIT_LONG / EXIT_SHORT semantics and realized PnL
- Simulated performance aggregation (simulated_pnl_summary)
- Performance API fallback logic
- Dashboard / API response integrity
- Redis key TTLs & structure
- Backward compatibility (pre-EXIT log entries)

## 2. Test Framework & Conventions

Recommended: pytest + pytest-asyncio (if async parts) + freezegun (time control) + fakeredis (or real ephemeral Redis in Docker).

Suggested structure:
```
tests/
  test_mode_resolution.py
  test_public_price_ingestion.py
  test_synthetic_gating.py
  test_decision_logging.py
  test_exit_signals_and_simulated_pnl.py
  test_performance_fallback.py
  test_api_contracts.py
  test_redis_schema.py
  test_backward_compatibility.py
```

Naming:
- Unit tests: granular functional logic
- Integration tests: end-to-end via running app container or orchestrated startup helper

## 3. Fixtures

Key fixtures:
- redis_client (fakeredis or test container)
- config_override (temp config file / env vars)
- helios_system (bootstrapped main orchestrator with patched components)
- time_travel (freezegun freezer)
- price_seed (helper to inject trade / orderbook Redis keys)

## 4. Mode Resolution Tests

File Targets: [main.py](main.py), [app/config_manager.py](app/config_manager.py)

Scenarios:
1. Valid credentials → mode == live; DataIngestor started; PublicPriceIngestor not started.
2. Missing credentials + ALLOW_PUBLIC_MODE=true (default) → mode == public; PublicPriceIngestor started; ExecutionManager not started.
3. Missing credentials + ALLOW_PUBLIC_MODE=false + ALLOW_SYNTHETIC=true → mode == synthetic; synthetic threads enabled.
4. Missing credentials + ALLOW_PUBLIC_MODE=false + ALLOW_SYNTHETIC=false → startup abort (assert raises / non-zero exit).

Assertions:
- HeliosSystem.operational_mode value
- Component presence (e.g., system.data_ingestor is None vs not None)
- Log message patterns (optional)

## 5. Public Price Ingestion Tests

File Target: [app/public_price_ingestor.py](app/public_price_ingestor.py)

Tests:
- Writes trade:{SYMBOL} and orderbook:{SYMBOL} keys with TTL set.
- Fallback: If Binance REST mock fails (raise), CoinGecko path populates at least one price.
- Staleness: Confirm keys expire (advance time beyond TTL, assert missing).

## 6. Synthetic Mode Gating

File Target: [app/data_ingestor.py](app/data_ingestor.py)

Test:
- With ALLOW_SYNTHETIC=true & no credentials: synthetic internal generation flag set (e.g., synthetic_mode True).
- With credentials present + ALLOW_SYNTHETIC=true → still live (synthetic disabled).
- Ensure no unintended synthetic threads in public mode.

## 7. Decision Logging Tests

Files: [app/decision_logger.py](app/decision_logger.py), [app/models.py](app/models.py)

Tests:
- Signal ingestion: Publish mock Redis message (LONG) → entry appended to decision_logs (length ≤500).
- TTL: Set time forward 24h + epsilon → list key removed.
- Bounded length: Insert 600 entries → list length == 500 and oldest trimmed.
- Fields presence: direction, symbol, timestamp.

## 8. EXIT Signal & Simulated PnL Tests

Focus: correctness of realized PnL computation and position lifecycle.

Scenario Set (for both LONG/EXIT_LONG and SHORT/EXIT_SHORT):
1. Open then exit:
   - Price entry: entry=100, exit=105, position_size=2.0 → realized_pnl= (105-100)*2 = 10 (LONG)
   - SHORT: entry=100, exit=95, position_size=1.5 → realized_pnl= (100-95)*1.5 = 7.5
2. Exit without open position: EXIT_* ignored (no new realized_pnl).
3. Overwrite existing position: LONG at 100 then LONG at 102 (strategy replacement) then EXIT_LONG at 101 → realized_pnl = (101-102)*size.
4. Precision: Ensure floating rounding not truncated (consider decimal tolerance 1e-9).

Assertions:
- decision_logs latest entry contains realized_pnl only on EXIT events
- position tracking cleared post-exit (internal state _open_positions absent for symbol)

## 9. Simulated Performance Aggregation

Redis Key: simulated_pnl_summary

Tests:
- After first realized trade: total_pnl, total_trades=1, wins or losses increment.
- Multiple trades update win_rate = wins / total_trades.
- Max drawdown: Create equity path by sequence of wins then losses; validate max_drawdown_pct calculation.
- peak_equity only increases during new highs.

Edge Cases:
- No trades yet: key may not exist → performance fallback uses zeros.

## 10. Performance API Fallback Tests

File: [app/health_dashboard.py](app/health_dashboard.py) (function serving /api/performance)

Tests:
- Live mode with performance_metrics present → uses live metrics (ignore simulated).
- Public mode with no performance_metrics → uses simulated_pnl_summary.
- Public mode with both keys (unlikely) → simulated still chosen only if live missing (verify conditional logic).
- JSON contract includes realized metrics fields (total_pnl etc.).

## 11. API Contract Tests

Endpoints:
- /api/prices: Contains mode, each symbol record has price & staleness flag (simulate aging).
- /api/decisions: Includes EXIT_LONG/EXIT_SHORT lines and realized_pnl field (may be null/absent for non-exit).
- /api/performance: Fallback behavior validated (see section 10).

Validation:
- HTTP 200
- JSON schema (define pydantic or jsonschema for robust checking)
- No unexpected extra fields (stability)

## 12. Redis Schema & TTL Tests

Keys & TTL expectations:
- trade:{SYMBOL} ~30s
- orderbook:{SYMBOL} ~60s
- decision_logs 86400s
- simulated_pnl_summary: refresh on each EXIT; TTL (if set) not prematurely expiring (decide whether TTL applied—test actual behavior)

Procedure:
- Insert then query TTL via redis_client.ttl(key) ensuring it falls within expected window.

## 13. Backward Compatibility Tests

Legacy log (without realized_pnl, position_size):
- Insert mock JSON (omit new fields) → API /api/decisions returns entry without failure.
- Ensure dashboard rendering logic (if template tests exist) tolerates missing fields.

## 14. Negative & Robustness Tests

- Malformed signal direction → ignored (no crash, logged warning).
- Duplicate EXIT without position → no simulated_pnl_summary mutation.
- Very large position_size (stress numeric ranges) still updates summary.

## 15. Test Data Generation Helpers

Implement utility functions:
```python
def publish_signal(redis_client, symbol, direction, price, ts=None, confidence=0.5):
    ...
```
Add them under tests/utils or a conftest fixture.

## 16. Running Tests

Local:
```bash
pytest -q
```

With coverage:
```bash
pytest --cov=app --cov-report=term-missing
```

Inside Docker (if adding a test service in docker-compose):
```bash
docker compose run --rm helios pytest -q
```

## 17. CI Considerations

- Parallelize Redis tests using isolated DB indices (fakeredis or separate containers).
- Fail fast on schema drift (JSON schema snapshots).
- Add coverage threshold (e.g., 75% initially).

## 18. Future Test Enhancements

- Property-based tests for PnL paths (hypothesis)
- Load tests for public ingestion polling cadence
- Mutation tests for signal direction handling

## 19. Summary Checklist Mapping

Relates to TODO items:
- test_mode_resolution.py → TODO #26, #29, #30
- test_decision_logging.py → TODO #26
- test_exit_signals_and_simulated_pnl.py → TODO #38
- test_performance_fallback.py → part of #38 (performance fallback)
- test_backward_compatibility.py → regression safety

## 20. Quality Gates

Before draft PR:
- All new tests green
- Coverage diff non-negative
- API schemas unchanged except documented additions (EXIT_* & realized_pnl)

---

This guide ensures systematic validation of new EXIT signal and simulated PnL functionality while maintaining prior system guarantees.