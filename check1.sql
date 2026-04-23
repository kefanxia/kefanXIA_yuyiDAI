-- check user cash balance
SELECT username, cash_balance FROM users ORDER BY username;

-- check the holdings
SELECT u.username, a.symbol, h.quantity, h.avg_cost, h.mkt_value
FROM holdings h
JOIN portfolios p ON p.portfolio_id = h.portfolio_id
JOIN users u ON u.user_id = p.user_id
JOIN assets a ON a.asset_id = h.asset_id
ORDER BY u.username, a.symbol;

-- refresh market value + Materialized View
UPDATE holdings h
SET mkt_value = h.quantity * (
    SELECT close_price FROM market_prices mp
    WHERE mp.asset_id = h.asset_id
    ORDER BY trade_date DESC LIMIT 1
);
REFRESH MATERIALIZED VIEW mv_portfolio_valuation;

-- check complete valuation
SELECT username, symbol, quantity, avg_cost,
       current_price, market_value, unrealised_pnl, return_pct
FROM mv_portfolio_valuation
ORDER BY username, market_value DESC;

--risk exposure
SELECT username, asset_type, asset_class_value,
       total_portfolio_value, exposure_pct
FROM v_risk_exposure
ORDER BY username, exposure_pct DESC;

--check the alerts
SELECT username, asset_type, current_pct,
       limit_pct, excess_pct, suggested_action
FROM rebalance_log
ORDER BY excess_pct DESC;

-- EXPLAIN ANALYZE
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM market_prices
WHERE trade_date BETWEEN '2024-01-01' AND '2024-12-31';