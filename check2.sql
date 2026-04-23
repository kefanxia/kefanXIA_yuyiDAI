
-- users 
SELECT column_name, data_type, character_maximum_length, 
       column_default, is_nullable
FROM information_schema.columns
WHERE table_name = 'users'
ORDER BY ordinal_position;

-- risk_profiles
SELECT column_name, data_type, column_default, is_nullable
FROM information_schema.columns
WHERE table_name = 'risk_profiles'
ORDER BY ordinal_position;

-- portfolios
SELECT column_name, data_type, column_default, is_nullable
FROM information_schema.columns
WHERE table_name = 'portfolios'
ORDER BY ordinal_position;

-- assets
SELECT column_name, data_type, column_default, is_nullable
FROM information_schema.columns
WHERE table_name = 'assets'
ORDER BY ordinal_position;

-- holdings
SELECT column_name, data_type, column_default, is_nullable
FROM information_schema.columns
WHERE table_name = 'holdings'
ORDER BY ordinal_position;

-- transactions
SELECT column_name, data_type, column_default, is_nullable
FROM information_schema.columns
WHERE table_name = 'transactions'
ORDER BY ordinal_position;

-- market_prices
SELECT column_name, data_type, column_default, is_nullable
FROM information_schema.columns
WHERE table_name = 'market_prices'
ORDER BY ordinal_position;

-- rebalance_log
SELECT column_name, data_type, column_default, is_nullable
FROM information_schema.columns
WHERE table_name = 'rebalance_log'
ORDER BY ordinal_position;
--all the tables
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

--details of the table
SELECT * FROM users ORDER BY username;

SELECT * FROM risk_profiles;

SELECT 
    u.username,
    rp.tolerance,
    rp.max_stock_pct,
    rp.max_bond_pct,
    rp.max_crypto_pct
FROM risk_profiles rp
JOIN users u ON u.user_id = rp.user_id
ORDER BY u.username;

SELECT * FROM portfolios ORDER BY name;

SELECT * FROM assets ORDER BY asset_type;

SELECT * FROM stocks;
SELECT * FROM bonds;
SELECT * FROM cryptos;

SELECT * FROM holdings ORDER BY portfolio_id;

SELECT * FROM transactions ORDER BY txn_at;

-- market_prices(data too large)
SELECT a.symbol, mp.trade_date, mp.open_price, mp.close_price, mp.volume
FROM market_prices mp
JOIN assets a ON a.asset_id = mp.asset_id
ORDER BY a.symbol, mp.trade_date DESC;

SELECT * FROM rebalance_log ORDER BY logged_at DESC;

--check BRIN and Partial Index

SELECT indexname, tablename, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename;

--check the triggers

SELECT trigger_name, event_manipulation, event_object_table, action_timing
FROM information_schema.triggers
WHERE trigger_schema = 'public'
ORDER BY event_object_table;

--views(materialized)

SELECT * FROM v_risk_exposure ORDER BY username;

-- REFRESH MATERIALIZED VIEW mv_portfolio_valuation;
SELECT * FROM mv_portfolio_valuation ORDER BY username, market_value DESC;