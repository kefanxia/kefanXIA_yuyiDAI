--Insert simulated users and holding data
INSERT INTO users (username, email, cash_balance) VALUES
    ('alice',   'alice@example.com',   500000.00),
    ('bob',     'bob@example.com',     200000.00),
    ('charlie', 'charlie@example.com', 100000.00);

INSERT INTO risk_profiles (user_id, tolerance, max_stock_pct, max_bond_pct, max_crypto_pct)
SELECT user_id, 'high',   70, 20, 10 FROM users WHERE username = 'alice';

INSERT INTO risk_profiles (user_id, tolerance, max_stock_pct, max_bond_pct, max_crypto_pct)
SELECT user_id, 'medium', 60, 30, 10 FROM users WHERE username = 'bob';

INSERT INTO risk_profiles (user_id, tolerance, max_stock_pct, max_bond_pct, max_crypto_pct)
SELECT user_id, 'low',    40, 50, 10 FROM users WHERE username = 'charlie';

INSERT INTO portfolios (user_id, name)
SELECT user_id, 'Alice Portfolio' FROM users WHERE username = 'alice';

INSERT INTO portfolios (user_id, name)
SELECT user_id, 'Bob Portfolio' FROM users WHERE username = 'bob';

INSERT INTO portfolios (user_id, name)
SELECT user_id, 'Charlie Portfolio' FROM users WHERE username = 'charlie';

-- Alice's transactions
INSERT INTO transactions (portfolio_id, asset_id, txn_type, quantity, price, txn_at)
SELECT p.portfolio_id, a.asset_id, 'BUY', 50, 130.73, '2023-01-10'
FROM portfolios p JOIN users u ON u.user_id = p.user_id
JOIN assets a ON a.symbol = 'NVDA'
WHERE u.username = 'alice';

INSERT INTO transactions (portfolio_id, asset_id, txn_type, quantity, price, txn_at)
SELECT p.portfolio_id, a.asset_id, 'BUY', 2, 16800.00, '2023-01-20'
FROM portfolios p JOIN users u ON u.user_id = p.user_id
JOIN assets a ON a.symbol = 'BTC-USD'
WHERE u.username = 'alice';

INSERT INTO transactions (portfolio_id, asset_id, txn_type, quantity, price, txn_at)
SELECT p.portfolio_id, a.asset_id, 'BUY', 5, 96.50, '2023-01-10'
FROM portfolios p JOIN users u ON u.user_id = p.user_id
JOIN assets a ON a.symbol = 'TLT'
WHERE u.username = 'alice';

-- Bob's transactions
INSERT INTO transactions (portfolio_id, asset_id, txn_type, quantity, price, txn_at)
SELECT p.portfolio_id, a.asset_id, 'BUY', 40, 245.00, '2022-06-15'
FROM portfolios p JOIN users u ON u.user_id = p.user_id
JOIN assets a ON a.symbol = 'NVDA'
WHERE u.username = 'bob';

INSERT INTO transactions (portfolio_id, asset_id, txn_type, quantity, price, txn_at)
SELECT p.portfolio_id, a.asset_id, 'BUY', 20, 98.20, '2022-06-15'
FROM portfolios p JOIN users u ON u.user_id = p.user_id
JOIN assets a ON a.symbol = 'TLT'
WHERE u.username = 'bob';

INSERT INTO transactions (portfolio_id, asset_id, txn_type, quantity, price, txn_at)
SELECT p.portfolio_id, a.asset_id, 'BUY', 0.5, 19200.00, '2022-06-20'
FROM portfolios p JOIN users u ON u.user_id = p.user_id
JOIN assets a ON a.symbol = 'BTC-USD'
WHERE u.username = 'bob';

-- Charlie's transactions（the first one OK，the second one will be rejected by Trigger）
INSERT INTO transactions (portfolio_id, asset_id, txn_type, quantity, price, txn_at)
SELECT p.portfolio_id, a.asset_id, 'BUY', 100, 96.00, '2022-01-10'
FROM portfolios p JOIN users u ON u.user_id = p.user_id
JOIN assets a ON a.symbol = 'TLT'
WHERE u.username = 'charlie';

INSERT INTO transactions (portfolio_id, asset_id, txn_type, quantity, price, txn_at)
SELECT p.portfolio_id, a.asset_id, 'BUY', 500, 500.00, '2023-11-01'
FROM portfolios p JOIN users u ON u.user_id = p.user_id
JOIN assets a ON a.symbol = 'NVDA'
WHERE u.username = 'charlie';

-- Check the changes in the user's balance
SELECT username, cash_balance FROM users ORDER BY username;

-- Check the holdings status
SELECT u.username, a.symbol, h.quantity, h.avg_cost, h.mkt_value
FROM holdings h
JOIN portfolios p ON p.portfolio_id = h.portfolio_id
JOIN users u ON u.user_id = p.user_id
JOIN assets a ON a.asset_id = h.asset_id
ORDER BY u.username, a.symbol;

-- Refresh market value (update all holdings with the latest price)
UPDATE holdings h
SET mkt_value = h.quantity * (
    SELECT close_price FROM market_prices mp
    WHERE mp.asset_id = h.asset_id
    ORDER BY trade_date DESC LIMIT 1
);

-- refresh Materialized View
REFRESH MATERIALIZED VIEW mv_portfolio_valuation;

-- View the complete holdings valuation
SELECT username, symbol, quantity, avg_cost,
       current_price, market_value, unrealised_pnl, return_pct
FROM mv_portfolio_valuation
ORDER BY username, market_value DESC;

-- risk exposure analysis
SELECT username, asset_type, 
       asset_class_value,
       total_portfolio_value,
       exposure_pct
FROM v_risk_exposure
ORDER BY username, exposure_pct DESC;

-- generate alerts automatically
CALL run_rebalancing_check();

-- check the rreesults
SELECT username, asset_type, current_pct, 
       limit_pct, excess_pct, suggested_action
FROM rebalance_log
ORDER BY excess_pct DESC;

-- 
CREATE OR REPLACE PROCEDURE run_rebalancing_check()
LANGUAGE plpgsql AS $$
DECLARE
    rec     RECORD;
    v_limit NUMERIC;
    v_action TEXT;
BEGIN
    FOR rec IN
        SELECT
            re.portfolio_id,
            re.username,
            re.asset_type,
            re.exposure_pct,
            re.total_portfolio_value,
            rp.max_stock_pct,
            rp.max_bond_pct,
            rp.max_crypto_pct
        FROM v_risk_exposure re
        JOIN portfolios p  ON p.portfolio_id = re.portfolio_id
        JOIN users u       ON u.user_id      = p.user_id
        LEFT JOIN risk_profiles rp ON rp.user_id = u.user_id
    LOOP
        v_limit := CASE rec.asset_type
            WHEN 'stock'  THEN COALESCE(rec.max_stock_pct,  60)
            WHEN 'bond'   THEN COALESCE(rec.max_bond_pct,   30)
            WHEN 'crypto' THEN COALESCE(rec.max_crypto_pct, 10)
            ELSE 100
        END;

        IF rec.exposure_pct > v_limit THEN
            v_action := 'REDUCE ' || upper(rec.asset_type) ||
                ' by ' || ROUND(rec.exposure_pct - v_limit, 1) ||
                '% — sell approx $' ||
                ROUND((rec.exposure_pct - v_limit) / 100.0
                      * rec.total_portfolio_value, 2);

            INSERT INTO rebalance_log
                (portfolio_id, username, asset_type,
                 current_pct, limit_pct, excess_pct, suggested_action)
            VALUES
                (rec.portfolio_id, rec.username, rec.asset_type,
                 rec.exposure_pct, v_limit,
                 rec.exposure_pct - v_limit, v_action);
        END IF;
    END LOOP;
    RAISE NOTICE 'Rebalancing check done.';
END;
$$;

CALL run_rebalancing_check();

SELECT username, asset_type, current_pct,
       limit_pct, excess_pct, suggested_action
FROM rebalance_log
ORDER BY excess_pct DESC;

-- Compare the execution plans of the two queries
-- Plan A：full table read（interdit all the index）
SET enable_indexscan = off;
SET enable_bitmapscan = off;
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM market_prices
WHERE trade_date BETWEEN '2024-01-01' AND '2024-12-31';

-- Plan B：restore index，and use BRIN
SET enable_indexscan = on;
SET enable_bitmapscan = on;
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM market_prices
WHERE trade_date BETWEEN '2024-01-01' AND '2024-12-31';