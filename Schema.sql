--  Portfolio Management Database — Complete Schema
--  Based on ER diagram
--  Data: BTC-USD (crypto), NVDA (stock), TLT (bond ETF)
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- LAYER 1: Users

CREATE TABLE users (
    user_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username     VARCHAR(50)  UNIQUE NOT NULL,
    email        VARCHAR(100) UNIQUE NOT NULL,
    cash_balance NUMERIC(18, 2) NOT NULL DEFAULT 100000.00,
    created_at   TIMESTAMP DEFAULT NOW()
);

-- Each user has at most one risk profile (1 → 0/1)

CREATE TABLE risk_profiles (
    profile_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id        UUID UNIQUE REFERENCES users(user_id) ON DELETE CASCADE,
    tolerance      VARCHAR(10) CHECK (tolerance IN ('low', 'medium', 'high')) DEFAULT 'medium',
    max_stock_pct  NUMERIC(5, 2) DEFAULT 60.00,
    max_bond_pct   NUMERIC(5, 2) DEFAULT 30.00,
    max_crypto_pct NUMERIC(5, 2) DEFAULT 10.00
);

-- LAYER 2: Portfolios (1 user → N portfolios)

CREATE TABLE portfolios (
    portfolio_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id      UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    name         VARCHAR(100) NOT NULL,
    created_at   TIMESTAMP DEFAULT NOW()
);

-- LAYER 3: Assets — ISA hierarchy

-- Parent table: shared attributes for ALL asset types
CREATE TABLE assets (
    asset_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_type VARCHAR(10) NOT NULL CHECK (asset_type IN ('stock', 'bond', 'crypto')),
    name       VARCHAR(100) NOT NULL,
    symbol     VARCHAR(20)  NOT NULL UNIQUE,  -- matches CSV Symbol column
    currency   VARCHAR(3)   NOT NULL DEFAULT 'USD'
);

-- ISA subtype 1: Stocks (asset_id: both PK and FK)
CREATE TABLE stocks (
    asset_id  UUID PRIMARY KEY REFERENCES assets(asset_id) ON DELETE CASCADE,
    ticker    VARCHAR(10) NOT NULL,
    sector    VARCHAR(50),
    market_cap NUMERIC(20, 2),
    pe_ratio  NUMERIC(10, 2),
    exchange  VARCHAR(20) DEFAULT 'NASDAQ'
);

-- ISA subtype 2: Bonds (asset_id: both PK and FK)
CREATE TABLE bonds (
    asset_id      UUID PRIMARY KEY REFERENCES assets(asset_id) ON DELETE CASCADE,
    coupon_rate   NUMERIC(6, 4),
    maturity_date DATE,
    face_value    NUMERIC(18, 2) DEFAULT 1000.00,
    issuer        VARCHAR(100),
    credit_rating VARCHAR(5)
);

-- ISA subtype 3: Cryptos (asset_id: both PK and FK)
CREATE TABLE cryptos (
    asset_id           UUID PRIMARY KEY REFERENCES assets(asset_id) ON DELETE CASCADE,
    coingecko_id       VARCHAR(50),
    blockchain         VARCHAR(50),
    protocol           VARCHAR(50),
    max_supply         NUMERIC(30, 8),
    circulating_supply NUMERIC(30, 8)
);

-- LAYER 4: Market Prices (time-series, from CSV)

-- Composite PK: one row per asset per day
CREATE TABLE market_prices (
    asset_id    UUID NOT NULL REFERENCES assets(asset_id) ON DELETE CASCADE,
    trade_date  DATE NOT NULL,
    open_price  NUMERIC(18, 6),
    high_price  NUMERIC(18, 6),
    low_price   NUMERIC(18, 6),
    close_price NUMERIC(18, 6) NOT NULL,
    volume      NUMERIC(24, 2),
    PRIMARY KEY (asset_id, trade_date)
);

-- BRIN index: efficient for time-series (sequential inserts by date)
-- Much smaller than B+Tree (~500x), equally fast for range queries
CREATE INDEX idx_market_prices_brin
    ON market_prices USING BRIN (trade_date);

-- Partial index: ultra-fast for the most common query (latest prices)
CREATE INDEX idx_market_prices_recent
    ON market_prices (asset_id, trade_date DESC)
    WHERE trade_date >= '2025-03-20';

-- LAYER 5: Holdings and Transactions

-- Current positions: maintained automatically by triggers
CREATE TABLE holdings (
    holding_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    portfolio_id UUID NOT NULL REFERENCES portfolios(portfolio_id) ON DELETE CASCADE,
    asset_id     UUID NOT NULL REFERENCES assets(asset_id),
    quantity     NUMERIC(18, 8) NOT NULL DEFAULT 0,
    avg_cost     NUMERIC(18, 6) NOT NULL DEFAULT 0,
    mkt_value    NUMERIC(18, 2),             -- updated by trigger on new market price
    last_updated TIMESTAMP DEFAULT NOW(),
    UNIQUE (portfolio_id, asset_id)          -- one row per asset per portfolio
);

-- Full transaction history
CREATE TABLE transactions (
    txn_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    portfolio_id UUID NOT NULL REFERENCES portfolios(portfolio_id),
    asset_id     UUID NOT NULL REFERENCES assets(asset_id),
    txn_type     VARCHAR(4) NOT NULL CHECK (txn_type IN ('BUY', 'SELL')),
    quantity     NUMERIC(18, 8) NOT NULL CHECK (quantity > 0),
    price        NUMERIC(18, 6) NOT NULL CHECK (price > 0),
    total_value  NUMERIC(18, 2) GENERATED ALWAYS AS (quantity * price) STORED,
    txn_at       TIMESTAMP DEFAULT NOW()
);

-- LAYER 6: Rebalance Log (output of stored procedure)

CREATE TABLE rebalance_log (
    log_id           BIGSERIAL PRIMARY KEY,
    portfolio_id     UUID REFERENCES portfolios(portfolio_id),
    username         VARCHAR(50),
    asset_type       VARCHAR(10),
    current_pct      NUMERIC(6, 2),
    limit_pct        NUMERIC(6, 2),
    excess_pct       NUMERIC(6, 2),
    suggested_action TEXT,
    logged_at        TIMESTAMP DEFAULT NOW()
);

-- TRIGGERS: Transactional Integrity

-- Trigger 1: Check cash balance before BUY; deduct on success
CREATE OR REPLACE FUNCTION check_and_deduct_cash()
RETURNS TRIGGER AS $$
DECLARE
    v_user_id UUID;
    v_cost    NUMERIC;
    v_balance NUMERIC;
BEGIN
    IF NEW.txn_type = 'BUY' THEN
        SELECT p.user_id INTO v_user_id
        FROM portfolios p WHERE p.portfolio_id = NEW.portfolio_id;

        v_cost := NEW.quantity * NEW.price;

        SELECT cash_balance INTO v_balance
        FROM users WHERE user_id = v_user_id;

        IF v_balance < v_cost THEN
            RAISE EXCEPTION 'Insufficient balance: need $%, have $%',
                ROUND(v_cost, 2), ROUND(v_balance, 2);
        END IF;

        -- Deduct cash immediately
        UPDATE users
        SET cash_balance = cash_balance - v_cost
        WHERE user_id = v_user_id;
    END IF;
    RETURN NEW;
END;
 $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_check_cash ON transactions;
CREATE TRIGGER trg_check_cash
    BEFORE INSERT ON transactions
    FOR EACH ROW EXECUTE FUNCTION check_and_deduct_cash();

-- Trigger 2: Update holdings after every transaction
CREATE OR REPLACE FUNCTION update_holding()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.txn_type = 'BUY' THEN
        INSERT INTO holdings (portfolio_id, asset_id, quantity, avg_cost)
        VALUES (NEW.portfolio_id, NEW.asset_id, NEW.quantity, NEW.price)
        ON CONFLICT (portfolio_id, asset_id) DO UPDATE
            SET avg_cost = (
                    (holdings.quantity * holdings.avg_cost)
                  + (NEW.quantity     * NEW.price)
                ) / (holdings.quantity + NEW.quantity),
                quantity     = holdings.quantity + NEW.quantity,
                last_updated = NOW();

    ELSIF NEW.txn_type = 'SELL' THEN
        -- Check sufficient holdings exist
        IF NOT EXISTS (
            SELECT 1 FROM holdings
            WHERE portfolio_id = NEW.portfolio_id
              AND asset_id     = NEW.asset_id
              AND quantity     >= NEW.quantity
        ) THEN
            RAISE EXCEPTION 'Insufficient holdings to sell % of asset %',
                NEW.quantity, NEW.asset_id;
        END IF;

        UPDATE holdings
        SET quantity     = quantity - NEW.quantity,
            last_updated = NOW()
        WHERE portfolio_id = NEW.portfolio_id
          AND asset_id     = NEW.asset_id;
        -- Return cash to user
        UPDATE users u
        SET cash_balance = cash_balance + (NEW.quantity * NEW.price)
        FROM portfolios p
        WHERE p.portfolio_id = NEW.portfolio_id
          AND u.user_id      = p.user_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_update_holding ON transactions;
CREATE TRIGGER trg_update_holding
    AFTER INSERT ON transactions
    FOR EACH ROW EXECUTE FUNCTION update_holding();

-- Trigger 3: Refresh mkt_value in holdings whenever a new price arrives
CREATE OR REPLACE FUNCTION refresh_mkt_value()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE holdings
    SET mkt_value    = quantity * NEW.close_price,
        last_updated = NOW()
    WHERE asset_id = NEW.asset_id
      AND quantity  > 0;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_refresh_mkt_value ON market_prices;
CREATE TRIGGER trg_refresh_mkt_value
    AFTER INSERT ON market_prices
    FOR EACH ROW EXECUTE FUNCTION refresh_mkt_value();

-- VIEWS: Analytics

-- Real-time portfolio valuation (holdings × latest price)
CREATE MATERIALIZED VIEW mv_portfolio_valuation AS
SELECT
    p.portfolio_id,
    p.name          AS portfolio_name,
    u.username,
    a.asset_type,
    a.symbol,
    h.quantity,
    h.avg_cost,
    latest.close_price                                        AS current_price,
    ROUND(h.quantity * latest.close_price, 2)                AS market_value,
    ROUND((h.quantity * latest.close_price)
        - (h.quantity * h.avg_cost), 2)                      AS unrealised_pnl,
    ROUND(((latest.close_price - h.avg_cost)
        / NULLIF(h.avg_cost, 0)) * 100, 2)                   AS return_pct
FROM holdings h
JOIN portfolios p  ON p.portfolio_id = h.portfolio_id
JOIN users u       ON u.user_id      = p.user_id
JOIN assets a      ON a.asset_id     = h.asset_id
LEFT JOIN LATERAL (
    SELECT close_price
    FROM market_prices
    WHERE asset_id = h.asset_id
    ORDER BY trade_date DESC
    LIMIT 1
) latest ON true
WHERE h.quantity > 0;

CREATE UNIQUE INDEX ON mv_portfolio_valuation (portfolio_id, symbol);

-- Risk exposure by asset class
CREATE OR REPLACE VIEW v_risk_exposure AS
SELECT
    p.portfolio_id,
    u.username,
    a.asset_type,
    ROUND(SUM(h.quantity * mp.close_price), 2)   AS asset_class_value,
    ROUND(SUM(SUM(h.quantity * mp.close_price))
          OVER (PARTITION BY p.portfolio_id), 2)  AS total_portfolio_value,
    ROUND(
        100.0 * SUM(h.quantity * mp.close_price)
        / NULLIF(SUM(SUM(h.quantity * mp.close_price))
                 OVER (PARTITION BY p.portfolio_id), 0),
	2)                                            AS exposure_pct
FROM holdings h
JOIN portfolios p ON p.portfolio_id = h.portfolio_id
JOIN users u      ON u.user_id      = p.user_id
JOIN assets a     ON a.asset_id     = h.asset_id
LEFT JOIN LATERAL (
    SELECT close_price
    FROM market_prices
    WHERE asset_id = h.asset_id
    ORDER BY trade_date DESC LIMIT 1
) mp ON true
WHERE h.quantity > 0
GROUP BY p.portfolio_id, u.username, a.asset_type;

-- STORED PROCEDURE: Automated Rebalancing Check

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
            v_action := format(
                'REDUCE %s by %.1f%% — sell ~$%s worth',
                upper(rec.asset_type),
                rec.exposure_pct - v_limit,
                ROUND((rec.exposure_pct - v_limit) / 100.0
                      * rec.total_portfolio_value, 2)
            );

            INSERT INTO rebalance_log
                (portfolio_id, username, asset_type,
                 current_pct, limit_pct, excess_pct, suggested_action)
            VALUES
                (rec.portfolio_id, rec.username, rec.asset_type,
                 rec.exposure_pct, v_limit,
                 rec.exposure_pct - v_limit, v_action);
        END IF;
    END LOOP;
    RAISE NOTICE 'Rebalancing check done. Check rebalance_log for alerts.';
END;
$$;

-- Insert the 3 assets from CSV file

-- NVDA → stock
INSERT INTO assets (asset_type, name, symbol, currency)
VALUES ('stock', 'NVIDIA Corporation', 'NVDA', 'USD');

INSERT INTO stocks (asset_id, ticker, sector, exchange)
SELECT asset_id, 'NVDA', 'Technology', 'NASDAQ'
FROM assets WHERE symbol = 'NVDA';

-- TLT → bond ETF
INSERT INTO assets (asset_type, name, symbol, currency)
VALUES ('bond', 'iShares 20+ Year Treasury Bond ETF', 'TLT', 'USD');

INSERT INTO bonds (asset_id, coupon_rate, issuer, credit_rating)
SELECT asset_id, 0.0000, 'US Treasury / BlackRock', 'AAA'
FROM assets WHERE symbol = 'TLT';

-- BTC-USD → crypto
INSERT INTO assets (asset_type, name, symbol, currency)
VALUES ('crypto', 'Bitcoin', 'BTC-USD', 'USD');

INSERT INTO cryptos (asset_id, coingecko_id, blockchain, max_supply)
SELECT asset_id, 'bitcoin', 'Bitcoin', 21000000
FROM assets WHERE symbol = 'BTC-USD';

