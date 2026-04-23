# Portfolio Management Database
A PostgreSQL-based portfolio management system that tracks user investments across stocks, bonds, and crypto. Built around real market data (NVDA, TLT, BTC-USD) with automatic rebalancing alerts.
# What It Does
The system lets users hold multiple portfolios, buy/sell assets, and automatically tracks their cash balance, holdings, and unrealised P&L. It also flags when a portfolio's asset allocation drifts beyond the user's risk tolerance limits.<br>
**Main features:** <br>
-ISA (class hierarchy) design for assets — stocks, bonds, and crypto all share a common assets table but have their own subtype tables with specific attributes<br>
-Triggers handle cash deduction on buy, cash return on sell, and holdings update automatically — no manual steps needed<br>
-A materialized view (mv_portfolio_valuation) gives a full snapshot of current positions with P&L<br>
-A stored procedure (run_rebalancing_check) compares each user's actual allocation against their risk profile and logs alerts<br>
-BRIN index on market_prices for efficient date-range queries on the time-series data; partial index for the most recent prices<br>
# Files
Schema.sql    — Full schema: tables, indexes, triggers, views, stored procedure, and initial asset data<br>
schema2.sql   — Sample data: 3 users (alice, bob, charlie) with transactions and verification queries<br>
check1.sql    — Operational checks: holdings, valuation, risk exposure, rebalance alerts, EXPLAIN ANALYZE<br>
check2.sql    — Schema inspection queries and full data dump for verification<br>
market_data.csv    — Combined historical price data for NVDA, TLT, and BTC-USD.<br>
# How to Run
-Step 1: Initialize Schema<br>
Run **Schema.sql** in PostgreSQL Query Tool (pgAdmin 4). This script creates the asset hierarchy (ISA), indexes, and the automated triggers for cash and holdings management.<br>
-Step 2: Import Market Data<br>
Import the market_data.csv file into the market_prices table.<br>
Via pgAdmin 4: Right-click market_prices -> Import/Export Data -> Select Import -> Choose the file path -> Ensure columns match.<br>
-Step 3: Load Sample Data<br>
Run **schema2.sql** This will:<br>
a.Create 3 users (Alice, Bob, Charlie) with specific risk tolerances.<br>
b.Execute a series of buy/sell transactions.<br>
c.Note: One transaction for "Charlie" is intentionally designed to fail with an "Insufficient balance" error to demonstrate the trigger validation.
# Verification & Testing
Run **check1.sql** to verify the following features:<br>
-Automated Holdings: Check how triggers updated the holdings table and calculated avg_cost without manual input.<br>
-Portfolio Valuation: Refresh the materialized view (REFRESH MATERIALIZED VIEW mv_portfolio_valuation;) to see real-time P&L.<br>
-Risk Alerts: Run the rebalancing procedure (CALL run_rebalancing_check();) to generate automated sell suggestions in the rebalance_log.<br>
-Performance: View the EXPLAIN ANALYZE output to see the BRIN index in action for date-range queries.<br>
Run **check2.sql** to inspect the internal database state:<br>
-Metadata Review: Displays column types and constraints for all tables.<br>
-ISA Integrity: Verifies that asset subtypes (stocks/bonds/crypto) correctly reference the parent assets table.<br>
-Trigger & Index Status: Lists all active triggers and index definitions (including the Partial Index for recent prices) to ensure the environment is fully optimized.
