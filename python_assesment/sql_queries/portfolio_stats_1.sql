CREATE TABLE IF NOT EXISTS test.portfolio_stats_1 (
    business_date DATE,
    portfolio_id varchar(10),
    nav DECIMAL(15, 2),
    daily_pnl DECIMAL(15, 2),
    ytd_return VARCHAR(10),
    sharpe_ratio DECIMAL(10, 4),
    volatility DECIMAL(10, 4),
    var_95 DECIMAL(15, 2)
);
