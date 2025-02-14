CREATE TABLE IF NOT EXISTS test.portfolio_stats_2 (
    business_date DATE,
    portfolio_id varchar(10),
    nav DECIMAL(15, 2),
    daily_pnl DECIMAL(15, 2),
    ytd_return DECIMAL(5, 4)
);