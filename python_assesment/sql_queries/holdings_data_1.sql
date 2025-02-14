CREATE TABLE IF NOT EXISTS test.holdings_data_1 (
    business_date DATE,
    portfolio_id varchar(10),
    security_id varchar(10),
    exchange VARCHAR(50),
    quantity INT,
    market_value DECIMAL(15, 2),
    currency VARCHAR(10)
);