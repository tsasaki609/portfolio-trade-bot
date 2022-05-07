CREATE DATABASE crypto_history;

CREATE TABLE crypto_history.bitflyerfx_ohlcv_btcjpy_1m (
    time DATETIME PRIMARY KEY,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE NOT NULL
);

CREATE TABLE crypto_history.bybit_ohlcv_btcusdt_1m (
    time DATETIME PRIMARY KEY,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE NOT NULL
);
