CREATE DATABASE IF NOT EXISTS wholesale;
USE wholesale;

DROP TABLE IF EXISTS warehouse;
DROP TABLE IF EXISTS district;
DROP TABLE IF EXISTS customer;

CREATE TABLE IF NOT EXISTS warehouse
(
    W_ID INTEGER NOT NULL,
    W_NAME VARCHAR(10),
    W_STREET_1 VARCHAR(20),
    W_STREET_2 VARCHAR(20),
    W_CITY VARCHAR(20),
    W_STATE CHAR(2),
    W_ZIP CHAR(9),
    W_TAX DECIMAL(4,4),
    W_YTD DECIMAL(12,2) DEFAULT 0.00,
    PRIMARY KEY (W_ID),
    FAMILY     w_txn_amt(W_ID, W_YTD),
    FAMILY     w_txn_tax(W_TAX),
    FAMILY     w_meta(W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP)
);

CREATE TABLE IF NOT EXISTS district
(
    D_W_ID      INTEGER NOT NULL REFERENCES warehouse (W_ID),
    D_ID        INTEGER NOT NULL,
    D_NAME      VARCHAR(10),
    D_STREET_1  VARCHAR(20),
    D_STREET_2  VARCHAR(20),
    D_CITY      VARCHAR(20),
    D_STATE     CHAR(2),
    D_ZIP       CHAR(9),
    D_TAX       DECIMAL(4, 4),
    D_YTD       DECIMAL(12, 2) DEFAULT 0.00,
    D_NEXT_O_ID INTEGER DEFAULT 1,
    PRIMARY KEY (D_W_ID, D_ID),
    FAMILY      d_txn_amt(D_ID, D_W_ID, D_YTD),
    FAMILY      d_order_info(D_NEXT_O_ID),
    FAMILY      d_txn_tax(D_TAX),
    FAMILY      d_meta(D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP)
);

CREATE TABLE IF NOT EXISTS customer
(
    C_W_ID         INTEGER NOT NULL,
    C_D_ID         INTEGER NOT NULL,
    C_ID           INTEGER NOT NULL,
    C_FIRST        VARCHAR(16),
    C_MIDDLE       CHAR(2),
    C_LAST         VARCHAR(16),
    C_STREET_1     VARCHAR(20),
    C_STREET_2     VARCHAR(20),
    C_CITY         VARCHAR(20),
    C_STATE        CHAR(2),
    C_ZIP          CHAR(9),
    C_PHONE        CHAR(16),
    C_SINCE        TIMESTAMP DEFAULT NOW(),
    C_CREDIT       CHAR(2),
    C_CREDIT_LIM   DECIMAL(12, 2),
    C_DISCOUNT     DECIMAL(4, 4),
    C_BALANCE      DECIMAL(12, 2)DEFAULT 0.00,
    C_YTD_PAYMENT  FLOAT DEFAULT 0.00,
    C_PAYMENT_CNT  INTEGER DEFAULT 0,
    C_DELIVERY_CNT INTEGER DEFAULT 0,
    C_DATA         VARCHAR(500),
    PRIMARY KEY (C_W_ID, C_D_ID, C_ID),
    FOREIGN KEY (C_W_ID, C_D_ID) references district (D_W_ID, D_ID),
    FAMILY         c_txn_info(C_ID, C_W_ID, C_D_ID, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT),
    FAMILY         c_full_name(C_FIRST, C_MIDDLE, C_LAST),
    FAMILY         c_full_address(C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP),
    FAMILY         c_meta(C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT),
    FAMILY         c_static_data(C_DATA)
);
