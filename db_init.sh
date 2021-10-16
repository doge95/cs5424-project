#!/bin/sh 
# This script is to create database and/ or import data into database
#set -x

USER="cs4224h"
HOME_DIR="/home/stuproj/cs4224h"
MAIN_SERVER="xcnd45"
SERVERS="xcnd45 xcnd46 xcnd47 xcnd48 xcnd49"
PORT="26278"
# Shared storage among servers
PROJECT_DATA_DIR="$HOME_DIR/project_files_4/data_files"
COCKROACH_DATA_DIR="$HOME_DIR/cockroach_data_files"
COCKROACH="/temp/cs4224h/cockroach-v21.1.7.linux-amd64/cockroach"
CERT_DIR="/temp/cs4224h/certs"
QUERY_OUTPUT="db_query.out"

prepare_files () {
    cp "$PROJECT_DATA_DIR/district.csv" $COCKROACH_DATA_DIR
    cp "$PROJECT_DATA_DIR/warehouse.csv" $COCKROACH_DATA_DIR
    cp "$PROJECT_DATA_DIR/item.csv" $COCKROACH_DATA_DIR
    split -l 60000 "$PROJECT_DATA_DIR/customer.csv" "$COCKROACH_DATA_DIR/customer"
    split -l 60000 "$PROJECT_DATA_DIR/order.csv" "$COCKROACH_DATA_DIR/order"
    split -l 750000 "$PROJECT_DATA_DIR/order-line.csv" "$COCKROACH_DATA_DIR/orderline"
    split -l 200000 "$PROJECT_DATA_DIR/stock.csv" "$COCKROACH_DATA_DIR/stock"
}

start_file_servers () {
    for server in $SERVERS; do 
        echo "Start python file server on $server ..."
        if [[ "$server" == "$MAIN_SERVER" ]]; then
            cd $COCKROACH_DATA_DIR; python3 -m http.server 8001 &> /dev/null &
        else 
            ssh -q $USER@$server "cd $COCKROACH_DATA_DIR; python3 -m http.server 8001 &> /dev/null &"
        fi
    done
}

stop_file_servers () {
    for server in $SERVERS; do 
        if [[ "$server" == "$MAIN_SERVER" ]]; then
            ps -ef | grep 'http.server 8001' | grep -v 'grep' | awk '{print $2}' | xargs kill 
        else
            echo "Stop python file server on $server ..."
            ssh -q $USER@$server "ps -ef | grep 'http.server 8001' | grep -v 'grep' | awk '{print \$2}' | xargs kill"
        fi
    done
}

check_db() {
    $COCKROACH sql --certs-dir=$CERT_DIR --host=$MAIN_SERVER:$PORT --execute='USE wholesale; show tables;' --format=raw &> $QUERY_OUTPUT
}

create_database () {
    $COCKROACH sql --certs-dir=$CERT_DIR  --host=$MAIN_SERVER:$PORT --execute='CREATE DATABASE IF NOT EXISTS wholesale;
USE wholesale;

DROP TABLE IF EXISTS stock;
DROP TABLE IF EXISTS order_line;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS district;
DROP TABLE IF EXISTS warehouse;

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

CREATE TABLE IF NOT EXISTS orders
(
    O_W_ID       INTEGER       NOT NULL,
    O_D_ID       INTEGER       NOT NULL,
    O_ID         INTEGER       NOT NULL,
    O_C_ID       INTEGER       NOT NULL,
    O_CARRIER_ID INTEGER,
    O_OL_CNT     DECIMAL(2, 0),
    O_ALL_LOCAL  DECIMAL(1, 0),
    O_ENTRY_D    TIMESTAMP,
    PRIMARY KEY  (O_W_ID, O_D_ID, O_ID),
    FOREIGN KEY  (O_W_ID, O_D_ID, O_C_ID) REFERENCES customer (C_W_ID, C_D_ID, C_ID),
    FAMILY o_txn_info (O_ID, O_W_ID, O_D_ID, O_C_ID, O_CARRIER_ID),
    FAMILY o_meta (O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D)
);

CREATE TABLE IF NOT EXISTS item
(
    I_ID         INTEGER            NOT NULL,
    I_NAME       VARCHAR(24)        NOT NULL,
    I_PRICE      DECIMAL(5, 2)      NOT NULL,
    I_IM_ID      INTEGER,
    I_DATA       VARCHAR(50),
    PRIMARY KEY  (I_ID),
    FAMILY       i_key_info (I_ID, I_NAME, I_PRICE),
    FAMILY       i_meta (I_IM_ID, I_DATA)
);

CREATE TABLE IF NOT EXISTS order_line
(
    OL_W_ID        INTEGER     NOT NULL,
    OL_D_ID        INTEGER     NOT NULL,
    OL_O_ID        INTEGER     NOT NULL,
    OL_NUMBER      INTEGER     NOT NULL,
    OL_I_ID        INTEGER     NOT NULL REFERENCES item (I_ID),
    OL_DELIVERY_D  TIMESTAMP,
    OL_AMOUNT      DECIMAL(6, 2),
    OL_SUPPLY_W_ID INTEGER,
    OL_QUANTITY    DECIMAL(2, 0),
    OL_DIST_INFO   CHAR(24),
    PRIMARY KEY    (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER),
    FOREIGN KEY    (OL_W_ID, OL_D_ID, OL_O_ID) REFERENCES orders (O_W_ID, O_D_ID, O_ID),
    FAMILY         ol_key_info (OL_NUMBER, OL_O_ID, OL_W_ID, OL_D_ID, OL_DELIVERY_D, OL_I_ID, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY),
    FAMILY         ol_meta (OL_DIST_INFO)
);

CREATE TABLE IF NOT EXISTS stock
(
    S_W_ID       INTEGER       NOT NULL REFERENCES warehouse (W_ID),
    S_I_ID       INTEGER       NOT NULL REFERENCES item (I_ID),
    S_QUANTITY   DECIMAL(4,0)  NOT NULL DEFAULT 0,
    S_YTD        DECIMAL(8,2)  NOT NULL DEFAULT 0.00,
    S_ORDER_CNT  INTEGER       NOT NULL DEFAULT 0,
    S_REMOTE_CNT INTEGER       NOT NULL DEFAULT 0,
    S_DIST_01    CHAR(24),
    S_DIST_02    CHAR(24),
    S_DIST_03    CHAR(24),
    S_DIST_04    CHAR(24),
    S_DIST_05    CHAR(24),
    S_DIST_06    CHAR(24),
    S_DIST_07    CHAR(24),
    S_DIST_08    CHAR(24),
    S_DIST_09    CHAR(24),
    S_DIST_10    CHAR(24),
    S_DATA       VARCHAR(50),
    PRIMARY KEY  (S_W_ID, S_I_ID),
    FAMILY       s_txn_info (S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT),
    FAMILY       s_meta (S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_DATA)
);'
}

import_data () {
    $COCKROACH sql --certs-dir=$CERT_DIR  --host=$MAIN_SERVER:$PORT --execute='USE wholesale;

TRUNCATE stock, order_line, item, orders, customer, district, warehouse;

IMPORT into warehouse(w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) CSV DATA ("http://127.0.0.1:8001/warehouse.csv");

IMPORT into district(D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID) CSV DATA ("http://127.0.0.1:8001/district.csv");

IMPORT into customer(C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE,  C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT,  C_DATA) CSV DATA ("http://127.0.0.1:8001/customeraa", "http://127.0.0.1:8001/customerab", "http://127.0.0.1:8001/customerac", "http://127.0.0.1:8001/customerad", "http://127.0.0.1:8001/customerae");

IMPORT INTO orders (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) CSV DATA ("http://127.0.0.1:8001/orderaa", "http://127.0.0.1:8001/orderab", "http://127.0.0.1:8001/orderac", "http://127.0.0.1:8001/orderad", "http://127.0.0.1:8001/orderae") with nullif = "null";

IMPORT INTO item (I_ID, I_NAME, I_PRICE, I_IM_ID, I_DATA) CSV DATA ("http://127.0.0.1:8001/item.csv");

IMPORT into order_line(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) CSV DATA ("http://127.0.0.1:8001/orderlineaa", "http://127.0.0.1:8001/orderlineab", "http://127.0.0.1:8001/orderlineac", "http://127.0.0.1:8001/orderlinead", "http://127.0.0.1:8001/orderlineae") with nullif = "null";

IMPORT into stock(S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04,  S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_DATA) CSV DATA ("http://127.0.0.1:8001/stockaa", "http://127.0.0.1:8001/stockab", "http://127.0.0.1:8001/stockac", "http://127.0.0.1:8001/stockad", "http://127.0.0.1:8001/stockae");'
}

if [ ! -d $HOME_DIR ]; then mkdir -p $HOME_DIR; fi
if [ ! -d $COCKROACH_DATA_DIR ]; then mkdir -p $COCKROACH_DATA_DIR; fi

# Download project files
if [ ! -d $PROJECT_DATA_DIR ]; then
    echo "Downloading project files..."
    wget -q https://www.comp.nus.edu.sg/~cs4224/project_files_4.zip && unzip -d $HOME_DIR project_files_4.zip && rm project_files_4.zip
    echo Done
fi

# Prepare cockcroach data files for DB import
if [ ! -f "$COCKROACH_DATA_DIR/district.csv" ]; then
    echo "Preparing cockroach data files..."
    prepare_files
    echo Done
fi

# Check database & tables
echo "Checking database & tables..."
check_db

# If wholesale database does not exit, create the database
if grep -q 'database "wholesale" does not exist' "$QUERY_OUTPUT"; then
    echo "Creating database..."
    create_database
else
    # If wholesale database exits, import data into the database
    echo "Open python file server on each node..."
    start_file_servers

    echo "Import data into database..."
    import_data

    echo "Stop python file server on each node..."
    stop_file_servers

fi
