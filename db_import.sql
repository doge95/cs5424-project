TRUNCATE stock, order_line, item, orders, customer, district, warehouse;

IMPORT into warehouse(w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) CSV DATA ("http://127.0.0.1:8001/warehouse.csv");

IMPORT into district(D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID) CSV DATA ("http://127.0.0.1:8001/district.csv");

IMPORT into customer(C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE,  C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT,  C_DATA) CSV DATA ('http://127.0.0.1:8001/customeraa', 'http://127.0.0.1:8001/customerab', 'http://127.0.0.1:8001/customerac', 'http://127.0.0.1:8001/customerad', 'http://127.0.0.1:8001/customerae');

IMPORT INTO orders (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) CSV DATA ('http://127.0.0.1:8001/orderaa', 'http://127.0.0.1:8001/orderab', 'http://127.0.0.1:8001/orderac', 'http://127.0.0.1:8001/orderad', 'http://127.0.0.1:8001/orderae') with nullif = 'null';

IMPORT INTO item (I_ID, I_NAME, I_PRICE, I_IM_ID, I_DATA) CSV DATA ("http://127.0.0.1:8001/item.csv");

IMPORT into order_line(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) CSV DATA ('http://127.0.0.1:8001/orderlineaa', 'http://127.0.0.1:8001/orderlineab', 'http://127.0.0.1:8001/orderlineac', 'http://127.0.0.1:8001/orderlinead', 'http://127.0.0.1:8001/orderlineae') with nullif = 'null';

IMPORT into stock(S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04,  S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_DATA) CSV DATA ('http://127.0.0.1:8001/stockaa', 'http://127.0.0.1:8001/stockab', 'http://127.0.0.1:8001/stockac', 'http://127.0.0.1:8001/stockad', 'http://127.0.0.1:8001/stockae');