import psycopg2
import csv
import os
from re import search
from read_script import *
# from DBConnection import DBConnection

conn = psycopg2.connect(
    database='wholesale',
    user='root',
    sslmode='verify-full',
    sslrootcert='../certs/ca.crt',
    sslcert='../certs/client.root.crt',
    sslkey='../certs/client.root.key',
    port=26278,
    host='xcnd45.comp.nus.edu.sg',
    password='cs4224hadmin'
)

transaction_file = '/Users/qianzhang/Documents/2021_2022_term1/cs5424/project/project_files/xact_files_A/'
list_of_files = os.listdir(transaction_file)
lines = []
count = 0
for file in list_of_files:
    f = open(transaction_file + file, "r")
    # append each line in the file to a list
    temp_data = f.read().splitlines()
    count += len(temp_data)
    lines.extend(temp_data)
    f.close()

data_cursor = conn.cursor()
for line in lines[:3]:
    input_params = line.split(',')
    print(input_params)

    if input_params[0] == 'n':
        pass
    elif input_params[0] == 'p':
        pass
    elif input_params[0] == 'd':
        pass
    elif input_params[0] == 'o':
        pass
    elif input_params[0] == 's':
        get_stock_level_transaction(data_cursor,
                                    int(input_params[1]),
                                    int(input_params[2]),
                                    int(input_params[3]),
                                    int(input_params[4]),
                                    )
    elif input_params[0] == 'i':
        get_popular_items_transaction(data_cursor,
                                      int(input_params[1]),
                                      int(input_params[2]),
                                      int(input_params[3])
                                      )
    elif input_params[0] == 't':
        get_top_balance_transaction(data_cursor)
    elif input_params[0] == 'r':
        get_related_customer_transaction(data_cursor,
                                         int(input_params[1]),
                                         int(input_params[2]),
                                         int(input_params[3]))


output_fir = '/Users/qianzhang/Documents/2021_2022_term1/cs5424/project/project_test_data/'

# Export dbstate.csv
# i. select sum(W YTD) from Warehouse
# ii. select sum(D YTD), sum(D NEXT O ID) from District
# iii. select sum(C BALANCE), sum(C YTD PAYMENT), sum(C PAYMENT CNT), sum(C DELIVERY CNT)
# from Customer
# iv. select max(O ID), sum(O OL CNT) from Order
# v. select sum(OL AMOUNT), sum(OL QUANTITY) from Order-Line
# vi. select sum(S QUANTITY), sum(S YTD), sum(S ORDER CNT), sum(S REMOTE CNT) from
# Stock

sum_from_warehouse_query='select sum(W_YTD) from Warehouse;'
data_cursor.execute(sum_from_warehouse_query)
(sum_W_YTD,) = data_cursor.fetchone()
print(sum_W_YTD)

sum_from_district_query='select sum(D_YTD), sum(D_NEXT_O_ID) from District;'
data_cursor.execute(sum_from_district_query)
(sum_D_YTD, sum_D_NEXT_O_ID) = data_cursor.fetchone()
print(sum_D_YTD, sum_D_NEXT_O_ID)

sum_from_customer_query='select sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) from Customer;'
data_cursor.execute(sum_from_customer_query)
(sum_C_BALANCE, sum_C_YTD_PAYMENT, sum_C_PAYMENT_CNT, sum_C_DELIVERY_CNT) = data_cursor.fetchone()
print(sum_C_BALANCE, sum_C_YTD_PAYMENT, sum_C_PAYMENT_CNT, sum_C_DELIVERY_CNT)

sum_from_order_query='select max(O_ID), sum(O_OL_CNT) from Orders;'
data_cursor.execute(sum_from_order_query)
(max_O_ID, sum_O_OL_CNT) = data_cursor.fetchone()
print(max_O_ID, sum_O_OL_CNT)

sum_from_order_line_query='select sum(OL_AMOUNT), sum(OL_QUANTITY) from Order_Line;'
data_cursor.execute(sum_from_order_line_query)
(sum_OL_AMOUNT, sum_OL_QUANTITY) = data_cursor.fetchone()
print(sum_OL_AMOUNT, sum_OL_QUANTITY)

sum_from_stock_query='select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from Stock;'
data_cursor.execute(sum_from_stock_query)
(sum_S_QUANTITY, sum_S_YTD, sum_S_ORDER_CNT, sum_S_REMOTE_CNT) = data_cursor.fetchone()
print(sum_S_QUANTITY, sum_S_YTD, sum_S_ORDER_CNT, sum_S_REMOTE_CNT)

with open(output_fir+'dbstate.csv', 'w') as csvfile:
    # creating a csv writer object
    csvwriter = csv.writer(csvfile)
    csvwriter.writerows([
        [sum_W_YTD],
        [sum_D_YTD], [sum_D_NEXT_O_ID],
        [sum_C_BALANCE], [sum_C_YTD_PAYMENT], [sum_C_PAYMENT_CNT], [sum_C_DELIVERY_CNT],
        [max_O_ID], [sum_O_OL_CNT],
        [sum_OL_AMOUNT], [sum_OL_QUANTITY],
        [sum_S_QUANTITY], [sum_S_YTD], [sum_S_ORDER_CNT], [sum_S_REMOTE_CNT]
    ])