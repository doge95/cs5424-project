import psycopg2
import csv
import os
import datetime
from re import search
from read_script import *
from statistics import mean, median
import numpy as np


# from DBConnection import DBConnection

def process_transactions(temp_data, data_cursor):
    for line in temp_data:
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
data_cursor = conn.cursor()

throughput_for_all = []
for file in list_of_files[:4]:
    print('This is file ', file)
    f = open(transaction_file + file, "r")
    # append each line in the file to a list
    temp_data = f.read().splitlines()

    clients_performance = []
    num_of_trxn = 0
    total_trxn_time = 0
    trxn_latency_lst = []
    for line in temp_data[:4]:
        input_params = line.split(',')
        print(input_params)

        start = datetime.datetime.now()
        process_transactions(input_params, data_cursor)
        time_diff = (datetime.datetime.now() - start).total_seconds()
        total_trxn_time += total_trxn_time + time_diff
        trxn_latency_lst.append(time_diff * 1000)
        num_of_trxn = num_of_trxn + 1

    client_throughput = round(num_of_trxn / total_trxn_time, 2)
    throughput_for_all.append(client_throughput)
    trxn_latency_ndarr_dist = np.array(trxn_latency_lst)
    clients_performance.append([
        num_of_trxn,
        round(total_trxn_time, 2),
        client_throughput,
        round(mean(trxn_latency_lst), 2),
        round(median(trxn_latency_lst), 2),
        round(np.percentile(trxn_latency_ndarr_dist, 95), 2),
        round(np.percentile(trxn_latency_ndarr_dist, 99), 2)
    ])

    count += len(temp_data)
    lines.extend(temp_data)
    f.close()

output_fir = '/Users/qianzhang/Documents/2021_2022_term1/cs5424/project/project_test_data/'

# Export client.csv
print('clients_performance', clients_performance)
with open(output_fir + 'clients.csv', 'w') as csvfile:
    # creating a csv writer object
    csvwriter = csv.writer(csvfile)
    csvwriter.writerows(clients_performance)

# Export throughput.csv
with open(output_fir + 'throughput.csv', 'w') as csvfile:
    # creating a csv writer object
    csvwriter = csv.writer(csvfile)
    csvwriter.writerows([[
        min(throughput_for_all),
        max(throughput_for_all),
        round(mean(throughput_for_all), 2)]
    ])

# Export dbstate.csv
# i. select sum(W YTD) from Warehouse
# ii. select sum(D YTD), sum(D NEXT O ID) from District
# iii. select sum(C BALANCE), sum(C YTD PAYMENT), sum(C PAYMENT CNT), sum(C DELIVERY CNT)
# from Customer
# iv. select max(O ID), sum(O OL CNT) from Order
# v. select sum(OL AMOUNT), sum(OL QUANTITY) from Order-Line
# vi. select sum(S QUANTITY), sum(S YTD), sum(S ORDER CNT), sum(S REMOTE CNT) from
# Stock

sum_from_warehouse_query = 'select sum(W_YTD) from Warehouse;'
data_cursor.execute(sum_from_warehouse_query)
(sum_W_YTD,) = data_cursor.fetchone()
print(sum_W_YTD)

sum_from_district_query = 'select sum(D_YTD), sum(D_NEXT_O_ID) from District;'
data_cursor.execute(sum_from_district_query)
(sum_D_YTD, sum_D_NEXT_O_ID) = data_cursor.fetchone()
print(sum_D_YTD, sum_D_NEXT_O_ID)

sum_from_customer_query = 'select sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) from Customer;'
data_cursor.execute(sum_from_customer_query)
(sum_C_BALANCE, sum_C_YTD_PAYMENT, sum_C_PAYMENT_CNT, sum_C_DELIVERY_CNT) = data_cursor.fetchone()
print(sum_C_BALANCE, sum_C_YTD_PAYMENT, sum_C_PAYMENT_CNT, sum_C_DELIVERY_CNT)

sum_from_order_query = 'select max(O_ID), sum(O_OL_CNT) from Orders;'
data_cursor.execute(sum_from_order_query)
(max_O_ID, sum_O_OL_CNT) = data_cursor.fetchone()
print(max_O_ID, sum_O_OL_CNT)

sum_from_order_line_query = 'select sum(OL_AMOUNT), sum(OL_QUANTITY) from Order_Line;'
data_cursor.execute(sum_from_order_line_query)
(sum_OL_AMOUNT, sum_OL_QUANTITY) = data_cursor.fetchone()
print(sum_OL_AMOUNT, sum_OL_QUANTITY)

sum_from_stock_query = 'select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from Stock;'
data_cursor.execute(sum_from_stock_query)
(sum_S_QUANTITY, sum_S_YTD, sum_S_ORDER_CNT, sum_S_REMOTE_CNT) = data_cursor.fetchone()
print(sum_S_QUANTITY, sum_S_YTD, sum_S_ORDER_CNT, sum_S_REMOTE_CNT)

with open(output_fir + 'dbstate.csv', 'w') as csvfile:
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
