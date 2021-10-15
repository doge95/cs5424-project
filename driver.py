import psycopg2
import csv
import os
import datetime
from re import search
from read_script import *
from statistics import mean, median
import numpy as np
import logging
from delivery import *
from new_order import *
from order_status import *
from payment import *

# from DBConnection import DBConnection

def process_transactions(input_params, conn):
    if input_params[0] == 'N':
        new_order(conn, 
                int(input_params[1]),
                int(input_params[2]),
                int(input_params[3]),
                int(input_params[4]),
                input_params[5],
                )
        global new_order_count
        new_order_count += 1
    elif input_params[0] == 'P':
        payment(conn, 
                int(input_params[1]),
                int(input_params[2]),
                int(input_params[3]),
                float(input_params[4]),
                )
        global payment_count
        payment_count += 1
        
    elif input_params[0] == 'D':
        delivery(conn, int(input_params[1]), int(input_params[2]))
        global delivery_count
        delivery_count += 1

    elif input_params[0] == 'O':
        order_status(conn, int(input_params[1]), int(input_params[2]), int(input_params[3]))
        global order_status_count
        order_status_count += 1

    elif input_params[0] == 'S':
        get_stock_level_transaction(conn,
                                    int(input_params[1]),
                                    int(input_params[2]),
                                    int(input_params[3]),
                                    int(input_params[4]),
                                    )
    elif input_params[0] == 'I':
        get_popular_items_transaction(conn,
                                      int(input_params[1]),
                                      int(input_params[2]),
                                      int(input_params[3])
                                      )
    elif input_params[0] == 'T':
        get_top_balance_transaction(conn)
    elif input_params[0] == 'R':
        get_related_customer_transaction(conn,
                                         int(input_params[1]),
                                         int(input_params[2]),
                                         int(input_params[3]))

def main(transaction_file, output_fir):
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

    # transaction_file = '/Users/ruiyan/Desktop/MSc/SEM1AY2021:2022/CS5424DD/project/project_files/xact_files_A/0.txt'
    # list_of_files = os.listdir(transaction_file)
    lines = []
    count = 0
    # data_cursor = conn.cursor()

    throughput_for_all = []
    clients_performance = []

    new_order_count = 0
    delivery_count = 0
    payment_count = 0
    order_status_count = 0
    # Get Client Number
    len_input_file = len(transaction_file)
    client_num = transaction_file[len_input_file - 6:len_input_file - 4].replace('/', '')

    # no need for production, prof's shell script will do
    # for file in list_of_files:
    for i in range(1):
        # f = open(transaction_file + file, "r")
        f = open(transaction_file, "r")
        # append each line in the file to a list
        temp_data = f.read().splitlines()

        num_of_trxn = 0
        total_trxn_time = 0
        trxn_latency_lst = []
        for line_num in range(len(temp_data)):
            line = temp_data[line_num]
            input_params = line.split(',')
            print(input_params)

            if input_params[0] == 'N':
                num_items = int(input_params[4])
                items = []
                for i in range(line_num + 1, line_num + num_items + 1):
                    items.append(temp_data[i].split(','))
                input_params.append(items)
                print(input_params)

            try:
                start = datetime.datetime.now()
                process_transactions(input_params, conn)
                time_diff = (datetime.datetime.now() - start).total_seconds()
                total_trxn_time += total_trxn_time + time_diff
                trxn_latency_lst.append(time_diff * 1000)
                num_of_trxn = num_of_trxn + 1

            except psycopg2.Error as e:
                conn.rollback()
                logging.debug("Exception: %s", e)
                continue

        client_throughput = round(num_of_trxn / total_trxn_time, 2)
        throughput_for_all.append(client_throughput)
        trxn_latency_ndarr_dist = np.array(trxn_latency_lst)
        client_performance_record = [
            client_num,
            num_of_trxn,
            round(total_trxn_time, 2),
            client_throughput,
            round(mean(trxn_latency_lst), 2),
            round(median(trxn_latency_lst), 2),
            round(np.percentile(trxn_latency_ndarr_dist, 95), 2),
            round(np.percentile(trxn_latency_ndarr_dist, 99), 2)
        ]
        # print('This is file ', file, client_performance_record)
        clients_performance.append(client_performance_record)

        count += len(temp_data)
        lines.extend(temp_data)
        f.close()

    conn.close()

    # print("new_order_count: ", new_order_count)
    # print("delivery_count: ", delivery_count)
    # print("payment_count: ", payment_count)
    # print("order_status_count: ", order_status_count)

    # output_fir = '/Users/ruiyan/Desktop/MSc/SEM1AY2021:2022/CS5424DD/project/project_files/xact_files_A/'
    # output_fir = '/home/stuproj/cs4224h/cockroach_output/'

    # Export client.csv
    print('clients_performance', clients_performance)

    with open(output_fir + 'clients.csv', 'a') as csvfile:
        # creating a csv writer object
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(clients_performance)

    # Export throughput.csv
    with open(output_fir + 'throughput.csv', 'a') as csvfile:
        # creating a csv writer object
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow([[
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

    # sum_from_warehouse_query = 'select sum(W_YTD) from Warehouse;'
    # data_cursor.execute(sum_from_warehouse_query)
    # (sum_W_YTD,) = data_cursor.fetchone()
    # print(sum_W_YTD)

    # sum_from_district_query = 'select sum(D_YTD), sum(D_NEXT_O_ID) from District;'
    # data_cursor.execute(sum_from_district_query)
    # (sum_D_YTD, sum_D_NEXT_O_ID) = data_cursor.fetchone()
    # print(sum_D_YTD, sum_D_NEXT_O_ID)

    # sum_from_customer_query = 'select sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) from Customer;'
    # data_cursor.execute(sum_from_customer_query)
    # (sum_C_BALANCE, sum_C_YTD_PAYMENT, sum_C_PAYMENT_CNT, sum_C_DELIVERY_CNT) = data_cursor.fetchone()
    # print(sum_C_BALANCE, sum_C_YTD_PAYMENT, sum_C_PAYMENT_CNT, sum_C_DELIVERY_CNT)

    # sum_from_order_query = 'select max(O_ID), sum(O_OL_CNT) from Orders;'
    # data_cursor.execute(sum_from_order_query)
    # (max_O_ID, sum_O_OL_CNT) = data_cursor.fetchone()
    # print(max_O_ID, sum_O_OL_CNT)

    # sum_from_order_line_query = 'select sum(OL_AMOUNT), sum(OL_QUANTITY) from Order_Line;'
    # data_cursor.execute(sum_from_order_line_query)
    # (sum_OL_AMOUNT, sum_OL_QUANTITY) = data_cursor.fetchone()
    # print(sum_OL_AMOUNT, sum_OL_QUANTITY)

    # sum_from_stock_query = 'select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from Stock;'
    # data_cursor.execute(sum_from_stock_query)
    # (sum_S_QUANTITY, sum_S_YTD, sum_S_ORDER_CNT, sum_S_REMOTE_CNT) = data_cursor.fetchone()
    # print(sum_S_QUANTITY, sum_S_YTD, sum_S_ORDER_CNT, sum_S_REMOTE_CNT)

    # with open(output_fir + 'dbstate.csv', 'w') as csvfile:
    #     # creating a csv writer object
    #     csvwriter = csv.writer(csvfile)
    #     csvwriter.writerows([
    #         [sum_W_YTD],
    #         [sum_D_YTD], [sum_D_NEXT_O_ID],
    #         [sum_C_BALANCE], [sum_C_YTD_PAYMENT], [sum_C_PAYMENT_CNT], [sum_C_DELIVERY_CNT],
    #         [max_O_ID], [sum_O_OL_CNT],
    #         [sum_OL_AMOUNT], [sum_OL_QUANTITY],
    #         [sum_S_QUANTITY], [sum_S_YTD], [sum_S_ORDER_CNT], [sum_S_REMOTE_CNT]
    #     ])
