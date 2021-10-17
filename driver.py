import psycopg2
import csv
import sys
import datetime
from statistics import mean, median
import numpy as np
import logging
from first_four_trans import *
from last_four_trans import *


def process_transactions(input_params, conn):
    if input_params[0] == 'N':
        print("New Order ", input_params)
        new_order(conn,
                  int(input_params[1]),
                  int(input_params[2]),
                  int(input_params[3]),
                  int(input_params[4]),
                  input_params[5],
                  )

    elif input_params[0] == 'P':
        print("Payment ", input_params)
        payment(conn,
                int(input_params[1]),
                int(input_params[2]),
                int(input_params[3]),
                float(input_params[4]),
                )

    elif input_params[0] == 'D':
        print("Delivery ", input_params)
        delivery(conn, int(input_params[1]), int(input_params[2]))

    elif input_params[0] == 'O':
        print("Order Status ", input_params)
        order_status(conn, int(input_params[1]), int(input_params[2]), int(input_params[3]))

    elif input_params[0] == 'S':
        print("Stock Level ", input_params)
        get_stock_level_transaction(conn,
                                    int(input_params[1]),
                                    int(input_params[2]),
                                    int(input_params[3]),
                                    int(input_params[4]),
                                    )
    elif input_params[0] == 'I':
        print("Popular Item ", input_params)
        get_popular_items_transaction(conn,
                                      int(input_params[1]),
                                      int(input_params[2]),
                                      int(input_params[3])
                                      )
    elif input_params[0] == 'T':
        print("Top Balance ", input_params)
        get_top_balance_transaction(conn)

    elif input_params[0] == 'R':
        print("Related Customer ", input_params)
        get_related_customer_transaction(conn,
                                         int(input_params[1]),
                                         int(input_params[2]),
                                         int(input_params[3]))


transaction_file = sys.argv[1]
output_fir = sys.argv[2]
host = sys.argv[3]

conn = psycopg2.connect(
    database='wholesale',
    user='root',
    sslmode='verify-full',
    sslrootcert='/temp/cs4224h/certs/ca.crt',
    sslcert='/temp/cs4224h/certs/client.root.crt',
    sslkey='/temp/cs4224h/certs/client.root.key',
    # sslrootcert='../certs/ca.crt',
    # sslcert='../certs/client.root.crt',
    # sslkey='../certs/client.root.key',
    port=26278,
    host=host,
    password='cs4224hadmin'
)

# transaction_file = '/Users/ruiyan/Desktop/MSc/SEM1AY2021:2022/CS5424DD/project/project_files_4/xact_files_A/0.txt'
throughput_for_all = []
clients_performance = []

# Get Client Number
char_position=transaction_file.rfind('.txt')
client_num = transaction_file[char_position - 2:char_position].replace('/', '')

f = open(transaction_file, "r")
# append each line in the file to a list
temp_data = f.read().splitlines()
num_of_trxn = 0
total_trxn_time = 0
trxn_latency_lst = []

for line_num in range(len(temp_data)):
    line = temp_data[line_num]
    input_params = line.split(',')
    # print(input_params)
    if input_params[0] == 'N':
        num_items = int(input_params[4])
        items = []
        for i in range(line_num + 1, line_num + num_items + 1):
            items.append(temp_data[i].split(','))
        input_params.append(items)
        # print(input_params)
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
    except Exception as e:
        conn.rollback()
        logging.debug("General Exception: %s", e)
        continue

client_throughput = 0 if total_trxn_time == 0 else round(num_of_trxn / total_trxn_time, 2)
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
f.close()

conn.close()


# Export client.csv
# print('clients_performance', clients_performance)
print(clients_performance, file=sys.stderr)
with open(output_fir + 'clients_' + client_num + '.csv', 'w') as csvfile:
    # creating a csv writer object
    csvwriter = csv.writer(csvfile)
    csvwriter.writerows(clients_performance)

# Export throughput.csv

throughput_data_frag = [[
    min(throughput_for_all),
    max(throughput_for_all),
    round(mean(throughput_for_all), 2)]
]
print(throughput_data_frag, file=sys.stderr)
with open(output_fir + 'throughput_' + client_num + '.csv', 'w') as csvfile:
    # creating a csv writer object
    csvwriter = csv.writer(csvfile)
    csvwriter.writerows(
        #     [[
        #     min(throughput_for_all),
        #     max(throughput_for_all),
        #     round(mean(throughput_for_all), 2)]
        # ]
        throughput_data_frag
    )