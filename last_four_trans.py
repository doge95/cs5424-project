# 5.
def get_stock_level_transaction(conn, w_id, d_id, t, l):
    data_cursor = conn.cursor()

    get_next_order_query = "select D_NEXT_O_ID from district where d_w_id={} and d_id={};".format(w_id, d_id)

    data_cursor.execute(get_next_order_query)
    # Get a list of order number from last L orders
    d_next_o_id = data_cursor.fetchone()[0]

    # Convert a list of integer to a list of string
    d_next_o_id_str = [str(i) for i in list(range(d_next_o_id - l, d_next_o_id))]
    in_scope_o_ids = "(" + ",".join(d_next_o_id_str) + ")"

    # 1. select items in last L order in order_line table
    # 2. select count from stock
    get_count_unique_items_query = "select count(*) from stock where s_w_id={} and s_quantity<{} and s_i_id in (" \
                                   "select distinct ol_i_id from order_line where ol_w_id={} and ol_d_id={} and ol_o_id in {}" \
                                   ");".format(w_id, t, w_id, d_id, in_scope_o_ids)
    data_cursor.execute(get_count_unique_items_query)
    count_items = data_cursor.fetchone()

    # print(count_items)
    # print('type: ', type(count_items))
    # print ('here')
    print(count_items[0])
    return count_items[0]


# get_stock_level_transaction('S',1,9,13,37)


# 6
def get_popular_items_transaction(conn, w_id, d_id, l):
    data_cursor = conn.cursor()

    get_next_order_query = "select D_NEXT_O_ID from district where d_w_id={} and d_id={};".format(w_id, d_id)

    data_cursor.execute(get_next_order_query)
    # 1. Get a list of order number from last L orders
    d_next_o_id = data_cursor.fetchone()[0]

    # Convert a list of integer to a list of string
    d_next_o_id_str = [str(i) for i in list(range(d_next_o_id - l, d_next_o_id))]
    in_scope_o_ids = "(" + ",".join(d_next_o_id_str) + ")"

    # 2. get popular item for each

    # 2.1 get popular items per order
    get_popular_items_per_order_query = "select a.ol_o_id, b.ol_i_id, b.ol_w_id, b.ol_d_id, max_ol_quantity from " \
                                        "(select ol_o_id, ol_w_id, ol_d_id, max(ol_quantity) as max_ol_quantity " \
                                        "from order_line where ol_w_id={} and ol_d_id={} and ol_o_id in {} " \
                                        "group by (ol_o_id, ol_w_id, ol_d_id)) a " \
                                        "inner join order_line b " \
                                        "on (a.OL_W_ID=b.OL_W_ID and a.OL_D_ID=b.OL_D_ID and a.OL_O_ID=b.OL_O_ID) " \
                                        "where a.max_ol_quantity=b.ol_quantity".format(w_id, d_id, in_scope_o_ids)

    # 2.2 enrich with item, orders and customer info
    get_full_details_query = "select ol_o_id, ol_i_id, ol_w_id, ol_d_id, max_ol_quantity, " \
                             "i_name, c_first, c_middle, c_last, o_entry_d " \
                             "from ({}) p left join orders " \
                             "on (p.ol_o_id=O_ID and p.ol_w_id=O_W_ID and p.ol_d_id=O_D_ID) " \
                             "left join item " \
                             "on p.ol_i_id=i_id " \
                             "left join customer " \
                             "on (o_c_id=c_id and ol_w_id=c_w_id and ol_d_id=c_d_id ); " \
                             "".format(get_popular_items_per_order_query)

    data_cursor.execute(get_full_details_query)
    popular_items_per_order = data_cursor.fetchall()

    # 2.3 Get unique list of popular items
    unique_popular_items = list({str(i[1]) for i in popular_items_per_order})
    unique_popular_item_ids = "(" + ",".join(unique_popular_items) + ")"

    # 2.4 count the order based on item, and divided by total number of orders in scope
    get_all_items_per_selected_order_query = "select i_name, COUNT( DISTINCT ol_o_id)/{} " \
                                             "from order_line " \
                                             "left join item " \
                                             "on ol_i_id=i_id " \
                                             "where ol_w_id={} and ol_d_id={} and ol_o_id in {} and ol_i_id in {} " \
                                             "group by i_name;".format(l, w_id, d_id, in_scope_o_ids,
                                                                       unique_popular_item_ids)

    data_cursor.execute(get_all_items_per_selected_order_query)
    popular_items_percentage = data_cursor.fetchall()

    # print(popular_items_percentage)
    # print('type: ', type(popular_items_percentage))

    orders = []
    for pop_record in popular_items_per_order:
        ol_o_id, ol_i_id, ol_w_id, ol_d_id, max_ol_quantity, i_name, c_first, c_middle, c_last, o_entry_d = pop_record
        orders.append({
            "ol_o_id": ol_o_id,
            "o_entry_d": o_entry_d,
            "c_first": c_first,
            "c_middle": c_middle,
            "c_last": c_last,
            "i_name": i_name,
            "ol_quantity": max_ol_quantity,
        })

    percentages = []
    for perc_record in popular_items_percentage:
        i_name, perc = perc_record
        percentages.append({
            'i_name': i_name,
            'percentage': perc
        })
    results = {
        'w_id': w_id,
        'd_id': d_id,
        'l': l,
        'orders': orders,
        'percentages': percentages
    }
    print(results)
    return results


# get_popular_items_transaction('I',1,9,37)


# 7
def get_top_balance_transaction(conn):
    data_cursor = conn.cursor()

    # get_top_ten_customers = "select c_first, c_middle, c_last, c_balance, w_name, d_name from customer " \
    #                         "join district on c_d_id = d_id " \
    #                         "join warehouse on c_w_id = w_id " \
    #                         "order by c_balance desc limit 10;"

    get_top_ten_customers = "select c_first, c_middle, c_last, c_balance, w_name, d_name from " \
                            "(select c_first, c_middle, c_last, c_balance, c_d_id, c_w_id from customer order by c_balance desc limit 10)  " \
                            "join district on c_d_id = d_id and d_w_id = c_w_id " \
                            "join warehouse on c_w_id = w_id;"

    data_cursor.execute(get_top_ten_customers)
    selected_customers_info = data_cursor.fetchall()

    results = []
    for record in selected_customers_info:
        # print(record)
        # print("c_first:{}, c_middle:{}, c_last:{}, c_balance:{}, w_name:{}, d_name:{}".format(*record))
        c_first, c_middle, c_last, c_balance, w_name, d_name = record
        results.append({
            'c_first': c_first,
            'c_middle': c_middle,
            'c_last': c_last,
            'c_balance': c_balance,
            'w_name': w_name,
            'd_name': d_name,
        })

    print(results)
    return results


# get_top_balance_transaction('T')


# 8
def get_related_customer_transaction(conn, c_w_id, c_d_id, c_id):
    data_cursor = conn.cursor()

    # 1. Get customers associated with different warehouses
    # get_in_scope_customers = "select distinct c_id, c_w_id, c_d_id from customer where c_w_id!={};".format(c_w_id)
    # select * from orders join customer on O_W_ID=c_w_id and O_D_ID=c_d_id and o_c_id=c_id where c_w_id!=1;

    # data_cursor.execute(get_in_scope_customers)
    # custs_w_diff_warehouse = data_cursor.fetchall() #c_id, c_w_id, c_d_id
    # print ('custs_w_diff_warehouse', custs_w_diff_warehouse)

    # 2. Get Orders for base customer
    # get_orders_for_input_customers = "select o_w_id, o_d_id, o_id from orders where o_w_id={} and o_d_id={} and o_c_id ={};".format(c_w_id, c_d_id, c_id)
    #
    # data_cursor.execute(get_orders_for_input_customers)
    # base_orders = data_cursor.fetchall()
    # print('base_orders', base_orders)

    # 3

    get_orders_for_selected_customers = "select * from order_line inner join " \
                                        "(select o_w_id, o_d_id, o_id, o_c_id from orders where o_w_id!={}) " \
                                        "on ol_w_id=o_w_id and ol_d_id=o_d_id and ol_o_id=o_id " \
        .format(c_w_id)

    # get all ol for input customers
    get_orders_for_input_customers = "select * from order_line inner join " \
                                     "(select o_w_id, o_d_id, o_id from orders where o_w_id={} and o_d_id={} and o_c_id ={})" \
                                     " on ol_w_id=o_w_id and ol_d_id=o_d_id and ol_o_id=o_id ".format(c_w_id, c_d_id,
                                                                                                      c_id)

    get_overlap_items = "select selected.ol_w_id, selected.ol_d_id, selected.ol_o_id, selected.o_c_id, count(distinct selected.ol_i_id) as count_items from ({}) selected " \
                        "inner join ({}) base on base.ol_i_id=selected.ol_i_id " \
                        "group by " \
                        "selected.ol_w_id, selected.ol_d_id, selected.ol_o_id, selected.o_c_id " \
                        "HAVING count(distinct selected.ol_i_id)>=2;" \
        .format(get_orders_for_selected_customers, get_orders_for_input_customers)

    data_cursor.execute(get_overlap_items)
    orders_with_similar_items = data_cursor.fetchall()  # o_w_id, o_d_id, o_id, o_c_id
    related_custs = []
    for record in orders_with_similar_items:
        c_w_id, c_d_id, o_id, c_id, count = record
        related_custs.append({
            'c_w_id': c_w_id,
            'c_d_id': c_d_id,
            'c_id': c_id,
        })
    # print (orders_with_similar_items)
    results = {
        "c_w_id": c_w_id,
        "c_d_id": c_d_id,
        "c_id": c_id,
        "related_custs": related_custs
    }
    print(results)
    return results

# get_related_customer_transaction("R", 1, 9, 1658)

# conn = psycopg2.connect(
#     database='wholesale',
#     user='root',
#     sslmode='verify-full',
#     sslrootcert='../certs/ca.crt',
#     sslcert='../certs/client.root.crt',
#     sslkey='../certs/client.root.key',
#     port=26278,
#     host='xcnd45.comp.nus.edu.sg',
#     password='cs4224hadmin'
# )
