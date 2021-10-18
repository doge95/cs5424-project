from psycopg2 import sql

def new_order (conn, cid, wid, did, num_items, items):
    with conn.cursor() as cur:
        # Retrieve the next available order number D_NEXT_O_ID and tax for district
        cur.execute(
            "SELECT D_NEXT_O_ID FROM district WHERE D_W_ID = %s AND D_ID = %s", (wid, did)
        )
        oid = cur.fetchone()[0]

        # Update the district by incrementing D_NEXT_O_ID by one
        cur.execute(
            "UPDATE district SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = %s AND D_ID = %s", (wid, did)
        )

        # Create a new order
        # O_ALL_LOCAL = 0 if there exists some i∈[1,NUM ITEMS] such that SUPPLIER_WAREHOUSE[i] ̸= W_ID; otherwise, O_ALL_LOCAL = 1
        item_nums_str, supply_w_ids_str, quantities_str = list(zip(*items))
        item_nums = [int(x) for x in item_nums_str]
        supply_w_ids = [int(x) for x in supply_w_ids_str]
        quantities = [int(x) for x in quantities_str]

        all_local = all(supply_w_id == wid for supply_w_id in supply_w_ids)
        o_all_local = 1 if all_local else 0
        cur.execute(
            "INSERT INTO orders (O_W_ID, O_D_ID, O_ID, O_C_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) VALUES (%s, %s, %s, %s, %s, %s, current_timestamp())", (wid, did, oid, cid, num_items, o_all_local)
        )

        total_amount = 0
        item_names = []
        s_quantities = []
        ol_amounts = []

        for i in range(len(item_nums)):
            iid = item_nums[i]
            s_wid = supply_w_ids[i]
            qty = quantities[i]

            # Retrieve the stock quantity for item ITEM_NUMBER[i] and warehouse SUPPLIER_WAREHOUSE[i]
            cur.execute(
                "SELECT S_QUANTITY FROM stock WHERE S_W_ID = %s AND S_I_ID = %s", (s_wid, iid)
            )
            sqty = cur.fetchone()[0]
            s_quantities.append(sqty)

            adjusted_sqty = sqty - qty
            if adjusted_sqty < 10: adjusted_sqty += 100

            # Update the stock
            # Update S_QUANTITY to ADJUSTED_QTY
            # Increment S_YTD by QUANTITY[i]
            # Increment S_ORDER_CNT by 1
            # Increment S_REMOTE_CNT by 1 if SUPPLIER_WAREHOUSE[i] ̸= W_ID
            if s_wid == wid:
                cur.execute(
                    "UPDATE stock SET (S_QUANTITY, S_YTD, S_ORDER_CNT) = (%s, S_YTD + %s, S_ORDER_CNT + 1) WHERE S_W_ID = %s AND S_I_ID = %s", (adjusted_sqty, qty, s_wid, iid)
                )
            else:
                cur.execute(
                    "UPDATE stock SET (S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT) = (%s, S_YTD + %s, S_ORDER_CNT + 1, S_REMOTE_CNT + 1) WHERE S_W_ID = %s AND S_I_ID = %s", (adjusted_sqty, qty, s_wid, iid)
                )

            # ITEM_AMOUNT = QUANTITY[i] × I_PRICE, where I_PRICE is the price of ITEM NUMBER[i]
            cur.execute(
                "SELECT I_NAME, I_PRICE FROM item WHERE I_ID = %s", (iid, )
            )
            i_name, i_price = cur.fetchone()
            item_amount = qty * i_price
            total_amount += item_amount
            item_names.append(i_name)
            ol_amounts.append(item_amount)

            # Create a new order-line
            s_dist = "s_dist_" + str(did).zfill(2)
            cur.execute(
                sql.SQL("INSERT INTO order_line (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, (SELECT {} FROM stock WHERE S_W_ID = %s AND S_I_ID = %s))").format(sql.Identifier(s_dist)), [wid, did, oid, i + 1, iid, item_amount, s_wid, qty, s_wid, iid]
            )

        # TOTAL_AMOUNT = TOTAL_AMOUNT × (1+D_TAX+W_TAX) × (1−C_DISCOUNT)
        cur.execute(
            "SELECT W_TAX FROM warehouse WHERE W_ID = %s", (wid, )
        )
        wtax = cur.fetchone()[0]

        cur.execute(
            "SELECT D_TAX FROM district WHERE D_W_ID = %s AND D_ID = %s", (wid, did)
        )
        dtax = cur.fetchone()[0]

        cur.execute(
            "SELECT C_DISCOUNT FROM customer WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", (wid, did, cid)
        )
        cdisct = cur.fetchone()[0]

        total_amount = total_amount * (1 + wtax + dtax) * (1 - cdisct)

        # Output Customer identifier (W ID, D ID, C ID), lastname C_LAST, credit C_CREDIT, discount C_DISCOUNT
        cur.execute(
            "SELECT C_W_ID, C_D_ID, C_ID, C_LAST, C_CREDIT FROM customer WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", (wid, did, cid)
        )
        customer = cur.fetchone()
        customer_id = customer[:3]
        customer_info = customer[3:]
        print("Customer Identifier, Last Name, Credit, Discount")
        print(customer_id, *customer_info, cdisct, sep=", ")

        # Warehouse tax rate W_TAX, District tax rate D_TAX
        print("Warehouse Tax Rate, District Tax Rate")
        print(wtax, dtax, sep=", ")

        # Order number O_ID, entry date O_ENTRY_D
        cur.execute(
            "SELECT O_ENTRY_D FROM orders WHERE O_W_ID = %s AND O_D_ID = %s AND O_ID = %s", (wid, did, oid)
        )
        o_entry_d = cur.fetchone()[0]
        print("Order Number, Entry Date")
        print(oid, o_entry_d, sep=", ")

        # Number of items NUM_ITEMS, Total amount for order TOTAL_AMOUNT
        print("Number of Items, Total Amount for Order")
        print(num_items, total_amount, sep=", ")

        #  For each ordered item ITEM_NUMBER[i], output ITEM NUMBER[i], I_NAME, SUPPLIER_WAREHOUSE[i], QUANTITY[i], OL_AMOUNT, S_QUANTITY
        print("Ordered Item Number, Item Name, Supplier Warehouse, Quantity, Total Price, Quantity in Stock")
        ordered_items = list(zip(range(1, num_items + 1), item_names, supply_w_ids, quantities, ol_amounts, s_quantities))
        for ol in ordered_items:
            print(*ol, sep=", ")

    conn.commit()

def payment (conn, cwid, cdid, cid, payment):
    with conn.cursor() as cur:
        # # Update the warehouse by incrementing W_YTD by PAYMENT
        cur.execute(
            "UPDATE warehouse SET W_YTD = W_YTD + %s WHERE W_ID = %s", (payment, cwid)
        )

        # # Update the district by incrementing D_YTD by PAYMENT
        cur.execute(
            "UPDATE district SET D_YTD = D_YTD + %s WHERE D_W_ID = %s AND D_ID = %s", (payment, cwid, cdid)
        )

        # # Update customer
        # # Decrement C_BALANCE by PAYMENT
        # # Increment C_YTD PAYMENT by PAYMENT
        # # Increment C_PAYMENT CNT by 1
        cur.execute(
            "UPDATE customer SET (C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT) = (C_BALANCE - %s, C_YTD_PAYMENT + %s, C_PAYMENT_CNT + 1) WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", (payment, payment, cwid, cdid, cid)
        )
    
        # Output customer
        cur.execute(
            "SELECT C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE FROM customer WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", (cwid, cdid, cid)
        )
        customer = cur.fetchone()
        customer_id = customer[:3]
        customer_name = customer[3:6]
        customer_address = customer[6:11]
        customer_info = customer[11:]
        print("Customer Identifier, Name, Address, Phone, Since, Credit, Credit Limit, Discount, Balance")
        print(customer_id, customer_name, customer_address, *customer_info, sep=", ")

        # Output warehouse 
        cur.execute(
            "SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM warehouse WHERE W_ID = %s", (cwid,)
        )
        warehouse_address = cur.fetchone()
        print("Warehouse Address:", *warehouse_address)

        # Output district 
        cur.execute(
            "SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM district WHERE D_ID = %s", (cdid,)
        )
        district_address = cur.fetchone()
        print("District Address:", *district_address)
        
    conn.commit()

def delivery (conn, wid, carrierid):
    for did in range(1, 11):
        with conn.cursor() as cur:
            # Retrieve the smallest order number O_ID for district with O_CARRIER_ID = null
            cur.execute(
                "SELECT O_ID, O_C_ID FROM orders WHERE O_W_ID = %s AND O_D_ID = %s AND O_CARRIER_ID IS NULL order by O_ID ASC LIMIT 1", (wid, did)
            )
            oid, cid = cur.fetchone()

            # Update the order X by setting O_CARRIER_ID to CARRIER_ID
            cur.execute(
                "UPDATE orders SET O_CARRIER_ID = %s WHERE O_ID = %s AND O_W_ID = %s AND O_D_ID = %s", (carrierid, oid, wid, did)
            )

            # Update all the order-lines in X by setting OL_DELIVERY_D to the current date and time
            cur.execute(
                "UPDATE order_line SET OL_DELIVERY_D = current_timestamp() WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s", (wid, did, oid)
            )

            # Update customer C
            # Increment C_BALANCE by B, where B denote the sum of OL_AMOUNT for all the items placed in order X
            # Increment C_DELIVERY_CNT by 1
            cur.execute(
                "UPDATE customer SET (C_BALANCE, C_DELIVERY_CNT) = (C_BALANCE + (SELECT SUM(OL_AMOUNT) FROM order_line WHERE OL_W_ID = %s AND OL_D_ID = %s and OL_O_ID = %s), C_DELIVERY_CNT + 1) WHERE C_ID = %s AND C_W_ID = %s AND C_D_ID = %s", (wid, did, oid, cid, wid, did)
            )
            
        conn.commit()

def order_status (conn, cwid, cdid, cid):
    with conn.cursor() as cur:
        # Output Customer’s name (C_FIRST, C_MIDDLE, C_LAST), balance C_BALANCE
        cur.execute(
            "SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM customer WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", (cwid, cdid, cid)
        )
        # print("order_status(): status message: %s", cur.statusmessage)
        customer_info = cur.fetchone()
        customer_name = customer_info[:3]
        balance = customer_info[3]
        print("Customer Name, Balance")
        print(customer_name, balance, sep=", ")

        # Output Order number O_ID, Entry date and time O_ENTRY_D, Carrier identifier O_ CARRIER_ID
        cur.execute(
            "SELECT O_ID, O_ENTRY_D, O_CARRIER_ID FROM orders WHERE O_W_ID = %s AND O_D_ID = %s AND O_C_ID = %s ORDER BY O_ENTRY_D DESC LIMIT 1", (cwid, cdid, cid)
        )
        last_order = cur.fetchone()
        last_order_id = last_order[0]

        print("Customer Last Order Number, Entry Date and Time, Carrier Identifier")
        print(*last_order, sep=", ")

        # Output Item number OL_I_ID, Supplying warehouse number OL_SUPPLY_W_ID, Quantity ordered OL_QUANTITY, Total price for ordered item OL_AMOUNT, Data and time of delivery OL_DELIVERY_D
        cur.execute(
            "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM order_line WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s", (cwid, cdid, last_order_id)
        )
        order_lines = cur.fetchall()
        print("Item Number, Supplying Warehouse Number, Quantity Ordered, Total Price, Delivery Date and Time")
        for order_line in order_lines:
            print(*order_line, sep=", ")
    
    conn.commit()