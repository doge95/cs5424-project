# This transaction processes a new order from a customer.
# Inputs:
# 1. Customer identifier (W ID, D ID, C ID)
# 2. Number of items to be ordered NUM ITEMS, NUM ITEMS ≤ 20
# 3. ITEM NUMBER[i] = Item number for ith item, i ∈ [1,NUM ITEMS]
# 4. SUPPLIER WAREHOUSE[i] = Supplier warehouse for ith item, i ∈ [1,NUM ITEMS] 
# 5. QUANTITY[i] = Quantity ordered for ith item, i ∈ [1,NUM ITEMS]

from read_script import *
import logging
import psycopg2
from psycopg2 import sql
from psycopg2.errors import SerializationFailure

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
        item_nums, supply_w_ids, quantities = list(zip(*items))
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

    logging.debug("new_order(): status message: %s", cur.statusmessage)
    conn.commit()


# def main():
#     items = [[63203,3,4], [64033,3,4], [76455,3,2]]
#     new_order(conn, 190, 3, 1, 3, items)
#     conn.close()

# if __name__ == "__main__":
#     main()