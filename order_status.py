# Order-Status Transaction consists of one line of input with four comma-separated values: O,C W ID,C D ID,C ID.

# This transaction queries the status of the last order of a customer. 
# Input: Customer identifier (C W ID, C D ID, C ID)
# Output the following information:
# 1. Customer’s name (C FIRST, C MIDDLE, C LAST), balance C BALANCE 
# 2. For the customer’s last order
# (a) Order number O ID
# (b) Entry date and time O ENTRY D
# (c) Carrier identifier O CARRIER ID
# 3. For each item in the customer’s last order
# (a) Item number OL I ID
# (b) Supplying warehouse number OL SUPPLY W ID
# (c) Quantity ordered OL QUANTITY
# (d) Total price for ordered item OL AMOUNT
# (e) Data and time of delivery OL DELIVERY D

from read_script import *
import logging
import psycopg2
from psycopg2.errors import SerializationFailure

def order_status (conn, cwid, cdid, cid):
    with conn.cursor() as cur:
        # Retrieve customer's name and balance
        cur.execute(
            "SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM customer WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", (cwid, cdid, cid)
        )
        # logging.debug("order_status(): status message: %s", cur.statusmessage)
        customer_info = cur.fetchone()
        print("Customer Name:", *customer_info[:3])
        print("Balance:", customer_info[3])

        # Retrieve customer's last order
        cur.execute(
            "SELECT O_ID, O_ENTRY_D, O_CARRIER_ID FROM orders WHERE O_W_ID = %s AND O_D_ID = %s AND O_C_ID = %s ORDER BY O_ENTRY_D DESC LIMIT 1", (cwid, cdid, cid)
        )
        last_order = cur.fetchone()
        last_order_id = last_order[0]

        print("Customer Last Order Number:", last_order_id)
        print("Entry Date and Time:", last_order[1])
        print("Carrier Identifier:", last_order[2])

        # Retrieve order lines from the last order
        cur.execute(
            "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM order_line WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s", (cwid, cdid, last_order_id)
        )
        order_lines = cur.fetchall()
        print("Item Number, Supplying Warehouse Number, Quantity Ordered, Total Price, Delivery Date and Time")
        for order_line in order_lines:
            print(*order_line)
        logging.debug("order_status(): status message: %s", cur.statusmessage)
        conn.commit()

# def main():
#     print("main")
#     order_status(conn, 10, 2, 2783)
#     conn.close()

# if __name__ == "__main__":
#     main()