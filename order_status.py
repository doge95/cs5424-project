# This transaction queries the status of the last order of a customer. 
# Input: Customer identifier (C_W_ID, C_D_ID, C_ID)

import logging

def order_status (conn, cwid, cdid, cid):
    with conn.cursor() as cur:
        # Output Customer’s name (C_FIRST, C_MIDDLE, C_LAST), balance C_BALANCE
        cur.execute(
            "SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM customer WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", (cwid, cdid, cid)
        )
        # logging.debug("order_status(): status message: %s", cur.statusmessage)
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
    logging.debug("order_status(): status message: %s", cur.statusmessage)

# def main():
#     order_status(conn, 10, 2, 2783)
#     conn.close()

# if __name__ == "__main__":
#     main()