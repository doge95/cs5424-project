# Payment Transaction consists of one line of input with five comma-separated values: P,C W ID,C D ID,C ID,PAYMENT.

# This transaction processes a payment made by a customer. Inputs:
# 1. Customer identifier (C W ID, C D ID, C ID)
# 2. Payment amount PAYMENT 
# Processing steps:
# 1. Update the warehouse C W ID by incrementing W YTD by PAYMENT
# 2. Update the district (C W ID,C D ID) by incrementing D YTD by PAYMENT 
# 3. Update the customer (C W ID, C D ID, C ID) as follows:
# • Decrement C BALANCE by PAYMENT
# • Increment C YTD PAYMENT by PAYMENT 
# • Increment C PAYMENT CNT by 1
# Output the following information:
# 1. Customer’s identifier (C W ID, C D ID, C ID), name (C FIRST, C MIDDLE, C LAST), address (C STREET 1, C STREET 2, C CITY, C STATE, C ZIP), C PHONE, C SINCE, C CREDIT, C CREDIT LIM, C DISCOUNT, C BALANCE
# 2. Warehouse’s address (W STREET 1, W STREET 2, W CITY, W STATE, W ZIP)
# 3. District’s address (D STREET 1, D STREET 2, D CITY, D STATE, D ZIP)
# 4. Payment amount PAYMENT

# When executing UPDATE statements from an application, make sure that you wrap the SQL-executing functions in a retry loop that handles transaction errors that can occur under contention.

from read_script import *
import logging
import psycopg2
from psycopg2.errors import SerializationFailure

def payment (conn, cwid, cdid, cid, payment):
    with conn.cursor() as cur:
        # Update warehouse
        cur.execute(
            "UPDATE warehouse SET W_YTD = W_YTD + %s WHERE W_ID = %s", (payment, cwid)
        )

        # Update district
        cur.execute(
            "UPDATE district SET D_YTD = D_YTD + %s WHERE D_W_ID = %s AND D_ID = %s", (payment, cwid, cdid)
        )

        # Update customer
        cur.execute(
            "UPDATE customer SET (C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT) = (C_BALANCE - %s, C_YTD_PAYMENT + %s, C_PAYMENT_CNT + %s) WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", (payment, payment, 1, cwid, cdid, cid)
        )
        # try catch? rollback?
        conn.commit()
    
        # Retrieve customer
        cur.execute(
            "SELECT * FROM customer WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", (cwid, cdid, cid)
        )
        customer = cur.fetchone()
        print("Customer Identifier: ", *customer[:3])
        print("Name: ", *customer[3:6])
        print("Address:", *customer[6:11])
        print("Phone: ", customer[11])
        print("Since: ", customer[12])
        print("Credit: ", customer[13])
        print("Credit Limit: ", customer[14])
        print("Discount: ", customer[15])
        print("Balance: ", customer[16])

        # Retrieve warehouse 
        cur.execute(
            "SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM warehouse WHERE W_ID = %s", (cwid,)
        )
        warehouse_address = cur.fetchone()
        print("Warehouse Address:", *warehouse_address)

        # Retrieve district 
        cur.execute(
            "SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM district WHERE D_ID = %s", (cdid,)
        )
        district_address = cur.fetchone()
        print("District Address:", *district_address)
        
        # logging first or commit first?
        logging.debug("payment(): status message: %s", cur.statusmessage)
    conn.commit()

# def main():
#     print("main")
#     payment(conn, 2, 2, 307, 0)
#     conn.close()

# if __name__ == "__main__":
#     main()