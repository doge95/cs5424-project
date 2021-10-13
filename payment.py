# This transaction processes a payment made by a customer.
# Inputs:
# 1. Customer identifier (C_W_ID, C_D_ID, C_ID)
# 2. Payment amount PAYMENT 

# When executing UPDATE statements from an application, make sure that you wrap the SQL-executing functions in a retry loop that handles transaction errors that can occur under contention.
import logging

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
    logging.debug("payment(): status message: %s", cur.statusmessage)

# def main():
#     payment(conn, 2, 2, 307, 0)
#     conn.close()

# if __name__ == "__main__":
#     main()