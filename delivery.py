# This transaction is used to process the delivery of the oldest yet-to-be-delivered order for each of the 10 districts in a specified warehouse.
# Inputs:
# 1. Warehouse number W_ID
# 2. Identifier of carrier assigned for the delivery CARRIER_ID 

import logging

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
        logging.debug("delivery(): status message: %s", cur.statusmessage)
        
# def main():
#     delivery(conn, 2, 8)
#     conn.close()

# if __name__ == "__main__":
#     main()