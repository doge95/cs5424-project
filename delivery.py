# Delivery Transaction consists of one line of input with three comma-separated values: D,W ID,CARRIER ID.

# This transaction is used to process the delivery of the oldest yet-to-be-delivered order for each of the 10 districts in a specified warehouse.
# Inputs:
# 1. Warehouse number W ID
# 2. Identifier of carrier assigned for the delivery CARRIER ID 
# Processing steps:
# 1. For DISTRICT NO = 1 to 10
# (a) Let N denote the value of the smallest order number O ID for district (W ID,DISTRICT NO)
# with O CARRIER ID = null; i.e.,
# N =min{t.O ID∈Order|t.O W ID =W ID, t.D ID =DISTRICT NO,t.O CARRIER ID=null}
# Let X denote the order corresponding to order number N, and let C denote the customer who placed this order
# (b) Update the order X by setting O CARRIER ID to CARRIER ID
# (c) Update all the order-lines in X by setting OL DELIVERY D to the current date and time
# (d) Update customer C as follows:
# • Increment C BALANCE by B, where B denote the sum of OL AMOUNT for all the items placed in order X
# • Increment C DELIVERY CNT by 1