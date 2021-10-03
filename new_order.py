# New Order Transaction consists of M+1 lines, where M denote the number of items in the new order. The first line consists of five comma-separated values: N,C ID,W ID,D ID, M. Each of the M remaining lines specifies an item in the order and consists of three comma-separated values: OL I ID,OL SUPPLY W ID,OL QUANTITY.

# Inputs:
# 1. Customer identifier (W ID, D ID, C ID)
# 2. Number of items to be ordered NUM ITEMS, NUM ITEMS ≤ 20
# 3. ITEM NUMBER[i] = Item number for ith item, i ∈ [1,NUM ITEMS]
# 4. SUPPLIER WAREHOUSE[i] = Supplier warehouse for ith item, i ∈ [1,NUM ITEMS] 5. QUANTITY[i] = Quantity ordered for ith item, i ∈ [1,NUM ITEMS]
# Processing steps:
# 1. Let N denote value of the next available order number D NEXT O ID for district (W ID,D ID) 2. Update the district (W ID, D ID) by incrementing D NEXT O ID by one
# 3. Create a new order with
# • O ID = N
# • ODID=DID
# • OWID=WID
# • OCID=CID
# • O ENTRY D = Current date and time
# • O CARRIER ID = null
# • OOLCNT=NUMITEMS
# • O ALL LOCAL=0ifthereexistssomei∈[1,NUM ITEMS]suchthatSUPPLIER WAREHOUSE[i] ̸= W ID; otherwise, O ALL LOCAL = 1
# 4. Initialize TOTAL AMOUNT = 0 5. Fori=1toNUMITEMS
# (a) Let S QUANTITY denote the stock quantity for item ITEM NUMBER[i] and warehouse SUPPLIER WAREHOUSE[i]
# (b) ADJUSTED QTY = S QUANTITY − QUANTITY [i] 5
# (c) If ADJUSTED QTY < 10, then set ADJUSTED QTY = ADJUSTED QTY + 100 (d) Update the stock for (ITEM NUMBER[i], SUPPLIER WAREHOUSE[i]) as follows:
# • Update S QUANTITY to ADJUSTED QTY
# • Increment S YTD by QUANTITY[i]
# • Increment S ORDER CNT by 1
# • Increment S REMOTE CNT by 1 if SUPPLIER WAREHOUSE[i] ̸= W ID
# (e) ITEM AMOUNT = QUANTITY[i] × I PRICE, where I PRICE is the price of ITEM NUMBER[i] (f) TOTAL AMOUNT = TOTAL AMOUNT + ITEM AMOUNT
# (g) Create a new order-line • OLOID=N
# • OLDID=DID
# • OLWID=WID
# • OL NUMBER = i
# • OL I ID = ITEM NUMBER[i]
# • OL SUPPLY W ID = SUPPLIER WAREHOUSE[i] • OL QUANTITY = QUANTITY[i]
# • OL AMOUNT = ITEM AMOUNT
# • OL DELIVERY D = null
# • OLDISTINFO=SDISTxx,wherexx=DID
# 6. TOTAL AMOUNT = TOTAL AMOUNT × (1+D TAX +W TAX) × (1−C DISCOUNT), where W TAX is the tax rate for warehouse W ID, D TAX is the tax rate for district (W ID, D ID), and C DISCOUNT is the discount for customer C ID.
# Output the following information:
# 1. Customer identifier (W ID, D ID, C ID), lastname C LAST, credit C CREDIT, discount C DISCOUNT 2. Warehouse tax rate W TAX, District tax rate D TAX
# 3. Order number O ID, entry date O ENTRY D
# 4. Number of items NUM ITEMS, Total amount for order TOTAL AMOUNT
# 5. For each ordered item ITEM NUMBER[i], i ∈ [1,NUM ITEMS]
# (a) ITEM NUMBER[i] (b) I NAME
# (c) SUPPLIER WAREHOUSE[i] (d) QUANTITY[i]
# (e) OL AMOUNT (f) S QUANTITY

