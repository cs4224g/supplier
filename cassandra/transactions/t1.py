import sys
import time
from cassandra.query import BatchStatement, SimpleStatement
from decimal import Decimal
from datetime import datetime

# also reads from stdin, which main.py is reading from
# from testing, if stdin in consumed here, it will not be consumed in main.
# returns list of (OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY)
def get_items(num_items):
    items = []
    for _ in range(num_items):
        items.append(tuple(map(int, input().split(','))))
    return items

def execute_t1(session, args_arr):
    print("T1 New Order Transaction called!")

    ####################### extract query inputs    
    assert len(args_arr) == 5, "Wrong length of argments for T1"
    c_id = int(args_arr[1])
    w_id = int(args_arr[2])
    d_id = int(args_arr[3])
    num_items = int(args_arr[4])
    items = get_items(num_items)

    districts = session.execute(f"""SELECT * FROM district WHERE d_w_id={w_id} AND d_id={d_id};""")
    if not districts:
        return 1
    district = districts[0]
    next_o_id = district.d_next_o_id

    session.execute(f"""UPDATE district SET D_NEXT_O_ID={next_o_id + 1} WHERE d_w_id={w_id} AND d_id={d_id}""")

    # check if next_o_id updated
    # district = session.execute(f"""SELECT * FROM district WHERE d_w_id={w_id} AND d_id={d_id}""")
    # print(district[0])

    customers = session.execute(f"""SELECT * FROM customer WHERE C_W_ID={w_id} AND C_D_ID={d_id} AND C_ID={c_id};""")
    if not customers:
        return 1
    customer = customers[0]

    is_all_local = len(list(filter(lambda tw: tw != w_id, [t[1] for t in items]))) == 0

    ### create new order in both order tables
    # strictly speaking, since they are not executed in batch, the O_ENTRY_D = toTimestamp(Now())
    # will differ slightly for the 2 orders, but there don't seem to be any queries sensitive to 
    # that timestamp, so leaving it as un-batched.
    session.execute(
        """INSERT INTO orders (
                            o_w_id,
                            o_d_id,
                            o_carrier_id,
                            o_id,
                            o_c_id,
                            o_all_local,
                            o_c_first,
                            o_c_last,
                            o_c_middle,
                            o_entry_d,
                            o_ol_cnt) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, toTimestamp(Now()), %s);""", 
                            (
                            w_id,
                            d_id,
                            -1,
                            next_o_id,
                            c_id,
                            (1 if is_all_local else 0),
                            customer.c_first,
                            customer.c_last,
                            customer.c_middle,
                            num_items
                            ))
    session.execute(
        """INSERT INTO order_by_carrier_id (
                            o_w_id,
                            o_d_id,
                            o_carrier_id,
                            o_id,
                            o_c_id,
                            o_all_local,
                            o_c_first,
                            o_c_last,
                            o_c_middle,
                            o_entry_d,
                            o_ol_cnt) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, toTimestamp(Now()), %s);""", 
                            (
                            w_id,
                            d_id,
                            -1,
                            next_o_id,
                            c_id,
                            (1 if is_all_local else 0),
                            customer.c_first,
                            customer.c_last,
                            customer.c_middle,
                            num_items
                            ))

    total_amount = 0
    item_names = []     #for printing later
    item_amounts = []   #for printing later
    remaining_qtys = [] #for printing later

    for i, tup in enumerate(items):
        ol_i_id, ol_supply_w_id, ol_quantity = tup

        stocks = session.execute(f"""SELECT * FROM stock WHERE S_W_ID={ol_supply_w_id} AND S_I_ID={ol_i_id};""")
        if not stocks:
            return 1
        stock = stocks[0]
        # print(stock)
        
        s_qty = stock.s_quantity
        adj_qty = s_qty - ol_quantity
        if adj_qty < 10:
            adj_qty += 100
        remaining_qtys.append(adj_qty)

        # with addition of S_QUANTITY to PK of stock, must delete and reinsert to update
        ts = int(time.time() * 10e6)
        batch = BatchStatement()
        batch.add(SimpleStatement(f"""
            DELETE from stock 
            USING TIMESTAMP {ts}
            WHERE s_w_id=%s
            AND s_i_id=%s
            AND s_quantity=%s;
            """), (ol_supply_w_id, ol_i_id, stock.s_quantity))

        batch.add(SimpleStatement(f"""
            INSERT INTO stock (
                S_W_ID,
                S_I_ID,
                S_QUANTITY,
                S_YTD,
                S_ORDER_CNT,
                S_REMOTE_CNT,
                S_DIST_01,
                S_DIST_02,
                S_DIST_03,
                S_DIST_04,
                S_DIST_05,
                S_DIST_06,
                S_DIST_07,
                S_DIST_08,
                S_DIST_09,
                S_DIST_10,
                S_DATA
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            USING TIMESTAMP %s;"""), 
            (
                stock.s_w_id,
                stock.s_i_id,
                adj_qty,
                stock.s_ytd + ol_quantity,
                stock.s_order_cnt + 1,
                stock.s_remote_cnt + (1 if ol_supply_w_id != w_id else 0),
                stock.s_dist_01,
                stock.s_dist_02,
                stock.s_dist_03,
                stock.s_dist_04,
                stock.s_dist_05,
                stock.s_dist_06,
                stock.s_dist_07,
                stock.s_dist_08,
                stock.s_dist_09,
                stock.s_dist_10,
                stock.s_data,
                ts + 1
            ))
        session.execute(batch)
        

        item_infos = session.execute(f"""SELECT * FROM item WHERE i_id={ol_i_id}""")
        if not item_infos:
            return 1
        item_info = item_infos[0]
        price = item_info.i_price
        item_amount = ol_quantity * price
        total_amount += item_amount

        item_names.append(item_info.i_name)
        item_amounts.append(item_amount)

        # note: OL_DELIVERY_D needs to be created with null here (T1), set in T3, and queried
        # in T4. So we DON'T insert OL_DELIVERY_D for an implicit null (col doesn't exist).
        # Don't need special null value bc we are not querying "WHERE OL_DELIVERY_D IS NULL"

        # print(f'insert into orderline',w_id, d_id,next_o_id,i+1)
        session.execute("""
            INSERT INTO order_line (
                    OL_W_ID,
                    OL_D_ID,
                    OL_O_ID,
                    OL_NUMBER,
                    OL_I_ID,
                    OL_AMOUNT,
                    OL_SUPPLY_W_ID,
                    OL_QUANTITY,
                    OL_DIST_INFO,
                    OL_I_NAME,
                    OL_I_PRICE,
                    OL_C_ID,
                    OL_C_FIRST,
                    OL_C_MIDDLE,
                    OL_C_LAST,
                    OL_O_ENTRY_D
                ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, toTimestamp(Now()));""", 
                (
                w_id,
                d_id,
                next_o_id,
                i+1, #1-based indexing
                ol_i_id,
                item_amount,
                ol_supply_w_id,
                ol_quantity,
                stock[2 + d_id],  # note: if schema of stocks changes, the index of S_DIST_XX will change too!
                item_info.i_name,
                item_info.i_price,
                c_id,
                customer.c_first,
                customer.c_last,
                customer.c_middle,
                ))
        
        # Create new order_line in 3 duplicate order_line tables too.
        session.execute("""
            INSERT INTO order_line_by_customer (
                OL_W_ID,
                OL_D_ID,
                OL_O_ID,
                OL_NUMBER,
                OL_I_ID,
                OL_C_ID
            ) VALUES (%s, %s, %s, %s, %s, %s);""",
            (
                w_id,
                d_id,
                next_o_id,
                i+1, #1-based indexing
                ol_i_id,
                c_id
            ))
        session.execute("""
            INSERT INTO order_line_by_item (
                OL_W_ID,
                OL_D_ID,
                OL_O_ID,
                OL_NUMBER,
                OL_I_ID,
                OL_C_ID
            ) VALUES (%s, %s, %s, %s, %s, %s);""",
            (
                w_id,
                d_id,
                next_o_id,
                i+1, #1-based indexing
                ol_i_id,
                c_id
            ))
        session.execute("""
            INSERT INTO order_line_by_order (
                OL_W_ID,
                OL_D_ID,
                OL_O_ID,
                OL_NUMBER,
                OL_I_ID,
                OL_C_ID
            ) VALUES (%s, %s, %s, %s, %s, %s);""",
            (
                w_id,
                d_id,
                next_o_id,
                i+1, #1-based indexing
                ol_i_id,
                c_id
            ))
  
    # print('bef', total_amount)
    d_tax = district.d_tax
    warehouses = session.execute(f"""SELECT * FROM warehouse WHERE w_id={w_id}""")
    if not warehouses:
        return 1
    warehouse = warehouses[0]
    w_tax = warehouse.w_tax
    total_amount *= (1 + d_tax + w_tax) * (1 - customer.c_discount)
    # print(d_tax)
    # print(w_tax)
    # print(customer.c_discount)
    # print('aft:', total_amount)

    #### print out required info
    print(customer.c_w_id, customer.c_d_id, customer.c_id, customer.c_last, customer.c_credit, customer.c_discount)
    print(w_tax, d_tax)
    print(next_o_id, datetime.now())  #current timestamp separate from query's ts so we don't query it again
    print(num_items, total_amount)
    for i in range(num_items):
        print(items[i][0], item_names[i], items[i][1], items[i][2], item_amounts[i], remaining_qtys[i])

    return 0

