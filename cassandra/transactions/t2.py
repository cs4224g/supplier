import time
from cassandra.query import BatchStatement, named_tuple_factory, SimpleStatement
from decimal import Decimal

def execute_t2(session, args_arr):
    print("T2 Payment Transaction called!")

    # extract query inputs    
    assert len(args_arr) == 5, "Wrong length of argments for T2"
    c_w_id = int(args_arr[1])
    c_d_id = int(args_arr[2])
    c_id = int(args_arr[3])
    payment = Decimal(args_arr[4])

    # session.row_factory = named_tuple_factory # seems like this is the default already
    query_warehouse = SimpleStatement(f"""SELECT * FROM warehouse WHERE w_id={c_w_id};""")
    ret_warehouse_res = session.execute(query_warehouse)
    if not ret_warehouse_res:
      return 1
    ret_warehouse = ret_warehouse_res[0]
    # print('warehouse: ', ret_warehouse)

    query_district = SimpleStatement(f"""SELECT * FROM district WHERE d_w_id={c_w_id} and d_id={c_d_id};""")
    ret_district_res = session.execute(query_district)
    if not ret_district_res:
      return 1
    ret_district = ret_district_res[0]
    # print('district: ', ret_district)

    query_customer = SimpleStatement(f"""SELECT * FROM customer WHERE c_w_id={c_w_id} and c_d_id={c_d_id} and c_id={c_id};""")
    ret_customer_res = session.execute(query_customer)
    if not ret_customer_res:
      return 1
    ret_customer = ret_customer_res[0]
    # print('customer: ', ret_customer)

    # update only if data hasn't changed since we queried warehouse
    upd_warehouse = SimpleStatement(f"""UPDATE warehouse SET w_ytd={ret_warehouse.w_ytd + payment} 
                                        WHERE w_id={c_w_id};""")

    session.execute(upd_warehouse)

    upd_district = SimpleStatement(f"""UPDATE district SET d_ytd={ret_district.d_ytd + payment} 
                                    WHERE d_w_id={c_w_id} AND d_id={c_d_id};""")
    session.execute(upd_district)

    ts = int(time.time() * 10e6)
    batch = BatchStatement()
    batch.add(SimpleStatement(f"""
        DELETE FROM customer 
        USING TIMESTAMP {ts}
        WHERE c_w_id={c_w_id} 
        and c_d_id={c_d_id} 
        and c_id={c_id};"""))

    batch.add(SimpleStatement(f"""
        INSERT INTO customer (c_w_id, 
                            c_d_id, 
                            c_id, 
                            c_balance, 
                            c_city, 
                            c_credit, 
                            c_credit_lim, 
                            c_d_name, 
                            c_data, 
                            c_delivery_cnt,
                            c_discount,
                            c_first,
                            c_last,
                            c_middle,
                            c_payment_cnt,
                            c_phone,
                            c_since,
                            c_state,
                            c_street_1,
                            c_street_2,
                            c_w_name,
                            c_ytd_payment,
                            c_zip) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    USING TIMESTAMP %s;"""), 
                            (ret_customer.c_w_id, 
                            ret_customer.c_d_id, 
                            ret_customer.c_id, 
                            ret_customer.c_balance - payment, 
                            ret_customer.c_city, 
                            ret_customer.c_credit, 
                            ret_customer.c_credit_lim, 
                            ret_customer.c_d_name, 
                            ret_customer.c_data, 
                            ret_customer.c_delivery_cnt,
                            ret_customer.c_discount,
                            ret_customer.c_first,
                            ret_customer.c_last,
                            ret_customer.c_middle,
                            ret_customer.c_payment_cnt + 1,
                            ret_customer.c_phone,
                            ret_customer.c_since,
                            ret_customer.c_state,
                            ret_customer.c_street_1,
                            ret_customer.c_street_2,
                            ret_customer.c_w_name,
                            ret_customer.c_ytd_payment + float(payment),
                            ret_customer.c_zip,
                            ts + 1 # so that this stmt is ran after the DELETE in this batch
                            ))
    
    session.execute(batch)
    
    # c_balance is duplicated in top_balance
    # c_balance is PK col in top_balance table, need to delete and reinsert updated row
    
    ts = int(time.time() * 10e6)
    batch = BatchStatement()
    batch.add(SimpleStatement(f"""
        DELETE FROM top_balance 
        USING TIMESTAMP {ts}
        WHERE c_w_id={c_w_id} 
        and c_d_id={c_d_id} 
        and c_balance={ret_customer.c_balance} 
        and c_id={c_id};"""))

    batch.add(SimpleStatement(f"""
        INSERT INTO top_balance (c_w_id,
                                    c_d_id,
                                    c_balance,
                                    c_id,
                                    c_d_name,
                                    c_first,
                                    c_last,
                                    c_middle,
                                    c_w_name) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    USING TIMESTAMP %s;"""), 
                            (ret_customer.c_w_id, 
                            ret_customer.c_d_id, 
                            ret_customer.c_balance - payment, 
                            ret_customer.c_id, 
                            ret_customer.c_d_name, 
                            ret_customer.c_first,
                            ret_customer.c_last,
                            ret_customer.c_middle,
                            ret_customer.c_w_name,
                            ts + 1))
    session.execute(batch)

    # print required info; printing updated values
    # print('--------------------')
    # ret_customer is before the update. 
    print(ret_customer.c_w_id, ret_customer.c_d_id, ret_customer.c_id, ret_customer.c_first, 
        ret_customer.c_middle, ret_customer.c_last, ret_customer.c_street_1, ret_customer.c_street_2, ret_customer.c_city, 
        ret_customer.c_state, ret_customer.c_zip, ret_customer.c_phone, ret_customer.c_since, ret_customer.c_credit, 
        ret_customer.c_credit_lim, ret_customer.c_discount, ret_customer.c_balance - payment)
    print(ret_warehouse.w_street_1, ret_warehouse.w_street_2, ret_warehouse.w_city, ret_warehouse.w_state, ret_warehouse.w_zip)
    print(ret_district.d_street_1, ret_district.d_street_2, ret_district.d_city, ret_district.d_state, ret_district.d_zip)
    print(payment)

    return 0




