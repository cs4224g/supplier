from cassandra.query import named_tuple_factory, SimpleStatement
from decimal import Decimal

def execute_t2(session, args_arr):
    # P,1,1,2203,3261.87
    print("T2 Payment Transaction called!")
    # print(args_arr)

    # extract query inputs    
    assert len(args_arr) == 5, "Wrong length of argments for T2"
    c_w_id = int(args_arr[1])
    c_d_id = int(args_arr[2])
    c_id = int(args_arr[3])
    payment = Decimal(args_arr[4])

    # session.row_factory = named_tuple_factory # seems like this is the default already
    query_warehouse = SimpleStatement(f"""SELECT * FROM warehouse WHERE w_id={c_w_id};""")
    ret_warehouse = session.execute(query_warehouse)[0]
    # print('warehouse: ', ret_warehouse)

    query_district = SimpleStatement(f"""SELECT * FROM district WHERE d_w_id={c_w_id} and d_id={c_d_id};""")
    ret_district = session.execute(query_district)[0]
    # print('district: ', ret_district)

    query_customer = SimpleStatement(f"""SELECT * FROM customer WHERE c_w_id={c_w_id} and c_d_id={c_d_id} and c_id={c_id};""")
    ret_customer = session.execute(query_customer)[0]
    # print('customer: ', ret_customer)

    # update only if data hasn't changed since we queried warehouse
    upd_warehouse = SimpleStatement(f"""UPDATE warehouse SET w_ytd={ret_warehouse.w_ytd + payment} 
                                        WHERE w_id={c_w_id} 
                                        IF w_ytd={ret_warehouse.w_ytd};""")
                                        
    # only if the first upd succeeds, trigger the rest
    if session.execute(upd_warehouse)[0].applied:
        upd_district = SimpleStatement(f"""UPDATE district SET d_ytd={ret_district.d_ytd + payment} 
                                        WHERE d_w_id={c_w_id} AND d_id={c_d_id};""")
        session.execute(upd_district)

        session.execute(SimpleStatement(f"""DELETE FROM customer 
                                            WHERE c_w_id={c_w_id} and c_d_id={c_d_id} and c_id={c_id};"""))
        # literal quoting below; doesn't escape timestamps properly
        session.execute(f"""
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
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""", 
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
                              ret_customer.c_zip))
        
        # c_balance is duplicated in top_balance
        # c_balance is PK col in top_balance table, need to delete and reinsert updated row
        session.execute(SimpleStatement(f"""
          DELETE FROM top_balance 
          WHERE c_w_id={c_w_id} 
          and c_d_id={c_d_id} 
          and c_balance={ret_customer.c_balance} 
          and c_id={c_id};"""))
        
        session.execute(f"""
          INSERT INTO top_balance (c_w_id,
                                   c_d_id,
                                   c_balance,
                                   c_id,
                                   c_d_name,
                                   c_first,
                                   c_last,
                                   c_middle,
                                   c_w_name) 
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);""", 
                            (ret_customer.c_w_id, 
                              ret_customer.c_d_id, 
                              ret_customer.c_balance - payment, 
                              ret_customer.c_id, 
                              ret_customer.c_d_name, 
                              ret_customer.c_first,
                              ret_customer.c_last,
                              ret_customer.c_middle,
                              ret_customer.c_w_name))

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




