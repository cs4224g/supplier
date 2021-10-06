from cassandra.query import named_tuple_factory, SimpleStatement
from decimal import Decimal

# todo: remove all prints and queries for checking if updates are correctly applied before testing.
def execute_t3(session, args_arr):
    # D,2,8
    print("T3 Delivery Transaction called!\n----------------------")
    # print(args_arr)

    ####################### extract query inputs    
    assert len(args_arr) == 3, "Wrong length of argments for T3"
    w_id = int(args_arr[1])
    new_carrier_id = int(args_arr[2])
    
    # for d_id in range(1, 2):
    for d_id in range(1, 11): # [1..10]
      # Orders table: (o_w_id, o_d_id, o_id) uniquely identify row
      # So given (o_w_id, o_d_id), each o_id returned should be unique
      # Expect: bc of clustering, smallest o_id is first.
      order = session.execute(f"""
        SELECT * FROM orders WHERE o_w_id=%s and o_d_id=%s and o_carrier_id=%s LIMIT 1;""",
        (w_id, d_id, -1))[0]
      assert order, f"No null carrier_id exists for w={w_id}, d={d_id}. Is this a problem or can silently skip?"  # ask
      print(order)

      # update carrier_id, which is a PK col
      session.execute(SimpleStatement(f"""
        DELETE FROM orders 
        WHERE o_w_id={order.o_w_id} 
          AND o_d_id={order.o_d_id} 
          AND o_carrier_id={order.o_carrier_id} 
          AND o_id={order.o_id} 
          AND o_c_id={order.o_c_id};"""))
      
      session.execute(f"""
      INSERT INTO orders (o_w_id,
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
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""", 
                          (order.o_w_id,
                          order.o_d_id,
                          new_carrier_id,
                          order.o_id,
                          order.o_c_id,
                          order.o_all_local,
                          order.o_c_first,
                          order.o_c_last,
                          order.o_c_middle,
                          order.o_entry_d,
                          order.o_ol_cnt))
      
      ######################## check update of carrier_id is correct
      chk = session.execute(f"""
        SELECT * FROM orders WHERE o_w_id=%s and o_d_id=%s and o_carrier_id=%s and o_id=%s LIMIT 1;""",
        (order.o_w_id, order.o_d_id, new_carrier_id, order.o_id))[0]
      print(chk)

      ######################## update order_line table
      # when queried with (ol_w_id, ol_d_id, ol_o_id), each rows return should have unique ol_number
      print()
      order_lines = session.execute(f"""
          SELECT * FROM order_line WHERE ol_w_id=%s AND ol_d_id=%s AND ol_o_id=%s;""",
          (w_id, d_id, order.o_id))

      # note: order_lines is an iterator and will be exhausted. Only one iteration is allowed without duplicating it.
      ol_amount_total = Decimal(0)
      for r in order_lines:
        print(r)
        ol_amount_total += r.ol_amount

        session.execute(f"""
          UPDATE order_line 
          SET ol_delivery_d=toTimestamp(Now())
          WHERE OL_W_ID=%s 
            AND OL_D_ID=%s
            AND OL_O_ID=%s
            AND OL_QUANTITY=%s
            AND OL_NUMBER=%s
            AND OL_C_ID=%s;""",
          (r.ol_w_id, r.ol_d_id, r.ol_o_id, r.ol_quantity, r.ol_number, r.ol_c_id))
        
      ######################## check order_lines correctly updated
      print()
      order_lines = session.execute(f"""
        SELECT * FROM order_line WHERE ol_w_id=%s AND ol_d_id=%s AND ol_o_id=%s;""",
        (w_id, d_id, order.o_id))
      for r in order_lines:
        print(r)

      ######################## update customer via drop and insert
      print('\ntotal', ol_amount_total)
      customer = session.execute(SimpleStatement(f"""SELECT * FROM customer WHERE c_w_id={w_id} and c_d_id={d_id} and c_id={order.o_c_id};"""))[0]
      print(customer)

      session.execute(SimpleStatement(f"""DELETE FROM customer WHERE c_w_id={w_id} and c_d_id={d_id} and c_id={order.o_c_id};"""))
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
                          (customer.c_w_id, 
                            customer.c_d_id, 
                            customer.c_id, 
                            customer.c_balance + ol_amount_total, 
                            customer.c_city, 
                            customer.c_credit, 
                            customer.c_credit_lim, 
                            customer.c_d_name, 
                            customer.c_data, 
                            customer.c_delivery_cnt + 1,
                            customer.c_discount,
                            customer.c_first,
                            customer.c_last,
                            customer.c_middle,
                            customer.c_payment_cnt,
                            customer.c_phone,
                            customer.c_since,
                            customer.c_state,
                            customer.c_street_1,
                            customer.c_street_2,
                            customer.c_w_name,
                            customer.c_ytd_payment,
                            customer.c_zip))

      ######################## check customer updated
      customer = session.execute(SimpleStatement(f"""SELECT * FROM customer WHERE c_w_id={w_id} and c_d_id={d_id} and c_id={order.o_c_id};"""))[0]
      print(customer)


      ######################## update carrier_id in duplicate tables
      # order_status
      print()
      order_status_before = session.execute(f"""
        SELECT * FROM order_status where O_W_ID=%s and O_D_ID=%s and O_C_ID=%s and O_ID=%s;""",
        (w_id, d_id, order.o_c_id, order.o_id))[0]
      print(order_status_before)

      assert order_status_before.o_carrier_id == -1, f"Inconsistency detected? Trying to update null order_status carrier_id but it's already non-null"


      session.execute(f"""
        UPDATE order_status SET o_carrier_id=%s where O_W_ID=%s and O_D_ID=%s and O_C_ID=%s and O_ID=%s;""",
        (new_carrier_id, w_id, d_id, order.o_c_id, order.o_id))

      order_status_after = session.execute(f"""
        SELECT * FROM order_status where O_W_ID=%s and O_D_ID=%s and O_C_ID=%s and O_ID=%s;""",
        (w_id, d_id, order.o_c_id, order.o_id))[0]
      print(order_status_after)