#transaction 5


#t = stock threshold, l = number of last orders to be examined
def execute_t5(connection, w_id, d_id, t, l):
    print('\n============== executing stock_level query ================\n')

    results = ''
    with connection.cursor() as cur:
        # cur.execute("SELECT o_entry_d\
        #             FROM proj.district, proj.orders \
        #             WHERE d_w_id = %s AND d_id = %s AND (o_w_id, o_d_id, o_id) = (d_w_id, d_id, (d_next_o_id - 1))"
        #             , (w_id, d_id))
        
        # #n_date denotes the value of the latest order date
        # n_date = cur.fetchone()[0] 

        #get all items in last n orders
        cur.execute("SELECT COUNT(DISTINCT ol_i_id)\
            FROM proj.stock, proj.order_line JOIN \
            (SELECT o_w_id, o_d_id, o_id\
            FROM proj.orders\
            WHERE (o_w_id, o_d_id) = (%s, %s)\
            AND o_entry_d <= (SELECT (o_entry_d)\
            FROM proj.district, proj.orders\
            WHERE (d_w_id, d_id) = (%s, %s) AND (o_w_id, o_d_id, o_id) = (d_w_id, d_id, (d_next_o_id - 1)))\
            LIMIT %s)\
            ON (ol_w_id, ol_d_id, ol_o_id) = (o_w_id, o_d_id, o_id)\
            WHERE (ol_w_id, ol_d_id) = (%s, %s)\
            AND (s_w_id, s_i_id) = (ol_w_id, ol_i_id)\
            AND s_quantity < %s",
            (w_id, d_id, w_id, d_id, l, w_id, d_id, t))
            
        results = cur.fetchall()   
        print(results[0][0]) 
        
    