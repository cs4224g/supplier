#transaction 6

class popular_item():

    #l denotes number of last orders to be examined
    def execute(self, connection, w_id, d_id, l):
        print('\n================ executing popular item query ================\n')

        #requirement point 1 & 2
        print("W_ID =", w_id,  "D_ID =", d_id)
        print("Number of last orders to be examined = ", l)

        results = ''
        with connection.cursor() as cur:
            cur.execute('SELECT o_id, o_entry_d, c_first, c_middle, c_last\
                        FROM orders, district, customer\
                        WHERE (o_w_id, o_d_id) = (d_w_id, d_id)\
                        AND (d_w_id, d_id) = (%s, %s)\
                        AND o_id < d_next_o_id\
                        AND (o_w_id, o_d_id, o_c_id) = (c_w_id, c_d_id, c_id)\
                        limit %s', (w_id, d_id, l))

            results = cur.fetchall()
            
            #requirement point 3
            #TODO: format results
            for order in results:
                print(order)
    
            #find popular items
            for order in results:
                cur.execute('SELECT i_name, ol_quantity \
                            FROM item, order_line \
                            WHERE ol_quantity = (SELECT max(ol_quantity) FROM order_line WHERE (ol_w_id, ol_d_id, ol_o_id) = (%s, %s, %s))\
                            AND i_id = ol_i_id AND (ol_w_id, ol_d_id, ol_o_id) = (%s, %s, %s)', 
                            (w_id, d_id, order[0], w_id, d_id, order[0]))

            results = cur.fetchall()
        print('data retrieved, printing...')
        
        #TODO: format output
        for record in results:
            print(record)