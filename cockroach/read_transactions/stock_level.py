#transaction 5

class stock_level():

    #t = stock threshold, l = number of last orders to be examined
    def execute(self, connection, w_id, d_id, t, l):
        print('\n============== executing stock_level query ================\n')

        results = ''
        with connection.cursor() as cur:
            cur.execute("SELECT o_entry_d\
                        FROM district, orders \
                        WHERE d_w_id = %s AND d_id = %s AND (o_d_id, o_w_id, o_id) = (d_w_id, d_id, (d_next_o_id - 1))"
                        , (w_id, d_id))
            
            #n_date denotes the value of the latest order date
            n_date = cur.fetchone()[0] 

            #get all items in last n orders
            #TODO: optimise???
            cur.execute("SELECT sum(DISTINCT ol_i_id) \
                        FROM order_line, orders, stock \
                        WHERE ol_d_id = %s AND ol_w_id = %s \
                            AND ol_o_id = o_id AND o_entry_d <= %s \
                            AND (s_w_id, s_i_id) = (ol_w_id, ol_i_id) \
                            AND s_quantity > %s\
                        LIMIT %s",
                        (d_id, w_id, n_date, t, l))
            results = cur.fetchall()

        print('data retrieved, printing...')
        
        #TODO: format output
        print(results[0])