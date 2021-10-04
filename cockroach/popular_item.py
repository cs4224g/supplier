#transaction 6

class popular_item():

    #l denotes number of last orders to be examined
    def execute(self, connection, w_id, d_id, l):
        print('\n================ executing popular_item query ================\n')

        #requirement point 1 & 2
        print("W_ID = %s, D_ID = %s", w_id, d_id)
        print("Number of last orders to be examined = %s", l)

        results = ''
        with connection.cursor() as cur:
            #get latest order date
            cur.execute("SELECT o_entry_d\
                        FROM district, orders\
                        WHERE d_w_id = %s AND d_id = %s AND (o_d_id, o_w_id, o_id) = (d_w_id, d_id, (d_next_o_id - 1))"
                        , (w_id, d_id))
            
            #n_date denotes the value of the latest order date
            n_date = cur.fetchone()[0] 

            #get list of order numbers
            cur.execute("SELECT o_id, o_entry_d, c_first, c_middle, c_last \
                        FROM orders, customer \
                        WHERE o_w_id = %s, o_d_id = %s, o_entry_d <= %s AND (o_w_id, o_d_id, o_c_id) = (w_id, d_id, c_id) \
                        LIMIT %s ", 
                        (w_id, d_id, n_date, l))
        
            results = cur.fetchall()

            #requirement point 3
            #TODO: format results
            for item in results:
                print(item)
    
            #find popular items


        print('data retrieved, printing...')
        
        #TODO: format output
        for record in results:
            print(record)

