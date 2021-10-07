#transaction 4

class order_status():

    def execute(self, connection, c_w_id, c_d_id, c_id):
        #connection = psycopg2.connect(opt.dsn)
        #print('connected succesfully')
        print('\n============== executing order-status query ================\n')
        results = ''

       
        with connection.cursor() as cur:
           #output customer name and balance
            cur.execute("SELECT c_first, c_middle, c_last, c_balance \
                        FROM customer \
                        WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s", 
                        (c_w_id, c_d_id, c_id))

            # for record in cur:
            #     print(record)
            print(cur.fetchone)

            #output customer last order information
            cur.execute("SELECT o_id, o_entry_d, o_carrier_id \
                        FROM orders \
                        WHERE o_w_id = %s AND o_d_id = %s AND o_c_id = %s", 
                        (c_w_id, c_d_id, c_id))

            o_id = ''
            for record in cur:
                o_id = record[0]
                print(record)

            #output all the items in the customer's last order
            cur.execute("SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d \
                        FROM order_line \
                        WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s", 

                        (c_w_id, c_d_id, o_id)
            )

            results = cur.fetchall()
        print('data retrieved, printing...')
        
        #TODO: formate output 
        for record in results:
            print(record)