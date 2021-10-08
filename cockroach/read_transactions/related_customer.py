#transaction 8

class related_customer():

    def execute(self, connection, c_w_id, c_d_id, c_id):
        #connection = psycopg2.connect(opt.dsn)
        #print('connected succesfully')
        print('\n================ executing related_customer query ================\n')

        print("Customer identifier: ")
        print(c_w_id, c_d_id, c_id)

        results = ''
        with connection.cursor() as cur:
            cur.execute("SELECT c_w_id, c_d_id, c_id, COUNT(DISTINCT o.ol_i_id) \
                        FROM customer, (orders \
                        INNER JOIN order_line ON (o_w_id, o_d_id, o_id) = (ol_w_id, ol_d_id, ol_o_id)) o, \
                        ( \
                            SELECT ol_o_id, ol_i_id \
                            FROM orders, order_line \
                            WHERE (ol_w_id, ol_d_id) = (%s, %s) \
                            AND (o_w_id, o_d_id) = (%s, %s) \
                            AND o_id = ol_o_id \
                            AND o_c_id = %s \
                        ) a \
                        WHERE o_w_id <> %s\
                        AND o.ol_i_id = a.ol_i_id \
                        AND (c_w_id, c_d_id, c_id) = (o_w_id, o_d_id, o_c_id) \
                        GROUP BY (c_w_id, c_d_id, c_id, o.ol_w_id, o.ol_d_id, o.ol_o_id)",
                        (c_w_id, c_d_id, c_w_id, c_d_id, c_id, c_w_id)
                        )
            results = cur.fetchall()
        
        print('data retrieved, printing...')
        
        #TODO: format output
        for record in results:
            if(record[3] > 2):
                print(record)
