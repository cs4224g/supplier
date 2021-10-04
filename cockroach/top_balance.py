#transaction 7

class top_balance():

    def execute(self, connection):
        #connection = psycopg2.connect(opt.dsn)
        #print('connected succesfully')
        print('\n================ executing top_balance query ================\n')

        results = ''
        with connection.cursor() as cur:
            cur.execute("SELECT c_first, c_middle, c_last, c_balance, w_name, d_name \
                        FROM customer, warehouse, district \
                        WHERE c_d_id = d_id AND d_w_id = w_id AND c_w_id = w_id \
                        ORDER BY c_balance DESC LIMIT 10;"
                        )
            results = cur.fetchall()
        print('data retrieved, printing...')
        
        #TODO: format output
        for record in results:
            print(record)




