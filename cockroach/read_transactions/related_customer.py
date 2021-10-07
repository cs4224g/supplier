#transaction 8

class related_customer():

    def execute(self, connection, c_w_id, c_d_id, c_id):
        #connection = psycopg2.connect(opt.dsn)
        #print('connected succesfully')
        print('\n================ executing related_customer query ================\n')

        results = ''
        with connection.cursor() as cur:
            cur.execute(""
                        )
            results = cur.fetchall()


            
        print('data retrieved, printing...')
        
        #TODO: format output
        for record in results:
            print(record)
