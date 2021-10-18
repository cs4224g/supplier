#retrieve 15 statistics

def get_stats(connection):
    print('\n============== retrieving statistics ================\n')
    #clear file
    open('0.csv', 'w').close()

    f = open("0.csv", 'a')

    with connection.cursor() as cur:
        # cur.execute('SELECT SUM(w_ytd), SUM(d_ytd), SUM(d_next_o_id), SUM(c_balance), SUM(c_ytd_payment),\
        #                 SUM(c_payment_cnt), SUM(c_delivery_cnt), SUM(o_ol_cnt), SUM(ol_amount), SUM(ol_quantity),\
        #                 SUM(s_quantity), SUM(s_ytd), SUM(s_order_cnt), SUM(s_remote_cnt)\
        #                 FROM warehouse, district, customer, stock, order_line, orders')
        #sum(w_ytd)
        cur.execute('SELECT SUM(w_ytd) FROM warehouse')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(d_ytd)
        cur.execute('SELECT SUM(d_ytd) FROM district')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(d_next_o_id)
        cur.execute('SELECT SUM(d_next_o_id) FROM district')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(c_balance)
        cur.execute('SELECT SUM(c_balance) FROM customer')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(c_ytd_payment)
        cur.execute('SELECT SUM(c_ytd_payment) FROM customer')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(c_payment_cnt)
        cur.execute('SELECT SUM(c_payment_cnt) FROM customer')
        f.write(str(cur.fetchone()[0]) + '\n')
    
        #sum(c_delivery_cnt)
        cur.execute('SELECT SUM(c_delivery_cnt) FROM customer')
        f.write(str(cur.fetchone()[0]) + '\n')

        #max(o_id)
        cur.execute('SELECT MAX(o_id) FROM orders')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(o_ol_cnt)
        cur.execute('SELECT SUM(o_ol_cnt) FROM orders')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(ol_amount)
        cur.execute('SELECT SUM(ol_amount) FROM order_line')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(ol_quantity)
        cur.execute('SELECT SUM(ol_quantity) FROM order_line')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(s_quantity)
        cur.execute('SELECT SUM(s_quantity) FROM stock')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(s_ytd)
        cur.execute('SELECT SUM(s_ytd) FROM stock')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(s_order_cnt)
        cur.execute('SELECT SUM(s_order_cnt) FROM stock')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(s_remote_cnt)
        cur.execute('SELECT SUM(s_remote_cnt) FROM stock')
        f.write(str(cur.fetchone()[0]) + '\n')

    f.close()
    print("Statistics written to 0.csv")


        
        