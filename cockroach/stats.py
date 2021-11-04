#retrieve 15 statistics
import psycopg2

def get_stats(connection, client):
    print('\n============== retrieving statistics ================\n')
    #clear file
    filename = str(client) + '.csv'
    open(filename, 'w').close()

    f = open(filename, 'a')

    with connection.cursor() as cur:
        #sum(w_ytd)
        cur.execute('SELECT SUM(w_ytd) FROM proj.warehouse')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(d_ytd)
        cur.execute('SELECT SUM(d_ytd) FROM proj.district')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(d_next_o_id)
        cur.execute('SELECT SUM(d_next_o_id) FROM proj.district')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(c_balance)
        cur.execute('SELECT SUM(c_balance) FROM proj.customer')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(c_ytd_payment)
        cur.execute('SELECT SUM(c_ytd_payment) FROM proj.customer')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(c_payment_cnt)
        cur.execute('SELECT SUM(c_payment_cnt) FROM proj.customer')
        f.write(str(cur.fetchone()[0]) + '\n')
    
        #sum(c_delivery_cnt)
        cur.execute('SELECT SUM(c_delivery_cnt) FROM proj.customer')
        f.write(str(cur.fetchone()[0]) + '\n')

        #max(o_id)
        cur.execute('SELECT MAX(o_id) FROM proj.orders')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(o_ol_cnt)
        cur.execute('SELECT SUM(o_ol_cnt) FROM proj.orders')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(ol_amount)
        cur.execute('SELECT SUM(ol_amount) FROM proj.order_line')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(ol_quantity)
        cur.execute('SELECT SUM(ol_quantity) FROM proj.order_line')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(s_quantity)
        cur.execute('SELECT SUM(s_quantity) FROM proj.stock')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(s_ytd)
        cur.execute('SELECT SUM(s_ytd) FROM proj.stock')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(s_order_cnt)
        cur.execute('SELECT SUM(s_order_cnt) FROM proj.stock')
        f.write(str(cur.fetchone()[0]) + '\n')

        #sum(s_remote_cnt)
        cur.execute('SELECT SUM(s_remote_cnt) FROM proj.stock')
        f.write(str(cur.fetchone()[0]) + '\n')

    f.close()
    #print("Statistics written to 0.csv")

def main():
    conn = psycopg2.connect("postgresql://root@192.168.51.3:26357?sslmode=disable")
    
    for i in range(0, 40):
        get_stats(conn, i)

if __name__ == "__main__":
    main()
        
        