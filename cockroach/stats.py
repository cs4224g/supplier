#retrieve 15 statistics
import psycopg2
from argparse import ArgumentParser, RawTextHelpFormatter

def get_stats(connection):
    print('\n============== retrieving statistics ================\n')
    #clear file
    filename = 'dbstate.csv'
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


def parse_cmdline():
    parser = ArgumentParser(description=__doc__,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument(
        "dsn",
        help="""database connection string

For cockroach demo, use
'postgresql://<username>:<password>@<hostname>:<port>/bank?sslmode=require',
with the username and password created in the demo cluster, and the hostname
and port listed in the (sql/tcp) connection parameters of the demo cluster
welcome message.

For CockroachCloud Free, use
'postgres://<username>:<password>@free-tier.gcp-us-central1.cockroachlabs.cloud:26257/<cluster-name>.bank?sslmode=verify-full&sslrootcert=<your_certs_directory>/cc-ca.crt'.

If you are using the connection string copied from the Console, your username,
password, and cluster name will be pre-populated. Replace
<your_certs_directory> with the path to the 'cc-ca.crt' downloaded from the
Console.

"""
    )

    parser.add_argument("-v", "--verbose",
                        action="store_true", help="print debug info")

    opt = parser.parse_args()
    return opt

def main():
    opt = parse_cmdline()
    conn = psycopg2.connect(opt.dsn)
    get_stats(conn)

if __name__ == "__main__":
    main()
        
        