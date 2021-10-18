import sys
import psycopg2
from argparse import ArgumentParser, RawTextHelpFormatter
from transactions.proj import new_order_transaction
from transactions.proj import payment_transaction
from transactions.proj import delivery_transaction
from transactions.order_status import execute_t4
from transactions.stock_level import execute_t5
from transactions.popular_item import execute_t6
from transactions.top_balance import execute_t7
from transactions.related_customer import execute_t8
from stats import get_stats

def main():

    opt = parse_cmdline()
    connection = psycopg2.connect(opt.dsn)
    print('connected succesfully')

    #execute transactions
    for line in sys.stdin:
        instruct = line.split()
        transact = line[0]
        if transact == 'N':
            new_order_transaction(connection, instruct[1], instruct[2], instruct[3], instruct[4], instruct[5], instruct[6], instruct[7])
        elif transact == 'P':
            payment_transaction(connection, instruct[1], instruct[2], instruct[3], instruct[4])
        elif transact == 'D':
            delivery_transaction(connection, instruct[1], instruct[2])
        elif transact == 'O':
            execute_t4(connection, instruct[1], instruct[2], instruct[3])
        elif transact == 'S':
            execute_t5(connection, instruct[1], instruct[2], instruct[3], instruct[4])
        elif transact == 'I':
            execute_t6(connection, instruct[1], instruct[2], instruct[3])
        elif transact == 'T':
            execute_t7(connection)
        elif transact == 'R':
            execute_t8(connection, instruct[1], instruct[2], instruct[3])

    #calc and write stats to '0.csv'
    get_stats(connection)    

def parse_cmdline():
    parser = ArgumentParser(description=__doc__,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument(
        "dsn",
        help="""\
            database connection string

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


if __name__ == "__main__":
    main()
