import sys
import psycopg2
from argparse import ArgumentParser, RawTextHelpFormatter
from top_balance import top_balance
from order_status import order_status


def main():
    # file = open(file_path, 'r')
    # opt = parse_cmdline()
    # for line in file:
    #     if line == 'T':
    #         top_balance().execute(opt)

    opt = parse_cmdline()
    print('starting')
    connection = psycopg2.connect(opt.dsn)
    print('connected succesfully')
    top_balance().execute(connection)
    order_status().execute(connection, '1', '1', '1')


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
    # file_path = sys.argv[1]
    # database_location = sys.argv[2]
    main()
