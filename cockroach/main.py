import sys
import time
import logging
import random
import psycopg2
import numpy as np
from psycopg2.errors import SerializationFailure
from argparse import ArgumentParser, RawTextHelpFormatter

from transactions.proj import new_order_transaction
from transactions.proj import payment_transaction
from transactions.proj import delivery_transaction
from transactions.proj import run_transaction
from transactions.order_status import execute_t4
from transactions.stock_level import execute_t5
from transactions.popular_item import execute_t6
from transactions.top_balance import execute_t7
from transactions.related_customer import execute_t8
from stats import get_stats

def main():

    #temporary username: test, pw: test1, host:192.168.51.3:26357, dbname = proj, sslmode default disabled 
    conn = psycopg2.connect("postgresql://test:test1@localhost:26257/supplier?sslmode=require")

    max_retries = 3
    no_transact = 0
    total_time = 0
    
    latencies = []
    
    #execute transactions
    with conn:
        for line in sys.stdin:
            instruct = line.strip().split(',')
            transact = line[0]
            start_time = time.time()
            no_retries = 0

            if transact == 'N':
                no_items = instruct[4]
                items = []
                warehouse = []
                quantity = []
                for i in range(0, int(no_items)):
                    next_item = sys.stdin.readline()
                    desc = next_item.strip().split(',')
                    items.append(int(desc[0]))
                    warehouse.append(int(desc[1]))
                    quantity.append(int(desc[2]))
                run_transaction(conn, lambda conn: new_order_transaction(conn, int(instruct[1]), int(instruct[2]), int(instruct[3]), int(instruct[4]), items, warehouse, quantity))
            elif transact == 'P':
                run_transaction(conn, lambda conn: payment_transaction(conn, instruct[1], instruct[2], instruct[3], instruct[4]))
            elif transact == 'D':
                run_transaction(conn, lambda conn: delivery_transaction(conn, instruct[1], instruct[2]))
            elif transact == 'O':
                run_transaction(conn, lambda conn: execute_t4(conn, instruct[1], instruct[2], instruct[3]))
            elif transact == 'S':
                run_transaction(conn, lambda conn: execute_t5(conn, instruct[1], instruct[2], instruct[3], instruct[4]))
            elif transact == 'I':
                run_transaction(conn, lambda conn: execute_t6(conn, instruct[1], instruct[2], instruct[3]))
            elif transact == 'T':
                run_transaction(conn, lambda conn: execute_t7(conn))
            elif transact == 'R':
                run_transaction(conn, lambda conn: execute_t8(conn, instruct[1], instruct[2], instruct[3]))
                    
                no_transact += 1
                latency = time.time() - start_time
                latencies.append(latency)
                total_time += latency

    transaction_throughput = no_transact/total_time
  
    #avg latency in ms
    avg_transac_latency = total_time/no_transact * 1000 
    #median transaction latency in ms
    med = np.percentile(latencies, 50) * 1000
    #95th percentile latency in ms
    p95 = np.percentile(latencies, 95) * 1000
    #99th percentile latency in ms
    p99 = np.percentile(latencies, 99) * 1000  
    
    #write to stderr
    measurements = [str(no_transact), str(total_time), str(transaction_throughput), str(avg_transac_latency), str(med), str(p95), str(p99)]
    m_str = ",".join(measurements)
    print(m_str, file=sys.stderr)

    #15 query stats
    #get_stats(connection) 


    
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
