import sys
import time
import psycopg2
import numpy as np
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

def main():

    opt = parse_cmdline()
    file_name = opt.t_file

    conn = psycopg2.connect(opt.dsn)
    #conn = psycopg2.connect("postgresql://test:test1@localhost:26257/supplier?sslmode=require")

    no_transact = 0
    total_time = 0
    failed_n = 0
    failed_d = 0
    failed_p = 0
    failed_o = 0
    failed_s = 0
    failed_i = 0
    failed_t = 0
    failed_r = 0
    t1_lat = [0]
    t2_lat = [0]
    t3_lat = [0]
    t4_lat = [0]
    t5_lat = [0]
    t6_lat = [0]
    t7_lat = [0]
    t8_lat = [0]

    latencies = []

    # execute transactions
    with conn:
        with open(file_name) as f:
            for line in f:
                print('\nCurrent Transaction = ' + str(no_transact))
                instruct = line.strip().split(',')
                transact = line[0]
                start_time = time.time()

                if transact == 'N':
                    no_items = instruct[4]
                    items = []
                    warehouse = []
                    quantity = []
                    for i in range(0, int(no_items)):
                        next_item = f.readline()
                        desc = next_item.strip().split(',')
                        items.append(int(desc[0]))
                        warehouse.append(int(desc[1]))
                        quantity.append(int(desc[2]))
                    failed_n += run_transaction(conn, lambda conn: new_order_transaction(conn, int(instruct[2]), int(
                        instruct[3]), int(instruct[1]), int(instruct[4]), items, warehouse, quantity))
                    latency = time.time() - start_time
                    latencies.append(latency)
                    total_time += latency
                    t1_lat.append(latency)
                elif transact == 'P':
                    failed_p += run_transaction(conn, lambda conn: payment_transaction(
                        conn, instruct[1], instruct[2], instruct[3], instruct[4]))
                    latency = time.time() - start_time
                    latencies.append(latency)
                    total_time += latency
                    t2_lat.append(latency)
                elif transact == 'D':
                    failed_d += run_transaction(
                        conn, lambda conn: delivery_transaction(conn, instruct[1], instruct[2]))
                    latency = time.time() - start_time
                    latencies.append(latency)
                    total_time += latency
                    t3_lat.append(latency)
                elif transact == 'O':
                    failed_o += run_transaction(conn, lambda conn: execute_t4(
                        conn, instruct[1], instruct[2], instruct[3]))
                    latency = time.time() - start_time
                    latencies.append(latency)
                    total_time += latency
                    t4_lat.append(latency)
                    #execute_t4(conn, instruct[1], instruct[2], instruct[3])
                elif transact == 'S':
                    failed_s += run_transaction(conn, lambda conn: execute_t5(
                        conn, instruct[1], instruct[2], instruct[3], instruct[4]))
                    latency = time.time() - start_time
                    latencies.append(latency)
                    total_time += latency
                    t5_lat.append(latency)
                    #execute_t5(conn, instruct[1], instruct[2], instruct[3], instruct[4])
                elif transact == 'I':
                    failed_i += run_transaction(conn, lambda conn: execute_t6(
                        conn, instruct[1], instruct[2], instruct[3]))
                    latency = time.time() - start_time
                    latencies.append(latency)
                    total_time += latency
                    t6_lat.append(latency)
                    #execute_t6(conn, instruct[1], instruct[2], instruct[3])
                elif transact == 'T':
                    failed_t += run_transaction(conn,
                                                lambda conn: execute_t7(conn))
                    latency = time.time() - start_time
                    latencies.append(latency)
                    total_time += latency
                    t7_lat.append(latency)
                    #conn: execute_t7(conn)
                elif transact == 'R':
                    failed_r += run_transaction(conn, lambda conn: execute_t8(
                        conn, instruct[1], instruct[2], instruct[3]))
                    latency = time.time() - start_time
                    latencies.append(latency)
                    total_time += latency
                    t8_lat.append(latency)
                    #execute_t8(conn, instruct[1], instruct[2], instruct[3])
                no_transact += 1

    succeeded_t = no_transact - failed_d - failed_i - failed_n - \
        failed_o - failed_p - failed_r - failed_s - failed_t
    transaction_throughput = succeeded_t/total_time

    print('successful transacts = ' + str(succeeded_t))
    print("Total failures:")
    print('T1:' + str(failed_n))
    print('T2:' + str(failed_p))
    print('T3:' + str(failed_d))
    print('T4:' + str(failed_i))
    print('T5:' + str(failed_o))
    print('T6:' + str(failed_s))
    print('T7:' + str(failed_t))
    print('T8:' + str(failed_r))

    print('avg latencies:')
    print('T1:' + str(np.average(t1_lat)))
    print('T2:' + str(np.average(t2_lat)))
    print('T3:' + str(np.average(t3_lat)))
    print('T4:' + str(np.average(t4_lat)))
    print('T5:' + str(np.average(t5_lat)))
    print('T6:' + str(np.average(t6_lat)))
    print('T7:' + str(np.average(t7_lat)))
    print('T8:' + str(np.average(t8_lat)))

    # avg latency in ms
    avg_transac_latency = total_time/no_transact * 1000
    # median transaction latency in ms
    med = np.percentile(latencies, 50) * 1000
    # 95th percentile latency in ms
    p95 = np.percentile(latencies, 95) * 1000
    # 99th percentile latency in ms
    p99 = np.percentile(latencies, 99) * 1000

    # write to stderr
    measurements = [str(no_transact), str(total_time), str(
        transaction_throughput), str(avg_transac_latency), str(med), str(p95), str(p99)]
    m_str = ",".join(measurements)
    print(m_str, file=sys.stderr)


def parse_cmdline():
    parser = ArgumentParser(description=__doc__,
                            formatter_class=RawTextHelpFormatter)

    parser.add_argument(
        "dsn",
        help="""\
            database connection string
            """
    )

    parser.add_argument(
        "t_file",
        help="""\
            Filename of transaction workload required
            """
    )

    parser.add_argument("-v", "--verbose",
                        action="store_true", help="print debug info")

    opt = parser.parse_args()
    return opt


if __name__ == "__main__":
    main()
