import sys, time
import numpy as np

from cassandra.query import named_tuple_factory, BatchStatement, SimpleStatement

from cassandra.cluster import Cluster
from numpy.core.numeric import Inf

from transactions.t1 import execute_t1
from transactions.t2 import execute_t2
from transactions.t3 import execute_t3
from transactions.t4 import execute_t4
from transactions.t5 import execute_t5
from transactions.t6 import execute_t6
from transactions.t7 import execute_t7
from transactions.t8 import execute_t8

xact_map = {
    "N":1,
    "P":2,
    "D":3,
    "O":4,
    "S":5,
    "I":6,
    "T":7,
    "R":8
}

xact_latency = [[0,0] for i in range(9)]
latencies = []

if __name__ == '__main__':
    # For connecting to multiple clusters
    # cluster = Cluster(['192.168.1.1', '192.168.1.2'])
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('wholesale_supplier')
    session.row_factory = named_tuple_factory

    num_xacts = 0
    total_exec_time = 0 # in seconds

    xact_count_t1 = 0
    xact_count_t2 = 0
    xact_count_t3 = 0
    xact_count_t4 = 0
    xact_count_t5 = 0
    xact_count_t6 = 0

    failure_count_t1 = 0
    failure_count_t2 = 0
    failure_count_t3 = 0
    failure_count_t4 = 0
    failure_count_t5 = 0
    failure_count_t6 = 0

    for line in sys.stdin:
        input_arr = line.split(",")
        xact = input_arr[0].strip()

        print(f'{line.strip()} | Xact {num_xacts+1}')
        start_time = time.time()
        
        if(xact == 'N'):
            failure_count_t1 += execute_t1(session, input_arr)
            xact_count_t1 += 1
        elif(xact == 'P'):
            failure_count_t2 += execute_t2(session, input_arr)
            xact_count_t2 += 1
        elif(xact == 'D'):
            failure_count_t3 += execute_t3(session, input_arr)
            xact_count_t3 += 1
        elif (xact == 'O'):
            failure_count_t4 += execute_t4(session, line)
            xact_count_t4 += 1
        elif (xact == 'S'):
            failure_count_t5 += execute_t5(session, line)
            xact_count_t5 += 1
        elif (xact == 'I'):
            failure_count_t6 += execute_t6(session, line)
            xact_count_t6 += 1
        elif (xact == 'T'):
            execute_t7(session)
        elif (xact == 'R'):
            execute_t8(session, line)
        else:
            print('fall thru', xact)

        latency_seconds = time.time() - start_time
        total_exec_time += latency_seconds
        num_xacts += 1
        latencies.append(latency_seconds)


        # Transaction-specific latencies
        xact_num = xact_map[xact]
        xact_latency[xact_num][0] += 1
        xact_latency[xact_num][1] += latency_seconds

    cluster.shutdown()

    throughput = num_xacts / total_exec_time if total_exec_time > 0 else 0
    avg_latency = total_exec_time / num_xacts * 1000 if num_xacts > 0 else Inf # in ms
    median_latency = np.percentile(latencies, 50) * 1000
    p95_latency = np.percentile(latencies, 95) * 1000
    p99_latency = np.percentile(latencies, 99) * 1000

    metrics = "{},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f}".format(
        num_xacts,
        total_exec_time,
        throughput,
        avg_latency,
        median_latency,
        p95_latency,
        p99_latency
    )
    print(metrics, file=sys.stderr)

    print("Total failures: ")
    print(f'T1: {failure_count_t1}/{xact_count_t1}')
    print(f'T2: {failure_count_t2}/{xact_count_t2}')
    print(f'T3: {failure_count_t3}/{xact_count_t3}')
    print(f'T4: {failure_count_t4}/{xact_count_t4}')
    print(f'T5: {failure_count_t5}/{xact_count_t5}')
    print(f'T5: {failure_count_t6}/{xact_count_t6}')
    

    print("Average transaction latency: ")
    for xact_num in range(1,9):
        total_time = xact_latency[xact_num][1]
        total_count = xact_latency[xact_num][0]
        xact_avg_latency = total_time / total_count if total_count > 0 else Inf
        xact_metric = f'T{xact_num}: {xact_avg_latency}s'
        print(xact_metric)
