import sys, time
import numpy as np

from cassandra.query import named_tuple_factory, BatchStatement, SimpleStatement

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RetryPolicy, WhiteListRoundRobinPolicy
from cassandra import ConsistencyLevel
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

# maps xact num to [total_xact_cnt, total_exec_time, failed_xact_cnt]
xact_info = [[0,0,0] for i in range(9)]
latencies = []

if __name__ == '__main__':
    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
        # retry_policy=RetryPolicy(), # DEFAULT
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        request_timeout=15,
        # row_factory=named_tuple_factory
    )
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect('wholesale_supplier')
    # session.row_factory = named_tuple_factory

    num_xacts = 0
    cnt = 0
    total_exec_time = 0 # in seconds

    for line in sys.stdin:
        input_arr = line.split(",")
        xact = input_arr[0].strip()
        cnt += 1
        print(f'{line.strip()} | Xact {cnt}')
        start_time = time.time()
        
        isFail = 0 # fail status
        if(xact == 'N'):
            isFail = execute_t1(session, input_arr)
        elif(xact == 'P'):
            isFail = execute_t2(session, input_arr)
        elif(xact == 'D'):
            isFail = execute_t3(session, input_arr)
        elif (xact == 'O'):
            isFail = execute_t4(session, line)
        elif (xact == 'S'):
            isFail = execute_t5(session, line)
        elif (xact == 'I'):
            isFail = execute_t6(session, line)
        elif (xact == 'T'):
            isFail = execute_t7(session)
        elif (xact == 'R'):
            isFail = execute_t8(session, line)
        else:
            print('fall thru', xact)

        latency_seconds = time.time() - start_time
        total_exec_time += latency_seconds
        num_xacts += (1 - isFail)
        latencies.append(latency_seconds)


        # Transaction-specific latencies
        xact_num = xact_map[xact]
        xact_info[xact_num][0] += 1
        xact_info[xact_num][1] += latency_seconds
        xact_info[xact_num][2] += isFail

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
    for i in range(1,9):
        print(f'T{i}: {xact_info[i][2]}/{xact_info[i][0]}')
    

    print("Average transaction latency: ")
    for xact_num in range(1,9):
        total_time = xact_info[xact_num][1]
        total_count = xact_info[xact_num][0]
        xact_avg_latency = total_time / total_count if total_count > 0 else Inf
        xact_metric = f'T{xact_num}: {xact_avg_latency}s'
        print(xact_metric)
