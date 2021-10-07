import sys, time

from cassandra.query import named_tuple_factory, BatchStatement, SimpleStatement

from cassandra.cluster import Cluster

from transactions.t4 import execute_t4
from transactions.t5 import execute_t5
from transactions.t6 import execute_t6
from transactions.t7 import execute_t7
from transactions.t8 import execute_t8


if __name__ == '__main__':
    # For connecting to multiple clusters
    # cluster = Cluster(['192.168.1.1', '192.168.1.2'])
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('wholesale_supplier')
    session.row_factory = named_tuple_factory

    for line in sys.stdin:
        input_arr = line.split(",")
        xact = input_arr[0].strip()
        start_time = time.time()
        if (xact == 'O'):
            execute_t4(session, line)
        elif (xact == 'S'):
            execute_t5(session, line)
        elif (xact == 'I'):
            execute_t6(session, line)
        elif (xact == 'T'):
            execute_t7(session)
        elif (xact == 'R'):
            execute_t8(session, line)
        print(f'Time taken: {time.time() - start_time}')

    cluster.shutdown()