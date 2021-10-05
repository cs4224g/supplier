import sys, time

from cassandra.query import named_tuple_factory, BatchStatement, SimpleStatement

from cassandra.cluster import Cluster

from transactions.t4 import execute_t4
from transactions.t8 import execute_t8
# from transactions.t5 import execute_t5


if __name__ == '__main__':
    # For connecting to multiple clusters
    # cluster = Cluster(['192.168.1.1', '192.168.1.2'])
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('wholesale_supplier')
    session.row_factory = named_tuple_factory

    for line in sys.stdin:
        input_arr = line.split(",")
        xact = input_arr[0]
        start_time = time.time()
        # TODO: Add respective if statements for other Xact types
        if (xact == 'O'):
            execute_t4(session, line)
        if (xact == 'R'):
            execute_t8(session, line)
        # elif (xact == 'S'):
        #     execute_t5(session, line)

        print(f'Time taken: {time.time() - start_time}')

    cluster.shutdown()