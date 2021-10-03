import sys

from cassandra.query import named_tuple_factory, BatchStatement, SimpleStatement

from cassandra.cluster import Cluster

from transactions.t2 import execute_t2
from transactions.t4 import execute_t4
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
        # TODO: Add respective if statements for other Xact types
        if (xact == 'O'):
            execute_t4(session, line)
        elif(xact == 'P'):
            execute_t2(session, input_arr)
        # elif (xact == 'S'):
        #     execute_t5(session, line)



    cluster.shutdown()