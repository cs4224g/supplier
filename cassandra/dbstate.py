"""
IMPORTANT!!!
These queries are slow and will exceed default timeout values. They must be increased on BOTH server and driver side. Cos timeout will be the minimum of the 2.
To fix:
    Server side:
    - Go to cassamdra.yaml file and increase value of `read_request_timeout_in_ms` AND `range_request_timeout_in_ms`. 
        See: https://docs.datastax.com/en/developer/python-driver/3.9/api/cassandra/#cassandra.ReadTimeout
    - restart cassandra node
    Client side:
    - increased session.default_timeout

Currently using 2 mins, seems to work fine.
"""
import time
from cassandra.cluster import Cluster
from cassandra.query import named_tuple_factory, SimpleStatement

if __name__ == '__main__':
    # For connecting to multiple clusters
    # cluster = Cluster(['192.168.1.1', '192.168.1.2'])
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('wholesale_supplier')
    session.row_factory = named_tuple_factory
    session.default_timeout = 120 

    print('Quering final db state. This will take a few minutes...')
    start_time = time.time()    

    f = open('dbstate.csv','w')

    query_strings = [
        'select sum(W_YTD) from warehouse;',
        'select sum(D_YTD), sum(D_NEXT_O_ID) from district;',
        'select sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) from customer;',
        'select max(O_ID), sum(O_OL_CNT) from orders;',
        'select sum(OL_AMOUNT), sum(OL_QUANTITY) from order_line;',
        'select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from stock;',
    ]

    for query_string in query_strings:    
        res = session.execute(SimpleStatement(query_string)).one()
        print(res)
        f.write(''.join([str(c) + '\n' for c in res]))

    print(f'Time taken: {time.time() - start_time}')

    f.close()