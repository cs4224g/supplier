from cassandra.query import named_tuple_factory, BatchStatement, SimpleStatement

from cassandra.cluster import Cluster


if __name__ == '__main__':
    # For connecting to multiple clusters
    # cluster = Cluster(['192.168.1.1', '192.168.1.2'])
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('test')
    session.row_factory = named_tuple_factory


    insert_stmt = "INSERT INTO test.example (W_ID, W_NAME, W_CATEGORY) VALUES (%s, %s, %s)"

    # Executes atomically
    batch = BatchStatement()
    batch.add(SimpleStatement(insert_stmt), (1, "kitty1", "cat"))
    batch.add(SimpleStatement(insert_stmt), (2, "kitty2", "cat"))
    batch.add(SimpleStatement(insert_stmt), (3, "doggo1", "dog"))
    session.execute(batch)

    rows = session.execute("SELECT W_ID, W_NAME, W_CATEGORY FROM test.example LIMIT 2")

    for r in rows:
        print(r)
        print(r.w_id, r.w_name, r.w_category)


    cluster.shutdown()