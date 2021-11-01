from cassandra.query import named_tuple_factory, SimpleStatement

def perform_transaction(session):
    session.row_factory = named_tuple_factory
    query_top_balance = SimpleStatement(
        f'SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, C_W_NAME, C_D_NAME \
        FROM wholesale_supplier.top_balance \
        LIMIT 10')
    top_balance = session.execute(query_top_balance)

    for customer in top_balance:
        print(customer.c_first, customer.c_middle, customer.c_middle)
        print(customer.c_balance)
        print(customer.c_w_name)
        print(customer.c_d_name)

def execute_t7(session_input):
    print("T7 program was called!")
    perform_transaction(session_input)
    return 0