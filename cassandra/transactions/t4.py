from cassandra.query import named_tuple_factory, SimpleStatement

def process_input(user_input):
    input_arr = user_input.split(",")
    global w_id
    w_id = input_arr[1]
    global d_id
    d_id = input_arr[2]
    global c_id
    c_id = input_arr[3]


def perform_transaction(session):
    session.row_factory = named_tuple_factory
    query_cust = session.execute(SimpleStatement(
        f'SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE \
        FROM wholesale_supplier.customer \
        WHERE C_W_ID = {w_id} AND C_D_ID = {d_id} AND C_ID = {c_id};'))
    if query_cust:
        cust = query_cust[0]
        print(cust.c_first, cust.c_middle, cust.c_last, cust.c_balance)
    else:
        return 1

    query_order_status = session.execute(SimpleStatement(
        f'SELECT O_ID, O_ENTRY_D, O_CARRIER_ID \
        FROM wholesale_supplier.order_status \
        WHERE O_W_ID = {w_id} AND O_D_ID = {d_id}\
         AND O_C_ID = {c_id} LIMIT 1;'))
    if query_order_status:
        retrieved_order = query_order_status[0]
        print(retrieved_order.o_id, retrieved_order.o_entry_d, retrieved_order.o_carrier_id)
    else:
        return 1

    query_orderline = session.execute(SimpleStatement(
        f'SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D \
        FROM wholesale_supplier.order_line\
         WHERE OL_W_ID = {w_id} AND OL_D_ID = {d_id}\
          AND OL_O_ID = {retrieved_order.o_id}'))
    if query_orderline:
        for orderline in query_orderline:
            print(orderline.ol_i_id, orderline.ol_supply_w_id, orderline.ol_quantity, orderline.ol_amount, orderline.ol_delivery_d)
    else:
        return 1

def execute_t4(session_input, user_input):
    print("T4 program was called!")
    process_input(user_input)
    perform_transaction(session_input)
    return 0