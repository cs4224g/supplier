from cassandra.query import named_tuple_factory, SimpleStatement

def process_input(user_input):
    # input_arr = sys.stdin.readline().split(",")
    input_arr = user_input.strip().split(",")
    global w_id
    w_id = input_arr[1]
    global d_id
    d_id = input_arr[2]
    global c_id
    c_id = input_arr[3]


def perform_transaction(session):
    session.row_factory = named_tuple_factory
    query_cust = SimpleStatement("SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM wholesale_supplier.customer WHERE C_W_ID = {w_id} AND C_D_ID = {d_id} AND C_ID = {c_id};".format(w_id = w_id, d_id = d_id, c_id = c_id))
    cust = session.execute(query_cust)[0]
    print(cust.c_first, cust.c_middle, cust.c_last, cust.c_balance)

    query_order_status = SimpleStatement("SELECT O_ID, O_ENTRY_D, O_CARRIER_ID FROM wholesale_supplier.order_status WHERE O_W_ID = {w_id} AND O_D_ID = {d_id} AND O_C_ID = {c_id} LIMIT 1;".format(w_id = w_id, d_id = d_id, c_id = c_id))
    retrieved_order = session.execute(query_order_status)[0]
    print(retrieved_order.o_id, retrieved_order.o_entry_d, retrieved_order.o_carrier_id)

    query_orderline = SimpleStatement("SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM wholesale_supplier.order_line WHERE OL_W_ID = {w_id} AND OL_D_ID = {d_id} AND OL_O_ID = {o_id};".format(w_id = w_id, d_id = d_id, o_id = retrieved_order.o_id))
    orderlines = session.execute(query_orderline)
    for orderline in orderlines:
        print(orderline.ol_i_id, orderline.ol_supply_w_id, orderline.ol_quantity, orderline.ol_amount, orderline.ol_delivery_d)

def execute_t4(session_input, user_input):
    print("T4 program was called!")
    process_input(user_input)
    perform_transaction(session_input)