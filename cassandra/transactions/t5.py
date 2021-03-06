from cassandra.query import named_tuple_factory, SimpleStatement, ValueSequence

def process_input(user_input):
    # input_arr = sys.stdin.readline().split(",")
    input_arr = user_input.split(",")
    global w_id
    w_id = input_arr[1]
    global d_id
    d_id = input_arr[2]
    global threshold
    threshold = input_arr[3]
    global no_last_orders
    no_last_orders = int(input_arr[4])


def perform_transaction(session):
    query_next_avail_order = session.execute(SimpleStatement(
        f'SELECT D_NEXT_O_ID \
        FROM wholesale_supplier.district \
        WHERE D_W_ID = {w_id} AND D_ID = {d_id}'))
    
    if not query_next_avail_order:
        return 1
    next_avail_order_id = query_next_avail_order[0].d_next_o_id

    query_last_orders = session.execute(SimpleStatement(
        f'SELECT OL_I_ID\
        FROM wholesale_supplier.order_line \
        WHERE OL_W_ID = {w_id} AND OL_D_ID = {d_id} \
        AND OL_O_ID < {next_avail_order_id} AND OL_O_ID >= {next_avail_order_id - no_last_orders}'))
    
    if not query_last_orders:
        return 1
    
    retrieved_item_ids = set()
    for row in query_last_orders:
        retrieved_item_ids.add(row.ol_i_id)
    
    query_items_stock_below_threshold = session.execute(SimpleStatement(
        f'SELECT COUNT(*)\
        FROM wholesale_supplier.stock \
        WHERE S_W_ID = {w_id} AND S_I_ID IN {format_list(retrieved_item_ids)} \
        AND S_QUANTITY < {threshold};'))

    if not query_items_stock_below_threshold:
        return 1
    items_count = query_items_stock_below_threshold
    print(f'{items_count[0].count}')
    return 0

def execute_t5(session_input, user_input):
    print("T5 program was called!")
    process_input(user_input)
    perform_transaction(session_input)
    return 0

def format_list(items):
    res = '('
    for item in items:
        res = res + f'{item},'
    res = res[:-1] + ')'
    return res 