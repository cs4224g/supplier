from cassandra.query import named_tuple_factory, SimpleStatement

def process_input(user_input):
    input_arr = user_input.split(",")
    global w_id
    w_id = input_arr[1]
    global d_id
    d_id = input_arr[2]
    global no_last_orders
    no_last_orders = input_arr[3]


def perform_transaction(session):
    query_next_order_id = session.execute(SimpleStatement(
        f'SELECT D_NEXT_O_ID \
        FROM wholesale_supplier.district \
        WHERE D_W_ID = {w_id} AND D_ID = {d_id}'))
    if not query_next_order_id:
        return 1

    # output 1
    print(w_id, d_id)
    # output 2
    print(no_last_orders)

    max_order_id = query_next_order_id[0].d_next_o_id
    min_order_id = max_order_id - int(no_last_orders)

    query_last_orders = session.execute(SimpleStatement(
        f'SELECT OL_O_ID, OL_O_ENTRY_D, OL_C_FIRST, OL_C_MIDDLE, OL_C_LAST, OL_C_ID \
        FROM wholesale_supplier.order_line \
        WHERE OL_W_ID = {w_id} AND OL_D_ID = {d_id} \
        AND OL_O_ID < {max_order_id} AND OL_O_ID >= {min_order_id}'))
    if not query_last_orders:
        return 1

    order_ids = set()

    for last_order in query_last_orders:
        # output 3a
        print(last_order.ol_o_id, last_order.ol_o_entry_d)
        # output 3b
        print(last_order.ol_c_first, last_order.ol_c_middle, last_order.ol_c_last)
        order_ids.add(last_order.ol_o_id)
    
    order_count = len(order_ids)
    print(order_count)

    items = {}
    items_quantity = {}
    popular_items = {}
 
    for order_id in order_ids:
        query_max_quantity_orderline = session.execute(SimpleStatement(
            f'SELECT MAX(OL_QUANTITY) \
            FROM wholesale_supplier.order_line \
            WHERE OL_W_ID = {w_id} AND OL_D_ID = {d_id} \
            AND OL_O_ID  = {order_id}'))
        if not query_max_quantity_orderline:
            return 1
        max_quantity = query_max_quantity_orderline[0].system_max_ol_quantity
        query_popular_item  = session.execute(SimpleStatement(
            f'SELECT OL_I_NAME, OL_I_ID, OL_QUANTITY \
            FROM wholesale_supplier.order_line WHERE OL_W_ID = {w_id} AND OL_D_ID = {d_id} \
            AND OL_O_ID = {order_id} AND OL_QUANTITY = {max_quantity}'))
        if not query_popular_item:
            return 1
        # there may be multiple popular items in an order
        query_items = query_popular_item
        for query_item in query_items:
            items[query_item.ol_i_id] = query_item.ol_i_name
            items_quantity[query_item.ol_i_id] = query_item.ol_quantity
            popular_items[query_item.ol_i_id] = 0

    
    for item_id in items:
        # output 3c
        print(items[item_id], items_quantity[item_id])
        query_popular_items = session.execute(SimpleStatement(
            f'SELECT COUNT(OL_O_ID) \
            FROM wholesale_supplier.order_line_by_item WHERE  OL_I_ID = {item_id} \
            AND OL_W_ID = {w_id} AND OL_D_ID = {d_id} \
            AND OL_O_ID < {max_order_id} AND OL_O_ID >= {min_order_id}'))
        if not query_popular_items:
            return 1
        order_count_for_item = query_popular_items[0].system_count_ol_o_id
        popular_items[item_id] = order_count_for_item
    
    for item_id in popular_items:
        item_count = popular_items[item_id]
        item_name = items[item_id]
        percentage_of_orders_with_popular_items = (item_count/order_count) * 100
        # output 4a
        print(item_name, percentage_of_orders_with_popular_items)

    return 0
    
def execute_t6(session_input, user_input):
    print("T6 program was called!")
    process_input(user_input)
    perform_transaction(session_input)
    return 0

def format_list(items):
    res = '('
    for item in items:
        res = res + f'{item},'
    res = res[:-1] + ')'
    return res 