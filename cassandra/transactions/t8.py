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
    # Test: R,10,9,2819
    query_items = SimpleStatement(
        f'SELECT OL_O_ID, OL_I_ID \
        FROM wholesale_supplier.order_line_by_customer \
        WHERE OL_W_ID={w_id} AND OL_D_ID={d_id} AND OL_C_ID={c_id}'
    )
    order_items = session.execute(query_items)

    # Maps order_id to a set of items
    order_items_dict = {}
    for order_item in order_items:
        o_id = order_item.ol_o_id
        i_id = order_item.ol_i_id
        if o_id in order_items_dict:
            order_items_dict[o_id].append(i_id)
        else:
            order_items_dict[o_id] = [i_id]
    
    result = set()
    
    # For each order
    for o_id in order_items_dict:
        potential_matches = set()
        # For each item, find another order with this item
        order_items = order_items_dict[o_id]

        for i_id in order_items:
            query_matching_orders_1 = SimpleStatement(
                f'SELECT OL_W_ID, OL_D_ID, OL_O_ID, OL_C_ID  \
                FROM wholesale_supplier.order_line_by_item \
                WHERE OL_I_ID = {i_id} and OL_W_ID < {w_id}'
            )
            matching_orders_1 = session.execute(query_matching_orders_1)
            query_matching_orders_2 = SimpleStatement(
                f'SELECT OL_W_ID, OL_D_ID, OL_C_ID, OL_O_ID  \
                FROM wholesale_supplier.order_line_by_item \
                WHERE OL_I_ID = {i_id} and OL_W_ID > {w_id}'
            )
            matching_orders_2 = session.execute(query_matching_orders_2)
            matching_orders = [m for m in matching_orders_1] + [m for m in matching_orders_2]

            for m in matching_orders:
                order_id = (m.ol_w_id, m.ol_d_id, m.ol_o_id)
                cust_id = (m.ol_w_id, m.ol_d_id, m.ol_c_id)
                if cust_id in result:
                    continue
                elif order_id in potential_matches:
                    result.add(cust_id)
                else:
                    potential_matches.add(order_id)

    for r in result:
        w_id_result, d_id_result, c_id_result = r
        print(f'{w_id_result} {d_id_result} {c_id_result}')

def execute_t8(session_input, user_input):
    print("T8 program was called!")
    process_input(user_input)
    perform_transaction(session_input)
    return 0


def format_list(items):
    res = '('
    for item in items:
        res = res + f'{item},'
    res = res[:-1] + ')'
    return res