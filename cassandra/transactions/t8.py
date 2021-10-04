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
    # R,10,9,2819
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
    potential_matches = set()
    # For each order
    for o_id in order_items_dict:
        # For each item, find another order with this item
        for i in range(len(order_items_dict[o_id]) - 1): # exclude last element
            i_id = order_items_dict[o_id][i]
            query_matching_orders_1 = SimpleStatement(
                f'SELECT OL_W_ID, OL_D_ID, OL_O_ID, OL_C_ID  \
                FROM wholesale_supplier.order_line_by_warehouse_item \
                WHERE OL_I_ID = {i_id} and OL_W_ID < {w_id}'
            )
            matching_orders_1 = session.execute(query_matching_orders_1)
            for m in matching_orders_1:
                potential_matches.add(m)

            query_matching_orders_2 = SimpleStatement(
                f'SELECT OL_W_ID, OL_D_ID, OL_C_ID, OL_O_ID  \
                FROM wholesale_supplier.order_line_by_warehouse_item \
                WHERE OL_I_ID = {i_id} and OL_W_ID > {w_id}'
            )
            matching_orders_2 = session.execute(query_matching_orders_2)
            for m in matching_orders_2:
                potential_matches.add(m)

            rem_items = order_items_dict[o_id][i+1:]
            rem_items_string = format_list(rem_items)

            # For each matching order, check if they match on a second item
            for m in potential_matches:
                w_id_2 = m.ol_w_id
                d_id_2 = m.ol_d_id
                c_id_2 = m.ol_c_id
                o_id_2 = m.ol_o_id
                if (w_id_2, d_id_2, c_id_2) in result:
                    continue
                
                query_match_customer = SimpleStatement(
                    f'SELECT OL_C_ID \
                    FROM wholesale_supplier.order_line_by_order \
                    WHERE OL_W_ID={w_id_2} AND OL_D_ID={d_id_2} AND OL_O_ID={o_id_2} AND \
                    OL_I_ID in {rem_items_string};'
                )
                match_customer = session.execute(query_match_customer)
                if match_customer:
                    result.add((w_id_2, d_id_2, c_id_2))

        
                
    
    # for potential_match in potential_matches:
    #     o_id, m = potential_match
    #     items_o_1 = set(order_items_dict[o_id])

    #     w_id_2 = order_item.ol_w_id
    #     d_id_2 = order_item.ol_d_id
    #     o_id_2 = order_item.ol_o_id
    #     c_id_2 = order_item.ol_c_id
    

    #     query_items_o_2 = SimpleStatement(
    #         f'SELECT OL_I_ID \
    #             FROM wholesale_supplier.order_line \
    #             WHERE OL_W_ID={w_id_2}, OL_D_ID={d_id_2}, OL_O_ID={o_id_2}'
    #     )
    #     res_items_o_2 = session.execute(query_items_o_2)
    #     items_o_2 = set()
    #     for item in res_items_o_2:
    #         items_o_2.add(item.ol_i_id)
    #     if len(items_o_1.intersection(items_o_2) > 1):
    #         result.add((w_id_2, d_id_2, c_id_2))

    for r in result:
        w_id_result, d_id_result, c_id_result = r
        print(f'{w_id_result} {d_id_result} {c_id_result}')

def execute_t8(session_input, user_input):
    print("T8 program was called!")
    process_input(user_input)
    perform_transaction(session_input)


def format_list(items):
    res = '('
    for item in items:
        res = res + f'{item},'
    res = res[:-1] + ')'
    return res