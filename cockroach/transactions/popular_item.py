#transaction 6
#l denotes number of last orders to be examined
def execute_t6(connection, w_id, d_id, l):
    print('\n================ executing popular item query ================\n')


    print(w_id, d_id, l)

    
    results = ''
    with connection.cursor() as cur:
        cur.execute('SELECT i_name, ol_quantity, o_id, o_entry_d, c_first, c_middle, c_last\
                    FROM proj.item, (proj.order_line JOIN (\
                    SELECT o_w_id, o_d_id, o_id, o_entry_d, c_first, c_middle, c_last\
                    FROM proj.orders, proj.district, proj.customer\
                    WHERE (o_w_id, o_d_id) = (d_w_id, d_id)\
                    AND (d_w_id, d_id) = (%s, %s)\
                    AND o_id < d_next_o_id AND o_id >= (d_next_o_id - %s)\
                    AND (o_w_id, o_d_id, o_c_id) = (c_w_id, c_d_id, c_id)\
                    ) ON (ol_w_id, ol_d_id, ol_o_id) = (o_w_id, o_d_id, o_id)) last_l\
                    WHERE ol_quantity = (SELECT max(ol_quantity) FROM proj.order_line WHERE (ol_w_id, ol_d_id, ol_o_id) = (%s, %s, last_l.o_id))\
                    AND i_id = ol_i_id AND (ol_w_id, ol_d_id, ol_o_id) = (o_w_id, o_d_id, o_id)', (w_id, d_id, l, w_id, d_id))
        results = cur.fetchall()

    format_res(results)


#format and print results
def format_res(res):
    for row in res:
        output = ''
        for item in row:
            output += str(item) + ' '
        print(output)
