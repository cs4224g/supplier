#transaction 4
def execute_t4(connection, c_w_id, c_d_id, c_id):
    print('\n============== executing order-status query ================\n')
    results = []

    
    with connection.cursor() as cur:

        cur.execute("SELECT c_first, c_middle, c_last, c_balance, o_id, o_carrier_id, o_entry_d, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d \
                    FROM (proj.customer\
                    JOIN proj.orders ON (o_w_id, o_d_id, o_c_id) = (c_w_id, c_d_id, c_id)), proj.order_line\
                    WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s\
                    AND o_entry_d = (SELECT max(o_entry_d) FROM proj.customer\
                    JOIN proj.orders ON (o_w_id, o_d_id, o_c_id) = (c_w_id, c_d_id, c_id)\
                    WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s) \
                    AND (ol_w_id, ol_d_id, ol_o_id) = (o_w_id, o_d_id, o_id)", 
                    (c_w_id, c_d_id, c_id, c_w_id, c_d_id, c_id))
     
        for row in cur:
            results.append(row)
    #print(results)
    format_res(results)

#format and prints results
def format_res(res):
    for row in res:
        output = ''
        for item in row:
            output += str(item) + ' '
    print(output)



