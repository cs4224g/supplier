#transaction 8


def execute_t8(connection, c_w_id, c_d_id, c_id):
    print('\n================ executing related_customer query ================\n')

    #print("Customer identifier: ")
    print(c_w_id, c_d_id, c_id)

    results = ''
    with connection.cursor() as cur:
        #table a represents all the items ever bough by customer with identifier c_w_id, c_d_id, c_id
        cur.execute("SELECT c_w_id, c_d_id, c_id, o.ol_o_id, COUNT(DISTINCT o.ol_i_id) \
                    FROM proj.customer, (proj.orders \
                    INNER JOIN proj.order_line ON (o_w_id, o_d_id, o_id) = (ol_w_id, ol_d_id, ol_o_id)) o, \
                    ( \
                        SELECT ol_o_id, ol_i_id \
                        FROM proj.orders, proj.order_line \
                        WHERE (ol_w_id, ol_d_id) = (%s, %s) \
                        AND (o_w_id, o_d_id) = (%s, %s) \
                        AND o_id = ol_o_id \
                        AND o_c_id = %s \
                    ) a \
                    WHERE o_w_id <> %s\
                    AND o.ol_i_id = a.ol_i_id \
                    AND (c_w_id, c_d_id, c_id) = (o_w_id, o_d_id, o_c_id) \
                    GROUP BY (c_w_id, c_d_id, c_id, o.ol_w_id, o.ol_d_id, o.ol_o_id)",
                    (c_w_id, c_d_id, c_w_id, c_d_id, c_id, c_w_id)
                    )
        results = cur.fetchall()
        
    final = []
    for record in results:
        #record[4] represents the number of items in common
        if(record[4] >= 2): 
            final.append([record[0], record[1], record[2]])
    format_res(final)

#format and prints results
def format_res(res):
    for row in res:
        output = ''
        for item in row:
            output += str(item) + ' '
        print(output)
