#transaction 7


def execute_t7(connection):

    print('\n================ executing top_balance query ================\n')

    results = ''
    with connection.cursor() as cur:
        cur.execute("SELECT c_first, c_middle, c_last, c_balance, w_name, d_name \
                    FROM proj.customer, proj.warehouse, proj.district \
                    WHERE c_d_id = d_id AND d_w_id = w_id AND c_w_id = w_id \
                    ORDER BY c_balance DESC LIMIT 10;"
                    )
        results = cur.fetchall()
    
    format_res(results)

#format: space separated with new line for each output
#format and prints results
def format_res(res):
    for row in res:
        output = ''
        for item in row:
            output += str(item) + ' '
        print(output)


