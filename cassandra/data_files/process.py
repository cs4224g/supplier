import pandas as pd

def print_headers(df):
    headers = ''
    for i in range(len(df.columns)):
        headers += f'\'{df.columns[i]}\''
        if i != len(df.columns) - 1:
            headers += ','
    #     print(f'{i}\t{df.columns[i]}')
    print(headers)

def readCSV():
    # reading raw csv files
    global warehouse
    global district
    global customer
    global order
    global item
    global order_line
    global stock
    warehouse = pd.read_csv('warehouse_header.csv',dtype=str)
    district = pd.read_csv('district_header.csv',dtype=str)
    customer = pd.read_csv('customer_header.csv',dtype=str)
    order = pd.read_csv('order_header.csv',dtype=str)
    item = pd.read_csv('item_header.csv',dtype=str)
    order_line = pd.read_csv('order-line_header.csv',dtype=str)
    stock = pd.read_csv('stock_header.csv',dtype=str)

    warehouse.columns = warehouse.columns.str.lower()
    district.columns = district.columns.str.lower()
    customer.columns = customer.columns.str.lower()
    order.columns = order.columns.str.lower()
    item.columns = item.columns.str.lower()
    order_line.columns = order_line.columns.str.lower()
    stock.columns = stock.columns.str.lower()

    # Convert null o_carrier_id entries of order table to -1
    df = order
    df = df[~df['o_carrier_id'].isna()]
    sum(df['o_carrier_id'].astype(int) < 0) # verify non-null carrier ids are all positive
    # replace NA o_carrier_id with -1
    order['o_carrier_id'].fillna(-1, inplace = True)


def warehouse_table():
    global warehouse_cass
    warehouse_cass = warehouse
    pri_col = ['w_id']
    rem_col = sorted(['w_name', 'w_street_1', 'w_street_2', 'w_city', 'w_state', 'w_zip', 'w_tax', 'w_ytd'])
    all_col = pri_col + rem_col
    warehouse_cass = warehouse_cass[all_col]
    print("warehouse")
    print_headers(warehouse_cass)
    warehouse_cass.to_csv('warehouse_cass.csv', index=False)

def district_table():
    global district_cass
    district_cass = pd.merge(district, warehouse,
                    left_on='d_w_id',
                    right_on='w_id',
                    how='inner')
    district_cass = district_cass.rename(columns={
        'w_street_1': 'd_w_street_1', 
        'w_street_2': 'd_w_street_2', 
        'w_city': 'd_w_city', 
        'w_state': 'd_w_state',
        'w_zip': 'd_w_zip',
        'w_tax': 'd_w_tax'
    })

    pri_col = ['d_w_id','d_id']
    rem_col = sorted(['d_name', 'd_street_1', 'd_street_2', 'd_city', 'd_state', 'd_zip', 'd_tax', 'd_ytd', 'd_next_o_id', 'd_w_street_1', 'd_w_street_2', 'd_w_city', 'd_w_state', 'd_w_zip', 'd_w_tax'])
    all_col = pri_col + rem_col
    district_cass = district_cass[all_col]

    print("district")
    print_headers(district_cass)
    district_cass.to_csv('district_cass.csv', index=False)


def customer_table():
    global customer_cass
    customer_cass = pd.merge(customer, district,
                    left_on=['c_w_id', 'c_d_id'],
                    right_on=['d_w_id', 'd_id'],
                    how='inner')
    customer_cass = pd.merge(customer_cass, warehouse,
                    left_on=['c_w_id'],
                    right_on=['w_id'],
                    how='inner')
    customer_cass = customer_cass.rename(columns={
        'd_name': 'c_d_name',
        'w_name': 'c_w_name'
    })
    pri_col = ['c_w_id', 'c_d_id', 'c_id', 'c_balance']
    rem_col = sorted(['c_first', 'c_middle', 'c_last', 'c_street_1', 'c_street_2', 'c_city', 'c_state', 'c_zip', 'c_phone', 'c_since', 'c_credit', 'c_credit_lim', 'c_discount', 'c_ytd_payment', 'c_payment_cnt', 'c_delivery_cnt', 'c_data', 'c_d_name', 'c_w_name'])
    all_col = pri_col + rem_col
    customer_cass = customer_cass[all_col]

    print("customer")
    print_headers(customer_cass)
    customer_cass.to_csv('customer_cass.csv', index=False)
    

def order_table():
    global order_cass
    order_cass = pd.merge(order, customer,
                    left_on=['o_w_id', 'o_d_id', 'o_c_id'],
                    right_on=['c_w_id', 'c_d_id', 'c_id'],
                    how='inner')
    order_cass = order_cass.rename(columns={
        'c_first': 'o_c_first',
        'c_middle': 'o_c_middle',
        'c_last': 'o_c_last'
    })
    pri_col = ['o_w_id', 'o_d_id', 'o_id', 'o_c_id']
    rem_col = sorted(['o_carrier_id', 'o_ol_cnt', 'o_all_local', 'o_entry_d', 'o_c_first', 'o_c_middle', 'o_c_last'])
    all_col = pri_col + rem_col
    order_cass = order_cass[all_col]
    print("order")
    print_headers(order_cass)
    order_cass.to_csv('order_cass.csv', index=False)
    
def order_by_carrier_id_table():
    global order_by_carrier_id_cass
    order_by_carrier_id_cass = pd.merge(order, customer,
                    left_on=['o_w_id', 'o_d_id', 'o_c_id'],
                    right_on=['c_w_id', 'c_d_id', 'c_id'],
                    how='inner')
    order_by_carrier_id_cass = order_by_carrier_id_cass.rename(columns={
        'c_first': 'o_c_first',
        'c_middle': 'o_c_middle',
        'c_last': 'o_c_last'
    })
    pri_col = ['o_w_id', 'o_d_id', 'o_carrier_id', 'o_id', 'o_c_id'] # o_carrier_id in PK
    rem_col = sorted(['o_ol_cnt', 'o_all_local', 'o_entry_d', 'o_c_first', 'o_c_middle', 'o_c_last'])
    all_col = pri_col + rem_col
    order_by_carrier_id_cass = order_by_carrier_id_cass[all_col]
    print("order_by_carrier_id")
    print_headers(order_by_carrier_id_cass)
    order_by_carrier_id_cass.to_csv('order_by_carrier_id_cass.csv', index=False)

def item_table():
    global item_cass
    item_cass = item
    pri_col = ['i_id']
    rem_col = sorted(['i_name', 'i_price', 'i_im_id', 'i_data'])
    all_col = pri_col + rem_col
    item_cass = item_cass[all_col]
    print("item")
    print_headers(item_cass)
    item_cass.to_csv('item_cass.csv', index=False)
    

def order_line_table():
    global order_line_cass
    order_line_cass = pd.merge(order_line, item,
                    left_on=['ol_i_id'],
                    right_on=['i_id'],
                    how='inner')
    order_line_cass = pd.merge(order_line_cass, order_cass,
                    left_on=['ol_w_id', 'ol_d_id', 'ol_o_id'],
                    right_on=['o_w_id', 'o_d_id', 'o_id'],
                    how='inner')

    order_line_cass = order_line_cass.rename(columns={
        'i_name': 'ol_i_name',
        'i_price': 'ol_i_price',
        'o_c_id': 'ol_c_id',
        'o_entry_d': 'ol_o_entry_d',
        'o_c_first': 'ol_c_first',
        'o_c_last': 'ol_c_last',
        'o_c_middle': 'ol_c_middle',
    })

    pri_col = ['ol_w_id', 'ol_d_id', 'ol_o_id', 'ol_quantity', 'ol_number', 'ol_c_id']
    rem_col = sorted(['ol_i_id', 'ol_delivery_d', 'ol_amount', 'ol_supply_w_id', 'ol_dist_info', 'ol_i_name', 'ol_i_price', 'ol_o_entry_d', 'ol_c_first', 'ol_c_middle', 'ol_c_last'])
    all_col = pri_col + rem_col
    order_line_cass = order_line_cass[all_col]
    print("order_line")
    print_headers(order_line_cass)
    order_line_cass.to_csv('order_line_cass.csv', index=False)

    

def stock_table():
    global stock_cass
    stock_cass = stock
    pri_col = ['s_w_id','s_i_id','s_quantity']
    rem_col = sorted(['s_ytd', 's_order_cnt', 's_remote_cnt', 's_dist_01', 's_dist_02', 's_dist_03', 's_dist_04', 's_dist_05', 's_dist_06', 's_dist_07', 's_dist_08', 's_dist_09', 's_dist_10', 's_data'])
    all_col = pri_col + rem_col
    stock_cass = stock_cass[all_col]
    print("stock")
    print_headers(stock_cass)
    stock_cass.to_csv('stock_cass.csv', index=False)



def top_balance_table():
    global top_balance_cass
    top_balance_cass = customer_cass[['c_w_id', 'c_d_id', 'c_id', 'c_first', 'c_middle', 'c_last', 'c_balance', 'c_w_name', 'c_d_name']]
    pri_col = ['c_w_id', 'c_d_id', 'c_balance', 'c_id']
    rem_col = sorted(['c_first', 'c_middle', 'c_last', 'c_w_name', 'c_d_name'])
    all_col = pri_col + rem_col
    top_balance_cass = top_balance_cass[all_col]
    print("top_balance")
    print_headers(top_balance_cass)
    top_balance_cass.to_csv('top_balance_cass.csv', index=False)


def order_status_table():
    global order_status_cass
    order_status_cass = pd.merge(order, customer,
                    left_on=['o_w_id', 'o_d_id', 'o_c_id'],
                    right_on=['c_w_id', 'c_d_id', 'c_id'],
                    how='inner')
    order_status_cass = order_status_cass.rename(columns={
        'c_first': 'o_c_first',
        'c_middle': 'o_c_middle',
        'c_last': 'o_c_last'
    })
    pri_col = ['o_w_id', 'o_d_id', 'o_c_id', 'o_id']
    rem_col = sorted(['o_carrier_id', 'o_ol_cnt', 'o_all_local', 'o_entry_d', 'o_c_first', 'o_c_middle', 'o_c_last'])
    all_col = pri_col + rem_col
    order_status_cass = order_status_cass[all_col]
    print("order_status")
    print_headers(order_status_cass)
    order_status_cass.to_csv('order_status_cass.csv', index=False)
    

def order_line_by_customer_table():
    global order_line_by_customer_cass
    order_line_by_customer_cass = order_line_cass
    pri_col = ['ol_w_id', 'ol_d_id', 'ol_c_id', 'ol_o_id', 'ol_i_id', 'ol_number']
    rem_col = sorted([])
    all_col = pri_col + rem_col
    order_line_by_customer_cass = order_line_by_customer_cass[all_col]
    print("order_line_by_customer")
    print_headers(order_line_by_customer_cass)
    order_line_by_customer_cass.to_csv('order_line_by_customer_cass.csv', index=False)

def order_line_by_item_table():
    global order_line_by_item_cass
    order_line_by_item_cass = order_line_cass
    pri_col = ['ol_i_id', 'ol_w_id', 'ol_d_id','ol_o_id', 'ol_number']
    rem_col = sorted(['ol_c_id'])
    all_col = pri_col + rem_col
    order_line_by_item_cass = order_line_by_item_cass[all_col]
    print("order_line_by_item")
    print_headers(order_line_by_item_cass)
    order_line_by_item_cass.to_csv('order_line_by_item_cass.csv', index=False)
    

def order_line_by_order_table():
    global order_line_by_order_cass
    order_line_by_order_cass = order_line_cass
    pri_col = ['ol_w_id', 'ol_d_id', 'ol_o_id', 'ol_i_id', 'ol_number']
    rem_col = sorted(['ol_c_id'])
    all_col = pri_col + rem_col
    order_line_by_order_cass = order_line_by_order_cass[all_col]
    print("order_line_by_order")
    print_headers(order_line_by_order_cass)
    order_line_by_order_cass.to_csv('order_line_by_order_cass.csv', index=False)


def loadScript():
    tables = ['warehouse','district','customer','order','order_by_carrier_id','item','order_line','stock','top_balance','order_status','order_line_by_customer','order_line_by_item','order_line_by_order']
    for table in tables:
        if table == 'order':
            print(f'COPY wholesale_supplier.{table}s FROM \'{table}_cass.csv\' WITH DELIMITER=\',\' AND HEADER=TRUE;')
        else:
            print(f'COPY wholesale_supplier.{table} FROM \'{table}_cass.csv\' WITH DELIMITER=\',\' AND HEADER=TRUE;')


if __name__ == '__main__':
    readCSV()

    warehouse_table()
    district_table()
    customer_table()
    order_table()
    order_by_carrier_id_table()
    item_table()
    order_line_table()              # needs item, order_cass
    stock_table()
    top_balance_table()             # needs customer_cass
    order_status_table()            # needs order, customer
    order_line_by_customer_table()  # needs order_line_cass
    order_line_by_item_table()      # needs order_line_cass
    order_line_by_order_table()     # needs order_line_cass

    print("----\ndone")

    loadScript()