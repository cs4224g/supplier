from cassandra.query import named_tuple_factory, SimpleStatement
from decimal import Decimal

def execute_t1(session, args_arr):
  print("T1 New Order Transaction called!\n----------------------")
  # print(args_arr)

  ####################### extract query inputs    
  # assert len(args_arr) == 3, "Wrong length of argments for T3"
  # w_id = int(args_arr[1])
  # new_carrier_id = int(args_arr[2])

  # given as params:
  w_id = 0
  d_id = 0
  c_id = 0
  num_items = 0
  items = [] # [(OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY)]

  district = """SELECT * FROM district WHERE PK=(w_id, d_id)"""
  next_o_id = district.d_next_o_id

  """UPDATE district SET D_NEXT_O_ID={next_o_id + 1} WHERE PK=(w_id, d_id)"""
  customer = """SELECT * FROM customer WHERE (w_id,d_id,c_id);"""

  is_all_local = len(filter(lambda tw: tw != w_id, [t[1] for t in items])) == 0
  """INSERT INTO orders (
                      o_w_id,
                      o_d_id,
                      o_carrier_id,
                      o_id,
                      o_c_id,
                      o_all_local,
                      o_c_first,
                      o_c_last,
                      o_c_middle,
                      o_entry_d,
                      o_ol_cnt) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);, 
                      (
                      o_w_id  --w_id,
                      o_d_id  --d_id,
                      o_carrier_id  --(-1),
                      o_id  --next_o_id,
                      o_c_id  --c_id,
                      o_all_local --(1 if is_all_local else 0),
                      o_c_first --customer.c_first,
                      o_c_last --customer.c_last,
                      o_c_middle --customer.c_middle,
                      o_entry_d --toTimestamp(now()), # see if this works
                      o_ol_cnt  --num_items
                      ))"""

  total_amount = 0
  for i, tup in enumerate(items):
    ol_i_id, ol_supply_w_id, ol_quantity = tup
    stock = """SELECT * FROM stock WHERE (ol_supply_w_id, ol_i_id)"""
    s_qty = stock.s_quantity
    adj_qty = s_qty - ol_quantity
    if adj_qty < 10:
      adj_qty += 100
    """
    UPDATE stock SET 
      s_quantity=%s, 
      s_ytd=%s,
      s_order_cnt=%s,
      s_remote_cnt=%s
    WHERE 
      (ol_supply_w_id, ol_i_id),

    (
      adj_qty, 
      stock.s_ytd + ol_quantity,
      stock.s_order_cnt + 1,
      stock.s_remote_cnt + (1 if ol_supply_w_id != w_id else 0)
    )
    """

    item_info = f"""SELECT * FROM item WHERE i_id={ol_i_id}"""
    price = item_info[0].i_price
    item_amount = ol_quantity * price
    total_amount += item_amount

    """INSERT INTO order_line (
          OL_W_ID,
          OL_D_ID,
          OL_O_ID,
          OL_NUMBER,
          OL_I_ID,
          OL_DELIVERY_D,
          OL_AMOUNT,
          OL_SUPPLY_W_ID,
          OL_QUANTITY,
          OL_DIST_INFO,
          OL_I_NAME,
          OL_I_PRICE,
          OL_C_ID,
        ) 
  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);, 
        (
OL_W_ID,      w_id
OL_D_ID,     d_id
OL_O_ID,     next_o_id
OL_NUMBER,    i+1 #1-basedd indexing
OL_I_ID,      ol_i_id
OL_DELIVERY_D,  ***NULL. Must decide on null timestamp. Check if there exist null delivery dates in starter dataset, and after merge
OL_AMOUNT,        item_amount
OL_SUPPLY_W_ID,   ol_supply_w_id
OL_QUANTITY,      ol_quantity 
OL_DIST_INFO,     stock.s_dist_<d_id>. Try if can index into Row instead of the name. Can access name by ['str'] access?
----- extras
OL_I_NAME,      item_info.i_name
OL_I_PRICE,     item_info.i_price
OL_C_ID,        customer.c_id
))"""

    # out of for loop
    d_tax = "get district tax"
    w_tax = "get WH tax"
    total_amount += (1 + d_tax + w_tax) * (1 - customer.c_discount)

    #### print loads of info
    
