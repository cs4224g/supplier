# transaction 1


class new_order():

    def execute(self, conn, W_ID, D_ID, C_ID, NUM_ITEMS, ITEM_NUMBER, SUPPLIER_WAREHOUSE, QUANTITY):
        print('\n================ executing top_balance query ================\n')

        with conn.cursor() as cur:
            # get W_TAX
            cur.execute(
                "SELECT W_TAX FROM proj.warehouse WHERE W_ID = %s", (
                    str(W_ID))
            )
            W_TAX = cur.fetchone()[0]

            # get N and D_TAX
            cur.execute(
                "SELECT D_TAX, D_NEXT_O_ID FROM proj.district WHERE D_W_ID = %s AND D_ID = %s", (
                    W_ID, D_ID)
            )
            district_info = cur.fetchone()
            D_TAX = district_info[0]
            N = district_info[1]

            # increment D_NEXT_O_ID
            cur.execute(
                "UPDATE proj.district SET D_NEXT_O_ID = %s WHERE D_W_ID = %s AND D_ID = %s", (
                    N + 1, W_ID, D_ID)
            )

            # get O_ALL_LOCAL
            ALL_LOCAL = 1
            for i in range(1, NUM_ITEMS + 1):
                if SUPPLIER_WAREHOUSE[i-1] != W_ID:
                    ALL_LOCAL = 0

            # get O_ENTRY_D
            O_ENTRY_D = datetime.now()

            # get C_LAST, C_CREDIT, C_DISCOUNT
            cur.execute(
                "SELECT C_LAST, C_CREDIT, C_DISCOUNT FROM proj.customer WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", (
                    W_ID, D_ID, C_ID)
            )
            customer_info = cur.fetchone()
            C_LAST = customer_info[0]
            C_CREDIT = customer_info[1]
            C_DISCOUNT = customer_info[2]

            # output customer identifier, lastname, credit, discount
            print(W_ID, D_ID, C_ID, C_LAST, C_CREDIT, C_DISCOUNT)
            # output tax rates
            print(W_TAX, D_TAX)
            # output order information
            print(N, O_ENTRY_D)

            # create new order
            cur.execute(
                "INSERT INTO proj.orders (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) VALUES (%s, %s, %s, %s, null, %s, %s, %s)", (
                    W_ID, D_ID, N, C_ID, NUM_ITEMS, ALL_LOCAL, O_ENTRY_D)
            )

            # step 4
            TOTAL_AMOUNT = 0

            # step 5
            for i in range(NUM_ITEMS):
                # get ADJUSTED_QTY
                cur.execute(
                    "SELECT S_QUANTITY FROM proj.stock WHERE S_W_ID = %s AND S_I_ID = %s", (
                        ITEM_NUMBER[i], SUPPLIER_WAREHOUSE[i])
                )
                S_QUANTITY = cur.fetchone()[0]
                ADJUSTED_QTY = S_QUANTITY - QUANTITY[i]
                if ADJUSTED_QTY < 10:
                    ADJUSTED_QTY += 100

                # update stock
                cur.execute(
                    "UPDATE proj.stock SET S_QUANTITY = %s, S_YTD = S_YTD + %s, S_ORDER_CNT = S_ORDER_CNT + %s, S_REMOTE_CNT = S_REMOTE_CNT + %s WHERE S_W_ID = %s AND S_I_ID = %s", (
                        ADJUSTED_QTY, QUANTITY[i], 1, 1 if SUPPLIER_WAREHOUSE[i] != W_ID else 0, ITEM_NUMBER[i], SUPPLIER_WAREHOUSE[i])
                )

                # get item amount
                cur.execute(
                    "SELECT I_NAME, I_PRICE FROM proj.item WHERE I_ID = %s", (
                        str(ITEM_NUMBER[i]))
                )
                item_info = cur.fetchone()
                I_NAME = item_info[0]
                I_PRICE = item_info[1]
                ITEM_AMOUNT = QUANTITY[i] * I_PRICE
                TOTAL_AMOUNT += ITEM_AMOUNT

                # create order-line
                DIST_INFO = "S_DIST_" + str(D_ID)
                cur.execute(
                    "INSERT INTO proj.order_line (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) VALUES (%s,%s,%s,%s,%s,null,%s,%s,%s,%s)", (
                        W_ID, D_ID, N, i, ITEM_NUMBER[i], ITEM_AMOUNT, SUPPLIER_WAREHOUSE[i], QUANTITY[i], DIST_INFO)
                )

                # output each ordered item information
                print(ITEM_NUMBER[i], I_NAME, SUPPLIER_WAREHOUSE[i],
                      QUANTITY[i], ITEM_AMOUNT, S_QUANTITY)

            # increment total amount
            TOTAL_AMOUNT *= (1 * D_TAX * W_TAX) * (1 - C_DISCOUNT)

        conn.commit()
        logging.debug("transfer_funds(): status message: %s",
                      cur.statusmessage)


# format: space separated with new line for each output
