# transaction 2


class new_order():

    def execute(self, conn, C_W_ID, C_D_ID, C_ID, PAYMENT):

        print('\n================ executing top_balance query ================\n')

        with conn.cursor() as cur:
            # update warehouse W_YTD
            cur.execute(
                "UPDATE proj.warehouse SET W_YTD = W_YTD + %s WHERE W_ID = %s", (
                    PAYMENT, C_W_ID)
            )
            # update district D_YTD
            cur.execute(
                "UPDATE proj.district SET D_YTD = D_YTD + %s WHERE D_W_ID = %s AND D_ID = %s", (
                    PAYMENT, C_W_ID, C_D_ID)
            )
            # update customer C_BALANCE, C_YTD_PAYMENT and C_PAYMENT_CNT
            cur.execute(
                "UPDATE proj.customer SET C_BALANCE = C_BALANCE - %s, C_YTD_PAYMENT = C_YTD_PAYMENT + %s, C_PAYMENT_CNT = C_PAYMENT_CNT + %s WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", (
                    PAYMENT, PAYMENT, 1,  C_W_ID, C_D_ID, C_ID)
            )
            # output customer identifier, name, address, etc
            cur.execute(
                "SELECT C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE FROM proj.customer WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", (
                    C_W_ID, C_D_ID, C_ID)
            )
            customer_info = cur.fetchone()[0]
            print(customer_info)
            # output warehouse address
            cur.execute(
                "SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM proj.warehouse WHERE W_ID = %s", str(C_W_ID))
            warehouse_info = cur.fetchone()[0]
            print(warehouse_info)
            # output district address
            cur.execute(
                "SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM proj.district WHERE D_W_ID = %s AND D_ID = %s", (C_W_ID, C_D_ID))
            district_info = cur.fetchone()[0]
            print(district_info)
            # output PAYMENT
            print(PAYMENT)

        conn.commit()
        logging.debug("transfer_funds(): status message: %s",
                      cur.statusmessage)


# format: space separated with new line for each output
