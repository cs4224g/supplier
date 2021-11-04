# transaction 3


class delivery():

    def execute(self, conn, W_ID, CARRIER_ID):
        print('\n================ executing top_balance query ================\n')

        with conn.cursor() as cur:
            # get districts' min order numbers and districts' min order numbers' customers
            cur.execute(
                "SELECT * FROM proj.orders AS T1 WHERE EXISTS (SELECT * FROM (SELECT O_W_ID, O_D_ID, MIN(O_ID) AS O_ID FROM proj.orders WHERE O_CARRIER_ID IS NULL GROUP BY O_W_ID, O_D_ID) AS T2 WHERE T1.O_W_ID=T2.O_W_ID AND T1.O_D_ID=T2.O_D_ID AND T1.O_ID=T2.O_ID )",
                str(W_ID)
            )
            orders_in_districts = cur.fetchmany(10)

            for order_in_district in orders_in_districts:
                # update order X O_CARRIER_ID
                cur.execute(
                    "UPDATE proj.orders SET O_CARRIER_ID = %s WHERE O_W_ID = %s AND O_D_ID = %s AND O_ID = %s", (
                        CARRIER_ID, order_in_district[0], order_in_district[1], order_in_district[2])
                )

                # update order lines in X
                cur.execute(
                    "UPDATE proj.order_line SET OL_DELIVERY_D = %s WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s", (
                        datetime.now(), order_in_district[0], order_in_district[1], order_in_district[2])
                )

                # update customer
                # get B
                cur.execute(
                    "SELECT OL_W_ID, OL_D_ID, OL_O_ID, SUM(OL_AMOUNT) FROM proj.order_line WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s GROUP BY (OL_W_ID, OL_D_ID, OL_O_ID)", (
                        order_in_district[0], order_in_district[1], order_in_district[2])
                )
                balance = cur.fetchone()
                B = balance[0]

                cur.execute(
                    "UPDATE proj.customer SET C_BALANCE = C_BALANCE + %s, C_DELIVERY_CNT = C_DELIVERY_CNT + %s WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", (
                        B, 1, order_in_district[0], order_in_district[1], order_in_district[3])
                )

        conn.commit()
        logging.debug("transfer_funds(): status message: %s",
                      cur.statusmessage)

# format: space separated with new line for each output
