#!/usr/bin/env python3
"""
Test psycopg with CockroachDB.
"""

import time
import logging
from datetime import datetime
from argparse import ArgumentParser, RawTextHelpFormatter

import psycopg2
from psycopg2.errors import SerializationFailure


def new_order_transaction(conn, W_ID, D_ID, C_ID, NUM_ITEMS, ITEM_NUMBER, SUPPLIER_WAREHOUSE, QUANTITY):
    print('\n================ executing new_order transaction ================\n')

    with conn.cursor() as cur:

        # get N and D_TAX
        cur.execute(
            "SELECT D_TAX, D_NEXT_O_ID FROM proj.district WHERE D_W_ID = %s AND D_ID = %s FOR UPDATE", (
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

        # get C_LAST, C_CREDIT, C_DISCOUNT, W_TAX
        cur.execute(
            "SELECT C_LAST, C_CREDIT, C_DISCOUNT, W_TAX FROM proj.customer, proj.warehouse WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s AND W_ID = %s", (
                W_ID, D_ID, C_ID, W_ID)
        )
        info = cur.fetchone()
        C_LAST = info[0]
        C_CREDIT = info[1]
        C_DISCOUNT = info[2]
        W_TAX = info[3]

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
        output_items = []
        for i in range(NUM_ITEMS):
            # get ADJUSTED_QTY
            cur.execute(
                "SELECT S_QUANTITY FROM proj.stock WHERE S_W_ID = %s AND S_I_ID = %s FOR UPDATE", (
                    SUPPLIER_WAREHOUSE[i], ITEM_NUMBER[i])
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
                    [ITEM_NUMBER[i]])
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
            output_items.append([ITEM_NUMBER[i], I_NAME, SUPPLIER_WAREHOUSE[i],
                                QUANTITY[i], ITEM_AMOUNT, S_QUANTITY])

        # increment total amount
        TOTAL_AMOUNT *= (1 * D_TAX * W_TAX) * (1 - C_DISCOUNT)

        print(NUM_ITEMS, TOTAL_AMOUNT)
        for output_item in output_items:
            print(output_item)

    conn.commit()
    print("new order transaction committed")


def payment_transaction(conn, C_W_ID, C_D_ID, C_ID, PAYMENT):
    print('\n================ executing payment transaction ================\n')
    with conn.cursor() as cur:
        # update warehouse W_YTD
        cur.execute(
            "UPDATE proj.warehouse SET W_YTD = W_YTD + %s WHERE W_ID = %s RETURNING W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP", (
                PAYMENT, C_W_ID)
        )
        warehouse_info = cur.fetchone()[0]
        # update district D_YTD
        cur.execute(
            "UPDATE proj.district SET D_YTD = D_YTD + %s WHERE D_W_ID = %s AND D_ID = %s RETURNING D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP", (
                PAYMENT, C_W_ID, C_D_ID)
        )
        district_info = cur.fetchone()[0]
        # update customer C_BALANCE, C_YTD_PAYMENT and C_PAYMENT_CNT
        cur.execute(
            "UPDATE proj.customer SET C_BALANCE = C_BALANCE - %s, C_YTD_PAYMENT = C_YTD_PAYMENT + %s, C_PAYMENT_CNT = C_PAYMENT_CNT + %s WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s RETURNING C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE", (
                PAYMENT, PAYMENT, 1,  C_W_ID, C_D_ID, C_ID)
        )
        customer_info = cur.fetchone()[0]

        # output information
        print(customer_info)
        print(warehouse_info)
        print(district_info)
        print(PAYMENT)
    conn.commit()
    print("payment transaction committed")


def delivery_transaction(conn, W_ID, CARRIER_ID):
    print('\n================ executing delivery transaction  ================\n')
    with conn.cursor() as cur:
        # get districts' min order numbers and districts' min order numbers' customers
        cur.execute(
            "SELECT O_W_ID, O_D_ID, O_ID, O_C_ID FROM proj.orders AS T1 WHERE EXISTS (SELECT * FROM (SELECT O_W_ID, O_D_ID, MIN(O_ID) AS O_ID FROM proj.orders WHERE O_CARRIER_ID IS NULL AND O_W_ID=%s GROUP BY O_W_ID, O_D_ID) AS T2 WHERE T1.O_W_ID=%s AND T1.O_W_ID=T2.O_W_ID AND T1.O_D_ID=T2.O_D_ID AND T1.O_ID=T2.O_ID )",
            (str(W_ID), str(W_ID))
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
            cur.execute(
                "UPDATE proj.customer \
                 SET C_BALANCE = C_BALANCE + temp.B, C_DELIVERY_CNT = C_DELIVERY_CNT + 1 \
                    FROM \
                    (SELECT SUM(OL_AMOUNT) AS B FROM proj.order_line WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s GROUP BY (OL_W_ID, OL_D_ID, OL_O_ID)) AS temp \
                    WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", (
                    order_in_district[0], order_in_district[1], order_in_district[2], order_in_district[0], order_in_district[1], order_in_district[3])
            )
        conn.commit()
    print("delivery transaction committed")


def run_transaction(conn, op, max_retries=5):
    """
    Execute the operation *op(conn)* retrying serialization failure.

    If the database returns an error asking to retry the transaction, retry it
    *max_retries* times before giving up (and propagate it).
    """
    # leaving this block the transaction will commit or rollback
    # (if leaving with an exception)
    # with conn:
    for retry in range(1, max_retries + 1):
        try:
            op(conn)

            # If we reach this point, we were able to commit, so we break
            # from the retry loop.
            return 0

        except SerializationFailure as e:
            # This is a retry error, so we roll back the current
            # transaction and sleep for a bit before retrying. The
            # sleep time increases for each failed transaction.
            conn.rollback()
            sleep_ms = 0.1
            time.sleep(sleep_ms)

        except psycopg2.Error as e:
            print(e)
            print('psycopg2 error')
            return 1

    # raise ValueError(
     #   f"Transaction did not succeed after {max_retries} retries")
    print('Transaction did not succeed after {max_retries} retries')
    return 1


def test_retry_loop(conn):
    """
    Cause a seralization error in the connection.

    This function can be used to test retry logic.
    """
    with conn.cursor() as cur:
        # The first statement in a transaction can be retried transparently on
        # the server, so we need to add a dummy statement so that our
        # force_retry() statement isn't the first one.
        cur.execute("SELECT now()")
        cur.execute("SELECT crdb_internal.force_retry('1s'::INTERVAL)")
    logging.debug("test_retry_loop(): status message: %s", cur.statusmessage)


def main():
    opt = parse_cmdline()
    logging.basicConfig(level=logging.DEBUG if opt.verbose else logging.INFO)

    conn = psycopg2.connect(opt.dsn)
    # create_accounts(conn)
    # print_balances(conn)

    amount = 100
    fromId = 1
    toId = 2

    try:

        # run_transaction(conn, lambda conn: payment_transaction(
        #     conn, 1, 1, 1, 10))
        run_transaction(conn, lambda conn: delivery_transaction(
            conn, 1, 2))

    # The function below is used to test the transaction retry logic.  It
    # can be deleted from production code.
    # run_transaction(conn, test_retry_loop)
    except ValueError as ve:
        # Below, we print the error and continue on so this example is easy to
        # run (and run, and run...).  In real code you should handle this error
        # and any others thrown by the database interaction.
        logging.debug("run_transaction(conn, op) failed: %s", ve)
        pass

    # print_balances(conn)

    # delete_accounts(conn)

    # Close communication with the database.
    conn.close()


def parse_cmdline():
    parser = ArgumentParser(description=__doc__,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument(
        "dsn",
        help="""database connection string

For cockroach demo, use
'postgresql://<username>:<password>@<hostname>:<port>/bank?sslmode=require',
with the username and password created in the demo cluster, and the hostname
and port listed in the (sql/tcp) connection parameters of the demo cluster
welcome message.

For CockroachCloud Free, use
'postgres://<username>:<password>@free-tier.gcp-us-central1.cockroachlabs.cloud:26257/<cluster-name>.bank?sslmode=verify-full&sslrootcert=<your_certs_directory>/cc-ca.crt'.

If you are using the connection string copied from the Console, your username,
password, and cluster name will be pre-populated. Replace
<your_certs_directory> with the path to the 'cc-ca.crt' downloaded from the
Console.

"""
    )

    parser.add_argument("-v", "--verbose",
                        action="store_true", help="print debug info")

    opt = parser.parse_args()
    return opt


if __name__ == "__main__":
    main()
