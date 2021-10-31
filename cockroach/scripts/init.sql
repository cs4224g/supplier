-- DROP DATABASE IF EXISTS bank CASCADE;
-- CREATE DATABASE bank;
-- CREATE TABLE bank.accounts (id INT PRIMARY KEY, balance DECIMAL);
DROP DATABASE IF EXISTS proj CASCADE;
DROP TABLE IF EXISTS proj.stock;
DROP TABLE IF EXISTS proj.order_line;
DROP TABLE IF EXISTS proj.item;
DROP TABLE IF EXISTS proj.orders;
DROP TABLE IF EXISTS proj.customer;
DROP TABLE IF EXISTS proj.district;
DROP TABLE IF EXISTS proj.warehouse;
CREATE DATABASE proj;
CREATE TABLE proj.warehouse(
    W_ID INT PRIMARY KEY,
    W_NAME STRING NOT NULL,
    W_STREET_1 STRING,
    W_STREET_2 STRING,
    W_CITY STRING,
    W_STATE STRING,
    W_ZIP STRING,
    W_TAX DECIMAL(4, 4),
    W_YTD DECIMAL(12, 2),
    INDEX index_w_ytd (W_YTD)
);
CREATE TABLE proj.district(
    D_W_ID INT references proj.warehouse (W_ID) ON DELETE CASCADE,
    D_ID INT,
    D_NAME STRING NOT NULL,
    D_STREET_1 STRING,
    D_STREET_2 STRING,
    D_CITY STRING,
    D_STATE STRING,
    D_ZIP STRING,
    D_TAX DECIMAL(4, 4),
    D_YTD DECIMAL(12, 2),
    D_NEXT_O_ID INT,
    PRIMARY KEY (D_W_ID, D_ID),
    INDEX index_next_o_id (D_NEXT_O_ID),
    INDEX index_d_ytd (D_YTD)
);
CREATE TABLE proj.customer(
    C_W_ID INT,
    C_D_ID INT,
    C_ID INT,
    C_FIRST STRING NOT NULL,
    C_MIDDLE STRING,
    C_LAST STRING,
    C_STREET_1 STRING,
    C_STREET_2 STRING,
    C_CITY STRING,
    C_STATE STRING,
    C_ZIP STRING,
    C_PHONE STRING,
    C_SINCE TIMESTAMP,
    C_CREDIT STRING NOT NULL,
    C_CREDIT_LIM DECIMAL(12, 2) NOT NULL,
    C_DISCOUNT DECIMAL(4, 4),
    C_BALANCE DECIMAL(12, 2),
    C_YTD_PAYMENT FLOAT,
    C_PAYMENT_CNT INT,
    C_DELIVERY_CNT INT,
    C_DATA STRING,
    PRIMARY KEY(C_W_ID, C_D_ID, C_ID),
    FOREIGN KEY(C_W_ID, C_D_ID) references proj.district (D_W_ID, D_ID), 
    INDEX index_c_ytd_payment (C_YTD_PAYMENT),
    INDEX index_c_balance (C_BALANCE),
    INDEX index_payment_cnt (C_PAYMENT_CNT)
);
CREATE TABLE proj.orders(
    O_W_ID INT,
    O_D_ID INT,
    O_ID INT,
    O_C_ID INT,
    O_CARRIER_ID INT,
    O_OL_CNT DECIMAL(2, 0),
    O_ALL_LOCAL DECIMAL(1, 0),
    O_ENTRY_D TIMESTAMP,
    PRIMARY KEY(O_W_ID, O_D_ID, O_ID),
    FOREIGN KEY(O_W_ID, O_D_ID, O_C_ID) references proj.customer(C_W_ID, C_D_ID, C_ID),
    INDEX index_o_entry_d (O_ENTRY_D),
    INDEX index_o_c_id (O_C_ID),
    INDEX index_o_carrier_id (O_CARRIER_ID)
);
CREATE TABLE proj.item(
    I_ID INT PRIMARY KEY,
    I_NAME STRING NOT NULL,
    I_PRICE DECIMAL(5, 2) NOT NULL,
    I_IM_ID INT NOT NULL,
    I_DATA STRING
);
CREATE TABLE proj.order_line(
    OL_W_ID INT,
    OL_D_ID INT,
    OL_O_ID INT,
    OL_NUMBER INT NOT NULL,
    OL_I_ID INT references proj.item (I_ID),
    OL_DELIVERY_D TIMESTAMP,
    OL_AMOUNT DECIMAL(7, 2) NOT NULL,--change from (6, 2) as stated on pdf to (7,2) to accomodate data files
    OL_SUPPLY_W_ID INT NOT NULL,
    OL_QUANTITY DECIMAL(2, 0),
    OL_DIST_INFO STRING,
    PRIMARY KEY(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER),
    FOREIGN KEY(OL_W_ID, OL_D_ID, OL_O_ID) references proj.orders (O_W_ID, O_D_ID, O_ID) ON DELETE CASCADE,
    FOREIGN KEY(OL_I_ID) references proj.item (I_ID) ON DELETE CASCADE,
    INDEX index_ol_i_id (OL_I_ID),
    INDEX index_ol_quantity (OL_QUANTITY)
);
CREATE TABLE proj.stock(
    S_W_ID INT references proj.warehouse (W_ID) ON DELETE CASCADE,
    S_I_ID INT references proj.item (I_ID) ON DELETE CASCADE,
    S_QUANTITY DECIMAL(4, 0) NOT NULL,
    S_YTD DECIMAL(8, 2),
    S_ORDER_CNT INT,
    S_REMOTE_CNT INT,
    S_DIST_01 STRING,
    S_DIST_02 STRING,
    S_DIST_03 STRING,
    S_DIST_04 STRING,
    S_DIST_05 STRING,
    S_DIST_06 STRING,
    S_DIST_07 STRING,
    S_DIST_08 STRING,
    S_DIST_09 STRING,
    S_DIST_10 STRING,
    S_DATA STRING,
    PRIMARY KEY(S_W_ID, S_I_ID),
    INDEX index_s_ytd (S_YTD),
    INDEX index_s_quantity (S_QUANTITY),
    INDEX index_s_order_cnt (S_ORDER_CNT),
    INDEX index_s_remote_cnt (S_REMOTE_CNT)
);