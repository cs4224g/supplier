CREATE KEYSPACE IF NOT EXISTS test 
WITH replication = {'class':'NodeTopology', 'replication_factor' : 1};

CREATE TABLE IF NOT EXISTS test.example (
	W_ID INT,
	W_NAME TEXT,
  W_CATEGORY TEXT,
    PRIMARY KEY ((W_ID), W_NAME, W_CATEGORY)
);