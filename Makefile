all: build

setup:
	pip install --upgrade pip --user
	pip install cassandra-driver

# Assuming you have added Cassandra bin directory to your Path 
test.setup:
	cqlsh < ./scripts/test.cql

test.teardown:
	cqlsh < ./scripts/test_reset.cql
