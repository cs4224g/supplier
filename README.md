# supplier
A Wholesale Supplier application powered by Apache Cassandra and CockroachDB

## Getting Started

### Cassandra 4.0.1 Installation

* Follow this [guide](https://www.youtube.com/watch?v=-1waKtjNt88)
* Add the bin directory to your PATH:

### Windows
Go to 'Edit Environment Variables' in Settings, edit the variable called 'Path', and add the bin directory to the bottom of the list
E.g. I add `D:\Downloads\apache-cassandra-4.0.1-bin.tar\apache-cassandra-4.0.1-bin\apache-cassandra-4.0.1\bin` to my list

### Linux
Just update the PATH variable directly
```
CASS_PATH=<full path to the bin directory>
export PATH=CASS_PATH:$PATH
```

### MacOS
Open up the /etc/paths, and add the directory the the bottom of the path then write it out
```
sudo nano /etc/paths
```
### Starting the Server
Open any terminal. Try running
```
cassandra
```
This should start the `cassandra` server locally after printing a whole bunch of log messages. You should see a message like this at the end
```
Node localhost/127.0.0.1 state jump to NORMAL
```
Running `cqlsh` would start the console to interact with the server


## CockroachDB Usage and Installation

### Set up cockroachDB cluster
To set up cockroachDB node cluster on xcnd40...xcnd44 run the respective commands on each machine:

xcnd40:
```
cockroach start --insecure --store=node1 --listen-addr=192.168.51.3:26357 --join=192.168.51.3:26357,192.168.51.4:26357,192.168.51.5:26357,192.168.51.6:26357,192.168.51.7:26357 --cache=.25 --max-sql-memory=.25 --http-addr=192.168.51.3:9000
```

xcnd41:
```
cockroach start --insecure --store=node2 --listen-addr=192.168.51.4:26357 --join=192.168.51.3:26357,192.168.51.4:26357,192.168.51.5:26357,192.168.51.6:26357,192.168.51.7:26357 --cache=.25 --max-sql-memory=.25 --http-addr=192.168.51.4:9000
```

xcnd42:
```
cockroach start --insecure --store=node3 --listen-addr=192.168.51.5:26357 --join=192.168.51.3:26357,192.168.51.4:26357,192.168.51.5:26357,192.168.51.6:26357,192.168.51.7:26357 --cache=.25 --max-sql-memory=.25 --http-addr=192.168.51.5:9000
```

xcnd43:
```
cockroach start --insecure --store=node4 --listen-addr=192.168.51.6:26357 --join=192.168.51.3:26357,192.168.51.4:26357,192.168.51.5:26357,192.168.51.6:26357,192.168.51.7:26357 --cache=.25 --max-sql-memory=.25 --http-addr=192.168.51.6:9000
```

xcnd44:
```
cockroach start --insecure --store=node5 --listen-addr=192.168.51.7:26357 --join=192.168.51.3:26357,192.168.51.4:26357,192.168.51.5:26357,192.168.51.6:26357,192.168.51.7:26357 --cache=.25 --max-sql-memory=.25 --http-addr=192.168.51.7:9000
```

Initialise the cluster by running:
```
cockroach init --insecure --host=192.168.51.3:26357
```

Make sure the host IP address follows the one shown above as the DSN is hard coded into main.py

### Importing data into CockroachDB

To initialise the tables and database, ```cd scripts``` to get to the correct folder and run:
```
cockroach sql --insecure --host=192.168.51.3:26357 --file ./init.sql
```
To import data, set up a python HTTP server on another window from the directory with the data files using:
```
python -m SimpleHTTPServer 9001
```
and run:

```
cockroach sql --insecure --host=192.168.51.3:26357 --file ./import_data.sql
```

Note that port number needs to be kept at 9001 as the port number is hardcoded into import_data.sql

### Running transactions
To run a client transaction, use the following command:
```
python3 main.py < {client}.csv 
```
Note that throughput metrics are printed to stderr.

To retrieve the 15 statistics for dbstate, run:
```
python3 stats.py 
```



## Other Docs
* [Google Drive](https://drive.google.com/drive/u/0/folders/17pflcjtitASINdO3Ek_BgWZ-aIsMcDo9)