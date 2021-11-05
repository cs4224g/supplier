# supplier
A Wholesale Supplier application powered by Apache Cassandra and CockroachDB


## Running the experiments

1. Reset the database accordingly.
2. In the respective folders, in our case `/temp/cs4224-group-g-cassandra` and `/temp/cs4224-group-g-cockroach`, run `./experiment.sh experiment {workload} {database}`. For example:

```
./experiment.sh experiment A cassandra
```

to run experiment A for cassandra in `/temp/cs4224-group-g-cassandra`.

## Cassandra Usage and Installation

### Installing and Setting up Cassandra locally

Please follow the instructions [here](/cassandra/installation.md).

### Set up Cassandra cluster

1. Have Java 8 installed and configured on the cluster. You may find it [here](https://www.oracle.com/sg/java/technologies/javase/javase8-archive-downloads.html#license-lightbox). To set up Java, run the following in the terminal:

```export JAVA_HOME="/{replace with directory of installation}/jdk1.8.0_202"
export PATH="${JAVA_HOME}/bin:{$PATH}"
```

2. Download Cassandra from [this link](https://www.apache.org/dyn/closer.lua/cassandra/4.0.1/apache-cassandra-4.0.1-bin.tar.gz) and unzip using:
   `tar xzvf apache-cassandra-4.0.1-bin.tar.gz`.
3. Change directory into `/bin` directory of `apache-cassandra-4.0.1`.
4. Run `./cassandra` to start up. If the default port for Cassandra is in use, try changing the port for Cassandra. This is done by editing `cassandra-env.sh` which can be found in the `conf` folder of `apache-cassandra-4.0.1. Modify the `JMX_PORT` to another unused port.
5. Add the cassandra bin directory to the path variables by changing the `./bashrc` file as follows:

```
> vim ~/.bashrc
```

Add the following line:

```
export PATH=/{replace with directory of installation}}/apache-cassandra-4.0.1/bin:$PATH
```

6. Modify `cassandra.yaml` accordingly to set up the seed. Follow the file [here](/cassandra/cassandra.yaml) as needed. Copy this into the `/conf` directory of the remaining nodes.
7. After configuration, restart all Cassandra servers so that they have the changes. To kill and restart Cassandra, follow [this section](####kill-and-restart-cassandra)
8. `rm -rf data/*` from the `apache-cassandra-4.0.1` folder.
9. To start running Cassandra in the background, run `nohup cassandra &`.

#### Kill and Restart Cassandra

1. Find PID of Cassandra process
   `ps auwx | grep cassandra`
2. Kill the process:
   `kill -9 {pid}`

### Populating Data into Cassandra

The following instructions assume you are running this on xcnd40.

1. `cd /temp/cs4224-group-g-cassandra` and run `cqlsh`.
2. Run `DROP KEYSPACE WHOLESALE_SUPPLIER;` in the cqlsh terminal.
3. `exit`.
4. `cqlsh < scripts/init.cql`
5. `cd data_files` and run `cqlsh < ../scripts/load.cql`.

## CockroachDB Usage and Installation

### Set up cockroachDB cluster
* To set up cockroachDB node cluster on xcnd40...xcnd44 run the respective commands on each machine:

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

* Initialise the cluster by running:
```
cockroach init --insecure --host=192.168.51.3:26357
```

Make sure the host IP address follows the one shown above as the DSN is hard coded into main.py

### Importing data into CockroachDB

* To initialise the tables and database, ```cd scripts``` to get to the correct folder and run:
```
cockroach sql --insecure --host=192.168.51.3:26357 --file ./init.sql
```
* To import data, set up a python HTTP server on another window from the directory with the data files using:
```
python -m SimpleHTTPServer 9001
```
and run:

```
cockroach sql --insecure --host=192.168.51.3:26357 --file ./import_data.sql
```

* Note that port number needs to be kept at 9001 as the port number is hardcoded into import_data.sql

### Running transactions
* To run a client transaction, use the following command:
```
python3 main.py < {client}.csv 
```
* Note that throughput metrics are printed to stderr.

* To retrieve the 15 statistics for dbstate, run:
```
python3 stats.py 
```


## Drivers

* Cassandra Python Driver: [link](https://github.com/datastax/python-driver)
```
pip install --upgrade pip --user
pip install cassandra-driver
python -c 'import cassandra; print(cassandra.__version__)'
```
* CockroachDB Python Driver
```
pip install psycopg2
```



## Other Docs
* [Google Drive](https://drive.google.com/drive/u/0/folders/17pflcjtitASINdO3Ek_BgWZ-aIsMcDo9)