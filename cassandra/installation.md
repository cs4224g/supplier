## Cassandra 4.0.1 Installation

- Follow this [guide](https://www.youtube.com/watch?v=-1waKtjNt88)
- Add the bin directory to your PATH:

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
