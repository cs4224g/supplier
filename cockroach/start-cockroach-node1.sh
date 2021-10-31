#!/bin/bash

server=$(hostname -s)
echo "Starting cockroach on $server"

ip=$(hostname -I)
echo "IP is $ip"

cockroach start --store=node1 --join=137.132.84.21 --advertise-addr=137.132.84.21 --cache=.25 --max-sql-memory=.25 --insecure
