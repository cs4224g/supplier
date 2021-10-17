#!/bin/sh
allocate_server() {
    experiment_type=$1
    echo "$experiment_type"
    for ((i=0; i<40; i++))
    do
        machine_name="xcnd$((40 + $i % 5))"
        echo "Assigning $machine_name to client $i..."
        file_name="xact_files_$experiment_type/$i.txt"
        echo $file_name
        stats="$i,1,2,3,4,5,6,7"
        # -q to suppress banner
        ssh -q $machine_name "cd /temp/cs4224-group-g-cassandra && \
        python3 main.py < $file_name && \
        2> $i.csv && \ 
        scp -q $i.csv cs4224g@xcnd40:/temp/cs4224-group-g-cassandra/stats && \
        rm $i.csv"
        # run_client $experiment_type $i
    done
    echo "sleeping for 5 seconds..."
    sleep 5

    touch "stats/client.csv"
    for ((i=0; i<40; i++))
    do
        cat "stats/$i.csv" >> "stats/client.csv"
        rm "stats/$i.csv"
    done
    # just in case not sorted, we sort each line by number (instead of string, so that 2 will come before 10,11,...)
    sort -n "stats/client.csv" > "stats/client_sorted.csv"
    mv "stats/client_sorted.csv" "stats/client.csv" 
    echo "Stats printed to: stats/client.csv" 
}

run_client() {
    echo "Running client program now..."
    file_name="xact_files_$1/c$2.txt"
    echo "$file_name"
}

get_overall_stats() {
    cd stats
    python3 throughput.py 
}

echo "Starting shell script..."
allocate_server $1