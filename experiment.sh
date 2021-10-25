#!/bin/bash
allocate_server() {
    experiment_type=$2
    db = $3
    echo "$experiment_type"
    echo "db"
    for ((i=0; i<40; i++))
    do
        machine_name="xcnd$((40 + $i % 5))"
        echo "Assigning $machine_name to client $i..."
        file_name="xact_files_$experiment_type/$i.txt"
        echo $file_name
        # -q to suppress banner
        ssh -q $machine_name "cd /temp/cs4224-group-g-$3 && \
        nohup ./run_client.sh $experiment_type $i &>/dev/null &"
    done
}

get_overall_stats() {
    cd stats
    touch "client.csv"
    for ((i=0; i<40; i++))
    do
        cat "$i.csv" >> "client.csv"
        rm "$i.csv"
    done
    # just in case not sorted, we sort each line by number (instead of string, so that 2 will come before 10,11,...)
    sort -n "client.csv" > "client_sorted.csv"
    mv "client_sorted.csv" "client.csv" 
    echo "Stats printed to: client.csv"
    python3 throughput.py
}

echo "Starting shell script..."
if [[ $1 = "experiment" ]]
then
    allocate_server
elif [[ $1 = "stats" ]]
then 
    get_overall_stats
fi