run_client() {
    # converts to upper case
    experiment_type=${1^^}
    client_id=$2
    db=$3
    file_name="xact_files_$experiment_type/$client_id.txt"
    echo "$file_name"
    # Add client id to start of file without newline
    echo -n "$client_id," > $client_id.csv 
    # Append stderr output to the same file
    python3 main.py < $file_name 2>> $client_id.csv 
    scp -q $client_id.csv cs4224g@xcnd40:/temp/cs4224-group-g-$db/stats
    rm $client_id.csv
}

echo "Running client program now..."
run_client $1 $2 $3