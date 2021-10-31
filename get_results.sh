#!/bin/bash
# This script is to get all client results from each nodes.
# Require argument - output directory path
# set -x
OUTPUT_DIR="$1"
MAIN_SERVER="xcnd45"
SERVERS="xcnd45 xcnd46 xcnd47 xcnd48 xcnd49"

server=`hostname -s`

for server in $SERVERS; do 
    if [[ "$server" == "$MAIN_SERVER" ]]; then
        cat $OUTPUT_DIR/*_performance.txt >> clients.csv
    else
        ssh -q $USER@$server "cat $OUTPUT_DIR/*_performance.txt" >> clients.csv
    fi
done