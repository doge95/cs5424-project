#!/bin/bash
# This script is to copy scripts and run client program in other nodes.
# Require one arguement: workload - [A|B]

# set -x

HOME_DIR="/temp/cs4224h"
DRIVER_DIR="$HOME_DIR/cockroach_driver"
USER="cs4224h"
MAIN_SERVER=`hostname -s`
SERVERS="xcnd45 xcnd46 xcnd47 xcnd48 xcnd49"

workload="$1"
case $workload in
	A)
		echo "Run workload A transaction files."
		;;
	B)
		echo "Run workload B transaction files."
		;;
	*)
		echo "Please specify a workload."
		exit
		;;
esac

# Require one arguement: workload - [A|B]
for server in $SERVERS; do 
    if [[ "$server" == "$MAIN_SERVER" ]]; then
        echo "Run cockroach client program on $server ..."
        $DRIVER_DIR/client_exec.sh
    else 
        echo "Copy cockroach client driver codes to $server ..."
        scp -r $HOME_DIR/cockroach_driver $USER@$server:$HOME_DIR
        echo "Run cockroach client program on $server ..."
        ssh -q $USER@$server "$DRIVER_DIR/client_exec.sh $workload &"
    fi
done