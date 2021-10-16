#!/bin/bash
set -x

HOME_DIR="/temp/cs4224h"
DRIVER_DIR="$HOME_DIR/cockroach_driver"
USER="cs4224h"
MAIN_SERVER="xcnd45"
#SERVERS="xcnd45 xcnd46 xcnd47 xcnd48 xcnd49"
SERVERS="xcnd46 xcnd47 xcnd48 xcnd49"

for server in $SERVERS; do 
    if [[ "$server" == "$MAIN_SERVER" ]]; then
        echo "Run cockroach client program on $server ..."
        $DRIVER_DIR/client_exec.sh
    else 
        echo "Copy cockroach client driver codes to $server ..."
        scp -r $HOME_DIR/cockroach_driver $USER@$server:$HOME_DIR
        echo "Run cockroach client program on $server ..."
        ssh -q $USER@$server "$DRIVER_DIR/client_exec.sh"
        
    fi
done