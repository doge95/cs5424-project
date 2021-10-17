#!/bin/bash
set -x

HOME_DIR="/temp/cs4224h"
PROJECT_XACTA="/home/stuproj/cs4224h/project_files_4/xact_files_A"
PROJECT_XACTB="/home/stuproj/cs4224h/project_files_4/xact_files_B"
OUTPUT_DIR="$HOME_DIR/cockroach_output"
CLIENT_ENV_DIR="$HOME_DIR/cockroach_client_env"
DRIVER_DIR="$HOME_DIR/cockroach_driver"

server=`hostname -s`
case $server in
	xcnd45)
		server_num=0
		;;
	xcnd46)
		server_num=1
		;;
	xcnd47)
		server_num=2
		;; 
	xcnd48)
		server_num=3
		;; 
	xcnd49)
		server_num=4
		;; 
    *)              
esac 

if [ ! -d $CLIENT_ENV_DIR ]; then
    # Create the virtual environment 
    python3 -m venv $CLIENT_ENV_DIR
fi

# Activating the virtual environment
source "$CLIENT_ENV_DIR/bin/activate"

# Upgrade pip version
python3 -m pip install --upgrade pip

# Install required packages
python3 -m pip install -r "$DRIVER_DIR/requirements.txt"

# Run client program
if [ ! -d $OUTPUT_DIR ]; then mkdir -p $OUTPUT_DIR; fi

for c in {0..39}; do
    if [ $(($c % 5)) -eq $server_num ]; then
        python3 $DRIVER_DIR/driver.py $PROJECT_XACTA/$c.txt $OUTPUT_DIR $server > $OUTPUT_DIR/${c}_output.txt 2>$OUTPUT_DIR/${c}_performance.txt &
    fi
done