#!/usr/bin/env bash
set x

CLIENT_ENV_DIR="cockroach_client_env"
server=`hostname -f`

case $server in
	xcnd45.comp.nus.edu.sg)
		server_num=0
		;;
	xcnd46.comp.nus.edu.sg)
		server_num=1
		;;
	xcnd47.comp.nus.edu.sg)
		server_num=2
		;; 
	xcnd48.comp.nus.edu.sg)
		server_num=3
		;; 
	xcnd49.comp.nus.edu.sg)
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
python3 -m pip install -r "./requirements.txt"

# Run client program
for c in {0..39}; do
    if [ $(($c % 5)) -eq $server_num ]; then
        # replace program with driver.py
        python3 ./driver.py $c.txt > ${c}_output.txt 2>${c}_performance.txt &
    fi
done
