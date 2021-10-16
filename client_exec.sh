#!/usr/bin/env bash
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

for c in {0..39}; do
    if [ $(($c % 5)) -eq $server_num ]; then
        # replace program with driver.py
        echo "driver.py < $c.txt 2> ${c}_performance.txt &"
    fi
done