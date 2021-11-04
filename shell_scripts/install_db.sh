#!/bin/bash
# This script is to install Cockroach DB and set a cluster.
# Require arguments: http port number, db port number
set -x

HOME_DIR="/temp/cs4224h/test"
BINARY_DIR="$HOME_DIR/cockroach-v21.1.7.linux-amd64"
COCKROACH="$BINARY_DIR/cockroach"
CERT_DIR="$HOME_DIR/certs"
CA_CERT_DIR="$HOME_DIR/my-safe-directory"
USER="cs4224h"
MAIN_SERVER=`hostname -s`
SERVERS="xcnd46 xcnd47 xcnd48 xcnd49 xcnd45"

http_port="$1"
db_port="$2"

install_binary() {
    for server in $SERVERS; do 
        echo "Downloading CockroachDB binary on $server ..."
        if [[ "$server" == "$MAIN_SERVER" ]]; then
            if [ ! -d $BINARY_DIR ]; then
                if [ ! -d $HOME_DIR ]; then mkdir $HOME_DIR; fi
                cd $HOME_DIR
                curl https://binaries.cockroachdb.com/cockroach-v21.1.7.linux-amd64.tgz | tar -xz
            fi
        else 
            ssh -q $USER@$server "if [ ! -d $BINARY_DIR ]; then if [ ! -d $HOME_DIR ]; then mkdir $HOME_DIR; fi; cd $HOME_DIR; curl https://binaries.cockroachdb.com/cockroach-v21.1.7.linux-amd64.tgz | tar -xz; fi"
        fi
    done
}

generate_certs() {
    if [ ! -d $CERT_DIR ]; then mkdir $CERT_DIR; fi
    if [ ! -d $CA_CERT_DIR ]; then mkdir $CA_CERT_DIR; fi
    
    echo "Generating CA certificates ..."
    $COCKROACH cert create-ca --certs-dir=$CERT_DIR --ca-key="$CA_CERT_DIR/ca.key"

    echo "Generating node certificates ..."
    for server in $SERVERS; do 
        if [ "$server" != "$MAIN_SERVER" ]; then
            node_ip=`ssh -q $USER@$server "hostname -i"`
            short_hostname=`ssh -q $USER@$server "hostname -s"`
            long_hostname=`ssh -q $USER@$server "hostname -f"`
            $COCKROACH cert create-node $node_ip $short_hostname $long_hostname localhost 127.0.0.1 --certs-dir=$CERT_DIR --ca-key="$CA_CERT_DIR/ca.key"
            # Make sure certs directory exists
            ssh -q $USER@$server "if [ ! -d $CERT_DIR ]; then mkdir $CERT_DIR; fi;"
            # Upload certs to node
            scp $CERT_DIR/ca.crt $CERT_DIR/node.crt $CERT_DIR/node.key $USER@$server:$CERT_DIR
            rm $CERT_DIR/node.crt $CERT_DIR/node.key
        fi
    done

    # Generate certs for localhost
    $COCKROACH cert create-node `hostname -i` `hostname -s` `hostname -f` localhost 127.0.0.1 --certs-dir=$CERT_DIR --ca-key="$CA_CERT_DIR/ca.key"

    echo "Generating client certificates ..."
    $COCKROACH cert create-client root --certs-dir=$CERT_DIR --ca-key="$CA_CERT_DIR/ca.key"
    for server in $SERVERS; do 
        if [ "$server" != "$MAIN_SERVER" ]; then
            echo "Copy client certificates to $server ..."
            scp $CERT_DIR/client.root.key $CERT_DIR/client.root.crt $USER@$server:$CERT_DIR
        fi
    done
}

start_nodes(){
    join_list="xcnd45:$db_port,xcnd46:$db_port,xcnd47:$db_port,xcnd48:$db_port,xcnd49:$db_port"
    for server in $SERVERS; do 
        echo "Start CockroachDB node on $server ..."
        if [[ "$server" == "$MAIN_SERVER" ]]; then
            cd $HOME_DIR
            $COCKROACH start --certs-dir=$CERT_DIR --listen-addr=$server:$db_port --join=$join_list --cache=.25 --max-sql-memory=.4 --background --http-addr=$server:$http_port
        else 
            hostname=`ssh -q $USER@$server "hostname -s"`
            ssh -q $USER@$server "cd $HOME_DIR; $COCKROACH start --certs-dir=$CERT_DIR --listen-addr=$hostname:$db_port --join=$join_list --cache=.25 --max-sql-memory=.4 --background --http-addr=$hostname:$http_port > /dev/null 2>&1 &"
        fi
    done
}

install_binary

generate_certs

start_nodes

echo "Initiate the cluster ..."
cd $HOME_DIR
$COCKROACH init --certs-dir=$CERT_DIR --host=$MAIN_SERVER:$db_port