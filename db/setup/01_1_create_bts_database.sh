#!/bin/bash
#
#Create  database base user and database
#
#Licence Apache 2.0
#Author Emmanuel Robert Ssebaggala <emmanuel.ssebaggala@bodastage.com>
#

#set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER bodastage WITH PASSWORD 'password';
    CREATE DATABASE bts owner bodastage;
EOSQL