#!/bin/bash
#
#Create schemas
#
#
#set -e
psql -v ON_ERROR_STOP=1 --username "bodastage" -d bts  <<-EOSQL
	-- Live network transformed data 
	CREATE SCHEMA plan_network
		AUTHORIZATION bodastage;

	
EOSQL