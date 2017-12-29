#!/bin/bash
#
#Create schemas
#
#
#set -e

for f in `find /docker-entrypoint-initdb.d/cm -iname "*.sql"`
do
	psql -v ON_ERROR_STOP=1 --username "bodastage" -d bts -f $f
done


