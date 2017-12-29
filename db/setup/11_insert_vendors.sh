#!/bin/bash
#
#Create schemas
#
#
#set -e
psql -v ON_ERROR_STOP=1 --username "bodastage" -d bts  <<-EOSQL

-- Ericsson 
--
INSERT INTO vendors
(pk, added_by,date_added,date_modified,modified_by, "name", notes)
VALUES (
nextval('seq_vendors_pk'),
0,
now()::timestamp,
now()::timestamp,
0,
'Ericsson',
'Ericsson AB'
);

-- Huawei 
INSERT INTO vendors
(pk, added_by,date_added,date_modified,modified_by, "name", notes)
VALUES (
nextval('seq_vendors_pk'),
0,
now()::timestamp,
now()::timestamp,
0,
'Huawei',
'Huawei Technologies Co Ltd'
);

-- ZTE
INSERT INTO vendors
(pk, added_by,date_added,date_modified,modified_by, "name", notes)
VALUES (
nextval('seq_vendors_pk'),
0,
now()::timestamp,
now()::timestamp,
0,
'ZTE',
'ZTE Corporation'
);

-- Nokia
INSERT INTO vendors
(pk, added_by,date_added,date_modified,modified_by, "name", notes)
VALUES (
nextval('seq_vendors_pk'),
0,
now()::timestamp,
now()::timestamp,
0,
'Nokia',
'Nokia Networks'
);
EOSQL