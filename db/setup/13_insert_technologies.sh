#!/bin/bash
#
#Create schemas
#
#
#set -e
psql -v ON_ERROR_STOP=1 --username "bodastage" -d bts  <<-EOSQL

-- GSM
INSERT INTO technologies
(pk, added_by,date_added,date_modified,modified_by, "name", notes)
VALUES (
nextval('seq_technologies_pk'),
0,
now()::timestamp,
now()::timestamp,
0,
'GSM',
'Global System for Mobile Communications'
);

-- UMTS
INSERT INTO technologies
(pk, added_by,date_added,date_modified,modified_by, "name", notes)
VALUES (
nextval('seq_technologies_pk'),
0,
now()::timestamp,
now()::timestamp,
0,
'UMTS',
'Universal Mobile Telecommunications System'
);

-- LTE
INSERT INTO technologies
(pk, added_by,date_added,date_modified,modified_by, "name", notes)
VALUES (
nextval('seq_technologies_pk'),
0,
now()::timestamp,
now()::timestamp,
0,
'LTE',
'Long-Term Evolution'
);
EOSQL