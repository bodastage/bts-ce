#!/bin/bash
#
#Create schemas
#
#
#set -e
psql -v ON_ERROR_STOP=1 --username "bodastage" -d bts  <<-EOSQL

-- Ericsson 2G
INSERT INTO managedobjects_schemas
(pk, added_by,date_added,date_modified,modified_by,notes, name, tech_pk, vendor_pk)
VALUES (
nextval('seq_managedobjects_schemas_pk'),
0,
now()::timestamp,
now()::timestamp,
0,
'Ericsson 2G',
'eri_cm_2g',
1,
1
);

-- Ericsson 3G
INSERT INTO managedobjects_schemas
(pk, added_by,date_added,date_modified,modified_by,notes, name, tech_pk, vendor_pk)
VALUES (
nextval('seq_managedobjects_schemas_pk'),
0,
now()::timestamp,
now()::timestamp,
0,
'Ericsson 3G',
'eri_cm_3g4g',
2,
1
);

-- Ericsson 4G
INSERT INTO managedobjects_schemas
(pk, added_by,date_added,date_modified,modified_by,notes, name, tech_pk, vendor_pk)
VALUES (
nextval('seq_managedobjects_schemas_pk'),
0,
now()::timestamp,
now()::timestamp,
0,
'Ericsson 4G',
'eri_cm_3g4g',
3,
1
);

-- Huawei 2G
INSERT INTO managedobjects_schemas
(pk, added_by,date_added,date_modified,modified_by,notes, name, tech_pk, vendor_pk)
VALUES (
nextval('seq_managedobjects_schemas_pk'),
0,
now()::timestamp,
now()::timestamp,
0,
'Huawei 2G',
'hua_cm_2g',
1,
2
);


-- Huawei 3G
INSERT INTO managedobjects_schemas
(pk, added_by,date_added,date_modified,modified_by,notes, name, tech_pk, vendor_pk)
VALUES (
nextval('seq_managedobjects_schemas_pk'),
0,
now()::timestamp,
now()::timestamp,
0,
'Huawei 3G',
'hua_cm_3g',
2,
2
);


-- Huawei 4G
INSERT INTO managedobjects_schemas
(pk, added_by,date_added,date_modified,modified_by,notes, name, tech_pk, vendor_pk)
VALUES (
nextval('seq_managedobjects_schemas_pk'),
0,
now()::timestamp,
now()::timestamp,
0,
'Huawei 4G',
'hua_cm_4g',
3,
2
);
EOSQL