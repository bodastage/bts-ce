#!/bin/bash
#
#Create schemas
#
#
#set -e
psql -v ON_ERROR_STOP=1 --username "bodastage" -d bts  <<-EOSQL

-- CM Settings
INSERT INTO settings (pk, name, data_type, string_value, label, category_id)
VALUES
(
1,
'cm_dag_schedule_interval',
'string',
'0 0 * * *',
'Process CM data frequency',
'Configuration Data'
);

-- -------------------------
INSERT INTO settings (pk, name, data_type, text_value, label, category_id)
VALUES
(
2,
'cm_dag_fuelux_scheduler_value',
'text',
'{}',
'CM Fuelux scheduler value',
'Configuration Management'
);

ALTER SEQUENCE seq_settings_pk RESTART WITH 3;

EOSQL