#!/bin/bash
#
#Create schemas
#
#
#set -e
psql -v ON_ERROR_STOP=1 --username "bodastage" -d bts  <<-EOSQL
	-- Ericsson 
    CREATE SCHEMA eri_cm_2g;
	CREATE SCHEMA eri_cm_3g4g;
	
	-- ZTE
	CREATE SCHEMA zte_cm_2g;
	CREATE SCHEMA zte_cm_3g;
	CREATE SCHEMA zte_cm_4g;

	-- Huawei
	CREATE SCHEMA hua_cm_2g;
	CREATE SCHEMA hua_cm_3g;
	CREATE SCHEMA hua_cm_4g;
	-- 
	CREATE SCHEMA hua_cm_2g3g;
	
	--Noika
	CREATE SCHEMA nok_cm_2g;
	CREATE SCHEMA nok_cm_3g;
	CREATE SCHEMA nok_cm_4g;
	
	-- History
	-- Ericsson 
    CREATE SCHEMA eri_cm_2g_hist;
	CREATE SCHEMA eri_cm_3g4g_hist;
	
	-- ZTE
	CREATE SCHEMA zte_cm_2g_hist;
	CREATE SCHEMA zte_cm_3g_hist;
	CREATE SCHEMA zte_cm_4g_hist;
                           
	-- Huawei              
	CREATE SCHEMA hua_cm_2g_hist;
	CREATE SCHEMA hua_cm_3g_hist;
	CREATE SCHEMA hua_cm_4g_hist;
    CREATE SCHEMA hua_cm_2g3g_hist;
	--Noika                
	CREATE SCHEMA nok_cm_2g_hist;
	CREATE SCHEMA nok_cm_3g_hist;
	CREATE SCHEMA nok_cm_4g_hist;
	
	
EOSQL