from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import os
import subprocess
import logging


class ZTECM(object):
    """Process ZTE configuration management data"""

    def __init__(self):
        self.db_engine = create_engine('postgresql://bodastage:password@database/bts')

    def extract_zte_bscs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        # BSC6900
        sql = """
            INSERT INTO live_network.nodes
            (pk,date_added, date_modified, type,"name", vendor_pk, tech_pk, added_by, modified_by)
            SELECT 
            NEXTVAL('live_network.seq_nodes_pk'),
            t1."DATETIME" AS date_added, 
            t1."DATETIME" AS date_modified, 
            'BSC' AS node_type,
            t1."userLabel" AS "name" , 
            3 AS vendor_pk, -- 1=Ericsson, 2=Huawei, 3=ZTE
            1 AS tech_pk , -- 1=gsm, 2-umts,3=lte
            0 AS added_by,
            0 AS modified_by
            FROM zte_bulkcm."SubNetwork_2" t1
--          INNER JOIN cm_loads t3 on t3.pk = t1."LOADID"
            LEFT OUTER  JOIN live_network.nodes t2 ON t1."userLabel" = t2."name"
            WHERE 
            t2."name" IS NULL
            AND t1."userDefinedNetworkType" = 'AN'
         ON CONFLICT ON CONSTRAINT unique_nodes
         DO NOTHING
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_zte_rncs(self):
        """Extract ZTE RNCs"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.nodes
            (pk,date_added, date_modified, type,"name", vendor_pk, tech_pk, added_by, modified_by)
            SELECT 
            NEXTVAL('live_network.seq_nodes_pk'),
            t1."DATETIME" AS date_added, 
            t1."DATETIME" AS date_modified, 
            'BSC' AS node_type,
            t1."userLabel" AS "name" , 
            3 AS vendor_pk, -- 1=Ericsson, 2=Huawei, 3=ZTE
            2 AS tech_pk , -- 1=gsm, 2-umts,3=lte
            0 AS added_by,
            0 AS modified_by
            FROM zte_cm."SubNetwork_2" t1
            INNER JOIN cm_loads t3 on t3.pk = t1."LOADID"
            LEFT OUTER  JOIN live_network.nodes t2 ON t1."userLabel" = t2."name"
            WHERE 
            t2."name" IS NULL
            AND t3.is_current_load = true
            AND t1."userDefinedNetworkType" = 'AN'
         ON CONFLICT ON CONSTRAINT unique_nodes
         DO NOTHING
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_zte_enodes(self):
        """Extract ZTE ENodeBs from itf-N dumps"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
         INSERT INTO live_network.sites
         (pk, date_added,date_modified, tech_pk, vendor_pk, "name", added_by, modified_by)
         SELECT 
         NEXTVAL('live_network.seq_sites_pk'),
         "DATETIME" AS date_added, 
         "DATETIME" AS date_modified, 
         3 AS tech_pk , -- 1=gsm, 2-umts,3=lte,
         3 AS vendor_pk, -- 1=Ericsson, 2=Huawei, 3=ZTE
         t1."userLabel",
         0 AS added_by,
         0 AS modified_by 
         FROM
         zte_cm."ENBFunction" t1
         INNER JOIN cm_loads t3 on t3.pk = t1."LOADID"
         LEFT OUTER  JOIN live_network.sites t2 ON t1."userLabel" = t2."name" AND t2.vendor_pk  = 3
         WHERE 
         t2."name" IS NULL
      	AND t3.is_current_load = true
         ON CONFLICT ON CONSTRAINT uq_site
         DO NOTHING
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_zte_2g_sites(self):
        """Extract ZTE 2G sites from itf-N dumps"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.sites
            (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, node_pk)
            SELECT DISTINCT
            NEXTVAL('live_network.seq_sites_pk'),
            t1."DATETIME" AS date_added, 
            t1."DATETIME" AS date_modified, 
            0 AS added_by,
            0 AS modified_by,
            1 AS tech_pk, -- tech 3 -lte, 2 -umts, 1-gms
            3 AS vendor_pk, -- 1- Ericsson, 2 - Huawei, 3 - zte, 4-nokika, etc...
            t1."userLabel" AS "name",
            t4.pk as node_pk -- node primary key
            from zte_cm."BtsSiteManager" t1
            INNER JOIN cm_loads t5 on t5.pk = t1."LOADID"
            LEFT JOIN live_network.sites t4 on t4."name" = t1."userLabel" 
               AND t4.vendor_pk = 3 and t4.tech_pk = 1
            WHERE 
            t4."name" IS NULL
            AND t5.is_current_load = true
            ON CONFLICT ON CONSTRAINT uq_site
            DO NOTHING
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))
        session.close()

    def extract_zte_2g_cells(self):
        """Extract Huawei 2G cells """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.cells
            (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, site_pk)
            SELECT DISTINCT
            nextval('live_network.seq_cells_pk'),
            t1."DATETIME" AS date_added, 
            t1."DATETIME" AS date_modified, 
            0 AS added_by,
            0 AS modified_by,
            1, -- tech 3 -lte, 2 -umts, 1-gms
            3, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
            t1."userLabel" AS name,
            t4.pk -- site primary key
            FROM zte_cm."GsmCell" t1
            -- LOAD
            INNER JOIN cm_loads t8 on t8.pk = t1."LOADID"
            -- NE/SYS
            INNER JOIN zte_cm."BtsSiteManager" t9 ON t9."FILENAME" = t1."FILENAME" 
            	AND t9."BtsSiteManager_id" = t1."BtsSiteManager_id" 
                AND t9."meContext_id" = t1."meContext_id"
                AND t9."SubNetwork_2_id" = t1."SubNetwork_2_id"
                AND t9."LOADID" = t1."LOADID"
            LEFT JOIN live_network.sites t4 on t4."name" = trim(t9."userLabel")
                AND t4.vendor_pk = 3 
                AND t4.tech_pk = 1
            LEFT JOIN live_network.cells t5 on t5."name" = trim(t1."userLabel")
                AND t5.tech_pk = 1
                AND t5.vendor_pk = 3
            WHERE
            t5."name" IS NULL
            AND t8.is_current_load = true
            ON CONFLICT ON CONSTRAINT uq_live_cells
            DO NOTHING
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_zte_2g_cell_params(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
             INSERT INTO live_network.gsm_cells_data
             (pk, name, cell_pk, ci, bcc, ncc, bsic, bcch, lac, latitude, longitude, cgi, azimuth, height, 
             mechanical_tilt, electrical_tilt, hsn, hopping_type, tch_carriers, mcc, mnc, modified_by, added_by, date_added, date_modified)
             SELECT 
             NEXTVAL('live_network.seq_gsm_cells_data_pk') as pk,
              t1."userLabel" AS name,
             t2.pk AS cell_pk,
             t1."cellIdentity"::integer AS ci,
             t1."bcc"::integer AS bcc,
             t1."ncc"::integer AS ncc,
             CONCAT(trim(t1."ncc"),trim(t1."bcc"))::integer AS bsic,
             t1."bcchFrequency"::integer AS bcch,
             t1."lac"::integer AS lac,
             t1."Latitude"::float AS latitude,
             t1."Longitude"::float as longitude ,
             CONCAT( TRIM(t1."mcc"),'-', TRIM(t1."mnc"),'-',TRIM(t1."lac"),'-',TRIM(t1."cellIdentity")) AS cgi,
             NULL AS azimuth,
             -- t6."azimuth"::integer AS azimuth,
             t1."altitude"::integer AS height,
             null AS mechanical_tilt,
             -- t1."SECTOR_ANGLE"::integer AS sector_angle,
             -- t6."MAXTA" AS ta
             -- t1."STATE" AS STATE -- ACTIVE or INACTIVE
             null AS electrical_tilt,
             null AS hsn,
             null AS hopping_type,
             null AS tch_carriers,
             t1."mcc"::integer as mcc,
             t1."mnc"::integer as mnc,
             0 AS modified_by,
             0 AS added_by,
             t1."DATETIME" AS date_added,
             t1."DATETIME" AS date_modified            
             FROM zte_cm."GsmCell" t1             
             INNER JOIN cm_loads t8 on t8.pk = t1."LOADID"
             INNER JOIN live_network.cells t2 on t2."name" = t1."userLabel" AND t2.vendor_pk = 3 AND t2.tech_pk = 1
             INNER JOIN live_network.sites t5 on t5.pk = t2.site_pk
              WHERE 
              t5."name" ='{0}'
              AND t8.is_current_load = true
              ON CONFLICT ON CONSTRAINT uq_live_gsm_cells_data
              DO NOTHING
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_zte_3g_sites(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.sites
            (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, node_pk)
            SELECT 
            NEXTVAL('live_network.seq_sites_pk'),
            t1."DATETIME" AS date_added, 
            t1."DATETIME" AS date_modified, 
            0 AS added_by,
            0 AS modified_by,
            2, -- tech 3 -lte, 2 -umts, 1-gms
            3, -- 1- Ericsson, 2 - Huawei, 3- ZTE, 4- Nokia
            t1."userLabel",
            t3.node_pk -- node primary key
            from zte_cm."NodeBFunction" t1
            INNER JOIN cm_loads t4 on t4.pk = t1."LOADID"
            LEFT JOIN live_network.sites t3 on t3."name" = t1."userLabel"
               AND t3.vendor_pk = 3 and t3.tech_pk = 2
            WHERE 
            t3."name" IS NULL
            AND t4.is_current_load = true
            ON CONFLICT ON CONSTRAINT uq_site
            DO NOTHING
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_zte_3g_cells(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        sites = session.query(Site).filter_by(vendor_pk=3).filter_by(tech_pk=2).all()

        i = 0
        sites_len = len(sites)
        while i < sites_len:
            # Handle iterations at the end of the site list
            end = i + 5;
            if sites_len < i + 5:
                end = sites_len

            placeholder_range = 5
            if end == sites_len:
                placeholder_range = end - i

            site_list = list(map(lambda x: x[1], sites[i:end]))
            # placeholders = map( lambda x: ':p'+x , range(5)) # [:p0,...,:p4]

            placeholders = []
            site_list_placeholders = {}
            for r in range(placeholder_range):
                placeholders.append(':p' + str(r))
                site_list_placeholders['p' + str(r)] = site_list[r]

            print(site_list_placeholders)
            print(site_list)

            i = i + 5
            sql = """
                 INSERT INTO live_network.cells
                 (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, site_pk)
                 SELECT 
                 nextval('live_network.seq_cells_pk'),
                 t1."DATETIME" AS date_added, 
                 t1."DATETIME" AS date_modified, 
                 0 AS added_by,
                 0 AS modified_by,
                 2, -- tech 3 -lte, 2 -umts, 1-gms
                 3, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
                 t1."userLabel" AS name,
                 t4.pk -- site primary key
                 FROM zte_cm."UtranCellFDD" t1
                 INNER JOIN cm_loads t6 on t6.pk = t1."LOADID"
                 INNER JOIN zte_cm."NodeBFunction" t2 ON t2."nodeBFunctionIubLink" = t1."utranCellIubLink"
                 INNER JOIN live_network.sites t4 on t4."name" = t2."userLabel"
                     AND t4.vendor_pk = 3
                     AND t4.tech_pk = 2
                 LEFT JOIN live_network.cells t5 on t5."name" = t1."userLabel"
                     AND t5.tech_pk = 2
                     AND t5.vendor_pk = 3
                 WHERE 
                 t5."name" IS NULL
                 AND t6.is_current_load = true
                 AND t4."name" IN ({})
             """.format(', '.join(placeholders))

            self.db_engine.execute(text(sql).execution_options(autocommit=True), **site_list_placeholders)

        session.close()

    def extract_zte_3g_cell_params(self):
        """Extract Ericsson UMTS cell parameters"""

        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        # Truncate paramete table
        # self.db_engine.execute(text("TRUNCATE TABLE live_network.umts_cells_data").execution_options(autocommit=True))
        # self.db_engine.execute(text("ALTER SEQUENCE live_network.seq_umts_cells_data_pk RESTART WITH 1;").
        #                        execution_options(autocommit=True))

        # The data is alot. Let's handle per site
        site_sql = """SELECT pk, "name" from live_network.sites where vendor_pk  = 3 and tech_pk = 2"""

        result = self.db_engine.execute(site_sql)

        for row in result:
            (site_pk, site_name) = row

            # print("Extracting cells parameters for site_pk: {0}, site_name: {1}".format(site_pk,site_name))

            sql = """
                INSERT INTO live_network.umts_cells_data
                (pk, date_added, date_modified, added_by, modified_by,bch_power,cell_id,cell_pk,lac,latitude, longitude, 
                maximum_transmission_power, "name", cpich_power, primary_sch_power, scrambling_code, rac, sac, 
                secondary_sch_power, site_pk, tech_pk, vendor_pk, uarfcn_dl,uarfcn_ul, ura_list, azimuth, cell_range, 
                height, site_sector_carrier, mcc,mnc,ura,localcellid)
                SELECT 
                NEXTVAL('live_network.seq_umts_cells_data_pk'),
                t1."DATETIME" as date_added, 
                t1."DATETIME" as date_modified, 
                0 as added_by,
                0 as modified_by,
                t1."bchPower"::integer,
                t1."cId"::integer as ci,
                t3.pk as cell_pk, -- cellid
                t1."lac"::integer,
                (t1."anteLatitude"::float/93206.76)*(-1::float*t1."anteLatitudeSign"::float) 
                as latitude,
                t1."anteLongitude"::float/46603.38 as longitude,
                t1."maximumTransmissionPower"::integer as maximum_transmission_power,
                t1."UtranCellFDD_id",
                t1."primaryCpichPower"::integer as cpich_power,
                t1."primarySchPower"::integer as primary_sch_power,
                t1."primaryScramblingCode"::integer as scrambling_code,
                t1."rac"::integer as rac,
                t1."sac"::integer as sac,
                t1."secondarySchPower"::integer as secondary_sch_power,
                t3.site_pk, -- site pk
                2, -- umts
                3, -- Ericsson
                t1."uarfcnDl"::integer,
                t1."uarfcnUl"::integer,
                t1."uraList",
                NULL as azimuth, -- t6."beamDirection"::integer, -- azimuth,
                NULL as cellrange, -- t5."cellRange"::integer, -- cellrange,
                t1."altitude"::integer, -- height
                NULL AS site_sector_carrier, -- concat(t2."MeContext_id", '_', t2."vsDataRbsLocalCell_id") as site_sector_carrier,
                t7."mcc"::integer as mcc,
                t7."mnc"::integer as mnc,
                t1."uraList" as ura ,
                t1."localCellId"::integer as localcellid
                FROM 
                zte_cm."UtranCellFDD" t1
                INNER JOIN cm_loads t9 on t9.pk = t1."LOADID"
                INNER JOIN zte_cm."RncFunction" t7 on  t7."SubNetwork_2_id" = t1."SubNetwork_2_id" 
                    AND t7."meContext_id" = t1."meContext_id"
                    AND t7."RncFunction_id" = t1."RncFunction_id" 
                INNER JOIN live_network.cells t3 on t3."name" = t1."userLabel"
                WHERE t7."meContext_id" = '{0}'
                AND t9.is_current_load = true
                ;
            """.format(site_name)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_zte_4g_cells(self):
        """Extract Huawei LTE Cells
         This extract the parameters in one query. Needs alot of memory for large networks
         """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.cells
            (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, site_pk)
            SELECT 
            NEXTVAL('live_network.seq_cells_pk'),
            t1."DATETIME" as date_added, 
            t1."DATETIME" as date_modified, 
            0 as added_by,
            0 as modified_by,
            3, -- tech 3 -lte, 2 -umts, 1-gms
            3, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
            CASE WHEN t1."userLabel" IS NULL THEN t1."EUtranCellFDD_id" ELSE t1."userLabel" END AS "name",
            t2.pk -- site primary key
            FROM zte_cm."EUtranCellFDD" t1
            INNER JOIN live_network.sites t2 on t2."name" = t1."meContext_id" 
                AND t2.vendor_pk = 1 and t2.tech_pk = 3 
            LEFT JOIN live_network.cells t3 on t3."name" = CASE WHEN t1."userLabel" IS NULL THEN t1."EUtranCellFDD_id" ELSE t1."userLabel" END
                AND t2.vendor_pk = 3 and t2.tech_pk = 3 
            WHERE
                t3."name" IS NULL
            ON CONFLICT ON CONSTRAINT uq_live_cells
            DO NOTHING
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_zte_4g_cell_params(self):
        """Extract Ericsson LTE cell parameters"""
        """Extract Ericsson LTE cell parameters"""

        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        # Truncate paramete table
        # self.db_engine.execute(text("TRUNCATE TABLE live_network.lte_cells_data").execution_options(autocommit=True))
        # self.db_engine.execute(text("ALTER SEQUENCE live_network.seq_lte_cells_data_pk RESTART WITH 1;").
        #                       execution_options(autocommit=True))

        # The data is alot. Let's handle per site
        site_sql = """SELECT pk, "name" from live_network.sites where vendor_pk  = 3 and tech_pk = 3"""

        result = self.db_engine.execute(site_sql)

        for row in result:
            (site_pk, site_name) = row

            # print("Extracting cells parameters for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                 INSERT INTO live_network.lte_cells_data
                (pk, name, cell_pk, dl_earfcn, ul_earfcn, mcc, mnc, tac, pci, ecgi, rach_root_sequence, max_tx_power, latitude, longitude,
                height, dl_bandwidth, ul_bandwidth, ta, ta_mode, tx_elements, rx_elements, scheduler, azimuth, mechanical_tilt, electrical_tilt, cell_range,
                site_pk, tech_pk, vendor_pk, modified_by, added_by, date_added, date_modified)
                SELECT 
                NEXTVAL('live_network.seq_lte_cells_data_pk'),
                t1."vsDataEUtranCellFDD_id" as name,
                t2.pk as cell_pk,
                t1."earfcnDl"::integer as uarfcn_dl,
                t1."earfcnUl"::integer as uarfcn_ul,
                null as mcc, -- t1."mcc"::integer as mcc,
                null as mnc, -- t1."mnc"::integer as mc,
                t1."tac"::integer as tac,
                t1."pci"::integer as pci,
                null as ecgi,
                NULL as rach_root_sequence, -- t1."rootSequenceIndex" as rach_root_sequence,
                null as max_tx_power,
                (t1."latitude"::float/93206.76)*(-1::float)  as latitude,
                t1."longitude"::float/46603.38 as longitude,
                null as height, -- t1."altitude"::integer as height,
                t1."bandWidthDl"::integer as dl_bandwidth,
                t1."bandWidthUl"::integer as ul_bandwidth,
                null as ta,
                null as ta_mode,
                null as tx_elements, -- t1."numOfTxAntennas"::integer as tx_elements,
                null as rx_elements, -- t1."numOfRxAntennas"::integer as rx_elements,
                null as scheduler,
                null as azimuth,
                null as mechanical_tilt,
                null as electrical_tilt,
                t1."cellRadius"::integer as cell_range,
                t2.site_pk as site_pk,
                t2.tech_pk as tech_pk,
                t2.vendor_pk as vendor_pk,
                0 as modified_by, 
                0 as added_by, 
                t1."DATETIME" as date_added, 
                t1."DATETIME" as date_modified
                FROM zte_cm."EUtranCellFDD" t1
                INNER JOIN cm_loads t9 on t9.pk = t1."LOADID"
                INNER JOIN live_network.cells t2 on t2."name" = t1."EUtranCellFDD_id"
                WHERE t1."meContext_id" = '{0}'
                AND 
                t9.is_current_load = true
                    """.format(site_name)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_zte_2g2g_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=3).filter_by(tech_pk=1).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t5.node_pk as svrnode_pk,
                t4.site_pk as svrsite_pk,
                t4.tech_pk as svrtech_pk,
                t4.vendor_pk as svrvendor_pk,
                t4.pk as svrcell_pk,
                -- nbr side
                t7.node_pk as nbrnode_pk,
                t6.site_pk as nbrsite_pk,
                t6.tech_pk as nbrtech_pk,
                t6.vendor_pk as nbrvendor_pk,
                t6.pk as nbrcell_pk,
                t1."DATETIME" ,
                t1."DATETIME" ,
                0, -- system
                0
                FROM 
                zte_cm."GsmRelation" t1
                INNER JOIN zte_cm."GsmCell" t2 on 
                    t2."FILENAME"  = t1."FILENAME" 
                    AND t2."DATETIME"  = t1."DATETIME"
                    AND t1."GsmCell_id" = t2."GsmCell_id"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."userLabel" 
                    AND t4.vendor_pk = 3 AND t4.tech_pk = 1
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 3 AND t5.tech_pk = 1
                -- ---------
                LEFT JOIN zte_cm."GsmCell" t8 on 
                    t8."FILENAME" = t1."FILENAME"
                    AND t8."DATETIME" = t1."DATETIME"
                    AND CONCAT('SubNetwork=',TRIM(t8."SubNetwork_id"),',SubNetwork=',TRIM(t8."SubNetwork_2_id"),',MeContext=',TRIM(t8."meContext_id"),',ManagedElement=',TRIM(t8."ManagedElement_id"),',BssFunction=',TRIM(t8."BssFunction_id"),',BtsSiteManager=',t8."BtsSiteManager_id",',GsmCell=',TRIM(t8."GsmCell_id")) = t1."adjacentCell"
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."userLabel"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                t1."GsmCell_id" IS NOT NULL
                 AND t4.site_pk = '{0}'

            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_zte_2g3g_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=3).filter_by(tech_pk=1).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t5.node_pk as svrnode_pk,
                t4.site_pk as svrsite_pk,
                t4.tech_pk as svrtech_pk,
                t4.vendor_pk as svrvendor_pk,
                t4.pk as svrcell_pk,
                -- nbr side
                t7.node_pk as nbrnode_pk,
                t6.site_pk as nbrsite_pk,
                t6.tech_pk as nbrtech_pk,
                t6.vendor_pk as nbrvendor_pk,
                t6.pk as nbrcell_pk,
                t1."DATETIME" ,
                t1."DATETIME" ,
                0, -- system
                0
                FROM 
                zte_cm."UtranRelation" t1
                INNER JOIN zte_cm."GsmCell" t2 on 
                    t2."FILENAME"  = t1."FILENAME" 
                    AND t2."DATETIME"  = t1."DATETIME"
                    AND t1."GsmCell_id" = t2."GsmCell_id"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."userLabel" 
                    AND t4.vendor_pk = 3 AND t4.tech_pk = 1
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 3 AND t5.tech_pk = 1
                -- ---------
                LEFT JOIN zte_cm."UtranCellFDD" t8 on 
                    t8."FILENAME" = t1."FILENAME"
                    AND t8."DATETIME" = t1."DATETIME"
                    AND CONCAT('SubNetwork=',TRIM(t8."SubNetwork_id"),',SubNetwork=',TRIM(t8."SubNetwork_2_id"),',MeContext=',TRIM(t8."meContext_id"),',ManagedElement=',TRIM(t8."ManagedElement_id"),',RncFunction=',TRIM(t8."RncFunction_id"),',UtranCellFDD=',t8."UtranCellFDD_id") = t1."adjacentCell"
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."userLabel"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                t1."GsmCell_id" IS NOT NULL
                AND t4.site_pk = '{0}'


            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_zte_2g4g_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=3).filter_by(tech_pk=1).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t5.node_pk as svrnode_pk,
                t4.site_pk as svrsite_pk,
                t4.tech_pk as svrtech_pk,
                t4.vendor_pk as svrvendor_pk,
                t4.pk as svrcell_pk,
                -- nbr side
                t7.node_pk as nbrnode_pk,
                t6.site_pk as nbrsite_pk,
                t6.tech_pk as nbrtech_pk,
                t6.vendor_pk as nbrvendor_pk,
                t6.pk as nbrcell_pk,
                t1."DATETIME" ,
                t1."DATETIME" ,
                0, -- system
                0
                FROM 
                zte_cm."EutranRelation" t1
                INNER JOIN zte_cm."GsmCell" t2 on 
                    t2."FILENAME"  = t1."FILENAME" 
                    AND t2."DATETIME"  = t1."DATETIME"
                    AND t1."GsmCell_id" = t2."GsmCell_id"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."userLabel" 
                    AND t4.vendor_pk = 3 AND t4.tech_pk = 1
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 3 AND t5.tech_pk = 1
                -- ---------
                LEFT JOIN zte_cm."EUtranCellFDD" t8 on 
                    t8."FILENAME" = t1."FILENAME"
                    AND t8."DATETIME" = t1."DATETIME"
                    AND CONCAT('SubNetwork=',TRIM(t8."SubNetwork_id"),',SubNetwork=',TRIM(t8."SubNetwork_2_id"),',MEID=',TRIM(t8."meContext_id"),',ENBFunctionFDD=',TRIM(t8."ENBFunction_id"),',EUtranCellFDD=',TRIM(t8."EUtranCellFDD_id")) = 
                    t1."refGExternalEutranCellFDD"
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."userLabel"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                t1."GsmCell_id" IS NOT NULL
                AND t4.site_pk = '{0}'

            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_zte_3g2g_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=3).filter_by(tech_pk=2).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t5.node_pk as svrnode_pk,
                t4.site_pk as svrsite_pk,
                t4.tech_pk as svrtech_pk,
                t4.vendor_pk as svrvendor_pk,
                t4.pk as svrcell_pk,
                -- nbr side
                t7.node_pk as nbrnode_pk,
                t6.site_pk as nbrsite_pk,
                t6.tech_pk as nbrtech_pk,
                t6.vendor_pk as nbrvendor_pk,
                t6.pk as nbrcell_pk,
                t1."DATETIME" ,
                t1."DATETIME" ,
                0, -- system
                0
                FROM 
                zte_cm."GsmRelation" t1
                INNER JOIN zte_cm."UtranCellFDD" t2 on 
                    t2."FILENAME"  = t1."FILENAME" 
                    AND t2."DATETIME"  = t1."DATETIME"
                    AND t1."UtranCellFDD_id" = t2."UtranCellFDD_id"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."userLabel" 
                    AND t4.vendor_pk = 3 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 3 AND t5.tech_pk = 2
                -- ---------
                LEFT JOIN zte_cm."GsmCell" t8 on 
                    t8."FILENAME" = t1."FILENAME"
                    AND t8."DATETIME" = t1."DATETIME"
                    AND CONCAT('SubNetwork=',TRIM(t8."SubNetwork_id"),',SubNetwork=',TRIM(t8."SubNetwork_2_id"),',MeContext=',TRIM(t8."meContext_id"),',ManagedElement=',TRIM(t8."ManagedElement_id"),',BssFunction=',TRIM(t8."BssFunction_id"),',BtsSiteManager=',t8."BtsSiteManager_id",',GsmCell=',TRIM(t8."GsmCell_id")) = t1."adjacentCell"
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."userLabel"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                t1."UtranCellFDD_id" IS NOT NULL
                AND t4.site_pk = '{0}'

            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_zte_3g3g_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=3).filter_by(tech_pk=2).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t5.node_pk as svrnode_pk,
                t4.site_pk as svrsite_pk,
                t4.tech_pk as svrtech_pk,
                t4.vendor_pk as svrvendor_pk,
                t4.pk as svrcell_pk,
                -- nbr side
                t7.node_pk as nbrnode_pk,
                t6.site_pk as nbrsite_pk,
                t6.tech_pk as nbrtech_pk,
                t6.vendor_pk as nbrvendor_pk,
                t6.pk as nbrcell_pk,
                t1."DATETIME" ,
                t1."DATETIME" ,
                0, -- system
                0
                FROM 
                zte_cm."UtranRelation" t1
                INNER JOIN zte_cm."UtranCellFDD" t2 on 
                    t2."FILENAME"  = t1."FILENAME" 
                    AND t2."DATETIME"  = t1."DATETIME"
                    AND t1."UtranCellFDD_id" = t2."UtranCellFDD_id"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."userLabel" 
                    AND t4.vendor_pk = 3 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 3 AND t5.tech_pk = 2
                -- ---------
                LEFT JOIN zte_cm."UtranCellFDD" t8 on 
                    t8."FILENAME" = t1."FILENAME"
                    AND t8."DATETIME" = t1."DATETIME"
                    AND CONCAT('SubNetwork=',TRIM(t8."SubNetwork_id"),',SubNetwork=',TRIM(t8."SubNetwork_2_id"),',MeContext=',TRIM(t8."meContext_id"),',ManagedElement=',TRIM(t8."ManagedElement_id"),',RncFunction=',TRIM(t8."RncFunction_id"),',UtranCellFDD=',t8."UtranCellFDD_id") = t1."adjacentCell"
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."userLabel"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                t1."UtranCellFDD_id" IS NOT NULL
                AND t4.site_pk = '{0}'

            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_zte_3g4g_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=3).filter_by(tech_pk=2).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t5.node_pk as svrnode_pk,
                t4.site_pk as svrsite_pk,
                t4.tech_pk as svrtech_pk,
                t4.vendor_pk as svrvendor_pk,
                t4.pk as svrcell_pk,
                -- nbr side
                t7.node_pk as nbrnode_pk,
                t6.site_pk as nbrsite_pk,
                t6.tech_pk as nbrtech_pk,
                t6.vendor_pk as nbrvendor_pk,
                t6.pk as nbrcell_pk,
                t1."DATETIME" ,
                t1."DATETIME" ,
                0, -- system
                0
                FROM 
                zte_cm."UtranRelation" t1
                INNER JOIN zte_cm."UtranCellFDD" t2 on 
                    t2."FILENAME"  = t1."FILENAME" 
                    AND t2."DATETIME"  = t1."DATETIME"
                    AND t1."UtranCellFDD_id" = t2."UtranCellFDD_id"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."userLabel" 
                    AND t4.vendor_pk = 3 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 3 AND t5.tech_pk = 2
                -- ---------
                LEFT JOIN zte_cm."EUtranCellFDD" t8 on 
                    t8."FILENAME" = t1."FILENAME"
                    AND t8."DATETIME" = t1."DATETIME"
                    AND CONCAT('SubNetwork=',TRIM(t8."SubNetwork_id"),',SubNetwork=',TRIM(t8."SubNetwork_2_id"),',MEID=',TRIM(t8."meContext_id"),',ENBFunctionFDD=',TRIM(t8."ENBFunction_id"),',EUtranCellFDD=',TRIM(t8."EUtranCellFDD_id")) = 
                    t1."adjacentCell"
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."userLabel"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                t1."EUtranCellFDD_id" IS NOT NULL
                AND t4.site_pk = '{0}'
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_zte_4g2g_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=3).filter_by(tech_pk=3).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t5.node_pk as svrnode_pk,
                t4.site_pk as svrsite_pk,
                t4.tech_pk as svrtech_pk,
                t4.vendor_pk as svrvendor_pk,
                t4.pk as svrcell_pk,
                -- nbr side
                t7.node_pk as nbrnode_pk,
                t6.site_pk as nbrsite_pk,
                t6.tech_pk as nbrtech_pk,
                t6.vendor_pk as nbrvendor_pk,
                t6.pk as nbrcell_pk,
                t1."DATETIME" ,
                t1."DATETIME" ,
                0, -- system
                0
                FROM 
                zte_cm."GsmRelation" t1
                INNER JOIN zte_cm."EUtranCellFDD" t2 on 
                    t2."FILENAME"  = t1."FILENAME" 
                    AND t2."DATETIME"  = t1."DATETIME"
                    AND t1."EUtranCellFDD_id" = t2."EUtranCellFDD_id"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."userLabel" 
                    AND t4.vendor_pk = 3 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 3 AND t5.tech_pk = 2
                -- ---------
                LEFT JOIN zte_cm."GsmCell" t8 on 
                    t8."FILENAME" = t1."FILENAME"
                    AND t8."DATETIME" = t1."DATETIME"
                    AND CONCAT('SubNetwork=',TRIM(t8."SubNetwork_id"),',SubNetwork=',TRIM(t8."SubNetwork_2_id"),',MeContext=',TRIM(t8."meContext_id"),',ManagedElement=',TRIM(t8."ManagedElement_id"),',BssFunction=',TRIM(t8."BssFunction_id"),',BtsSiteManager=',t8."BtsSiteManager_id",',GsmCell=',TRIM(t8."GsmCell_id")) = t1."adjacentCell"
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."userLabel"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                t1."GsmCell_id" IS NOT NULL
                AND t4.site_pk = '{0}'
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_zte_4g3g_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=3).filter_by(tech_pk=3).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t5.node_pk as svrnode_pk,
                t4.site_pk as svrsite_pk,
                t4.tech_pk as svrtech_pk,
                t4.vendor_pk as svrvendor_pk,
                t4.pk as svrcell_pk,
                -- nbr side
                t7.node_pk as nbrnode_pk,
                t6.site_pk as nbrsite_pk,
                t6.tech_pk as nbrtech_pk,
                t6.vendor_pk as nbrvendor_pk,
                t6.pk as nbrcell_pk,
                t1."DATETIME" ,
                t1."DATETIME" ,
                0, -- system
                0
                FROM 
                zte_cm."UtranRelation" t1
                INNER JOIN zte_cm."EUtranCellFDD" t2 on 
                    t2."FILENAME"  = t1."FILENAME" 
                    AND t2."DATETIME"  = t1."DATETIME"
                    AND t1."EUtranCellFDD_id" = t2."EUtranCellFDD_id"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."userLabel" 
                    AND t4.vendor_pk = 3 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 3 AND t5.tech_pk = 2
                -- ---------
                LEFT JOIN zte_cm."UtranCellFDD" t8 on 
                    t8."FILENAME" = t1."FILENAME"
                    AND t8."DATETIME" = t1."DATETIME"
                    AND CONCAT('SubNetwork=',TRIM(t8."SubNetwork_id"),',SubNetwork=',TRIM(t8."SubNetwork_2_id"),',MeContext=',TRIM(t8."meContext_id"),',ManagedElement=',TRIM(t8."ManagedElement_id"),',RncFunction=',TRIM(t8."RncFunction_id"),',UtranCellFDD=',t8."UtranCellFDD_id") = t1."adjacentCell"
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."userLabel"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                t1."UtranCellFDD_id" IS NOT NULL
                AND t4.site_pk = '{0}'
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_zte_4g4g_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=3).filter_by(tech_pk=3).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t5.node_pk as svrnode_pk,
                t4.site_pk as svrsite_pk,
                t4.tech_pk as svrtech_pk,
                t4.vendor_pk as svrvendor_pk,
                t4.pk as svrcell_pk,
                -- nbr side
                t7.node_pk as nbrnode_pk,
                t6.site_pk as nbrsite_pk,
                t6.tech_pk as nbrtech_pk,
                t6.vendor_pk as nbrvendor_pk,
                t6.pk as nbrcell_pk,
                t1."DATETIME" ,
                t1."DATETIME" ,
                0, -- system
                0
                FROM 
                zte_cm."EUtranRelation" t1
                INNER JOIN zte_cm."EUtranCellFDD" t2 on 
                    t2."FILENAME"  = t1."FILENAME" 
                    AND t2."DATETIME"  = t1."DATETIME"
                    AND t1."EUtranCellFDD_id" = t2."EUtranCellFDD_id"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."userLabel" 
                    AND t4.vendor_pk = 3 AND t4.tech_pk = 3
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 3 AND t5.tech_pk = 3
                -- ---------
                LEFT JOIN zte_cm."EUtranCellFDD" t8 on 
                    t8."FILENAME" = t1."FILENAME"
                    AND t8."DATETIME" = t1."DATETIME"
                    AND CONCAT('SubNetwork=',TRIM(t8."SubNetwork_id"),',SubNetwork=',TRIM(t8."SubNetwork_2_id"),',MEID=',TRIM(t8."meContext_id"),',ENBFunctionFDD=',TRIM(t8."ENBFunction_id"),',EUtranCellFDD=',TRIM(t8."EUtranCellFDD_id")) = 
                    t1."adjacentCell"
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."userLabel"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                t1."EUtranCellFDD_id" IS NOT NULL
                AND t4.site_pk = '{0}'

            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_externals_on_2g(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.gsm_external_cells
        (pk, name, cell_pk, node_pk, mcc, mnc, lac, bcch, ncc, bcc, ci, rac, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_gsm_external_cells_pk') as pk,
        t1."userLabel" AS "name",
        t3.pk AS cell_pk,
        1 AS node_pk, -- SubNetwork
        t1."mcc"::integer AS mcc,
        t1."mnc"::integer AS mnc,
        t1."lac"::integer AS lac,
        t1."bcchFrequency"::integer AS bcch,
        t1."ncc"::integer AS ncc,
        t1."bcc"::integer AS bcc,
        t1."cellIdentity"::integer AS ci,
        t1."rac"::integer AS rac,
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        zte_cm."ExternalGsmCell" T1
        LEFT JOIN live_network.cells t3 on t3."name" = t1."userLabel"
        LEFT JOIN live_network.gsm_external_cells t4 on t4."name" = t1."userLabel" 
            AND t4.lac = t1."lac"::integer
            AND t4.ci = t1."cellIdentity"::integer
        WHERE 
        t4.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_externals_on_3g(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.umts_external_cells
        (pk, name, cell_pk,  mcc, mnc, lac, rncid, ci, psc, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_umts_external_cells_pk') AS pk,
        t1."userLabel" AS "name",
        t3.pk AS cell_pk,
        t1."mcc"::integer AS mcc,
        t1."mnc"::integer AS mnc,
        t1."lac"::integer AS lac,
        t1."rncId"::integer AS rncid,
        t1."cId"::integer AS ci,
        t1."primaryScramblingCode"::integer AS psc,
        0 AS modified_by,
        0 AS added_by,
        now()::timestamp AS date_added,
        now()::timestamp AS date_modified
        FROM
        zte_cm."ExternalUtranCellFDD" t1
        LEFT JOIN live_network.cells t3 on t3."name" = t1."userLabel"
        LEFT JOIN live_network.gsm_external_cells t4 on t4."name" = t1."userLabel" 
            AND t4.lac = t1."lac"::integer
            AND t4.ci = t1."cId"::integer
        WHERE 
        t4.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_externals_on_4g(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.lte_external_cells
        (pk, name, cell_pk, node_pk, mcc, mnc, pci, dl_earfcn, ci, tac, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_lte_external_cells_pk') AS pk,
        t1."userLabel" AS "name",
        t3.pk AS cell_pk,
        1 AS node_pk,
        t1."mcc"::integer AS mcc,
        t1."mnc"::integer AS mnc,
        t1."pci"::integer AS pci,
        t1."earfcnDl"::integer AS dl_earfcn,
        t1."cellIdentity"::integer AS ci,
        t1."tac"::integer AS tac,
        0 AS modified_by,
        0 AS added_by,
        now()::timestamp AS date_added,
        now()::timestamp AS date_modified
        FROM
        zte_cm."ExternalEUtranCellFDD" t1
        LEFT JOIN live_network.cells t3 on t3."name" = t1."userLabel"
        LEFT JOIN live_network.gsm_external_cells t4 on t4."name" = t1."userLabel" 
            AND t4.lac = t1."lac"::integer
            AND t4.ci = t1."cellIdentity"::integer
        WHERE 
        t4.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()