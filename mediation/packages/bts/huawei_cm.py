from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import os
import subprocess
import logging

class HuaweiCM(object):
    """Process Huawie configuration management data"""

    def __init__(self):
        self.db_engine = create_engine('postgresql://bodastage:password@database/bts')

    def extract_live_network_bscs(self):
        """Extact BSCs from the live network"""
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
            t1."SYSOBJECTID" AS "name" , 
            2 AS vendor_pk, -- 1=Ericsson, 2=Huawei
            1 AS tech_pk , -- 1=gsm, 2-umts,3=lte
            0 AS added_by,
            0 AS modified_by
            FROM huawei_cm."SYS" t1
             INNER JOIN cm_loads t3 on t3.pk = t1."LOADID"
             INNER JOIN huawei_cm."BSCBASIC" t4 ON t4."FILENAME" = t1."FILENAME" AND t4."LOADID" = t1."LOADID"
            LEFT OUTER  JOIN live_network.nodes t2 ON t1."SYSOBJECTID" = t2."name"
            WHERE 
            t2."name" IS NULL
             ON CONFLICT ON CONSTRAINT unique_nodes
             DO NOTHING
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_rncs(self):
        """Extract Huawei RNCs"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
             INSERT INTO live_network.nodes
             (pk,date_added, date_modified, type,"name", vendor_pk, tech_pk, added_by, modified_by)
             SELECT 
             NEXTVAL('live_network.seq_nodes_pk'),
             t1."DATETIME" AS date_added, 
             t1."DATETIME" AS date_modified, 
             'RNC' AS node_type,
             t1."SYSOBJECTID" AS "name" , 
             2 AS vendor_pk, -- 1=Ericsson, 2=Huawei, 3-ZTE
             2 AS tech_pk , -- 1=gsm, 2-umts,3=lte
             0 AS added_by,
             0 AS modified_by
             FROM huawei_cm."SYS" t1
             INNER JOIN cm_loads t3 on t3.pk = t1."LOADID"
             INNER JOIN huawei_cm."URNCBASIC" t4 ON t4."FILENAME" = t1."FILENAME" AND t4."LOADID" = t1."LOADID"
             LEFT OUTER  JOIN live_network.nodes t2 ON t1."SYSOBJECTID" = t2."name"
             WHERE 
             t2."name" IS NULL
             AND t3.is_current_load = true
             ON CONFLICT ON CONSTRAINT unique_nodes
             DO NOTHING
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_enodebs(self):
        """Extract Huawei ENodeBs from itf-N dumps"""
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
             2 AS vendor_pk, -- 1=Ericsson, 2=Huawei
             t1."ENODEBFUNCTIONNAME",
             0 AS added_by,
             0 AS modified_by 
             FROM
             huawei_cm."ENODEBFUNCTION" t1
             INNER JOIN cm_loads t3 on t3.pk = t1."LOADID"
             LEFT OUTER  JOIN live_network.sites t2 ON t1."ENODEBFUNCTIONNAME" = t2."name"
             WHERE 
             t2."name" IS NULL
              AND t3.is_current_load = true
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_2g_sites(self):
        """Extract Huawei 2G sites from itf-N dumps"""
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
            2 AS vendor_pk, -- 1- Ericsson, 2 - Huawei, 3 - zte, 4-nokika, etc...
            t1."BTSNAME" AS "name",
            t3.pk as node_pk -- node primary key
            from huawei_cm."BTS" t1
            INNER JOIN cm_loads t5 on t5.pk = t1."LOADID"
            INNER JOIN  huawei_cm."SYS" t2 ON t2."FILENAME" = t1."FILENAME" AND t2."LOADID" = t1."LOADID"
            INNER join live_network.nodes t3 on t3."name" = t2."SYSOBJECTID" 
                AND t3.vendor_pk = 2 and t3.tech_pk = 1
            LEFT JOIN live_network.sites t4 on t4."name" = t1."BTSNAME" 
               AND t4.vendor_pk = 2 and t4.tech_pk = 1
            WHERE 
            t4."name" IS NULL
            AND t5.is_current_load = true
            ON CONFLICT ON CONSTRAINT uq_site
            DO NOTHING
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))
        session.close()

    def extract_live_network_2g_cells(self):
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
            2, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
            t1."CELLNAME" AS name,
            t4.pk -- site primary key
            FROM huawei_cm."GCELL" t1
            -- LOAD
            INNER JOIN cm_loads t8 on t8.pk = t1."LOADID"
            -- NE/SYS
            INNER JOIN huawei_cm."SYS" t9 ON t9."FILENAME" = t1."FILENAME" AND t9."LOADID" = t1."LOADID"
            INNER JOIN live_network.nodes t3 on t3."name" = t9."SYSOBJECTID" 
                    AND t3.vendor_pk = 2
                    AND t3.tech_pk = 1
            INNER JOIN huawei_cm."CELLBIND2BTS" t6 ON t6."FILENAME" = t1."FILENAME" AND t6."CELLID" = t1."CELLID" AND t6."LOADID" = t1."LOADID"
            INNER JOIN huawei_cm."BTS" t7 on t7."FILENAME" = t1."FILENAME" AND t7."BTSID" = t6."BTSID" AND t7."LOADID" = t1."LOADID"
            INNER JOIN live_network.sites t4 on t4."name" = t7."BTSNAME"
                AND t4.vendor_pk = 2 
                AND t4.tech_pk = 1
                AND t4.node_pk = t3.pk
            LEFT JOIN live_network.cells t5 on t5."name" = trim(t1."CELLNAME")
                AND t5.tech_pk = 1
                AND t5.vendor_pk = 2
            WHERE
            t5."name" IS NULL
            AND t8.is_current_load = true
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_2g_cells_params(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        # @TODO: Update live_network.gsm_cells_data instead of truncating it
        # Truncate paramete table
        # self.db_engine.execute(text("TRUNCATE TABLE live_network.gsm_cells_data").execution_options(autocommit=True))
        # self.db_engine.execute(text("ALTER SEQUENCE live_network.seq_gsm_cells_data_pk RESTART WITH 1;").
        #                       execution_options(autocommit=True))

        # The data is alot. Let's handle per site
        site_sql = """SELECT pk, "name" from live_network.sites where vendor_pk  = 2 and tech_pk = 1"""

        result = self.db_engine.execute(site_sql)

        # for row in result:
        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=1).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            logging.info("Extracting cells parameters for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                         INSERT INTO live_network.gsm_cells_data
                         (pk, name, cell_pk, ci, bcc, ncc, bsic, bcch, lac, latitude, longitude, cgi, azimuth, height, 
                         mechanical_tilt, electrical_tilt, hsn, hopping_type, tch_carriers, mcc, mnc, modified_by, added_by, date_added, date_modified)
                         SELECT 
                         NEXTVAL('live_network.seq_gsm_cells_data_pk') as pk,
                          t1."CELLNAME" AS name,
                         t2.pk AS cell_pk,
                         t1."CI"::integer AS ci,
                         t1."BCC"::integer AS bcc,
                         t1."NCC"::integer AS ncc,
                         CONCAT(trim(t1."NCC"),trim(t1."BCC"))::integer AS bsic,
                         t4."FREQ"::integer AS bcch,
                         t1."LAC"::integer AS lac,
                         t6."LATIINT"::float AS latitude,
                         t6."LONGIINT"::float as longitude ,
                         CONCAT( TRIM(t1."MCC"),'-', TRIM(t1."MNC"),'-',TRIM(t1."LAC"),'-',TRIM(t1."CI")) AS cgi,
                         t6."ANTAANGLE"::integer AS azimuth,
                         t6."ALTITUDE"::integer AS height,
                         null AS mechanical_tilt,
                         -- t1."SECTOR_ANGLE"::integer AS sector_angle,
                         -- t6."MAXTA" AS ta
                         -- t1."STATE" AS STATE -- ACTIVE or INACTIVE
                         null AS electrical_tilt,
                         null AS hsn,
                         null AS hopping_type,
                         null AS tch_carriers,
                         t1."MCC"::integer as mcc,
                         t1."MNC"::integer as mnc,
                       0 AS modified_by,
                         0 AS added_by,
                         t1."DATETIME" AS date_added,
                         t1."DATETIME" AS date_modified            
                         FROM huawei_cm."GCELL" t1             
                         INNER JOIN cm_loads t8 on t8.pk = t1."LOADID"
                         INNER JOIN live_network.cells t2 on t2."name" = t1."CELLNAME" AND t2.vendor_pk = 2 AND t2.tech_pk = 1
                         INNER JOIN huawei_cm."GCELLBASICPARA" t3 on t3."FILENAME" = t1."FILENAME" AND t3."LOADID" = t1."LOADID"
                         INNER JOIN huawei_cm."GTRX" t4 on t4."FILENAME" = t1."FILENAME" AND t4."CELLID" = t1."CELLID" AND t4."LOADID" = t1."LOADID"
                         INNER JOIN live_network.sites t5 on t5.pk = t2.site_pk
                         INNER JOIN huawei_cm."GCELLLCS" t6 on t6."FILENAME" = t1."FILENAME" AND t6."CELLID" = t1."CELLID" AND t6."LOADID" = t1."LOADID"
                         INNER JOIN huawei_cm."CELLBIND2BTS" t7 on t7."CELLID" = t1."CELLID" AND t6."FILENAME" = t1."FILENAME" AND t7."LOADID" = t1."LOADID"
                          WHERE 
                          t5."name" ='{0}'
                          AND t8.is_current_load = true
                         ;
                     """.format(site_name)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_2g2g_nbrs_internal(self):
        """Extract Huawei 2G-2G neighbour relations"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=1).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print("Extracting Huawei 2G-2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t4.node_pk AS svrnode_pk,
                t3.site_pk AS svrsite_pk,
                t3.tech_pk AS svrtech_pk,
                t3.vendor_pk AS svrvendor_pk,
                t3.pk AS svrcell_pk,
                -- nbr side
                t7.node_pk AS nbrnode_pk,
                t6.site_pk AS nbrsite_pk,
                t6.tech_pk AS nbrtech_pk,
                t6.vendor_pk AS nbrvendor_pk,
                t6.pk AS nbrcell_pk,
                t1."DATETIME" AS date_added,
                t1."DATETIME" AS date_modified,
                0 AS modified_by,
                0 AS added_by
                from huawei_cm."G2GNCELL" t1
                --LOAD
                INNER JOIN cm_loads t8 on t8.pk = t1."LOADID"
                -- svr side
                INNER JOIN huawei_cm."GCELL" t2 ON t2."FILENAME" = t1."FILENAME" AND t2."CELLID" = t1."SRC2GNCELLID" AND t2."LOADID" = t1."LOADID"
                INNER JOIN live_network.cells t3 ON t3.name = t2."CELLNAME" AND t3.vendor_pk = 2 AND t3.tech_pk = 1
                INNER JOIN live_network.sites t4 ON t4.pk = t3.site_pk AND t4.vendor_pk = 2 AND t4.tech_pk = 1
                -- nbr side
                INNER JOIN huawei_cm."GCELL" t5 on  t5."FILENAME" = t1."FILENAME" AND t5."CELLID" = t1."NBR2GNCELLID" 
                INNER JOIN live_network.cells t6 ON t6.name = t5."CELLNAME" AND t6.vendor_pk = 2 AND t6.tech_pk = 1
                INNER JOIN live_network.sites t7 ON t7.pk = t6.site_pk AND t7.vendor_pk = 2 AND t7.tech_pk = 1
                WHERE
                 t3.site_pk = '{0}'
                 AND t8.is_current_load = true
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_2g2g_nbrs_external(self):
        """
        Extract Huawei 2G-2G neighbour relations
        """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=1).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print("Extracting Huawei 2G-2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                 INSERT INTO live_network.relations 
                 (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                 SELECT 
                 NEXTVAL('live_network.seq_relations_pk'),
                 -- serving side
                 t5.node_pk AS svrnode_pk,
                 t4.site_pk AS svrsite_pk,
                 t4.tech_pk AS svrtech_pk,
                 t4.vendor_pk AS svrvendor_pk,
                 t4.pk AS svrcell_pk,
                 -- nbr side
                 t7.node_pk AS nbrnode_pk,
                 t6.site_pk AS nbrsite_pk,
                 t6.tech_pk AS nbrtech_pk,
                 t6.vendor_pk AS nbrvendor_pk,
                 t6.pk AS nbrcell_pk,
                 t1."DATETIME" ,
                 t1."DATETIME" ,
                 0, -- system
                 0
                 FROM 
                 huawei_cm_2g."G2GNCELL" t1
                --LOAD
                INNER JOIN cm_loads t8 on t8.pk = t1."LOADID"
                -- 
                 INNER JOIN huawei_cm_2g."GCELL" t2 ON 
                     t2."FILENAME" = t1."FILENAME" 
                     AND t2."LOADID" = t1."LOADID"
                     AND t1."SRC2GNCELLID" = t2."CELLID"
                 LEFT JOIN huawei_cm_2g."GEXT2GCELL" t3 ON 
                     t3."FILENAME"  = t1."FILENAME"
                     AND t3."LOADID" = t1."LOADID"
                     AND t3."EXT2GCELLID" = t1."NBR2GNCELLID"
                 INNER JOIN live_network.cells t4 ON 
                     t4.name = t2."CELLNAME" 
                     AND t4.vendor_pk = 2 AND t4.tech_pk = 1
                 INNER JOIN live_network.sites t5 ON 
                     t5.pk = t4.site_pk 
                     AND t5.vendor_pk = 2 AND t5.tech_pk = 1
                 -- --------------
                 -- nbrs side 
                 LEFT JOIN live_network.cells t6 ON 
                     t6.name = t3."EXT2GCELLNAME"
                 LEFT JOIN live_network.sites t7 ON 
                     t7.pk = t6.site_pk 
                  WHERE 
                      t4.site_pk = '{0}'
                  AND t8.is_current_load = true
             """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_2g2g_nbrs_with_other_vendors(self):
        """Extract Huawei 2G2G relations with Ericsson"""
        """
        Extract  Huawei 2G- Ericsson 2G neighbour relations

        Source cell is Huawei and nbr cell is Huawei but on diffrent BSCs
        """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=1).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print("Extracting Huawei 2G- Ericsson 2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk') as pk,
                -- serving side
--                 t10."SYSOBJECTID" AS svrnode,
--                 t2."CELLNAME" AS svrcell,
--                 t3."EXT2GCELLNAME" AS nbrcell,
                -- serving side
                t5.node_pk AS svrnode_pk,
                t4.site_pk AS svrsite_pk,
                t4.tech_pk AS svrtech_pk,
                t4.vendor_pk AS svrvendor_pk,
                t4.pk AS svrcell_pk,
                -- nbr side
                t7.node_pk AS nbrnode_pk,
                t6.site_pk AS nbrsite_pk,
                t6.tech_pk AS nbrtech_pk,
                t6.vendor_pk AS nbrvendor_pk,
                t6.pk AS svrcell_pk,
                t1."DATETIME" AS date_added ,
                t1."DATETIME" AS date_modified,
                0 as added_by, 
                0 AS modified_by
                FROM 
                huawei_cm."G2GNCELL" t1
                --LOAD
                INNER JOIN cm_loads t9 on t9.pk = t1."LOADID"
                -- SYS/NE
                INNER JOIN huawei_cm."SYS" t10 ON t10."FILENAME" = t1."FILENAME" 
                    AND t10."LOADID" = t1."LOADID"
                INNER JOIN huawei_cm_2g."GCELL" t2 ON 
                    t2."FILENAME" = t1."FILENAME" 
                    AND t2."LOADID" = t1."LOADID"
                    AND t1."SRC2GNCELLID" = t2."CELLID"
                LEFT JOIN huawei_cm."GEXT2GCELL" t3 ON 
                    t3."FILENAME"  = t1."FILENAME"
                    AND t3."LOADID" = t1."LOADID"
                    AND t3."EXT2GCELLID" = t1."NBR2GNCELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 1 AND t4.tech_pk = 1
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 1 AND t5.tech_pk = 1
                INNER JOIN live_network.cells t6 ON 
                    t6.name = t3."EXT2GCELLNAME"
                    AND t6.vendor_pk = 1 AND t6.tech_pk = 1
                INNER JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                    AND t7.vendor_pk = 1 AND t7.tech_pk = 1
                INNER JOIN ericsson_cm_2g."INTERNAL_CELL" t8 ON 
                    t8."CI" = t3."EXT2GCELLID"
                WHERE 
                    t4.site_pk = '{0}'
                    AND t9.is_current_load = true
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_2g2g_nbrs(self):
        self.extract_live_network_2g2g_nbrs_internal()
        self.extract_live_network_2g2g_nbrs_external()

    def extract_live_network_2g3g_nbrs(self):
        """Extract Huawei 2G3G relations """
        """
        Extract  Huawei 2G- 3G neighbour relations
        """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=1).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print("Extracting Huawei 2G- 3G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                        INSERT INTO live_network.relations 
                        (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                        SELECT 
                        NEXTVAL('live_network.seq_relations_pk') as pk,
                        -- serving side
        --                 t10."SYSOBJECTID" AS svrnode,
        --                 t2."CELLNAME" AS svrcell,
        --                 t3."EXT2GCELLNAME" AS nbrcell,
                        -- serving side
                        t7.node_pk AS svrnode_pk,
                        t4.site_pk AS svrsite_pk,
                        t4.tech_pk AS svrtech_pk,
                        t4.vendor_pk AS svrvendor_pk,
                        t4.pk AS svrcell_pk,
                        -- nbr side
                        t8.node_pk AS nbrnode_pk,
                        t5.site_pk AS nbrsite_pk,
                        t5.tech_pk AS nbrtech_pk,
                        t5.vendor_pk AS nbrvendor_pk,
                        t5.pk AS svrcell_pk,
                        t1."DATETIME" AS date_added ,
                        t1."DATETIME" AS date_modified,
                        0 as added_by, 
                        0 AS modified_by
                    FROM huawei_cm."G3GNCELL" t1
                    INNER JOIN cm_loads t9 on t9.pk = t1."LOADID"
                    INNER JOIN huawei_cm."GCELL" t2 ON t2."CELLID" = t1."SRC3GNCELLID" AND t2."LOADID" = t1."LOADID" AND t2."FILENAME" = t1."FILENAME" 
                    INNER JOIN huawei_cm."GEXT3GCELL" t3 ON t3."EXT3GCELLID" = t1."NBR3GNCELLID" AND t3."LOADID" = t1."LOADID" AND t3."FILENAME" = t1."FILENAME" 
                    INNER JOIN live_network.cells t4 on t4."name" = t2."CELLNAME" 
                    INNER JOIN live_network.cells t5 on t5."name" = t3."EXT3GCELLNAME"  
                    INNER JOIN live_network.sites t7 on t7.pk = t4.site_pk
                    INNER JOIN live_network.sites t8 on t8.pk = t5.site_pk
                    WHERE 
                    t9.is_current_load = true
                    AND t4.site_pk = '{0}'
                    """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_2g4g_nbrs(self):
        """
        Extract  Huawei 2G- 4G neighbour relations
        """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=1).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print("Extracting Huawei 2G - 4G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                        INSERT INTO live_network.relations 
                        (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                        SELECT 
                        NEXTVAL('live_network.seq_relations_pk') as pk,
                        -- serving side
                        t7.node_pk AS svrnode_pk,
                        t4.site_pk AS svrsite_pk,
                        t4.tech_pk AS svrtech_pk,
                        t4.vendor_pk AS svrvendor_pk,
                        t4.pk AS svrcell_pk,
                        -- nbr side
                        t8.node_pk AS nbrnode_pk,
                        t5.site_pk AS nbrsite_pk,
                        t5.tech_pk AS nbrtech_pk,
                        t5.vendor_pk AS nbrvendor_pk,
                        t5.pk AS svrcell_pk,
                        t1."DATETIME" AS date_added ,
                        t1."DATETIME" AS date_modified,
                        0 as added_by, 
                        0 AS modified_by
                    FROM huawei_cm."GLTENCELL" t1
                    INNER JOIN cm_loads t9 on t9.pk = t1."LOADID"
                    INNER JOIN huawei_cm."GCELL" t2 ON t2."CELLID" = t1."SRCLTENCELLID" AND t2."LOADID" = t1."LOADID" AND t2."FILENAME" = t1."FILENAME" 
                    INNER JOIN huawei_cm."GEXTLTECELL" t3 ON t3."EXTLTECELLID" = t1."NBRLTENCELLID" AND t3."LOADID" = t1."LOADID" AND t3."FILENAME" = t1."FILENAME" 
                    INNER JOIN live_network.cells t4 on t4."name" = t2."CELLNAME" 
                    INNER JOIN live_network.cells t5 on t5."name" = t3."EXTLTECELLNAME" 
                    INNER JOIN live_network.sites t7 on t7.pk = t4.site_pk
                    INNER JOIN live_network.sites t8 on t8.pk = t5.site_pk
                    WHERE 
                    t9.is_current_load = true
                    AND t4.site_pk = '{0}'
                    """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g2g_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=2).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print("Extracting Huawei 3G - 2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                    INSERT INTO live_network.relations 
                    (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                    SELECT 
                    NEXTVAL('live_network.seq_relations_pk') as pk,
                    -- serving side
                        t7.node_pk AS svrnode_pk,
                        t4.site_pk AS svrsite_pk,
                        t4.tech_pk AS svrtech_pk,
                        t4.vendor_pk AS svrvendor_pk,
                        t4.pk AS svrcell_pk,
                        -- nbr side
                        t8.node_pk AS nbrnode_pk,
                        t5.site_pk AS nbrsite_pk,
                        t5.tech_pk AS nbrtech_pk,
                        t5.vendor_pk AS nbrvendor_pk,
                        t5.pk AS svrcell_pk,
                        t1."DATETIME" AS date_added ,
                        t1."DATETIME" AS date_modified,
                        0 as added_by, 
                        0 AS modified_by
                    FROM huawei_cm."U2GNCELL" t1
                    INNER JOIN cm_loads t9 on t9.pk = t1."LOADID"
                    INNER JOIN huawei_cm."UCELL" t2 ON t2."CELLID" = t1."CELLID" AND t2."LOADID" = t1."LOADID" AND t2."FILENAME" = t1."FILENAME" 
                    INNER JOIN huawei_cm."UEXT2GCELL" t3 ON t3."GSMCELLINDEX" = t1."GSMCELLINDEX" AND t3."LOADID" = t1."LOADID" AND t3."FILENAME" = t1."FILENAME" 
                    INNER JOIN live_network.cells t4 on t4."name" = t2."CELLNAME" 
                    INNER JOIN live_network.cells t5 on t5."name" = t3."GSMCELLNAME" and t5.tech_pk = 1
                    INNER JOIN live_network.sites t7 on t7.pk = t4.site_pk
                    INNER JOIN live_network.sites t8 on t8.pk = t5.site_pk
                    WHERE 
                    t9.is_current_load = true
                    AND t4.site_pk = '{0}'
                    """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g3g_intrafreq_nbrs_internal(self):
        """Extract Huawei 3g-3g interfreq relations on same RNC"""
        """
        Extract  Huawei 3G- Huawei 3G neighbour relations on same RNC
        """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=2).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print("Extracting Huawei 3G- Huawei 2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk') as pk,
                -- serving side
                t5.node_pk AS svrnode_pk,
                t4.site_pk AS svrsite_pk,
                t4.tech_pk AS svrtech_pk,
                t4.vendor_pk AS svrvendor_pk,
                t4.pk AS svrcell_pk,
                -- nbr side
                t7.node_pk AS nbrnode_pk,
                t6.site_pk AS nbrsite_pk,
                t6.tech_pk AS nbrtech_pk,
                t6.vendor_pk AS nbrvendor_pk,
                t6.pk AS nbrcell_pk,
                t1."DATETIME" AS date_added,
                t1."DATETIME" AS date_modified,
                0, -- system
                0
                FROM 
                huawei_cm."UINTRAFREQNCELL" t1
                --LOAD
                INNER JOIN cm_loads t8 on t8.pk = t1."LOADID"
                INNER JOIN huawei_cm."UCELL" t2 on 
                    t2."FILENAME"  = t1."FILENAME" 
                    AND t2."LOADID" = t1."LOADID"
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN huawei_cm."UCELL" t3 on 
                    t3."FILENAME" = t1."FILENAME"
                    AND t3."LOADID" = t1."LOADID"
                    AND t3."CELLID" = t1."NCELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                INNER JOIN live_network.cells t6 ON 
                    t6.name = t3."CELLNAME"
                    AND t6.vendor_pk = 2 AND t6.tech_pk = 2
                INNER JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                    AND t7.vendor_pk = 2 AND t7.tech_pk = 2
                WHERE 
                 t4.site_pk = '{0}'
                    AND t8.is_current_load = true
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g3g_intrafreq_nbrs_external(self):
        """Extract Huawei 3g-3g interfreq relations on different RNCs"""
        """
        Extract  Huawei 3G- Huawei 3G neighbour relations on different RNCs
        """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=2).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print("Extracting Huawei 3G- Huawei 2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk') AS pk,
                -- serving side
                t5.node_pk AS svrnode_pk,
                t4.site_pk AS svrsite_pk,
                t4.tech_pk AS svrtech_pk,
                t4.vendor_pk AS svrvendor_pk,
                t4.pk AS svrcell_pk,
                -- nbr side
                t7.node_pk AS nbrnode_pk,
                t6.site_pk AS nbrsite_pk,
                t6.tech_pk AS nbrtech_pk,
                t6.vendor_pk AS nbrvendor_pk,
                t6.pk AS nbrcell_pk,
                t1."DATETIME" ,
                t1."DATETIME" ,
                0, -- system
                0
                FROM 
                huawei_cm."UINTRAFREQNCELL" t1
                --LOAD
                INNER JOIN cm_loads t8 on t8.pk = t1."LOADID"
                
                INNER JOIN huawei_cm."UCELL" t2 on 
                    t2."FILENAME"  = t1."FILENAME" 
                    AND t2."LOADID" = t1."LOADID"
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN huawei_cm."UEXT3GCELL" t3 on 
                    t3."FILENAME" = t1."FILENAME"
                    AND t3."LOADID" = t1."LOADID"
                    AND t3."CELLID" = t1."NCELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                INNER JOIN live_network.cells t6 ON 
                    t6.name = t3."CELLNAME"
                    AND t6.vendor_pk = 2 AND t6.tech_pk = 2
                INNER JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                    AND t7.vendor_pk = 2 AND t7.tech_pk = 2
                WHERE 
                 t4.site_pk = '{0}'
                 AND t8.is_current_load = true
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g3g_interfreq_nbrs_internal(self):
        """Extract Huawei 3g-3g interfreq relations on same RNC"""
        """
        Extract  Huawei 3G- Huawei 3G neighbour relations on same RNC with diffrent frequencies
        """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=2).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print("Extracting Huawei 3G- Huawei 2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t5.node_pk AS svrnode_pk,
                t4.site_pk AS svrsite_pk,
                t4.tech_pk AS svrtech_pk,
                t4.vendor_pk AS svrvendor_pk,
                t4.pk AS svrcell_pk,
                -- nbr side
                t7.node_pk AS nbrnode_pk,
                t6.site_pk AS nbrsite_pk,
                t6.tech_pk AS nbrtech_pk,
                t6.vendor_pk AS nbrvendor_pk,
                t6.pk AS nbrcell_pk,
                t1."DATETIME" ,
                t1."DATETIME" ,
                0 AS added_by, -- system
                0 AS modified_by
                FROM 
                huawei_cm."UINTERFREQNCELL" t1
                --LOAD
                INNER JOIN cm_loads t8 on t8.pk = t1."LOADID"
                INNER JOIN huawei_cm."UCELL" t2 on 
                    t2."FILENAME"  = t1."FILENAME"
                    AND t2."LOADID" = t1."LOADID"
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN huawei_cm."UCELL" t3 on 
                    t3."FILENAME" = t1."FILENAME"
                    AND t3."LOADID" = t1."LOADID"
                    AND t3."CELLID" = t1."NCELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                INNER JOIN live_network.cells t6 ON 
                    t6.name = t3."CELLNAME"
                    AND t6.vendor_pk = 2 AND t6.tech_pk = 2
                INNER JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                    AND t7.vendor_pk = 2 AND t7.tech_pk = 2
                WHERE 
                 t4.site_pk = '{0}'
                 AND t8.is_current_load = true
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g3g_interfreq_nbrs_external(self):
            """Extract Huawei 3g-3g interfreq relations on different RNCs"""
            """
            Extract  Huawei 3G- Huawei 3G neighbour relations on different RNCs and different frequencies
            """
            Session = sessionmaker(bind=self.db_engine)
            session = Session()

            metadata = MetaData()
            Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
            for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=2).yield_per(5):
                (site_pk, site_name) = (site[0], site[1])

                print("Extracting Huawei 3G- Huawei 3G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

                sql = """
                    INSERT INTO live_network.relations 
                    (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                    SELECT 
                    NEXTVAL('live_network.seq_relations_pk'),
                    -- serving side
                    t5.node_pk AS svrnode_pk,
                    t4.site_pk AS svrsite_pk,
                    t4.tech_pk AS svrtech_pk,
                    t4.vendor_pk AS svrvendor_pk,
                    t4.pk AS svrcell_pk,
                    -- nbr side
                    t7.node_pk AS nbrnode_pk,
                    t6.site_pk AS nbrsite_pk,
                    t6.tech_pk AS nbrtech_pk,
                    t6.vendor_pk AS nbrvendor_pk,
                    t6.pk AS nbrcell_pk,
                    t1."DATETIME" ,
                    t1."DATETIME" ,
                    0, -- system
                    0
                    FROM 
                    huawei_cm."UINTERFREQNCELL" t1
                    INNER JOIN huawei_cm."UCELL" t2 on 
                        t2."FILENAME"  = t1."FILENAME" 
                        AND t1."CELLID" = t2."CELLID"
                    INNER JOIN huawei_cm."UEXT3GCELL" t3 on 
                        t3."FILENAME" = t1."FILENAME"
                        AND t3."CELLID" = t1."NCELLID"
                    INNER JOIN live_network.cells t4 ON 
                        t4.name = t2."CELLNAME" 
                        AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                    INNER JOIN live_network.sites t5 ON 
                        t5.pk = t4.site_pk 
                        AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                    INNER JOIN live_network.cells t6 ON 
                        t6.name = t3."CELLNAME"
                        AND t6.vendor_pk = 2 AND t6.tech_pk = 2
                    INNER JOIN live_network.sites t7 ON 
                        t7.pk = t6.site_pk 
                        AND t7.vendor_pk = 2 AND t7.tech_pk = 2
                    WHERE 
                     t4.site_pk = '{0}'
                """.format(site_pk)

                self.db_engine.execute(text(sql).execution_options(autocommit=True))

            session.close()

    def extract_live_network_3g3g_intrafreq_nbrs_with_all_vendors(self):
        """ H// 3G - E// 3G nbrs with the same frequency"""
        """Extract Huawei 3g-3g interfreq relations on same RNC"""
        """
        Extract  Huawei 3G - 3G neighbour relations on same RNC with different frequencies
        """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=2).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print(
            "Extracting Huawei 3G- Vendor 3G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t5.node_pk AS svrnode_pk,
                t4.site_pk AS svrsite_pk,
                t4.tech_pk AS svrtech_pk,
                t4.vendor_pk AS svrvendor_pk,
                t4.pk AS svrcell_pk,
                -- nbr side
                t7.node_pk AS nbrnode_pk,
                t6.site_pk AS nbrsite_pk,
                t6.tech_pk AS nbrtech_pk,
                t6.vendor_pk AS nbrvendor_pk,
                t6.pk AS svrcell_pk,
                t1."DATETIME" ,
                t1."DATETIME" ,
                0, -- system
                0
                FROM 
                huawei_cm."UINTERFREQNCELL" t1
                INNER JOIN huawei_cm."UCELL" t2 ON 
                    t2."FILENAME"   = t1."FILENAME"  
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN huawei_cm."UCELL" t3 ON 
                    t3."FILENAME"  = t1."FILENAME" 
                    AND t3."CELLID" = t1."NCELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                -- -----------
                LEFT JOIN huawei_cm."UEXT3GCELL" t8 ON 
                    t8."FILENAME"  = t1."FILENAME" 
                    AND t8."CELLID" = t1."NCELLID"
                LEFT JOIN live_network.cells t6 ON
                    t6.name = t8."CELLNAME"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                 t4.site_pk = '{0}'
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g3g_interfreq_nbrs_with_all_vendors(self):
        """
        Extract  Huawei 3G- Huawei 3G neighbour relations on same RNC with diffrent frequencies
        """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=2).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print(
            "Extracting Huawei 3G- Ericsson 3G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t5.node_pk AS svrnode_pk,
                t4.site_pk AS svrsite_pk,
                t4.tech_pk AS svrtech_pk,
                t4.vendor_pk AS svrvendor_pk,
                t4.pk AS svrcell_pk,
                -- nbr side
                t7.node_pk AS nbrnode_pk,
                t6.site_pk AS nbrsite_pk,
                t6.tech_pk AS nbrtech_pk,
                t6.vendor_pk AS nbrvendor_pk,
                t6.pk AS svrcell_pk,
                t1."DATETIME" ,
                t1."DATETIME" ,
                0, -- system
                0
                FROM 
                huawei_cm."UINTERFREQNCELL" t1
                INNER JOIN huawei_cm."UCELL" t2 on 
                    t2."FILENAME"  = t1."FILENAME" 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                -- ---------
                LEFT JOIN huawei_cm."UEXT3GCELL" t8 on 
                    t8."FILENAME" = t1."FILENAME"
                    AND t8."CELLID" = t1."NCELLID"
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."CELLNAME"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                 t4.site_pk = '{0}'
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g3g_nbrs(self):
        self.extract_live_network_3g3g_intrafreq_nbrs_external()
        self.extract_live_network_3g3g_intrafreq_nbrs_internal()
        self.extract_live_network_3g3g_interfreq_nbrs_internal()
        self.extract_live_network_3g3g_interfreq_nbrs_external()
        self.extract_live_network_3g3g_intrafreq_nbrs_with_all_vendors()
        self.extract_live_network_3g3g_interfreq_nbrs_with_all_vendors()

    def extract_live_network_2g3g_nbrs_with_all_vendors(self):
        """Extract 2G - 3G nbrs with all vendors"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=1).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print(
            "Extracting Huawei 3G- Ericsson 3G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t5.node_pk AS svrnode_pk,
                t4.site_pk AS svrsite_pk,
                t4.tech_pk AS svrtech_pk,
                t4.vendor_pk AS svrvendor_pk,
                t4.pk AS svrcell_pk,
                -- nbr side
                t7.node_pk AS nbrnode_pk,
                t6.site_pk AS nbrsite_pk,
                t6.tech_pk AS nbrtech_pk,
                t6.vendor_pk AS nbrvendor_pk,
                t6.pk AS svrcell_pk,
                t1."DATETIME" ,
                t1."DATETIME" ,
                0, -- system
                0
                FROM 
                huawei_cm."G3GNCELL" t1
                INNER JOIN huawei_cm."GCELL" t2 on 
                    t2."FILENAME"  = t1."FILENAME" 
                    AND t1."SRC3GNCELLID" = t2."CELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 1
                    AND t4.site_pk = '{0}'
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 
                    AND t5.tech_pk = 1
                    AND t5.pk = '{0}'
                -- nbr-----------
                LEFT JOIN huawei_cm."GEXT3GCELL" t8 on 
                    t8.neid = t1.neid
                    AND t1."NBR3GNCELLID" = t8."EXT3GCELLID"
                -- Vendor=Ericsson and Technology = UMTS
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."EXT3GCELLNAME"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                 t4.site_pk = '{0}'
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_huawei_2g4g_nbrs_with_all_vendors(self):
        """Extract 2G - 3G nbrs with all vendors"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=1).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print(
            "Extracting Huawei 3G- Ericsson 3G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t5.node_pk AS svrnode_pk,
                t4.site_pk AS svrsite_pk,
                t4.tech_pk AS svrtech_pk,
                t4.vendor_pk AS svrvendor_pk,
                t4.pk AS nbrcell_pk,
                -- nbr side
                t7.node_pk AS nbrnode_pk,
                t6.site_pk AS nbrsite_pk,
                t6.tech_pk AS nbrtech_pk,
                t6.vendor_pk AS nbrvendor_pk,
                t6.pk AS svrcell_pk,
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM 
                huawei_cm."U2GNCELL" t1
                INNER JOIN huawei_cm."UCELL" t2 on 
                    t2."FILENAME"  = t1."FILENAME" 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                -- nbr-----------
                LEFT JOIN huawei_cm_3g."UEXT2GCELL" t8 on 
                    t8."FILENAME" = t1."FILENAME"
                    AND t1."GSMCELLINDEX" = t8."GSMCELLINDEX"
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."GSMCELLNAME"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                 t4.site_pk = '{0}'
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g4g_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=2).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print(
            "Extracting Huawei 3G - 4G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                    t7.node_pk AS svrnode_pk,
                    t4.site_pk AS svrsite_pk,
                    t4.tech_pk AS svrtech_pk,
                    t4.vendor_pk AS svrvendor_pk,
                    t4.pk AS svrcell_pk,
                    -- nbr side
                    t8.node_pk AS nbrnode_pk,
                    t5.site_pk AS nbrsite_pk,
                    t5.tech_pk AS nbrtech_pk,
                    t5.vendor_pk AS nbrvendor_pk,
                    t5.pk AS svrcell_pk,
                    t1."DATETIME" AS date_added ,
                    t1."DATETIME" AS date_modified,
                    0 as added_by, 
                    0 AS modified_by
                FROM huawei_cm."ULTENCELL" t1
                INNER JOIN cm_loads t9 on t9.pk = t1."LOADID"
                INNER JOIN huawei_cm."UCELL" t2 ON t2."CELLID" = t1."CELLID" AND t2."LOADID" = t1."LOADID" AND t2."FILENAME" = t1."FILENAME" 
                INNER JOIN huawei_cm."ULTECELL" t3 ON t3."LTECELLINDEX" = t1."LTECELLINDEX" AND t3."LOADID" = t1."LOADID" AND t3."FILENAME" = t1."FILENAME" 
                INNER JOIN live_network.cells t4 on t4."name" = t2."CELLNAME" 
                INNER JOIN live_network.cells t5 on t5."name" = t3."LTECELLNAME"
                INNER JOIN live_network.sites t7 on t7.pk = t4.site_pk
                INNER JOIN live_network.sites t8 on t8.pk = t5.site_pk
                WHERE 
                t9.is_current_load = true
                AND  t4.site_pk = '{0}'

            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_4g2g_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=3).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print(
            "Extracting Huawei 4G - 2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                t5.node_pk AS svrnode_pk,
                t4.site_pk AS svrsite_pk,
                t4.tech_pk AS svrtech_pk,
                t4.vendor_pk AS svrvendor_pk,
                t4.pk AS svrcell_pk,
                -- nbr side
                t7.node_pk AS nbrnode_pk,
                t6.site_pk AS nbrsite_pk,
                t6.tech_pk AS nbrtech_pk,
                t6.vendor_pk AS nbrvendor_pk,
                t6.pk AS nbrcell_pk,
                t1."DATETIME" ,
                t1."DATETIME",
                0 as added_by,
                0 as modified_by
                FROM 
                huawei_cm."GERANNCELL" t1
                INNER JOIN cm_loads t9 on t9.pk = t1."LOADID"
                INNER JOIN huawei_cm."CELL" t2 on 
                    t2."FILENAME"  = t1."FILENAME" 
                    AND t1."LOCALCELLID" = t2."CELLID"
                    AND t2."LOADID" = t1."LOADID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 3
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 3
                -- ---------
                LEFT JOIN huawei_cm."GERANEXTERNALCELL" t8 ON
                    t8."FILENAME" = t1."FILENAME"
                    AND t8."GERANCELLID" = t1."GERANCELLID"
                    AND t8."LOADID" = t1."LOADID"
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."CELLNAME"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                 t4.site_pk = '{0}'
                 AND t9.is_current_load = true
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_4g3g_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=3).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print(
                "Extracting Huawei 4G - 2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                 INSERT INTO live_network.relations 
                 (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                 SELECT 
                 NEXTVAL('live_network.seq_relations_pk'),
                 -- serving side
                 t5.node_pk AS svrnode_pk,
                 t4.site_pk AS svrsite_pk,
                 t4.tech_pk AS svrtech_pk,
                 t4.vendor_pk AS svrvendor_pk,
                 t4.pk AS svrcell_pk,
                 -- nbr side
                 t7.node_pk AS nbrnode_pk,
                 t6.site_pk AS nbrsite_pk,
                 t6.tech_pk AS nbrtech_pk,
                 t6.vendor_pk AS nbrvendor_pk,
                 t6.pk AS nbrcell_pk,
                 t1."DATETIME" ,
                 t1."DATETIME" ,
                 0 as added_by, -- system
                 0 as modified_by
                 FROM 
                 huawei_cm."UTRANNCELL" t1
                 INNER JOIN cm_loads t9 on t9.pk = t1."LOADID"
                 INNER JOIN huawei_cm."CELL" t2 on 
                     t2."FILENAME"  = t1."FILENAME" 
                     AND t1."LOCALCELLID" = t2."CELLID"
                 INNER JOIN live_network.cells t4 ON 
                     t4.name = t2."CELLNAME" 
                     AND t4.vendor_pk = 2 AND t4.tech_pk = 3
                 INNER JOIN live_network.sites t5 ON 
                     t5.pk = t4.site_pk 
                     AND t5.vendor_pk = 2 AND t5.tech_pk = 3
                 -- ---------
                 LEFT JOIN huawei_cm."UTRANEXTERNALCELL" t8 on 
                     t8."FILENAME" = t1."FILENAME"
                     AND t8."CELLID" = t1."CELLID"
                 LEFT JOIN live_network.cells t6 ON 
                     t6.name = t8."CELLNAME"
                 LEFT JOIN live_network.sites t7 ON 
                     t7.pk = t6.site_pk 
                 WHERE 
                  t9.is_current_load = true
                 AND t4.site_pk = '{0}'
             """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_4g4g_intrafreq_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=3).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print(
                "Extracting Huawei 4G - 2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                 INSERT INTO live_network.relations 
                 (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                 SELECT 
                 NEXTVAL('live_network.seq_relations_pk'),
                 -- serving side
                 t5.node_pk AS svrnode_pk,
                 t4.site_pk AS svrsite_pk,
                 t4.tech_pk AS svrtech_pk,
                 t4.vendor_pk AS svrvendor_pk,
                 t4.pk AS svrcell_pk,
                 -- nbr side
                 t7.node_pk AS nbrnode_pk,
                 t6.site_pk AS nbrsite_pk,
                 t6.tech_pk AS nbrtech_pk,
                 t6.vendor_pk AS nbrvendor_pk,
                 t6.pk AS nbrcell_pk,
                 t1."DATETIME" ,
                 t1."DATETIME" ,
                 0, -- system
                 0
                 FROM 
                 huawei_cm."EUTRANINTRAFREQNCELL" t1
                 INNER JOIN cm_loads t9 on t9.pk = t1."LOADID"
                 INNER JOIN huawei_cm."CELL" t2 on 
                     t2."FILENAME"  = t1."FILENAME" 
                     AND t1."LOCALCELLID" = t2."CELLID"
                     AND t1."LOADID" = t2."LOADID"
                 INNER JOIN live_network.cells t4 ON 
                     t4.name = t2."CELLNAME" 
                     AND t4.vendor_pk = 2 AND t4.tech_pk = 3
                 INNER JOIN live_network.sites t5 ON 
                     t5.pk = t4.site_pk 
                     AND t5.vendor_pk = 2 AND t5.tech_pk = 3
                 -- ---------
                 LEFT JOIN huawei_cm."EUTRANEXTERNALCELL" t8 ON 
                     t8."FILENAME" = t1."FILENAME"
                     AND t8."CELLID" = t1."CELLID"
                     AND t8."LOADID" = t1."LOADID"
                 LEFT JOIN live_network.cells t6 ON 
                     t6.name = t8."CELLNAME"
                 LEFT JOIN live_network.sites t7 ON 
                     t7.pk = t6.site_pk 
                 WHERE 
                 t9.is_current_load = true
                 AND t4.site_pk = '{0}'
             """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_4g4g_interfreq_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=3).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print(
                "Extracting Huawei 4G - 4G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                 INSERT INTO live_network.relations 
                 (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                 SELECT 
                 NEXTVAL('live_network.seq_relations_pk'),
                 -- serving side
                 t5.node_pk AS svrnode_pk,
                 t4.site_pk AS svrsite_pk,
                 t4.tech_pk AS svrtech_pk,
                 t4.vendor_pk AS svrvendor_pk,
                 t4.pk AS svrcell_pk,
                 -- nbr side
                 t7.node_pk AS nbrnode_pk,
                 t6.site_pk AS nbrsite_pk,
                 t6.tech_pk AS nbrtech_pk,
                 t6.vendor_pk AS nbrvendor_pk,
                 t6.pk AS nbrcell_pk,
                 t1."DATETIME" ,
                 t1."DATETIME" ,
                 0, -- system
                 0
                 FROM 
                 huawei_cm."EUTRANINTERFREQNCELL" t1
                 INNER JOIN cm_loads t9 on t9.pk = t1."LOADID"
                 INNER JOIN huawei_cm."CELL" t2 on 
                     t2."FILENAME"  = t1."FILENAME" 
                     AND t1."LOCALCELLID" = t2."CELLID"
                     AND t1."LOADID" = t2."LOADID"
                 INNER JOIN live_network.cells t4 ON 
                     t4.name = t2."CELLNAME" 
                     AND t4.vendor_pk = 2 AND t4.tech_pk = 3
                 INNER JOIN live_network.sites t5 ON 
                     t5.pk = t4.site_pk 
                     AND t5.vendor_pk = 2 AND t5.tech_pk = 3
                 -- ---------
                 LEFT JOIN huawei_cm."EUTRANEXTERNALCELL" t8 ON 
                     t8."FILENAME" = t1."FILENAME"
                     AND t8."CELLID" = t1."CELLID"
                     AND t8."LOADID" = t1."LOADID"
                 LEFT JOIN live_network.cells t6 ON 
                     t6.name = t8."CELLNAME"
                 LEFT JOIN live_network.sites t7 ON 
                     t7.pk = t6.site_pk 
                 WHERE 
                 t9.is_current_load = true
                 AND t4.site_pk = '{0}'
             """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_4g4g_nbrs(self):
        self.extract_live_network_4g4g_intrafreq_nbrs()
        self.extract_live_network_4g4g_interfreq_nbrs()

    def extract_live_network_2g2g_nbrs_params(self):
        pass

    def extract_live_network_2g3g_nbrs_params(self):
        pass

    def extract_live_network_2g4g_nbrs_params(self):
        pass

    def extract_live_network_3g3g_nbrs_params(self):
        pass

    def extract_live_network_3g4g_nbrs_params(self):
        """Extract Huawei 3G4G relations from GExport dumps"""
        pass

    def extract_live_network_4g2g_nbrs_params(self):
        pass

    def extract_live_network_4g3g_nbrs_params(self):
        pass

    def extract_live_network_4g4g_nbrs_params(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=2).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            print(
            "Extracting Huawei 3G- Vendor 3G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t5.node_pk AS svrnode_pk,
                t4.site_pk AS svrsite_pk,
                t4.tech_pk AS svrtech_pk,
                t4.vendor_pk AS svrvendor_pk,
                t4.pk AS svrcell_pk,
                -- nbr side
                t7.node_pk AS nbrnode_pk,
                t6.site_pk AS nbrsite_pk,
                t6.tech_pk AS nbrtech_pk,
                t6.vendor_pk AS nbrvendor_pk,
                t6.pk AS svrcell_pk,
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM 
                huawei_cm_4g."EUTRANINTRAFREQNCELL" t1
                INNER JOIN huawei_cm_3g."UCELL" t2 on 
                    t2.neid  = t1.neid 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN huawei_cm_3g."UCELL" t3 on 
                    t3.neid = t1.neid
                    AND t3."CELLID" = t1."NCELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                -- -----------
                LEFT JOIN huawei_cm_3g."UEXT3GCELL" t8 on 
                    t8.neid = t1.neid
                    AND t8."CELLID" = t1."NCELLID"
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."CELLNAME"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                 t4.site_pk = '{0}'
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g_sites(self):
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
            2, -- 1- Ericsson, 2 - Huawei,
            t1."NODEBNAME",
            t2.pk -- node primary key
            from huawei_cm."UNODEB" t1
            INNER JOIN cm_loads t4 on t4.pk = t1."LOADID"
            INNER JOIN huawei_cm."SYS" t5 on t5."FILENAME" = t1."FILENAME" AND t5."LOADID" = t1."LOADID"
            INNER join live_network.nodes t2 on t2."name" = t5."SYSOBJECTID" 
                AND t2.vendor_pk = 2 and t2.tech_pk = 2
            LEFT JOIN live_network.sites t3 on t3."name" = t1."NODEBNAME"
               AND t2.vendor_pk = 2 and t2.tech_pk = 2
            WHERE 
            t3."name" IS NULL
            AND t4.is_current_load = true
            ON CONFLICT ON CONSTRAINT uq_site
            DO NOTHING
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g_cells(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        sites = session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=2).all()

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
                 2, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
                 t1."CELLNAME" AS name,
                 t4.pk -- site primary key
                 FROM huawei_cm."UCELL" t1
                 INNER JOIN cm_loads t6 on t6.pk = t1."LOADID"
                 INNER JOIN huawei_cm."SYS" t7 on t7."FILENAME" = t1."FILENAME" AND t7."LOADID" = t1."LOADID"
                 INNER JOIN live_network.nodes t3 on t3."name" = t7."SYSOBJECTID" 
                         AND t3.vendor_pk = 2
                         AND t3.tech_pk = 2
                 INNER JOIN live_network.sites t4 on t4."name" = t1."NODEBNAME"
                     AND t4.vendor_pk = 2
                     AND t4.tech_pk = 2
                     AND t4.node_pk = t3.pk
                 LEFT JOIN live_network.cells t5 on t5."name" = t1."CELLNAME"
                     AND t5.tech_pk = 2
                     AND t5.vendor_pk = 2
                 WHERE 
                 t5."name" IS NULL
                 AND t6.is_current_load = true
                 AND t4."name" IN ({})
             """.format(', '.join(placeholders))

            self.db_engine.execute(text(sql).execution_options(autocommit=True), **site_list_placeholders)

        session.close()

    def extract_live_network_3g_cells_params(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        # Truncate paramete table
        # self.db_engine.execute(text("TRUNCATE TABLE live_network.umts_cells_data").execution_options(autocommit=True))
        # self.db_engine.execute(text("ALTER SEQUENCE live_network.seq_umts_cells_data_pk RESTART WITH 1;").
        #                       execution_options(autocommit=True))

        # The data is alot. Let's handle per site
        site_sql = """SELECT pk, "name" from live_network.sites where vendor_pk  = 2 and tech_pk = 2"""

        result = self.db_engine.execute(site_sql)

        for row in result:
            (site_pk,site_name)=row

            print("Extracting cells parameters for site_pk: {0}, site_name: {1}".format(site_pk,site_name))

            sql = """
                INSERT INTO live_network.umts_cells_data
                (pk, date_added, date_modified, added_by, modified_by,bch_power,cell_id,cell_pk,lac,latitude, longitude, 
                maximum_transmission_power, "name", cpich_power, primary_sch_power, scrambling_code, rac, sac, 
                secondary_sch_power, site_pk, tech_pk, vendor_pk, uarfcn_dl,uarfcn_ul, ura_list, azimuth, cell_range, 
                height, site_sector_carrier, mcc,mnc,ura,localcellid, ci)
                SELECT 
                NEXTVAL('live_network.seq_umts_cells_data_pk'),
                 t1."DATETIME" AS date_added, 
                t1."DATETIME" AS date_modified, 
                0 AS added_by,
                0 AS modified_by,
                t5."BCHPOWER"::integer AS bch_power,
                t1."CELLID"::integer,
                t3.pk AS cell_pk, -- cellid
                t1."LAC"::integer AS lac,
                -- (t4."antennaPosition_latitude"::float/93206.76)*(-1::float*t4."antennaPosition_latitudeSign"::float) 
                null AS latitude,
                -- t4."antennaPosition_longitude"::float/46603.38 AS longitude,
                null AS longitude,
                t1."MAXTXPOWER"::integer AS maximum_transmission_power,
                t1."CELLNAME",
                t4."MAXPCPICHPOWER"::integer  AS cpich_power,
                t6."PSCHPOWER"::integer AS primary_sch_power,
                t1."PSCRAMBCODE"::integer AS scrambling_code,
                -- t1."LAC" AS lac,
                t1."RAC"::integer,
                t1."SAC"::integer,
                null AS secondary_sch_power,
                t3.site_pk, -- site pk
                2, -- umts
                2, -- Huawei
                t1."UARFCNDOWNLINK"::integer,
                t1."UARFCNUPLINK"::integer,
                null AS ura_list ,
                null AS azimuth, -- azimuth,
                null AS cell_range, -- cellrange,
                null AS height, -- height
                null AS site_sector_carrier,
                t7."MCC"::integer AS mcc,
                t7."MNC"::integer AS mnc,
                t8."URAID" AS ura ,
                t1."LOCELL"::integer AS localcellid,
                t1."CELLID"::integer AS ci
                FROM 
                huawei_cm."UCELL" t1
                INNER JOIN cm_loads t9 on t9.pk = t1."LOADID"
                INNER JOIN live_network.cells t3 on t3."name" = t1."CELLNAME" and t3.vendor_pk = 2 and t3.tech_pk = 2 
                INNER JOIN huawei_cm."UPCPICH" t4 on t4."FILENAME" = t1."FILENAME" AND  t4."CELLID" = t1."CELLID" AND t4."LOADID" = t1."LOADID" 
                INNER JOIN huawei_cm."UBCH" t5 on t5."FILENAME" = t1."FILENAME" AND t5."CELLID" = t1."CELLID" AND t5."LOADID" = t1."LOADID"
                INNER JOIN huawei_cm."UPSCH" t6 on t6."FILENAME" = t1."FILENAME" ANd t6."CELLID" = t1."CELLID" AND t6."LOADID" = t1."LOADID"
                INNER JOIN huawei_cm."UCNOPERATOR" t7 on t7."FILENAME" = t1."FILENAME" AND t7."LOADID" = t1."LOADID"
                INNER JOIN huawei_cm."UCELLURA" t8 on t8."FILENAME" = t1."FILENAME" AND t8."CELLID" = t1."CELLID" AND t8."LOADID" = t1."LOADID"
                WHERE t1."NODEBNAME" = '{0}'
                AND t9.is_current_load = true
            """.format(site_name)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_4g_cells(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.cells
            (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, site_pk)
            SELECT 
            NEXTVAL('live_network.seq_cells_pk'),
            t1."DATETIME" AS date_added, 
            t1."DATETIME" AS date_modified, 
            0 AS added_by,
            0 AS modified_by,
            3, -- tech 3 -lte, 2 -umts, 1-gms
            2, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
            t1."CELLNAME" AS name,
            t2.pk -- site primary key
            FROM huawei_cm."CELL" t1
            INNER JOIN cm_loads t4 on t4.pk = t1."LOADID"
            INNER JOIN huawei_cm."ENODEBFUNCTION" t5 on t5."FILENAME" = t1."FILENAME" AND t5."LOADID" = t1."LOADID"
            INNER JOIN live_network.sites t2 on t2."name" = t5."ENODEBFUNCTIONNAME" 
                AND t2.vendor_pk = 2 and t2.tech_pk = 3 
            LEFT JOIN live_network.cells t3 on t3."name" = t1."CELLNAME"
                AND t2.vendor_pk = 2 and t2.tech_pk = 3 
            WHERE
                t3."name" IS NULL
            AND t4.is_current_load = true
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_4g_cells_params(self):
        """Extract Ericsson LTE cell parameters"""
        """Extract Huawei LTE cell parameters"""

        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        # @TODO: Review how to reload this. Delete perhaps!!
        # Truncate paramete table
        # self.db_engine.execute(text("TRUNCATE TABLE live_network.lte_cells_data").execution_options(autocommit=True))
        # self.db_engine.execute(text("ALTER SEQUENCE live_network.seq_lte_cells_data_pk RESTART WITH 1;").
        #                        execution_options(autocommit=True))

        # The data is alot. Let's handle per site
        site_sql = """SELECT pk, "name" from live_network.sites where vendor_pk  = 2 and tech_pk = 3"""

        result = self.db_engine.execute(site_sql)

        for row in result:
            (site_pk, site_name) = row

            print("Extracting cells parameters for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                        INSERT INTO live_network.lte_cells_data
                        (pk, name, cell_pk, dl_earfcn, ul_earfcn, mcc, mnc, tac, pci, ecgi, rach_root_sequence, max_tx_power, latitude, longitude,
                        height, dl_bandwidth, ul_bandwidth, ta, ta_mode, tx_elements, rx_elements, scheduler, azimuth, mechanical_tilt, electrical_tilt, cell_range,
                        site_pk, tech_pk, vendor_pk, modified_by, added_by, date_added, date_modified)
                        SELECT 
                        NEXTVAL('live_network.seq_lte_cells_data_pk'),
                       t1."CELLNAME" AS name,
                        t2.pk AS cell_pk,
                        t1."DLEARFCN"::integer AS uarfcn_dl,
                        t3.dl_freq_low AS uarfcn_ul,
                        t6."MCC"::integer AS mcc,
                        t6."MNC"::integer AS mnc,
                        t4."TAC"::integer AS tac,
                        t1."PHYCELLID"::integer AS pci,
                        null AS ecgi,
                        t1."ROOTSEQUENCEIDX" AS rach_root_sequence,
                        null AS max_tx_power,
                        null AS latitude,
                        null AS longitude,
                        null AS height,
                        t1."DLBANDWIDTH"::integer AS dl_bandwidth,
                        null AS ul_bandwidth,
                        null AS ta,
                        null AS ta_mode,
                        t1."TXRXMODE"::integer AS tx_elements, -- @TODO: Conform
                        t1."TXRXMODE"::integer AS rx_elements, -- @TODO: Conform
                        t7."DLSCHSTRATEGY"::integer AS scheduler,
                        null AS azimuth,
                        null AS mechanical_tilt,
                        null AS electrical_tilt,
                        t1."CELLRADIUS"::integer AS cell_range,
                        t2.site_pk AS site_pk,
                        t2.tech_pk AS tech_pk,
                        t2.vendor_pk AS vendor_pk,
                        0 AS modified_by, 
                        0 AS added_by, 
                        t1."DATETIME" AS date_added, 
                        t1."DATETIME" AS date_modified
                        FROM huawei_cm."CELL" t1
                        INNER JOIN cm_loads t8 on t8.pk = t1."LOADID"
                        INNER JOIN live_network.cells t2 on t2."name" = t1."CELLNAME" AND t2.vendor_pk = 2 AND t2.tech_pk = 3
                        INNER JOIN public.lte_frequency_bands t3 on t3.band_id = t1."FREQBAND"::integer
                        INNER JOIN huawei_cm."CNOPERATORTA" t4 on t4."FILENAME" = t1."FILENAME" AND t4."LOADID" = t1."LOADID"
                        INNER JOIN huawei_cm."UCNOPERATOR" t6 on t6."FILENAME"  = t1."FILENAME" AND t6."LOADID" = t1."LOADID"
                        INNER JOIN huawei_cm."CELLDLSCHALGO" t7 on t7."FILENAME" = t1."FILENAME" AND t7."LOCALCELLID" = t1."LOCALCELLID" AND t7."LOADID" = t1."LOADID"
                        WHERE t1."ENODEBFUNCTIONNAME" = '{0}'
                        AND t8.is_current_load = true
                    """.format(site_name)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_2g_externals_on_2g(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.gsm_external_cells
        (pk, name, cell_pk, node_pk, mcc, mnc, lac, bcch, ncc, bcc, ci, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_gsm_external_cells_pk') AS pk,
        t1."EXT2GCELLNAME" AS "name",
        t3.pk AS cell_pk,
        t2.pk AS node_pk,
        t1."MCC"::integer AS mcc,
        t1."MNC"::integer AS mnc,
        t1."LAC"::integer AS lac,
        t1."BCCH"::integer AS bcch,
        t1."NCC"::integer AS ncc,
        t1."BCC"::integer AS bcc,
        t1."CI"::integer AS ci,
        0 AS modified_by,
        0 AS added_by,
        now()::timestamp AS date_added,
        now()::timestamp AS date_modified
        FROM
        huawei_cm."GEXT2GCELL" t1
        INNER JOIN cm_loads t6 on t6.pk = t1."LOADID"
        INNER JOIN huawei_cm."SYS" t5 on t5."FILENAME" = t1."FILENAME" AND t5."LOADID" = t1."LOADID"
        LEFT JOIN live_network.nodes t2 ON t2."name" = t5."SYSOBJECTID" 
        LEFT JOIN live_network.cells t3 on t3."name" = t1."EXT2GCELLNAME"
        LEFT JOIN live_network.gsm_external_cells t4 on t4."name" = t1."EXT2GCELLNAME" 
            AND t4.lac = t1."LAC"::integer
            AND t4.ci = t1."CI"::integer
        WHERE 
        t4.pk IS NULL
        AND  t6.is_current_load = true
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_2g_externals_on_3g(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.gsm_external_cells
        (pk, name, cell_pk, node_pk, mcc, mnc, lac, bcch, ncc, bcc, ci,rac, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_gsm_external_cells_pk') AS pk,
        t1."GSMCELLNAME" AS "name",
        t3.pk AS cell_pk,
        t2.pk AS node_pk,
        t1."MCC"::integer AS mcc,
        t1."MNC"::integer AS mnc,
        t1."LAC"::integer AS lac,
        t1."BCCHARFCN"::integer AS bcch,
        t1."NCC"::integer AS ncc,
        t1."BCC"::integer AS bcc,
        t1."CID"::integer AS ci,
        t1."RAC"::integer AS rac,
        0 AS modified_by,
        0 AS added_by,
        now()::timestamp AS date_added,
        now()::timestamp AS date_modified
        FROM
        huawei_cm."UEXT2GCELL" t1
        INNER JOIN cm_loads t6 on t6.pk = t1."LOADID"
        INNER JOIN huawei_cm."SYS" t5 on t5."FILENAME" = t1."FILENAME" AND t5."LOADID" = t1."LOADID"
        LEFT JOIN live_network.nodes t2 ON t2."name" = t5."SYSOBJECTID" 
        LEFT JOIN live_network.cells t3 on t3."name" = t1."GSMCELLNAME"
        LEFT JOIN live_network.gsm_external_cells t4 on t4."name" = t1."GSMCELLNAME" 
            AND t4.lac = t1."LAC"::integer
            AND t4.ci = t1."CID"::integer
        WHERE 
        t4.pk IS NULL
        AND  t6.is_current_load = true
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_2g_externals_on_4g(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.gsm_external_cells
            (pk, name, cell_pk, node_pk, mcc, mnc, lac, bcch,rac, modified_by, added_by, date_added, date_modified)
            SELECT 
            NEXTVAL('live_network.seq_gsm_external_cells_pk') AS pk,
            t1."CELLNAME" AS "name",
            t3.pk AS cell_pk,
            NULL  AS node_pk,
            t1."MCC"::integer AS mcc,
            t1."MNC"::integer AS mnc,
            t1."LAC"::integer AS lac,
            t1."GERANARFCN"::integer AS bcch,
            NULL AS rac,
            0 AS modified_by,
            0 AS added_by,
            now()::timestamp AS date_added,
            now()::timestamp AS date_modified
            FROM
            huawei_cm."GERANEXTERNALCELL" t1
            INNER JOIN cm_loads t6 on t6.pk = t1."LOADID"
            LEFT JOIN live_network.cells t3 on t3."name" = t1."CELLNAME"
            LEFT JOIN live_network.gsm_external_cells t4 on t4."name" = t1."CELLNAME" 
                AND t4.lac = t1."LAC"::integer
            WHERE 
            t4.pk IS NULL
            AND  t6.is_current_load = true
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g_externals_on_2g(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.umts_external_cells
        (pk, name, cell_pk,  mcc, mnc, lac, rncid, ci, psc, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_umts_external_cells_pk') AS pk,
        t1."EXT3GCELLNAME" AS "name",
        t3.pk AS cell_pk,
        t1."MCC"::integer AS mcc,
        t1."MNC"::integer AS mnc,
        t1."LAC"::integer AS lac,
        t1."RNCID"::integer AS rncid,
        t1."CI"::integer AS ci,
        t1."SCRAMBLE"::integer AS psc,
        0 AS modified_by,
        0 AS added_by,
        now()::timestamp AS date_added,
        now()::timestamp AS date_modified
        FROM
        huawei_cm."GEXT3GCELL" t1
        INNER JOIN cm_loads t6 on t6.pk = t1."LOADID"
        INNER JOIN huawei_cm."SYS" t5 on t5."FILENAME" = t1."FILENAME" AND t5."LOADID" = t1."LOADID"
        LEFT JOIN live_network.nodes t2 ON t2."name" = t5."SYSOBJECTID" 
        LEFT JOIN live_network.cells t3 on t3."name" = t1."EXT3GCELLNAME"
        LEFT JOIN live_network.umts_external_cells t4 on t4."name" = t1."EXT3GCELLNAME" 
            AND t4.lac = t1."LAC"::integer
            AND t4.ci = t1."CI"::integer
        WHERE 
        t4.pk IS NULL
        AND  t6.is_current_load = true
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g_externals_on_3g(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.umts_external_cells
        (pk, name, cell_pk, node_pk, lac, rac, ci, psc, uarfcn_dl, uarfcn_ul, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_umts_external_cells_pk') AS pk,
        t1."CELLNAME" AS "name",
        t3.pk AS cell_pk,
        t2.pk AS node_pk,
        t1."LAC"::integer AS lac,
        t1."RAC"::integer AS rac,
        t1."CELLID"::integer AS ci,
        t1."PSCRAMBCODE"::integer AS psc,
        t1."UARFCNDOWNLINK"::integer AS uarfcn_dl,
        t1."UARFCNUPLINK"::integer AS ul_uarfcn,
        0 AS modified_by,
        0 AS added_by,
        now()::timestamp AS date_added,
        now()::timestamp AS date_modified
        FROM
        huawei_cm."UEXT3GCELL" t1
        INNER JOIN cm_loads t6 on t6.pk = t1."LOADID"
        INNER JOIN huawei_cm."SYS" t5 on t5."FILENAME" = t1."FILENAME" AND t5."LOADID" = t1."LOADID"
        LEFT JOIN live_network.nodes t2 ON t2."name" = t5."SYSOBJECTID" 
        LEFT JOIN live_network.cells t3 on t3."name" = t1."CELLNAME"
        LEFT JOIN live_network.umts_external_cells t4 on t4."name" = t1."CELLNAME" 
            AND t4.lac = t1."LAC"::integer
        WHERE 
        t4.pk IS NULL
        AND  t6.is_current_load = true
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g_externals_on_4g(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.umts_external_cells
        (pk, name, cell_pk,  mcc, mnc, lac, rac, rncid, ci, psc, uarfcn_dl, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_umts_external_cells_pk') AS pk,
        t1."CELLNAME" AS "name",
        t3.pk AS cell_pk,
        t1."MCC"::integer AS mcc,
        t1."MNC"::integer AS mnc,
        t1."LAC"::integer AS lac,
        t1."RAC"::integer AS rac,
        t1."RNCID"::integer AS rncid,
        t1."CELLID"::integer AS ci,
        t1."PSCRAMBCODE"::integer AS psc,
        t1."UTRANDLARFCN"::integer AS uarfcn_dl,
        0 AS modified_by,
        0 AS added_by,
        now()::timestamp AS date_added,
        now()::timestamp AS date_modified
        FROM
        huawei_cm."UTRANEXTERNALCELL" t1
        INNER JOIN cm_loads t6 on t6.pk = t1."LOADID"
        LEFT JOIN live_network.cells t3 on t3."name" = t1."CELLNAME"
        LEFT JOIN live_network.umts_external_cells t4 on t4."name" = t1."CELLNAME" 
            AND t4.lac = t1."LAC"::integer
        WHERE 
        t4.pk IS NULL
        AND  t6.is_current_load = true
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_4g_externals_on_2g(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.lte_external_cells
        (pk, name, cell_pk, node_pk, mcc, mnc, pci, dl_earfcn, ci, tac, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_lte_external_cells_pk') AS pk,
        t1."EXTLTECELLNAME" AS "name",
        t3.pk AS cell_pk,
        t2.pk AS node_pk,
        t1."MCC"::integer AS mcc,
        t1."MNC"::integer AS mnc,
        t1."PCID"::integer AS pci,
        t1."FREQ"::integer AS dl_earfcn,
        t1."CI"::integer AS ci,
        t1."TAC"::integer AS tac,
        0 AS modified_by,
        0 AS added_by,
        now()::timestamp AS date_added,
        now()::timestamp AS date_modified
        FROM
        huawei_cm."GEXTLTECELL" t1
        INNER JOIN cm_loads t6 on t6.pk = t1."LOADID"
        LEFT JOIN live_network.cells t3 on t3."name" = t1."EXTLTECELLNAME"
        INNER JOIN live_network.sites t2 on t2.pk  = t3.site_pk
        LEFT JOIN live_network.lte_external_cells t4 on t4."name" = t1."EXTLTECELLNAME" 
            AND t4.ci = t1."CI"::integer
        WHERE 
        t4.pk IS NULL
        AND  t6.is_current_load = true
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_4g_externals_on_3g(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.lte_external_cells
            (pk, name, cell_pk, node_pk, mcc, mnc, pci, dl_earfcn, ci, tac, modified_by, added_by, date_added, date_modified)
            SELECT 
            NEXTVAL('live_network.seq_lte_external_cells_pk') AS pk,
            t1."LTECELLNAME" AS "name",
            t3.pk AS cell_pk,
            t5.pk AS node_pk,
            t1."MCC"::integer AS mcc,
            t1."MNC"::integer AS mnc,
            t1."CELLPHYID"::integer AS pci,
            t1."LTEARFCN"::integer AS dl_earfcn,
            t1."LTECELLINDEX"::integer AS ci,
            t1."TAC"::integer AS tac,
            0 AS modified_by,
            0 AS added_by,
            now()::timestamp AS date_added,
            now()::timestamp AS date_modified
            FROM
            huawei_cm."ULTECELL" t1
            INNER JOIN cm_loads t6 on t6.pk = t1."LOADID"
            INNER JOIN huawei_cm."SYS" t7 on t7."FILENAME" = t1."FILENAME" AND t7."LOADID" = t1."LOADID"
            LEFT JOIN live_network.cells t3 on t3."name" = t1."LTECELLNAME"
            LEFT JOIN live_network.sites t2 ON t2.pk = t3.site_pk
            LEFT JOIN live_network.lte_external_cells t4 on t4."name" = t1."LTECELLNAME" 
            LEFT JOIN live_network.nodes t5 ON t5."name" = t7."SYSOBJECTID"
            WHERE 
            t4.pk IS NULL
            AND  t6.is_current_load = true
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_4g_externals_on_4g(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.lte_external_cells
            (pk, name, cell_pk, node_pk, mcc, mnc, pci, dl_earfcn, ci, tac, modified_by, added_by, date_added, date_modified)
            SELECT 
            NEXTVAL('live_network.seq_lte_external_cells_pk') AS pk,
            t1."CELLNAME" AS "name",
            t3.pk AS cell_pk,
            t2.pk AS node_pk,
            t1."MCC"::integer AS mcc,
            t1."MNC"::integer AS mnc,
            t1."PHYCELLID"::integer AS pci,
            t1."DLEARFCN"::integer AS dl_earfcn,
            t1."CELLID"::integer AS ci,
            t1."TAC"::integer AS tac,
            0 AS modified_by,
            0 AS added_by,
            now()::timestamp AS date_added,
            now()::timestamp AS date_modified
            FROM
            huawei_cm."EUTRANEXTERNALCELL" t1
            INNER JOIN cm_loads t6 on t6.pk = t1."LOADID"
            LEFT JOIN live_network.cells t3 on t3."name" = t1."CELLNAME"
            INNER JOIN live_network.sites t2 ON t2.pk = t3.site_pk
            LEFT JOIN live_network.lte_external_cells t4 on t4."name" = t1."CELLNAME" 
            WHERE 
            t4.pk IS NULL
            AND  t6.is_current_load = true
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_externals_on_2g(self):
        """Extract live network externals defined on Huawei 2G"""
        self.extract_live_network_2g_externals_on_2g()
        self.extract_live_network_3g_externals_on_2g()
        self.extract_live_network_4g_externals_on_2g()

    def extract_live_network_externals_on_3g(self):
        """Extract live network externals defined on Huawei 3G"""
        self.extract_live_network_2g_externals_on_3g()
        self.extract_live_network_3g_externals_on_3g()
        self.extract_live_network_4g_externals_on_3g()

    def extract_live_network_externals_on_4g(self):
        """Extract live network externals defined on Huawei 4G"""
        self.extract_live_network_2g_externals_on_4g()
        self.extract_live_network_3g_externals_on_4g()
        self.extract_live_network_4g_externals_on_4g()
