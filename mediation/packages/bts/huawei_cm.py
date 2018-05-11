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

    def extract_bscs_from_gexport_data(self):
        """Extract Huawei BSCs from GExport data"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        # BSC6900
        sql = """
            WITH gsm_sys as 
            (
                SELECT DISTINCT * from huawei_gexport_gsm."SYS_BSC6900GSM"
                UNION
                SELECT DISTINCT * from huawei_gexport_gsm."SYS_BSC6910GSM"
            )
            INSERT INTO live_network.nodes
            (pk,date_added, date_modified, type,"name", vendor_pk, tech_pk, added_by, modified_by)
            SELECT 
            NEXTVAL('live_network.seq_nodes_pk'),
            "DATETIME" as date_added, 
            "DATETIME" as date_modified, 
            'BSC' as node_type,
            t1."SYSOBJECTID" as "name" , 
            2 as vendor_pk, -- 1=Ericsson, 2=Huawei
            1 as tech_pk , -- 1=gsm, 2-umts,3=lte
            0 as added_by,
            0 as modified_by
            FROM gsm_sys t1
            LEFT OUTER  JOIN live_network.nodes t2 ON t1."SYSOBJECTID" = t2."name"
            WHERE 
            t2."name" IS NULL
            AND 
            t1."TECHNOLOGY" = 'GSM'
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_bscs_from_nbi_data(self):
        """Extract Huawei BSCs from North Bound Interface dumps"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        # BSC6900
        sql = """
            INSERT INTO live_network.nodes
            (pk,date_added, date_modified, type,"name", vendor_pk, tech_pk, added_by, modified_by)
            SELECT 
            NEXTVAL('live_network.seq_nodes_pk'),
            "DATETIME" as date_added, 
            "DATETIME" as date_modified, 
            'BSC' as node_type,
            t1."neid" as "name" , 
            2 as vendor_pk, -- 1=Ericsson, 2=Huawei
            1 as tech_pk , -- 1=gsm, 2-umts,3=lte
            0 as added_by,
            0 as modified_by
            FROM huawei_nbi_gsm."BSCBASIC" t1
            LEFT OUTER  JOIN live_network.nodes t2 ON t1."neid" = t2."name"
            WHERE 
            t2."name" IS NULL
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_bscs_from_mml_data(self):
        """Extract Huawei BSCs from MML dumps"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        # BSC6900
        sql = """
            INSERT INTO live_network.nodes
            (pk,date_added, date_modified, type,"name", vendor_pk, tech_pk, added_by, modified_by)
            SELECT 
            NEXTVAL('live_network.seq_nodes_pk'),
            "varDateTime" as date_added, 
            "varDateTime" as date_modified, 
            'BSC' as node_type,
            t1."SYSOBJECTID" as "name" , 
            2 as vendor_pk, -- 1=Ericsson, 2=Huawei
            1 as tech_pk , -- 1=gsm, 2-umts,3=lte
            0 as added_by,
            0 as modified_by
            FROM huawei_mml_gsm."SYS" t1
            LEFT OUTER  JOIN live_network.nodes t2 ON t1."SYSOBJECTID" = t2."name"
            WHERE 
            t2."name" IS NULL
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))
        session.close()

    def extract_bscs_from_cfgsyn_data(self):
        """Extract Huawei BSCs from CFG SYN data"""
        pass

    def extract_bscs(self):
        """Extract Huawei BSCs"""
        self.extract_bscs_from_nbi_data()
        self.extract_bscs_from_gexport_data()
        self.extract_bscs_from_mml_data()
        self.extract_bscs_from_cfgsyn_data()

    def extract_rncs_from_gexport(self):
        """Extract Huawei RNCs from GExport dumps"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        # BSC6900
        sql = """
             WITH umts_sys as 
             (
                 SELECT DISTINCT * from huawei_gexport_wcdma."SYS_BSC6900UMTS"
                 UNION
                 SELECT DISTINCT * from huawei_gexport_wcdma."SYS_BSC6910UMTS"
             )
                INSERT INTO live_network.nodes
                (pk,date_added, date_modified, type,"name", vendor_pk, tech_pk, added_by, modified_by)
             SELECT 
              NEXTVAL('live_network.seq_nodes_pk'),
             "DATETIME" as date_added, 
             "DATETIME" as date_modified, 
             'BSC' as node_type,
             t1."SYSOBJECTID" as "name" , 
             2 as vendor_pk, -- 1=Ericsson, 2=Huawei
             1 as tech_pk , -- 1=gsm, 2-umts,3=lte
             0 as added_by,
             0 as modified_by
             FROM umts_sys t1
             LEFT OUTER  JOIN live_network.nodes t2 ON t1."SYSOBJECTID" = t2."name"
             WHERE 
             t2."name" IS NULL
             AND 
             t1."TECHNOLOGY" = 'WCDMA'
          """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_rncs_from_nbi_data(self):
        """Extract Huawei RNCs from North Bound Interface dumps"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.nodes
            (pk,date_added, date_modified, type,"name", vendor_pk, tech_pk, added_by, modified_by)
            SELECT 
            NEXTVAL('live_network.seq_nodes_pk'),
            "varDateTime" as date_added, 
            "varDateTime" as date_modified, 
            'RNC' as node_type,
            "neid" as "name" , 
            2 as vendor_pk, -- 1=Ericsson, 2=Huawei, 3-ZTE
            2 as tech_pk , -- 1=gsm, 2-umts,3=lte
            0 as added_by,
            0 as modified_by
            FROM huawei_nbi_umts.urncbasic t1
            LEFT OUTER  JOIN live_network.nodes t2 ON t1."neid" = t2."name"
            WHERE 
            t2."name" IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_rncs_from_mml_data(self):
        """Extract RNCs from MML dumps"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        # BSC6900
        sql = """
            INSERT INTO live_network.nodes
            (pk,date_added, date_modified, type,"name", vendor_pk, tech_pk, added_by, modified_by)
            SELECT 
            NEXTVAL('live_network.seq_nodes_pk'),
            "varDateTime" as date_added, 
            "varDateTime" as date_modified, 
            'BSC' as node_type,
            t1."SYSOBJECTID" as "name" , 
            2 as vendor_pk, -- 1=Ericsson, 2=Huawei
            1 as tech_pk , -- 1=gsm, 2-umts,3=lte
            0 as added_by,
            0 as modified_by
            FROM huawei_mml_umts."SYS" t1
            LEFT OUTER  JOIN live_network.nodes t2 ON t1."SYSOBJECTID" = t2."name"
            WHERE 
            t2."name" IS NULL
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))
        session.close()

    def extract_rncs_from_cfgsyn_data(self):
        """Extract Huawei RNCs from Baseline dumps"""
        pass

    def extract_rncs(self):
        """Extract Huawei RNCs"""
        self.extract_rncs_from_gexport_data()
        self.extract_rncs_from_nbi_data()
        self.extract_rncs_from_mml_data()
        self.extract_rncs_from_cfgsyn_data()

    def extract_enodebs_from_gexport_data(self):
        """Extract Huawei ENodeBs from GExport dumps"""
        pass

    def extract_enodebs_from_nbi_data(self):
        """Extract Huawei ENodeBs from itf-N dumps"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
             INSERT INTO live_network.sites
             (pk, date_added,date_modified, tech_pk, vendor_pk, "name", added_by, modified_by)
             SELECT 
             NEXTVAL('live_network.seq_sites_pk'),
             "varDateTime" as date_added, 
             "varDateTime" as date_modified, 
             3 as tech_pk , -- 1=gsm, 2-umts,3=lte,
             2 as vendor_pk, -- 1=Ericsson, 2=Huawei
             t1."ENODEBFUNCTIONNAME",
             0 as added_by,
             0 as modified_by
             FROM huawei_nbi_lte.enodebfunction t1
             LEFT OUTER  JOIN live_network.sites t2 ON t1."ENODEBFUNCTIONNAME" = t2."name"
             WHERE 
             t2."name" IS NULL
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_enodebs_from_mml_data(self):
        """Extract Huawei ENodeBs from MML dumps"""
        pass

    def extract_enodebs_from_cfgsyn_data(self):
        """Extract Huawei ENodeBs from baseline syn"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
             INSERT INTO live_network.sites
             (pk, date_added,date_modified, tech_pk, vendor_pk, "name", added_by, modified_by)
             SELECT 
             NEXTVAL('live_network.seq_sites_pk'),
             "DATETIME" as date_added, 
             "DATETIME" as date_modified, 
             3 as tech_pk , -- 1=gsm, 2-umts,3=lte,
             2 as vendor_pk, -- 1=Ericsson, 2=Huawei
             t1."eNodeBFunctionName",
             0 as added_by,
             0 as modified_by
             FROM huawei_cfgsyn."eNodeBFunction" t1
             LEFT OUTER  JOIN live_network.sites t2 ON t1."eNodeBFunctionName" = t2."name" 
             AND t2.tech_pK = 3 
             AND t2.vendor_pk = 2
             WHERE 
             t2."name" IS NULL
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_2g_sites_from_gexport_data(self):
        """Extract Huawei 2G sites from GExport dumps"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            WITH gsm_bts as 
            (
                SELECT DISTINCT * from huawei_gexport_gsm."BTS_BSC6900GSM"
                UNION
                SELECT DISTINCT * from huawei_gexport_gsm."BTS_BSC6910GSM"
            ), gsm_sys as  (
                SELECT DISTINCT * from huawei_gexport_gsm."SYS_BSC6900GSM"
                UNION
                SELECT DISTINCT * from huawei_gexport_gsm."SYS_BSC6910GSM"
            )
            INSERT INTO live_network.sites
            (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, node_pk)
            SELECT 
            NEXTVAL('live_network.seq_sites_pk'),
            t1."DATETIME" as date_added, 
            t1."DATETIME" as date_modified, 
            0 as added_by,
            0 as modified_by,
            1 as tech_pk, -- tech 3 -lte, 2 -umts, 1-gms
            2 as vendor_pk, -- 1- Ericsson, 2 - Huawei, 3 - zte, 4-nokika, etc...
            t1."BTSNAME",
            t2.pk -- node primary key
            from gsm_bts t1
            INNER JOIN gsm_sys tt2 on tt2."FILENAME" = t1."FILENAME"
            INNER join live_network.nodes t2 on t2."name" = tt2."SYSOBJECTID" 
                AND t2.vendor_pk = 2 and t2.tech_pk = 1
            LEFT JOIN live_network.sites t3 on t3."name" = t1."BTSNAME" 
               AND t2.vendor_pk = 2 and t2.tech_pk = 1
            WHERE 
            t3."name" IS NULL

        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))
        session.close()

    def extract_2g_sites_from_nbi_data(self):
        """Extract Huawei 2G sites from itf-N dumps"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.sites
            (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, node_pk)
            SELECT 
            NEXTVAL('live_network.seq_sites_pk'),
            t1."varDateTime" as date_added, 
            t1."varDateTime" as date_modified, 
            0 as added_by,
            0 as modified_by,
            1 as tech_pk, -- tech 3 -lte, 2 -umts, 1-gms
            2 as vendor_pk, -- 1- Ericsson, 2 - Huawei, 3 - zte, 4-nokika, etc...
            t1."BTSNAME",
            t2.pk -- node primary key
            from huawei_nbi_gsm.bts t1
            INNER join live_network.nodes t2 on t2."name" = t1."neid" 
                AND t2.vendor_pk = 2 and t2.tech_pk = 1
            LEFT JOIN live_network.sites t3 on t3."name" = t1."BTSNAME" 
               AND t2.vendor_pk = 2 and t2.tech_pk = 1
            WHERE 
            t3."name" IS NULL
            AND trim(t1.module_type) = 'Radio'
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))
        session.close()

    def extract_2g_sites_from_mml_data(self):
        """Extract Huawei 2G sites from MML dumps"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.sites
            (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, node_pk)
            SELECT 
            NEXTVAL('live_network.seq_sites_pk'),
            t1."varDateTime" as date_added, 
            t1."varDateTime" as date_modified, 
            0 as added_by,
            0 as modified_by,
            1 as tech_pk, -- tech 3 -lte, 2 -umts, 1-gms
            2 as vendor_pk, -- 1- Ericsson, 2 - Huawei, 3 - zte, 4-nokika, etc...
            t1."BTSNAME",
            t2.pk -- node primary key
            from huawei_mml_gsm."BTS" t1
            INNER JOIN huawei_mml_gsm."SYS" tt2 on tt2."FileName" = t1."FileName"
            INNER join live_network.nodes t2 on t2."name" = tt2."SYSOBJECTID" 
                AND t2.vendor_pk = 2 and t2.tech_pk = 1
            LEFT JOIN live_network.sites t3 on t3."name" = t1."BTSNAME" 
               AND t3.vendor_pk = 2 and t3.tech_pk = 1
            WHERE 
            t3."name" IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))
        session.close()

    def extract_2g_sites_from_cfgsyn_data(self):
        """Extract Huawei 2G sites from baseline syn"""
        pass

    def extract_2g_sites(self):
        """Extract 2G sites"""
        self.extract_2g_sites_from_gexport_data()
        self.extract_2g_sites_from_nbi_data()
        self.extract_2g_sites_from_mml_data()
        self.extract_2g_sites_from_cfgsyn_data()

    def extract_2g_cells_from_gexport_data(self):
        """Extract Huawei 2G cells from GExport dumps"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            WITH sys_gsm as (
                SELECT * FROM huawei_gexport_gsm."SYS_BSC6900GSM"
                UNION 
                SELECT * FROM huawei_gexport_gsm."SYS_BSC6910GSM"
            ), gcell_gsm as (
                SELECT * FROM huawei_gexport_gsm."GCELL_BSC6900GSM"
                UNION 
                SELECT * FROM huawei_gexport_gsm."GCELL_BSC6910GSM"
            ), cellbind2bts_gsm as (
                SELECT * FROM huawei_gexport_gsm."CELLBIND2BTS_BSC6900GSM"
                UNION 
                SELECT * FROM huawei_gexport_gsm."CELLBIND2BTS_BSC6910GSM"
            ), bts_gsm as (
                SELECT * FROM huawei_gexport_gsm."BTS_BSC6900GSM"
                UNION 
                SELECT * FROM huawei_gexport_gsm."BTS_BSC6910GSM"
            )
            INSERT INTO live_network.cells
            (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, site_pk)
            SELECT 
            nextval('live_network.seq_cells_pk'),
            t1."DATETIME" as date_added, 
            t1."DATETIME" as date_modified, 
            0 as added_by,
            0 as modified_by,
            1, -- tech 3 -lte, 2 -umts, 1-gms
            2, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
            t1."CELLNAME" AS name,
            t4.pk -- site primary key
            FROM gcell_gsm t1
            INNER JOIN sys_gsm tt2 on tt2."FILENAME" = t1."FILENAME"
            INNER JOIN live_network.nodes t3 on t3."name" = tt2."SYSOBJECTID"
                    AND t3.vendor_pk = 2
                    AND t3.tech_pk = 1
            INNER JOIN cellbind2bts_gsm t6 on t6."FILENAME" = tt2."FILENAME" AND tt2."SYSOBJECTID" = t3.name AND t6."CELLID" = t1."CELLID"
            INNER JOIN bts_gsm t7 on t7."FILENAME" = tt2."FILENAME" AND tt2."SYSOBJECTID" = t3.name AND t7."BTSID" = t6."BTSID"
            INNER JOIN live_network.sites t4 on t4."name" = t7."BTSNAME"
                AND t4.vendor_pk = 2 
                AND t4.tech_pk = 1
                AND t4.node_pk = t3.pk
            LEFT JOIN live_network.cells t5 on t5."name" = trim(t1."CELLNAME")
                AND t5.tech_pk = 1
                AND t5.vendor_pk = 2
            WHERE
            t5."name" IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_2g_cells_from_nbi_data(self):
        """Extract Huawei 2G cells from itf-N dumps"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.cells
            (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, site_pk)
            SELECT 
            nextval('live_network.seq_cells_pk'),
            t1."varDateTime" as date_added, 
            t1."varDateTime" as date_modified, 
            0 as added_by,
            0 as modified_by,
            1, -- tech 3 -lte, 2 -umts, 1-gms
            2, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
            t1."CELLNAME" AS name,
            t4.pk -- site primary key
            FROM huawei_nbi_gsm."GCELL" t1
            INNER JOIN live_network.nodes t3 on t3."name" = t1."neid" 
                    AND t3.vendor_pk = 2
                    AND t3.tech_pk = 1
            INNER JOIN huawei_nbi_gsm."CELLBIND2BTS" t6 on t6."neid" = t3.name AND t6."CELLID" = t1."CELLID"
            INNER JOIN huawei_nbi_gsm."BTS" t7 on t7."neid" = t3.name AND t7."BTSID" = t6."BTSID"
            INNER JOIN live_network.sites t4 on t4."name" = t7."BTSNAME"
                AND t4.vendor_pk = 2 
                AND t4.tech_pk = 1
                AND t4.node_pk = t3.pk
            LEFT JOIN live_network.cells t5 on t5."name" = trim(t1."CELLNAME")
                AND t5.tech_pk = 1
                AND t5.vendor_pk = 2
            WHERE
            t5."name" IS NULL
            AND trim(t1.module_type) = 'Radio'
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_2g_cells_from_mml_data(self):
        """Extract Huawei 2G cells from MML dumps"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.cells
            (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, site_pk)
            SELECT 
            nextval('live_network.seq_cells_pk'),
            t1."varDateTime" as date_added, 
            t1."varDateTime" as date_modified, 
            0 as added_by,
            0 as modified_by,
            1, -- tech 3 -lte, 2 -umts, 1-gms
            2, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
            t1."CELLNAME" AS name,
            t4.pk -- site primary key
            FROM huawei_mml_gsm."GCELL" t1
            INNER JOIN huawei_mml_gsm."SYS" tt2 on tt2."FileName" = t1."FileName"
            INNER JOIN live_network.nodes t3 on t3."name" = tt2."SYSOBJECTID"
                    AND t3.vendor_pk = 2
                    AND t3.tech_pk = 1
            INNER JOIN huawei_mml_gsm."CELLBIND2BTS" t6 on t6."FileName" = tt2."FileName" AND tt2."SYSOBJECTID" = t3.name AND t6."CELLID" = t1."CELLID"
            INNER JOIN huawei_mml_gsm."BTS" t7 on t7."FileName" = tt2."FileName" AND tt2."SYSOBJECTID" = t3.name AND t7."BTSID" = t6."BTSID"
            INNER JOIN live_network.sites t4 on t4."name" = t7."BTSNAME"
                AND t4.vendor_pk = 2 
                AND t4.tech_pk = 1
                AND t4.node_pk = t3.pk
            LEFT JOIN live_network.cells t5 on t5."name" = trim(t1."CELLNAME")
                AND t5.tech_pk = 1
                AND t5.vendor_pk = 2
            WHERE
            t5."name" IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()
    def extract_2g_cells_from_cfgsyn_data(self):
        """Extract Huawei 2G cells from baseline syn dumps"""
        pass

    def extract_2g_cells(self):
        """Extract Huawei 2G cells"""
        self.extract_2g_cells_from_gexport_data()
        self.extract_2g_cells_from_nbi_data()
        self.extract_2g_cells_from_mml_data()
        self.extract_2g_cells_from_cfgsyn_data()

    def extract_2g_cell_params_from_gexport_data(self):
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
                         NEXTVAL('live_network.seq_gsm_cells_data_pk'),
                         t1."CELLNAME" as name,
                         t2.pk as cell_pk,
                         t1."CI"::integer as ci,
                         t1."BCC"::integer as bcc,
                         t1."NCC"::integer as ncc,
                         CONCAT(trim(t1."NCC"),trim(t1."BCC"))::integer as bsic,
                         t4."FREQ"::integer as bcch,
                         t1."LAC"::integer as lac,
                         t6."LATIINT"::float as latitude,
                         t6."LONGIINT"::float,
                         CONCAT( TRIM(t1."MCC"),'-', TRIM(t1."MNC"),'-',TRIM(t1."LAC"),'-',TRIM(t1."CI")) as cgi,
                         t6."ANTAANGLE"::integer as azimuth,
                         t6."ALTITUDE"::integer as height,
                         null as mechanical_tilt,
                         -- t1."SECTOR_ANGLE"::integer as sector_angle,
                         -- t6."MAXTA" as ta
                         -- t1."STATE" as STATE -- ACTIVE or INACTIVE
                         null as electrical_tilt,
                         null as hsn,
                         null as hopping_type,
                         null as tch_carriers,
                         t1."MCC",
                         t1."MNC",
                         0 as modified_by,
                         0 as added_by,
                         t1."varDateTime" as date_added,
                         t1."varDateTime" as date_modified
                         FROM hua_cm_2g.gcell t1
                         INNER JOIN live_network.cells t2 on t2."name" = t1."CELLNAME" AND t2.vendor_pk = 2 AND t2.tech_pk = 1
                         INNER JOIN hua_cm_2g.gcellbasicpara t3 on t3."CELLID" = t1."CELLID" AND t3.neid = t1.neid 
                         INNER JOIN hua_cm_2g.gtrx t4 on t4."neid" = t1.neid AND t4."CELLID" = t1."CELLID"
                         INNER JOIN live_network.sites t5 on t5.pk = t2.site_pk
                         INNER JOIN hua_cm_2g.gcelllcs t6 on t6.neid = t1.neid AND t6."CELLID" = t1."CELLID"
                         INNER JOIN hua_cm_2g.cellbind2bts t7 on t7."CELLID" = t1."CELLID" AND t6.neid = t1.neid
                         WHERE 
                         t5."name" ='{0}'
                         AND trim(t1.module_type) = 'Radio'
                         ;
                     """.format(site_name)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_2g_cell_params_from_nbi_data(self):
        """Extract Huawei 2G cell parameters from itf-N dumps"""
        pass

    def extract_2g_cell_params_from_mml_data(self):
        """Extract Huawei 2G cell parameters from MML dumps"""
        pass

    def extract_2g_cell_params_from_cfgsyn_data(self):
        """Extract Huawei 2G cell parameters from baseline syn dumps"""
        pass

    def extract_2g_cell_params(self):
        """Extract 2G cell params"""
        self.extract_2g_cell_params_from_gexport_data()
        self.extract_2g_cell_params_from_nbi_data()
        self.extract_2g_cell_params_from_mml_data()
        self.extract_2g_cell_params_from_cfgsyn_data()

    def extract_2g2g_nbrs_from_gexport_data(self):
        pass

    def extract_2g2g_nbrs_from_nbi_data(self):
        pass

    def extract_2g2g_nbrs_from_mml_data(self):
        pass

    def extract_2g2g_nbrs_from_cfgsyn_data(self):
        pass

    def extract_2g2g_nbrs(self):
        self.extract_2g2g_nbrs_from_gexport_data()
        self.extract_2g2g_nbrs_from_nbi_data()
        self.extract_2g2g_nbrs_from_mml_data()
        self.extract_2g2g_nbrs_from_cfgsyn_data()

    def extract_2g3g_nbrs_from_gexport_data(self):
        pass

    def extract_2g3g_nbrs_from_nbi_data(self):
        pass

    def extract_2g3g_nbrs_from_mml_data(self):
        pass

    def extract_2g3g_nbrs_from_cfgsyn_data(self):
        pass

    def extract_2g3g_nbrs(self):
        self.extract_2g3g_nbrs_from_gexport_data()
        self.extract_2g3g_nbrs_from_nbi_data()
        self.extract_2g3g_nbrs_from_mml_data()
        self.extract_2g3g_nbrs_from_cfgsyn_data()

    def extract_2g4g_nbrs_from_gexport_data(self):
        pass

    def extract_2g4g_nbrs_from_nbi_data(self):
        pass

    def extract_2g4g_nbrs_from_mml_data(self):
        pass

    def extract_2g4g_nbrs_from_cfgsyn_data(self):
        pass

    def extract_2g4g_nbrs(self):
        self.extract_2g4g_nbrs_from_gexport_data()
        self.extract_2g4g_nbrs_from_nbi_data()
        self.extract_2g4g_nbrs_from_mml_data()
        self.extract_2g4g_nbrs_from_cfgsyn_data()

    def extract_3g2g_nbrs_from_gexport_data(self):
        pass

    def extract_3g2g_nbrs_from_nbi_data(self):
        pass

    def extract_3g2g_nbrs_from_mml_data(self):
        pass

    def extract_3g2g_nbrs_from_cfgsyn_data(self):
        pass

    def extract_3g2g_nbrs(self):
        self.extract_3g2g_nbrs_from_gexport_data()
        self.extract_3g2g_nbrs_from_nbi_data()
        self.extract_3g2g_nbrs_from_mml_data()
        self.extract_3g2g_nbrs_from_cfgsyn_data()

    def extract_3g3g_nbrs_from_gexport_data(self):
        pass

    def extract_3g3g_nbrs_from_nbi_data(self):
        pass

    def extract_3g3g_nbrs_from_mml_data(self):
        pass

    def extract_3g3g_nbrs_from_cfgsyn_data(self):
        pass

    def extract_3g3g_nbrs(self):
        self.extract_3g3g_nbrs_from_gexport_data()
        self.extract_3g3g_nbrs_from_nbi_data()
        self.extract_3g3g_nbrs_from_mml_data()
        self.extract_3g3g_nbrs_from_cfgsyn_data()

    def extract_3g4g_nbrs_from_gexport_data(self):
        """Extract Huawei 3G4G relations from GExport dumps"""
        pass


    def extract_3g4g_nbrs_from_nbi_data(self):
        """Extract Huawei 3G4G relations from itf-N dumps"""
        pass

    def extract_3g4g_nbrs_from_mml_data(self):
        pass

    def extract_3g4g_nbrs_from_cfgsyn_data(self):
        pass

    def extract_3g4g_nbrs(self):
        """Extract Huawei 3G4G relations from itf-N dumps"""
        self.extract_3g4g_nbrs_from_gexport_data()
        self.extract_3g4g_nbrs_from_nbi_data()
        self.extract_3g4g_nbrs_from_mml_data()
        self.extract_3g4g_nbrs_from_cfgsyn_data()

    def extract_4g2g_nbrs_from_gexport_data(self):
        pass

    def extract_4g2g_nbrs_from_nbi_data(self):
        pass

    def extract_4g2g_nbrs_from_mml_data(self):
        pass

    def extract_4g2g_nbrs_from_cfgsyn_data(self):
        pass

    def extract_4g2g(self):
        self.extract_4g2g_nbrs_from_gexport_data()
        self.extract_4g2g_nbrs_from_nbi_data()
        self.extract_4g2g_nbrs_from_mml_data()
        self.extract_4g2g_nbrs_from_cfgsyn_data()

    def extract_4g3g_nbrs_from_gexport_data(self):
        pass

    def extract_4g3g_nbrs_from_nbi_data(self):
        pass

    def extract_4g3g_nbrs_from_mml_data(self):
        pass

    def extract_4g3g_nbrs_from_cfgsyn_data(self):
        pass

    def extract_4g3g(self):
        self.extract_4g3g_nbrs_from_gexport_data()
        self.extract_4g3g_nbrs_from_nbi_data()
        self.extract_4g3g_nbrs_from_mml_data()
        self.extract_4g3g_nbrs_from_cfgsyn_data()

    def extract_4g4g_nbrs_from_gexport_data(self):
        pass

    def extract_4g4g_nbrs_from_nbi_data(self):
        pass

    def extract_4g4g_nbrs_from_mml_data(self):
        pass

    def extract_4g4g_nbrs_from_cfgsyn_data(self):
        pass

    def extract_4g4g_nbrs(self):
        self.extract_4g4g_nbrs_from_gexport_data()
        self.extract_4g4g_nbrs_from_nbi_data()
        self.extract_4g4g_nbrs_from_mml_data()
        self.extract_4g4g_nbrs_from_cfgsyn_data()


    def extract_2g2g_nbrs_params_from_gexport_data(self):
        pass

    def extract_2g2g_nbrs_params_from_nbi_data(self):
        pass

    def extract_2g2g_nbrs_params_from_mml_data(self):
        pass

    def extract_2g2g_nbrs_params_from_cfgsyn_data(self):
        pass

    def extract_2g2g_nbrs_params(self):
        self.extract_2g2g_nbrs_params_from_gexport_data()
        self.extract_2g2g_nbrs_params_from_nbi_data()
        self.extract_2g2g_nbrs_params_from_mml_data()
        self.extract_2g2g_nbrs_params_from_cfgsyn_data()

    def extract_2g3g_nbrs_params_from_gexport_data(self):
        pass

    def extract_2g3g_nbrs_params_from_nbi_data(self):
        pass

    def extract_2g3g_nbrs_params_from_mml_data(self):
        pass

    def extract_2g3g_nbrs_params_from_cfgsyn_data(self):
        pass

    def extract_2g3g_nbrs_params(self):
        self.extract_2g3g_nbrs_params_from_gexport_data()
        self.extract_2g3g_nbrs_params_from_nbi_data()
        self.extract_2g3g_nbrs_params_from_mml_data()
        self.extract_2g3g_nbrs_params_from_cfgsyn_data()

    def extract_2g4g_nbrs_params_from_gexport_data(self):
        pass

    def extract_2g4g_nbrs_params_from_nbi_data(self):
        pass

    def extract_2g4g_nbrs_params_from_mml_data(self):
        pass

    def extract_2g4g_nbrs_params_from_cfgsyn_data(self):
        pass

    def extract_2g4g_nbrs_params(self):
        self.extract_2g4g_nbrs_params_from_gexport_data()
        self.extract_2g4g_nbrs_params_from_nbi_data()
        self.extract_2g4g_nbrs_params_from_mml_data()
        self.extract_2g4g_nbrs_params_from_cfgsyn_data()

    def extract_3g2g_nbrs_params_from_gexport_data(self):
        pass

    def extract_3g2g_nbrs_params_from_nbi_data(self):
        pass

    def extract_3g2g_nbrs_params_from_mml_data(self):
        pass

    def extract_3g2g_nbrs_params_from_cfgsyn_data(self):
        pass

    def extract_3g2g_nbrs_params(self):
        self.extract_3g2g_nbrs_params_from_gexport_data()
        self.extract_3g2g_nbrs_params_from_nbi_data()
        self.extract_3g2g_nbrs_params_from_mml_data()
        self.extract_3g2g_nbrs_params_from_cfgsyn_data()

    def extract_3g3g_nbrs_params_from_gexport_data(self):
        pass

    def extract_3g3g_nbrs_params_from_nbi_data(self):
        pass

    def extract_3g3g_nbrs_params_from_mml_data(self):
        pass

    def extract_3g3g_nbrs_params_from_cfgsyn_data(self):
        pass

    def extract_3g3g_nbrs_params(self):
        self.extract_3g3g_nbrs_params_from_gexport_data()
        self.extract_3g3g_nbrs_params_from_nbi_data()
        self.extract_3g3g_nbrs_params_from_mml_data()
        self.extract_3g3g_nbrs_params_from_cfgsyn_data()

    def extract_3g4g_nbrs_params_from_gexport_data(self):
        """Extract Huawei 3G4G relations from GExport dumps"""
        pass

    def extract_3g4g_nbrs_params_from_nbi_data(self):
        """Extract Huawei 3G4G relations from itf-N dumps"""
        pass

    def extract_3g4g_nbrs_params_from_mml_data(self):
        pass

    def extract_3g4g_nbrs_params_from_cfgsyn_data(self):
        pass

    def extract_3g4g_nbrs_params(self):
        self.extract_3g4g_nbrs_params_from_gexport_data()
        self.extract_3g4g_nbrs_params_from_nbi_data()
        self.extract_3g4g_nbrs_params_from_mml_data()
        self.extract_3g4g_nbrs_params_from_cfgsyn_data()

    def extract_4g2g_nbrs_params_from_gexport_data(self):
        pass

    def extract_4g2g_nbrs_params_from_nbi_data(self):
        pass

    def extract_4g2g_nbrs_params_from_mml_data(self):
        pass

    def extract_4g2g_nbrs_params_from_cfgsyn_data(self):
        pass

    def extract_4g2g_params(self):
        self.extract_4g2g_nbrs_params_from_gexport_data()
        self.extract_4g2g_nbrs_params_from_nbi_data()
        self.extract_4g2g_nbrs_params_from_mml_data()
        self.extract_4g2g_nbrs_params_from_cfgsyn_data()

    def extract_4g3g_nbrs_params_from_gexport_data(self):
        pass

    def extract_4g3g_nbrs_params_from_nbi_data(self):
        pass

    def extract_4g3g_nbrs_params_from_mml_data(self):
        pass

    def extract_4g3g_nbrs_params_from_cfgsyn_data(self):
        pass

    def extract_4g3g_params(self):
        self.extract_4g3g_nbrs_params_from_gexport_data()
        self.extract_4g3g_nbrs_params_from_nbi_data()
        self.extract_4g3g_nbrs_params_from_mml_data()
        self.extract_4g3g_nbrs_params_from_cfgsyn_data()

    def extract_4g4g_nbrs_params_from_gexport_data(self):
        pass

    def extract_4g4g_nbrs_params_from_nbi_data(self):
        pass

    def extract_4g4g_nbrs_params_from_mml_data(self):
        pass

    def extract_4g4g_nbrs_params_from_cfgsyn_data(self):
        pass

    def extract_4g4g_nbrs_params(self):
        self.extract_4g4g_nbrs_params_from_gexport_data()
        self.extract_4g4g_nbrs_params_from_nbi_data()
        self.extract_4g4g_nbrs_params_from_mml_data()
        self.extract_4g4g_nbrs_params_from_cfgsyn_data()

    def extract_3g_sites_from_gexport_data(self):
        pass

    def extract_3g_sites_from_nbi_data(self):
        pass

    def extract_3g_sites_from_mml_data(self):
        pass

    def extract_3g_sites_from_cfgsyn_data(self):
        pass

    def extract_3g_sites(self):
        self.extract_3g_sites_from_gexport_data()
        self.extract_3g_sites_from_nbi_data()
        self.extract_3g_sites_from_mml_data()
        self.extract_3g_sites_from_cfgsyn_data()

    def extract_3g_cells_from_gexport_data(self):
        pass

    def extract_3g_cells_from_nbi_data(self):
        pass

    def extract_3g_cells_from_mml_data(self):
        pass

    def extract_3g_cells_from_cfgsyn_data(self):
        pass

    def extract_3g_cells(self):
        self.extract_3g_cells_from_gexport_data()
        self.extract_3g_cells_from_nbi_data()
        self.extract_3g_cells_from_mml_data()
        self.extract_3g_cells_from_cfgsyn_data()

    def extract_3g_cells_params_from_gexport_data(self):
        pass

    def extract_3g_cells_params_from_nbi_data(self):
        pass

    def extract_3g_cells_params_from_mml_data(self):
        pass

    def extract_3g_cells_params_from_cfgsyn_data(self):
        pass

    def extract_3g_cells_params(self):
        self.extract_3g_cells_params_from_gexport_data()
        self.extract_3g_cells_params_from_nbi_data()
        self.extract_3g_cells_params_from_mml_data()
        self.extract_3g_cells_params_from_cfgsyn_data()

    def extract_4g_cells_from_gexport_data(self):
        pass

    def extract_4g_cells_from_nbi_data(self):
        pass

    def extract_4g_cells_from_mml_data(self):
        pass

    def extract_4g_cells_from_cfgsyn_data(self):
        pass

    def extract_4g_cells(self):
        self.extract_4g_cells_from_gexport_data()
        self.extract_4g_cells_from_nbi_data()
        self.extract_4g_cells_from_mml_data()
        self.extract_4g_cells_from_cfgsyn_data()


    def extract_4g_cells_params_from_gexport_data(self):
        pass

    def extract_4g_cells_params_from_nbi_data(self):
        pass

    def extract_4g_cells_params_from_mml_data(self):
        pass

    def extract_4g_cells_params_from_cfgsyn_data(self):
        pass

    def extract_4g_cells_params(self):
        self.extract_4g_cells_params_from_gexport_data()
        self.extract_4g_cells_params_from_nbi_data()
        self.extract_4g_cells_params_from_mml_data()
        self.extract_4g_cells_params_from_cfgsyn_data()