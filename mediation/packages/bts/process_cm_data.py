from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import os
import subprocess

class ProcessCMData(object):
    """ Process network configuration data"""

    def __init__(self, dbname = None, dbuser = None, dbpass = None, dbhost = None):
        ''' Constructor for this class. '''

        self._dbhost=dbhost
        self._dbname=dbname
        self._dbuser=dbuser
        self._dbpass=dbpass

        if dbname is None: self._dbname="bts"
        if dbuser is None: self._dbuser="bodastage"
        if dbpass is None: self._dbpass="password"
        if dbhost is None: self._dbhost="locahost"

        # self.db_engine = create_engine('postgresql://{0}:{1}@{2}/{3}'.format(self._dbuser, self._dbpass, self._dbhost, self._dbname))
        self.db_engine = create_engine('postgresql://bodastage:password@database/bts')

    def extract_rncs(self, vendor= None):
        """Extract RNCs from provided vendors or else get from all vendors"""

        if vendor == None or vendor == 'ericsson':
            self.extract_ericsson_rncs()

    def extract_ericsson_rncs(self):
        """Extract RNCs from ericsson CM data"""
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
            "MeContext_id" as "name" , 
            1 as vendor_pk, -- 1=Ericsson, 2=Huawei
            2 as tech_pk , -- 1=gsm, 2-umts,3=lte
            0 as added_by,
            0 as modified_by
            FROM ericsson_bulkcm."RncFunction" t1
            LEFT OUTER  JOIN live_network.nodes t2 ON t1."MeContext_id" = t2."name"
            WHERE 
            t2."name" IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()


    def extract_ericsson_bscs(self):
        """Extract BSCs from Ericsson CM data(ericsson_cm_2g."BSC")"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
             INSERT INTO live_network.nodes
             (pk,date_added, date_modified, type,"name", vendor_pk, tech_pk, added_by, modified_by)
             SELECT 
             NEXTVAL('live_network.seq_nodes_pk'),
             "varDateTime" as date_added, 
             "varDateTime" as date_modified, 
             'BSC' as node_type,
             t1."BSC_NAME" as "name" , 
             1 as vendor_pk, -- 1=Ericsson, 2=Huawei
             1 as tech_pk , -- 1=gsm, 2-umts,3=lte
             0 as added_by,
             0 as modified_by
             FROM ericsson_cm_2g."BSC" t1
             LEFT OUTER  JOIN live_network.nodes t2 ON t1."BSC_NAME" = t2."name"
             WHERE 
             t2."name" IS NULL
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_2g_sites(self):
        """Extract Ericsson 2G Sites"""
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
            1, -- tech 3 -lte, 2 -umts, 1-gms
            1, -- 1- Ericsson, 2 - Huawei, 3 - zte, 4-nokika, etc...
            t1."SITE_NAME",
            t2.pk -- node primary key
            from ericsson_cm_2g."SITE" t1
            INNER join live_network.nodes t2 on t2."name" = t1."BSC_NAME" 
                AND t2.vendor_pk = 1 and t2.tech_pk = 1
            LEFT JOIN live_network.sites t3 on t3."name" = t1."SITE_NAME" 
               AND t3.vendor_pk = 1 and t3.tech_pk = 1
            WHERE 
            t3."name" IS NULL

        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_2g_cells(self):
        """Extract Ericsson GSM Cells"""
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
            1, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
            t1."CELL_NAME",
            t4.pk -- site primary key
            FROM ericsson_cm_2g."INTERNAL_CELL" t1
            INNER JOIN live_network.nodes t3 on t3."name" = t1."BSC_NAME" 
                    AND t3.vendor_pk = 1
                    AND t3.tech_pk = 1
            INNER JOIN live_network.sites t4 on t4."name" = LEFT(t1."CELL_NAME", LENGTH(t1."CELL_NAME")-1)
                AND t4.vendor_pk = 1 
                AND t4.tech_pk = 1
                AND t4.node_pk = t3.pk
            LEFT JOIN live_network.cells t5 on t5."name" = t1."CELL_NAME"
                AND t5.tech_pk = 1
                AND t5.vendor_pk = 1
            WHERE 
            t5."name" IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_2g_externals_on_2g(self):
        """Extract Ericsson 2G Externals"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.gsm_external_cells
        (pk, name, cell_pk, node_pk, mcc, mnc, lac, bcch, ncc, bcc, ci, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_gsm_external_cells_pk') as pk,
        t1."CELL_NAME" AS "name",
        t3.pk AS cell_pk,
        t2.pk AS node_pk,
        t1."MCC"::integer AS mcc,
        t1."MNC"::integer AS mnc,
        t1."LAC"::integer AS lac,
        t1."BCCHNO"::integer AS bcch,
        t1."NCC"::integer AS ncc,
        t1."BCC"::integer AS bcc,
        t1."CI"::integer AS ci,
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        ericsson_cm_2g."EXTERNAL_CELL" t1
        LEFT JOIN live_network.nodes t2 ON t2."name" = t1."BSC_NAME"
        LEFT JOIN live_network.cells t3 on t3."name" = t1."CELL_NAME"
        LEFT JOIN live_network.gsm_external_cells t4 on t4."name" = t1."CELL_NAME" 
            AND t4.lac = t1."LAC"::integer
            AND t4.ci = t1."CI"::integer
        WHERE 
        t4.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_3g_externals_on_2g(self):
        """"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.umts_external_cells
        (pk, name, cell_pk,  mcc, mnc, lac, rncid, ci, psc, dl_uarfcn, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_umts_external_cells_pk') as pk,
        t1."CELL_NAME" AS "name",
        t3.pk AS cell_pk,
        t1."MCC"::integer AS mcc,
        t1."MNC"::integer AS mnc,
        t1."LAC"::integer AS lac,
        t1."RNCID"::integer as rncid,
        t1."CI"::integer as ci,
        t1."SCRCODE"::integer AS psc,
        t1."FDDARFCN"::integer AS dl_uarfcn,
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        ericsson_cm_2g."EXTERNAL_CELL" t1
        LEFT JOIN live_network.nodes t2 ON t2."name" = t1."BSC_NAME"
        LEFT JOIN live_network.cells t3 on t3."name" = t1."CELL_NAME"
        LEFT JOIN live_network.umts_external_cells t4 on t4."name" = t1."CELL_NAME" 
            AND t4.lac = t1."LAC"::integer
            AND t4.ci = t1."CI"::integer
        WHERE 
        t4.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_2g_externals_on_3g(self):
        """Extract Ericsson 3G Externals in 3G"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """

        INSERT INTO live_network.gsm_external_cells
        (pk, name, cell_pk, node_pk, mcc, mnc, lac, bcch, ncc, bcc, ci, rac, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_gsm_external_cells_pk') as pk,
        t1."CELL_NAME" AS "name",
        t3.pk AS cell_pk,
        t2.pk AS node_pk,
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
        ericsson_cm_3g."ExternalGsmCell"
        LEFT JOIN live_network.nodes t2 ON t2."name" = t1."BSC_NAME"
        LEFT JOIN live_network.cells t3 on t3."name" = t1."CELL_NAME"
        LEFT JOIN live_network.gsm_external_cells t4 on t4."name" = t1."userLabel" 
            AND t4.lac = t1."LAC"::integer
            AND t4.ci = t1."CI"::integer
        WHERE 
        t4.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_3g_externals_on_3g(self):
        """Extract Ericsson 2G Externals in 3G"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """

        INSERT INTO live_network.umts_external_cells
        (pk, name, cell_pk, mcc, mnc, lac, rncid, ci, psc, dl_uarfcn, ul_arfcn, rac, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_gsm_external_cells_pk') as pk,
        t1."userLabel" AS "name",
        t3.pk AS cell_pk,
        t1."mcc" AS mcc,
        t1."mnc" AS mnc,
        t1."lac" AS lac,
        t1."rncId" as rncid,
        t1."cId" as ci,
        t1."primaryScramblingCode" AS psc,
        t1."uarfcnDl" AS dl_uarfcn,
        t1."uarfcnUl" AS ul_uarfcn,
        t1."rac" AS rac,
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM 
        ericsson_cm_3g."ExternalUtranCell" t1
        LEFT JOIN live_network.cells t3 on t3."name" = t1."userLabel"
        LEFT JOIN live_network.umts_external_cells t4 on t4."name" = t1."userLabel" 
        AND t4.lac = t1."lac"::integer
        AND t4.ci = t1."cId"::integer
        WHERE 
        t4.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_4g_externals_on_3g(self):
        """Extract Ericsson 2G Externals in 3G"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.lte_external_cells
        (pk, name, cell_pk, mcc, mnc, pci, dl_earfcn, ci, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_gsm_external_cells_pk') as pk,
        t1."userLabel" AS "name",
        t3.pk AS cell_pk,
        null AS mcc,
        null AS mnc,
        (t1."localCellId"::integer + 3 * t1."physicalLayerCellIdGroup"::integer) AS pci,
        t1."earfcndl"::integer AS dl_earfcn_dl,
        t1."localCellId"::integer AS ci,
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        ericsson_cm_3g."ExternalEUtranCellFDD" t1
        LEFT JOIN live_network.cells t3 on t3."name" = t1."userLabel"
        LEFT JOIN live_network.lte_external_cells t4 on t4."name" = t1."userLabel" 
            AND t4.ci = t1."localCellId"::integer
        LEFT JOIN live_network.nodes t5 ON t5.name = 'SubNetwork' AND t5.vendor_pk = 1 AND t5.tech_pk = 3
        WHERE 
        t4.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()



    def extract_ericsson_enodebs(self):
        """Extract Ericsson ENodebs"""
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
            1 as vendor_pk, -- 1=Ericsson, 2=Huawei
            "MeContext_id",
            0 as added_by,
            0 as modified_by
            FROM ericsson_cm_4g."ENodeBFunction" t1
            LEFT OUTER  JOIN live_network.sites t2 ON t1."MeContext_id" = t2."name"
            WHERE 
            t2."name" IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()


    def extract_ericsson_3g_sites(self):
        """Extract Ericsson NodeBs"""
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
            2, -- tech 3 -lte, 2 -umts, 1-gms
            1, -- 1- Ericsson, 2 - Huawei,
            t1."MeContext_id",
            t2.pk -- node primary key
            from ericsson_bulkcm."NodeBFunction" t1
            INNER join live_network.nodes t2 on t2."name" = t1."SubNetwork_2_id" 
                AND t2.vendor_pk = 1 and t2.tech_pk = 2
            LEFT JOIN live_network.sites t3 on t3."name" = t1."MeContext_id" 
               AND t2.vendor_pk = 1 and t2.tech_pk = 2
            WHERE 
            t3."name" IS NULL
            
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_3g_cells(self):
        """Extract Ericsson UTMS Cells"""
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
            2, -- tech 3 -lte, 2 -umts, 1-gms
            1, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
            t1."userLabel",
            t4.pk -- site primary key
            FROM ericsson_cm_3g."UtranCell" t1
            INNER JOIN ericsson_bulkcm."NodeBFunction" t2 on t2."nodeBFunctionIubLink" = t1."utranCellIubLink"
            INNER JOIN live_network.nodes t3 on t3."name" = t1."MeContext_id" 
                    AND t3.vendor_pk = 1
                    AND t3.tech_pk = 2
            INNER JOIN live_network.sites t4 on t4."name" = t2."MeContext_id"
                AND t4.vendor_pk = 1 
                AND t4.tech_pk = 2
                AND t4.node_pk = t3.pk
            LEFT JOIN live_network.cells t5 on t5."name" = t1."UtranCell_id"
                AND t5.tech_pk = 2
                AND t5.vendor_pk = 1
            WHERE 
            t5."name" IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_3g_cells_per_site(self):
        """Extract 3G cells in bunches
        """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        sites = session.query(Site).filter_by(vendor_pk=1).filter_by(tech_pk=2).all()
        # for site in session.query(Site).filter_by(vendor_pk=1).filter_by(tech_pk=2).yield_per(5):
        i = 0
        sites_len = len(sites)
        while i < sites_len:
            # Handle iterations at the end of the site list
            end = i+5;
            if sites_len < i+5:
                end = sites_len

            placeholder_range = 5
            if end == sites_len:
                placeholder_range = end-i

            site_list = list( map( lambda x:x[1], sites[i:end]) )
            # placeholders = map( lambda x: ':p'+x , range(5)) # [:p0,...,:p4]

            placeholders = []
            site_list_placeholders = {}
            for r in range(placeholder_range):
                placeholders.append(':p'+ str(r))
                site_list_placeholders['p'+ str(r)] = site_list[r]

            # print(site_list_placeholders)
            # print(site_list)

            i = i+5
            sql = """
                INSERT INTO live_network.cells
                (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, site_pk)
                SELECT 
                nextval('live_network.seq_cells_pk'),
                t1."varDateTime" as date_added, 
                t1."varDateTime" as date_modified, 
                0 as added_by,
                0 as modified_by,
                2, -- tech 3 -lte, 2 -umts, 1-gms
                1, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
                t1."userLabel",
                t4.pk -- site primary key
                FROM ericsson_cm_3g."UtranCell" t1
                INNER JOIN ericsson_cm_3g."NodeBFunction" t2 on t2."nodeBFunctionIubLink" = t1."utranCellIubLink"
                INNER JOIN live_network.nodes t3 on t3."name" = t1."MeContext_id" 
                        AND t3.vendor_pk = 1
                        AND t3.tech_pk = 2
                INNER JOIN live_network.sites t4 on t4."name" = t2."MeContext_id"
                    AND t4.vendor_pk = 1 
                    AND t4.tech_pk = 2
                    AND t4.node_pk = t3.pk
                LEFT JOIN live_network.cells t5 on t5."name" = t1."UtranCell_id"
                    AND t5.tech_pk = 2
                    AND t5.vendor_pk = 1
                WHERE 
                t5."name" IS NULL
                AND t4."name" IN ({})
            """.format( ', '.join(placeholders) )

            self.db_engine.execute(text(sql).execution_options(autocommit=True),**site_list_placeholders)

        session.close()

    def extract_ericsson_4g_cells(self):
        """Extract Ericsson LTE Cells
        This extract the parameters in one query. Needs alot of memory for large networks
        """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.cells
            (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, site_pk)
            SELECT 
            NEXTVAL('live_network.seq_cells_pk'),
            t1."varDateTime" as date_added, 
            t1."varDateTime" as date_modified, 
            0 as added_by,
            0 as modified_by,
            3, -- tech 3 -lte, 2 -umts, 1-gms
            1, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
            CASE WHEN t1."userLabel" IS NULL THEN t1."vsDataEUtranCellFDD_id" ELSE t1."userLabel" END AS "name",
            t2.pk -- site primary key
            FROM ericsson_cm_3g."EUtranCellFDD" t1
            INNER JOIN live_network.sites t2 on t2."name" = t1."MeContext_id" 
                AND t2.vendor_pk = 1 and t2.tech_pk = 3 
            LEFT JOIN live_network.cells t3 on t3."name" = CASE WHEN t1."userLabel" IS NULL THEN t1."vsDataEUtranCellFDD_id" ELSE t1."userLabel" END,
                AND t2.vendor_pk = 1 and t2.tech_pk = 3 
            WHERE
                t3."name" IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_4g_cells_per_site(self):
        """" Extrcts Ericsson 4G Cells  in bunces
            This is ideal for large networks or whne BTS is run on as system with limited resources
        """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        sites = session.query(Site).filter_by(vendor_pk=1).filter_by(tech_pk=3).all()
        # for site in session.query(Site).filter_by(vendor_pk=1).filter_by(tech_pk=2).yield_per(5):
        i = 0
        sites_len = len(sites)
        while i < sites_len:
            end = i+5;
            if sites_len < i+5:
                end = sites_len

            placeholder_range = 5
            if end == sites_len:
                placeholder_range = end-i

            site_list = list( map( lambda x:x[1], sites[i:end]) )
            # placeholders = map( lambda x: ':p'+x , range(5)) # [:p0,...,:p4]
            placeholders = []
            site_list_placeholders = {}
            for r in range(placeholder_range):
                placeholders.append(':p'+ str(r))
                site_list_placeholders['p'+ str(r)] = site_list[r]

            # print(site_list_placeholders)
            # print(site_list)

            i = i+5
            sql = """
            INSERT INTO live_network.cells
            (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, site_pk)
            SELECT 
            NEXTVAL('live_network.seq_cells_pk'),
            t1."varDateTime" as date_added, 
            t1."varDateTime" as date_modified, 
            0 as added_by,
            0 as modified_by,
            3, -- tech 3 -lte, 2 -umts, 1-gms
            1, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
            CASE WHEN t1."userLabel" IS NULL THEN t1."vsDataEUtranCellFDD_id" ELSE t1."userLabel" END AS "name",
            t2.pk -- site primary key
            FROM ericsson_cm_3g."EUtranCellFDD" t1
            INNER JOIN live_network.sites t2 on t2."name" = t1."MeContext_id" 
                AND t2.vendor_pk = 1 and t2.tech_pk = 3 
            LEFT JOIN live_network.cells t3 on t3."name" = t1."userLabel"
                AND t2.vendor_pk = 1 and t2.tech_pk = 3 
            WHERE
                t3."name" IS NULL
                AND t2."name" IN ({})
            """.format( ', '.join(placeholders) )

            self.db_engine.execute(text(sql).execution_options(autocommit=True),**site_list_placeholders)

        session.close()

    def extract_ericsson_4g_cell_params(self):
        """Extract Ericsson LTE cell parameters"""
        """Extract Ericsson LTE cell parameters"""

        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        # Truncate paramete table
        # self.db_engine.execute(text("TRUNCATE TABLE live_network.lte_cells_data").execution_options(autocommit=True))
        # self.db_engine.execute(text("ALTER SEQUENCE live_network.seq_lte_cells_data_pk RESTART WITH 1;").
        #                       execution_options(autocommit=True))

        # The data is alot. Let's handle per site
        site_sql = """SELECT pk, "name" from live_network.sites where vendor_pk  = 1 and tech_pk = 3"""

        result = self.db_engine.execute(site_sql)

        for row in result:
            (site_pk, site_name) = row

            # print("Extracting cells parameters for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                        INSERT INTO live_network.lte_cells_data
                        (pk, name, cell_pk, uarfcn_dl, uarfcn_ul, mcc, mnc, tac, pci, ecgi, rach_root_sequence, max_tx_power, latitude, longitude,
                        height, dl_bandwidth, ul_bandwidth, ta, ta_mode, tx_elements, rx_elements, scheduler, azimuth, mechanical_tilt, electrical_tilt, cell_range,
                        site_pk, tech_pk, vendor_pk, modified_by, added_by, date_added, date_modified)
                        SELECT 
                        NEXTVAL('live_network.seq_lte_cells_data_pk'),
                        t1."vsDataEUtranCellFDD_id" as name,
                        t2.pk as cell_pk,
                        t1."earfcndl"::integer as uarfcn_dl,
                        t1."earfcnul"::integer as uarfcn_ul,
                        t1."mcc"::integer as mcc,
                        t1."mnc"::integer as mc,
                        t1."tac"::integer as tac,
                        t1."physicalLayerCellIdGroup"::integer as pci,
                        null as ecgi,
                        t1."rachRootSequence" as rach_root_sequence,
                        null as max_tx_power,
                        (t1."latitude"::float/93206.76)*(-1::float)  as latitude,
                        t1."longitude"::float/46603.38 as longitude,
                        t1."altitude"::integer as height,
                        t1."dlChannelBandwidth"::integer as dl_bandwidth,
                        t1."ulChannelBandwidth"::integer as ul_bandwidth,
                        null as ta,
                        null as ta_mode,
                        t1."numOfTxAntennas"::integer as tx_elements,
                        t1."numOfRxAntennas"::integer as rx_elements,
                        null as scheduler,
                        null as azimuth,
                        null as mechanical_tilt,
                        null as electrical_tilt,
                        t1."cellRange"::integer as cell_range,
                        t2.site_pk as site_pk,
                        t2.tech_pk as tech_pk,
                        t2.vendor_pk as vendor_pk,
                        0 as modified_by, 
                        0 as added_by, 
                        t1."varDateTime" as date_added, 
                        t1."varDateTime" as date_modified
                        FROM ericsson_cm_3g."EUtranCellFDD" t1
                        INNER JOIN live_network.cells t2 on t2."name" = t1."vsDataEUtranCellFDD_id"
                        WHERE t1."MeContext_id" = '{0}';
                    """.format(site_name)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_3g_cell_params(self):
        """Extract Ericsson UMTS cell parameters"""

        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        # Truncate paramete table
        # self.db_engine.execute(text("TRUNCATE TABLE live_network.umts_cells_data").execution_options(autocommit=True))
        # self.db_engine.execute(text("ALTER SEQUENCE live_network.seq_umts_cells_data_pk RESTART WITH 1;").
        #                        execution_options(autocommit=True))

        # The data is alot. Let's handle per site
        site_sql = """SELECT pk, "name" from live_network.sites where vendor_pk  = 1 and tech_pk = 2"""

        result = self.db_engine.execute(site_sql)

        for row in result:
            (site_pk,site_name)=row

            # print("Extracting cells parameters for site_pk: {0}, site_name: {1}".format(site_pk,site_name))

            sql = """
                INSERT INTO live_network.umts_cells_data
                (pk, date_added, date_modified, added_by, modified_by,bch_power,cell_id,cell_pk,lac,latitude, longitude, 
                maximum_transmission_power, "name", cpich_power, primary_sch_power, scrambling_code, rac, sac, 
                secondary_sch_power, site_pk, tech_pk, vendor_pk, uarfcn_dl,uarfcn_ul, ura_list, azimuth, cell_range, 
                height, site_sector_carrier, mcc,mnc,ura,localcellid)
                SELECT 
                NEXTVAL('live_network.seq_umts_cells_data_pk'),
                t1."varDateTime" as date_added, 
                t1."varDateTime" as date_modified, 
                0 as added_by,
                0 as modified_by,
                t1."bchPower"::integer,
                t1."cId"::integer as ci,
                t3.pk as cell_pk, -- cellid
                t1."lac"::integer,
                (t4."antennaPosition_latitude"::float/93206.76)*(-1::float*t4."antennaPosition_latitudeSign"::float) 
                as latitude,
                t4."antennaPosition_longitude"::float/46603.38 as longitude,
                t1."maximumTransmissionPower"::integer as maximum_transmission_power,
                t1."UtranCell_id",
                t1."primaryCpichPower"::integer as cpich_power,
                t1."primarySchPower"::integer as primary_sch_power,
                t1."primaryScramblingCode"::integer as scrambling_code,
                t1."rac"::integer as rac,
                t1."sac"::integer as sac,
                t1."secondarySchPower"::integer as secondary_sch_power,
                t3.site_pk, -- site pk
                2, -- umts
                1, -- Ericsson
                t1."uarfcnDl"::integer,
                t1."uarfcnUl"::integer,
                t1."uraList",
                t6."beamDirection"::integer, -- azimuth,
                t5."cellRange"::integer, -- cellrange,
                t6."height"::integer, -- height
                concat(t2."MeContext_id", '_', t2."vsDataRbsLocalCell_id") as site_sector_carrier,
                t7."mcc"::integer as mcc,
                t7."mnc"::integer as mnc,
                t1."uraList" as ura ,
                t1."localCellId"::integer as localcellid
                FROM 
                ericsson_cm_3g."UtranCell" t1
                INNER JOIN ericsson_cm_3g."RbsLocalCell" t2 on t2."localCellId" = t1."cId" and t2."SubNetwork_2_id" = t1."SubNetwork_2_id" 
                    -- and t2."MeContext_id" = t1."MeContext_id"
                INNER JOIN live_network.cells t3 on t3."name" = t1."userLabel"
                INNER JOIN ericsson_bulkcm."vsDataUtranCell" t4 on t4."UtranCell_id" = t1."UtranCell_id" and t4."SubNetwork_2_id" = t1."SubNetwork_2_id" 
                INNER JOIN ericsson_bulkcm."vsDataCarrier" t5 on  t5."SubNetwork_2_id" = t1."SubNetwork_2_id" 
                    and t5."MeContext_id" = t2."MeContext_id"
                    and concat('S',TRIM(t5."vsDataSector_id"),'C', TRIM(t5."vsDataCarrier_id")) = t2."vsDataRbsLocalCell_id" 
                INNER JOIN ericsson_bulkcm."vsDataSector" t6 on t6."SubNetwork_2_id" = t1."SubNetwork_2_id"
                    and t6."MeContext_id" = t5."MeContext_id"
                    and t6."vsDataSector_id" = t5."vsDataSector_id"
                INNER JOIN ericsson_cm_3g."RncFunction" t7 on  t7."SubNetwork_2_id" = t1."SubNetwork_2_id" 
                    AND t6."MeContext_id" = t5."MeContext_id"
                    AND t7."RncFunction_id" = t1."RncFunction_id" 
                WHERE t6."MeContext_id" = '{0}';
            """.format(site_name)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_2g_cell_params(self):
        """Extract Ericsson LTE cell parameters"""
        """Extract Ericsson GSM cell parameters"""

        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        # Truncate paramete table
        self.db_engine.execute(text("TRUNCATE TABLE live_network.gsm_cells_data").execution_options(autocommit=True))
        self.db_engine.execute(text("ALTER SEQUENCE live_network.seq_gsm_cells_data_pk RESTART WITH 1;").
                               execution_options(autocommit=True))

        # The data is alot. Let's handle per site
        site_sql = """SELECT pk, "name" from live_network.sites where vendor_pk  = 1 and tech_pk = 1"""

        result = self.db_engine.execute(site_sql)

        # for row in result:
        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=1).filter_by(tech_pk=1).yield_per(5):
            (site_pk, site_name) = (site[0],site[1])

            # print("Extracting cells parameters for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                        INSERT INTO live_network.gsm_cells_data
                        (pk, name, cell_pk, ci, bcc, ncc, bsic, bcch, lac, latitude, longitude, cgi, azimuth, height, 
                        mechanical_tilt, electrical_tilt, hsn, hopping_type, tch_carriers, mcc, mnc, modified_by, added_by, date_added, date_modified)
                        SELECT 
                        NEXTVAL('live_network.seq_gsm_cells_data_pk'),
                        t1."CELL_NAME" as name,
                        t2.pk as cell_pk,
                        t1."CI"::integer as ci,
                        t1."BCC"::integer as bcc,
                        t1."NCC"::integer as ncc,
                        CONCAT(trim(t1."NCC"),trim(t1."BCC"))::integer as bsic,
                        t1."BCCHNO"::integer as bcch,
                        t1."LAC"::integer as lac,
                        (CASE WHEN t1."LATITUDE" = '?' THEN '0' ELSE t1."LATITUDE" END)::float as latitude,
                        (CASE WHEN t1."LONGITUDE" = '?' THEN '0' ELSE t1."LATITUDE" END)::float as longitude,
                        CONCAT( TRIM(t1."MCC"),'-', TRIM(t1."MNC"),'-',TRIM(t1."LAC"),'-',TRIM(t1."CI")) as cgi,
                        t1."CELL_DIR"::integer as azimuth,
                        t1."HEIGHT"::integer as height,
                        t1."ANTENNA_TILT"::integer as mechanical_tilt,
                        -- t1."SECTOR_ANGLE"::integer as sector_angle,
                        -- t1."MAX_TA" as ta
                        -- t1."STATE" as STATE -- ACTIVE or INACTIVE
                        null as electrical_tilt,
                        null as hsn,
                        null as hopping_type,
                        null as tch_carriers,
                        t1."MCC"::integer,
                        t1."MNC"::integer,
                        0 as modified_by,
                        0 as added_by,
                        t1."varDateTime" as date_added,
                        t1."varDateTime" as date_modified
                        FROM ericsson_cm_2g."INTERNAL_CELL" t1
                        INNER JOIN live_network.cells t2 on t2."name" = t1."CELL_NAME" AND t2.vendor_pk = 1 AND t2.tech_pk = 1
                        INNER JOIN live_network.sites t3 on t3."name" = LEFT(t1."CELL_NAME", LENGTH(t1."CELL_NAME")-1)
                        WHERE 
                        t3."name" ='{0}';
                    """.format(site_name)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_3g3g_nbrs(self):
       """"Extract Ericsson 3G 3G nbrs"""
       self.extract_ericsson_3g3g_nbrs_with_ericsson()
       self.extract_ericsson_3g3g_nbrs_with_other_vendors()
       
    def extract_ericsson_3g3g_nbrs_with_ericsson(self):
        """Extract Ericsson UMTS-UMTS neighbour relations"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        print("extract_ericsson_3g3g_nbrs_with_ericsson")

        sql = """
            INSERT INTO live_network.relations 
            (pk, svrnode_pk,svrsite_pk,svrtech_pk,svrvendor_pk,svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
            SELECT 
            NEXTVAL('live_network.seq_relations_pk'),
            -- serving side
            t4.node_pk as svrnode_pk, 
            t3.site_pk as svrsite_pk, 
            t3.tech_pk as svrtech_pk,
            t3.vendor_pk as svrvendor_pk ,
            t3.pk as svrcell_pk,
            -- nbr side 
            t7.node_pk as nbrnode_pk, 
            t6.site_pk as nbrsite_pk, 
            t6.tech_pk as nbrtech_pk,
            t6.vendor_pk as nbrvendor_pk ,
            t6.pk as nbrcell_pk,
            -- meta fields 
            t1."varDateTime" ,
            t1."varDateTime" ,
            0, -- system
            0
            FROM ericsson_cm_3g."UtranRelation" t1 
            INNER JOIN ericsson_cm_3g."UtranCell" t2 ON t1."adjacentCell" = concat('SubNetwork=ONRM_ROOT_MO_R,SubNetwork=',trim(t2."SubNetwork_2_id"),',MeContext=',trim(t2."MeContext_id"),',ManagedElement=',trim(t2."ManagedElement_id"),',RncFunction=',trim(t2."RncFunction_id"),',UtranCell=',trim(t2."UtranCell_id"))
            -- -- serving side
            INNER JOIN ericsson_cm_3g."UtranCell" t10 ON 
                t10."UtranCell_id" = t1."UtranCell_id" 
                AND TRIM(t10."SubNetwork_2_id") = TRIM(t1."SubNetwork_2_id") 
                AND TRIM(t10."MeContext_id") = TRIM(t1."MeContext_id")
                AND TRIM(t10."ManagedElement_id") = TRIM(t1."ManagedElement_id")
                AND TRIM(t10."RncFunction_id") = TRIM(t1."RncFunction_id")
            INNER JOIN live_network.cells t3 ON t3."name" = TRIM(t10."userLabel") AND t3.vendor_pk = 1 AND t3.tech_pk = 2
            INNER JOIN live_network.sites t4 ON t4.pk = t3.site_pk
            INNER JOIN live_network.nodes t5 ON t5.pk = t4.node_pk 
            -- -- nbr side 
            INNER JOIN live_network.cells t6 ON t6."name" = TRIM(t2."userLabel") AND t3.vendor_pk = 1 AND t3.tech_pk = 2
            INNER JOIN live_network.sites t7 ON t7.pk = t6.site_pk
            -- This part is to extract only new relations
            LEFT JOIN live_network.relations t9 ON t9.svrcell_pk = t3.pk 
                AND t9.nbrcell_pk = t6.pk
            WHERE 
                t9.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_3g3g_nbrs_per_site(self):
        """Extract Ericsson UMTS-UMTS neighbour relations"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=1).filter_by(tech_pk=2).yield_per(5):
            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk,svrtech_pk,svrvendor_pk,svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t4.node_pk as svrnode_pk, 
                t3.site_pk as svrsite_pk, 
                t3.tech_pk as svrtech_pk,
                t3.vendor_pk as svrvendor_pk ,
                t3.pk as svrcell_pk,
                -- nbr side 
                t7.node_pk as nbrnode_pk, 
                t6.site_pk as nbrsite_pk, 
                t6.tech_pk as nbrtech_pk,
                t6.vendor_pk as nbrvendor_pk ,
                t6.pk as nbrcell_pk,
                -- meta fields 
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM ericsson_cm_3g."UtranRelation" t1 
                INNER JOIN ericsson_cm_3g."UtranCell" t2 ON t1."adjacentCell" = concat('SubNetwork=ONRM_ROOT_MO_R,SubNetwork=',trim(t2."SubNetwork_2_id"),',MeContext=',trim(t2."MeContext_id"),',ManagedElement=',trim(t2."ManagedElement_id"),',RncFunction=',trim(t2."RncFunction_id"),',UtranCell=',trim(t2."UtranCell_id"))
                -- -- serving side
                INNER JOIN ericsson_cm_3g."UtranCell" t10 ON 
                    t10."UtranCell_id" = t1."UtranCell_id" 
                    AND TRIM(t10."SubNetwork_2_id") = TRIM(t1."SubNetwork_2_id") 
                    AND TRIM(t10."MeContext_id") = TRIM(t1."MeContext_id")
                    AND TRIM(t10."ManagedElement_id") = TRIM(t1."ManagedElement_id")
                    AND TRIM(t10."RncFunction_id") = TRIM(t1."RncFunction_id")
                INNER JOIN live_network.cells t3 ON t3."name" = TRIM(t10."userLabel") AND t3.vendor_pk = 1 AND t3.tech_pk = 2
                INNER JOIN live_network.sites t4 ON t4.pk = t3.site_pk
                INNER JOIN live_network.nodes t5 ON t5.pk = t4.node_pk 
                -- -- nbr side 
                INNER JOIN live_network.cells t6 ON t6."name" = TRIM(t2."userLabel") AND t3.vendor_pk = 1 AND t3.tech_pk = 2
                INNER JOIN live_network.sites t7 ON t7.pk = t6.site_pk
                -- This part is to extract only new relations
                LEFT JOIN live_network.relations t9 ON t9.svrcell_pk = t3.pk 
                    AND t9.nbrcell_pk = t6.pk
                WHERE 
                    t9.pk IS NULL
                    AND t4."name" = :svr_site
            """
            # print(sql)
            # print(site)
            self.db_engine.execute(text(sql).execution_options(autocommit=True), svr_site=site[1])

        session.close()

    def extract_ericsson_3g4g_nbrs(self):
        """Extract Ericsson UMTS-LTE neighbour relations"""
        pass

    def extract_ericsson_4g4g_nbrs(self):
        """Extract Ericsson LTE-LTE neighbour relations"""
        pass


    def extract_ericsson_2g2g_nbrs(self):
        """Extract Ericsson 2G-2G neighbour relations"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=1).filter_by(tech_pk=1).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            # print("Extracting E// 2G-2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk,svrtech_pk,svrvendor_pk,svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t4.node_pk as srvnode_pk,
                t2.site_pk as svrsite_pk,
                1 as svrtech_pk,
                1 as svrvendor_pk,
                t2.pk as svrcell_pk,
                -- nbr side
                t5.node_pk as nbrnode_pk,
                t3.site_pk as nbrsite_pk,
                1 as nbrtech_pk,
                1 as nbrvendor_pk,
                t3.pk as nbrcell_pk,
                t1."varDateTime" as date_added,
                t1."varDateTime" as date_modified,
                0 as modified_by,
                0 as added_by
                FROM 
                ericsson_cm_2g."NREL" t1
                INNER JOIN live_network.cells t2 ON t2."name" = t1."CELL_NAME" AND t2.vendor_pk = 1 AND t2.tech_pk = 1
                LEFT JOIN live_network.cells t3 on t3."name" = t1."NREL_NAME" AND t3.tech_pk = 1
                INNER JOIN live_network.sites t4 on t4.pk = t2.site_pk AND  t4.tech_pk = 1
                LEFT JOIN live_network.sites t5 on t5.pk = t3.site_pk AND t5.tech_pk = 1
                LEFT JOIN live_network.relations t6 ON t6.svrcell_pk = t2.pk AND t6.nbrcell_pk = t3.pk
                WHERE  t6.pk IS NULL 
                AND t2.site_pk = '{0}'
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_huawei_bscs(self):
        """Extract BSCs from Huawei CM data(hua_cm_2g.bscbasic)"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
             INSERT INTO live_network.nodes
             (pk,date_added, date_modified, type,"name", vendor_pk, tech_pk, added_by, modified_by)
             SELECT 
             NEXTVAL('live_network.seq_nodes_pk'),
             "varDateTime" as date_added, 
             "varDateTime" as date_modified, 
             'BSC' as node_type,
             t1."neid" as "name" , 
             2 as vendor_pk, -- 1=Ericsson, 2=Huawei
             1 as tech_pk , -- 1=gsm, 2-umts,3=lte
             0 as added_by,
             0 as modified_by
             FROM hua_cm_2g.bscbasic t1
             LEFT OUTER  JOIN live_network.nodes t2 ON t1."neid" = t2."name"
             WHERE 
             t2."name" IS NULL
             AND trim(t1.module_type) = 'Radio'
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_huawei_2g_sites(self):
        """Extract Huawei 2G Sites"""
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
            from hua_cm_2g.bts t1
            INNER join live_network.nodes t2 on t2."name" = t1."neid" 
                AND t2.vendor_pk = 2 and t2.tech_pk = 1
            LEFT JOIN live_network.sites t3 on t3."name" = t1."BTSNAME" 
               AND t2.vendor_pk = 2 and t2.tech_pk = 1
            WHERE 
            t3."name" IS NULL
            AND trim(t1.module_type) = 'Radio'

        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))


    def extract_huawei_2g_cells(self):
        """Extract Huawesi GSM Cells"""
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
            FROM hua_cm_2g.gcell t1
            INNER JOIN live_network.nodes t3 on t3."name" = t1."neid" 
                    AND t3.vendor_pk = 2
                    AND t3.tech_pk = 1
            INNER JOIN hua_cm_2g.cellbind2bts t6 on t6."neid" = t3.name AND t6."CELLID" = t1."CELLID"
            INNER JOIN hua_cm_2g.bts t7 on t7."neid" = t3.name AND t7."BTSID" = t6."BTSID"
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


    def extract_huawei_2g_cell_params(self):
        """Extract Ericsson LTE cell parameters"""
        """Extract Huawei GSM cell parameters"""

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
            (site_pk, site_name) = (site[0],site[1])

            # print("Extracting cells parameters for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

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

    def extract_huawei_rncs(self):
        """Extract Huawei 3G RNCs"""
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
            FROM hua_cm_3g.urncbasic t1
            LEFT OUTER  JOIN live_network.nodes t2 ON t1."neid" = t2."name"
            WHERE 
            t2."name" IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_3g_sites(self):
        """Extract Ericsson NodeBs"""
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
            2, -- tech 3 -lte, 2 -umts, 1-gms
            1, -- 1- Ericsson, 2 - Huawei,
            t1."MeContext_id",
            t2.pk -- node primary key
            from ericsson_bulkcm."NodeBFunction" t1
            INNER join live_network.nodes t2 on t2."name" = t1."SubNetwork_2_id" 
                AND t2.vendor_pk = 1 and t2.tech_pk = 2
            LEFT JOIN live_network.sites t3 on t3."name" = t1."MeContext_id" 
               AND t2.vendor_pk = 1 and t2.tech_pk = 2
            WHERE 
            t3."name" IS NULL

        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_huawei_3g_sites(self):
        """Extract Ericsson NodeBs"""
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
            2, -- tech 3 -lte, 2 -umts, 1-gms
            2, -- 1- Ericsson, 2 - Huawei,
            t1."NODEBNAME",
            t2.pk -- node primary key
            from hua_cm_3g.unodeb t1
            INNER join live_network.nodes t2 on t2."name" = t1."neid" 
                AND t2.vendor_pk = 2 and t2.tech_pk = 2
            LEFT JOIN live_network.sites t3 on t3."name" = t1."NODEBNAME"
               AND t2.vendor_pk = 2 and t2.tech_pk = 2
            WHERE 
            t3."name" IS NULL
            
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_huawei_3g_cells(self):
        """Extract Huawei 3G cells in bunches
        """
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

            # print(site_list_placeholders)
            # print(site_list)

            i = i + 5
            sql = """
                INSERT INTO live_network.cells
                (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, site_pk)
                SELECT 
                nextval('live_network.seq_cells_pk'),
                t1."varDateTime" as date_added, 
                t1."varDateTime" as date_modified, 
                0 as added_by,
                0 as modified_by,
                2, -- tech 3 -lte, 2 -umts, 1-gms
                2, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
                t1."CELLNAME" AS name,
                t4.pk -- site primary key
                FROM hua_cm_3g.ucell t1
                INNER JOIN live_network.nodes t3 on t3."name" = t1."neid" 
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
                AND t4."name" IN ({})
            """.format(', '.join(placeholders))

            self.db_engine.execute(text(sql).execution_options(autocommit=True), **site_list_placeholders)

        session.close()


    def extract_huawei_3g_cell_params(self):
        """Extract Huawei UMTS cell parameters"""

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

            # print("Extracting cells parameters for site_pk: {0}, site_name: {1}".format(site_pk,site_name))

            sql = """
                INSERT INTO live_network.umts_cells_data
                (pk, date_added, date_modified, added_by, modified_by,bch_power,cell_id,cell_pk,lac,latitude, longitude, 
                maximum_transmission_power, "name", cpich_power, primary_sch_power, scrambling_code, rac, sac, 
                secondary_sch_power, site_pk, tech_pk, vendor_pk, uarfcn_dl,uarfcn_ul, ura_list, azimuth, cell_range, 
                height, site_sector_carrier, mcc,mnc,ura,localcellid)
                SELECT 
                NEXTVAL('live_network.seq_umts_cells_data_pk'),
                t1."varDateTime" as date_added, 
                t1."varDateTime" as date_modified, 
                0 as added_by,
                0 as modified_by,
                t5."BCHPOWER"::integer as bch_power,
                t1."CELLID"::integer,
                t3.pk as cell_pk, -- cellid
                t1."LAC"::integer as lac,
                -- (t4."antennaPosition_latitude"::float/93206.76)*(-1::float*t4."antennaPosition_latitudeSign"::float) 
                null as latitude,
                -- t4."antennaPosition_longitude"::float/46603.38 as longitude,
                null as longitude,
                t1."MAXTXPOWER"::integer as maximum_transmission_power,
                t1."CELLNAME",
                t4."MAXPCPICHPOWER"::integer  as cpich_power,
                t6."PSCHPOWER"::integer as primary_sch_power,
                t1."PSCRAMBCODE"::integer as scrambling_code,
                -- t1."LAC" as lac,
                t1."RAC"::integer,
                t1."SAC"::integer,
                null as secondary_sch_power,
                t3.site_pk, -- site pk
                2, -- umts
                2, -- Huawei
                t1."UARFCNDOWNLINK"::integer,
                t1."UARFCNUPLINK"::integer,
                null as ura_list ,
                null as azimuth, -- azimuth,
                null as cell_range, -- cellrange,
                null as height, -- height
                null as site_sector_carrier,
                t7."MCC" as mcc,
                t7."MNC" as mnc,
                t8."URAID" as ura ,
                t1."LOCELL" as localcellid,
                t1."CELLID" as ci
                FROM 
                hua_cm_3g.ucell t1
                INNER JOIN live_network.cells t3 on t3."name" = t1."CELLNAME" and t3.vendor_pk = 2 and t3.tech_pk = 2
                INNER JOIN hua_cm_3g.upcpich t4 on t4."neid" = t1.neid AND  t4."CELLID" = t1."CELLID" 
                INNER JOIN hua_cm_3g.ubch t5 on t5.neid = t1.neid AND t5."CELLID" = t1."CELLID"
                INNER JOIN hua_cm_3g.upsch t6 on t6.neid = t1.neid ANd t6."CELLID" = t1."CELLID"
                INNER JOIN hua_cm_3g.ucnoperator t7 on t7.neid = t1.neid
                INNER JOIN hua_cm_3g.ucellura t8 on t8.neid = t1.neid AND t8."CELLID" = t1."CELLID"
                WHERE t1."NODEBNAME" = '{0}';
            """.format(site_name)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()


    def extract_huawei_enodebs(self):
        """Extract Ericsson ENodebs"""
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
            FROM hua_cm_4g.enodebfunction t1
            LEFT OUTER  JOIN live_network.sites t2 ON t1."ENODEBFUNCTIONNAME" = t2."name"
            WHERE 
            t2."name" IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()


    def extract_huawei_4g_cells(self):
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
            t1."varDateTime" as date_added, 
            t1."varDateTime" as date_modified, 
            0 as added_by,
            0 as modified_by,
            3, -- tech 3 -lte, 2 -umts, 1-gms
            2, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
            t1."CELLNAME" as name,
            t2.pk -- site primary key
            FROM hua_cm_4g.cell t1
            INNER JOIN live_network.sites t2 on t2."name" = t1."neid" 
                AND t2.vendor_pk = 2 and t2.tech_pk = 3 
            LEFT JOIN live_network.cells t3 on t3."name" = t1."CELLNAME"
                AND t2.vendor_pk = 2 and t2.tech_pk = 3 
            WHERE
                t3."name" IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()


    def extract_huawei_4g_cell_params(self):
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

            # print("Extracting cells parameters for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                        INSERT INTO live_network.lte_cells_data
                        (pk, name, cell_pk, uarfcn_dl, uarfcn_ul, mcc, mnc, tac, pci, ecgi, rach_root_sequence, max_tx_power, latitude, longitude,
                        height, dl_bandwidth, ul_bandwidth, ta, ta_mode, tx_elements, rx_elements, scheduler, azimuth, mechanical_tilt, electrical_tilt, cell_range,
                        site_pk, tech_pk, vendor_pk, modified_by, added_by, date_added, date_modified)
                        SELECT 
                        NEXTVAL('live_network.seq_lte_cells_data_pk'),
                        t1."CELLNAME" as name,
                        t2.pk as cell_pk,
                        t1."DLEARFCN"::integer as uarfcn_dl,
                        t3.dl_freq_low as uarfcn_ul,
                        t6."MCC"::integer as mcc,
                        t6."MNC"::integer as mnc,
                        t4."TAC"::integer as tac,
                        t1."PHYCELLID"::integer as pci,
                        null as ecgi,
                        t1."ROOTSEQUENCEIDX" as rach_root_sequence,
                        null as max_tx_power,
                        null as latitude,
                        null as longitude,
                        null as height,
                        t1."DLBANDWIDTH"::integer as dl_bandwidth,
                        null as ul_bandwidth,
                        null as ta,
                        null as ta_mode,
                        t1."TXRXMODE"::integer as tx_elements, -- @TODO: Conform
                        t1."TXRXMODE"::integer as rx_elements, -- @TODO: Conform
                        t7."DLSCHSTRATEGY"::integer as scheduler,
                        null as azimuth,
                        null as mechanical_tilt,
                        null as electrical_tilt,
                        t1."CELLRADIUS"::integer as cell_range,
                        t2.site_pk as site_pk,
                        t2.tech_pk as tech_pk,
                        t2.vendor_pk as vendor_pk,
                        0 as modified_by, 
                        0 as added_by, 
                        t1."varDateTime" as date_added, 
                        t1."varDateTime" as date_modified
                        FROM hua_cm_4g.cell t1
                        INNER JOIN live_network.cells t2 on t2."name" = t1."CELLNAME" AND t2.vendor_pk = 2 AND t2.tech_pk = 3
                        INNER JOIN public.lte_frequency_bands t3 on t3.band_id = t1."FREQBAND"::integer
                        INNER JOIN hua_cm_4g.cnoperatorta t4 on t4.neid = t1.neid
                        INNER JOIN hua_cm_4g.cnoperator t6 on t6.neid  = t1.neid
                        INNER JOIN hua_cm_4g.celldlschalgo t7 on t7.neid = t1.neid AND t7."LOCALCELLID" = t1."LOCALCELLID"
                        WHERE t1."neid" = '{0}';
                    """.format(site_name)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()


    def extract_huawei_2g2g_nbrs(self):
        """Extract Huawei 2G2G nbr relations"""
        self.extract_huawei_2g2g_nbrs_internal()
        self.extract_huawei_2g2g_nbrs_external()
        self.extract_huawei_2g2g_nbrs_with_other_vendors()

    def extract_huawei_2g2g_nbrs_internal(self):
        """Extract Huawei 2G-2G neighbour relations"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=1).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            # print("Extracting Huawei 2G-2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t4.node_pk as svrnode_pk,
                t3.site_pk as svrsite_pk,
                t3.tech_pk as svrtech_pk,
                t3.vendor_pk as svrvendor_pk,
                t3.pk as svrcell_pk,
                -- nbr side
                t7.node_pk as nbrnode_pk,
                t6.site_pk as nbrsite_pk,
                t6.tech_pk as nbrtech_pk,
                t6.vendor_pk as nbrvendor_pk,
                t6.pk as nbrcell_pk,
                t1."varDateTime" as date_added,
                t1."varDateTime" as date_modified,
                0 as modified_by,
                0 as added_by
                from hua_cm_2g.g2gncell t1
                -- svr side
                INNER JOIN hua_cm_2g.gcell t2 ON t2.neid = t1.neid AND t2."CELLID" = t1."SRC2GNCELLID"
                INNER JOIN live_network.cells t3 ON t3.name = t2."CELLNAME" AND t3.vendor_pk = 2 AND t3.tech_pk = 1
                INNER JOIN live_network.sites t4 ON t4.pk = t3.site_pk AND t4.vendor_pk = 2 AND t4.tech_pk = 1
                -- nbr side
                INNER JOIN hua_cm_2g.gcell t5 on  t5.neid = t1.neid AND t5."CELLID" = t1."NBR2GNCELLID" 
                INNER JOIN live_network.cells t6 ON t6.name = t5."CELLNAME" AND t6.vendor_pk = 2 AND t6.tech_pk = 1
                INNER JOIN live_network.sites t7 ON t7.pk = t6.site_pk AND t7.vendor_pk = 2 AND t7.tech_pk = 1
                WHERE
                 t3.site_pk = '{0}'
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_huawei_2g2g_nbrs_external(self):
        """
        Extract Huawei 2G-2G neighbour relations

        Source cell is Huawei and nbr cell is Huawei but on diffrent BSCs
        """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=1).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            # print("Extracting Huawei 2G-2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

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
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM 
                hua_cm_2g.g2gncell t1
                INNER JOIN hua_cm_2g.gcell t2 ON 
                    t2.neid = t1.neid 
                    AND t1."SRC2GNCELLID" = t2."CELLID"
                LEFT JOIN hua_cm_2g.gext2gcell t3 ON 
                    t3.neid  = t1.neid
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
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_huawei_2g2g_nbrs_with_other_vendors(self):
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

            # print("Extracting Huawei 2G- Ericsson 2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t1.neid as svrnode,
                t2."CELLNAME" as svrcell,
                t3."EXT2GCELLNAME" as nbrcell,
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
                t6.pk as svrcell_pk,
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM 
                hua_cm_2g.g2gncell t1
                INNER JOIN hua_cm_2g.gcell t2 ON 
                    t2.neid = t1.neid 
                    AND t1."SRC2GNCELLID" = t2."CELLID"
                LEFT JOIN hua_cm_2g.gext2gcell t3 ON 
                    t3.neid  = t1.neid
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
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_huawei_3g3g_nbrs(self):
        """Extract Huawei 3G3G relations"""
        self.extract_huawei_3g3g_intrafreq_nbrs_internal()
        self.extract_huawei_3g3g_intrafreq_nbrs_external()
        self.extract_huawei_3g3g_interfreq_nbrs_internal()
        self.extract_huawei_3g3g_interfreq_nbrs_external()

        self.extract_huawei_3g3g_intrafreq_nbrs_with_all_vendors()
        # self.extract_huawei_3g3g_intrafreq_nbrs_with_ericsson()
        # self.extract_huawei_3g3g_interfreq_nbrs_with_ericsson()

    def extract_huawei_3g3g_intrafreq_nbrs_internal(self):
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

            # print("Extracting Huawei 3G- Huawei 2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

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
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM 
                hua_cm_3g.uintrafreqncell t1
                INNER JOIN hua_cm_3g.UCELL t2 on 
                    t2.neid  = t1.neid 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN hua_cm_3g.UCELL t3 on 
                    t3.neid = t1.neid
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

    def extract_huawei_3g3g_intrafreq_nbrs_external(self):
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

            # print("Extracting Huawei 3G- Huawei 2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

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
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM 
                hua_cm_3g.uintrafreqncell t1
                INNER JOIN hua_cm_3g.UCELL t2 on 
                    t2.neid  = t1.neid 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN hua_cm_3g.uext3gcell t3 on 
                    t3.neid = t1.neid
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


    def extract_huawei_3g3g_interfreq_nbrs_internal(self):
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

            # print("Extracting Huawei 3G- Huawei 2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

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
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM 
                hua_cm_3g.uinterfreqncell t1
                INNER JOIN hua_cm_3g.UCELL t2 on 
                    t2.neid  = t1.neid 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN hua_cm_3g.UCELL t3 on 
                    t3.neid = t1.neid
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

    def extract_huawei_3g3g_interfreq_nbrs_external(self):
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

                # print("Extracting Huawei 3G- Huawei 3G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

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
                    t1."varDateTime" ,
                    t1."varDateTime" ,
                    0, -- system
                    0
                    FROM 
                    hua_cm_3g.uinterfreqncell t1
                    INNER JOIN hua_cm_3g.UCELL t2 on 
                        t2.neid  = t1.neid 
                        AND t1."CELLID" = t2."CELLID"
                    INNER JOIN hua_cm_3g.uext3gcell t3 on 
                        t3.neid = t1.neid
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

    def extract_huawei_3g3g_intrafreq_nbrs_with_all_vendors(self):
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

            # print(
            # "Extracting Huawei 3G- Vendor 3G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

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
                t6.pk as svrcell_pk,
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM 
                hua_cm_3g.uintrafreqncell t1
                INNER JOIN hua_cm_3g.UCELL t2 on 
                    t2.neid  = t1.neid 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN hua_cm_3g.UCELL t3 on 
                    t3.neid = t1.neid
                    AND t3."CELLID" = t1."NCELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                -- -----------
                LEFT JOIN hua_cm_3g.uext3gcell t8 on 
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

    def extract_huawei_3g3g_interfreq_nbrs_with_all_vendors(self):
        """
        Extract  Huawei 3G- Huawei 3G neighbour relations on same RNC with diffrent frequencies
        """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=2).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            # print(
            # "Extracting Huawei 3G- Ericsson 3G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

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
                t6.pk as svrcell_pk,
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM 
                hua_cm_3g.uinterfreqncell t1
                INNER JOIN hua_cm_3g.UCELL t2 on 
                    t2.neid  = t1.neid 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                -- ---------
                LEFT JOIN hua_cm_3g.uext3gcell t8 on 
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

    def extract_huawei_3g3g_intrafreq_nbrs_with_ericsson(self):
        """ H// 3G - E// 3G nbrs with the same frequency"""
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

            # print(
            # "Extracting Huawei 3G- Huawei 2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

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
                t6.pk as svrcell_pk,
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM 
                hua_cm_3g.uintrafreqncell t1
                INNER JOIN hua_cm_3g.UCELL t2 on 
                    t2.neid  = t1.neid 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN hua_cm_3g.UCELL t3 on 
                    t3.neid = t1.neid
                    AND t3."CELLID" = t1."NCELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                -- -----------
                INNER JOIN hua_cm_3g.uext3gcell t8 on 
                    t8.neid = t1.neid
                    AND t8."CELLID" = t1."NCELLID"
                -- Vendor=Ericsson and Technology = UMTS
                INNER JOIN live_network.cells t6 ON 
                    t6.name = t8."CELLNAME"
                    AND t6.vendor_pk = 1 AND t6.tech_pk = 2
                INNER JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                    AND t7.vendor_pk = 1 AND t7.tech_pk = 2
                WHERE 
                 t4.site_pk = '{0}'
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_huawei_3g3g_interfreq_nbrs_with_ericsson(self):
        """
        Extract  Huawei 3G- Huawei 3G neighbour relations on same RNC with diffrent frequencies
        """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=2).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            # print(
            # "Extracting Huawei 3G- Ericsson 3G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

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
                t6.pk as svrcell_pk,
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM 
                hua_cm_3g.uinterfreqncell t1
                INNER JOIN hua_cm_3g.UCELL t2 on 
                    t2.neid  = t1.neid 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                -- ---------
                INNER JOIN hua_cm_3g.uext3gcell t8 on 
                    t8.neid = t1.neid
                    AND t8."CELLID" = t1."NCELLID"
                -- Vendor=Ericsson and Technology = UMTS
                INNER JOIN live_network.cells t6 ON 
                    t6.name = t8."CELLNAME"
                    AND t6.vendor_pk = 1 AND t6.tech_pk = 2
                INNER JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                    AND t7.vendor_pk = 1 AND t7.tech_pk = 2
                WHERE 
                 t4.site_pk = '{0}'
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_huawei_2g3g_nbrs_with_all_vendors(self):
        """Extract 2G - 3G nbrs with all vendors"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=1).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            # print(
            # "Extracting Huawei 3G- Ericsson 3G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

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
                t6.pk as svrcell_pk,
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM 
                hua_cm_2g.g3gncell t1
                INNER JOIN hua_cm_2g.gcell t2 on 
                    t2.neid  = t1.neid 
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
                LEFT JOIN hua_cm_2g.gext3gcell t8 on 
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

            # print(
            # "Extracting Huawei 3G- Ericsson 3G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

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
                t4.pk as nbrcell_pk,
                -- nbr side
                t7.node_pk as nbrnode_pk,
                t6.site_pk as nbrsite_pk,
                t6.tech_pk as nbrtech_pk,
                t6.vendor_pk as nbrvendor_pk,
                t6.pk as svrcell_pk,
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM 
                hua_cm_3g.u2gncell t1
                INNER JOIN hua_cm_3g.ucell t2 on 
                    t2.neid  = t1.neid 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                -- nbr-----------
                LEFT JOIN hua_cm_3g.uext2gcell t8 on 
                    t8.neid = t1.neid
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

    def extract_huawei_2g3g_nbrs(self):
        """Extact Huawei 2G-3G nbrs"""
        self.extract_huawei_2g3g_nbrs_with_all_vendors()

    def extract_huawei_3g2g_nbrs(self):
        self.extract_huawei_2g4g_nbrs_with_all_vendors(self)

    def extract_huawei_3g4g_nbrs(self):
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

            # print(
            # "Extracting Huawei 3G - 4G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

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
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM 
                hua_cm_3g.ultencell t1
                INNER JOIN hua_cm_3g.UCELL t2 on 
                    t2.neid  = t1.neid 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                -- ---------
                LEFT JOIN hua_cm_3g.ultecell t8 on 
                    t8.neid = t1.neid
                    AND t8."LTECELLINDEX" = t1."LTECELLINDEX"
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."LTECELLNAME"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                 t4.site_pk = '{0}'
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_huawei_2g4g_nbrs(self):
        pass

    def extract_huawei_4g2g_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=3).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            # print(
            # "Extracting Huawei 4G - 2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

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
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM 
                hua_cm_4g.geranncell t1
                INNER JOIN hua_cm_4g.cell t2 on 
                    t2.neid  = t1.neid 
                    AND t1."LOCALCELLID" = t2."CELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 3
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 3
                -- ---------
                LEFT JOIN hua_cm_4g.geranexternalcell t8 on 
                    t8.neid = t1.neid
                    AND t8."GERANCELLID" = t1."GERANCELLID"
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."CELLNAME"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                 t4.site_pk = '{0}'
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_huawei_4g3g_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=3).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            # print(
            # "Extracting Huawei 4G - 2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

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
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM 
                hua_cm_4g.utranncell t1
                INNER JOIN hua_cm_4g.cell t2 on 
                    t2.neid  = t1.neid 
                    AND t1."LOCALCELLID" = t2."CELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 3
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 3
                -- ---------
                LEFT JOIN hua_cm_4g.utranexternalcell t8 on 
                    t8.neid = t1.neid
                    AND t8."CELLID" = t1."CELLID"
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."CELLNAME"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                 t4.site_pk = '{0}'
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()



    def extract_huawei_4g4g_nbrs(self):
        self.extract_huawei_4g4g_intrafreq_nbrs()
        self.extract_huawei_4g4g_interfreq_nbrs()

    def extract_huawei_4g4g_intrafreq_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=2).filter_by(tech_pk=3).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            # print(
            # "Extracting Huawei 4G - 2G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

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
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM 
                hua_cm_4g.eutranintrafreqncell t1
                INNER JOIN hua_cm_4g.cell t2 on 
                    t2.neid  = t1.neid 
                    AND t1."LOCALCELLID" = t2."CELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 3
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 3
                -- ---------
                LEFT JOIN hua_cm_4g.eutranexternalcell t8 on 
                    t8.neid = t1.neid
                    AND t8."CELLID" = t1."CELLID"
                LEFT JOIN live_network.cells t6 ON 
                    t6.name = t8."CELLNAME"
                LEFT JOIN live_network.sites t7 ON 
                    t7.pk = t6.site_pk 
                WHERE 
                 t4.site_pk = '{0}'
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()


    def extract_huawei_4g4g_interfreq_nbrs(self):
        pass

    def extract_ericsson_2g3g_nbrs(self):
        """Extract Ericsson 2G-2G neighbour relations"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Site = Table('sites', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for site in session.query(Site).filter_by(vendor_pk=1).filter_by(tech_pk=1).yield_per(5):
            (site_pk, site_name) = (site[0], site[1])

            # print("Extracting E// 2G-3G relations for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk,svrtech_pk,svrvendor_pk,svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- serving side
                t4.node_pk as srvnode_pk,
                t2.site_pk as svrsite_pk,
                1 as svrtech_pk,
                1 as svrvendor_pk,
                t2.pk as svrcell_pk,
                -- nbr side
                t5.node_pk as nbrnode_pk,
                t3.site_pk as nbrsite_pk,
                1 as nbrtech_pk,
                1 as nbrvendor_pk,
                t3.pk as nbrcell_pk,
                t1."varDateTime" as date_added,
                t1."varDateTime" as date_modified,
                0 as modified_by,
                0 as added_by
                FROM 
                ericsson_cnaiv2.utran_nrel t1
                INNER JOIN live_network.cells t2 ON t2."name" = t1."CELL_NAME" AND t2.vendor_pk = 1 AND t2.tech_pk = 1
                LEFT JOIN live_network.cells t3 on t3."name" = t1."NREL_NAME" AND t3.tech_pk = 2
                INNER JOIN live_network.sites t4 on t4.pk = t2.site_pk AND  t4.tech_pk = 1
                LEFT JOIN live_network.sites t5 on t5.pk = t3.site_pk AND t5.tech_pk = 2
                LEFT JOIN live_network.relations t6 ON t6.svrcell_pk = t2.pk AND t6.nbrcell_pk = t3.pk
                WHERE  t6.pk IS NULL 
                AND t2.site_pk = '{0}'
            """.format(site_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_2g4g_nbrs(self):
        pass

    def extract_ericsson_3g2g_nbrs(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Cell = Table('cells', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for cell in session.query(Cell).filter_by(vendor_pk=1).filter_by(tech_pk=2).yield_per(5):
            (cell_pk, cell_name) = (cell[0], cell[1])

            # print(
            # "Extracting Ericsson 3G - 2G relations for cell_pk: {0}, cell_name: {1}".format(cell_pk, cell_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- Serving side
                t6.node_pk as svrnode_pk,
                t5.site_pk as svrsite_pk,
                t5.pk as nbrcell_pk,
                t5.tech_pk as nbrtech_pk,
                t5.vendor_pk as nbrvendor_pk,
                -- NBR side
                t4.node_pk as nbrnode_pk,
                t3.site_pk as nbrsite_pk,
                t3.pk as svrcell_pk,
                t3.tech_pk as svrtech_pk,
                t3.vendor_pk as svrvendor_pk,
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM
                ericsson_cm_3g."UtranCell" t1
                INNER JOIN ericsson_cm_3g."GsmRelation" t2 ON 
                    TRIM(t2."SubNetwork_2_id") = TRIM(t1."SubNetwork_2_id")
                    AND TRIM(t2."MeContext_id") = TRIM(t1."MeContext_id")
                    AND TRIM(t2."UtranCell_id") = TRIM(t2."UtranCell_id")
                    AND t2."UtranCell_id" =  '{0}'
                INNER JOIN live_network.cells t5 on t5.name = TRIM(t1."userLabel")
                INNER JOIN live_network.sites t6 on t6.pk = t5.site_pk AND  t6.tech_pk = 1
                -- nbr side
                LEFT JOIN live_network.cells t3 on t3.name = REPLACE(t2."adjacentCell", CONCAT('SubNetwork=',TRIM(t2."SubNetwork_id"),',ExternalGsmCell='),'')
                    AND t3.tech_pk = 1
                LEFT JOIN live_network.sites t4 ON t4.pk = t3.site_pk
                AND t4.tech_pk = 1
                -- INNER JOIN ericsson_cm_3g."ExternalGsmCell" t3 on
                --	CONCAT('SubNework=',t3."SubNetwork_id",',ExternalGsmCell=',t3."userLabel") = t2."adjacentCell"
                WHERE 
                t1."UtranCell_id" = '{0}'
            """.format(cell_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_3g3g_nbrs_with_other_vendors(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        Cell = Table('cells', metadata, autoload=True, autoload_with=self.db_engine, schema="live_network")
        for cell in session.query(Cell).filter_by(vendor_pk=1).filter_by(tech_pk=2).yield_per(5):
            (cell_pk, cell_name) = (cell[0], cell[1])

            # print(
            # "Extracting Ericsson 3G - 3G relations for cell_pk: {0}, cell_name: {1}".format(cell_pk, cell_name))

            sql = """
                INSERT INTO live_network.relations 
                (pk, svrnode_pk,svrsite_pk, svrtech_pk, svrvendor_pk, svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
                SELECT 
                NEXTVAL('live_network.seq_relations_pk'),
                -- Serving side
                t6.node_pk as svrnode_pk,
                t5.site_pk as svrsite_pk,
                t5.pk as nbrcell_pk,
                t5.tech_pk as nbrtech_pk,
                t5.vendor_pk as nbrvendor_pk,
                -- NBR side
                t4.node_pk as nbrnode_pk,
                t3.site_pk as nbrsite_pk,
                t3.pk as svrcell_pk,
                t3.tech_pk as svrtech_pk,
                t3.vendor_pk as svrvendor_pk,
                t1."varDateTime" ,
                t1."varDateTime" ,
                0, -- system
                0
                FROM
                ericsson_cm_3g."UtranCell" t1
                INNER JOIN ericsson_cm_3g."UtranRelation" t2 ON 
                    t2."SubNetwork_2_id" = t1."SubNetwork_2_id"
                    AND t2."MeContext_id" = t1."MeContext_id"
                    AND t2."UtranCell_id" = t2."UtranCell_id"
                    AND t2."UtranCell_id" = '{0}'
                INNER JOIN live_network.cells t5 ON t5.name = t2."UtranCell_id" AND t5.vendor_pk = 1 AND t5.tech_pk = 2
                INNER JOIN live_network.sites t6 on t6.pk = t5.site_pk AND t6.vendor_pk = 1 AND t6.tech_pk = 2
                
                -- nbr side
                LEFT JOIN live_network.cells t3 on 
                    t3.name = REPLACE(t2."adjacentCell", CONCAT('SubNetwork=',TRIM(t1."SubNetwork_id"),',ExternalUtranCell='),'')
                    AND t3.tech_pk = 3
                LEFT JOIN live_network.sites t4 ON t4.pk = t3.site_pk  AND t4.tech_pk = 3
                -- INNER JOIN ericsson_bulkcm.externalutrancell t3 on
                --	CONCAT('SubNework=',t3."SubNetwork_id",',ExternalGsmCell=',t3."userLabel") = t2."adjacentCell"
                WHERE 
                t1."UtranCell_id" = '{0}'
            """.format(cell_pk)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_3g4g_nbrs(self):
        pass

    def extract_ericsson_4g2g_nbrs(self):
        pass

    def extract_ericsson_4g3g_nbrs(self):
        pass

    def detect_format_and_move_ericsson_cm_raw_files(self):
        """Detect """

        # Uncompress files
        os.system("""
            for f in `ls -1  /mediation/data/cm/ericsson/in`
            do 
                f=" /mediation/data/cm/ericsson/in/$f"
                /mediation/bin/uncompress.sh "$f" /mediation/data/cm/ericsson/in
            done 
        """)

        # BulkCM
        os.system("""
            OIFS="$IFS"
            IFS=$'\n'
            for f in `ls -1 /mediation/data/cm/ericsson/in/`
            do 
                f="/mediation/data/cm/ericsson/in/$f"
                [ -f "$f" ] && head -2 "$f" | grep '<bulkCmConfigDataFile ' 2>&1 > /dev/null
                [ $? -eq 0 ] && mv "$f" /mediation/data/cm/ericsson/raw/bulkcm
            done 
        """)

        # CNAIV2
        os.system("""
            OIFS="$IFS"
            IFS=$'\n'
            for f in `ls -1 /mediation/data/cm/ericsson/in/`
            do  
                f="/mediation/data/cm/ericsson/in/$f"
                [ -f "$f" ] && head -1 "$f" | grep '..cnai ' 2>&1 > /dev/null
                [ $? -eq 0 ] && mv "$f" /mediation/data/cm/ericsson/raw/cnaiv2 
            done 
        """)

    def detect_format_and_move_huawei_cm_raw_files(self):
        """Detect Huawei raw files format and move them to the respective <format>_tech folder"""

        # Uncompress files
        os.system("""
            for f in `ls -1  /mediation/data/cm/huawei/in/`
            do 
                f="/mediation/data/cm/huawei/in/$f"
                /mediation/bin/uncompress.sh "$f" /mediation/data/cm/huawei/in
            done 
        """)

        # Gexport GSM
        os.system("""
            OIFS="$IFS"
            IFS=$'\n'
            for f in `ls -1 /mediation/data/cm/huawei/in/`
            do 
                f="/mediation/data/cm/huawei/in/$f"
                [ -f "$f" ] && head -10 "$f" | grep 'object technique="GSM"' 2>&1 > /dev/null
                [ $? -eq 0 ] && mv "$f" /mediation/data/cm/huawei/raw/gexport_gsm 
            done 
        """)

        # Gexport UMTS
        os.system("""
            OIFS="$IFS"
            IFS=$'\n'
            for f in `ls -1 /mediation/data/cm/huawei/in/`
            do 
                f="/mediation/data/cm/huawei/in/$f"
                [ -f "$f" ] && head -10 "$f" | grep 'object technique="WCDMA"' 2>&1 > /dev/null
                [ $? -eq 0 ] && mv "$f" /mediation/data/cm/huawei/raw/gexport_wcdma 
            done 
        """)

        # Gexport LTE
        os.system("""
            OIFS="$IFS"
            IFS=$'\n'
            for f in `ls -1 /mediation/data/cm/huawei/in/`
            do 
                f="/mediation/data/cm/huawei/in/$f"
                [ -f "$f" ] && head -10 "$f" | grep 'object technique="LTE"' 2>&1 > /dev/null
                [ $? -eq 0 ] && mv "$f" /mediation/data/cm/huawei/raw/gexport_lte 
            done 
        """)

        # Gexport CDMA
        os.system("""
            OIFS="$IFS"
            IFS=$'\n'
            for f in `ls -1 /mediation/data/cm/huawei/in/`
            do 
                f="/mediation/data/cm/huawei/in/$f"
                [ -f "$f" ] && head -10 "$f" | grep 'object technique="CDMA"' 2>&1 > /dev/null
                [ $? -eq 0 ] && mv "$f" /mediation/data/cm/huawei/raw/gexport_cdma 
            done 
        """)

        # Gexport SRAN
        os.system("""
            OIFS="$IFS"
            IFS=$'\n'
            for f in `ls -1 /mediation/data/cm/huawei/in/`
            do 
                f="/mediation/data/cm/huawei/in/$f"
                [ -f "$f" ] && head -10 "$f" | grep 'object technique="SRAN"' 2>&1 > /dev/null
                [ $? -eq 0 ] && mv "$f" /mediation/data/cm/huawei/raw/gexport_sran
            done 
        """)

        # Gexport Other
        os.system("""
            OIFS="$IFS"
            IFS=$'\n'
            for f in `ls -1 /mediation/data/cm/huawei/in/`
            do 
                f="/mediation/data/cm/huawei/in/$f"
                [ -f "$f" ] && head -10 "$f" | grep 'object technique="" ' 2>&1 > /dev/null
                [ $? -eq 0 ] && mv "$f" /mediation/data/cm/huawei/raw/gexport_other
            done 
        """)

        # NBI GSM
        os.system("""
            OIFS="$IFS"
            IFS=$'\n'
            for f in `ls -1 /mediation/data/cm/huawei/in/`
            do 
                f="/mediation/data/cm/huawei/in/$f"
                [ -f "$f" ] && head -5 "$f" | grep 'xsi:schemaLocation="http://www.huawei.com/specs/SOM CMEGBSS_NRM_Spec' 2>&1 > /dev/null
                [ $? -eq 0 ] && mv "$f" /mediation/data/cm/huawei/raw/nbi_gsm
            done 
        """)

        # NBI UMTS
        os.system("""
            OIFS="$IFS"
            IFS=$'\n'
            for f in `ls -1 /mediation/data/cm/huawei/in/`
            do 
                f="/mediation/data/cm/huawei/in/$f"
                [ -f "$f" ] && head -5 "$f" | grep 'xsi:schemaLocation="http://www.huawei.com/specs/SOM CMEWRAN_NRM_Spec' 2>&1 > /dev/null
                [ $? -eq 0 ] && mv "$f" /mediation/data/cm/huawei/raw/nbi_umts
            done 
        """)

        # NBI LTE
        os.system("""
            OIFS="$IFS"
            IFS=$'\n'
            for f in `ls -1 /mediation/data/cm/huawei/in/`
            do 
                f="/mediation/data/cm/huawei/in/$f"
                [ -f "$f" ] && head -5 "$f" | grep 'xsi:schemaLocation="http://www.huawei.com/specs/SRAN CMELTE_NRM_Spec' 2>&1 > /dev/null
                [ $? -eq 0 ] && mv "$f" /mediation/data/cm/huawei/raw/nbi_lte
            done 
        """)

        # NBI SRAN Multi-RAT
        os.system("""
            OIFS="$IFS"
            IFS=$'\n'
            for f in `ls -1 /mediation/data/cm/huawei/in/`
            do 
                f="/mediation/data/cm/huawei/in/$f"
                [ -f "$f" ] && head -5 "$f" | grep 'xmlns="http://www.huawei.com/specs/SRAN" xsi:schemaLocation="http://www.huawei.com/specs/SRAN CMEMRAT_NRM_Spec' 2>&1 > /dev/null
                [ $? -eq 0 ] && mv "$f" /mediation/data/cm/huawei/raw/nbi_sran
            done 
        """)

        # NBI SRAN
        os.system("""
            OIFS="$IFS"
            IFS=$'\n'
            for f in `ls -1 /mediation/data/cm/huawei/in/`
            do 
                f="/mediation/data/cm/huawei/in/$f"
                [ -f "$f" ] && head -5 "$f" | grep 'xmlns="http://www.huawei.com/specs/SRAN" xsi:schemaLocation="http://www.huawei.com/specs/SRAN CMEUMTS_NRM_Spec' 2>&1 > /dev/null
                [ $? -eq 0 ] && mv "$f" /mediation/data/cm/huawei/raw/nbi_sran
            done 
        """)

        # NBI SRAN
        os.system("""
            OIFS="$IFS"
            IFS=$'\n'
            for f in `ls -1 /mediation/data/cm/huawei/in/`
            do 
                f="/mediation/data/cm/huawei/in/$f"
                [ -f "$f" ] && head -5 "$f" | grep 'xmlns="http://www.huawei.com/specs/SRAN" xsi:schemaLocation="http://www.huawei.com/specs/SRAN "' 2>&1 > /dev/null
                [ $? -eq 0 ] && mv "$f" /mediation/data/cm/huawei/raw/nbi_sran
            done 
        """)

        # Huawei 2G MML
        os.system("""
            OIFS="$IFS"
            IFS=$'\n'
            for f in `ls -1 /mediation/data/cm/huawei/in/`
            do 
                f="/mediation/data/cm/huawei/in/$f"
                [ -f "$f" ] && head -20 "$f" | grep ' BSCBASIC:' 2>&1 > /dev/null
                [ $? -eq 0 ] && mv "$f" /mediation/data/cm/huawei/raw/mml_gsm
            done 
        """)

        # Huawei 3G MML
        os.system("""
            OIFS="$IFS"
            IFS=$'\n'
            for f in `ls -1 /mediation/data/cm/huawei/in/`
            do 
                f="/mediation/data/cm/huawei/in/$f"
                [ -f "$f" ] && head -20 "$f" | grep ' URNCBASIC:' 2>&1 > /dev/null
                [ $? -eq 0 ] && mv "$f" /mediation/data/cm/huawei/raw/mml_umts
            done 
        """)

        # Huawei Baseline Synch Dump
        os.system("""
            OIFS="$IFS"
            IFS=$'\n'
            for f in `ls -1 /mediation/data/cm/huawei/in/`
            do 
                f="/mediation/data/cm/huawei/in/$f"
                [ -f "$f" ] && head -5 "$f" | grep 'bulkcm_xml_baseline_syn' 2>&1 > /dev/null
                [ $? -eq 0 ] && mv "$f" /mediation/data/cm/huawei/raw/cfgsyn
            done 
        """)