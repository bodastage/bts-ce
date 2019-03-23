import psycopg2
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import os
import logging

class NokiaCM(object):

    def __init__(self):
        ''' Constructor for this class. '''

        #@TODO: Refactor
        sqlalchemy_db_uri = 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(
            os.getenv("BTS_DB_USER", "bodastage"),
            os.getenv("BTS_DB_PASS", "password"),
            os.getenv("BTS_DB_HOST", "database"),
            os.getenv("BTS_DB_PORT", "5432"),
            os.getenv("BTS_DB_NAME", "bts"),
        )

        self.db_engine = create_engine(sqlalchemy_db_uri)

        self.logger = logging.getLogger('network-baseline')
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def extract_live_network_bscs(self):
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
            TRIM(t1."name") AS "name" , 
            4 AS vendor_pk, -- 1=Ericsson, 2=Huawei
            1 AS tech_pk , -- 1=gsm, 2-umts,3=lte
            0 AS added_by,
            0 AS modified_by
            FROM nokia_cm."BSC" t1
             INNER JOIN cm_loads t3 on t3.pk = t1."LOADID"
            LEFT OUTER  JOIN live_network.nodes t2 ON TRIM(t1."name") = t2."name"
            WHERE 
            t2."name" IS NULL
             ON CONFLICT ON CONSTRAINT unique_nodes
             DO NOTHING
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

    def extract_live_network_rncs(self):
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
             TRIM(t1."name") AS "name" , 
             4 AS vendor_pk, -- 1=Ericsson, 2=Huawei, 3-ZTE
             2 AS tech_pk , -- 1=gsm, 2-umts,3=lte
             0 AS added_by,
             0 AS modified_by
             FROM nokia_cm."RNC" t1
             INNER JOIN cm_loads t3 on t3.pk = t1."LOADID"
             LEFT OUTER  JOIN live_network.nodes t2 ON TRIM(t1."name") = t2."name"
             WHERE 
             t2."name" IS NULL
             AND t3.is_current_load = true
             ON CONFLICT ON CONSTRAINT unique_nodes
             DO NOTHING
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_enodebs(self):
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
             4 AS vendor_pk, -- 1=Ericsson, 2=Huawei
             TRIM(t1."name"),
             0 AS added_by,
             0 AS modified_by 
             FROM
             nokia_cm."LNBTS" t1
             INNER JOIN cm_loads t3 on t3.pk = t1."LOADID"
             LEFT OUTER  JOIN live_network.sites t2 ON TRIM(t1."name") = t2."name"
             WHERE 
             t2."name" IS NULL
              AND t3.is_current_load = true
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_2g_sites(self):
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
            4 AS vendor_pk, -- 1- Ericsson, 2 - Huawei, 3 - zte, 4-nokika, etc...
            CONCAT(TRIM(t1."name"),'(',TRIM(t1."lapdLinkName"),')') AS "name",
            t3.pk as node_pk -- node primary key
            from nokia_cm."BCF" t1
            INNER JOIN cm_loads t5 on t5.pk = t1."LOADID"
            INNER JOIN nokia_cm."BSC" t2 ON t2."FILENAME" = t1."FILENAME" 
                AND t2."LOADID" = t1."LOADID" 
                AND SUBSTRING(t1."DISTNAME",'(BSC-\d+)') =   SUBSTRING(t2."DISTNAME",'(BSC-\d+)')
            INNER join live_network.nodes t3 on TRIM( t3."name") = TRIM(t2."name")
                AND t3.vendor_pk = 4 and t3.tech_pk = 1
            LEFT JOIN live_network.sites t4 on t4."name" = CONCAT(TRIM(t1."name"),'(',TRIM(t1."lapdLinkName"),')')
                AND t4.vendor_pk = 4 and t4.tech_pk = 1
            WHERE 
            t4."name" IS NULL
            AND t5.is_current_load = true
            ON CONFLICT ON CONSTRAINT uq_site
            DO NOTHING
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))
        session.close()

    def extract_live_network_2g_cells(self):
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
            1 AS tech_pk, -- tech 3 -lte, 2 -umts, 1-gms
            4 AS vendor_pk, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
            CONCAT(TRIM(t1."name"),'(',TRIM(t1."cellId"),')') AS name,
            t4.pk as site_pk-- site primary key
            FROM nokia_cm."BTS" t1
            -- LOAD
            INNER JOIN cm_loads t8 on t8.pk = t1."LOADID"
            INNER JOIN nokia_cm."BCF" t9 ON 
                t9."FILENAME" = t1."FILENAME" 
                AND t9."LOADID" = t1."LOADID" 
                AND CONCAT(TRIM(t9."DISTNAME"), '/BTS-',TRIM(t1."segmentId")) = TRIM(t1."DISTNAME")
            INNER JOIN live_network.sites t4 ON 
            t4."name" = CONCAT(TRIM(t9."name"),'(',TRIM(t9."lapdLinkName"),')')
                AND t4.vendor_pk = 4
                AND t4.tech_pk = 1
            LEFT JOIN live_network.cells t5 on  t5."name" = CONCAT(TRIM(t1."name"),'(',TRIM(t1."cellId",')'))
                AND t5.tech_pk = 1
                AND t5.vendor_pk = 4
            WHERE
            t5."name" IS NULL
            AND t8.is_current_load = true
            ON CONFLICT ON CONSTRAINT uq_live_cells
            DO NOTHING
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
            4, -- 1- Ericsson, 2 - Huawei, 3-ZTE, 4-Nokia
            TRIM(t1."name" ) as name,
            t5.pk as node_pk
            FROM
            nokia_cm."WBTS" t1
            INNER JOIN cm_loads t2 ON 
                t2.pk = t1."LOADID"
            INNER JOIN nokia_cm."RNC" t3 ON
                t3."LOADID" = t1."LOADID"
                AND t3."DISTNAME" = SUBSTRING(t1."DISTNAME", '.*RNC-\d+')
                AND t3."FILENAME" = t1."FILENAME"
            INNER JOIN live_network.nodes t5 ON t5."name" = TRIM(t3."name")
            LEFT JOIN live_network.sites t4 on t4."name" = TRIM(t1."name")
                AND t4.vendor_pk = 4 and t4.tech_pk = 2
            WHERE 
            t4."name" IS NULL
            AND t2.is_current_load = true
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
        sites = session.query(Site).filter_by(vendor_pk=4).filter_by(tech_pk=2).all()

        self.logger.info("Extracting live network 3G cells for Nokia...")
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

            placeholders = []
            site_list_placeholders = {}
            for r in range(placeholder_range):
                placeholders.append(':p' + str(r))
                site_list_placeholders['p' + str(r)] = site_list[r]


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
                 2 AS tech_pk, -- tech 3 -lte, 2 -umts, 1-gms
                 4 AS vendor_pk, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
                 CONCAT(TRIM(t1."name"),'(',TRIM(t1."CId"),')') AS name,
                 t4.pk as site_pk-- site primary key
                 FROM nokia_cm."WCEL" t1
                 INNER JOIN cm_loads t6 ON
                 	t6.pk = t1."LOADID"
                 INNER JOIN nokia_cm."WBTS" t7 on 
                 	t7."FILENAME" = t1."FILENAME" 
                    AND t7."LOADID" = t1."LOADID"
                    AND TRIM(t7."DISTNAME") = SUBSTRING(t1."DISTNAME",'.*WBTS-\d+')
                 INNER JOIN nokia_cm."RNC" t8  ON
                 	t8."FILENAME" = t1."FILENAME"
                    AND t8."LOADID" = t1."LOADID"
                    AND TRIM(t8."DISTNAME") = SUBSTRING(t1."DISTNAME", '.*RNC-\d+')
                 INNER JOIN live_network.nodes t3 on t3."name" = TRIM(t8."name" )
                         AND t3.vendor_pk = 4
                         AND t3.tech_pk = 2
                 INNER JOIN live_network.sites t4 on t4."name" = TRIM(t7."name")
                     AND t4.vendor_pk = 4
                     AND t4.tech_pk = 2
                     AND t4.node_pk = t3.pk
                 LEFT JOIN live_network.cells t5 on t5."name" = CONCAT(TRIM(t1."name"),'(',TRIM(t1."CId"),')')
                     AND t5.tech_pk = 2
                     AND t5.vendor_pk = 4
                 WHERE 
                 t4."name" IN ({})
                 AND t6.is_current_load = true
             """.format(', '.join(placeholders))

            self.db_engine.execute(text(sql).execution_options(autocommit=True), **site_list_placeholders)

        self.logger.info("Completed extraction of live network Nokia 3G cells")

    def extract_live_network_3g_cells_params(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        # Truncate paramete table
        # self.db_engine.execute(text("TRUNCATE TABLE live_network.umts_cells_data").execution_options(autocommit=True))
        # self.db_engine.execute(text("ALTER SEQUENCE live_network.seq_umts_cells_data_pk RESTART WITH 1;").
        #                       execution_options(autocommit=True))

        # The data is alot. Let's handle per site
        site_sql = """SELECT pk, "name" from live_network.sites where vendor_pk  = 4 and tech_pk = 2"""

        result = self.db_engine.execute(site_sql)

        for row in result:
            (site_pk, site_name) = row

            self.logger.info("Extracting cells parameters for site_pk: {0}, site_name: {1}".format(site_pk, site_name))

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
                NULL AS bchpower, -- t5."BCHPOWER"::integer AS bch_power,
                t1."CId"::integer,
                t4.pk AS cell_pk, -- cellid
                t1."LAC"::integer AS lac,
                -- (t4."antennaPosition_latitude"::float/93206.76)*(-1::float*t4."antennaPosition_latitudeSign"::float) 
                null AS latitude,
                -- t4."antennaPosition_longitude"::float/46603.38 AS longitude,
                null AS longitude,
                t1."PtxCellMax"::float AS maximum_transmission_power,
               CONCAT(TRIM(t1."name"),'(',TRIM(t1."CId"),')') as cell_name,
                t1."PtxPrimaryCPICH"::float  AS cpich_power,
                t1."PtxPrimarySCH"::float AS primary_sch_power,
                t1."PriScrCode"::integer AS scrambling_code,
                -- t1."LAC" AS lac,
                t1."RAC"::integer,
                t1."SAC"::integer,
                t1."PtxSecSCH"::float AS secondary_sch_power,
                t4.site_pk, -- site pk
                2, -- umts
                4, -- Nokia
                t1."UARFCN"::integer as dl_uarfcn,
                null as ul_uarfcn, -- t1."UARFCNUPLINK"::integer,
                t1."URAId" AS ura_list ,
                null AS azimuth, -- azimuth,
                null AS cell_range, -- cellrange,
                null AS height, -- height
                null AS site_sector_carrier,
                t1."WCELMCC"::integer AS mcc,
                t1."WCELMNC"::integer AS mnc,
                t1."URAId" AS ura ,
                NULL AS locelcellid, -- t1."LOCELL"::integer AS localcellid,
                t1."CId"::integer AS ci
                FROM 
                nokia_cm."WCEL" t1
                INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
                INNER JOIN nokia_cm."WBTS" t3 on 
                    t3."FILENAME" = t1."FILENAME" 
                    AND t3."LOADID" = t1."LOADID"
                    AND TRIM(t3."DISTNAME") = SUBSTRING(t1."DISTNAME",'.*WBTS-\d+')
                LEFT JOIN live_network.cells t4 ON 
                	t4.name = CONCAT(TRIM(t1."name"),'(',TRIM(t1."CId"),')')
                WHERE TRIM(t3."name") = '{0}'
                AND t2.is_current_load = true
            """.format(site_name)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_4g_cells(self):
        pass

    def extract_live_network_2g_externals_on_2g(self):
        pass

    def extract_live_network_2g_externals_on_3g(self):
        pass

    def extract_live_network_2g_externals_on_4g(self):
        pass

    def extract_live_network_3g_externals_on_2g(self):
        pass

    def extract_live_network_3g_externals_on_3g(self):
        pass

    def extract_live_network_3g_externals_on_4g(self):
        pass

    def extract_live_network_4g_externals_on_2g(self):
        pass

    def extract_live_network_4g_externals_on_3g(self):
        pass

    def extract_live_network_4g_externals_on_4g(self):
        pass

    def extract_live_network_externals_on_2g(self):
        self.extract_live_network_2g_externals_on_2g()
        self.extract_live_network_3g_externals_on_2g()
        self.extract_live_network_4g_externals_on_2g()

    def extract_live_network_externals_on_3g(self):
        self.extract_live_network_2g_externals_on_3g()
        self.extract_live_network_3g_externals_on_3g()
        self.extract_live_network_4g_externals_on_3g()

    def extract_live_network_externals_on_4g(self):
        self.extract_live_network_2g_externals_on_4g()
        self.extract_live_network_3g_externals_on_4g()
        self.extract_live_network_4g_externals_on_4g()

    def extract_live_network_4g_cells_params(self):
        pass

    def extract_live_network_2g2g_nbrs(self):
        pass

    def extract_live_network_2g3g_nbrs(self):
        pass

    def extract_live_network_2g4g_nbrs(self):
        pass

    def extract_live_network_3g2g_nbrs(self):
        pass

    def extract_live_network_3g3g_nbrs(self):
        pass

    def extract_live_network_3g4g_nbrs(self):
        pass

    def extract_live_network_4g2g_nbrs(self):
        pass

    def extract_live_network_4g3g_nbrs(self):
        pass

    def extract_live_network_4g4g_nbrs(self):
        pass
