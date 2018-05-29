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
            t1."varDateTime" as date_added, 
            t1."varDateTime" as date_modified, 
            'BSC' as node_type,
            t1."neid" as "name" , 
            2 as vendor_pk, -- 1=Ericsson, 2=Huawei
            1 as tech_pk , -- 1=gsm, 2-umts,3=lte
            0 as added_by,
            0 as modified_by
            FROM huawei_cm_2g."BSCBASIC" t1
            LEFT OUTER  JOIN live_network.nodes t2 ON t1."neid" = t2."name"
            WHERE 
            t2."name" IS NULL
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
             "varDateTime" as date_added, 
             "varDateTime" as date_modified, 
             'RNC' as node_type,
             "neid" as "name" , 
             2 as vendor_pk, -- 1=Ericsson, 2=Huawei, 3-ZTE
             2 as tech_pk , -- 1=gsm, 2-umts,3=lte
             0 as added_by,
             0 as modified_by
             FROM huawei_cm_3g."URNCBASIC" t1
             LEFT OUTER  JOIN live_network.nodes t2 ON t1."neid" = t2."name"
             WHERE 
             t2."name" IS NULL
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
             "varDateTime" as date_added, 
             "varDateTime" as date_modified, 
             3 as tech_pk , -- 1=gsm, 2-umts,3=lte,
             2 as vendor_pk, -- 1=Ericsson, 2=Huawei
             t1."ENODEBFUNCTIONNAME",
             0 as added_by,
             0 as modified_by 
             FROM
             huawei_cm_4g."ENODEBFUNCTION" t1
             LEFT OUTER  JOIN live_network.sites t2 ON t1."ENODEBFUNCTIONNAME" = t2."name"
             WHERE 
             t2."name" IS NULL
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
            from huawei_cm_2g."BTS" t1
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

    def extract_live_network_2g_cells(self):
        """Extract Huawei 2G cells """
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
            FROM huawei_cm_2g."GCELL" t1
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
                         t1."MCC"::integer,
                         t1."MNC"::integer,
                         0 as modified_by,
                         0 as added_by,
                         t1."varDateTime" as date_added,
                         t1."varDateTime" as date_modified            
                         FROM huawei_cm_2g."GCELL" t1             
                         INNER JOIN live_network.cells t2 on t2."name" = t1."CELLNAME" AND t2.vendor_pk = 2 AND t2.tech_pk = 1
                         INNER JOIN huawei_cm_2g."GCELLBASICPARA" t3 on t3."CELLID" = t1."CELLID" AND t3.neid = t1.neid 
                         INNER JOIN huawei_cm_2g."GTRX" t4 on t4."neid" = t1.neid AND t4."CELLID" = t1."CELLID"
                         INNER JOIN live_network.sites t5 on t5.pk = t2.site_pk
                         INNER JOIN huawei_cm_2g."GCELLLCS" t6 on t6.neid = t1.neid AND t6."CELLID" = t1."CELLID"
                         INNER JOIN huawei_cm_2g."CELLBIND2BTS" t7 on t7."CELLID" = t1."CELLID" AND t6.neid = t1.neid
                         WHERE 
                         t5."name" ='{0}'
                        -- AND trim(t1.module_type) = 'Radio'
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
                from hua_cm_2g."G2GNCELL" t1
                -- svr side
                INNER JOIN hua_cm_2g."GCELL" t2 ON t2.neid = t1.neid AND t2."CELLID" = t1."SRC2GNCELLID"
                INNER JOIN live_network.cells t3 ON t3.name = t2."CELLNAME" AND t3.vendor_pk = 2 AND t3.tech_pk = 1
                INNER JOIN live_network.sites t4 ON t4.pk = t3.site_pk AND t4.vendor_pk = 2 AND t4.tech_pk = 1
                -- nbr side
                INNER JOIN hua_cm_2g."GCELL" t5 on  t5.neid = t1.neid AND t5."CELLID" = t1."NBR2GNCELLID" 
                INNER JOIN live_network.cells t6 ON t6.name = t5."CELLNAME" AND t6.vendor_pk = 2 AND t6.tech_pk = 1
                INNER JOIN live_network.sites t7 ON t7.pk = t6.site_pk AND t7.vendor_pk = 2 AND t7.tech_pk = 1
                WHERE
                 t3.site_pk = '{0}'
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
                 hua_cm_2g."G2GNCELL" t1
                 INNER JOIN hua_cm_2g."GCELL" t2 ON 
                     t2.neid = t1.neid 
                     AND t1."SRC2GNCELLID" = t2."CELLID"
                 LEFT JOIN hua_cm_2g."GEXT2GCELL" t3 ON 
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
                hua_cm_2g."G2GNCELL" t1
                INNER JOIN hua_cm_2g."GCELL" t2 ON 
                    t2.neid = t1.neid 
                    AND t1."SRC2GNCELLID" = t2."CELLID"
                LEFT JOIN hua_cm_2g."GEXT2GCELL" t3 ON 
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

    def extract_live_network_2g2g_nbrs(self):
        self.extract_live_network_2g2g_nbrs_internal()
        self.extract_live_network_2g2g_nbrs_external()

    def extract_live_network_2g3g_nbrs(self):
        pass

    def extract_live_network_2g4g_nbrs(self):
        pass

    def extract_live_network_3g2g_nbrs(self):
        pass

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
                hua_cm_3g."UINTRAFREQNCELL" t1
                INNER JOIN hua_cm_3g."UCELL" t2 on 
                    t2.neid  = t1.neid 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN hua_cm_3g."UCELL" t3 on 
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
                hua_cm_3g."UINTRAFREQNCELL" t1
                INNER JOIN hua_cm_3g."UCELL" t2 on 
                    t2.neid  = t1.neid 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN hua_cm_3g."UEXT3GCELL" t3 on 
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
                hua_cm_3g."UINTERFREQNCELL" t1
                INNER JOIN hua_cm_3g."UCELL" t2 on 
                    t2.neid  = t1.neid 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN hua_cm_3g."UCELL" t3 on 
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
                    hua_cm_3g."UINTERFREQNCELL" t1
                    INNER JOIN hua_cm_3g."UCELL" t2 on 
                        t2.neid  = t1.neid 
                        AND t1."CELLID" = t2."CELLID"
                    INNER JOIN hua_cm_3g."UEXT3GCELL" t3 on 
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
                hua_cm_3g."UINTERFREQNCELL" t1
                INNER JOIN hua_cm_3g."UCELL" t2 on 
                    t2.neid  = t1.neid 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN hua_cm_3g."UCELL" t3 on 
                    t3.neid = t1.neid
                    AND t3."CELLID" = t1."NCELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                -- -----------
                LEFT JOIN hua_cm_3g."UEXT3GCELL" t8 on 
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
                hua_cm_3g."UINTERFREQNCELL" t1
                INNER JOIN hua_cm_3g."UCELL" t2 on 
                    t2.neid  = t1.neid 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                -- ---------
                LEFT JOIN hua_cm_3g."UEXT3GCELL" t8 on 
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
                INNER JOIN hua_cm_2g."GCELL" t2 on 
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
                LEFT JOIN hua_cm_2g."GEXT3GCELL" t8 on 
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
                INNER JOIN hua_cm_3g."UCELL" t2 on 
                    t2.neid  = t1.neid 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                -- nbr-----------
                LEFT JOIN hua_cm_3g."UEXT2GCELL" t8 on 
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

    def extract_live_network_3g4g_nbrs(self):
        pass

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
                hua_cm_4g."GERANNCELL" t1
                INNER JOIN hua_cm_4g."CELL" t2 on 
                    t2.neid  = t1.neid 
                    AND t1."LOCALCELLID" = t2."CELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 3
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 3
                -- ---------
                LEFT JOIN hua_cm_4g."GERANEXTERNALCELL" t8 on 
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
                 INNER JOIN hua_cm_4g."CELL" t2 on 
                     t2.neid  = t1.neid 
                     AND t1."LOCALCELLID" = t2."CELLID"
                 INNER JOIN live_network.cells t4 ON 
                     t4.name = t2."CELLNAME" 
                     AND t4.vendor_pk = 2 AND t4.tech_pk = 3
                 INNER JOIN live_network.sites t5 ON 
                     t5.pk = t4.site_pk 
                     AND t5.vendor_pk = 2 AND t5.tech_pk = 3
                 -- ---------
                 LEFT JOIN hua_cm_4g."UTRANEXTERNALCELL" t8 on 
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
                 hua_cm_4g."EUTRANINTRAFREQNCELL" t1
                 INNER JOIN hua_cm_4g."CELL" t2 on 
                     t2.neid  = t1.neid 
                     AND t1."LOCALCELLID" = t2."CELLID"
                 INNER JOIN live_network.cells t4 ON 
                     t4.name = t2."CELLNAME" 
                     AND t4.vendor_pk = 2 AND t4.tech_pk = 3
                 INNER JOIN live_network.sites t5 ON 
                     t5.pk = t4.site_pk 
                     AND t5.vendor_pk = 2 AND t5.tech_pk = 3
                 -- ---------
                 LEFT JOIN hua_cm_4g."EUTRANEXTERNALCELL" t8 on 
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

    def extract_live_network_4g4g_interfreq_nbrs(self):
        pass

    def extract_live_network_4g4g_nbrs(self):
        self.extract_live_network_4g4g_intrafreq_nbrs()
        self.extract_live_network_4g4g_interfreq_nbrs()

    def extract_live_network_2g2g_nbrs_params(self):
        pass

    def extract_live_network_2g3g_nbrs_params(self):
        pass

    def extract_live_network_2g4g_nbrs_params(self):
        pass

    def extract_live_network_3g2g_nbrs(self):
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
                hua_cm_4g."EUTRANINTRAFREQNCELL" t1
                INNER JOIN hua_cm_3g."UCELL" t2 on 
                    t2.neid  = t1.neid 
                    AND t1."CELLID" = t2."CELLID"
                INNER JOIN hua_cm_3g."UCELL" t3 on 
                    t3.neid = t1.neid
                    AND t3."CELLID" = t1."NCELLID"
                INNER JOIN live_network.cells t4 ON 
                    t4.name = t2."CELLNAME" 
                    AND t4.vendor_pk = 2 AND t4.tech_pk = 2
                INNER JOIN live_network.sites t5 ON 
                    t5.pk = t4.site_pk 
                    AND t5.vendor_pk = 2 AND t5.tech_pk = 2
                -- -----------
                LEFT JOIN hua_cm_3g."UEXT3GCELL" t8 on 
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
            t1."varDateTime" as date_added, 
            t1."varDateTime" as date_modified, 
            0 as added_by,
            0 as modified_by,
            2, -- tech 3 -lte, 2 -umts, 1-gms
            2, -- 1- Ericsson, 2 - Huawei,
            t1."NODEBNAME",
            t2.pk -- node primary key
            from huawei_cm_3g."UNODEB" t1
            INNER join live_network.nodes t2 on t2."name" = t1."neid" 
                AND t2.vendor_pk = 2 and t2.tech_pk = 2
            LEFT JOIN live_network.sites t3 on t3."name" = t1."NODEBNAME"
               AND t2.vendor_pk = 2 and t2.tech_pk = 2
            WHERE 
            t3."name" IS NULL

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
                 t1."varDateTime" as date_added, 
                 t1."varDateTime" as date_modified, 
                 0 as added_by,
                 0 as modified_by,
                 2, -- tech 3 -lte, 2 -umts, 1-gms
                 2, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
                 t1."CELLNAME" AS name,
                 t4.pk -- site primary key
                 FROM hua_cm_3g."UCELL" t1
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
                INNER JOIN hua_cm_3g."UPCPICH" t4 on t4."neid" = t1.neid AND  t4."CELLID" = t1."CELLID" 
                INNER JOIN hua_cm_3g."UBCH" t5 on t5.neid = t1.neid AND t5."CELLID" = t1."CELLID"
                INNER JOIN hua_cm_3g."UPSCH" upsch t6 on t6.neid = t1.neid ANd t6."CELLID" = t1."CELLID"
                INNER JOIN hua_cm_3g."UCNOPERATOR" t7 on t7.neid = t1.neid
                INNER JOIN hua_cm_3g."UCELLURA" t8 on t8.neid = t1.neid AND t8."CELLID" = t1."CELLID"
                WHERE t1."NODEBNAME" = '{0}';
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
            t1."varDateTime" as date_added, 
            t1."varDateTime" as date_modified, 
            0 as added_by,
            0 as modified_by,
            3, -- tech 3 -lte, 2 -umts, 1-gms
            2, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
            t1."CELLNAME" as name,
            t2.pk -- site primary key
            FROM huawei_cm_4g."CELL" t1
            INNER JOIN live_network.sites t2 on t2."name" = t1."neid" 
                AND t2.vendor_pk = 2 and t2.tech_pk = 3 
            LEFT JOIN live_network.cells t3 on t3."name" = t1."CELLNAME"
                AND t2.vendor_pk = 2 and t2.tech_pk = 3 
            WHERE
                t3."name" IS NULL
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
                        FROM huawei_cm_4g."CELL" t1
                        INNER JOIN live_network.cells t2 on t2."name" = t1."CELLNAME" AND t2.vendor_pk = 2 AND t2.tech_pk = 3
                        INNER JOIN public.lte_frequency_bands t3 on t3.band_id = t1."FREQBAND"::integer
                        INNER JOIN huawei_cm_4g."CNOPERATORTA" t4 on t4.neid = t1.neid
                        INNER JOIN huawei_cm_4g."CNOPERATOR" t6 on t6.neid  = t1.neid
                        INNER JOIN huawei_cm_4g."CELLDLSCHALGO" t7 on t7.neid = t1.neid AND t7."LOCALCELLID" = t1."LOCALCELLID"
                        WHERE t1."neid" = '{0}';
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
        NEXTVAL('live_network.seq_gsm_external_cells_pk') as pk,
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
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        huawei_cm_2g."GEXT2GCELL" t1
        LEFT JOIN live_network.nodes t2 ON t2."name" = t1."neid"
        LEFT JOIN live_network.cells t3 on t3."name" = t1."EXT2GCELLNAME"
        LEFT JOIN live_network.gsm_external_cells t4 on t4."name" = t1."EXT2GCELLNAME" 
            AND t4.lac = t1."LAC"::integer
            AND t4.ci = t1."CI"::integer
        WHERE 
        t4.pk IS NULL
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
        NEXTVAL('live_network.seq_gsm_external_cells_pk') as pk,
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
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        huawei_cm_3g."UEXT2GCELL" t1
        LEFT JOIN live_network.nodes t2 ON t2."name" = t1."neid"
        LEFT JOIN live_network.cells t3 on t3."name" = t1."GSMCELLNAME"
        LEFT JOIN live_network.gsm_external_cells t4 on t4."name" = t1."GSMCELLNAME" 
            AND t4.lac = t1."LAC"::integer
            AND t4.ci = t1."CID"::integer
        WHERE 
        t4.pk IS NULL
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
            NEXTVAL('live_network.seq_gsm_external_cells_pk') as pk,
            t1."CELLNAME" AS "name",
            t3.pk AS cell_pk,
            t2.pk AS node_pk,
            t1."MCC"::integer AS mcc,
            t1."MNC"::integer AS mnc,
            t1."LAC"::integer AS lac,
            t1."GERANARFCN"::integer AS bcch,
            t1."RAC"::integer AS rac,
            0 as modified_by,
            0 as added_by,
            now()::timestamp as date_added,
            now()::timestamp as date_modified
            FROM
            huawei_cm_4g."GERANEXTERNALCELL" t1
            LEFT JOIN live_network.nodes t2 ON t2."name" = t1."neid"
            LEFT JOIN live_network.cells t3 on t3."name" = t1."CELLNAME"
            LEFT JOIN live_network.gsm_external_cells t4 on t4."name" = t1."CELLNAME" 
                AND t4.lac = t1."LAC"::integer
            WHERE 
            t4.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g_externals_on_2g(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.umts_external_cells
        (pk, name, cell_pk,  mcc, mnc, lac, rncid, ci, psc, dl_uarfcn, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_umts_external_cells_pk') as pk,
        t1."EXT3GCELLNAME" AS "name",
        t3.pk AS cell_pk,
        t1."MCC"::integer AS mcc,
        t1."MNC"::integer AS mnc,
        t1."LAC"::integer AS lac,
        t1."RNCID"::integer as rncid,
        t1."CI"::integer as ci,
        t1."SCRAMBLE"::integer AS psc,
        t1."FDDARFCN"::integer AS dl_uarfcn,
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        huawei_cm_2g."GEXT3GCELL" t1
        LEFT JOIN live_network.nodes t2 ON t2."name" = t1."neid"
        LEFT JOIN live_network.cells t3 on t3."name" = t1."EXT3GCELLNAME"
        LEFT JOIN live_network.umts_external_cells t4 on t4."name" = t1."EXT3GCELLNAME" 
            AND t4.lac = t1."LAC"::integer
            AND t4.ci = t1."CI"::integer
        WHERE 
        t4.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g_externals_on_3g(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.umts_external_cells
        (pk, name, cell_pk,  mcc, mnc, lac, rac, rncid, ci, psc, dl_uarfcn, ul_uarfcn, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_umts_external_cells_pk') as pk,
        t1."CELLNAME" AS "name",
        t3.pk AS cell_pk,
        t1."MCC"::integer AS mcc,
        t1."MNC"::integer AS mnc,
        t1."LAC"::integer AS lac,
        t1."RAC"::integer AS rac,
        t1."RNCID"::integer as rncid,
        t1."CELLID"::integer as ci,
        t1."PSCRAMBCODE"::integer AS psc,
        t1."UARFCNDOWNLINK"::integer AS dl_uarfcn,
        t1."UARFCNUPLINK"::integer AS ul_uarfcn,
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        huawei_cm_3g."UEXT3GCELL" t1
        LEFT JOIN live_network.nodes t2 ON t2."name" = t1."neid"
        LEFT JOIN live_network.cells t3 on t3."name" = t1."CELLNAME"
        LEFT JOIN live_network.umts_external_cells t4 on t4."name" = t1."CELLNAME" 
            AND t4.lac = t1."LAC"::integer
        WHERE 
        t4.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g_externals_on_4g(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.lte_external_cells
        (pk, name, cell_pk,  mcc, mnc, lac, rac, rncid, ci, psc, dl_uarfcn, ul_uarfcn, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_umts_external_cells_pk') as pk,
        t1."CELLNAME" AS "name",
        t3.pk AS cell_pk,
        t1."MCC"::integer AS mcc,
        t1."MNC"::integer AS mnc,
        t1."LAC"::integer AS lac,
        t1."RAC"::integer AS rac,
        t1."RNCID"::integer as rncid,
        t1."CELLID"::integer as ci,
        t1."UTRANDLARFCN"::integer AS dl_uarfcn,
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        huawei_cm_4g."UTRANEXTERNALCELL" t1
        LEFT JOIN live_network.nodes t2 ON t2."name" = t1."neid"
        LEFT JOIN live_network.cells t3 on t3."name" = t1."CELLNAME"
        LEFT JOIN live_network.lte_external_cells t4 on t4."name" = t1."CELLNAME" 
            AND t4.lac = t1."LAC"::integer
        WHERE 
        t4.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_4g_externals_on_2g(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.lte_external_cells
        (pk, name, cell_pk, mcc, mnc, pci, dl_earfcn, ci, tac, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_gsm_external_cells_pk') as pk,
        t1."EXTLTECELLNAME" AS "name",
        t3.pk AS cell_pk,
        t2.pk AS node_pk,
        t1."MCC"::integer AS mcc,
        t1."MNC"::integer AS mnc,
        t1."PCID"::integer AS pci,
        t1."FREQ"::integer AS dl_earfcn_dl,
        t1."EXTLTECELLID"::integer AS ci,
        t1."TAC" as tac,
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        huawei_cm_2g."GEXTLTECELL" t1
        LEFT JOIN live_network.cells t3 on t3."name" = t1."EXTLTECELLNAME"
        LEFT JOIN live_network.lte_external_cells t4 on t4."name" = t1."EXTLTECELLNAME" 
            AND t4.ci = t1."localCellId"
        WHERE 
        t4.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_4g_externals_on_3g(self):
        """
        @todo: Can't find the MO name for this"""
        pass

    def extract_live_network_4g_externals_on_4g(self):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.lte_external_cells
            (pk, name, cell_pk, mcc, mnc, pci, dl_earfcn, ci, tac, modified_by, added_by, date_added, date_modified)
            SELECT 
            NEXTVAL('live_network.seq_gsm_external_cells_pk') as pk,
            t1."CELLNAME" AS "name",
            t3.pk AS cell_pk,
            t2.pk AS node_pk,
            t1."MCC"::integer AS mcc,
            t1."MNC"::integer AS mnc,
            t1."PHYCELLID"::integer AS pci,
            t1."DELEARFCN"::integer AS dl_earfcn,
            t1."CELLID"::integer AS ci,
            t1."TAC" as tac,
            0 as modified_by,
            0 as added_by,
            now()::timestamp as date_added,
            now()::timestamp as date_modified
            FROM
            huawei_cm_4g."EUTRANEXTERNALCELL" t1
            LEFT JOIN live_network.cells t3 on t3."name" = t1."EXTLTECELLNAME"
            LEFT JOIN live_network.lte_external_cells t4 on t4."name" = t1."EXTLTECELLNAME" 
            WHERE 
            t4.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()
