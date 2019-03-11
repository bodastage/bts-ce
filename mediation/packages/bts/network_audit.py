import psycopg2
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import os


class NetworkAudit(object):

    def __init__(self):
        ''' Constructor for this class. '''

        sqlalchemy_db_uri = 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(
            os.getenv("BTS_DB_USER", "bodastage"),
            os.getenv("BTS_DB_PASS", "password"),
            os.getenv("BTS_DB_HOST", "database"),
            os.getenv("BTS_DB_PORT", "5432"),
            os.getenv("BTS_DB_NAME", "bts"),
        )

        self.engine = create_engine(sqlalchemy_db_uri)

    def parameter_baseline_ericsson_umts(self):
        """Compute baseline parameter discrepancies for Ericsson 3G"""

        # Vendor parameter to pseudo parameter map
        # @TODO: Add these mappings in db
        vendor_pseudo_parameter_map = {}
        vendor_pseudo_parameter_map['UtranCell-'+'bchPower'] = 'bch_power'
        vendor_pseudo_parameter_map['UtranCell-' + 'lac'] = 'lac'
        vendor_pseudo_parameter_map['UtranCell-' + 'primaryCpichPower'] = 'cpich_power'
        vendor_pseudo_parameter_map['UtranCell-' + 'primaryScramblingCode'] = 'scrambling_code'
        vendor_pseudo_parameter_map['UtranCell-' + 'maximumTransmissionPower'] = 'maximum_transmission_power'

        # Get baseline values
        # Configure this list in a table
        sql = """
            SELECT t3."name" as mo,  t2."name" as parameter, t1."value", t3.pk as mo_pk, t2.pk as parameter_pk FROM 
            live_network.base_line_values t1
            inner join vendor_parameters t2 on t2.pk = t1.parameter_pk
            inner join managedobjects t3 on t3.pk = t2.parent_pk
            where 
            t3."name" = 'UtranCell' AND t2."name" in ('bchPower','primaryCpichPower','primaryScramblingCode')
        """

        self.engine.execute(text(sql).execution_options(autocommit=True))

        result = self.engine.execute(sql)

        for row in result:
            # print(row)
            # print("row['parameter']: {}".format(row['parameter']))
            vendor_parameter = row['parameter']
            mo = row['mo']
            baseline_value = row['value']
            pseudo_parameter = vendor_pseudo_parameter_map[row['mo'] + '-'+row['parameter']]

            parameter_sql = """
                SELECT t6."name" AS node_name, 
                t5."name" AS site_name, 
                t4."name" AS cell_name,
                t2."name" AS parameter, 
                t3."name" AS mo,
                t1."{3}" AS network_value,
                '{2}' AS baseline_value,
                t7."name" AS technology,
                t8."name" AS vendor
                FROM live_network.umts_cells_data t1
                INNER JOIN vendor_parameters t2 ON t2."name" = '{0}'
                INNER JOIN managedobjects t3 ON t3.pk = t2.parent_pk and t3."name" = '{1}'
                INNER JOIN live_network.cells t4 ON t4.pk = t1.cell_pk 
                INNER JOIN live_network.sites t5 ON t5.pk = t4.site_pk
                INNER JOIN live_network.nodes t6 ON t6.pk = t5.node_pk
                INNER JOIN technologies t7 ON t7.pk = t4.tech_pk
                INNER JOIN vendors t8 ON t8.pk = t4.vendor_pk
                WHERE 
                t1."bch_power" != {2}
                AND t1.tech_pk = 2
                AND t4.tech_pk = 2
                AND t6.tech_pk = 2
                AND t3.tech_pk = 2
                LIMIT 5
            """.format(vendor_parameter, mo, baseline_value, pseudo_parameter)

            r = self.engine.execute(parameter_sql)

            for rw in r:
                print(rw)
                pseudo_parameter = rw['parameter']
                managed_object = row['mo']
                vendor_parameter = row['parameter']
                network_value = rw['network_value']
                baseline_value = rw['baseline_value']
                vendor = rw['vendor']
                technology = rw['technology']
                node_name = rw['node_name']
                site_name = rw['site_name']
                cell_name = rw['cell_name']

                insert_sql = """
                INSERT INTO network_audit.baseline_parameter_discrepancies(
                pk, pseudo_parameter, managed_object, vendor_parameter, network_value, baseline_value, vendor, technology, date_added, added_by, date_modified, modified_by, node_name, site_name, cell_name)
                VALUES ( NEXTVAL('network_audit.seq_baseline_parameter_discrepancies_pk'), 
                '{0}', '{1}', '{2}', '{3}', '{4}', '{5}','{6}', now()::timestamp, 0, now()::timestamp, 0, '{7}', '{8}', '{9}')
                """.format(pseudo_parameter, managed_object, vendor_parameter, network_value, baseline_value, vendor, technology, node_name, site_name, cell_name)

                self.engine.execute(text(insert_sql).execution_options(autocommit=True))

    def generate_incosistent_gsm_externals(self):
        """
        GSM externals where the external cell parameters don't match the internal cell parameters
        """
        Session = sessionmaker(bind=self.engine)
        session = Session()

        sql = """
            INSERT INTO 
            network_audit.incosistent_2g_externals
            (nodename, ext_vendor, int_vendor, int_cellname, ext_mnc, ext_mcc, ext_bcc, ext_ncc, ext_bcch, ext_lac,
            int_mnc, int_mcc, int_bcc, int_ncc, int_bcch, int_lac, age, date_added, date_modified, added_by, modified_by)
            SELECT 
            t5."name" as nodename,
            t6."name" as ext_vendor,
            t7."name" as int_vendor,
            
            t1."name" as int_cellname,
            -- externals values
            t1.mnc::integer as ext_mnc,
            t1.mcc::integer as ext_mcc,
            t1.bcc::integer as ext_bcc, 
            t1.ncc::integer as ext_ncc,
            t1.bcch::integer as ext_bcch,
            t1.lac::integer as ext_lac,
            
            -- internal values
            t2.mnc::integer as int_mnc,
            t2.mcc::integer as int_mcc,
            t2.bcc::integer as int_bcc,
            t2.ncc::integer as int_ncc,
            t2.bcch::integer as int_bcch,
            t2.lac::integer as inter_lac,
            datediff( 'day', COALESCE(t4.date_added, t1.date_added)::DATE, COALESCE(t4.date_modified, t1.date_added)::DATE ) as age,
            COALESCE(t4.date_added, NOW()::DATE) as date_added,
            COALESCE(t4.date_modified, NOW()::DATE) as date_modified , 
            0 as added_by,
            0 as modified_by
            FROM 
            live_network.gsm_external_cells t1
            INNER JOIN live_network.gsm_cells_data t2
                ON t2.cell_pk = t1.cell_pk
            INNER JOIN live_network.nodes t5 on t5.pk = t1.node_pk
            INNER JOIN vendors t6 ON t6.pk = t5.vendor_pk
            INNER JOIN live_network.cells t3 
                ON t3.pk = t1.cell_pk
            INNER JOIN vendors t7 on t7.pk = t3.vendor_pk
            LEFT JOIN network_audit.incosistent_2g_externals t4 
                ON t4.int_cellname = t1."name"
            WHERE 
            t1.mnc != t2.mnc
            OR t1.mcc != t2.mcc
            OR t1.bcc != t2.bcc
            OR t1.ncc != t2.ncc
            OR t1.bcch::integer != t2.bcch::integer
            OR t1.lac != t2.lac
            ON CONFLICT ON CONSTRAINT unique_incosistent_2g_externals
            DO
            UPDATE SET age = DATEDIFF( 'day', COALESCE(network_audit.incosistent_2g_externals.date_added)::DATE, COALESCE(EXCLUDED.date_modified)::DATE ) ,
                       date_modified = network_audit.incosistent_2g_externals.date_modified
        """

        self.engine.execute(sql)

        # Delete inconsistencies that nolonger exist in external 2G cells
        sql = """
            DELETE FROM network_audit.incosistent_2g_externals t1
            WHERE t1."int_cellname" IN (
                SELECT t1.name
                FROM 
                live_network.gsm_external_cells t1
                INNER JOIN live_network.gsm_cells_data t2
                    ON t2.cell_pk = t1.cell_pk
                INNER JOIN live_network.nodes t5 on t5.pk = t1.node_pk
                INNER JOIN vendors t6 ON t6.pk = t5.vendor_pk
                INNER JOIN live_network.cells t3 
                    ON t3.pk = t1.cell_pk
                INNER JOIN vendors t7 on t7.pk = t3.vendor_pk
                LEFT JOIN network_audit.incosistent_2g_externals t4 
                    ON t4.int_cellname = t1."name"
                WHERE 
                t1.mnc = t2.mnc
                OR t1.mcc = t2.mcc
                OR t1.bcc = t2.bcc
                OR t1.ncc = t2.ncc
                OR t1.bcch::integer = t2.bcch::integer
                OR t1.lac = t2.lac
            )
        """
        self.engine.execute(sql)
        session.close()

    def generate_incosistent_umts_externals(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()

        sql = """
            INSERT INTO 
            network_audit.incosistent_3g_externals
            (nodename, ext_vendor, int_vendor, ext_cellname, ext_mnc, ext_mcc, ext_dl_uarfcn, ext_rac, ext_lac, ext_psc,
            int_mnc, int_mcc, int_dl_uarfcn, int_rac, int_lac, int_psc, age, date_added, date_modified, added_by, modified_by)
            SELECT 
            t5."name" as nodename,
            t6."name" as ext_vendor,
            t7."name" as int_vendor,
            
            t1."name" as ext_cellname,
            -- externals values
            t1.mnc as ext_mnc,
            t1.mcc as ext_mcc,
            t1.uarfcn_dl as ext_dl_uarfcn, 
            t1.rac as ext_rac,
            t1.lac as ext_lac,
            t1.psc as ext_psc,
            
            -- internal values
            t2.mnc as int_mnc,
            t2.mcc as int_mcc,
            t2.uarfcn_dl as int_dl_uarfcn,
            t2.rac as int_rac,
            t2.lac as int_lac,
            t2.scrambling_code as int_psc,
            datediff( 'day', COALESCE(t4.date_added, t1.date_added)::DATE, COALESCE(t4.date_modified, t1.date_added)::DATE ) as age,
            COALESCE(t4.date_added, now()::date) as date_added,
            COALESCE(t4.date_modified, now()::date) as date_modified , 
            0 as added_by,
            0 as modified_by
            FROM 
            live_network.umts_external_cells t1
            INNER JOIN live_network.umts_cells_data t2
                ON t2.cell_pk = t1.cell_pk
            INNER JOIN live_network.nodes t5 on t5.pk = t1.node_pk
            INNER JOIN vendors t6 ON t6.pk = t5.vendor_pk
            INNER JOIN live_network.cells t3 
                ON t3.pk = t1.cell_pk
            INNER JOIN vendors t7 on t7.pk = t3.vendor_pk
            LEFT JOIN network_audit.incosistent_3g_externals t4 
                ON t4.ext_cellname = t1."name"
            WHERE 
            t1.mnc != t2.mnc
            OR t1.mcc != t2.mcc
            OR t1.rac != t2.rac
            OR t1.lac != t2.lac
            OR t1.uarfcn_dl::integer != t2.uarfcn_dl::integer
            OR t1.psc != t2.scrambling_code
            ON CONFLICT ON CONSTRAINT unique_incosistent_3g_externals
            DO
            UPDATE SET age = DATEDIFF( 'day', COALESCE(network_audit.incosistent_3g_externals.date_added)::DATE, COALESCE(EXCLUDED.date_modified)::DATE ) ,
                       date_modified = network_audit.incosistent_3g_externals.date_modified
        """

        self.engine.execute(sql)

        # Delete inconsistencies that nolonger exist in external 2G cells
        sql = """
            DELETE FROM network_audit.incosistent_3g_externals t1
            WHERE t1."ext_cellname" IN (
                SELECT t1.name
                FROM 
                live_network.umts_external_cells t1
                INNER JOIN live_network.umts_cells_data t2
                    ON t2.cell_pk = t1.cell_pk
                INNER JOIN live_network.nodes t5 on t5.pk = t1.node_pk
                INNER JOIN vendors t6 ON t6.pk = t5.vendor_pk
                INNER JOIN live_network.cells t3 
                    ON t3.pk = t1.cell_pk
                INNER JOIN vendors t7 on t7.pk = t3.vendor_pk
                LEFT JOIN network_audit.incosistent_3g_externals t4 
                    ON t4.ext_cellname = t1."name"
                WHERE 
                        t1.mnc = t2.mnc
                        OR t1.mcc = t2.mcc
                        OR t1.rac = t2.rac
                        OR t1.lac = t2.lac
                        OR t1.uarfcn_dl::integer = t2.uarfcn_dl::integer
                        OR t1.psc = t2.scrambling_code
            )
        """
        self.engine.execute(sql)
        session.close()

    def generate_incosistent_lte_externals(self):
        """
        Generate incosistent LTE external cells
        """
        Session = sessionmaker(bind=self.engine)
        session = Session()

        sql = """
            INSERT INTO 
            network_audit.incosistent_4g_externals
            (pk, nodename, ext_vendor, int_vendor, ext_cellname, ext_mnc, ext_mcc, ext_dl_earfcn, ext_pci,
            int_mnc, int_mcc, int_dl_earfcn, int_pci, age, date_added, date_modified, added_by, modified_by)
            SELECT 
            NEXTVAL('network_audit.seq_incosistent_4g_externals_pk') as pk,
            t5."name" as nodename,
            t6."name" as ext_vendor,
            t7."name" as int_vendor,
            
            t1."name" as ext_cellname,
            -- externals values
            t1.mnc as ext_mnc,
            t1.mcc as ext_mcc,
            t1.dl_earfcn as ext_dl_earfcn, 
            t1.pci as ext_pci,
            
            -- internal values
            t2.mnc as int_mnc,
            t2.mcc as int_mcc,
            t2.dl_earfcn as int_dl_earfcn,
            t2.PCI as int_pci,
            datediff( 'day', COALESCE(t4.date_added, t1.date_added)::DATE, COALESCE(t4.date_modified, t1.date_added)::DATE ) as age,
            COALESCE(t4.date_added, now()::date) as date_added,
            COALESCE(t4.date_modified, now()::date) as date_modified , 
            0 as added_by,
            0 as modified_by
            FROM 
            live_network.lte_external_cells t1
            INNER JOIN live_network.lte_cells_data t2
                ON t2.cell_pk = t1.cell_pk
            INNER JOIN live_network.nodes t5 on t5.pk = t1.node_pk
            INNER JOIN vendors t6 ON t6.pk = t5.vendor_pk
            INNER JOIN live_network.cells t3 
                ON t3.pk = t1.cell_pk
            INNER JOIN vendors t7 on t7.pk = t3.vendor_pk
            LEFT JOIN network_audit.incosistent_3g_externals t4 
                ON t4.ext_cellname = t1."name"
            WHERE 
            t1.mnc != t2.mnc
            OR t1.mcc != t2.mcc
            OR t1.pci != t2.pci
            OR t1.dl_earfcn != t2.dl_earfcn
            ON CONFLICT ON CONSTRAINT unique_incosistent_4g_externals
            DO
            UPDATE SET age = DATEDIFF( 'day', COALESCE(network_audit.incosistent_4g_externals.date_added)::DATE, COALESCE(EXCLUDED.date_modified)::DATE ) ,
                       date_modified = network_audit.incosistent_4g_externals.date_modified

        """

        self.engine.execute(sql)

        # Delete inconsistencies that nolonger exist in external 2G cells
        sql = """
            DELETE FROM network_audit.incosistent_4g_externals t1
            WHERE t1."ext_cellname" IN (
                SELECT t1.name
                FROM 
                live_network.lte_external_cells t1
                INNER JOIN live_network.lte_cells_data t2
                    ON t2.cell_pk = t1.cell_pk
                INNER JOIN live_network.nodes t5 on t5.pk = t1.node_pk
                INNER JOIN vendors t6 ON t6.pk = t5.vendor_pk
                INNER JOIN live_network.cells t3 
                    ON t3.pk = t1.cell_pk
                INNER JOIN vendors t7 on t7.pk = t3.vendor_pk
                LEFT JOIN network_audit.incosistent_3g_externals t4 
                    ON t4.ext_cellname = t1."name"
                WHERE 
                t1.mnc = t2.mnc
                OR t1.mcc = t2.mcc
                OR t1.pci = t2.pci
                OR t1.dl_earfcn = t2.dl_earfcn
            )
        """
        self.engine.execute(sql)
        session.close()

    def generate_missing_one_way_relations(self):
        """
        Generate missing opposite relations. For examle, if a relation from A to B exists and from B to A
        doesn't, then B to A is reported as a mmising one way relation
        """
        Session = sessionmaker(bind=self.engine)
        session = Session()

        sql = """
            INSERT INTO network_audit.missing_one_way_relations
            (svrvendor, svrtech, svrnode, svrsite, svrcell, nbrvendor, nbrtech, nbrsite, nbrcell, age, modified_by, added_by, date_added, date_modified)
            select 
            t14.name as svrvendor,
            t13.name as svrtech,
            t12.name as svrnode,
            t11.name as svrsite,
            t01.name as svrcell, 
            
            t24.name as nbrvendor,
            t23.name as nbrtech,
            -- t22.name as nbrnode,
            t21.name as nbrsite,
            t02.name as nbrcell,
            
            --
            0 AS age,
            0 as modified_by,
            0 as added_by,
            now()::timestamp as date_added,
            now()::timestamp as date_modified
            from live_network.relations t1
            INNER JOIN live_network.cells t01 on t01.pk = t1.svrcell_pk
            INNER JOIN live_network.sites t11 on t11.pk = t1.svrsite_pk
            INNER JOIN live_network.nodes t12 on t12.pk = t1.svrnode_pk
            INNER JOIN technologies t13 on t13.pk = t12.tech_pk
            INNER JOIN vendors t14 on t14.pk = t12.vendor_pk
            -- 
            INNER JOIN live_network.cells t02 on t02.pk = t1.nbrcell_pk
            INNER JOIN live_network.sites t21 on t21.pk = t1.nbrsite_pk
            INNER JOIN live_network.nodes t22 on t22.pk = t1.nbrnode_pk
            INNER JOIN technologies t23 on t23.pk = t22.tech_pk
            INNER JOIN vendors t24 on t24.pk = t22.vendor_pk
            where 
            (t1.nbrcell_pk, t1.svrcell_pk)
            NOT IN (
                SELECT svrcell_pk, nbrcell_pk from live_network.relations
            )
            ON CONFLICT ON CONSTRAINT unique_missing_one_way_relations
            DO
            UPDATE SET age = DATEDIFF( 'day', COALESCE(network_audit.missing_one_way_relations.date_added)::DATE, COALESCE(EXCLUDED.date_modified)::DATE ) ,
                       date_modified = network_audit.missing_one_way_relations.date_modified
        """

        self.engine.execute(sql)

        # Delete old
        sql = """
            DELETE FROM 
            network_audit.missing_one_way_relations t1
            WHERE 
            (svrvendor, svrtech, svrnode, svrsite, svrcell, nbrvendor, nbrtech, nbrsite, nbrcell)
            NOT IN  (
                select 
                t14.name as svrvendor,
                t13.name as svrtech,
                t12.name as svrnode,
                t11.name as svrsite,
                t01.name as svrcell, 

                t24.name as nbrvendor,
                t23.name as nbrtech,
                -- t22.name as nbrnode,
                t21.name as nbrsite,
                t02.name as nbrcell
                from live_network.relations t1
                INNER JOIN live_network.cells t01 on t01.pk = t1.svrcell_pk
                INNER JOIN live_network.sites t11 on t11.pk = t1.svrsite_pk
                INNER JOIN live_network.nodes t12 on t12.pk = t1.svrnode_pk
                INNER JOIN technologies t13 on t13.pk = t12.tech_pk
                INNER JOIN vendors t14 on t14.pk = t12.vendor_pk
                -- 
                INNER JOIN live_network.cells t02 on t02.pk = t1.nbrcell_pk
                INNER JOIN live_network.sites t21 on t21.pk = t1.nbrsite_pk
                INNER JOIN live_network.nodes t22 on t22.pk = t1.nbrnode_pk
                INNER JOIN technologies t23 on t23.pk = t22.tech_pk
                INNER JOIN vendors t24 on t24.pk = t22.vendor_pk
                where 
                (t1.nbrcell_pk, t1.svrcell_pk)
                NOT IN (
                SELECT svrcell_pk, nbrcell_pk from live_network.relations
                )
            )
        """
        self.engine.execute(sql)
        session.close()


    def generate_missing_cosite_relations(self):
        """
        Generate missing cosite relation
        """
        Session = sessionmaker(bind=self.engine)
        session = Session()

        sql = """
            INSERT INTO network_audit.missing_cosite_relations
            (pk, svrvendor, svrtech, svrnode, svrsite, svrcell, nbrvendor, nbrtech, nbrnode, nbrsite, nbrcell,
            age, date_added, date_modified)
            SELECT
            NEXTVAL('network_audit.seq_missing_cosite_relations_pk') AS pk,
            t5.name AS svrvendor,
            t6.name AS svrtech,
            t7.name AS svrnode,
            t1.name AS svrsite,
            t2.name AS svrcell,
            
            -- nbr 
            t8.name AS nbrvendor,
            t9.name AS nbrtech,
            t7.name AS nbrnode,
            t1.name AS nbrsite,
            t3.name AS nbrcell,
            datediff( 'day', COALESCE(t10.date_added, t1.date_added)::DATE, COALESCE(t1.date_added, t1.date_added)::DATE ) AS age,
            COALESCE(t2.date_added, t2.date_added::date) AS date_added,
            COALESCE(t2.date_modified, t2.date_added::date) AS date_modified
            FROM 
            
            live_network.sites t1
            INNER JOIN live_network.cells t2 on t2.site_pk = t1.pk 
            INNER JOIN live_network.cells t3 on t3.site_pk = t1.pk
            LEFT JOIN live_network.relations t4 
                ON t4.svrsite_pk = t1.pk 
                AND t4.nbrsite_pk = t1.pk 
                AND t2.pk = t4.svrcell_pk
                AND t3.pk = t4.nbrcell_pk
            -- 
            INNER JOIN vendors t5 ON t5.pk = t2.vendor_pk
            INNER JOIN technologies t6 ON t6.pk = t2.tech_pk
            INNER JOIN live_network.nodes t7 on t7.pk = t1.node_pk
            -- 
            INNER JOIN vendors t8 ON t8.pk = t3.vendor_pk
            INNER JOIN technologies t9 ON t9.pk = t3.tech_pk
            -- 
            LEFT JOIN network_audit.missing_cosite_relations t10 on t10.svrcell = t2.name AND t10.nbrcell = t3.name
            WHERE 
            t2.site_pk = t3.site_pk
            -- AND t1.pk = 1
            AND t2.pk != t3.pk
            AND t4.pk IS NULL
            ON CONFLICT ON CONSTRAINT unique_missing_cosite_relations
            DO
            UPDATE SET age = DATEDIFF( 'day', COALESCE(network_audit.missing_cosite_relations.date_added)::DATE, COALESCE(EXCLUDED.date_modified)::DATE ) ,
                       date_modified = network_audit.missing_cosite_relations.date_modified
        """
        self.engine.execute(sql)

        # Delete old
        sql = """
             DELETE FROM 
            network_audit.missing_cosite_relations t1
            WHERE 
            (svrvendor, svrtech, svrnode, svrsite, svrcell, nbrvendor, nbrtech, nbrsite, nbrcell)
            NOT IN  (
                select 
                t5.name as svrvendor,
                t6.name as svrtech,
                t7.name as svrnode,
                t1.name as svrsite,
                t2.name as svrcell, 

                t8.name as nbrvendor,
                t9.name as nbrtech,
                -- t22.name as nbrnode,
                t1.name as nbrsite,
                t1.name as nbrcell
                from             
                live_network.sites t1
                INNER JOIN live_network.cells t2 on t2.site_pk = t1.pk 
                INNER JOIN live_network.cells t3 on t3.site_pk = t1.pk
                LEFT JOIN live_network.relations t4 
                    ON t4.svrsite_pk = t1.pk 
                    AND t4.nbrsite_pk = t1.pk 
                    AND t2.pk = t4.svrcell_pk
                    AND t3.pk = t4.nbrcell_pk
                -- 
                INNER JOIN vendors t5 ON t5.pk = t2.vendor_pk
                INNER JOIN technologies t6 ON t6.pk = t2.tech_pk
                INNER JOIN live_network.nodes t7 on t7.pk = t1.node_pk
                -- 
                INNER JOIN vendors t8 ON t8.pk = t3.vendor_pk
                INNER JOIN technologies t9 ON t9.pk = t3.tech_pk
                -- 
                LEFT JOIN network_audit.missing_cosite_relations t10 on t10.svrcell = t2.name AND t10.nbrcell = t3.name
                WHERE 
                t2.site_pk = t3.site_pk
                -- AND t1.pk = 1
                AND t2.pk != t3.pk
                AND t4.pk IS NOT NULL
            )
        """
        self.engine.execute(sql)

        session.close()

    def generate_redundant_externals(self):
        """
        Generate external definitions that exist in the live network but there are no matching internal cells
        :return:
        """
