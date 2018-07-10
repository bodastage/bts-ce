import psycopg2
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text


class NetworkAudit(object):

    def __init__(self):
        ''' Constructor for this class. '''

        # engine = create_engine('postgresql://bodastage:password@database/bts')
        self.engine = create_engine('postgresql://bodastage:password@192.168.99.100/bts')

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

    def incosistent_gsm_externals(self):
        """
        GSM externals where the external cell parameters don't match the internal cell parameters
        """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO 
            network_audit.incosistent_2g_externals
            (nodename, ext_vendor, int_vendor, ext_cellname, ext_mnc, ext_mcc, ext_bcc, ext_ncc, ext_bcch, ext_lac,
            int_mnc, int_mcc, int_bcc, int_ncc, int_bcch, int_lac, age, date_added, date_modified, added_by, modified_by)
            SELECT 
            t5."name" as nodename,
            t6."name" as ext_vendor,
            t7."name" as int_vendor,
            
            t1."name" as ext_cellname,
            -- externals values
            t1.mnc as ext_mnc,
            t1.mcc as ext_mcc,
            t1.bcc as ext_bcc, 
            t1.ncc as ext_ncc,
            t1.bcch as ext_bcch,
            t1.lac as ext_lac,
            
            -- internal values
            t2.mnc as int_mnc,
            t2.mcc as int_mcc,
            t2.bcc as int_bcc,
            t2.ncc as int_ncc,
            t2.bcch as int_bcch,
            t2.lac as inter_lac,
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
                ON t4.ext_cellname = t1."name"
            WHERE 
            t1.mnc != t2.mnc
            OR t1.mcc != t2.mcc
            OR t1.bcc != t2.bcc
            OR t1.ncc != t2.ncc
            OR t1.bcch::integer != t2.bcch::integer
            OR t1.lac != t2.lac

        """

        self.engine.execute(sql)

        # Delete inconsistencies that nolonger exist in external 2G cells
        sql = """
            DELETE FROM 
            network_audit.incosistent_2g_externals t1
            WHERE 
            t1."ext_cellname" IN (
            SELECT "name" FROM live_network.gsm_external_cells t2 
                WHERE  t2.mnc  = t1.mnc
                AND t2.mcc = t1.mcc
                AND t2.bcc = t1.bcc
                AND t2.ncc = t1.ncc
                AND t2.bcch::integer = t1.bcch::integer
                AND t2.lac = t1.lac
            )
        """
        self.engine.execute(sql)
        session.close()

    def incosistent_umts_externals(self):
        Session = sessionmaker(bind=self.db_engine)
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
            t1.bcc as ext_dl_uarfcn, 
            t1.ncc as ext_rac,
            t1.bcch as ext_lac,
            t1.lac as ext_psc,
            
            -- internal values
            t2.mnc as int_mnc,
            t2.mcc as int_mcc,
            t2.bcc as int_dl_uarfcn,
            t2.ncc as int_rac,
            t2.bcch as int_lac,
            t2.lac as int_psc,
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
            OR t1.dl_uarfcn::integer != t2.dl_uarfcn::integer
            OR t1.psc != t2.psc

        """

        self.engine.execute(sql)

        # Delete inconsistencies that nolonger exist in external 2G cells
        sql = """
            DELETE FROM 
            network_audit.incosistent_3g_externals t1
            WHERE 
            t1."ext_cellname" IN (
            SELECT "name" FROM live_network.umts_external_cells t2 
                WHERE  t2.mnc  = t1.mnc
                AND t2.mcc = t1.mcc
                AND t2.dl_uarfcn = t1.dl_uarfcn
                AND t2.rac = t1.rac
                AND t2.psc = t1.psc
                AND t2.lac = t1.lac
            )
        """
        self.engine.execute(sql)
        session.close()

    def incosistent_lte_externals(self):
        pass