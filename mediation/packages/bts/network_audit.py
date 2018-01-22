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

            # print(parameter_sql)

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

