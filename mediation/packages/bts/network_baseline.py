import psycopg2
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import logging


# @todo: use logger
class NetworkBaseLine(object):

    def __init__(self, db_name='bts', db_user='bodastage', db_pass='password', db_host='database'):
        ''' Constructor for this class. '''
        self.engine = create_engine('postgresql://{}:{}@{}/{}'.format(db_user, db_pass, db_host, db_name))
        self.logger = logging.getLogger('network-baseline')

        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def run(self, vendor_id, tech_id):
        """Run network baseline"""
        conn = psycopg2.connect("dbname=bts user=bodastage password=password host=database")

        conn.autocommit = True

        cur = conn.cursor()

        # Get the schema name for vendor's cm data
        cur.execute("""SELECT pk, "name" FROM managedobjects_schemas WHERE tech_pk = %s and vendor_pk = %s""",
                    (tech_id, vendor_id))
        schema = cur.fetchone()
        schema_name = schema[1]

        # Get MOs
        # UMTS, Ericsson
        cur.execute("""
            SELECT DISTINCT t1.pk, t1."name" 
            FROM managedobjects t1
            INNER JOIN live_network.baseline_parameter_config t2 on t2.mo_pk = t1.pk
            WHERE t1.tech_pk = %s and t1.vendor_pk =%s """, (tech_id, vendor_id))

        mos = cur.fetchall()

        # print(mos)

        for idx in range(len(mos)):
            mo_name = mos[idx][1]
            mo_pk = str(mos[idx][0])

            print("mo_name: {0} mo_pk: {1}".format(mo_name, mo_pk))
            # Iterate through the parameters
            cur.execute("""
                SELECT t1.pk, t1."name" 
                FROM vendor_parameters t1
                INNER JOIN live_network.baseline_parameter_config t2 on t2.parameter_pk = t1.pk
                WHERE 
                parent_pk = %s """, (mo_pk,))

            parameters = cur.fetchall()
            for i in range(len(parameters)):
                parameter_pk = parameters[i][0]
                parameter_name = parameters[i][1]

                sql = """
                    SELECT "{2}" AS parameter, count(1) as cnt
                    FROM  {0}."{1}"
                    WHERE 
                    "{2}" IS NOT NULL AND TRIM("{2}") != '####'
                    GROUP BY "{2}"
                    ORDER BY cnt DESC
                    LIMIT 1
                """.format(schema_name, mo_name, parameter_name)

                print(sql)

                parameter_value = ""

                try:
                    cur.execute(sql)
                    parameter_value = cur.fetchone()
                except:
                    continue

                # print(sql)
                # print (parameter_value)
                if parameter_value == None: continue

                print (parameter_value)

                base_line_value = str(parameter_value[0]).strip()
                print ("base_line_value:{0}".format(base_line_value))

                # if base_line_value is None: continue

                # Skip values greater than 200 characters
                # if len(base_line_value) > 200: continue

                # Insert base line value
                sql = """
                INSERT INTO live_network.base_line_values
                (pk, parameter_pk, value, date_added, date_modified, added_by, modified_by)
                VALUES 
                (
                    NEXTVAL('live_network.seq_base_line_values_pk'),
                    %s,
                    %s,
                    now()::timestamp,
                    now()::timestamp,
                    0,
                    0
                )
                """

                try:
                    cur.execute(sql, (parameter_pk, base_line_value))
                except Exception as ex:

                    # psycopg2.errorcodes.UNIQUE_VIOLATION : #Update if unique constraint voilation exception is thrown
                    if ex.pgcode == 23505:
                        update_sql = """
                            UPDATE live_network.base_line_values
                            SET value = %s,
                            date_modified = now()::timestamp,
                            modified_by = 0
                            WHERE 
                            paremeter_pk = %s
                        """

                        try:
                            cur.execute(update_sql, (parameter_pk, base_line_value))
                        except:
                            continue

                    continue

    def generate_huawei_2g_discrencies(self):
        """Generate Huawei 2G network baseline discrepancies"""

        self.generate_huawei_2g_cell_level_discrepancies()

    def generate_huawei_2g_cell_level_discrepancies(self):
        """Generate Huawei 2G baseline descripancies for cell level parameters"""
        engine = create_engine('postgresql://bodastage:password@database/bts')
        vendor_pk = 2
        tech_pk = 1
        schema_name = 'hua_cm_2g'

        conn = psycopg2.connect("dbname=bts user=bodastage password=password host=database")
        conn.autocommit = True
        cur = conn.cursor()

        # Get MO
        sql = """
            SELECT  DISTINCT
            t3.name as mo,
            t3.pk as pk,
            t3.affect_level
            FROM 
            live_network.base_line_values t1
            INNER JOIN vendor_parameters t2 on t2.pk = t1.parameter_pk
            INNER JOIN managedobjects t3 on t3.pk  = t2.parent_pk 
                 AND t3.vendor_pk = {} AND t3.tech_pk = {}
                 AND t3.affect_level  = 1
        """.format(vendor_pk, tech_pk)
        cur.execute(sql)
        mo_list = cur.fetchall()

        for mo in mo_list:
            mo_name, mo_pk, mo_affect_level = mo

            # Get parameters
            sql = """
                SELECT 
                t2.name as pname,
                t2.pk as pk
                FROM 
                live_network.base_line_values t1
                INNER JOIN vendor_parameters t2 on t2.pk = t1.parameter_pk
                INNER JOIN managedobjects t3 on t3.pk  = t2.parent_pk 
                INNER JOIN network_entities t4 on t4.pk = t3.affect_level
                    AND t3.vendor_pk = {} AND t3.tech_pk = {}
                WHERE
                t3.name = '{}'
            """.format(vendor_pk, tech_pk, mo_name)
            cur.execute(sql)

            parameters = cur.fetchall()

            attr_list = [p[0] for p in parameters]

            str_param_values = ",".join(["t_mo.{0}{1}{0}".format('"', p) for p in attr_list])
            str_param_names = ",".join(["{0}{1}{0}".format('\'', p) for p in attr_list])

            # Join all cell level mos with the primary cell mo i.e. GCELL
            cell_level_join = """ INNER JOIN {0}.GCELL gcell ON gcell."CELLID" = t_mo."CELLID" AND gcell.neid = t_mo.neid 
                              AND gcell.module_type = t_mo.module_type """.format(schema_name)

            # Add new entries
            sql = """
             INSERT INTO network_audit.network_baseline 
             (node, site, cellname, mo, parameter, bvalue, nvalue, vendor, technology, age, modified_by, added_by, date_added, date_modified)
             SELECT TT1.* FROM (
                 SELECT
                 t8.name as node,
                 t7.name as site,
                t4.cellname,
                t3.name as mo,
                t2.name as parameter,
                t1.value as bvalue,
                TRIM(t4.pvalue) as nvalue,
                t9.name as vendor,
                t10.name as technology,
                1 as age,
                0 as modified_by,
                0 as added_by,
                date_time as date_added,
                date_time as date_modified
                from live_network.base_line_values t1
                INNER JOIN vendor_parameters t2 on t2.pk = t1.parameter_pk
                INNER JOIN managedobjects t3 on t3.pk = t2.parent_pk
                INNER JOIN live_network.baseline_parameter_config t5 on t5.mo_pk = t3.pk AND t5.parameter_pk = t2.pk
                INNER JOIN (
                    SELECT * FROM (
                        SELECT
                        '{2}' as "MO",
                        gcell."CELLNAME" as cellname,
                        gcell."varDateTime" as date_time,
                        unnest(array[{0}]) AS pname,
                        unnest(array[{1}]) AS pvalue
                        FROM
                        hua_cm_2g.{2} t_mo
                        {3}
                        WHERE
                        t_mo.module_type = 'Radio'
                        ) TT
                    ) t4 on t4.pname = t2.name AND trim(t4.pvalue) != t1.value
                INNER JOIN live_network.cells t6 on t6.name = t4.cellname
                INNER JOIN live_network.sites t7 on t7.pk = t6.site_pk
                INNER JOIN live_network.nodes t8 on t8.pk = t7.node_pk
                INNER JOIN vendors t9 on t9.pk = t6.vendor_pk
                INNER JOIN technologies t10 ON t10.pk = t6.tech_pk
                ) TT1
            LEFT JOIN network_audit.network_baseline TT2 on TT2.node = TT1.node
                AND TT2.site  = TT1.site 
                AND TT2.cellname = TT1.cellname
                AND TT2.mo = TT1.mo
                AND TT2.parameter = TT1.parameter
                AND TT2.bvalue = TT1.bvalue
                AND TT2.nvalue = TT1.nvalue
            WHERE
            TT2.cellname is NULL
            """.format(str_param_names, str_param_values, mo_name, cell_level_join)
            print(sql)
            cur.execute(sql)

            # Delete old entries
            sql = """
                WITH rd AS (
                SELECT TT2.* FROM 
                network_audit.network_baseline TT2
                LEFT JOIN 
                (
                    select
                     t8.name as node,
                     t7.name as site,
                    t4.cellname,
                    t3.name as mo,
                    t2.name as parameter,
                    t1.value as bvalue,
                    TRIM(t4.pvalue) as nvalue,
                    t9.name as vendor,
                    t10.name as technology,
                    0 as modified_by,
                    0 as added_by,
                    date_time as date_added,
                    date_time as date_modified
                    from live_network.base_line_values t1
                    INNER JOIN vendor_parameters t2 on t2.pk = t1.parameter_pk
                    INNER JOIN managedobjects t3 on t3.pk = t2.parent_pk
                    INNER JOIN live_network.baseline_parameter_config t5 on t5.mo_pk = t3.pk AND t5.parameter_pk = t2.pk
                    INNER JOIN (
                      SELECT * FROM (
                                SELECT
                                '{2}' as "MO",
                                gcell."CELLNAME" as cellname,
                                gcell."varDateTime" as date_time,
                                unnest(array[{0}]) AS pname,
                                unnest(array[{1}]) AS pvalue
                                FROM
                                hua_cm_2g.{2} t_mo
                                {3}
                                WHERE
                                t_mo.module_type = 'Radio'
                                ) TT
                        ) t4 on t4.pname = t2.name AND trim(t4.pvalue) != t1.value
                    INNER JOIN live_network.cells t6 on t6.name = t4.cellname
                    INNER JOIN live_network.sites t7 on t7.pk = t6.site_pk
                    INNER JOIN live_network.nodes t8 on t8.pk = t7.node_pk
                    INNER JOIN vendors t9 on t9.pk = t6.vendor_pk
                    INNER JOIN technologies t10 ON t10.pk = t6.tech_pk
                    ) TT1 ON TT2.node = TT1.node
                AND TT2.site  = TT1.site 
                AND TT2.cellname = TT1.cellname
                AND TT2.mo = TT1.mo
                AND TT2.parameter = TT1.parameter
                AND TT2.bvalue = TT1.bvalue
                AND TT2.nvalue = TT1.nvalue
                WHERE
                TT1.cellname IS NULL
                )
                DELETE FROM network_audit.network_baseline t1
                WHERE t1.pk  IN (SELECT pk from rd)
            """.format(str_param_names, str_param_values, mo_name, cell_level_join)
            print(sql)
            cur.execute(sql)

            # Update old entries
            sql = """
                WITH rd AS (
                    SELECT TT2.pk, TT1.* FROM 
                    network_audit.network_baseline TT2
                    INNER JOIN 
                    (
                        select
                         t8.name as node,
                         t7.name as site,
                        t4.cellname,
                        t3.name as mo,
                        t2.name as parameter,
                        t1.value as bvalue,
                        trim(t4.pvalue) as nvalue,
                        t9.name as vendor,
                        t10.name as technology,
                        0 as modified_by,
                        0 as added_by,
                        date_time as date_added,
                        date_time as date_modified
                        from live_network.base_line_values t1
                        INNER JOIN vendor_parameters t2 on t2.pk = t1.parameter_pk
                        INNER JOIN managedobjects t3 on t3.pk = t2.parent_pk
                        INNER JOIN live_network.baseline_parameter_config t5 on t5.mo_pk = t3.pk AND t5.parameter_pk = t2.pk
                        INNER JOIN (
                          SELECT * FROM (
                                    SELECT
                                    '{2}' as "MO",
                                    gcell."CELLNAME" as cellname,
                                    gcell."varDateTime" as date_time,
                                    unnest(array[{0}]) AS pname,
                                    unnest(array[{1}]) AS pvalue
                                    FROM
                                    hua_cm_2g.{2} t_mo
                                    {3}
                                    WHERE
                                    t_mo.module_type = 'Radio'
                                    ) TT
                            ) t4 on t4.pname = t2.name AND trim(t4.pvalue) != t1.value
                        INNER JOIN live_network.cells t6 on t6.name = t4.cellname
                        INNER JOIN live_network.sites t7 on t7.pk = t6.site_pk
                        INNER JOIN live_network.nodes t8 on t8.pk = t7.node_pk
                        INNER JOIN vendors t9 on t9.pk = t6.vendor_pk
                        INNER JOIN technologies t10 ON t10.pk = t6.tech_pk
                        ) TT1 ON TT2.node = TT1.node
                    AND TT2.site  = TT1.site 
                    AND TT2.cellname = TT1.cellname
                    AND TT2.mo = TT1.mo
                    AND TT2.parameter = TT1.parameter
                    AND TT2.bvalue = TT1.bvalue
                    AND TT2.nvalue = TT1.nvalue
                )
                UPDATE network_audit.network_baseline AS nb
                SET 
                date_modified = rd.date_added, 
                age=DATE_PART('day',AGE(nb.date_added, rd.date_added))
                FROM 
                rd 
                where 
                rd.pk = nb.pk
            """.format(str_param_names, str_param_values, mo_name, cell_level_join)
            print(sql)
            cur.execute(sql)

    def generate_huawei_2g_site_level_discrepancies(self):
        """Generate Huawei 2G baseline discrepancies for site level parameters"""
        engine = create_engine('postgresql://bodastage:password@database/bts')
        vendor_pk = 2
        tech_pk = 1
        schema_name = 'hua_cm_2g'

        conn = psycopg2.connect("dbname=bts user=bodastage password=password host=database")
        conn.autocommit = True
        cur = conn.cursor()

        # Get MO
        sql = """
            SELECT  DISTINCT
            t3.name as mo,
            t3.pk as pk,
            t3.affect_level
            FROM 
            live_network.base_line_values t1
            INNER JOIN vendor_parameters t2 on t2.pk = t1.parameter_pk
            INNER JOIN managedobjects t3 on t3.pk  = t2.parent_pk 
                 AND t3.vendor_pk = {} AND t3.tech_pk = {}
                 AND t3.affect_level  = 4
        """.format(vendor_pk, tech_pk)
        cur.execute(sql)
        mo_list = cur.fetchall()

        for mo in mo_list:
            mo_name, mo_pk, mo_affect_level = mo

            # Get parameters
            sql = """
                SELECT 
                t2.name as pname,
                t2.pk as pk
                FROM 
                live_network.base_line_values t1
                INNER JOIN vendor_parameters t2 on t2.pk = t1.parameter_pk
                INNER JOIN managedobjects t3 on t3.pk  = t2.parent_pk 
                INNER JOIN network_entities t4 on t4.pk = t3.affect_level
                    AND t3.vendor_pk = {} AND t3.tech_pk = {}
                    AND t3.affect_level  = 4
                WHERE
                t3.name = '{}'
            """.format(vendor_pk, tech_pk, mo_name)
            cur.execute(sql)

            parameters = cur.fetchall()

            attr_list = [p[0] for p in parameters]

            str_param_values = ",".join(["t_mo.{0}{1}{0}".format('"', p) for p in attr_list])
            str_param_names = ",".join(["{0}{1}{0}".format('\'', p) for p in attr_list])

            # Join all cell level mos with the primary cell mo i.e. GCELL.
            # p_mo for primary MO
            cell_level_join = """ INNER JOIN {0}.BTS p_mo ON p_mo."BTSID" = t_mo."BTSID" AND p_mo.neid = t_mo.neid 
                              AND p_mo.module_type = t_mo.module_type """.format(schema_name)

            # Add new entries
            sql = """
             INSERT INTO network_audit.baseline_site_parameters 
             (node, site,  mo, parameter, bvalue, nvalue, vendor, technology, age, modified_by, added_by, date_added, date_modified)
             SELECT TT1.* FROM (
                 SELECT
                 t8.name as node,
                 t7.name as site,
                t3.name as mo,
                t2.name as parameter,
                t1.value as bvalue,
                TRIM(t4.pvalue) as nvalue,
                t9.name as vendor,
                t10.name as technology,
                1 as age,
                0 as modified_by,
                0 as added_by,
                date_time as date_added,
                date_time as date_modified
                from live_network.base_line_values t1
                INNER JOIN vendor_parameters t2 on t2.pk = t1.parameter_pk
                INNER JOIN managedobjects t3 on t3.pk = t2.parent_pk
                INNER JOIN live_network.baseline_parameter_config t5 on t5.mo_pk = t3.pk AND t5.parameter_pk = t2.pk
                INNER JOIN (
                    SELECT * FROM (
                        SELECT
                        '{2}' as "MO",
                        p_mo."BTSNAME" as sitename,
                        p_mo."varDateTime" as date_time,
                        unnest(array[{0}]) AS pname,
                        unnest(array[{1}]) AS pvalue
                        FROM
                        hua_cm_2g.{2} t_mo
                        {3}
                        ) TT
                    ) t4 on t4.pname = t2.name AND trim(t4.pvalue) != t1.value
                INNER JOIN live_network.sites t7 on  t7.name = t4.sitename 
                INNER JOIN live_network.nodes t8 on t8.pk = t7.node_pk
                INNER JOIN vendors t9 on t9.pk = t7.vendor_pk
                INNER JOIN technologies t10 ON t10.pk = t7.tech_pk
                ) TT1
            LEFT JOIN network_audit.baseline_site_parameters TT2 on TT2.node = TT1.node
                AND TT2.site  = TT1.site 
                AND TT2.mo = TT1.mo
                AND TT2.parameter = TT1.parameter
                AND TT2.bvalue = TT1.bvalue
                AND TT2.nvalue = TT1.nvalue
            WHERE
            TT2.site is NULL
            """.format(str_param_names, str_param_values, mo_name, cell_level_join)
            print(sql)
            cur.execute(sql)

            # Delete old entries
            sql = """
                WITH rd AS (
                SELECT TT2.* FROM 
                network_audit.baseline_site_parameters TT2
                LEFT JOIN 
                (
                    select
                    t8.name as node,
                    t7.name as site,
                    t3.name as mo,
                    t2.name as parameter,
                    t1.value as bvalue,
                    TRIM(t4.pvalue) as nvalue,
                    t9.name as vendor,
                    t10.name as technology,
                    0 as modified_by,
                    0 as added_by,
                    date_time as date_added,
                    date_time as date_modified
                    from live_network.base_line_values t1
                    INNER JOIN vendor_parameters t2 on t2.pk = t1.parameter_pk
                    INNER JOIN managedobjects t3 on t3.pk = t2.parent_pk
                    INNER JOIN live_network.baseline_parameter_config t5 on t5.mo_pk = t3.pk AND t5.parameter_pk = t2.pk
                    INNER JOIN (
                      SELECT * FROM (
                                SELECT
                                '{2}' as "MO",
                                p_mo."BTSNAME" as sitename,
                                p_mo."varDateTime" as date_time,
                                unnest(array[{0}]) AS pname,
                                unnest(array[{1}]) AS pvalue
                                FROM
                                hua_cm_2g.{2} t_mo
                                {3}
                                ) TT
                        ) t4 on t4.pname = t2.name AND trim(t4.pvalue) != t1.value
                    INNER JOIN live_network.sites t7 on  t7.name = t4.sitename 
                    INNER JOIN live_network.nodes t8 on t8.pk = t7.node_pk
                    INNER JOIN vendors t9 on t9.pk = t7.vendor_pk
                    INNER JOIN technologies t10 ON t10.pk = t7.tech_pk
                    ) TT1 ON TT2.node = TT1.node
                AND TT2.site  = TT1.site 
                AND TT2.mo = TT1.mo
                AND TT2.parameter = TT1.parameter
                AND TT2.bvalue = TT1.bvalue
                AND TT2.nvalue = TT1.nvalue
                WHERE
                TT1.site IS NULL
                )
                DELETE FROM network_audit.baseline_site_parameters t1
                WHERE t1.pk  IN (SELECT pk from rd)
            """.format(str_param_names, str_param_values, mo_name, cell_level_join)
            print(sql)
            cur.execute(sql)

            # Update old entries
            sql = """
                WITH rd AS (
                    SELECT TT2.pk, TT1.* FROM 
                    network_audit.baseline_site_parameters TT2
                    INNER JOIN 
                    (
                        select
                         t8.name as node,
                         t7.name as site,
                        t3.name as mo,
                        t2.name as parameter,
                        t1.value as bvalue,
                        trim(t4.pvalue) as nvalue,
                        t9.name as vendor,
                        t10.name as technology,
                        0 as modified_by,
                        0 as added_by,
                        date_time as date_added,
                        date_time as date_modified
                        from live_network.base_line_values t1
                        INNER JOIN vendor_parameters t2 on t2.pk = t1.parameter_pk
                        INNER JOIN managedobjects t3 on t3.pk = t2.parent_pk
                        INNER JOIN live_network.baseline_parameter_config t5 on t5.mo_pk = t3.pk AND t5.parameter_pk = t2.pk
                        INNER JOIN (
                          SELECT * FROM (
                                    SELECT
                                    '{2}' as "MO",
                                    p_mo."BTSNAME" as sitename,
                                    p_mo."varDateTime" as date_time,
                                    unnest(array[{0}]) AS pname,
                                    unnest(array[{1}]) AS pvalue
                                    FROM
                                    hua_cm_2g.{2} t_mo
                                    {3}
                                    ) TT
                            ) t4 on t4.pname = t2.name AND trim(t4.pvalue) != t1.value
                        INNER JOIN live_network.sites t7 on  t7.name = t4.sitename 
                        INNER JOIN live_network.nodes t8 on t8.pk = t7.node_pk
                        INNER JOIN vendors t9 on t9.pk = t7.vendor_pk
                        INNER JOIN technologies t10 ON t10.pk = t7.tech_pk
                        ) TT1 ON TT2.node = TT1.node
                    AND TT2.site  = TT1.site 
                    AND TT2.mo = TT1.mo
                    AND TT2.parameter = TT1.parameter
                    AND TT2.bvalue = TT1.bvalue
                    AND TT2.nvalue = TT1.nvalue
                )
                UPDATE network_audit.baseline_site_parameters AS nb
                SET 
                date_modified = rd.date_added, 
                age=DATE_PART('day',AGE(nb.date_added, rd.date_added))
                FROM 
                rd 
                where 
                rd.pk = nb.pk
            """.format(str_param_names, str_param_values, mo_name, cell_level_join)
            print(sql)
            cur.execute(sql)

    def generate_huawei_2g_node_level_discrepancies(self):
        """Generate Huawei 2G baseline discrepancies for node level parameters"""
        engine = create_engine('postgresql://bodastage:password@database/bts')
        vendor_pk = 2
        tech_pk = 1
        schema_name = 'hua_cm_2g'

        conn = psycopg2.connect("dbname=bts user=bodastage password=password host=database")
        conn.autocommit = True
        cur = conn.cursor()

        # Get MO
        sql = """
            SELECT  DISTINCT
            t3.name as mo,
            t3.pk as pk,
            t3.affect_level
            FROM 
            live_network.base_line_values t1
            INNER JOIN vendor_parameters t2 on t2.pk = t1.parameter_pk
            INNER JOIN managedobjects t3 on t3.pk  = t2.parent_pk 
                 AND t3.vendor_pk = {} AND t3.tech_pk = {}
                 AND t3.affect_level  = 7 -- BSC
        """.format(vendor_pk, tech_pk)
        cur.execute(sql)
        mo_list = cur.fetchall()

        for mo in mo_list:
            mo_name, mo_pk, mo_affect_level = mo

            # Get parameters
            sql = """
                SELECT 
                t2.name as pname,
                t2.pk as pk
                FROM 
                live_network.base_line_values t1
                INNER JOIN vendor_parameters t2 on t2.pk = t1.parameter_pk
                INNER JOIN managedobjects t3 on t3.pk  = t2.parent_pk 
                INNER JOIN network_entities t4 on t4.pk = t3.affect_level
                    AND t3.vendor_pk = {} AND t3.tech_pk = {}
                    AND t3.affect_level  = 7 -- BSC
                WHERE
                t3.name = '{}'
            """.format(vendor_pk, tech_pk, mo_name)
            cur.execute(sql)

            parameters = cur.fetchall()

            attr_list = [p[0] for p in parameters]

            str_param_values = ",".join(["t_mo.{0}{1}{0}".format('"', p) for p in attr_list])
            str_param_names = ",".join(["{0}{1}{0}".format('\'', p) for p in attr_list])

            # Join all cell level mos with the primary cell mo i.e. GCELL.
            # p_mo for primary MO
            cell_level_join = """ INNER JOIN {0}.BSCBASIC p_mo ON p_mo.neid = t_mo.neid 
                              AND p_mo.module_type = t_mo.module_type """.format(schema_name)

            # Add new entries
            sql = """
             INSERT INTO network_audit.baseline_node_parameters 
             (node,  mo, parameter, bvalue, nvalue, vendor, technology, age, modified_by, added_by, date_added, date_modified)
             SELECT TT1.* FROM (
                SELECT
                t8.name as node,
                t3.name as mo,
                t2.name as parameter,
                t1.value as bvalue,
                TRIM(t4.pvalue) as nvalue,
                t9.name as vendor,
                t10.name as technology,
                1 as age,
                0 as modified_by,
                0 as added_by,
                date_time as date_added,
                date_time as date_modified
                from live_network.base_line_values t1
                INNER JOIN vendor_parameters t2 on t2.pk = t1.parameter_pk
                INNER JOIN managedobjects t3 on t3.pk = t2.parent_pk
                INNER JOIN live_network.baseline_parameter_config t5 on t5.mo_pk = t3.pk AND t5.parameter_pk = t2.pk
                INNER JOIN (
                    SELECT * FROM (
                        SELECT
                        '{2}' as "MO",
                        p_mo.neid as node,
                        p_mo."varDateTime" as date_time,
                        unnest(array[{0}]) AS pname,
                        unnest(array[{1}]) AS pvalue
                        FROM
                        hua_cm_2g.{2} t_mo
                        {3}
                        ) TT
                    ) t4 on t4.pname = t2.name AND trim(t4.pvalue) != t1.value 
                INNER JOIN live_network.nodes t8 on t8.name = t4.node
                INNER JOIN vendors t9 on t9.pk = t8.vendor_pk
                INNER JOIN technologies t10 ON t10.pk = t8.tech_pk
                ) TT1
            LEFT JOIN network_audit.baseline_node_parameters TT2 on TT2.node = TT1.node
                AND TT2.mo = TT1.mo
                AND TT2.parameter = TT1.parameter
                AND TT2.bvalue = TT1.bvalue
                AND TT2.nvalue = TT1.nvalue
            WHERE
            TT2.node is NULL
            """.format(str_param_names, str_param_values, mo_name, cell_level_join)
            print(sql)
            cur.execute(sql)

            # Delete old entries
            sql = """
                WITH rd AS (
                SELECT TT2.* FROM 
                network_audit.baseline_node_parameters TT2
                LEFT JOIN 
                (
                    select
                    t8.name as node,
                    t3.name as mo,
                    t2.name as parameter,
                    t1.value as bvalue,
                    TRIM(t4.pvalue) as nvalue,
                    t9.name as vendor,
                    t10.name as technology,
                    0 as modified_by,
                    0 as added_by,
                    date_time as date_added,
                    date_time as date_modified
                    from live_network.base_line_values t1
                    INNER JOIN vendor_parameters t2 on t2.pk = t1.parameter_pk
                    INNER JOIN managedobjects t3 on t3.pk = t2.parent_pk
                    INNER JOIN live_network.baseline_parameter_config t5 on t5.mo_pk = t3.pk AND t5.parameter_pk = t2.pk
                    INNER JOIN (
                      SELECT * FROM (
                                SELECT
                                '{2}' as "MO",
                                p_mo.neid as node,
                                p_mo."varDateTime" as date_time,
                                unnest(array[{0}]) AS pname,
                                unnest(array[{1}]) AS pvalue
                                FROM
                                hua_cm_2g.{2} t_mo
                                {3}
                                ) TT
                        ) t4 on t4.pname = t2.name AND trim(t4.pvalue) != t1.value
                    INNER JOIN live_network.nodes t8 on t8.name = t4.node
                    INNER JOIN vendors t9 on t9.pk = t8.vendor_pk
                    INNER JOIN technologies t10 ON t10.pk = t8.tech_pk
                    ) TT1 ON TT2.node = TT1.node
                AND TT2.mo = TT1.mo
                AND TT2.parameter = TT1.parameter
                AND TT2.bvalue = TT1.bvalue
                AND TT2.nvalue = TT1.nvalue
                WHERE
                TT1.node IS NULL
                )
                DELETE FROM network_audit.baseline_node_parameters t1
                WHERE t1.pk  IN (SELECT pk from rd)
            """.format(str_param_names, str_param_values, mo_name, cell_level_join)
            print(sql)
            cur.execute(sql)

            # Update old entries
            sql = """
                WITH rd AS (
                    SELECT TT2.pk, TT1.* FROM 
                    network_audit.baseline_node_parameters TT2
                    INNER JOIN 
                    (
                        select
                         t8.name as node,
                        t3.name as mo,
                        t2.name as parameter,
                        t1.value as bvalue,
                        trim(t4.pvalue) as nvalue,
                        t9.name as vendor,
                        t10.name as technology,
                        0 as modified_by,
                        0 as added_by,
                        date_time as date_added,
                        date_time as date_modified
                        from live_network.base_line_values t1
                        INNER JOIN vendor_parameters t2 on t2.pk = t1.parameter_pk
                        INNER JOIN managedobjects t3 on t3.pk = t2.parent_pk
                        INNER JOIN live_network.baseline_parameter_config t5 on t5.mo_pk = t3.pk AND t5.parameter_pk = t2.pk
                        INNER JOIN (
                          SELECT * FROM (
                                    SELECT
                                    '{2}' as "MO",
                                    p_mo.neid as node,
                                    p_mo."varDateTime" as date_time,
                                    unnest(array[{0}]) AS pname,
                                    unnest(array[{1}]) AS pvalue
                                    FROM
                                    hua_cm_2g.{2} t_mo
                                    {3}
                                    ) TT
                            ) t4 on t4.pname = t2.name AND trim(t4.pvalue) != t1.value
                        INNER JOIN live_network.nodes t8 on t8.name = t4.node
                        INNER JOIN vendors t9 on t9.pk = t8.vendor_pk
                        INNER JOIN technologies t10 ON t10.pk = t8.tech_pk
                        ) TT1 ON TT2.node = TT1.node
                    AND TT2.mo = TT1.mo
                    AND TT2.parameter = TT1.parameter
                    AND TT2.bvalue = TT1.bvalue
                    AND TT2.nvalue = TT1.nvalue
                )
                UPDATE network_audit.baseline_node_parameters AS nb
                SET 
                date_modified = rd.date_added, 
                age=DATE_PART('day',AGE(nb.date_added, rd.date_added))
                FROM 
                rd 
                where 
                rd.pk = nb.pk
            """.format(str_param_names, str_param_values, mo_name, cell_level_join)
            print(sql)
            cur.execute(sql)

    def run_network_baseline(self):
        """
        Run baseline for all technologies

        :return:
        """

        insert_qry = """
            INSERT INTO baseline.network_baseline
            (date_time, vendor, nename, mo, parameter, bvalue)
            SELECT 
                date_time,
                vendor, 
                nename, 
                mo, 
                parameter,
                pvalue
            FROM 
                baseline.parameter_value_counts t1
            WHERE 
            (vendor, nename, mo, parameter, occurence) IN 
            (
                SELECT 
                    vendor, nename, mo, parameter, MAX(occurence)
                FROM baseline.parameter_value_counts t2
                GROUP BY vendor, nename, mo, parameter
            )
            ON CONFLICT ON CONSTRAINT uq_network_baseline
            DO UPDATE 
            SET date_modified = NOW()::TIMESTAMP
        """
        self.engine.execute(text(insert_qry))

        # Delete old values
        delete_qry = """
           DELETE FROM  baseline.network_baseline
            WHERE 
            (vendor, nename, mo, parameter, bvalue) NOT IN 
            (
                SELECT 
                    vendor, nename, mo, parameter, bvalue 
                FROM baseline.parameter_value_counts t2
                GROUP BY vendor, nename, mo, parameter
            )
            
        """
        self.engine.execute(text(delete_qry))

    def compute_huawei_4g_value_counts(self):
        """
        Compute Huawei baseline for 4G.

        The baseline is computed by grouping values per tracking area.

        :return:
        """

        tech = '4G'

        # List of parameters to ignore
        ignore_list = ['LOADID', 'VARDATE', 'DATETIME', 'REGION', 'NENAME', 'CELLID', 'ID', 'FILENAME', 'TECHNOLOGY', 'VENDOR', 'VERSION', 'NETYPE', 'CELLNAME']

        self.logger.info("Processing Huawei baseline for {}...".format(tech))

        # Get list of mos configured in process_config
        result = self.engine.execute(text("SELECT * FROM baseline.process_config WHERE process = true AND technology = :tech AND vendor = :vendor"), tech=tech, vendor='HUAWEI')
        for row in result:
            vendor = row['vendor']
            technology = row['technology']
            mo = row['mo']

            self.logger.info("vendor:{}, technology:{}, mo:{}".format(vendor, technology, mo))

            # Get field names from information_schema
            field_qry = """
                SELECT t1.column_name as field 
                FROM
                    information_schema.columns t1
                    LEFT JOIN baseline.parameter_ignore_list t2 
                        ON t1.table_name = t2.mo
                        AND t1.column_name = t2.parameter
                WHERE 
                    table_schema = 'huawei_cm'
                    AND table_name = :mo
                    AND t2.parameter is NULL
                    AND UPPER(t1.column_name) NOT IN ('{}')
            """.format("','".join(ignore_list))

            field_result = self.engine.execute(text(field_qry), mo=mo)

            # self.logger.info([row[0] for row in field_result])

            # self.logger.info(field_qry)

            for f in field_result:
                parameter = f[0]

                self.logger.info("Processing baseline for {}.{}...".format(mo, parameter))

                value_qry = """
                INSERT INTO  baseline.parameter_value_counts
                (date_time, vendor, nename, mo, parameter, pvalue, occurence)
                SELECT 
                    MAX(t1."DATETIME") AS date_time,
                    'HUAWEI' as vendor,
                    t4."TAC" AS nename,
                    '{0}' AS mo,
                    '{1}' AS parameter,
                    t1."{1}" AS pvalue,
                    COUNT(t1."{1}") AS occurence
                FROM huawei_cm."{0}" t1
                INNER JOIN cm_loads t5 on t5.pk = t1."LOADID"
                INNER JOIN huawei_cm."CELL" t2
                    ON t2."CELLID" = t1."LOCALCELLID"
                    AND t2."LOADID" = t1."LOADID"
                INNER JOIN huawei_cm."ENODEBFUNCTION" t3 
                    ON t3."ENODEBFUNCTIONNAME" =  t2."ENODEBFUNCTIONNAME"
                    AND t3."LOADID" = t1."LOADID"
                INNER JOIN huawei_cm."CNOPERATORTA" t4 
                    ON t4."ENODEBFUNCTIONNAME"  = t3."ENODEBFUNCTIONNAME"
                    AND t4."LOADID" = t1."LOADID"

                WHERE 
                    t1."{1}" IS NOT NULL
                    AND t5.is_current_load = true
                GROUP BY t4."TAC", t1."{1}"
                ON CONFLICT ON CONSTRAINT uq_parameter_value_counts
                DO NOTHING
                """.format(mo, parameter)

                # self.logger.info(value_qry)
                try:
                    self.engine.execute(text(value_qry))
                except Exception as e:
                    self.logger.error(str(e))

    def compute_huawei_2g3g_value_counts(self, tech='2G'):
        """
        Runs baseline for Huawei 2G, 3G, or 2G/3G

        The is implemented by the same function because the MOs are defined on a controller i.e. BSC and RNC which
        are picked from the SYS MO.

        :param: string The technology the mo exists on. It could be 2G,3G,2G/3G
        :return:
        """

        # List of parameters to ignore
        ignore_list = ['LOADID', 'VARDATE', 'DATETIME', 'REGION', 'NENAME', 'CELLID', 'ID', 'FILENAME', 'TECHNOLOGY', 'VENDOR', 'VERSION', 'NETYPE', 'CELLNAME']

        self.logger.info("Processing Huawei baseline for {}...".format(tech))

        # Get list of mos configured in process_config
        result = self.engine.execute(text("SELECT * FROM baseline.process_config WHERE process = true AND technology = :tech AND vendor = :vendor"), tech=tech, vendor='HUAWEI')
        for row in result:
            vendor = row['vendor']
            technology = row['technology']
            mo = row['mo']

            self.logger.info("vendor:{}, technology:{}, mo:{}".format(vendor, technology, mo))

            # Get field names from information_schema
            field_qry = """
                SELECT t1.column_name as field 
                FROM
                    information_schema.columns t1
                    LEFT JOIN baseline.parameter_ignore_list t2 
                        ON t1.table_name = t2.mo
                        AND t1.column_name = t2.parameter
                WHERE 
                    table_schema = 'huawei_cm'
                    AND table_name = :mo
                    AND t2.parameter is NULL
                    AND UPPER(t1.column_name) NOT IN ('{}')
            """.format("','".join(ignore_list))

            field_result = self.engine.execute(text(field_qry), mo=mo)

            # self.logger.info([row[0] for row in field_result])

            # self.logger.info(field_qry)

            self.logger.info('Processing parameters...')
            for f in field_result:
                parameter = f[0]

                self.logger.info("Processing baseline for {}.{}...".format(mo, parameter))

                value_qry = """
                INSERT INTO  baseline.parameter_value_counts
                (date_time, vendor, nename, mo, parameter, pvalue, occurence)
                SELECT 
                    MAX(t1."DATETIME") AS date_time,
                    'HUAWEI' as vendor,
                    t2."SYSOBJECTID" AS nename,
                    '{0}' AS mo,
                    '{1}' AS parameter,
                    t1."{1}" AS pvalue,
                    COUNT(t1."{1}") AS occurence
                FROM
                    huawei_cm."{0}" t1
                    INNER JOIN huawei_cm."SYS" t2 
                        ON t2."FILENAME" = t1."FILENAME"
                        AND t2."LOADID" = t1."LOADID"
                    INNER JOIN cm_loads t3 on t3.pk = t1."LOADID"
                WHERE t3.is_current_load = true AND t1."{1}" IS NOT NULL
                GROUP BY 
                    t2."SYSOBJECTID", t1."{1}"
                ON CONFLICT ON CONSTRAINT uq_parameter_value_counts
                DO NOTHING
                
                """.format(mo, parameter)

                self.engine.execute(text(value_qry))

    def compute_ericsson_2g_value_counts(self):
        """
        Runs baseline for Huawei 2G, 3G, or 2G/3G

        The is implemented by the same function because the MOs are defined on a controller i.e. BSC and RNC which
        are picked from the SYS MO.

1        :return:
        """

        tech = '2G'

        # List of parameters to ignore
        ignore_list = ['LOADID', 'VARDATE', 'DATETIME', 'REGION', 'NENAME', 'CELLID', 'ID', 'FILENAME', 'TECHNOLOGY', 'VENDOR', 'VERSION', 'NETYPE', 'CELLNAME']

        self.logger.info("Processing Huawei baseline for {}...".format(tech))

        # Get list of mos configured in process_config
        result = self.engine.execute(text("SELECT * FROM baseline.process_config WHERE process = true AND technology = :tech AND vendor = :vendor"), tech=tech, vendor='ERICSSON')
        for row in result:
            vendor = row['vendor']
            technology = row['technology']
            mo = row['mo']

            self.logger.info("vendor:{}, technology:{}, mo:{}".format(vendor, technology, mo))

            # Get field names from information_schema
            field_qry = """
                SELECT t1.column_name as field 
                FROM
                    information_schema.columns t1
                    LEFT JOIN baseline.parameter_ignore_list t2 
                        ON t1.table_name = t2.mo
                        AND t1.column_name = t2.parameter
                WHERE 
                    table_schema = 'ericsson_cm'
                    AND table_name = :mo
                    AND t2.parameter is NULL
                    AND UPPER(t1.column_name) NOT IN ('{}')
            """.format("','".join(ignore_list))

            field_result = self.engine.execute(text(field_qry), mo=mo)

            # self.logger.info([row[0] for row in field_result])

            # self.logger.info(field_qry)

            self.logger.info('Processing parameters...')
            for f in field_result:
                parameter = f[0]

                self.logger.info("Processing baseline for {}.{}...".format(mo, parameter))

                value_qry = """
                INSERT INTO  baseline.parameter_value_counts
                (date_time, vendor, nename, mo, parameter, pvalue, occurence)
                SELECT 
                    MAX(t1."DATETIME") AS date_time,
                    'ERICSSON' as vendor,
                    t1."BSC_NAME" AS nename,
                    '{0}' AS mo,
                    '{1}' AS parameter,
                    t1."{1}" AS pvalue,
                    COUNT(t1."{1}") AS occurence
                FROM
                    ericsson_cm."{0}" t1
                    INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
                WHERE t2.is_current_load = true AND t1."{1}" IS NOT NULL
                GROUP BY 
                    t1."BSC_NAME", t1."{1}"
                ON CONFLICT ON CONSTRAINT uq_parameter_value_counts
                DO NOTHING
                """.format(mo, parameter)

                self.engine.execute(text(value_qry))

    def compute_ericsson_3g_value_counts(self):
        """
        Runs baseline for Huawei 2G, 3G, or 2G/3G

        The is implemented by the same function because the MOs are defined on a controller i.e. BSC and RNC which
        are picked from the SYS MO.

        :param: string tech The technology.
        :return:
        """

        tech = "3G"

        # List of parameters to ignore
        ignore_list = ['LOADID', 'VARDATE', 'DATETIME', 'REGION', 'NENAME', 'CELLID', 'ID', 'FILENAME', 'TECHNOLOGY', 'VENDOR', 'VERSION', 'NETYPE', 'CELLNAME']

        self.logger.info("Processing Ericsson baseline for {}...".format(tech))

        # Get list of mos configured in process_config
        result = self.engine.execute(text("SELECT * FROM baseline.process_config WHERE process = true AND technology = :tech AND vendor = :vendor"), tech=tech, vendor='ERICSSON')
        for row in result:
            vendor = row['vendor']
            technology = row['technology']
            mo = row['mo']

            self.logger.info("vendor:{}, technology:{}, mo:{}".format(vendor, technology, mo))

            # Get field names from information_schema
            field_qry = """
                SELECT t1.column_name as field 
                FROM
                    information_schema.columns t1
                    LEFT JOIN baseline.parameter_ignore_list t2 
                        ON t1.table_name = t2.mo
                        AND t1.column_name = t2.parameter
                WHERE 
                    table_schema = 'ericsson_cm'
                    AND table_name = :mo
                    AND t2.parameter is NULL
                    AND UPPER(t1.column_name) NOT IN ('{}')
            """.format("','".join(ignore_list))

            field_result = self.engine.execute(text(field_qry), mo=mo)

            for f in field_result:
                parameter = f[0]

                self.logger.info("Processing baseline for {}.{}...".format(mo, parameter))

                value_qry = """
                INSERT INTO  baseline.parameter_value_counts
                (date_time, vendor, nename, mo, parameter, pvalue, occurence)
                SELECT 
                    MAX(t1."DATETIME") AS date_time,
                    'ERICSSON' as vendor,
                    t1."SubNetwork_2_id" AS nename,
                    '{0}' AS mo,
                    '{1}' AS parameter,
                    t1."{1}" AS pvalue,
                    COUNT(t1."{1}") AS occurence
                FROM
                    ericsson_cm."{0}" t1
                    INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
                WHERE t2.is_current_load = true AND t1."{1}" IS NOT NULL
                GROUP BY 
                    t1."SubNetwork_2_id", t1."{1}"
                ON CONFLICT ON CONSTRAINT uq_parameter_value_counts
                DO NOTHING
                """.format(mo, parameter)

                self.engine.execute(text(value_qry))

    def compute_ericsson_4g_value_counts(self):
        """
        Runs baseline for Huawei 2G, 3G, or 2G/3G

        The is implemented by the same function because the MOs are defined on a controller i.e. BSC and RNC which
        are picked from the SYS MO.

        :param: string tech The technology.
        :return:
        """

        tech = "4G"

        # List of parameters to ignore
        ignore_list = ['LOADID', 'VARDATE', 'DATETIME', 'REGION', 'NENAME', 'CELLID', 'ID', 'FILENAME', 'TECHNOLOGY', 'VENDOR', 'VERSION', 'NETYPE', 'CELLNAME']

        self.logger.info("Processing Ericsson baseline for {}...".format(tech))

        # Get list of mos configured in process_config
        result = self.engine.execute(text("SELECT * FROM baseline.process_config WHERE process = true AND technology = :tech AND vendor = :vendor"), tech=tech, vendor='ERICSSON')
        for row in result:
            vendor = row['vendor']
            technology = row['technology']
            mo = row['mo']

            self.logger.info("vendor:{}, technology:{}, mo:{}".format(vendor, technology, mo))

            # Get field names from information_schema
            field_qry = """
                    SELECT t1.column_name as field 
                    FROM
                        information_schema.columns t1
                        LEFT JOIN baseline.parameter_ignore_list t2 
                            ON t1.table_name = t2.mo
                            AND t1.column_name = t2.parameter
                    WHERE 
                        table_schema = 'ericsson_cm'
                        AND table_name = :mo
                        AND t2.parameter is NULL
                        AND UPPER(t1.column_name) NOT IN ('{}')
                """.format("','".join(ignore_list))

            field_result = self.engine.execute(text(field_qry), mo=mo)

            for f in field_result:
                parameter = f[0]

                self.logger.info("Processing baseline for {}.{}...".format(mo, parameter))

                value_qry = """
                    INSERT INTO  baseline.parameter_value_counts
                    (date_time, vendor, nename, mo, parameter, pvalue, occurence)
                    SELECT 
                        MAX(t1."DATETIME") AS date_time,
                        'ERICSSON' as vendor,
                        t1."SubNetwork_2_id" AS nename,
                        '{0}' AS mo,
                        '{1}' AS parameter,
                        t1."{1}" AS pvalue,
                        COUNT(t1."{1}") AS occurence
                    FROM
                        ericsson_cm."{0}" t1
                        INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
                    WHERE t2.is_current_load = true AND t1."{1}" IS NOT NULL
                    GROUP BY 
                        t1."SubNetwork_2_id", t1."{1}"
                    ON CONFLICT ON CONSTRAINT uq_parameter_value_counts
                    DO NOTHING
                    """.format(mo, parameter)

                self.engine.execute(text(value_qry))

    def compute_zte_2g_value_counts(self):
        """
        Runs baseline for Huawei 2G, 3G, or 2G/3G

        The is implemented by the same function because the MOs are defined on a controller i.e. BSC and RNC which
        are picked from the SYS MO.

        :param: string tech The technology.
        :return:
        """

        tech = "2G"

        # List of parameters to ignore
        ignore_list = ['LOADID', 'VARDATE', 'DATETIME', 'REGION', 'NENAME', 'CELLID', 'ID', 'FILENAME', 'TECHNOLOGY', 'VENDOR', 'VERSION', 'NETYPE', 'CELLNAME']

        self.logger.info("Processing Ericsson baseline for {}...".format(tech))

        # Get list of mos configured in process_config
        result = self.engine.execute(text("SELECT * FROM baseline.process_config WHERE process = true AND technology = :tech AND vendor = :vendor"), tech=tech, vendor='ZTE')
        for row in result:
            vendor = row['vendor']
            technology = row['technology']
            mo = row['mo']

            self.logger.info("vendor:{}, technology:{}, mo:{}".format(vendor, technology, mo))

            # Get field names from information_schema
            field_qry = """
                    SELECT t1.column_name as field 
                    FROM
                        information_schema.columns t1
                        LEFT JOIN baseline.parameter_ignore_list t2 
                            ON t1.table_name = t2.mo
                            AND t1.column_name = t2.parameter
                    WHERE 
                        table_schema = 'zte_cm'
                        AND table_name = :mo
                        AND t2.parameter is NULL
                        AND UPPER(t1.column_name) NOT IN ('{}')
                """.format("','".join(ignore_list))

            field_result = self.engine.execute(text(field_qry), mo=mo)

            for f in field_result:
                parameter = f[0]

                self.logger.info("Processing baseline for {}.{}...".format(mo, parameter))

                value_qry = """
                    INSERT INTO  baseline.parameter_value_counts
                    (date_time, vendor, nename, mo, parameter, pvalue, occurence)
                    SELECT 
                        MAX(t1."DATETIME") AS date_time,
                        'ZTE' as vendor,
                        t2."SubNetwork_2_id" AS nename,
                        '{0}' AS mo,
                        '{1}' AS parameter,
                        t1."{1}" AS pvalue,
                        COUNT(t1."{1}") AS occurence
                    FROM
                        zte_cm."{0}" t1
                        INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
                    WHERE t2.is_current_load = true AND t1."{1}" IS NOT NULL
                    GROUP BY 
                        t1."SubNetwork_2_id", t1."{1}"
                        ON CONFLICT ON CONSTRAINT uq_parameter_value_counts
                        DO NOTHING
                    """.format(mo, parameter)

                self.engine.execute(text(value_qry))

    def compute_zte_3g_value_counts(self):
        """
        Runs baseline for Huawei 2G, 3G, or 2G/3G

        The is implemented by the same function because the MOs are defined on a controller i.e. BSC and RNC which
        are picked from the SYS MO.

        :param: string tech The technology.
        :return:
        """

        tech = "3G"

        # List of parameters to ignore
        ignore_list = ['LOADID', 'VARDATE', 'DATETIME', 'REGION', 'NENAME', 'CELLID', 'ID', 'FILENAME', 'TECHNOLOGY', 'VENDOR', 'VERSION', 'NETYPE', 'CELLNAME']

        self.logger.info("Processing ZTE baseline for {}...".format(tech))

        # Get list of mos configured in process_config
        result = self.engine.execute(text("SELECT * FROM baseline.process_config WHERE process = true AND technology = :tech AND vendor = :vendor"), tech=tech, vendor='ZTE')

        for row in result:
            vendor = row['vendor']
            technology = row['technology']
            mo = row['mo']

            self.logger.info("vendor:{}, technology:{}, mo:{}".format(vendor, technology, mo))

            # Get field names from information_schema
            field_qry = """
                         SELECT t1.column_name as field 
                         FROM
                             information_schema.columns t1
                             LEFT JOIN baseline.parameter_ignore_list t2 
                                 ON t1.table_name = t2.mo
                                 AND t1.column_name = t2.parameter
                         WHERE 
                             table_schema = 'zte_cm'
                             AND table_name = :mo
                             AND t2.parameter is NULL
                             AND UPPER(t1.column_name) NOT IN ('{}')
                     """.format("','".join(ignore_list))

            field_result = self.engine.execute(text(field_qry), mo=mo)

            for f in field_result:
                parameter = f[0]

                self.logger.info("Processing baseline for {}.{}...".format(mo, parameter))

                value_qry = """
                         INSERT INTO  baseline.parameter_value_counts
                         (date_time, vendor, nename, mo, parameter, pvalue, occurence)
                         SELECT 
                             MAX(t1."DATETIME") AS date_time,
                             'ZTE' as vendor,
                             t2."SubNetwork_2_id" AS nename,
                             '{0}' AS mo,
                             '{1}' AS parameter,
                             t1."{1}" AS pvalue,
                             COUNT(t1."{1}") AS occurence
                         FROM
                             zte_cm."{0}" t1
                             INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
                         WHERE t2.is_current_load = true AND t1."{1}" IS NOT NULL
                         GROUP BY 
                             t1."SubNetwork_2_id", t1."{1}"
                        ON CONFLICT ON CONSTRAINT uq_parameter_value_counts
                        DO NOTHING
                         """.format(mo, parameter)

                self.engine.execute(text(value_qry))

    def compute_zte_4g_value_counts(self):
        """
        Runs baseline for Huawei 2G, 3G, or 2G/3G

        The is implemented by the same function because the MOs are defined on a controller i.e. BSC and RNC which
        are picked from the SYS MO.

        :param: string tech The technology.
        :return:
        """

        tech = "4G"

        # List of parameters to ignore
        ignore_list = ['LOADID', 'VARDATE', 'DATETIME', 'REGION', 'NENAME', 'CELLID', 'ID', 'FILENAME', 'TECHNOLOGY', 'VENDOR', 'VERSION', 'NETYPE', 'CELLNAME']

        self.logger.info("Processing ZTE baseline for {}...".format(tech))

        # Get list of mos configured in process_config
        result = self.engine.execute(text("SELECT * FROM baseline.process_config WHERE process = true AND technology = :tech AND vendor = :vendor"), tech=tech, vendor='ZTE')
        for row in result:
            vendor = row['vendor']
            technology = row['technology']
            mo = row['mo']

            self.logger.info("vendor:{}, technology:{}, mo:{}".format(vendor, technology, mo))

            # Get field names from information_schema
            field_qry = """
                    SELECT t1.column_name as field 
                    FROM
                        information_schema.columns t1
                        LEFT JOIN baseline.parameter_ignore_list t2 
                            ON t1.table_name = t2.mo
                            AND t1.column_name = t2.parameter
                    WHERE 
                        table_schema = 'zte_cm'
                        AND table_name = :mo
                        AND t2.parameter is NULL
                        AND UPPER(t1.column_name) NOT IN ('{}')
                """.format("','".join(ignore_list))

            field_result = self.engine.execute(text(field_qry), mo=mo)

            for f in field_result:
                parameter = f[0]

                self.logger.info("Processing baseline for {}.{}...".format(mo, parameter))

                value_qry = """
                    INSERT INTO  baseline.parameter_value_counts
                    (date_time, vendor, nename, mo, parameter, pvalue, occurence)
                    SELECT 
                        MAX(t1."DATETIME") AS date_time,
                        'ZTE' as vendor,
                        t2."SubNetwork_2_id" AS nename,
                        '{0}' AS mo,
                        '{1}' AS parameter,
                        t1."{1}" AS pvalue,
                        COUNT(t1."{1}") AS occurence
                    FROM
                        zte_cm."{0}" t1
                        INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
                    WHERE t2.is_current_load = true AND t1."{1}" IS NOT NULL
                    GROUP BY 
                        t1."SubNetwork_2_id", t1."{1}"
                ON CONFLICT ON CONSTRAINT uq_parameter_value_counts
                DO NOTHING
                    """.format(mo, parameter)

                self.engine.execute(text(value_qry))

    def delete_counts(self):
        """
        Delete entries from the parameter value counts table

        :return:
        """

        qry = "TRUNCATE TABLE baseline.parameter_value_counts"
        self.engine.execute(text(qry))

    def run_huawei_2g3g_audit(self, tech='2G'):

        vendor = 'HUAWEI'
        result = self.engine.execute(text("""
            SELECT 
            * 
            FROM baseline.network_baseline t1
            INNER JOIN baseline.process_config t2 
                ON t2.mo = t1.mo 
                AND t2.vendor =  t1.vendor
            WHERE
                t1.vendor = :vendor
                AND t2.technology = :tech
        """), tech=tech, vendor=vendor)

        for row in result:
            vendor = row[2]
            nename = row[3]
            mo = row[4]
            parameter = row[5]
            bvalue = row[6]


            sql = """
            INSERT INTO network_audit.network_baseline
            (pk, vendor, technology, nename, mo, parameter, bvalue, nvalue, age, modified_by, added_by, date_added, date_modified)
            SELECT 
            NEXTVAL('network_audit.seq_network_baseline_pk'),
            '{0}' AS "VENDOR",
            '{1}' AS "TECHNOLOGY",
            '{2}' AS nename,
            '{3}' AS mo,
            '{4}' AS parameter,
            '{5}' as bvalue,
            t1."{4}" as nvalue,
            0 as age,
            0 as modified_by,
            0 as added_by,
            now()::timestamp as date_added,
            now()::timestamp as date_modified 
            FROM
                huawei_cm."{3}" t1
                INNER JOIN huawei_cm."SYS" t2 
                    ON t2."FILENAME" = t1."FILENAME"
                    AND t2."LOADID" = t1."LOADID"
                INNER JOIN cm_loads t3 on t3.pk = t1."LOADID"
            WHERE t3.is_current_load = true 
                AND t1."{4}" IS NOT NULL
                AND t2."SYSOBJECTID" = '{2}'
                AND t1."{4}" != '{5}'
            ON CONFLICT ON CONSTRAINT unique_network_baseline
            DO
            UPDATE SET age = DATEDIFF( 'day', COALESCE(network_audit.network_baseline.date_added)::DATE, COALESCE(EXCLUDED.date_modified)::DATE ) ,
                       date_modified = network_audit.network_baseline.date_modified
            """.format(vendor, tech, nename,mo, parameter, bvalue)
            self.engine.execute(text(sql))

            # Delete old
            sql = """
                DELETE FROM 
                network_audit.network_baseline t1
                WHERE 
                (vendor, technology, nename, mo, parameter, bvalue, nvalue)
                NOT IN  (
                    SELECT 
                    '{0}' AS "VENDOR",
                    '{1}' AS "TECHNOLOGY",
                    '{2}' AS nename,
                    '{3}' AS mo,
                    '{4}' AS parameter,
                    '{5}' as bvalue,
                    t1."{4}" as nvalue 
                    FROM
                        huawei_cm."{3}" t1
                        INNER JOIN huawei_cm."SYS" t2 
                            ON t2."FILENAME" = t1."FILENAME"
                            AND t2."LOADID" = t1."LOADID"
                        INNER JOIN cm_loads t3 on t3.pk = t1."LOADID"
                    WHERE t3.is_current_load = true 
                        AND t1."{4}" IS NOT NULL
                        AND t2."SYSOBJECTID" = '{2}'
                        AND t1."{4}" != '{5}'
                )
                AND  t1.vendor = '{0}'
                AND t1.technology = '{1}'
                AND t1.nename = '{2}'
                AND t1.mo = '{3}'
                AND t1.parameter = '{4}'
            """.format(vendor, tech, nename,mo, parameter, bvalue)
            self.engine.execute(sql)

    def run_huawei_4g_audit(self):

        tech  = '4G'
        vendor = 'HUAWEI'

        result = self.engine.execute(text("""
            SELECT 
            * 
            FROM baseline.network_baseline t1
            INNER JOIN baseline.process_config t2 
                ON t2.mo = t1.mo 
                AND t2.vendor =  t1.vendor
            WHERE
                t1.vendor = :vendor
                AND t2.technology = :tech
        """), tech=tech, vendor=vendor)

        for row in result:
            vendor = row[2]
            nename = row[3]
            mo = row[4]
            parameter = row[5]
            bvalue = row[6]


            sql = """
            INSERT INTO network_audit.network_baseline
            (pk, vendor, technology, nename, mo, parameter, bvalue, nvalue, age, modified_by, added_by, date_added, date_modified)
            SELECT 
            NEXTVAL('network_audit.seq_network_baseline_pk'),
            '{0}' AS "VENDOR",
            '{1}' AS "TECHNOLOGY",
            '{2}' AS nename,
            '{3}' AS mo,
            '{4}' AS parameter,
            '{5}' as bvalue,
            t1."{4}" as nvalue,
            0 as age,
            0 as modified_by,
            0 as added_by,
            now()::timestamp as date_added,
            now()::timestamp as date_modified 
            FROM huawei_cm."{3}" t1
            INNER JOIN cm_loads t5 on t5.pk = t1."LOADID"
            INNER JOIN huawei_cm."CELL" t2
                ON t2."CELLID" = t1."LOCALCELLID"
                AND t2."LOADID" = t1."LOADID"
            INNER JOIN huawei_cm."ENODEBFUNCTION" t3 
                ON t3."ENODEBFUNCTIONNAME" =  t2."ENODEBFUNCTIONNAME"
                AND t3."LOADID" = t1."LOADID"
            INNER JOIN huawei_cm."CNOPERATORTA" t4 
                ON t4."ENODEBFUNCTIONNAME"  = t3."ENODEBFUNCTIONNAME"
                AND t4."LOADID" = t1."LOADID"
            WHERE t3.is_current_load = true 
                AND t1."{4}" IS NOT NULL
                AND t2."TAC" = '{2}'
                AND t1."{4}" != '{5}'
            ON CONFLICT ON CONSTRAINT unique_network_baseline
            DO
            UPDATE SET age = DATEDIFF( 'day', COALESCE(network_audit.network_baseline.date_added)::DATE, COALESCE(EXCLUDED.date_modified)::DATE ) ,
                       date_modified = network_audit.network_baseline.date_modified
            """.format(vendor, tech, nename,mo, parameter, bvalue)
            self.engine.execute(text(sql))

            # Delete old
            sql = """
                DELETE FROM 
                network_audit.network_baseline t1
                WHERE 
                (vendor, technology, nename, mo, parameter, bvalue, nvalue)
                NOT IN  (
                    SELECT 
                    '{0}' AS "VENDOR",
                    '{1}' AS "TECHNOLOGY",
                    '{2}' AS nename,
                    '{3}' AS mo,
                    '{4}' AS parameter,
                    '{5}' as bvalue,
                    t1."{4}" as nvalue 
                    FROM huawei_cm."{3}" t1
                    INNER JOIN cm_loads t5 on t5.pk = t1."LOADID"
                    INNER JOIN huawei_cm."CELL" t2
                        ON t2."CELLID" = t1."LOCALCELLID"
                        AND t2."LOADID" = t1."LOADID"
                    INNER JOIN huawei_cm."ENODEBFUNCTION" t3 
                        ON t3."ENODEBFUNCTIONNAME" =  t2."ENODEBFUNCTIONNAME"
                        AND t3."LOADID" = t1."LOADID"
                    INNER JOIN huawei_cm."CNOPERATORTA" t4 
                        ON t4."ENODEBFUNCTIONNAME"  = t3."ENODEBFUNCTIONNAME"
                        AND t4."LOADID" = t1."LOADID"
                    WHERE t3.is_current_load = true 
                        AND t1."{4}" IS NOT NULL
                        AND t2."TAC" = '{2}'
                        AND t1."{4}" != '{5}'
                )
                AND  t1.vendor = '{0}'
                AND t1.technology = '{1}'
                AND t1.nename = '{2}'
                AND t1.mo = '{3}'
                AND t1.parameter = '{4}'
            """.format(vendor, tech, nename,mo, parameter, bvalue)
            self.engine.execute(sql)

    def run_ericsson_2g_audit(self):

        tech  = '2G'
        vendor = 'ERICSSON'

        result = self.engine.execute(text("""
            SELECT 
            * 
            FROM baseline.network_baseline t1
            INNER JOIN baseline.process_config t2 
                ON t2.mo = t1.mo 
                AND t2.vendor =  t1.vendor
            WHERE
                t1.vendor = :vendor
                AND t2.technology = :tech
        """), tech=tech, vendor=vendor)

        for row in result:
            vendor = row[2]
            nename = row[3]
            mo = row[4]
            parameter = row[5]
            bvalue = row[6]


            sql = """
            INSERT INTO network_audit.network_baseline
            (pk, vendor, technology, nename, mo, parameter, bvalue, nvalue, age, modified_by, added_by, date_added, date_modified)
            SELECT 
            NEXTVAL('network_audit.seq_network_baseline_pk'),
            '{0}' AS "VENDOR",
            '{2}' AS nename,
            '{3}' AS mo,
            '{4}' AS parameter,
            '{5}' as bvalue,
            t1."{4}" as nvalue,
            0 as age,
            0 as modified_by,
            0 as added_by,
            now()::timestamp as date_added,
            now()::timestamp as date_modified 
            FROM
                ericsson_cm."{3}" t1
                INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
            WHERE t2.is_current_load = true 
                AND t1."{4}" IS NOT NULL
                AND t1."BSC_NAME" = '{2}'
                AND t1."{4}" != '{5}'
            ON CONFLICT ON CONSTRAINT unique_network_baseline
            DO
            UPDATE SET age = DATEDIFF( 'day', COALESCE(network_audit.network_baseline.date_added)::DATE, COALESCE(EXCLUDED.date_modified)::DATE ) ,
                       date_modified = network_audit.network_baseline.date_modified
            """.format(vendor, tech, nename,mo, parameter, bvalue)
            self.engine.execute(text(sql))

            # Delete old
            sql = """
                DELETE FROM 
                network_audit.network_baseline t1
                WHERE 
                (vendor, technology, nename, mo, parameter, bvalue, nvalue)
                NOT IN  (
                    SELECT 
                    '{0}' AS "VENDOR",
                    '{1}' AS "TECHNOLOGY",
                    '{2}' AS nename,
                    '{3}' AS mo,
                    '{4}' AS parameter,
                    '{5}' as bvalue,
                    t1."{4}" as nvalue 
                FROM
                    ericsson_cm."{3}" t1
                    INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
                WHERE t2.is_current_load = true 
                    AND t1."{4}" IS NOT NULL
                    AND t1."BSC_NAME" = '{2}'
                    AND t1."{4}" != '{5}'
                )
                AND  t1.vendor = '{0}'
                AND t1.technology = '{1}'
                AND t1.nename = '{2}'
                AND t1.mo = '{3}'
                AND t1.parameter = '{4}'
            """.format(vendor, tech, nename,mo, parameter, bvalue)
            self.engine.execute(sql)

    def run_ericsson_3g_audit(self):

        tech  = '3G'
        vendor = 'ERICSSON'

        result = self.engine.execute(text("""
            SELECT 
            * 
            FROM baseline.network_baseline t1
            INNER JOIN baseline.process_config t2 
                ON t2.mo = t1.mo 
                AND t2.vendor =  t1.vendor
            WHERE
                t1.vendor = :vendor
                AND t2.technology = :tech
        """), tech=tech, vendor=vendor)

        for row in result:
            vendor = row[2]
            nename = row[3]
            mo = row[4]
            parameter = row[5]
            bvalue = row[6]


            sql = """
            INSERT INTO network_audit.network_baseline
            (pk, vendor, technology, nename, mo, parameter, bvalue, nvalue, age, modified_by, added_by, date_added, date_modified)
            SELECT 
            NEXTVAL('network_audit.seq_network_baseline_pk'),
            '{0}' AS "VENDOR",
            '{1}' AS "TECHNOLOGY",
            '{2}' AS nename,
            '{3}' AS mo,
            '{4}' AS parameter,
            '{5}' as bvalue,
            t1."{4}" as nvalue,
            0 as age,
            0 as modified_by,
            0 as added_by,
            now()::timestamp as date_added,
            now()::timestamp as date_modified 
            FROM
                ericsson_cm."{3}" t1
                INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
            WHERE t2.is_current_load = true 
                AND t1."{4}" IS NOT NULL
                AND t1."SubNetwork_2_id" = '{2}'
                AND t1."{4}" != '{5}'
            ON CONFLICT ON CONSTRAINT unique_network_baseline
            DO
            UPDATE SET age = DATEDIFF( 'day', COALESCE(network_audit.network_baseline.date_added)::DATE, COALESCE(EXCLUDED.date_modified)::DATE ) ,
                       date_modified = network_audit.network_baseline.date_modified
            """.format(vendor, tech, nename,mo, parameter, bvalue)
            self.engine.execute(text(sql))

            # Delete old
            sql = """
                DELETE FROM 
                network_audit.network_baseline t1
                WHERE 
                (vendor, technology, nename, mo, parameter, bvalue, nvalue)
                NOT IN  (
                    SELECT 
                    '{0}' AS "VENDOR",
                    '{1}' AS "TECHNOLOGY",
                    '{2}' AS nename,
                    '{3}' AS mo,
                    '{4}' AS parameter,
                    '{5}' as bvalue,
                    t1."{4}" as nvalue 
                FROM
                    ericsson_cm."{3}" t1
                    INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
                WHERE t2.is_current_load = true 
                    AND t1."{4}" IS NOT NULL
                    AND t1."SubNetwork_2_id" = '{2}'
                    AND t1."{4}" != '{5}'
                )
                AND  t1.vendor = '{0}'
                AND t1.technology = '{1}'
                AND t1.nename = '{2}'
                AND t1.mo = '{3}'
                AND t1.parameter = '{4}'
            """.format(vendor, tech, nename,mo, parameter, bvalue)
            self.engine.execute(sql)

    def run_ericsson_4g_audit(self):

        tech  = '4G'
        vendor = 'ERICSSON'
        result = self.engine.execute(text("""
            SELECT 
            * 
            FROM baseline.network_baseline t1
            INNER JOIN baseline.process_config t2 
                ON t2.mo = t1.mo 
                AND t2.vendor =  t1.vendor
            WHERE
                t1.vendor = :vendor
                AND t2.technology = :tech
        """), tech=tech, vendor=vendor)

        for row in result:
            vendor = row[2]
            nename = row[3]
            mo = row[4]
            parameter = row[5]
            bvalue = row[6]


            sql = """
            INSERT INTO network_audit.network_baseline
            (pk, vendor, technology, nename, mo, parameter, bvalue, nvalue, age, modified_by, added_by, date_added, date_modified)
            SELECT 
            NEXTVAL('network_audit.seq_network_baseline_pk'),
            '{0}' AS "VENDOR",
            '{1}' AS "TECHNOLOGY",
            '{2}' AS nename,
            '{3}' AS mo,
            '{4}' AS parameter,
            '{5}' as bvalue,
            t1."{4}" as nvalue,
            0 as age,
            0 as modified_by,
            0 as added_by,
            now()::timestamp as date_added,
            now()::timestamp as date_modified 
            FROM
                ericsson_cm."{3}" t1
                INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
            WHERE t2.is_current_load = true 
                AND t1."{4}" IS NOT NULL
                AND t1."SubNetwork_2_id" = '{2}'
                AND t1."{4}" != '{5}'
            ON CONFLICT ON CONSTRAINT unique_network_baseline
            DO
            UPDATE SET age = DATEDIFF( 'day', COALESCE(network_audit.network_baseline.date_added)::DATE, COALESCE(EXCLUDED.date_modified)::DATE ) ,
                       date_modified = network_audit.network_baseline.date_modified
            """.format(vendor, tech, nename,mo, parameter, bvalue)
            self.engine.execute(text(sql))

            # Delete old
            sql = """
                DELETE FROM 
                network_audit.network_baseline t1
                WHERE 
                (vendor, technology, nename, mo, parameter, bvalue, nvalue)
                NOT IN  (
                    SELECT 
                    '{0}' AS "VENDOR",
                    '{1}' AS "TECHNOLOGY",
                    '{2}' AS nename,
                    '{3}' AS mo,
                    '{4}' AS parameter,
                    '{5}' as bvalue,
                    t1."{4}" as nvalue 
                FROM
                    ericsson_cm."{3}" t1
                    INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
                WHERE t2.is_current_load = true 
                    AND t1."{4}" IS NOT NULL
                    AND t1."SubNetwork_2_id" = '{2}'
                    AND t1."{4}" != '{5}'

                )
                AND  t1.vendor = '{0}'
                AND t1.technology = '{1}'
                AND t1.nename = '{2}'
                AND t1.mo = '{3}'
                AND t1.parameter = '{4}'
            """.format(vendor, tech, nename,mo, parameter, bvalue)
            self.engine.execute(sql)

    def run_zte_2g_audit(self):

        tech  = '2G'
        vendor = 'ZTE'

        result = self.engine.execute(text("""
            SELECT 
            * 
            FROM baseline.network_baseline t1
            INNER JOIN baseline.process_config t2 
                ON t2.mo = t1.mo 
                AND t2.vendor =  t1.vendor
            WHERE
                t1.vendor = :vendor
                AND t2.technology = :tech
        """), tech=tech, vendor=vendor)

        for row in result:
            vendor = row[2]
            nename = row[3]
            mo = row[4]
            parameter = row[5]
            bvalue = row[6]


            sql = """
            INSERT INTO network_audit.network_baseline
            (pk, vendor, technology, nename, mo, parameter, bvalue, nvalue, age, modified_by, added_by, date_added, date_modified)
            SELECT 
            NEXTVAL('network_audit.seq_network_baseline_pk'),
            '{0}' AS "VENDOR",
            '{1}' AS "TECHNOLOGY",
            '{2}' AS nename,
            '{3}' AS mo,
            '{4}' AS parameter,
            '{5}' as bvalue,
            t1."{4}" as nvalue,
            0 as age,
            0 as modified_by,
            0 as added_by,
            now()::timestamp as date_added,
            now()::timestamp as date_modified 
            FROM
                ericsson_cm."{3}" t1
                INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
            WHERE t2.is_current_load = true 
                AND t1."{4}" IS NOT NULL
                AND t1."SubNetwork_2_id" = '{2}'
                AND t1."{4}" != '{5}'
            ON CONFLICT ON CONSTRAINT unique_network_baseline
            DO
            UPDATE SET age = DATEDIFF( 'day', COALESCE(network_audit.network_baseline.date_added)::DATE, COALESCE(EXCLUDED.date_modified)::DATE ) ,
                       date_modified = network_audit.network_baseline.date_modified
            """.format(vendor, tech, nename,mo, parameter, bvalue)
            self.engine.execute(text(sql))

            # Delete old
            sql = """
                DELETE FROM 
                network_audit.network_baseline t1
                WHERE 
                (vendor, technology, nename, mo, parameter, bvalue, nvalue)
                NOT IN  (
                    SELECT 
                    '{0}' AS "VENDOR",
                    '{1}' AS "TECHNOLOGY",
                    '{2}' AS nename,
                    '{3}' AS mo,
                    '{4}' AS parameter,
                    '{5}' as bvalue,
                    t1."{4}" as nvalue 
                FROM
                    ericsson_cm."{3}" t1
                    INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
                WHERE t2.is_current_load = true 
                    AND t1."{4}" IS NOT NULL
                    AND t1."SubNetwork_2_id" = '{2}'
                    AND t1."{4}" != '{5}'
                )
                AND  t1.vendor = '{0}'
                AND t1.technology = '{1}'
                AND t1.nename = '{2}'
                AND t1.mo = '{3}'
                AND t1.parameter = '{4}'
            """.format(vendor, tech, nename,mo, parameter, bvalue)
            self.engine.execute(sql)

    def run_zte_3g_audit(self):

        tech  = '3G'
        vendor = 'ZTE'

        result = self.engine.execute(text("""
            SELECT 
            * 
            FROM baseline.network_baseline t1
            INNER JOIN baseline.process_config t2 
                ON t2.mo = t1.mo 
                AND t2.vendor =  t1.vendor
            WHERE
                t1.vendor = :vendor
                AND t2.technology = :tech
        """), tech=tech, vendor=vendor)

        for row in result:
            vendor = row[2]
            nename = row[3]
            mo = row[4]
            parameter = row[5]
            bvalue = row[6]


            sql = """
            INSERT INTO network_audit.network_baseline
            (pk, vendor, technology, nename, mo, parameter, bvalue, nvalue, age, modified_by, added_by, date_added, date_modified)
            SELECT 
            NEXTVAL('network_audit.seq_network_baseline_pk'),
            '{0}' AS "VENDOR",
            '{1}' AS "TECHNOLOGY",
            '{2}' AS nename,
            '{3}' AS mo,
            '{4}' AS parameter,
            '{5}' as bvalue,
            t1."{4}" as nvalue,
            0 as age,
            0 as modified_by,
            0 as added_by,
            now()::timestamp as date_added,
            now()::timestamp as date_modified 
            FROM
                ericsson_cm."{3}" t1
                INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
            WHERE t2.is_current_load = true 
                AND t1."{4}" IS NOT NULL
                AND t1."SubNetwork_2_id" = '{2}'
                AND t1."{4}" != '{5}'
            ON CONFLICT ON CONSTRAINT unique_network_baseline
            DO
            UPDATE SET age = DATEDIFF( 'day', COALESCE(network_audit.network_baseline.date_added)::DATE, COALESCE(EXCLUDED.date_modified)::DATE ) ,
                       date_modified = network_audit.network_baseline.date_modified
            """.format(vendor, tech, nename,mo, parameter, bvalue)
            self.engine.execute(text(sql))

            # Delete old
            sql = """
                DELETE FROM 
                network_audit.network_baseline t1
                WHERE 
                (vendor, technology, nename, mo, parameter, bvalue, nvalue)
                NOT IN  (
                    SELECT 
                    '{0}' AS "VENDOR",
                    '{1}' AS "TECHNOLOGY",
                    '{2}' AS nename,
                    '{3}' AS mo,
                    '{4}' AS parameter,
                    '{5}' as bvalue,
                    t1."{4}" as nvalue 
                    FROM
                        ericsson_cm."{3}" t1
                        INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
                    WHERE t2.is_current_load = true 
                        AND t1."{4}" IS NOT NULL
                        AND t1."SubNetwork_2_id" = '{2}'
                        AND t1."{4}" != '{5}'

                )
                AND  t1.vendor = '{0}'
                AND t1.technology = '{1}'
                AND t1.nename = '{2}'
                AND t1.mo = '{3}'
                AND t1.parameter = '{4}'
            """.format(vendor, tech, nename,mo, parameter, bvalue)
            self.engine.execute(sql)

    def run_zte_4g_audit(self):

        tech  = '4G'
        vendor = 'ZTE'

        result = self.engine.execute(text("""
            SELECT 
            * 
            FROM baseline.network_baseline t1
            INNER JOIN baseline.process_config t2 
                ON t2.mo = t1.mo 
                AND t2.vendor =  t1.vendor
            WHERE
                t1.vendor = :vendor
                AND t2.technology = :tech
        """), tech=tech, vendor=vendor)

        for row in result:
            vendor = row[2]
            nename = row[3]
            mo = row[4]
            parameter = row[5]
            bvalue = row[6]


            sql = """
            INSERT INTO network_audit.network_baseline
            (pk, vendor, technology, nename, mo, parameter, bvalue, nvalue, age, modified_by, added_by, date_added, date_modified)
            SELECT 
            NEXTVAL('network_audit.seq_network_baseline_pk'),
            '{0}' AS "VENDOR",
            '{1}' AS "TECHNOLOGY",
            '{2}' AS nename,
            '{3}' AS mo,
            '{4}' AS parameter,
            '{5}' as bvalue,
            t1."{4}" as nvalue,
            0 as age,
            0 as modified_by,
            0 as added_by,
            now()::timestamp as date_added,
            now()::timestamp as date_modified 
            FROM
                ericsson_cm."{3}" t1
                INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
            WHERE t2.is_current_load = true 
                AND t1."{4}" IS NOT NULL
                AND t1."SubNetwork_2_id" = '{2}'
                AND t1."{4}" != '{5}'
            ON CONFLICT ON CONSTRAINT unique_network_baseline
            DO
            UPDATE SET age = DATEDIFF( 'day', COALESCE(network_audit.network_baseline.date_added)::DATE, COALESCE(EXCLUDED.date_modified)::DATE ) ,
                       date_modified = network_audit.network_baseline.date_modified
            """.format(vendor, tech, nename,mo, parameter, bvalue)
            self.engine.execute(text(sql))

            # Delete old
            sql = """
                DELETE FROM 
                network_audit.network_baseline t1
                WHERE 
                (vendor, technology, nename, mo, parameter, bvalue, nvalue)
                NOT IN  (
                    SELECT 
                    '{0}' AS "VENDOR",
                    '{1}' AS "TECHNOLOGY",
                    '{2}' AS nename,
                    '{3}' AS mo,
                    '{4}' AS parameter,
                    '{5}' as bvalue,
                    t1."{4}" as nvalue 
                FROM
                    ericsson_cm."{3}" t1
                    INNER JOIN cm_loads t2 on t2.pk = t1."LOADID"
                WHERE t2.is_current_load = true 
                    AND t1."{4}" IS NOT NULL
                    AND t1."SubNetwork_2_id" = '{2}'
                    AND t1."{4}" != '{5}'
                )
                AND  t1.vendor = '{0}'
                AND t1.technology = '{1}'
                AND t1.nename = '{2}'
                AND t1.mo = '{3}'
                AND t1.parameter = '{4}'
            """.format(vendor, tech, nename,mo, parameter, bvalue)
            self.engine.execute(sql)

    def run_baseline_audit(self):
        # network_baseline

        # Huawei
        self.run_huawei_2g3g_audit('2G')
        self.run_huawei_2g3g_audit('3G')
        self.run_huawei_4g_audit()

        # Ericsson
        self.run_ericsson_2g_audit()
        self.run_ericsson_3g_audit()
        self.run_ericsson_4g_audit()

        # ZTE
        self.run_zte_2g_audit()
        self.run_zte_3g_audit()
        self.run_zte_4g_audit()

