import psycopg2
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text


# @todo: use logger
class NetworkBaseLine(object):

    def __init__(self, dbname = None, dbuser = None, dbpass = None, dbhost = None):
        ''' Constructor for this class. '''
        pass
 
 
    def run(self,vendor_id, tech_id):
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
            WHERE tech_pk = %s and vendor_pk = %s""", (tech_id, vendor_id))

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
                    FROM  {0}.{1}
                    WHERE "{2}" IS NOT NULL
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

                base_line_value  = str(parameter_value[0]).strip()
                print ("base_line_value:{0}".format(base_line_value) )

                # if base_line_value is None: continue

                #Skip values greater than 200 characters
                #if len(base_line_value) > 200: continue

                #Insert base line value
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
        """Generate Huawei 2G baseline descripancies"""
        engine = create_engine('postgresql://bodastage:password@database/bts')
        vendor_pk = 2
        tech_pk  = 1
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

            attr_list = [ p[0] for p in parameters ]

            str_param_values = ",".join([ "t_mo.{0}{1}{0}".format('"',p) for p in attr_list] )
            str_param_names  = ",".join([ "{0}{1}{0}".format('\'', p) for p in attr_list])

            cell_level_join  = ""

            if mo_affect_level == 1 :
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

        #
        # for row in baseline_values:
        #     bvalue, pname, mo, affect_level = row
        #
        #     sql = """
        #        SELECT "{2}" AS parameter, count(1) as cnt
        #        FROM  {0}.{1}
        #        GROUP BY "{2}"
        #        ORDER BY cnt DESC
        #        LIMIT 1
        #     """.format(schema_name, mo, pname)
        #
        #     parameter_value = ""
        #
        #     try:
        #         cur.execute(sql)
        #         parameter_value = cur.fetchone()
        #     except:
        #         continue
        #
        #     base_line_value = str(parameter_value[0]).strip()

        #
        #
        # Session = sessionmaker(bind=engine)
        # session = Session()
        #
        # metadata = MetaData()
        # managed_objects_table = Table('managedobjects', metadata, autoload=True, autoload_with=engine)
        # baseline_parameter_config = Table('baseline_parameter_config', metadata, autoload=True, autoload_with=engine, schema='live_network')
        # vendor_parameters = Table('vendor_parameters', metadata, autoload=True, autoload_with=engine)
        #
        # baseline_mos = session.query(managed_objects_table, vendor_parameters,managed_objects_table.c.name).\
        #     filter_by(vendor_pk=vendor_pk, tech_pk=tech_pk).\
        #     join(baseline_parameter_config, baseline_parameter_config.c.mo_pk==managed_objects_table.c.pk).\
        #     join(vendor_parameters, vendor_parameters.c.pk==baseline_parameter_config.c.parameter_pk).\
        #     all()
        #
        # print(baseline_mos[0])

        # for m,v,w in baseline_mos:
        #     # print(m)
        #     # print(v)
        #     print(w)
        #     print("-------------")
        # print(managed_objects)




