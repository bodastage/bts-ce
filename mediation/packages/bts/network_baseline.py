import psycopg2

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

