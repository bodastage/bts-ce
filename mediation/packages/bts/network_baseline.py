import psycopg2

class NetworkBaseLine(object):

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
 
 
    def run(self):
        """Run network baseline"""
        conn = psycopg2.connect("dbname={0} user={1} password={2} host={3}".format(self._dbname, self._dbuser, self._dbpass, self._dbhost))

        conn.autocommit = True

        cur = conn.cursor()

        # Get MOs
        # UMTS, Ericsson
        cur.execute("""SELECT pk, "name" FROM managedobjects WHERE tech_pk = %s and vendor_pk = %s""", (2,1))

        mos = cur.fetchall()

        print(mos)

        for idx in range(len(mos)):
            mo_name = mos[idx][1]
            mo_pk = str(mos[idx][0])

            print("mo_name: {0} mo_pk: {1}".format(mo_name, mo_pk))
            # Iterate through the parameters
            cur.execute("""SELECT pk, "name" FROM vendor_parameters where parent_pk = %s """, (mo_pk,))

            parameters = cur.fetchall()
            for i in range(len(parameters)):
                parameter_pk = parameters[i][0]
                parameter_name = parameters[i][1]

                sql = """
                    SELECT "{0}" AS parameter, count(1) as cnt
                    FROM  eri_cm_3g4g.{1}
                    GROUP BY "{0}"
                    ORDER BY cnt DESC
                    LIMIT 1
                """.format(parameter_name, mo_name)

                parameter_value = ""

                try:
                    cur.execute(sql)
                    parameter_value = cur.fetchone()
                except:
                    continue

                print(sql)
                print (parameter_value)
                if parameter_value == None: continue

                print (parameter_value)

                base_line_value  = parameter_value[0]
                print ("base_line_value:{0}".format(base_line_value) )

                if base_line_value is None: continue

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

