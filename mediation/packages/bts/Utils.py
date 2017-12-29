from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

class Utils(object):
	"""Utility class"""
	
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
		
	def truncate_schema_tables(self, schema = "public", tables = None):
		"""Truncate cm tables"""
		engine = create_engine('postgresql://{0}:{1}@{2}/{3}'.format(self._dbuser, self._dbpass, self._dbhost, self._dbname))

		Session = sessionmaker(bind=engine)
		session = Session()
		
		sql="""
		SELECT table_name FROM information_schema.tables 
		WHERE table_schema = '{0}'
		""".format(schema)
		
		result = engine.execute(sql)
				
		for row in result:
			table=row[0]
			qry="truncate {0}.{1}".format(schema,table)
			engine.execute(text(qry).execution_options(autocommit=True))