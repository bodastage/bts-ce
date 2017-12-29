from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

class ProcessCMData(object):
	""" Process network configuration data"""
	
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
		
		self.db_engine = create_engine('postgresql://{0}:{1}@{2}/{3}'.format(self._dbuser, self._dbpass, self._dbhost, self._dbname))
		
	def extract_rncs(self, vendor= None):
		"""Extract RNCs from provided vendors or else get from all vendors"""
		
		if vendor == None or vendor == 'ericsson':
			extract_ericsson_rncs()
		
	def extract_ericsson_rncs():
		"""Extract RNCs from ericsson CM data"""
		Session = sessionmaker(bind=self.db_engine)
		session = Session()
		
		sql = """
			SELECT 
			"varDateTime" as date_added, 
			"varDateTime" as date_modified, 
			'RNC' as node_type,
			"MeContext_id" as "name" , 
			1 as vendor_pk, -- 1=Ericsson, 2=Huawei
			2 as tech_pk , -- 1=gsm, 2-umts,3=lte
			0 as added_by,
			0 as modified_by
			FROM eri_cm_3g4g.rncfunction t1
			LEFT OUTER  JOIN live_network.nodes t2 ON t1."MeContext_id" = t2."name"
			WHERE 
			t2."name" IS NULL
		"""
		
		engine.execute(text(sql).execution_options(autocommit=True))
		
		session.close()
		
	def extract_ericsson_enodebs():
		"""Extract Ericsson ENodebs"""
		pass
		
	def extract_ericsson_3g_sites():
		"""Extract Ericsson NodeBs"""
		pass
		
	def extract_ericsson_3g_cells():
		"""Extract Ericsson UTMS Cells"""
		pass
		
	def extract_ericsson_4g_cells():
		"""Extract Ericsson LTE Cells"""
		pass
		
	def extract_ericsson_4g_cell_params():
		"""Extract Ericsson LTE cell parameters"""
		pass
		
	def extract_ericsson_3g_cell_params():
		"""Extract Ericsson UMTS cell parameters"""
		pass
		
	def extract_ericsson_3g2g_nbrs():
		"""Extract Ericsson UMTS-GSM neighbour relations"""
		pass
		
	def extract_ericsson_3g3g_nbrs():
		"""Extract Ericsson UMTS-UMTS neighbour relations"""
		pass
		
		
	def extract_ericsson_3g4g_nbrs():
		"""Extract Ericsson UMTS-LTE neighbour relations"""
		pass
		
	def extract_ericsson_4g4g_nbrs():
		"""Extract Ericsson LTE-LTE neighbour relations"""
		pass