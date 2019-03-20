import psycopg2
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import os

class NokiaCM(object):

    def __init__(self):
        ''' Constructor for this class. '''

        #@TODO: Refactor
        sqlalchemy_db_uri = 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(
            os.getenv("BTS_DB_USER", "bodastage"),
            os.getenv("BTS_DB_PASS", "password"),
            os.getenv("BTS_DB_HOST", "database"),
            os.getenv("BTS_DB_PORT", "5432"),
            os.getenv("BTS_DB_NAME", "bts"),
        )

        self.engine = create_engine(sqlalchemy_db_uri)


    def extract_live_network_bscs(self):
        pass

    def extract_live_network_rncs(self):
        pass

    def extract_live_network_enodebs(self):
        pass

    def extract_live_network_2g_sites(self):
        pass

    def extract_live_network_2g_cells(self):
        pass

    def extract_live_network_2g_cells_params(self):
        pass

    def extract_live_network_3g_sites(self):
        pass

    def extract_live_network_3g_cells(self):
        pass

    def extract_live_network_3g_cells_params(self):
        pass

    def extract_live_network_4g_cells(self):
        pass

    def extract_live_network_2g_externals_on_2g(self):
        pass

    def extract_live_network_2g_externals_on_3g(self):
        pass

    def extract_live_network_2g_externals_on_4g(self):
        pass

    def extract_live_network_3g_externals_on_2g(self):
        pass

    def extract_live_network_3g_externals_on_3g(self):
        pass

    def extract_live_network_3g_externals_on_4g(self):
        pass

    def extract_live_network_4g_externals_on_2g(self):
        pass

    def extract_live_network_4g_externals_on_3g(self):
        pass

    def extract_live_network_4g_externals_on_4g(self):
        pass

    def extract_live_network_externals_on_2g(self):
        self.extract_live_network_2g_externals_on_2g()
        self.extract_live_network_3g_externals_on_2g()
        self.extract_live_network_4g_externals_on_2g()

    def extract_live_network_externals_on_3g(self):
        self.extract_live_network_2g_externals_on_3g()
        self.extract_live_network_3g_externals_on_3g()
        self.extract_live_network_4g_externals_on_3g()

    def extract_live_network_externals_on_4g(self):
        self.extract_live_network_2g_externals_on_4g()
        self.extract_live_network_3g_externals_on_4g()
        self.extract_live_network_4g_externals_on_4g()

    def extract_live_network_4g_cells_params(self):
        pass

    def extract_live_network_2g2g_nbrs(self):
        pass

    def extract_live_network_2g3g_nbrs(self):
        pass

    def extract_live_network_2g4g_nbrs(self):
        pass

    def extract_live_network_3g2g_nbrs(self):
        pass

    def extract_live_network_3g3g_nbrs(self):
        pass

    def extract_live_network_3g4g_nbrs(self):
        pass

    def extract_live_network_4g2g_nbrs(self):
        pass

    def extract_live_network_4g3g_nbrs(self):
        pass

    def extract_live_network_4g4g_nbrs(self):
        pass
