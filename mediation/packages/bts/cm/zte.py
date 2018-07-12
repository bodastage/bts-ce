#
# ZTE related

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import os
import subprocess
import logging


class ZTECM(object):
    """Process ZTE  configuration management data"""

    def __init__(self):
        self.db_engine = create_engine('postgresql://bodastage:password@database/bts')

    def extract_live_network_bscs(self):
        pass

    def extract_live_network_2g_sites(self):
        """
        Extract live network 2G sites
        :return:
        """
        pass

    def extract_live_network_2g_cells(self):
        """
        Extract live network 2G cells
        :return:
        """
        pass

    def extract_live_network_2g_cell_params(self):
        """
        Extract live network 2G cell parameters
        """

        pass

    def extract_live_network_2g_externals(self):
        """
        Extrfact live network 2G externals
        """
        pass

    def extract_live_network_3g_externals(self):
        pass

    def extract_live_network_4g_externals(self):
        pass

    def extract_live_network_rncs(self):
        """
        Extract 3G RNCs
        """
        pass

    def extract_live_network_3g_sites(self):
        """
        Extract live network 3G nodeBs
        """
        pass

    def extract_live_network_3g_cells(self):
        """
        Extract live network 3G cells
        """
        pass

    def extract_live_network_3g_cell_params(self):
        """
        Extract live network 3G cell parameters
        """
        pass

    def extract_live_network_3g_relations(self):
        pass









