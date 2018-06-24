#
# ZTE related

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import os
import subprocess
import logging

# SQLAlchemy database engine
engine = create_engine('postgresql://bodastage:password@database/bts')

class ZTECM(object):
    """Process ZTE  configuration management data"""

    def __init__(self):
        self.db_engine = create_engine('postgresql://bodastage:password@database/bts')




