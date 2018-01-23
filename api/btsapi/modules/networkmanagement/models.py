from flask_sqlalchemy import SQLAlchemy
from btsapi.extensions import db, ma
import datetime
from sqlalchemy import Table, MetaData, inspect


metadata = MetaData()

# @TODO: Write table definitions out. only reflection for the data tables

class LiveCell(db.Model):
    """Create live network cells data model"""
    __table__ = Table('cells', metadata, autoload=True, autoload_with=db.engine, schema='live_network')
    pass


class LiveNode(db.Model):
    """Create live network cells data model"""
    __table__ = Table('nodes', metadata, autoload=True, autoload_with=db.engine, schema='live_network')
    pass


class LiveSite(db.Model):
    """Create live network cells data model"""
    __table__ = Table('sites', metadata, autoload=True, autoload_with=db.engine, schema='live_network')
    pass


class LiveCell3G(db.Model):
    """Create live network UMTS cells data model"""
    __table__ = Table('umts_cells_data', metadata, autoload=True, autoload_with=db.engine, schema='live_network')
    pass


class LiveCell3GMASchema(ma.ModelSchema):
    """Live network UMTS cells marshmallow model"""
    class Meta:
        model = LiveCell3G
