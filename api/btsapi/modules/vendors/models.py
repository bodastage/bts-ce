from flask_sqlalchemy import SQLAlchemy
from btsapi.common.model_helpers import dump_datetime;
from btsapi import db, ma;
import datetime

class Vendor(db.Model):
    """Vendors model"""

    __tablename__ = 'vendors'

    pk = db.Column(db.Integer,  db.Sequence('seq_vendors_pk',), primary_key=True, nullable=False )
    name = db.Column(db.String(255), unique=True, nullable=False)
    notes = db.Column(db.Text)
    modified_by = db.Column(db.Integer)
    added_by = db.Column(db.Integer)
    date_added = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    date_modified = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow)

    def __init__(self,name,notes,modified_by,added_by):
        self.name = name
        self.notes = notes
        self.modified_by = modified_by
        self.added_by = added_by


class VendorSchema(ma.ModelSchema):
    """Flask Marshmallow Schema for Vendor model"""
    class Meta:
        model = Vendor
