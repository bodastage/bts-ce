from flask_sqlalchemy import SQLAlchemy
from btsapi.extensions import db, ma
import datetime

# @TODO: Add foreign key constraints
class ManagedObject(db.Model):
    """ManagedObject model"""

    __tablename__ = 'managedobjects'

    pk = db.Column(db.Integer, db.Sequence('seq_managedobjects_pk', ), primary_key=True, nullable=False)
    label = db.Column(db.String(255))
    name = db.Column(db.String(255), nullable=False)
    notes = db.Column(db.Text)
    parent_pk = db.Column(db.Integer, nullable=False)
    tech_pk = db.Column(db.Integer, nullable=False)
    vendor_pk = db.Column(db.Integer, nullable=False)
    modified_by = db.Column(db.Integer)
    added_by = db.Column(db.Integer)
    date_added = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    date_modified = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow)


class ManagedObjectSchema(db.Model):
    """Managed objects schemas table"""

    __tablename__ = 'managedobjects_schemas'

    pk = db.Column(db.Integer, db.Sequence('seq_managedobjects_schemas_pk', ), primary_key=True, nullable=False)
    name = db.Column(db.String(255), nullable=False)
    notes = db.Column(db.Text)
    tech_pk = db.Column(db.Integer, nullable=False)
    vendor_pk = db.Column(db.Integer, nullable=False)
    modified_by = db.Column(db.Integer)
    added_by = db.Column(db.Integer)
    date_added = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    date_modified = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow)


class ManagedObjectsMASchema(ma.ModelSchema):
    """Flask Marshmallow Schema for Managed objects model"""
    class Meta:
        model = ManagedObject