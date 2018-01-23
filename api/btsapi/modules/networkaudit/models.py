from flask_sqlalchemy import SQLAlchemy
from btsapi.extensions import db, ma
import datetime


class AuditCategory(db.Model):
    """Audit category"""
    __tablename__ = 'audit_categories'
    __table_args__ = {'schema': 'network_audit'}

    pk = db.Column(db.Integer, db.Sequence('seq_audit_categories_pk', ), primary_key=True, nullable=False)
    name = db.Column(db.String(255), nullable=False)
    notes = db.Column(db.Text)
    parent_pk = db.Column(db.Integer, nullable=False, default=0)
    modified_by = db.Column(db.Integer)
    added_by = db.Column(db.Integer)
    date_added = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    date_modified = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow)
    in_built  = db.Column(db.Boolean)


class AuditRule(db.Model):
    """Audit rule model"""
    __tablename__ = 'audit_rules'
    __table_args__ = {'schema': 'network_audit'}

    pk = db.Column(db.Integer, db.Sequence('seq_audit_rules_pk', ), primary_key=True, nullable=False)
    name = db.Column(db.String(255), nullable=False)
    notes = db.Column(db.Text)
    category_pk = db.Column(db.Integer, nullable=False, default=0)
    modified_by = db.Column(db.Integer)
    added_by = db.Column(db.Integer)
    date_added = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    date_modified = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow)
    in_built  = db.Column(db.Boolean)
    table_name = db.Column(db.String(200))
    sql = db.Column(db.Text)
    last_run_date = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    first_run_date = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)