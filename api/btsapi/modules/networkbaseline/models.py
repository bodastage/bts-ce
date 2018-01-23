from flask_sqlalchemy import SQLAlchemy, BaseQuery
from btsapi import db, ma;
import datetime


class NetworkBaseline(db.Model):
    """Network Base line model"""

    __tablename__ = 'base_live_values'
    __table_args__ = {'schema': 'live_network'}

    pk = db.Column(db.Integer, db.Sequence('seq_base_live_values_pk', ), primary_key=True, nullable=False)
    parameter_pk = db.Column(db.Integer, nullable=False)
    value = db.Column(db.String(200))
    modified_by = db.Column(db.Integer)
    added_by = db.Column(db.Integer)
    date_added = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    date_modified = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow)


class NetworkBaselineSchema(ma.ModelSchema):
    """Network Base line marshmallow schema"""
    class Meta:
        model = NetworkBaseline


class NetworkBaselineView(db.Model):
    """Network Baseline view model"""
    __tablename__ = 'vw_baseline'
    __table_args__ = {'schema': 'live_network'}

    vendor = db.Column(db.String(50))
    technology = db.Column(db.String(50))
    mo = db.Column(db.String(200), primary_key=True)
    parameter = db.Column(db.String(200), primary_key=True)
    value = db.Column(db.String(200))
    date_added = db.Column(db.String(50))
    date_modified = db.Column(db.String(50))


class NetworkBaselineViewSchema(ma.ModelSchema):
    """Network Baseline view marshmallow schema"""
    class Meta:
        model = NetworkBaselineView
