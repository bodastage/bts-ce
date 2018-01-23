from flask import Blueprint, request, render_template, \
                  flash, g, session, redirect, url_for, \
                  jsonify, make_response
from btsapi.modules.managedobjects.models import ManagedObject, ManagedObjectSchema, ManagedObjectsMASchema
from btsapi.extensions import db
import datetime
from datatables import DataTables, ColumnDT
from sqlalchemy import  text, Table, MetaData
import json
from btsapi import app
from flask_login import login_required

mod_managedobjects = Blueprint('managedobjects', __name__, url_prefix='/api/managedobjects')


@mod_managedobjects.route('/tree/<int:parent_pk>/', methods=['GET'])
@login_required
def get_aci_tree_data(parent_pk):
    """Get aci tree data for managed objects"""
    vendor_pk = request.args.get('vendor_pk', None)
    tech_pk = request.args.get('tech_pk', None)
    swversion_pk = request.args.get('swversion_pk', None)
    search_term = request.args.get('search_term', None)

    mo_aci_entries = []
    query = None

    if vendor_pk is not None and tech_pk is not None and swversion_pk is None and parent_pk is None:
        query = ManagedObject.query.filter_by(vendor_pk=vendor_pk,tech_pk=tech_pk)

    if vendor_pk is not None and tech_pk is None and swversion_pk is None and parent_pk is None:
        query = ManagedObject.query.filter_by(vendor_pk=vendor_pk)

    if vendor_pk is not None and tech_pk is not None and swversion_pk is None and parent_pk is not None \
            and search_term is not None and len(search_term) == 0:
        query = ManagedObject.query.filter_by(vendor_pk=vendor_pk, tech_pk=tech_pk,parent_pk=parent_pk)

    if vendor_pk is not None and tech_pk is not None and swversion_pk is None and parent_pk is not None \
            and search_term is not None and len(search_term) > 0:
        query = ManagedObject.query.filter_by(vendor_pk=vendor_pk, tech_pk=tech_pk).\
            filter(ManagedObject.name.ilike('%{}%'.format(search_term))).filter(ManagedObject.pk != parent_pk)

        # app.logger.info(str(query))
        # app.logger.info("vendor_pk:{}, tech_pk:{} parent_pk:{}, search_term:{}".format(vendor_pk, tech_pk, parent_pk, search_term))
        mo_children = []

        aci_tree_nodes = []
        for mo in query.all():
            p_pk = mo.parent_pk
            c_pk = mo.pk
            c_name = mo.name

            # app.logger.info("p_pk:{1}, c_pk:{0}, c_name: {2}".format(c_pk, p_pk, c_name))

            # Mark whether an MO is before the parent
            is_before_parent = False

            while p_pk != parent_pk:
                child_mo = ManagedObject.query.filter_by(pk=p_pk).first()

                if child_mo is None:
                    is_before_parent = True
                    break
                # app.logger.info("child_mo: {} pk:{}".format(child_mo.name, child_mo.pk))

                p_pk = child_mo.parent_pk
                c_pk = child_mo.pk
                c_name = child_mo.name

            if is_before_parent is True: continue

            if c_name not in mo_children:
                mo_aci_entries.append(dict(id=c_pk, label=c_name, inode=True, open=True))
                mo_children.append(c_name)

        # app.logger.info("Endi....")
        return jsonify(mo_aci_entries)

    if query is None:
        return jsonify([])

    for mo in query.all():
        mo_aci_entries.append(dict(id=mo.pk, label=mo.name, inode=True, open=False))

    # @TODO: Add pagination
    return jsonify(mo_aci_entries)


@mod_managedobjects.route('/tree/cached', methods=['GET'])
@login_required
def get_cached_mo_tree():
    """Get aci tree data from precomputed tree from the db"""
    vendor_pk = request.args.get("vendor_pk", None)
    tech_pk = request.args.get('tech_pk', None)

    metadata = MetaData()
    cache_table = Table('cache', metadata, autoload=True, autoload_with=db.engine, schema='public')

    # vendor = Ericsson, technology = UMTS or LTE
    if int(vendor_pk) == 1 and int(tech_pk) == 2:
        response = app.response_class(
            response=db.session.query(cache_table).filter_by(name="mo_aci_tree").first().data,
            status=200,
            mimetype='application/json'
        )
        return response

    return jsonify([])


@mod_managedobjects.route('/fields/<int:mo_pk>/', methods=['GET'])
@login_required
def get_fields_in_mo_table(mo_pk):
    """Get the column files in the managed objects cm data table"""

    fields = []

    # Get the vendor and technology pk's
    managedobject = ManagedObject.query.filter_by(pk=mo_pk).first()
    vendor_pk = managedobject.vendor_pk
    tech_pk = managedobject.tech_pk
    mo_name = managedobject.name

    # Get the schema
    managedobject_schema = ManagedObjectSchema.query.filter_by(vendor_pk=vendor_pk, tech_pk=tech_pk).first()
    schema_name = managedobject_schema.name.lower()
    mo_table_name = "{0}.{1}".format(schema_name,mo_name)

    metadata = MetaData()
    mo_data_table = Table(mo_name.lower(), metadata, autoload=True,autoload_with=db.engine, schema=schema_name)

    fields = [c.name for c in mo_data_table.columns]

    return jsonify(fields)


@mod_managedobjects.route('/dt/<int:mo_pk>/', methods=['GET'])
@login_required
def get_dt_data(mo_pk):
    """Get managed objects values in jQuery datatables format"""

    # Get the vendor and technology pk's
    managedobject = ManagedObject.query.filter_by(pk=mo_pk).first()
    vendor_pk = managedobject.vendor_pk
    tech_pk = managedobject.tech_pk
    mo_name = managedobject.name

    # Get the schema
    managedobject_schema = ManagedObjectSchema.query.filter_by(vendor_pk=vendor_pk, tech_pk=tech_pk).first()
    schema_name = managedobject_schema.name.lower()

    metadata = MetaData()
    mo_data_table = Table(mo_name.lower(), metadata, autoload=True, autoload_with=db.engine, schema=schema_name)

    columns = []
    for c in mo_data_table.columns:
        columns.append(ColumnDT( c, column_name=c.name, mData=c.name))

    query = db.session.query(mo_data_table)

    # GET request parameters
    params = request.args.to_dict()

    row_table = DataTables(params, query, columns)

    return jsonify(row_table.output_result())