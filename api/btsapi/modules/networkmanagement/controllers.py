from flask import Blueprint, request, render_template, \
                  flash, g, session, redirect, url_for, \
                  jsonify, make_response
from btsapi.modules.networkmanagement.models import LiveCell3G, LiveCell3GMASchema
from btsapi.extensions import  db
import datetime
import math
from sqlalchemy import Table, MetaData
from datatables import DataTables, ColumnDT
from btsapi import app
from flask_login import login_required

mod_netmgt = Blueprint('networkmanagement', __name__, url_prefix='/api/network')


@mod_netmgt.route('/live/cells/3g', methods=['GET'], strict_slashes=False)
@login_required
def get_live_network_cells():
    page = request.args.get('page', 0)
    size = request.args.get('size', 10)
    search = request.args.get('search', None)

    # includes direction i.e column:asc or column:desc , default is natual order
    order_by = request.args.get('order_by', None)

    total = LiveCell3G.query.count()

    query = LiveCell3G.query.limit(size).offset(page)

    # @TODO: Add search filter
    if search is not None:
        query = query.filter(LiveCell3G.name.ilike('%{}%'.format(search)))

    if order_by is not None:
        query = query.order_by(order_by)

    cell_schema = LiveCell3GMASchema()

    pages = int(math.ceil(total/float(size)))
    return jsonify({
        "content":     [cell_schema.dump(v).data for v in query.all()],
        "size": size,
        "page": page,
        "last": ( int(page) == pages),
        "total": total,
        "pages": pages
    })


@mod_netmgt.route('/tree/cached', methods=['GET'], strict_slashes=False)
@login_required
def get_network_tree():
    """Get network pre computed live network tree"""
    source = request.args.get('source', "live") # live or plan

    # @TODO: Create model definitions
    metadata = MetaData()
    cache_table = Table('cache', metadata, autoload=True, autoload_with=db.engine, schema='public')

    response = app.response_class(
        response=db.session.query(cache_table).filter_by(name="live_network_aci_tree").first().data,
        status=200,
        mimetype='application/json'
    )
    return response


@mod_netmgt.route('/relations/dt', methods=['GET'], strict_slashes=False)
@login_required
def get_relations_dt_data():
    """Get relations in jQuery datatable data format"""

    metadata = MetaData()
    relations_table = Table('vw_relations', metadata, autoload=True, autoload_with=db.engine, schema='live_network')

    columns = []
    for c in relations_table.columns:
        columns.append(ColumnDT( c, column_name=c.name, mData=c.name))

    query = db.session.query(relations_table)

    # GET request parameters
    params = request.args.to_dict()

    row_table = DataTables(params, query, columns)

    return jsonify(row_table.output_result())


@mod_netmgt.route('/nodes/dt', methods=['GET'], strict_slashes=False)
@login_required
def get_nodes_dt_data():
    """Get nodes in jQuery datatable data format"""

    metadata = MetaData()
    nodes_table = Table('vw_nodes', metadata, autoload=True, autoload_with=db.engine, schema='live_network')

    columns = []
    for c in nodes_table.columns:
        columns.append(ColumnDT( c, column_name=c.name, mData=c.name))

    query = db.session.query(nodes_table)

    # GET request parameters
    params = request.args.to_dict()

    row_table = DataTables(params, query, columns)

    return jsonify(row_table.output_result())


@mod_netmgt.route('/sites/dt', methods=['GET'], strict_slashes=False)
@login_required
def get_site_dt_data():
    """Get sites in jQuery datatable data format"""

    metadata = MetaData()
    sites_table = Table('vw_sites', metadata, autoload=True, autoload_with=db.engine, schema='live_network')

    columns = []
    for c in sites_table.columns:
        columns.append(ColumnDT( c, column_name=c.name, mData=c.name))

    query = db.session.query(sites_table)

    # GET request parameters
    params = request.args.to_dict()

    row_table = DataTables(params, query, columns)

    return jsonify(row_table.output_result())


@mod_netmgt.route('/live/cells/fields', methods=['GET'], strict_slashes=False)
@login_required
def get_network_cells_field_list():
    """Get field list"""
    fields = []

    tech_pk = request.args.get("tech_pk","2") # Default is 3G
    cell_data_table = None

    if tech_pk == "1":
        metadata = MetaData()
        cell_data_table = Table('vw_gsm_cells_data', metadata, autoload=True, autoload_with=db.engine,
                                schema='live_network')

    if tech_pk == "2":
        metadata = MetaData()
        cell_data_table = Table('vw_umts_cells_data', metadata, autoload=True, autoload_with=db.engine,
                                schema='live_network')

    if tech_pk == "3":
        metadata = MetaData()
        cell_data_table = Table('vw_lte_cells_data', metadata, autoload=True, autoload_with=db.engine,
                                schema='live_network')

    if cell_data_table is None:
        return jsonify([])

    fields = [c.name for c in cell_data_table.columns]

    return jsonify(fields)


@mod_netmgt.route('/live/cells/dt', methods=['GET'], strict_slashes=False)
@login_required
def get_cells_dt_data():
    """Get sites in jQuery datatable data format"""

    tech_pk = request.args.get("tech_pk", "2")  # Default is 3G

    cell_data_table = None

    metadata = MetaData()

    # UMTS Cells
    if tech_pk == "2":
        cell_data_table = Table('vw_umts_cells_data', metadata, autoload=True, autoload_with=db.engine,
                                schema='live_network')

    # GSM Cells
    if tech_pk == "1":
        cell_data_table = Table('vw_gsm_cells_data', metadata, autoload=True, autoload_with=db.engine,
                                schema='live_network')

    # LTE Cells
    if tech_pk == "3":
        cell_data_table = Table('vw_lte_cells_data', metadata, autoload=True, autoload_with=db.engine,
                                schema='live_network')


    columns = []
    for c in cell_data_table.columns:
        columns.append(ColumnDT( c, column_name=c.name, mData=c.name))

    query = db.session.query(cell_data_table)

    # GET request parameters
    params = request.args.to_dict()

    row_table = DataTables(params, query, columns)

    return jsonify(row_table.output_result())


@mod_netmgt.route('/live/extcells/fields', methods=['GET'], strict_slashes=False)
@login_required
def get_network_extcells_field_list():
    """Get external cells field/parameter """
    fields = []

    tech_pk = request.args.get("tech_pk","2") # Default is 3G
    cell_data_table = None

    if tech_pk == "1":
        metadata = MetaData()
        cell_data_table = Table('vw_gsm_external_cells', metadata, autoload=True, autoload_with=db.engine,
                                schema='live_network')

    if tech_pk == "2":
        metadata = MetaData()
        cell_data_table = Table('vw_umts_external_cells', metadata, autoload=True, autoload_with=db.engine,
                                schema='live_network')

    if cell_data_table is None:
        return jsonify([])

    fields = [c.name for c in cell_data_table.columns]

    return jsonify(fields)



@mod_netmgt.route('/live/extcells/dt', methods=['GET'], strict_slashes=False)
@login_required
def get_external_cells_dt_data():
    """Get external cells parameter data in jQuery datatable data format"""

    tech_pk = request.args.get("tech_pk", "2")  # Default is 3G

    cell_data_table = None

    metadata = MetaData()

    # UMTS External Cells
    if tech_pk == "2":
        cell_data_table = Table('vw_umts_external_cells', metadata, autoload=True, autoload_with=db.engine,
                                schema='live_network')

    # GSM External Cells
    if tech_pk == "1":
        cell_data_table = Table('vw_gsm_external_cells', metadata, autoload=True, autoload_with=db.engine,
                                schema='live_network')

    if cell_data_table is None:
        return jsonify([])

    columns = []
    for c in cell_data_table.columns:
        columns.append(ColumnDT( c, column_name=c.name, mData=c.name))

    query = db.session.query(cell_data_table)

    # GET request parameters
    params = request.args.to_dict()

    row_table = DataTables(params, query, columns)

    return jsonify(row_table.output_result())