from flask import Blueprint, request, render_template, \
                  flash, g, session, redirect, url_for, \
                  jsonify, make_response
from btsapi.modules.networkbaseline.models import NetworkBaseline, NetworkBaselineView, NetworkBaselineViewSchema
from btsapi.extensions import db
import datetime
from datatables import DataTables, ColumnDT
from flask_login import login_required

mod_networkbaseline = Blueprint('networkbaseline', __name__, url_prefix='/api/networkbaseline')


@mod_networkbaseline.route('/dt/', methods=['GET'])
@login_required
def get_dt_data():
    """Get baseline values in jQuery datatables format"""

    # Define columns
    columns = [
        ColumnDT(NetworkBaselineView.vendor,column_name="vendor", mData="vendor"),
        ColumnDT(NetworkBaselineView.technology, column_name="technology", mData="technology"),
        ColumnDT(NetworkBaselineView.mo, column_name="mo", mData="mo"),
        ColumnDT(NetworkBaselineView.parameter, column_name="parameter", mData="parameter"),
        ColumnDT(NetworkBaselineView.value, column_name="value", mData="value"),
        ColumnDT(NetworkBaselineView.date_added, column_name="date_added", mData="date_added"),
        ColumnDT(NetworkBaselineView.date_modified, column_name="date_modified", mData="date_modified")
    ]

    query = db.session.query(NetworkBaselineView.vendor, NetworkBaselineView.technology, NetworkBaselineView.mo,
                              NetworkBaselineView.parameter, NetworkBaselineView.value, NetworkBaselineView.date_added,
                              NetworkBaselineView.date_modified)

    # GET request parameters
    params = request.args.to_dict()

    row_table = DataTables(params, query, columns)

    return jsonify(row_table.output_result())