from flask import Blueprint, request, render_template, \
                  flash, g, session, redirect, url_for, \
                  jsonify, make_response
from btsapi.modules.vendors.models import Vendor, VendorSchema
from btsapi.extensions import db
import datetime
from sqlalchemy import text
from flask_login import login_required

# Define the blueprint: 'auth', set its url prefix: app.url/auth
mod_vendors = Blueprint('vendors', __name__, url_prefix='/api/vendors')


@mod_vendors.route('/', methods=['GET'])
@login_required
def get_vendors():
    """Get a list of all the vendors in the system"""

    vendor_schema = VendorSchema()

    return jsonify( [vendor_schema.dump(v).data for v in Vendor.query.all()] )


@mod_vendors.route('/<int:id>', methods=['GET'])
@login_required
def get_vendor(id):
    """Get vendor details"""

    vendor_schema = VendorSchema()

    return jsonify(vendor_schema.dump(Vendor.query.get(id)).data)


@mod_vendors.route('/<int:id>', methods=['PUT'])
@login_required
def update_vendor(id):
    """Update vendor details"""
    content = request.get_json()
    vendor = Vendor.query.filter_by(pk=id)

    if "name" in content: vendor.name = content['name']
    if "notes" in content: vendor.notes = content['notes']
    if "date_modified" in content: vendor.date_modified = datetime.datetime.utcnow
    if "modified_by" in content: vendor.modified_by = content['modified_by']

    db.session.commit()

    return jsonify({})


@mod_vendors.route('/<int:id>', methods=['DELETE'])
@login_required
def delete_vendor(id):
    """Delete vendor"""
    Vendor.query.filter_by(pk=id).delete()

    db.session.commit()

    return jsonify({"status":"OK"})


@mod_vendors.route('/', methods=['POST'])
@login_required
def add_vendor():
    """Add a vendor"""
    content = request.get_json()
    vendor = Vendor(content['name'], content['notes'], content['added_by'], content['modified_by'],)

    db.session.add(vendor)
    db.session.commit()

    return jsonify({"status":"OK"})