from flask import Blueprint, request, render_template, \
                  flash, g, session, redirect, url_for, \
                  jsonify, make_response
from btsapi.modules.technologies.models import Technology, TechnologySchema
from btsapi.extensions import db
import datetime
from sqlalchemy import text
from flask_login import login_required

mod_technologies = Blueprint('technologies', __name__, url_prefix='/api/technologies')


@mod_technologies.route('/', methods=['GET'])
@login_required
def get_technologies():
    """Get a list of all the technologies in the system"""

    technology_schema = TechnologySchema()

    return jsonify( [technology_schema.dump(v).data for v in Technology.query.all()] )


@mod_technologies.route('/<int:id>', methods=['GET'])
@login_required
def get_technology(id):
    """Get technology details"""

    technology_schema = TechnologySchema()

    return jsonify(technology_schema.dump(Technology.query.get(id)).data)


@mod_technologies.route('/<int:id>', methods=['PUT'])
@login_required
def update_technology(id):
    """Update vendor details"""
    content = request.get_json()
    technology = Technology.query.filter_by(pk=id)

    if "name" in content: technology.name = content['name']
    if "notes" in content: technology.notes = content['notes']
    if "date_modified" in content: technology.date_modified = datetime.datetime.utcnow
    if "modified_by" in content: technology.modified_by = content['modified_by']

    db.session.commit()

    return jsonify({})


@mod_technologies.route('/<int:id>', methods=['DELETE'])
@login_required
def delete_technology(id):
    """Delete technology"""
    Technology.query.filter_by(pk=id).delete()

    db.session.commit()

    return jsonify({"status":"OK"})


@mod_technologies.route('/', methods=['POST'])
@login_required
def add_technology():
    """Add a technology"""
    content = request.get_json()
    technology = Technology(content['name'], content['notes'], content['added_by'], content['modified_by'],)

    db.session.add(technology)
    db.session.commit()

    return jsonify({"status":"OK"})