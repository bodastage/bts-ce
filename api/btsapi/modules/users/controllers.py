from flask import Blueprint, request, render_template, \
                  flash, g, session, redirect, url_for, \
                  jsonify, make_response
from btsapi.modules.users.models import User, UserSchema
from btsapi.extensions import db
import datetime
from flask_login import login_required

mod_users = Blueprint('users', __name__, url_prefix='/api/users')


@mod_users.route('/', methods=['GET'])
@login_required
def get_users():
    """Get a list of all the users in the system"""

    ma_schema = UserSchema()

    return jsonify( [ma_schema.dump(v).data for v in User.query.all()] )


@mod_users.route('/<int:id>', methods=['GET'])
@login_required
def get_user(id):
    """Get vendor details"""

    ma_schema = UserSchema()

    return jsonify(ma_schema.dump(User.query.get(id)).data)


@mod_users.route('/<int:id>', methods=['PUT'])
@login_required
def update_user(id):
    """Update user details"""
    content = request.get_json()
    user = User.query.filter_by(pk=id).first()

    # @TODO: Throw exception if an attempt to change the user name is made here

    if "first_name" in content: user.first_name = content['first_name']
    if "last_name" in content: user.last_name = content['last_name']
    if "other_names" in content: user.other_names = content['other_names']
    if "phone_number" in content: user.first_name = content['phone_number']
    if "photo" in content: user.photo = content['photo']
    if "password" in content: user.photo = content['password']

    db.session.commit()

    return jsonify({})


@mod_users.route('/<int:id>', methods=['DELETE'])
@login_required
def delete_user(id):
    """Delete user"""
    User.query.filter_by(pk=id).delete()

    db.session.commit()

    return jsonify({"status":"OK"})


@mod_users.route('/', methods=['POST'])
@login_required
def add_user():
    """Add a user"""
    content = request.get_json()

    user = User(username=content['username'],
                first_name=content['first_name'],
                last_name=content['last_name'],
                other_names=content['other_names'],
                phone_number=content['phone_number'],
                job_title=content['job_title'],
                is_account_non_expired=content['is_account_non_expired'],
                is_account_non_locked=content['is_account_non_locked'],
                is_enabled=content['is_enabled'],
                token=content['token'],
                photo=content['photo'],)

    db.session.add(user)
    db.session.commit()

    return jsonify({"status":"OK"})