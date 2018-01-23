from flask import Blueprint, request, render_template, \
                  flash, g, session, redirect, url_for, \
                  jsonify, make_response
from btsapi.modules.users.models import User, UserSchema
from btsapi.extensions import db
import datetime
import base64

# @TODO: Change this endpoint to /api/authentication
mod_auth = Blueprint('authetication', __name__, url_prefix='/authenticate')


@mod_auth.route('/', methods=['POST'])
def authenticate_user():
    """Authenticate user"""
    username = request.form['username']
    password = request.form['password']

    user = User.query.filter_by(username=username, password=password).first()

    if user is not None:
        ma_schema = UserSchema()
        user_data = ma_schema.dump(user).data
        user_data['id'] = user.pk
        user_data['token'] = base64.b64encode(user.token)

        del user_data['pk']

        return jsonify(user_data)
    else:
        return jsonify({"message":"Invalid credentials"}),404
