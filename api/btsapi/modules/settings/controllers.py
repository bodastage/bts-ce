from flask import Blueprint, request, render_template, \
    flash, g, session, redirect, url_for, \
    jsonify, make_response
from btsapi.modules.settings.models import Setting, SettingMASchema
from btsapi.extensions import  db
import datetime
import math
from flask_login import login_required

mod_settings = Blueprint('settings', __name__, url_prefix='/api/settings')


@mod_settings.route('/', methods=['GET'], strict_slashes=False)
@login_required
def get_all_settings():
    """Get all settings"""

    setting_schema = SettingMASchema()

    return jsonify( [setting_schema.dump(v).data for v in Setting.query.all()] )


@mod_settings.route('/<string:name>', methods=['GET'], strict_slashes=False)
@login_required
def get_setting_value_by_name(name):
    """Get value for a setting"""

    setting_schema = SettingMASchema()

    return jsonify(setting_schema.dump(Setting.query.filter_by(name=name).first()).data)


@mod_settings.route('/<int:id>', methods=['GET'], strict_slashes=False)
@login_required
def get_setting_value_by_id(id):
    """Get value for a setting"""

    setting_schema = SettingMASchema()

    return jsonify(setting_schema.dump(Setting.query.filter_by(pk=id).first()).data)


@mod_settings.route('/category/<cat_id>', methods=['GET'], strict_slashes=False)
@login_required
def get_settings_by_category_id(cat_id):
    """Get value for a setting"""

    setting_schema = SettingMASchema()

    return jsonify(setting_schema.dump(Setting.query.filter_by(category_id=cat_id).all(), many=True).data)


@mod_settings.route('/<int:id>',methods=['POST'], strict_slashes=False)
@login_required
def update_setting(id):
    """Update setting

       data_type = string|integer|timestamp|float
    """
    content = request.get_json()

    name = content['name']
    value= content['value']
    data_type = content['date_type']

    setting = Setting.query.filter_by(pk=id)

    if "name" in content: setting.name = content['name']
    if "value" in content:
        if data_type == 'string': setting.string_value = content['value']
        if data_type == 'integer': setting.integer_value = int(content['value'])
        if data_type == 'float': setting.float_value = float(content['value'])
        if data_type == 'timestamp': setting.timestamp_value = datetime(content['value'])

    db.session.commit()

    return jsonify({})