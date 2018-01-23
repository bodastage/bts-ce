from flask_sqlalchemy import SQLAlchemy
from btsapi.extensions import  db, ma
import datetime
from sqlalchemy import Table, MetaData
import marshmallow


metadata = MetaData()


class Setting(db.Model):
    """Settings model"""
    __table__ = Table('settings', metadata, autoload=True, autoload_with=db.engine, schema='public')
    pass


class SettingMASchema(ma.ModelSchema):
    """Settings marshmallow schema"""
    value = marshmallow.fields.Method("get_actual_value")
    id = marshmallow.fields.Method("get_id")

    def get_id(self,model):
        return model.pk

    def get_actual_value(self,model):
        """Return a single value from the settings table for each setting"""
        if model.data_type == 'string': return model.string_value
        if model.data_type == 'integer': return model.integer_value
        if model.data_type == 'float' or model.data_type == 'double' : return model.float_value
        if model.data_type == 'long_string': return model.long_string_value
        if model.data_type == 'timestamp': return model.timestamp_value
        return model.string_value

    class Meta:
        model = Setting
        fields = ('id','name', 'label', 'value', 'category')
