from flask_sqlalchemy import SQLAlchemy
from btsapi import db, ma;
import datetime

class User(db.Model):
    """Users model"""

    __tablename__ = 'users'

    pk = db.Column(db.Integer, db.Sequence('seq_users_pk', ), primary_key=True, nullable=False)
    username = db.Column(db.String(255), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)
    first_name = db.Column(db.String(255), nullable=False)
    last_name = db.Column(db.String(255))
    other_names = db.Column(db.String(255))
    job_title = db.Column(db.String(255))
    phone_number = db.Column(db.String(255))
    photo = db.Column(db.Text)
    token = db.Column(db.String(255))
    is_account_non_expired = db.Column(db.Boolean, default=True)
    is_account_non_locked = db.Column(db.Boolean, default=True)
    is_enabled = db.Column(db.Boolean, default=True)

    def __init__(self,username, password,first_name, last_name,other_names, phone_number, photo, job_title, token,
                 is_account_non_expired, is_account_non_locked, is_enabled):
        self.username = username
        self.password = password
        self.first_name = first_name
        self.last_name = last_name
        self.other_names = other_names
        self.phone_number = phone_number
        self.photo = photo
        self.job_title = job_title
        self.token = token
        self.is_account_non_expired = is_account_non_expired
        self.is_account_non_locked = is_account_non_locked
        self.is_enabled = is_enabled

    def is_authenticated(self):
        return True

    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return str(self.email)

class UserSchema(ma.ModelSchema):
    """Flask Marshmallow Schema for Vendor model"""

    class Meta:
        model = User
        fields = ('pk','username','password','first_name','last_name','other_names','job_title','phone_number','photo')