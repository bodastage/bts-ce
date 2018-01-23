# Import SQLAlchemy
from flask_sqlalchemy import SQLAlchemy

# Import Flask Marshmallow
from flask_marshmallow import Marshmallow

# Login
from flask_login import user_loaded_from_header, user_loaded_from_request, LoginManager

ma = Marshmallow()
db = SQLAlchemy()
login_manager = LoginManager()