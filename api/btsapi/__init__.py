# Import flask and template operators
from flask import Flask, render_template, request
from flask.sessions import SecureCookieSessionInterface
from btsapi.extensions import db, ma, login_manager
from flask_login import UserMixin
from flask_cors import CORS
import base64
from flask_login import user_loaded_from_header, user_loaded_from_request
from flask import g
from btsapi.modules.users.models import User

# This prevents setting the Flask Session cookie whenever the user authenticated using your header_loader.
# Reference: https://flask-login.readthedocs.io/en/latest/
@user_loaded_from_header.connect
def user_loaded_from_header(self, user=None):
    g.login_via_header = True


class CustomSessionInterface(SecureCookieSessionInterface):
    """Prevent creating session from API requests."""
    def save_session(self, *args, **kwargs):
        if g.get('login_via_header'):
            return
        return super(CustomSessionInterface, self).save_session(*args,
                                                                **kwargs)

# #############################################################################


def create_app():
    # Define the WSGI application object
    app = Flask(__name__)

    # Disable strict forward slashed requirement
    app.url_map.strict_slashes = False

    # Configurations
    app.config.from_object('btsapi.config')

    # Define the database object which is imported
    # by modules and controllers
    db.init_app(app) #flask_sqlalchemy
    ma.init_app(app) #flask_marshmallow

    # Enable CORS -- Remove this if not useful
    CORS(app,  origins="*")

    login_manager.init_app(app)

    # Disable sessions for API calls
    app.session_interface = CustomSessionInterface()

    return app


@login_manager.request_loader
def load_user_from_header(req):
    """Handle API authentication"""
    token = req.headers.get('Authorization')

    if token is None:
        return None

    token = token.replace('Bearer ', '', 1)
    try:
        token = base64.b64decode(token)
    except TypeError:
        pass
    user = User.query.filter_by(token=token).first()
    if user:
        return user

    return None


# Initialize app and push application context so that extension can initiate the application context
app = create_app()
app.app_context().push()


# This is added to handle CORS
# Taken from https://mortoray.com/2014/04/09/allowing-unlimited-access-with-cors/
@app.after_request
def add_cors(resp):
    """ Ensure all responses have the CORS headers. This ensures any failures are also accessible
        by the client. """

    resp.headers['Access-Control-Allow-Origin'] = request.headers.get('Origin','*')
    resp.headers['Access-Control-Allow-Credentials'] = 'true'
    resp.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS, GET, PUT, DELETE'
    resp.headers['Access-Control-Allow-Headers'] = request.headers.get(
        'Access-Control-Request-Headers', 'Authorization' )
    # set low for debugging
    # if app.debug:
    #    resp.headers['Access-Control-Max-Age'] = '1'

    return resp


# Intercept pre-flight requests
@app.before_request
def handle_options_header():
    if request.method == 'OPTIONS':
        headers = {}
        headers['Access-Control-Allow-Origin'] = request.headers.get('Origin', '*')
        headers['Access-Control-Allow-Credentials'] = 'true'
        headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS, GET, PUT, DELETE'
        headers['Access-Control-Allow-Headers'] = request.headers.get(
            'Access-Control-Request-Headers', 'Authorization')
        return '', 200, headers


# Import modules using the blueprint handlers variable (mod_vendors)
from btsapi.modules.vendors.controllers import mod_vendors as vendors_module
from btsapi.modules.users.controllers import mod_users as mod_users
from btsapi.modules.authentication.controllers import mod_auth as mod_auth
from btsapi.modules.networkbaseline.controllers import mod_networkbaseline as mod_networkbaseline
from btsapi.modules.technologies.controllers import mod_technologies as mod_technologies
from btsapi.modules.managedobjects.controllers import mod_managedobjects as mod_managedobjects
from btsapi.modules.networkmanagement.controllers import mod_netmgt as mod_netmgt
from btsapi.modules.settings.controllers import mod_settings as mod_settings
from btsapi.modules.networkaudit.controllers import mod_networkaudit as mod_networkaudit

# Register blueprint(s)
app.register_blueprint(vendors_module)
app.register_blueprint(mod_users)
app.register_blueprint(mod_auth)
app.register_blueprint(mod_networkbaseline)
app.register_blueprint(mod_technologies)
app.register_blueprint(mod_managedobjects)
app.register_blueprint(mod_netmgt)
app.register_blueprint(mod_settings)
app.register_blueprint(mod_networkaudit)


# TP error handling
@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404

