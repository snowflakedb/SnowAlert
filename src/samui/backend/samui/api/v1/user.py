import datetime as dt

from flask import Blueprint, request, jsonify, g
from sqlalchemy.exc import IntegrityError
import logbook

from ...common import db
from ...orm.user import User, UserRole
from ...utils.auth import generate_token, verify_token, requires_auth

logger = logbook.Logger(__name__)

user_api = Blueprint('user', __name__)


@user_api.route('', methods=['GET'])
@requires_auth([UserRole.USER, UserRole.ADMIN])
def get_user():
    return jsonify(result=g.current_user)


@user_api.route('', methods=['POST'])
def register():
    incoming = request.get_json()
    email = incoming['email']

    # Add the user.
    user = User(name=incoming['name'], email=email, password=incoming['password'], role=UserRole.USER,
                organization_id=incoming['organizationId'], creation_time=dt.datetime.utcnow())
    db.session.add(user)

    try:
        db.session.commit()
    except IntegrityError:
        return jsonify({
            'error': {
                'title': 'User Registration Failed',
                'details': 'A user with that email already exists'
            }
        }), 409

    logger.info(f'Created a new user: {email}')

    return jsonify(token=generate_token(user, remember=False))


@user_api.route('/login', methods=['POST'])
def login():
    incoming = request.get_json()
    email = incoming['email']
    user = User.get_user_with_email_and_password(email, incoming['password'])
    if user:
        logger.debug(f'User {email} logged in')

        # Update login time for user.
        user.last_login_time = dt.datetime.utcnow()
        db.session.commit()

        return jsonify(token=generate_token(user, remember=incoming['remember']))

    logger.debug(f'Failed login attempt for user: {email}')
    return jsonify({
        'error': {
            'title': 'Authentication Error',
            'details': 'Invalid email or password'
        }
    }), 403


@user_api.route('/validate', methods=['POST'])
def validate_token():
    incoming = request.get_json()
    is_valid = verify_token(incoming['token'])

    if is_valid:
        return jsonify(token_is_valid=True)
    else:
        return jsonify({
            'error': {
                'title': 'Authentication Error',
                'details': 'Invalid token'
            }
        }), 403
