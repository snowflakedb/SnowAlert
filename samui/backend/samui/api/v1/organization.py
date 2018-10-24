import datetime as dt

from flask import Blueprint, request, jsonify
from sqlalchemy.exc import IntegrityError
import logbook

from ...common import db
from ...orm.user import UserRole
from ...orm.organization import Organization
from ...utils.auth import requires_auth

logger = logbook.Logger(__name__)

organization_api = Blueprint('organization', __name__)


@organization_api.route('', methods=['GET'])
def get_organizations():
    logger.info('Fetching details for all organizations...')
    organizations = [o.to_dict() for o in Organization.query.all()]

    # Create a mock organization to enable user registration.
    if not organizations:
        test_organization = Organization(title='Test', creation_time=dt.datetime.utcnow())
        db.session.add(test_organization)
        db.session.commit()
        return jsonify(organizations=[test_organization.to_dict()])

    return jsonify(organizations=organizations)


@organization_api.route('/<organization_id>', methods=['GET'])
@requires_auth([UserRole.USER, UserRole.ADMIN])
def get_organization(current_user, organization_id):
    logger.info(f'Fetching details for organization {organization_id}...')
    organization = db.session.query(Organization).filter(Organization.organization_id == organization_id).first()

    if not organization:
        return jsonify({
            'error': {
                'title': f'Organization {organization_id} Not Found',
                'details': 'An organization with that ID wasn\'t found'
            }
        }), 404

    if organization.organization_id != current_user.organization_id:
        return jsonify({
            'error': {
                'title': f'Unauthorized Access',
                'details': 'A user can only ask for details about his own organization'
            }
        }), 403

    return jsonify(organization.to_dict())


@organization_api.route('', methods=['POST'])
@requires_auth([UserRole.ADMIN])
def create_organization(current_user):
    incoming = request.get_json()
    title = incoming['title']

    # Add the organization.
    organization = Organization(title=title, owner_id=current_user.user_id, creation_time=dt.datetime.utcnow())
    db.session.add(organization)

    try:
        db.session.commit()
    except IntegrityError:
        return jsonify({
            'error': {
                'title': 'Organization Creation Failed',
                'details': 'An organization with that title already exists'
            }
        }), 409

    logger.info(f'Created a new organization: {title}')

    return jsonify(organization_id=organization.organization_id)
