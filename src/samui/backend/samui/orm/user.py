from enum import Enum

from sqlalchemy.ext.hybrid import hybrid_property

from ..common import db, bcrypt


class UserRole(Enum):
    """
    An Enum for all possible user roles.
    """
    USER = 'user'
    ADMIN = 'admin'


class User(db.Model):
    user_id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255))
    email = db.Column(db.String(255), unique=True)
    role = db.Column(db.Enum(UserRole))
    _password = db.Column('password', db.String(255))
    creation_time = db.Column(db.DateTime)
    last_login_time = db.Column(db.DateTime)

    organization_id = db.Column(db.Integer, db.ForeignKey('organization.organization_id'), nullable=False)
    organization = db.relationship('Organization', backref='members', lazy=True)

    @hybrid_property
    def password(self):
        return self._password

    @password.setter
    def password(self, plaintext):
        self._password = bcrypt.generate_password_hash(plaintext).decode('utf8')

    @staticmethod
    def get_user_with_email_and_password(email, password):
        user = User.query.filter_by(email=email).first()
        if user and bcrypt.check_password_hash(user.password, password):
            return user
        else:
            return None
