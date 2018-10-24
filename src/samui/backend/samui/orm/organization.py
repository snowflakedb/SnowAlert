from ..common import db


class Organization(db.Model):
    organization_id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(255), unique=True)
    creation_time = db.Column(db.DateTime)

    def to_dict(self):
        return {
            'organization_id': self.organization_id,
            'title': self.title,
            'creation_time': self.creation_time
        }
