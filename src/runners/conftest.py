import pytest

from runners.helpers import db
from scripts import install


@pytest.fixture(scope="session")
def db_schemas(request):
    db.connect()

    @request.addfinalizer
    def fin():
        install.main(
            admin_role=None,
            uninstall=True,
        )

    install.main(
        admin_role=None,  # uses user's default_role
        samples=False,
        pk_passwd='',
        jira=False,
    )

    yield
