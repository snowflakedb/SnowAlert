import pytest

from runners.helpers import db


@pytest.fixture(scope="session")
def db_schemas(request):
    db.connect()

    @request.addfinalizer
    def fin():
        db.execute('DROP SCHEMA data')
        db.execute('DROP SCHEMA rules')
        db.execute('DROP SCHEMA results')

    from scripts import install
    install.main(
        admin_role='snowalert_testing',
        samples=False,
        pk_passwd='',
        jira=False,
    )

    yield
