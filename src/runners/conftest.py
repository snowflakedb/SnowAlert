import pytest

from runners.helpers import db, dbconfig
from scripts import install


@pytest.fixture(scope="session")
def db_schemas(request):
    @request.addfinalizer
    def fin():
        install.main(
            uninstall=True,
        )

    install.main(
        samples=False,
        pk_passwd='',
        jira=False,
        set_env_vars=True,
    )

    # reload to pick up env vars set by installer
    import importlib
    importlib.reload(dbconfig)
    importlib.reload(db)
    importlib.reload(install)

    yield
