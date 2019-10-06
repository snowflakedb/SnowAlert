import pytest

from runners.helpers import db, dbconfig
from scripts import install


@pytest.fixture(scope="session")
def db_schemas(request):
    @request.addfinalizer
    def fin():
        install.main(uninstall=True)

    install.main(samples=True, pk_passphrase='', jira=False, set_env_vars=True)

    # reload to pick up env vars set by installer
    import importlib

    importlib.reload(dbconfig)
    importlib.reload(db)
    importlib.reload(install)

    yield


@pytest.fixture
def delete_results():
    yield
    db.execute(f"DELETE FROM results.alerts")
    db.execute(f"DELETE FROM results.violations")
    db.execute(f"DELETE FROM results.run_metadata")
    db.execute(f"DELETE FROM results.query_metadata")
